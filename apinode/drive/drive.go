// Copyright 2023 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package drive

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/cubefs/cubefs/apinode/crypto"
	"github.com/cubefs/cubefs/apinode/oplog"
	"github.com/cubefs/cubefs/apinode/oplog/kafka"
	"github.com/cubefs/cubefs/apinode/sdk"
	"github.com/cubefs/cubefs/apinode/sdk/impl"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/closer"
	"github.com/cubefs/cubefs/blobstore/util/log"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/config"
	"golang.org/x/sync/singleflight"
	"golang.org/x/time/rate"
	"gopkg.in/natefinch/lumberjack.v2"
)

// const vaules.
const (
	headerRange = "Range"
	contextSpan = "context-span"

	HeaderRequestID = "x-cfa-trace-id"
	HeaderUserID    = "x-cfa-user-id"
	HeaderVolume    = "x-cfa-volume"
	HeaderCrc32     = "x-cfa-content-crc32"
	ChecksumPrefix  = "x-cfa-content-"
	// headerSign      = "x-cfa-sign"
	HeaderCipherMeta = "x-cfa-cipher-meta"
	HeaderCipherBody = "x-cfa-cipher-body"

	UserPropertyPrefix = "x-cfa-meta-"

	typeFile   = "file"
	typeFolder = "folder"

	internalMetaPrefix = "x-cfa-"
	internalMetaMD5    = internalMetaPrefix + "md5"

	maxMultipartNumber = 10000
)

var noneTransmitter, _ = crypto.NoneCryptor().Transmitter("")

// TODO: defines inode in sdk.

// Inode type of inode.
type Inode uint64

// Uint64 returns uint64.
func (i Inode) Uint64() uint64 {
	return uint64(i)
}

// FileID file id.
type FileID = Inode

// FilePath cleaned file path.
//
//	It ends with a separator if path is directory.
type FilePath string

// Clean replace origin path to cleaned path.
func (p *FilePath) Clean() error {
	s := p.String()
	path := filepath.Clean(s)
	if path == "" || path[0] != '/' {
		return sdk.ErrInvalidPath
	}
	*p = FilePath(path)
	return nil
}

// Split splits path immediately following the final path separator.
func (p *FilePath) Split() (FilePath, string) {
	dir, filename := filepath.Split(string(*p))
	return FilePath(dir), filename
}

func (p *FilePath) String() string {
	return string(*p)
}

// UserID user id.
type UserID string

// Valid return user id is empty or not.
func (u *UserID) Valid() bool {
	return *u != ""
}

type FileInfo struct {
	ID         uint64            `json:"fileId"`
	Name       string            `json:"name"`
	Type       string            `json:"type"`
	Size       int64             `json:"size"`
	Ctime      int64             `json:"ctime"`
	Mtime      int64             `json:"mtime"`
	Atime      int64             `json:"atime"`
	Properties map[string]string `json:"properties"`
}

func (fi *FileInfo) IsDir() bool {
	return fi.Type == typeFolder
}

func inode2file(ino *sdk.InodeInfo, fileID uint64, name string, properties map[string]string) *FileInfo {
	typ := typeFile
	if proto.IsDir(ino.Mode) {
		typ = typeFolder
	}
	return &FileInfo{
		ID:         fileID,
		Name:       name,
		Type:       typ,
		Size:       int64(ino.Size),
		Ctime:      ino.CreateTime.Unix(),
		Mtime:      ino.ModifyTime.Unix(),
		Atime:      ino.AccessTime.Unix(),
		Properties: properties,
	}
}

const (
	maxTaskPoolSize     = 8
	defaultLimiterBurst = 2000
)

type ArgsListDir struct {
	Path   FilePath `json:"path"`
	Marker string   `json:"marker,omitempty"`
	Limit  int      `json:"limit"`
	Filter string   `json:"filter,omitempty"`
}

type ArgsGetProperties struct {
	Path FilePath `json:"path"`
}

type ArgsSetProperties struct {
	Path FilePath `json:"path"`
}

type ArgsDelProperties struct {
	Path FilePath `json:"path"`
}

type kafkaConfig struct {
	Addrs            string `json:"addrs"`
	Topic            string `json:"topic"`
	FailedRecordFile string `json:"failedRecordFile"`
}

// DriveNode drive node.
type DriveNode struct {
	masterAddr  string      // the master address of the default cluster
	clusterID   string      // default cluster id
	volumeName  string      // default volume name
	vol         sdk.IVolume // default volume
	clusters    []string    // all cluster id
	mu          sync.RWMutex
	userRouter  *userRouteMgr
	clusterMgr  sdk.ClusterManager
	groupRouter singleflight.Group // for get user route
	limiter     *rate.Limiter

	out      *oplog.Output
	recorder io.Writer

	cryptor crypto.Cryptor

	closer.Closer
}

// New returns a drive node.
func New(limiter *rate.Limiter) *DriveNode {
	cm := impl.NewClusterMgr()
	urm, err := NewUserRouteMgr()
	if err != nil {
		log.Fatal(err)
	}
	if err := crypto.Init(); err != nil {
		log.Fatal(err)
	}
	return &DriveNode{
		userRouter: urm,
		clusterMgr: cm,
		limiter:    limiter,
		out:        oplog.NewOutput(),
		cryptor:    crypto.NewCryptor(),
		Closer:     closer.New(),
	}
}

func (d *DriveNode) Start(cfg *config.Config) error {
	d.masterAddr = cfg.GetString(proto.MasterAddr)
	d.clusterID = cfg.GetString("clusterID")
	d.volumeName = cfg.GetString("volumeName")

	_, ctx := trace.StartSpanFromContext(context.TODO(), "")
	if err := d.clusterMgr.AddCluster(ctx, d.clusterID, d.masterAddr); err != nil {
		log.Errorf("add cluster [clusterID: %s, masterAddr: %s] error: %v", d.clusterID, d.masterAddr, err)
		return err
	}
	cluster := d.clusterMgr.GetCluster(d.clusterID)
	if cluster == nil {
		return fmt.Errorf("not get cluster clusterID: %s", d.clusterID)
	}
	d.vol = cluster.GetVol(d.volumeName)
	if d.vol == nil {
		return fmt.Errorf("not get volume volumeName: %s", d.volumeName)
	}

	if err := d.initClusterConfig(); err != nil {
		return err
	}

	if err := d.initOplog(cfg); err != nil {
		return err
	}
	go d.run()
	return nil
}

func (d *DriveNode) GetUserRouteInfo(ctx context.Context, uid UserID) (*UserRoute, error) {
	ur := d.userRouter.Get(uid)
	if ur == nil {
		// query file and set cache
		r, err, _ := d.groupRouter.Do(string(uid), func() (interface{}, error) {
			r, err := d.getUserRouteFromFile(ctx, uid)
			if err != nil {
				return nil, err
			}
			d.userRouter.Set(uid, r)
			return r, nil
		})
		if err != nil {
			return nil, err
		}
		ur = r.(*UserRoute)
	}
	return ur, nil
}

func (d *DriveNode) getFileEncryptor(ctx context.Context, key []byte, r io.Reader) (io.Reader, error) {
	span := trace.SpanFromContextSafe(ctx)
	st := time.Now()
	er, err := d.cryptor.FileEncryptor(key, r)
	span.AppendTrackLog("ccfe", st, err)
	return er, err
}

func (d *DriveNode) getFileDecryptor(ctx context.Context, key []byte, r io.Reader) (io.Reader, error) {
	span := trace.SpanFromContextSafe(ctx)
	st := time.Now()
	er, err := d.cryptor.FileDecryptor(key, r)
	span.AppendTrackLog("ccfd", st, err)
	return er, err
}

// get full path and volume by uid
// filePath is an absolute of client
func (d *DriveNode) getUserRouterAndVolume(ctx context.Context, uid UserID) (*UserRoute, sdk.IVolume, error) {
	span := trace.SpanFromContextSafe(ctx)
	st := time.Now()
	defer func() { span.AppendTrackLog("civ", st, nil) }()
	ur, err := d.GetUserRouteInfo(ctx, uid)
	if err != nil {
		return nil, nil, err
	}
	cluster := d.clusterMgr.GetCluster(ur.ClusterID)
	if cluster == nil {
		return nil, nil, sdk.ErrNoCluster
	}
	volume := cluster.GetVol(ur.VolumeID)
	if volume == nil {
		return nil, nil, sdk.ErrNoVolume
	}
	return ur, volume, nil
}

func (d *DriveNode) lookupFile(ctx context.Context, vol sdk.IVolume, parentIno Inode, path string) (*sdk.DirInfo, error) {
	info, err := d.lookup(ctx, vol, parentIno, path)
	if err != nil {
		return nil, err
	}
	if info.IsDir() {
		return nil, sdk.ErrNotFile
	}
	return info, nil
}

func (d *DriveNode) lookupDir(ctx context.Context, vol sdk.IVolume, parentIno Inode, path string) (*sdk.DirInfo, error) {
	info, err := d.lookup(ctx, vol, parentIno, path)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, sdk.ErrNotDir
	}
	return info, nil
}

func (d *DriveNode) lookupFileID(ctx context.Context, vol sdk.IVolume, parentIno Inode, path string, fileID uint64) (err error) {
	dirInfo, err := d.lookup(ctx, vol, parentIno, path)
	if err == sdk.ErrNotFound {
		if fileID != 0 {
			return sdk.ErrConflict
		}
		return nil
	} else if err != nil {
		return err
	}
	if dirInfo.FileId != fileID {
		return sdk.ErrConflict
	}
	return nil
}

func (d *DriveNode) lookup(ctx context.Context, vol sdk.IVolume, parentIno Inode, path string) (info *sdk.DirInfo, err error) {
	err = sdk.ErrBadRequest
	span := trace.SpanFromContextSafe(ctx)
	st := time.Now()
	defer func() { span.AppendTrackLog("clu", st, err) }()
	names := strings.Split(path, "/")
	for _, name := range names {
		if name == "" {
			continue
		}
		info, err = vol.Lookup(ctx, parentIno.Uint64(), name)
		if err != nil {
			return
		}
		parentIno = Inode(info.Inode)
	}
	return
}

func (d *DriveNode) createDir(ctx context.Context, vol sdk.IVolume, parentIno Inode, path string, recursive bool) (ino Inode, fileID uint64, err error) {
	span := trace.SpanFromContextSafe(ctx)
	st := time.Now()
	defer func() { span.AppendTrackLog("ccd", st, err) }()
	ino = parentIno
	fileID = 0
	if path == "" || path == "/" {
		return
	}

	var (
		dirInfo *sdk.DirInfo
		inoInfo *sdk.InodeInfo
	)
	err = sdk.ErrBadRequest
	names := strings.Split(path, "/")
	for i, name := range names {
		if name == "" {
			continue
		}
		dirInfo, err = vol.Lookup(ctx, parentIno.Uint64(), name)
		if err != nil {
			if err != sdk.ErrNotFound {
				return
			}
			if i != len(names)-1 && !recursive {
				return
			}
			inoInfo, fileID, err = vol.Mkdir(ctx, parentIno.Uint64(), name)
			if err != nil {
				if err != sdk.ErrExist {
					return
				}
				dirInfo, err = vol.Lookup(ctx, parentIno.Uint64(), name)
				if err != nil {
					return
				}
				if !dirInfo.IsDir() {
					err = sdk.ErrConflict
					return
				}
				parentIno = Inode(dirInfo.Inode)
				fileID = dirInfo.FileId
			} else {
				parentIno = Inode(inoInfo.Inode)
			}
			ino = parentIno
		} else {
			if !dirInfo.IsDir() {
				err = sdk.ErrConflict
				return
			}
			parentIno = Inode(dirInfo.Inode)
			fileID = dirInfo.FileId
			ino = parentIno
		}
	}
	return
}

func (d *DriveNode) createFile(ctx context.Context, vol sdk.IVolume, parentIno Inode, path string) (info *sdk.InodeInfo, fileID uint64, err error) {
	dir, file := filepath.Split(filepath.Clean(path))
	if file == "" {
		err = sdk.ErrBadRequest
		return
	}
	if dir != "" && dir != "/" {
		parentIno, _, err = d.createDir(ctx, vol, parentIno, dir, true)
		if err != nil {
			return
		}
	}
	info, fileID, err = vol.CreateFile(ctx, parentIno.Uint64(), file)
	if err != nil {
		if err != sdk.ErrExist {
			return
		}
		var dirInfo *sdk.DirInfo
		dirInfo, err = vol.Lookup(ctx, parentIno.Uint64(), file)
		if err != nil {
			return
		}
		fileID = dirInfo.FileId
		info, err = vol.GetInode(ctx, dirInfo.Inode)
	}
	return
}

func (d *DriveNode) initOplog(cfg *config.Config) error {
	oplogMap := cfg.GetValue("oplog")
	if oplogMap == nil {
		return nil
	}
	kafkaMap := oplogMap.(map[string]interface{})["kafka"]
	if kafkaMap != nil {
		data, err := json.Marshal(kafkaMap)
		if err != nil {
			return err
		}

		cfg := kafkaConfig{}
		if err = json.Unmarshal(data, &cfg); err != nil {
			return err
		}

		if cfg.FailedRecordFile == "" {
			return fmt.Errorf("not configure failedRecordFile in kafka")
		}

		sink, err := kafka.NewKafkaSink(cfg.Addrs, cfg.Topic)
		if err != nil {
			return err
		}
		d.recorder = &lumberjack.Logger{
			Filename: cfg.FailedRecordFile,
			MaxAge:   7,
		}
		d.out.AddSinks(sink)
		d.out.StartConsumer(sink.Name(), d)
	}
	return nil
}

func (d *DriveNode) initClusterConfig() error {
	dirInfo, err := d.lookup(context.TODO(), d.vol, volumeRootIno, "/usr/clusters.conf")
	if err != nil {
		log.Errorf("lookup /usr/clusters.conf error: %v", err)
		return err
	}
	inoInfo, err := d.vol.GetInode(context.TODO(), dirInfo.Inode)
	if err != nil {
		log.Errorf("get inode error: %v, ino=%d", err, dirInfo.Inode)
		return err
	}
	data := make([]byte, inoInfo.Size)
	n, err := d.vol.ReadFile(context.TODO(), dirInfo.Inode, 0, data)
	if err != nil {
		return err
	}
	cfg := ClusterConfig{}

	if err = json.Unmarshal(data[:n], &cfg); err != nil {
		log.Errorf("umarshal cluster config error: %v", err)
		return err
	}
	if len(cfg.Clusters) == 0 {
		return fmt.Errorf("cluster config is empty")
	}

	if cfg.RequestLimit > 0 && cfg.RequestLimit != int(d.limiter.Limit()) {
		d.limiter.SetLimit(rate.Limit(cfg.RequestLimit))
	}
	if cfg.LimiterBrust > 0 && d.limiter.Burst() != cfg.LimiterBrust {
		d.limiter.SetBurst(cfg.LimiterBrust)
	}

	var clusters []string
	for _, cluster := range cfg.Clusters {
		for i := 0; i < cluster.Priority; i++ {
			clusters = append(clusters, cluster.ClusterID)
		}
		if err = d.clusterMgr.AddCluster(context.TODO(), cluster.ClusterID, cluster.Master); err != nil {
			log.Errorf("add cluster %v error: %v", cluster, err)
			return err
		}
		log.Infof("get cluster %v", cluster)
	}
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(clusters), func(i, j int) {
		clusters[i], clusters[j] = clusters[j], clusters[i]
	})

	d.mu.Lock()
	d.clusters = clusters
	d.mu.Unlock()
	return nil
}

func (d *DriveNode) run() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-d.Closer.Done():
			return
		case <-ticker.C:
			d.initClusterConfig()
		}
	}
}
