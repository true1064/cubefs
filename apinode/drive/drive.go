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
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/cubefs/cubefs/apinode/sdk"
	"github.com/cubefs/cubefs/apinode/sdk/impl"
	"github.com/cubefs/cubefs/blobstore/util/closer"
	"github.com/cubefs/cubefs/blobstore/util/log"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/config"
	"golang.org/x/sync/singleflight"
)

const (
	headerRange = "Range"

	headerRequestID = "x-cfa-request-id"
	headerUserID    = "x-cfa-user-id"
	headerCrc32     = "x-cfa-content-crc32"
	// headerSign      = "x-cfa-sign"

	userPropertyPrefix = "x-cfa-meta-"
)

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
//  It ends with a separator if path is directory.
type FilePath string

// Valid returns valid or not.
func (p *FilePath) Valid() bool {
	s := *p
	return s != "" && s[0] != '.'
}

// IsDir returns true if path is directory.
func (p *FilePath) IsDir() bool {
	s := *p
	return p.Valid() && s[len(s)-1] == os.PathSeparator
}

// IsFile returns true if path is file.
func (p *FilePath) IsFile() bool {
	s := *p
	return p.Valid() && s[len(s)-1] != os.PathSeparator
}

// Clean replace origin path to cleaned path.
func (p *FilePath) Clean() {
	s := string(*p)
	isDir := len(s) > 0 && s[len(s)-1] == os.PathSeparator
	path := filepath.Clean(s)
	if isDir {
		path += string(os.PathSeparator)
	}
	*p = FilePath(path)
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
	ID         uint64            `json:"id"`
	Name       string            `json:"name"`
	Type       string            `json:"type"`
	Size       int64             `json:"size"`
	Ctime      int64             `json:"ctime"`
	Mtime      int64             `json:"mtime"`
	Atime      int64             `json:"atime"`
	Properties map[string]string `json:"properties"`
}

func inode2file(ino *sdk.InodeInfo, name string, properties map[string]string) *FileInfo {
	typ := "file"
	if proto.IsDir(ino.Mode) {
		typ = "dir"
	}
	return &FileInfo{
		ID:         ino.Inode,
		Name:       name,
		Type:       typ,
		Size:       int64(ino.Size),
		Ctime:      ino.CreateTime.Unix(),
		Mtime:      ino.ModifyTime.Unix(),
		Atime:      ino.AccessTime.Unix(),
		Properties: properties,
	}
}

type SharedFileInfo struct {
	ID    uint64 `json:"id"`
	Path  string `json:"path"`
	Owner string `json:"owner"`
	Type  string `json:"type"`
	Size  int64  `json:"size"`
	Ctime int64  `json:"ctime"`
	Mtime int64  `json:"mtime"`
	Atime int64  `json:"atime"`
	Perm  string `json:"perm"` // only rd or rw
}

const (
	maxTaskPoolSize = 8
)

type ArgsListDir struct {
	Path   string `json:"path"`
	Owner  UserID `json:"owner,omitempty"`
	Marker string `json:"marker,omitempty"`
	Limit  int    `json:"limit"`
	Filter string `json:"filter,omitempty"`
}

type ArgsShare struct {
	Path string `json:"path"`
	Perm string `json:"perm"`
}

type ArgsUnShare struct {
	Path  string `json:"path"`
	Users string `json:"users,omitempty"`
}

type ArgsGetProperties struct {
	Path string `json:"path"`
}

type ArgsSetProperties struct {
	Path string `json:"path"`
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
	groupRouter *singleflight.Group
	stop        chan struct{}
	closer.Closer
}

// New returns a drive node.
func New() *DriveNode {
	cm := impl.NewClusterMgr()
	urm, err := NewUserRouteMgr()
	if err != nil {
		log.Fatal(err)
	}
	return &DriveNode{
		userRouter:  urm,
		clusterMgr:  cm,
		groupRouter: &singleflight.Group{},
		stop:        make(chan struct{}),
		Closer:      closer.New(),
	}
}

func (d *DriveNode) Start(cfg *config.Config) error {
	d.masterAddr = cfg.GetString("masterAddr")
	d.clusterID = cfg.GetString("clusterID")
	d.volumeName = cfg.GetString("volumeName")

	if err := d.clusterMgr.AddCluster(context.TODO(), d.clusterID, d.masterAddr); err != nil {
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

// get full path and volume by uid
// filePath is an absolute of client
func (d *DriveNode) getRootInoAndVolume(ctx context.Context, uid UserID) (Inode, sdk.IVolume, error) {
	userRouter, err := d.GetUserRouteInfo(ctx, uid)
	if err != nil {
		return 0, nil, err
	}
	cluster := d.clusterMgr.GetCluster(userRouter.ClusterID)
	if cluster == nil {
		return 0, nil, sdk.ErrNoCluster
	}
	volume := cluster.GetVol(userRouter.VolumeID)
	if volume == nil {
		return 0, nil, sdk.ErrNoVolume
	}
	return userRouter.RootFileID, volume, nil
}

func (d *DriveNode) lookup(ctx context.Context, vol sdk.IVolume, parentIno Inode, path string) (info *sdk.DirInfo, err error) {
	err = sdk.ErrBadRequest
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

func (d *DriveNode) createDir(ctx context.Context, vol sdk.IVolume, parentIno Inode, path string, recursive bool) (info *sdk.InodeInfo, err error) {
	var dirInfo *sdk.DirInfo
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
				err = sdk.ErrNotFound
				return
			}
			info, err = vol.Mkdir(ctx, parentIno.Uint64(), name)
			if err != nil {
				return nil, err
			}
			parentIno = Inode(info.Inode)
			continue
		}
		if !dirInfo.IsDir() {
			return nil, sdk.ErrConflict
		}
		parentIno = Inode(dirInfo.Inode)
	}
	if info == nil && err == nil {
		info, err = vol.GetInode(ctx, parentIno.Uint64())
	}
	return
}

func (d *DriveNode) createFile(ctx context.Context, vol sdk.IVolume, parentIno Inode, path string) (info *sdk.InodeInfo, err error) {
	dir, file := filepath.Split(filepath.Clean(path))
	if file == "" {
		err = sdk.ErrBadRequest
		return
	}
	if dir != "" && dir != "/" {
		info, err = d.createDir(ctx, vol, parentIno, dir, true)
		if err != nil {
			return
		}
		parentIno = Inode(info.Inode)
	}
	info, err = vol.CreateFile(ctx, parentIno.Uint64(), file)
	if err != nil {
		if err != sdk.ErrExist {
			return
		}
		var dirInfo *sdk.DirInfo
		dirInfo, err = vol.Lookup(ctx, parentIno.Uint64(), file)
		if err != nil {
			return
		}
		info, err = vol.GetInode(ctx, dirInfo.Inode)
	}
	return
}

func (d *DriveNode) initClusterConfig() error {
	dirInfo, err := d.lookup(context.TODO(), d.vol, volumeRootIno, "/usr/clusters.conf")
	if err != nil {
		return err
	}
	inoInfo, err := d.vol.GetInode(context.TODO(), dirInfo.Inode)
	if err != nil {
		log.Errorf("get inode error: %v, ino=%d", err, dirInfo.Inode)
		return err
	}
	data := make([]byte, inoInfo.Size)
	if _, err = d.vol.ReadFile(context.TODO(), dirInfo.Inode, 0, data); err != nil {
		return err
	}
	cfg := ClusterConfig{}

	if err = json.Unmarshal(data, &cfg); err != nil {
		log.Errorf("umarshal cluster config error: %v", err)
		return err
	}
	if len(cfg.Clusters) == 0 {
		return fmt.Errorf("cluster config is empty")
	}

	clusters := make([]string, 0, len(cfg.Clusters))
	for _, cluster := range cfg.Clusters {
		for i := 0; i < cluster.Priority; i++ {
			clusters = append(clusters, cluster.ClusterID)
		}
		if cluster.ClusterID == d.clusterID {
			continue
		}
		if err = d.clusterMgr.AddCluster(context.TODO(), cluster.ClusterID, cluster.Master); err != nil {
			return err
		}
	}
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(clusters), func(i, j int) {
		clusters[i], clusters[j] = clusters[j], clusters[i]
	})

	d.mu.Lock()
	defer d.mu.Unlock()
	d.clusters = clusters
	return nil
}

func (d *DriveNode) run() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-d.stop:
			return
		case <-ticker.C:
			d.initClusterConfig()
		}
	}
}
