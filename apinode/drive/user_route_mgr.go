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
	"crypto/md5"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"math/rand"
	"path/filepath"
	"time"

	"github.com/cubefs/cubefs/apinode/sdk"
	"github.com/cubefs/cubefs/blobstore/common/memcache"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/google/uuid"
)

type UserRoute struct {
	Uid         UserID `json:"uid"`
	ClusterType int8   `json:"clusterType"`
	ClusterID   string `json:"clusterId"`
	VolumeID    string `json:"volumeId"`
	DriveID     string `json:"driveId"`
	RootPath    string `json:"rootPath"`
	RootFileID  FileID `json:"rootFileId"`
	CipherKey   []byte `json:"cipherKey"`
	ReadOnly    bool   `json:"readonly"`
	Ctime       int64  `json:"ctime"`
	Params      string `json:"params"` // cfs
}

func (ur *UserRoute) Marshal() ([]byte, error) {
	return json.Marshal(ur)
}

func (ur *UserRoute) Unmarshal(data []byte) error {
	return json.Unmarshal(data, &ur)
}

func (ur *UserRoute) CanWrite() error {
	if ur == nil {
		return nil
	}
	if ur.ReadOnly {
		return sdk.ErrReadOnly
	}
	return nil
}

type ClusterInfo struct {
	ClusterID string `json:"id"`
	Master    string `json:"master"`
	Priority  int    `json:"priority"`
}

type ClusterConfig struct {
	Clusters     []ClusterInfo `json:"clusters"`
	RequestLimit int           `json:"requestLimit"` // the number of request per second
	LimiterBrust int           `json:"limiterBrust"` // the size of token bucket
}

const (
	volumeRootIno    = 1
	hashMask         = 1024
	defaultCacheSize = 1 << 17
	volumeConfigPath = "/config/apps.cfg"
)

type userRouteMgr struct {
	cache   *memcache.MemCache
	closeCh chan struct{}
}

func NewUserRouteMgr() (*userRouteMgr, error) {
	lruCache, err := memcache.NewMemCache(defaultCacheSize)
	if err != nil {
		return nil, err
	}
	m := &userRouteMgr{cache: lruCache, closeCh: make(chan struct{})}

	return m, nil
}

func (d *DriveNode) createUserRoute(ctx context.Context, uid UserID) error {
	// Assign clusters and volumes
	cluster, clusterid, volumeid, err := d.assignVolume(ctx, uid)
	if err != nil {
		return err
	}
	vol := cluster.GetVol(volumeid)
	if vol == nil {
		return sdk.ErrNoVolume
	}
	vol, err = vol.GetDirSnapshot(ctx, 1)
	if err != nil {
		return err
	}

	rootPath := getRootPath(uid)
	// create user root path
	ino, _, err := d.createDir(ctx, vol, volumeRootIno, rootPath, true)
	if err != nil {
		return err
	}
	cipherKey, err := d.cryptor.GenKey()
	if err != nil {
		return err
	}

	// Locate the user file of the default cluster according to the hash of uid
	ur := &UserRoute{
		Uid:         uid,
		ClusterType: 1,
		ClusterID:   clusterid,
		VolumeID:    volumeid,
		DriveID:     uuid.New().String(),
		RootPath:    rootPath,
		RootFileID:  ino,
		CipherKey:   cipherKey,
		ReadOnly:    false,
		Ctime:       time.Now().Unix(),
	}
	// 4.Write mappings to extended attributes
	return d.setUserRouteToFile(ctx, uid, ur)
}

// There may be a problem of inaccurate count here. cfs does not support distributed file locking.
// There is only increment here, and it is not so accurate.
func (d *DriveNode) assignVolume(ctx context.Context, uid UserID) (cluster sdk.ICluster, clusterid, volumeid string, err error) {
	span := trace.SpanFromContextSafe(ctx)
	d.mu.RLock()
	if len(d.clusters) == 0 {
		d.mu.RUnlock()
		err = sdk.ErrNoCluster
		return
	}
	idx := rand.Int31n(int32(len(d.clusters)))
	clusterid = d.clusters[idx]
	d.mu.RUnlock()
	cluster = d.clusterMgr.GetCluster(clusterid)
	if cluster == nil {
		err = sdk.ErrNoCluster
		return
	}
	vols := cluster.ListVols()
	if len(vols) == 0 {
		err = sdk.ErrNoVolume
		return
	}

	data := md5.Sum([]byte(uid))
	val := crc32.ChecksumIEEE(data[0:])
	volumeid = vols[int(val)%len(vols)].Name
	span.Infof("assign cluster=%s volume=%s for user=%s", clusterid, volumeid, string(uid))
	return
}

func (d *DriveNode) setUserRouteToFile(ctx context.Context, uid UserID, ur *UserRoute) error {
	file := getUserRouteFile(uid)
	fileIno := uint64(0)
	dir, name := filepath.Split(file)
	ino, _, err := d.createDir(ctx, d.vol, volumeRootIno, dir, true)
	if err != nil {
		return err
	}
	inoInfo, _, err := d.vol.CreateFile(ctx, ino.Uint64(), name)
	if err != nil {
		if err != sdk.ErrExist {
			return err
		}

		var dirInfo *sdk.DirInfo
		dirInfo, err = d.vol.Lookup(ctx, ino.Uint64(), name)
		if err != nil {
			return err
		}
		fileIno = uint64(dirInfo.Inode)

		var val string
		val, err = d.vol.GetXAttr(ctx, fileIno, string(ur.Uid))
		if err != nil {
			return err
		}
		if len(val) > 0 {
			return json.Unmarshal([]byte(val), ur)
		}
	} else {
		fileIno = inoInfo.Inode
	}

	val, err := ur.Marshal()
	if err != nil {
		return err
	}

	err = d.vol.SetXAttrNX(ctx, fileIno, string(ur.Uid), string(val))
	if err == sdk.ErrExist {
		return nil
	}
	return err
}

func (d *DriveNode) getOrCreateUserRoute(ctx context.Context, uid UserID) (*UserRoute, error) {
	ur, err := d.getUserRouteFromFile(ctx, uid)
	if err == sdk.ErrNoUser {
		span := trace.SpanFromContextSafe(ctx)
		span.Info("to create new user:", uid)
		if err = d.createUserRoute(ctx, uid); err != nil {
			span.Error("create new user:", uid, err.Error())
			return nil, err
		}
		ur, err = d.getUserRouteFromFile(ctx, uid)
	}
	return ur, err
}

func (d *DriveNode) getUserRouteFromFile(ctx context.Context, uid UserID) (*UserRoute, error) {
	userRouteFile := getUserRouteFile(uid)
	dirInfo, err := d.lookup(ctx, d.vol, volumeRootIno, userRouteFile)
	if err != nil {
		if err == sdk.ErrNotFound {
			return nil, sdk.ErrNoUser
		}
		return nil, err
	}
	data, err := d.vol.GetXAttr(ctx, dirInfo.Inode, string(uid))
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, sdk.ErrNoUser
	}
	ur := &UserRoute{}
	if err = ur.Unmarshal([]byte(data)); err != nil {
		return nil, sdk.ErrInternalServerError.Extend(err)
	}
	return ur, nil
}

func getUserRouteFile(uid UserID) string {
	h1, h2 := hashUid(uid)
	return fmt.Sprintf("/usr/clusters/%d/%d", h1%hashMask, h2%hashMask)
}

func getRootPath(uid UserID) string {
	h1, h2 := hashUid(uid)
	return fmt.Sprintf("/%d/%d/%s", h1%hashMask, h2%hashMask, string(uid))
}

func hashUid(uid UserID) (h1, h2 uint32) {
	data := md5.Sum([]byte(uid))
	h1 = crc32.ChecksumIEEE(data[:])

	s := fmt.Sprintf("%d", h1)
	data = md5.Sum([]byte(s))
	h2 = crc32.ChecksumIEEE(data[:])
	return h1, h2
}

func (m *userRouteMgr) Get(key UserID) *UserRoute {
	value := m.cache.Get(key)
	if value == nil {
		return nil
	}
	return value.(*UserRoute)
}

func (m *userRouteMgr) Set(key UserID, value *UserRoute) {
	m.cache.Set(key, value)
}

func (m *userRouteMgr) Remove(key UserID) {
	m.cache.Remove(key)
}
