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
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/memcache"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

type UserRoute struct {
	Uid         UserID `json:"uid"`
	ClusterType int8   `json:"clusterType"`
	ClusterID   string `json:"clusterId"`
	VolumeID    string `json:"volumeId"`
	DriveID     int    `json:"driveId"`
	Capacity    uint64 `json:"capacity"`
	RootPath    string `json:"rootPath"`
	RootFileID  int64  `json:"rootFileId"`
	Ctime       int64  `json:"ctime"`
	Params      string `json:"params"` // cfs
}

type ConfigEntry struct {
	Path   string `json:"path"`
	Status int8   `json:"status"`
	Mtime  int64  `json:"mtime"`
}

type UserConfig struct {
	Uid       UserID                 `json:"uid"`
	DirPaths  map[string]ConfigEntry `json:"dirPaths"`  // cloud dir paths
	FilePaths map[string]ConfigEntry `json:"filePaths"` // cloud file paths
}

const (
	typeDir           = 1
	typeFile          = 2
	statusNormal      = 1
	statusLocked      = 2
	hashBucketNum     = 12
	hashUserRoute     = 5
	defaultCacheSize  = 1 << 17
	ReportIntervalSec = 600
	attrKeyFilePaths  = "filePaths"
	attrKeyDirPaths   = "dirPaths"
	userConfigPath    = "/.user/config"
)

var (
	ErrUidNotExist = errors.New("uid is not exist")
	ErrUidIsExist  = errors.New("uid is exist")
	ErrUidInvalid  = errors.New("uid is invalid")
)

type userRouteMgr struct {
	cache   *memcache.MemCache
	closeCh chan struct{}
}

type IUserRoute interface {
	GetUserRoute(ctx context.Context, uid UserID) (ur *UserRoute, err error)
	CreateUserRoute(ctx context.Context, uid UserID) (err error)
	AddPath(ctx context.Context, uid UserID, args *ArgsPath) (err error)
}

func (d *DriveNode) GetUserRoute(ctx context.Context, uid UserID) (ur *UserRoute, err error) {
	ur = d.userRouter.CacheGet(uid)
	if ur == nil {
		// query file and set cache
		ur, err = d.getUserRouteFromFile(ctx, uid)
		if err != nil {
			return
		}
		d.userRouter.CacheSet(uid, ur)
		return
	}
	return
}

func NewUserRouteMgr() (*userRouteMgr, error) {
	lruCache, err := memcache.NewMemCache(context.Background(), defaultCacheSize)
	if err != nil {
		return nil, err
	}
	m := &userRouteMgr{cache: lruCache, closeCh: make(chan struct{})}

	go m.loopReportCapacityToCloud()

	return m, nil
}

func (m *userRouteMgr) loopReportCapacityToCloud() {
	log.Infof("loop report capacity to cloud service")

	ticker := time.NewTicker(time.Duration(ReportIntervalSec) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-m.closeCh:
			log.Warnf("loop report capacity to cloud service done.")
			return
		case <-ticker.C:
			// todo: report to cloud service, need cloudKit sdk
		}
	}
}

func (d *DriveNode) CreateUserRoute(ctx context.Context, uid UserID) (err error) {
	// 1.todo: Applying space from the cloud service, need cloudKit sdk
	capacity := uint64(100)

	// 2.todo: Apply to cfs for cluster and volume information
	clusterid, volumeid := "1", "101"
	rootPath, err := getRootPath(uid)
	if err != nil {
		return err
	}

	// 3.Locate the user file of the default cluster according to the hash of uid
	ur := &UserRoute{
		Uid:       uid,
		ClusterID: clusterid,
		VolumeID:  volumeid,
		Capacity:  capacity,
		RootPath:  rootPath,
		Ctime:     time.Now().Unix(),
	}

	// 4.Write mappings to extended attributes
	err = d.setUserRouteToFile(ctx, uid, ur)
	if err != nil {
		return
	}
	// 5.update cache
	d.userRouter.CacheSet(uid, ur)

	return
}

func (d *DriveNode) setUserRouteToFile(ctx context.Context, uid UserID, ur *UserRoute) (err error) {
	inode, users, err := d.getUserMapFromFile(ctx, uid)
	if err != nil {
		return
	}
	if _, ok := users[string(uid)]; ok {
		return ErrUidIsExist
	}
	b, _ := json.Marshal(ur)
	users[string(uid)] = string(b)
	newUsers, err := mapToJson(users)
	if err != nil {
		return
	}
	err = d.defaultVolume.SetXAttr(ctx, inode, XAttrUserKey, newUsers)
	if err != nil {
		return
	}
	return
}

func (d *DriveNode) getUserRouteFromFile(ctx context.Context, uid UserID) (ur *UserRoute, err error) {
	_, users, err := d.getUserMapFromFile(ctx, uid)
	if err != nil {
		return
	}
	if _, ok := users[string(uid)]; !ok {
		return nil, ErrUidNotExist
	}
	err = json.Unmarshal([]byte(users[string(uid)]), ur)
	if err != nil {
		return nil, err
	}
	return
}

func (d *DriveNode) getUserMapFromFile(ctx context.Context, uid UserID) (inode uint64, users map[string]string, err error) {
	userRouteFile, err := getUserRouteFile(uid)
	if err != nil {
		return
	}
	dirInfo, err := d.lookup(ctx, d.defaultVolume, 0, userRouteFile)
	if err != nil {
		return
	}
	data, err := d.defaultVolume.GetXAttr(ctx, dirInfo.Inode, XAttrUserKey)
	if err != nil {
		return
	}
	users, err = jsonToMap(data)
	if err != nil {
		return
	}
	return dirInfo.Inode, users, nil
}

func jsonToMap(s string) (map[string]string, error) {
	m := make(map[string]string)
	err := json.Unmarshal([]byte(s), &m)
	if err != nil {
		return nil, err
	}
	return m, nil
}

func mapToJson(m map[string]string) (string, error) {
	b, err := json.Marshal(m)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func getUserRouteFile(uid UserID) (string, error) {
	l1, l2 := hash(string(uid))
	userRouteFile := fmt.Sprintf("/user/clusters/%d/%d", l1, l2)
	return userRouteFile, nil
}

func getRootPath(uid UserID) (string, error) {
	iUid, err := strconv.Atoi(string(uid))
	if err != nil {
		return "", ErrUidInvalid
	}
	hashNum := iUid % hashUserRoute
	rootPath := fmt.Sprintf("/%d/%s", hashNum, uid)
	return rootPath, nil
}

func (d *DriveNode) AddPath(ctx context.Context, uid UserID, args *ArgsPath) (err error) {
	// 1.Get clusterid, volumeid from default cluster
	ur := d.userRouter.CacheGet(uid)
	if ur == nil {
		ur, err = d.getUserRouteFromFile(ctx, uid)
		if err != nil {
			return
		}
	}
	// 2.add new path to user's config file attribute
	switch args.Type {
	case typeDir:
		return d.writePath(ctx, args.Path, attrKeyDirPaths, ur)
	case typeFile:
		return d.writePath(ctx, args.Path, attrKeyFilePaths, ur)
	}
	return
}

func (d *DriveNode) writePath(ctx context.Context, path, attrKey string, ur *UserRoute) (err error) {
	vol := d.clusterMgr.GetCluster(ur.ClusterID).GetVol(ur.VolumeID)
	cf, err := d.lookup(ctx, vol, uint64(ur.RootFileID), userConfigPath)
	if err != nil {
		return
	}
	configEntry := ConfigEntry{path, statusNormal, time.Now().Unix()}
	dataMaps := make(map[string]ConfigEntry)
	data, err := vol.GetXAttr(ctx, cf.Inode, attrKey)
	if err != nil {
		return
	}
	err = json.Unmarshal([]byte(data), &dataMaps)
	if err != nil {
		return
	}
	dataMaps[path] = configEntry
	bytesData, err := json.Marshal(dataMaps)
	if err != nil {
		return
	}
	return vol.SetXAttr(ctx, cf.Inode, attrKey, string(bytesData))
}

func hash(s string) (l1, l2 int) {
	hasher := md5.New()
	hasher.Write([]byte(s))
	md5Hash := hex.EncodeToString(hasher.Sum(nil))

	// Hash the MD5 hash and get the file number
	h := 0
	for i := 0; i < len(md5Hash); i++ {
		h = 3*h + int(md5Hash[i])
	}
	preNum := h % (hashBucketNum * hashBucketNum)
	l1 = preNum / hashBucketNum
	l2 = preNum % hashBucketNum
	return l1, l2
}

// IUserCache
type IUserCache interface {
	CacheGet(key UserID) *UserRoute
	CacheSet(key UserID, value *UserRoute)
}

func (m *userRouteMgr) CacheGet(key UserID) *UserRoute {
	value := m.cache.Get(key)
	if value == nil {
		return nil
	}
	ur, ok := value.(UserRoute)
	if !ok {
		return nil
	}

	return &ur
}

func (m *userRouteMgr) CacheSet(key UserID, value *UserRoute) {
	m.cache.Set(key, value)
}
