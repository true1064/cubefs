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
)

type UserRoute struct {
	Uid         UserID `json:"uid"`
	ClusterType int8   `json:"clusterType"`
	ClusterID   string `json:"clusterId"`
	VolumeID    string `json:"volumeId"`
	DriveID     string `json:"driveId"`
	Capacity    uint64 `json:"capacity"`
	RootPath    string `json:"rootPath"`
	RootFileID  FileID `json:"rootFileId"`
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

type ClusterInfo struct {
	ClusterID string `json:"id"`
	Master    string `json:"master"`
	Priority  int    `json:"priority"`
}

type ClusterConfig struct {
	clusters []ClusterInfo `json:"clusters"`
}

type VolumeAlloc map[string]int

type ClusterAlloc map[string]VolumeAlloc

const (
	typeDir           = 1
	typeFile          = 2
	statusNormal      = 1
	statusLocked      = 2
	hashBucketNum     = 12
	hashUserRoute     = 5
	defaultCacheSize  = 1 << 17
	ReportIntervalSec = 600
	VolMaxUserCount   = 1024 * 2048 // A volume can be assigned to up to so many users
	attrKeyFilePaths  = "filePaths"
	attrKeyDirPaths   = "dirPaths"
	userConfigPath    = "/.user/config"
	volAllocPath      = "/user/alloc" // default cluster store each volume alloc numbers
)

var (
	ErrUidNotExist = errors.New("uid is not exist")
	ErrUidIsExist  = errors.New("uid is exist")
	ErrUidInvalid  = errors.New("uid is invalid")
	ErrNoCluster   = errors.New("no cluster available")
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
	ur = d.userRouter.Get(uid)
	if ur == nil {
		// query file and set cache
		ur, err, _ = d.groupRouter.Do(string(uid), func() (interface{}, error) {
			ur, err := d.getUserRouteFromFile(ctx, uid)
			if err != nil {
				return nil, err
			}
			d.userRouter.Set(uid, ur)
			return ur, nil
		})
	}
	return
}

func NewUserRouteMgr() (*userRouteMgr, error) {
	lruCache, err := memcache.NewMemCache(defaultCacheSize)
	if err != nil {
		return nil, err
	}
	m := &userRouteMgr{cache: lruCache, closeCh: make(chan struct{})}

	return m, nil
}

func (d *DriveNode) CreateUserRoute(ctx context.Context, uid UserID, capacity uint64) (err error) {
	// 2.Assign clusters and volumes to users
	clusterid, volumeid, err := d.assignVolume(ctx)
	if err != nil {
		return
	}
	// 3.Locate the user file of the default cluster according to the hash of uid
	rootPath, err := getRootPath(uid)
	if err != nil {
		return err
	}
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
	d.userRouter.Set(uid, ur)

	return
}

// There may be a problem of inaccurate count here. cfs does not support distributed file locking.
// There is only increment here, and it is not so accurate.
func (d *DriveNode) assignVolume(ctx context.Context) (clusterid, volumeid string, err error) {
	fileInfo, err := d.lookup(ctx, d.defaultVolume, 0, volAllocPath)
	if err != nil {
		return
	}
	clusters, err := d.defaultVolume.GetXAttr(ctx, fileInfo.Inode, "clusters")
	if err != nil {
		return
	}
	ca := make(map[string]VolumeAlloc)
	err = json.Unmarshal([]byte(clusters), &ca)
	if err != nil {
		return
	}
	if len(ca) == 0 {
		return "", "", ErrNoCluster
	}

ALLOC:
	for cluster, va := range ca {
		for vid, count := range va {
			if count < VolMaxUserCount {
				clusterid = cluster
				volumeid = vid
				count += 1
				ca[cluster][vid] = count
				b, err := json.Marshal(ca)
				if err != nil {
					return "", "", err
				}
				err = d.defaultVolume.SetXAttr(ctx, fileInfo.Inode, "clusters", string(b))
				if err != nil {
					return "", "", err
				}
			}
		}
	}
	var update bool
	if clusterid == "" {
		clusterVols := d.getClusterAndVolumes()
		for c, vols := range clusterVols {
			if _, ok := ca[c]; !ok {
				update = true
				va := make(VolumeAlloc)
				for _, v := range vols {
					va[v] = 0
				}
				ca[c] = va
			}
		}
		if update {
			goto ALLOC
		}
		if !update {
			return "", "", ErrNoCluster
		}
	}
	return
}

func (d *DriveNode) getClusterAndVolumes() map[string][]string {
	clusterVols := make(map[string][]string)
	clusters := d.clusterMgr.ListCluster()
	for _, c := range clusters {
		vols := d.clusterMgr.GetCluster(c.Cid).ListVols()
		volNames := make([]string, 0)
		for _, v := range vols {
			volNames = append(volNames, v.Name)
		}
		clusterVols[c.Cid] = volNames
	}
	return clusterVols
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
	ur := d.userRouter.Get(uid)
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
	cf, err := d.lookup(ctx, vol, ur.RootFileID, userConfigPath)
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
	Get(key UserID) *UserRoute
	Set(key UserID, value *UserRoute)
}

func (m *userRouteMgr) Get(key UserID) *UserRoute {
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

func (m *userRouteMgr) Set(key UserID, value *UserRoute) {
	m.cache.Set(key, value)
}
