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
	Type   int8   `json:"type"`
	Status int8   `json:"status"`
	Mtime  int64  `json:"mtime"`
}

type UserConfig struct {
	Uid      UserID        `json:"uid"`
	AppPaths []ConfigEntry `json:"appPaths"` // cloud path
}

const (
	statusNormal     = 1
	statusLocked     = 2
	hashBucketNum    = 10
	defaultCacheSize = 1 << 17
)

var (
	ErrUidNotExist    = errors.New("uid: not exist")
	ErrUidInvalid     = errors.New("uid is invalid")
	ErrParseUserRoute = errors.New("parse user route error")
)

type userRouteMgr struct {
	userCache IUserCache
}

type IUserRoute interface {
	Get(uid UserID) (ur *UserRoute, err error)
	Create(uid UserID) (err error)
	AddPath(uid UserID, args *ArgsPath) (err error)
}

func (m *userRouteMgr) Get(uid UserID) (ur *UserRoute, err error) {
	ur = m.userCache.Get(uid)
	if ur == nil {
		// todo: query file and set cache
		return nil, ErrUidNotExist
	}
	return ur, nil
}

func NewUserRouteMgr() (m IUserRoute, err error) {
	lruCache, err := memcache.NewMemCache(context.Background(), defaultCacheSize)
	if err != nil {
		return nil, err
	}
	m = &userRouteMgr{userCache: &userCache{cache: lruCache}}
	return
}

func (m *userRouteMgr) Create(uid UserID) (err error) {
	// 1.todo: Applying for space to the cloud service
	capacity := uint64(100)
	// 2.todo: Apply to cfs for cluster and volume information
	clusterid, volumeid := "1", "101"
	iUid, err := strconv.Atoi(string(uid))
	if err != nil {
		return ErrUidInvalid
	}
	hashNum := iUid % 5
	rootPath := fmt.Sprintf("/%d/%s", hashNum, uid)
	fmt.Printf("rootPath:%s", rootPath)

	// 3.Locate the user file of the default cluster according to the hash of uid
	l1, l2 := hash(iUid)
	userRouteFile := fmt.Sprintf("/user/clusters/%d/%d", l1, l2)
	fmt.Println(userRouteFile)
	us := UserRoute{
		Uid:       uid,
		ClusterID: clusterid,
		VolumeID:  volumeid,
		Capacity:  capacity,
		RootPath:  rootPath,
		Ctime:     time.Now().Unix(),
	}
	fmt.Println(us)
	// 4.todo:Write mappings to extended attributes

	// 5.update cache
	m.userCache.Set(uid, us)

	return
}

func (m *userRouteMgr) AddPath(uid UserID, args *ArgsPath) (err error) {
	// 1.Get clusterid, volumeid from default cluster
	userRoute := m.userCache.Get(uid)

	if userRoute == nil {
		iUid, err := strconv.Atoi(string(uid))
		if err != nil {
			return ErrUidInvalid
		}
		l1, l2 := hash(iUid)
		userRouteFile := fmt.Sprintf("/user/clusters/%d/%d", l1, l2)
		userRoute, err = getUserRoute(userRouteFile)
		if err != nil {
			return err
		}
	}

	configFile := fmt.Sprintf("/%s/.user/config", userRoute.RootPath)
	// 2.Store user cloud directory
	uc, err := m.read(configFile)
	if err != nil {
		log.Error(err)
		return
	}
	pi := ConfigEntry{args.Path, args.Type, statusNormal, time.Now().Unix()}
	uc.AppPaths = append(uc.AppPaths, pi)
	err = m.Write(uc, configFile)
	if err != nil {
		log.Error(err)
		return
	}
	return
}

func hash(num int) (l1, l2 int) {
	preNum := num % (hashBucketNum * hashBucketNum)
	l1 = preNum / hashBucketNum
	l2 = preNum % hashBucketNum
	return l1, l2
}

func getUserRoute(path string) (us *UserRoute, err error) {
	// todo: sdk read default cluster user info
	us = &UserRoute{}
	return
}

func (m *userRouteMgr) read(path string) (uc UserConfig, err error) {
	var bytesData []byte
	// todo: sdk read file
	if bytesData == nil {
		return
	}
	err = json.Unmarshal(bytesData, &uc)
	if err != nil {
		log.Error("json unmarshal error")
	}
	return
}

func (m *userRouteMgr) Write(uc UserConfig, path string) (err error) {
	bytesData, err := json.Marshal(uc)
	if err != nil {
		log.Error("json marshal error")
	}

	// todo: sdk write file
	log.Info(bytesData)
	return
}

// IUserCache
type IUserCache interface {
	Get(key UserID) *UserRoute
	Set(key UserID, value UserRoute)
}

type userCache struct {
	cache *memcache.MemCache
}

func (uc *userCache) Get(key UserID) *UserRoute {
	value := uc.cache.Get(key)
	if value == nil {
		return nil
	}
	ur, ok := value.(UserRoute)
	if !ok {
		return nil
	}

	return &ur
}

func (uc *userCache) Set(key UserID, value UserRoute) {
	uc.cache.Set(key, value)
}
