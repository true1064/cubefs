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
	"encoding/json"
	"fmt"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/util/log"
)

type UserRoute struct {
	Uid         UserID `json:"uid"`
	ClusterType int8   `json:"clusterType"`
	ClusterID   int    `json:"clusterId"`
	VolumeID    int    `json:"volumeId"`
	DriveID     int    `json:"driveId"`
	Capacity    uint64 `json:"capacity"`
	RootPath    string `json:"rootPath"`
	RootFileID  int64  `json:"rootFileId"`
	Ctime       int64  `json:"ctime"`
	Params      string `json:"params"` //cfs
}

type ConfigEntry struct {
	Path   string `json:"path"`
	Type   int8   `json:"type"`
	Status int8   `json:"status"`
	Mtime  int64  `json:"mtime"`
}

type ArgsPath struct {
	Path string `json:"path"`
	Type int8   `json:"type"`
}

type UserConfig struct {
	Uid      UserID        `json:"uid"`
	AppPaths []ConfigEntry `json:"appPaths"` //cloud path
}

const hashBucketNum = 10

type userRouteMgr struct {
	//TODO user route info store in lru cache
}

type IUserRoute interface {
	Get(uid int) (ur UserRoute, err error)
}

func (m *userRouteMgr) Get(uid int) (ur UserRoute, err error) {
	return
}

// createDrive handle drive apis.
func (d *DriveNode) createDrive(c *rpc.Context) {
	rid := d.requestID(c)
	uid := d.userID(c)
	err := d.CreateUserRoute(uid)
	if err != nil {
		c.RespondError(err)
		return
	}
	log.LogInfo("got", rid, uid)
	c.Respond()
}

func (d *DriveNode) addUserConfig(c *rpc.Context) {
	args := new(ArgsPath)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	rid := d.requestID(c)
	uid := d.userID(c)
	err := d.AddPath(uid, args)
	if err != nil {
		c.RespondError(err)
		return
	}
	log.LogInfo("got", rid, uid)
	c.Respond()
}

func (d *DriveNode) getUserConfig(c *rpc.Context) {
	rid := d.requestID(c)
	uid := d.userID(c)
	log.LogInfo("got", rid, uid)
	c.Respond()
}

func (d *DriveNode) CreateUserRoute(uid UserID) (err error) {
	//1.Authenticate the token and get the uid

	//2.Applying for space to the cloud service
	capacity := uint64(100)

	//3.Apply to cfs for cluster and volume information
	clusterid, volumeid := 1, 101
	hashNum := uid % 5
	rootPath := fmt.Sprintf("/%d/%d", hashNum, uid)
	fmt.Printf("rootPath:%s", rootPath)

	//4.Locate the user file of the default cluster according to the hash of uid
	l1, l2 := hash(int(uid))
	userRouteFile := fmt.Sprintf("/user/clusters/%d/%d", l1, l2)
	fmt.Println(userRouteFile)
	us := UserRoute{
		Uid:       uid,
		ClusterID: clusterid,
		VolumeID:  volumeid,
		Capacity:  capacity,
		RootPath:  rootPath,
	}
	fmt.Println(us)
	//5.todo:Write mappings to extended attributes

	//6.update cache

	return
}

func (d *DriveNode) AddPath(uid UserID, args *ArgsPath) (err error) {
	//1.Authenticate the token and get the uid

	//2.Get clusterid, volumeid from default cluster
	l1, l2 := hash(int(uid))
	userRouteFile := fmt.Sprintf("/user/clusters/%d/%d", l1, l2)
	userRoute, err := getUserRoute(userRouteFile)
	if err != nil {
		log.LogError(err)
		return
	}
	configFile := fmt.Sprintf("/%s/.user/config", userRoute.RootPath)
	//3.Store user cloud directory
	uc, err := d.userRouteMgr.read(configFile)
	if err != nil {
		log.LogError(err)
		return
	}
	pi := ConfigEntry{args.Path, args.Type, 1, time.Now().Unix()}
	uc.AppPaths = append(uc.AppPaths, pi)
	err = d.userRouteMgr.Write(uc, configFile)
	if err != nil {
		log.LogError(err)
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

func getUserRoute(path string) (us UserRoute, err error) {
	//todo: sdk read default cluster user info
	us = UserRoute{}
	return
}

func (m *userRouteMgr) read(path string) (uc UserConfig, err error) {
	var bytesData []byte
	//todo: sdk read file
	if bytesData == nil {
		return
	}
	err = json.Unmarshal(bytesData, uc)
	if err != nil {
		log.LogError("json unmarshal error")
	}
	return
}

func (m *userRouteMgr) Write(uc UserConfig, path string) (err error) {
	bytesData, err := json.Marshal(uc)
	if err != nil {
		log.LogError("json marshal error")
	}
	//todo: sdk write file
	log.LogInfo(bytesData)
	return
}
