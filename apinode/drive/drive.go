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
	"path"

	"github.com/cubefs/cubefs/blobstore/util/closer"

	"github.com/cubefs/cubefs/apinode/sdk"
)

const (
	headerRequestID = "x-cfa-request-id"
	headerUserID    = "x-cfa-user-id"
)

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

type UserID string

type ArgsListDir struct {
	Path   string `json:"path"`
	Type   string `json:"type"`
	Owner  string `json:"owner,omitempty"`
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

// DriveNode drive node.
type DriveNode struct {
	userRouter IUserRoute
	clusterMgr sdk.ClusterManager

	closer.Closer
}

// New returns a drive node.
func New() *DriveNode {
	return &DriveNode{
		Closer: closer.New(),
	}
}

// get full path and volume by uid
// filePath is an absolute of client
func (d *DriveNode) getFilePathAndVolume(filePath string, uid string) (string, sdk.Volume, error) {
	userRouter, err := d.userRouter.Get(UserID(uid))
	if err != nil {
		return "", nil, err
	}
	cluster := d.clusterMgr.GetCluster(userRouter.ClusterID)
	if cluster == nil {
		return "", nil, sdk.ErrNotFound
	}
	volume := cluster.GetVol(userRouter.VolumeID)
	if volume == nil {
		return "", nil, sdk.ErrNotFound
	}
	filePath = path.Join(userRouter.RootPath, filePath)
	return filePath, volume, nil
}
