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
	"strings"

	"github.com/cubefs/cubefs/blobstore/util/closer"

	"github.com/cubefs/cubefs/apinode/sdk"
)

const (
	headerRequestID = "x-cfa-request-id"
	headerUserID    = "x-cfa-user-id"
)

const (
	maxTaskPoolSize = 8
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
// filePath is an absolute path of client
func (d *DriveNode) getRootInoAndVolume(uid string) (uint64, sdk.IVolume, error) {
	userRouter, err := d.userRouter.Get(UserID(uid))
	if err != nil {
		return 0, nil, err
	}
	cluster := d.clusterMgr.GetCluster(userRouter.ClusterID)
	if cluster == nil {
		return 0, nil, sdk.ErrNotFound
	}
	volume := cluster.GetVol(userRouter.VolumeID)
	if volume == nil {
		return 0, nil, sdk.ErrNotFound
	}
	return uint64(userRouter.RootFileID), volume, nil
}

func (d *DriveNode) lookup(ctx context.Context, vol sdk.IVolume, parentIno uint64, path string) (info *sdk.DirInfo, err error) {
	names := strings.Split(path, "/")
	for _, name := range names {
		if name == "" {
			continue
		}
		info, err = vol.Lookup(ctx, parentIno, name)
		if err != nil {
			return
		}
		parentIno = info.Inode
	}
	return
}
