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
	"net/http"
	"path"

	"github.com/cubefs/cubefs/blobstore/common/rpc"
)

type ArgsMkDir struct {
	Path      string `json:"path"`
	Type      string `json:"type"`
	Recursive bool   `json:"recursive"`
}

type ArgsRename struct {
	Src  string `json:"src"`
	Dest string `json:"dest"`
}

// POST /v1/files?path=/abc&type=folder&recursive=bool
func (d *DriveNode) mkDir(c *rpc.Context) {
	ctx, _ := d.ctxSpan(c)
	args := new(ArgsMkDir)
	if err := c.ParseArgs(args); err != nil {
		c.RespondStatus(http.StatusBadRequest)
		return
	}

	uid := d.userID(c)
	inode, vol, err := d.getRootInoAndVolume(ctx, uid)
	if err != nil {
		c.RespondError(err)
		return
	}

	_, err = vol.Mkdir(ctx, inode.Uint64(), args.Path)
	if err != nil {
		c.RespondError(err)
		return
	}
}

// POST /v1/files/rename?src=/abc/hello/file1.json&dst=/abc/file1.json
func (d *DriveNode) rename(c *rpc.Context) {
	ctx, _ := d.ctxSpan(c)
	args := new(ArgsRename)
	if err := c.ParseArgs(args); err != nil {
		c.RespondStatus(http.StatusBadRequest)
		return
	}

	uid := d.userID(c)

	ur, err := d.GetUserRoute(ctx, uid)
	if err != nil {
		c.RespondError(err)
		return
	}

	vol := d.clusterMgr.GetCluster(ur.ClusterID).GetVol(ur.VolumeID)
	srcDir := path.Dir(path.Join(ur.RootPath, args.Src))
	srcParInfo, err := d.lookup(ctx, vol, 0, srcDir)
	if err != nil {
		c.RespondError(err)
		return
	}
	destDir := path.Dir(path.Join(ur.RootPath, args.Dest))
	destParInfo, err := d.lookup(ctx, vol, 0, destDir)
	if err != nil {
		c.RespondError(err)
		return
	}

	err = vol.Rename(ctx, srcParInfo.Inode, destParInfo.Inode, path.Base(args.Src), path.Base(args.Dest))
	if err != nil {
		c.RespondError(err)
		return
	}
}
