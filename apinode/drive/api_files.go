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
	ctx, span := d.ctxSpan(c)
	args := new(ArgsMkDir)
	if err := c.ParseArgs(args); err != nil {
		c.RespondStatus(http.StatusBadRequest)
		return
	}

	uid := string(d.userID(c))
	if uid == "" {
		c.RespondStatus(http.StatusBadRequest)
		return
	}

	inode, vol, err := d.getRootInoAndVolume(ctx, uid)
	if err != nil {
		span.Errorf("Failed to get volume: %v", err)
		c.RespondError(err)
		return
	}

	_, err = vol.Mkdir(ctx, inode, args.Path)
	if err != nil {
		c.RespondError(err)
		return
	}
}

// POST /v1/files/rename?src=/abc&dst=/hello
func (d *DriveNode) rename(c *rpc.Context) {
	ctx, span := d.ctxSpan(c)
	args := new(ArgsRename)
	if err := c.ParseArgs(args); err != nil {
		c.RespondStatus(http.StatusBadRequest)
		return
	}

	uid := string(d.userID(c))
	if uid == "" {
		c.RespondStatus(http.StatusBadRequest)
		return
	}

	inode, vol, err := d.getRootInoAndVolume(ctx, uid)
	if err != nil {
		span.Errorf("Failed to get volume: %v", err)
		c.RespondError(err)
		return
	}

	userRouter, err := d.GetUserRoute(ctx, UserID(uid))
	if err != nil {
		c.RespondError(err)
		return
	}
	// todo: need modify
	srcPath := path.Join(userRouter.RootPath, args.Src)
	destPath := path.Join(userRouter.RootPath, args.Dest)
	err = vol.Rename(ctx, inode, inode, srcPath, destPath)
	if err != nil {
		c.RespondError(err)
		return
	}
}
