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

	"github.com/cubefs/cubefs/blobstore/common/rpc"
)

type ArgsGetDrive struct {
	Uid string `json:"uid"`
}

type ArgsUpdateDrive struct {
	Uid        string `json:"uid"`
	RootPath   string `json:"rootPath,omitempty"`
	RootFileID uint64 `json:"rootFileId,omitempty"`
	OnlyCache  bool   `json:"onlyCache,omitempty"`
}

type CreateDriveResult struct {
	DriveID string `json:"driveId"`
}

type AppPathInfo struct {
	Path   string `json:"path"`
	Status int    `json:"status"`
	MTime  int64  `json:"mtime"`
}

// createDrive handle drive apis.
func (d *DriveNode) handleCreateDrive(c *rpc.Context) {
	ctx, span := d.ctxSpan(c)
	uid := d.userID(c)
	driveid, err := d.CreateUserRoute(ctx, uid)
	if err != nil {
		span.Errorf("crate user route error: %v, uid=%s", err, string(uid))
		d.respError(c, err)
		return
	}
	d.respData(c, CreateDriveResult{driveid})
}

func (d *DriveNode) handleGetDrive(c *rpc.Context) {
	ctx, span := d.ctxSpan(c)
	uid := d.userID(c)
	args := new(ArgsGetDrive)
	if d.checkError(c, nil, c.ParseArgs(args)) {
		return
	}
	if d.admin == "" || d.admin != string(uid) {
		c.RespondStatus(http.StatusForbidden)
		return
	}
	ur, err := d.GetUserRouteInfo(ctx, UserID(args.Uid))
	if err != nil {
		span.Errorf("get user route error: %v, uid=%s", err, string(args.Uid))
		d.respError(c, err)
		return
	}
	d.respData(c, ur)
}

func (d *DriveNode) handleUpdateDrive(c *rpc.Context) {
	ctx, span := d.ctxSpan(c)
	uid := d.userID(c)

	args := new(ArgsUpdateDrive)
	if d.checkError(c, nil, c.ParseArgs(args)) {
		return
	}

	if d.admin == "" || string(uid) != d.admin {
		c.RespondStatus(http.StatusForbidden)
		return
	}

	if args.RootFileID == 0 && args.RootPath == "" {
		return
	}

	oldUr, err := d.GetUserRouteInfo(ctx, UserID(args.Uid))
	if err != nil {
		span.Errorf("get user route error: %v, uid=%s", err, args.Uid)
		d.respError(c, err)
		return
	}

	newUr := &UserRoute{}
	*newUr = *oldUr
	if args.RootFileID != 0 {
		newUr.RootFileID = Inode(args.RootFileID)
	}

	if args.RootPath != "" {
		newUr.RootPath = args.RootPath
	}

	if !args.OnlyCache {
		if d.checkError(c, func(err error) { span.Errorf("update user route error: %v", err) }, d.updateUserRoute(ctx, UserID(args.Uid), newUr)) {
			return
		}
	}
	d.userRouter.Set(UserID(args.Uid), newUr)
}
