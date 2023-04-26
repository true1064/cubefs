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

	"github.com/cubefs/cubefs/blobstore/common/rpc"
)

type ArgsAddUserConfig struct {
	Path string `json:"path"`
}

type CreateDriveResult struct {
	DriveID string `json:"driveId"`
}

type AppPathInfo struct {
	Path   string `json:"path"`
	Status int    `json:"status"`
	MTime  int64  `json:"mtime"`
}

type GetUserConfigResult struct {
	ClusterID string        `json:"clusterid"`
	VolumeID  string        `json:"volumeid"`
	RootPath  string        `json:"rootPath"`
	AppPaths  []AppPathInfo `json:"appPaths"`
}

// createDrive handle drive apis.
func (d *DriveNode) handleCreateDrive(c *rpc.Context) {
	ctx, span := d.ctxSpan(c)
	uid := d.userID(c)
	driveid, err := d.CreateUserRoute(ctx, uid)
	if err != nil {
		span.Errorf("crate user route error: %v, uid=%s", err, string(uid))
		c.RespondError(err)
		return
	}
	c.RespondJSON(CreateDriveResult{driveid})
}

func (d *DriveNode) handleAddUserConfig(c *rpc.Context) {
	ctx, span := d.ctxSpan(c)
	args := new(ArgsAddUserConfig)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	uid := d.userID(c)
	err := d.addUserConfig(ctx, uid, args.Path)
	if err != nil {
		span.Errorf("add user config error: %v, uid=%s", err, string(uid))
		c.RespondError(err)
		return
	}
	c.Respond()
}

func (d *DriveNode) handleDelUserConfig(c *rpc.Context) {
	ctx, span := d.ctxSpan(c)
	args := new(ArgsAddUserConfig)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	uid := d.userID(c)
	err := d.delUserConfig(ctx, uid, args.Path)
	if err != nil {
		span.Errorf("add user config error: %v, uid=%s", err, string(uid))
		c.RespondError(err)
		return
	}
	c.Respond()
}

func (d *DriveNode) handleGetUserConfig(c *rpc.Context) {
	ctx, span := d.ctxSpan(c)
	uid := d.userID(c)
	ur, err := d.GetUserRouteInfo(ctx, uid)
	if err != nil {
		span.Errorf("get user route error: %v, uid=%s", err, string(uid))
		c.RespondError(err)
		return
	}
	xattrs, err := d.getUserConfigFromFile(ctx, uid, ur.ClusterID, ur.VolumeID, uint64(ur.RootFileID))
	if err != nil {
		span.Errorf("get user config error: %v, uid=%s", err, string(uid))
		c.RespondError(err)
		return
	}
	res := &GetUserConfigResult{
		ClusterID: ur.ClusterID,
		VolumeID:  ur.VolumeID,
		RootPath:  ur.RootPath,
	}
	for k, v := range xattrs {
		var ent ConfigEntry
		if err := json.Unmarshal([]byte(v), &ent); err != nil {
			continue
		}
		res.AppPaths = append(res.AppPaths, AppPathInfo{
			Path:   k,
			Status: int(ent.Status),
			MTime:  ent.Time,
		})
	}
	c.RespondJSON(res)
}
