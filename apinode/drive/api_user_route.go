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
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

type ArgsPath struct {
	Path string `json:"path"`
	Type int8   `json:"type"`
}

// createDrive handle drive apis.
func (d *DriveNode) createDrive(c *rpc.Context) {
	rid := d.requestID(c)
	uid := d.userID(c)
	err := d.userRouter.Create(uid)
	if err != nil {
		c.RespondError(err)
		return
	}
	log.Info("got", rid, uid)
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
	err := d.userRouter.AddPath(uid, args)
	if err != nil {
		c.RespondError(err)
		return
	}
	log.Info("got", rid, uid)
	c.Respond()
}

func (d *DriveNode) getUserConfig(c *rpc.Context) {
	rid := d.requestID(c)
	uid := d.userID(c)
	log.Info("got", rid, uid)
	c.Respond()
}
