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
)

// ArgsFileUpload file upload argument.
type ArgsFileUpload struct {
	aPath
	aFileID
	aOwner
}

func (d *DriveNode) handleFileUpload(c *rpc.Context) {
	_, span := d.ctxSpan(c)

	args := new(ArgsFileUpload)
	if err := c.ParseArgs(args); err != nil {
		span.Warn("parse args error:", err.Error())
		c.RespondError(err)
		return
	}
	c.Respond()
}

func (d *DriveNode) handleFileWrite(c *rpc.Context) {
	c.Respond()
}

func (d *DriveNode) handleFileDownload(c *rpc.Context) {
	c.Respond()
}

func (d *DriveNode) handleFileRename(c *rpc.Context) {
	c.Respond()
}

func (d *DriveNode) handleFileCopy(c *rpc.Context) {
	c.Respond()
}
