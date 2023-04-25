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

type ArgsMkDir struct {
	Path      FilePath `json:"path"`
	Recursive bool     `json:"recursive,omitempty"`
}

// POST /v1/files/mkdir?path=/abc&recursive=bool
func (d *DriveNode) handleMkDir(c *rpc.Context) {
	ctx, span := d.ctxSpan(c)
	args := new(ArgsMkDir)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}

	if err := args.Path.Clean(); err != nil {
		span.Warnf("invalid path: %s", args.Path)
		c.RespondError(err)
		return
	}

	uid := d.userID(c)
	rootIno, vol, err := d.getRootInoAndVolume(ctx, uid)
	if err != nil {
		c.RespondError(err)
		return
	}

	_, err = d.createDir(ctx, vol, rootIno, args.Path.String(), args.Recursive)
	if err != nil {
		span.Errorf("create dir %s error: %s, uid=%s recursive=%v", args.Path, err.Error(), uid, args.Recursive)
		c.RespondError(err)
		return
	}
	c.Respond()
}
