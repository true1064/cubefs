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
	"fmt"
	"io"
	"net/http"
	"path/filepath"

	"github.com/cubefs/cubefs/apinode/sdk"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
)

// ArgsFileUpload file upload argument.
type ArgsFileUpload struct {
	Path   string `json:"path"`
	FileID FileID `json:"fileId,omitempty"`
	Owner  UserID `json:"owner,omitempty"`
}

func (d *DriveNode) handleFileUpload(c *rpc.Context) {
	args := new(ArgsFileUpload)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	ctx, span := d.ctxSpan(c)

	uid := args.Owner
	if !uid.Valid() {
		uid = d.userID(c)
	}

	root, vol, err := d.getRootInoAndVolume(ctx, uid)
	if err != nil {
		c.RespondError(err)
		return
	}

	dir, filename := filepath.Split(args.Path)
	if filename == "" {
		span.Warn("invalid path:", args.Path)
		c.RespondError(sdk.ErrBadRequest)
		return
	}
	info, err := d.createDir(ctx, vol, root, dir)
	if err != nil {
		c.RespondError(err)
		return
	}

	extend := d.getProperties(c)
	inode, err := vol.UploadFile(ctx, &sdk.UploadFileReq{
		ParIno: info.Inode,
		Name:   filename,
		OldIno: uint64(args.FileID),
		Extend: extend,
		Body:   c.Request.Body,
	})
	if err != nil {
		c.RespondError(err)
		return
	}

	c.RespondJSON(inode2file(inode, filename, extend))
}

// ArgsFileWrite file write.
type ArgsFileWrite struct {
	FileID FileID `json:"fileId"`
}

func (d *DriveNode) handleFileWrite(c *rpc.Context) {
	args := new(ArgsFileWrite)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	ctx, span := d.ctxSpan(c)

	uid := d.userID(c)
	_, vol, err := d.getRootInoAndVolume(ctx, uid)
	if err != nil {
		c.RespondError(err)
		return
	}

	inode, err := vol.GetInode(ctx, uint64(args.FileID))
	if err != nil {
		c.RespondError(err)
		return
	}

	ranged, err := parseRange(c.Request.Header.Get(headerRange), int64(inode.Size))
	if err != nil {
		span.Warn(err)
		c.RespondError(sdk.ErrBadRequest)
		return
	}

	size, err := c.RequestLength()
	if err != nil {
		span.Warn(err)
		c.RespondError(sdk.ErrBadRequest)
		return
	}

	span.Infof("write file: %d offset:%d size:%d", args.FileID, ranged.Start, size)
	c.RespondError(vol.WriteFile(ctx, uint64(args.FileID),
		uint64(ranged.Start), uint64(size), c.Request.Body))
}

// ArgsFileDownload file download argument.
type ArgsFileDownload struct {
	FileID FileID `json:"fileId"`
	Owner  UserID `json:"owner,omitempty"`
}

func (d *DriveNode) handleFileDownload(c *rpc.Context) {
	args := new(ArgsFileDownload)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	ctx, span := d.ctxSpan(c)

	uid := args.Owner
	if !uid.Valid() {
		uid = d.userID(c)
	}

	_, vol, err := d.getRootInoAndVolume(ctx, uid)
	if err != nil {
		c.RespondError(err)
		return
	}

	inode, err := vol.GetInode(ctx, uint64(args.FileID))
	if err != nil {
		c.RespondError(err)
		return
	}

	offset := uint64(0)
	size := inode.Size
	header := c.Request.Header.Get(headerRange)
	if header != "" {
		ranged, err := parseRange(header, int64(inode.Size))
		if err != nil {
			span.Warn(err)
			c.RespondError(sdk.ErrBadRequest)
			return
		}
		offset = uint64(ranged.Start)
		size = uint64(ranged.End - ranged.Start + 1)
	}

	status := http.StatusOK
	headers := make(map[string]string)
	if size < inode.Size {
		status = http.StatusPartialContent
		headers[rpc.HeaderContentRange] = fmt.Sprintf("bytes %d-%d/%d",
			offset, offset+size-1, inode.Size)
	}

	body := makeReader(ctx, vol, inode.Inode, offset)
	c.RespondWithReader(status, int(size), rpc.MIMEStream, body, headers)
}

type downReader struct {
	ctx    context.Context
	vol    sdk.IVolume
	inode  uint64
	offset uint64
}

func (r *downReader) Read(p []byte) (n int, err error) {
	n, err = r.vol.ReadFile(r.ctx, r.inode, r.offset, p)
	r.offset += uint64(n)
	return
}

func makeReader(ctx context.Context, vol sdk.IVolume, ino, off uint64) io.Reader {
	return &downReader{ctx: ctx, vol: vol, inode: ino, offset: off}
}

func (d *DriveNode) handleFileRename(c *rpc.Context) {
	c.Respond()
}

func (d *DriveNode) handleFileCopy(c *rpc.Context) {
	c.Respond()
}
