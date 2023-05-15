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

	"github.com/cubefs/cubefs/apinode/sdk"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
)

// ArgsFileUpload file upload argument.
type ArgsFileUpload struct {
	Path   FilePath `json:"path"`
	FileID FileID   `json:"fileId,omitempty"`
	Owner  UserID   `json:"owner,omitempty"`
}

func (d *DriveNode) handleFileUpload(c *rpc.Context) {
	args := new(ArgsFileUpload)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	ctx, span := d.ctxSpan(c)

	originPath := string(args.Path)
	if args.Path.Clean(); !args.Path.IsFile() {
		span.Warnf("not a file path: %s -> %s", originPath, args.Path)
		c.RespondError(sdk.ErrInvalidPath)
		return
	}

	uid := args.Owner
	if !uid.Valid() {
		uid = d.userID(c)
	}

	root, vol, err := d.getRootInoAndVolume(ctx, uid)
	if err != nil {
		c.RespondError(err)
		return
	}

	dir, filename := args.Path.Split()
	info, err := d.createDir(ctx, vol, root, dir.String(), true)
	if err != nil {
		c.RespondError(err)
		return
	}

	reader, err := newCrc32Reader(c.Request.Header, c.Request.Body, span.Warnf)
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
		Body:   reader,
	})
	if err != nil {
		c.RespondError(err)
		return
	}

	d.out.Publish(ctx, makeOpLog(OpUploadFile, uid, string(args.Path), "size", inode.Size))
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

	reader, err := newCrc32Reader(c.Request.Header, c.Request.Body, span.Warnf)
	if err != nil {
		c.RespondError(err)
		return
	}

	var size uint64
	if l, _ := c.RequestLength(); l > 0 {
		size = uint64(l)
	} else {
		size-- // max
	}
	span.Infof("write file: %d offset:%d size:%d", args.FileID, ranged.Start, size)
	if err = vol.WriteFile(ctx, uint64(args.FileID), uint64(ranged.Start), size, reader); err != nil {
		c.RespondError(err)
		return
	}
	//d.out.Publish(ctx, makeOpLog(OpUpdateFile, uid, string(args.Path), "size", inode.Size))
	c.Respond()
}

// ArgsFileDownload file download argument.
type ArgsFileDownload struct {
	Path  FilePath `json:"path"`
	Owner UserID   `json:"owner,omitempty"`
}

func (d *DriveNode) handleFileDownload(c *rpc.Context) {
	args := new(ArgsFileDownload)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	ctx, span := d.ctxSpan(c)

	if args.Path.Clean(); !args.Path.IsFile() {
		span.Warn("invalid path", args.Path)
		c.RespondError(sdk.ErrInvalidPath)
		return
	}

	uid := args.Owner
	if !uid.Valid() {
		uid = d.userID(c)
	}

	root, vol, err := d.getRootInoAndVolume(ctx, uid)
	if err != nil {
		c.RespondError(err)
		return
	}

	file, err := d.lookup(ctx, vol, root, args.Path.String())
	if err != nil {
		span.Warn(err)
		c.RespondError(err)
		return
	}

	inode, err := vol.GetInode(ctx, file.Inode)
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
	err    error
	inode  uint64
	offset uint64
}

func (r *downReader) Read(p []byte) (n int, err error) {
	if r.err != nil {
		err = r.err
		return
	}
	n, err = r.vol.ReadFile(r.ctx, r.inode, r.offset, p)
	if err != nil {
		r.err = err
	}
	if r.err == nil && n < len(p) {
		r.err = io.EOF
		err = r.err
	}
	r.offset += uint64(n)
	return
}

func makeReader(ctx context.Context, vol sdk.IVolume, ino, off uint64) io.Reader {
	return &downReader{ctx: ctx, vol: vol, inode: ino, offset: off}
}

// ArgsFileRename rename file or dir.
type ArgsFileRename struct {
	Src FilePath `json:"src"`
	Dst FilePath `json:"dst"`
}

func (d *DriveNode) handleFileRename(c *rpc.Context) {
	args := new(ArgsFileRename)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	ctx, span := d.ctxSpan(c)

	args.Src.Clean()
	args.Dst.Clean()
	if !args.Src.Valid() || !args.Dst.Valid() {
		span.Warn("invalid rename", args)
		c.RespondError(sdk.ErrInvalidPath)
		return
	}
	span.Info("to rename", args)

	root, vol, err := d.getRootInoAndVolume(ctx, d.userID(c))
	if err != nil {
		c.RespondError(err)
		return
	}

	if args.Src.IsDir() {
		args.Src = args.Src[:len(args.Src)-1]
	}
	if args.Dst.IsDir() {
		args.Dst = args.Dst[:len(args.Dst)-1]
	}

	srcDir, srcName := args.Src.Split()
	srcParent, err := d.lookup(ctx, vol, root, string(srcDir))
	if err != nil {
		span.Warn("lookup src", srcDir, err)
		c.RespondError(err)
		return
	}
	dstDir, dstName := args.Dst.Split()
	dstParent, err := d.lookup(ctx, vol, root, string(dstDir))
	if err != nil {
		span.Warn("lookup dst", dstDir, err)
		c.RespondError(err)
		return
	}

	err = vol.Rename(ctx, srcParent.Inode, dstParent.Inode, srcName, dstName)
	if err != nil {
		span.Error("rename error", args, err)
	}
	d.out.Publish(ctx, makeOpLog(OpUpdateFile, d.userID(c), string(args.Src), "dst", string(args.Dst)))
	c.RespondError(err)
}

// ArgsFileCopy rename file or dir.
type ArgsFileCopy struct {
	Src  FilePath `json:"src"`
	Dst  FilePath `json:"dst"`
	Meta bool     `json:"meta,omitempty"`
}

func (d *DriveNode) handleFileCopy(c *rpc.Context) {
	args := new(ArgsFileCopy)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	ctx, span := d.ctxSpan(c)

	args.Src.Clean()
	args.Dst.Clean()
	if !args.Src.IsFile() || !args.Dst.IsFile() {
		span.Warn("invalid copy", args)
		c.RespondError(sdk.ErrInvalidPath)
		return
	}
	span.Info("to copy", args)

	root, vol, err := d.getRootInoAndVolume(ctx, d.userID(c))
	if err != nil {
		c.RespondError(err)
		return
	}

	file, err := d.lookup(ctx, vol, root, args.Src.String())
	if err != nil {
		span.Warn(err)
		c.RespondError(err)
		return
	}

	inode, err := vol.GetInode(ctx, file.Inode)
	if err != nil {
		c.RespondError(err)
		return
	}
	body := makeReader(ctx, vol, inode.Inode, 0)

	// TODO: remove shared meta
	extend := make(map[string]string)
	if args.Meta {
		extend, err = vol.GetXAttrMap(ctx, inode.Inode)
		if err != nil {
			c.RespondError(err)
			return
		}
	}

	dir, filename := args.Dst.Split()
	dstParent, err := d.createDir(ctx, vol, root, dir.String(), true)
	if err != nil {
		c.RespondError(err)
		return
	}

	_, err = vol.UploadFile(ctx, &sdk.UploadFileReq{
		ParIno: dstParent.Inode,
		Name:   filename,
		OldIno: 0,
		Extend: extend,
		Body:   body,
	})
	if err != nil {
		d.out.Publish(ctx, makeOpLog(OpCopyFile, d.userID(c), string(args.Src), "dst", string(args.Dst)))
	}
	c.RespondError(err)
}
