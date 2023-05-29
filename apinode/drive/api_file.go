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
		c.RespondError(sdk.ErrInvalidPath.Extend(originPath))
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
	ur, err := d.GetUserRouteInfo(ctx, uid)
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

	reader, err := d.cryptor.TransDecryptor(c.Request.Header.Get(headerCipherMaterial), c.Request.Body)
	if err != nil {
		span.Warn(err)
		c.RespondError(err)
		return
	}
	reader, err = newCrc32Reader(c.Request.Header, reader, span.Warnf)
	if err != nil {
		span.Warn(err)
		c.RespondError(err)
		return
	}
	reader, err = d.cryptor.FileEncryptor(ur.CipherKey, reader)
	if err != nil {
		span.Warn(err)
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
		span.Error(err)
		c.RespondError(err)
		return
	}

	d.out.Publish(ctx, makeOpLog(OpUploadFile, uid, string(args.Path), "size", inode.Size))
	c.RespondJSON(inode2file(inode, filename, extend))
}

// ArgsFileWrite file write.
type ArgsFileWrite struct {
	Path   FilePath `json:"path"`
	FileID FileID   `json:"fileId"`
}

func (d *DriveNode) handleFileWrite(c *rpc.Context) {
	args := new(ArgsFileWrite)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	ctx, span := d.ctxSpan(c)

	if err := args.Path.Clean(); err != nil {
		c.RespondError(sdk.ErrInvalidPath.Extend(args.Path))
		return
	}

	uid := d.userID(c)
	_, vol, err := d.getRootInoAndVolume(ctx, uid)
	if err != nil {
		c.RespondError(err)
		return
	}
	ur, err := d.GetUserRouteInfo(ctx, uid)
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
		if err == errOverSize {
			span.Error(err)
			c.RespondError(sdk.ErrWriteOverSize)
			return
		}
		span.Warn(err)
		c.RespondError(sdk.ErrBadRequest.Extend(err))
		return
	}

	reader, err := d.cryptor.TransDecryptor(c.Request.Header.Get(headerCipherMaterial), c.Request.Body)
	if err != nil {
		span.Warn(err)
		c.RespondError(err)
		return
	}
	reader, err = newCrc32Reader(c.Request.Header, reader, span.Warnf)
	if err != nil {
		span.Warn(err)
		c.RespondError(err)
		return
	}

	l, err := c.RequestLength()
	if err != nil {
		span.Warn(err)
		c.RespondError(sdk.ErrBadRequest.Extend(err))
		return
	}
	size := uint64(l)

	first, err := d.blockReaderFirst(ctx, vol, inode, uint64(ranged.Start), ur.CipherKey)
	if err != nil {
		span.Warn(err)
		c.RespondError(err)
		return
	}
	last, err := d.blockReaderLast(ctx, vol, inode, uint64(ranged.End), ur.CipherKey)
	if err != nil {
		span.Warn(err)
		c.RespondError(err)
		return
	}

	reader, err = d.cryptor.FileEncryptor(ur.CipherKey, io.MultiReader(first, reader, last))
	if err != nil {
		span.Warn(err)
		c.RespondError(err)
		return
	}
	span.Infof("write file: %d offset:%d size:%d", args.FileID, ranged.Start, size)
	if err = vol.WriteFile(ctx, uint64(args.FileID), uint64(ranged.Start), size, reader); err != nil {
		span.Error(err)
		c.RespondError(err)
		return
	}
	// d.out.Publish(ctx, makeOpLog(OpUpdateFile, uid, string(args.Path), "size", inode.Size))
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
	ur, err := d.GetUserRouteInfo(ctx, uid)
	if err != nil {
		c.RespondError(err)
		return
	}

	file, err := d.lookup(ctx, vol, root, args.Path.String())
	if err != nil {
		c.RespondError(err)
		return
	}

	inode, err := vol.GetInode(ctx, file.Inode)
	if err != nil {
		c.RespondError(err)
		return
	}
	if inode.Size == 0 {
		c.Respond()
		return
	}

	ranged := ranges{Start: 0, End: int64(inode.Size) - 1}
	if header := c.Request.Header.Get(headerRange); header != "" {
		ranged, err = parseRange(header, int64(inode.Size))
		if err != nil {
			span.Warn(err)
			c.RespondError(sdk.ErrBadRequest.Extend(err))
			return
		}
	}
	size := int(ranged.End - ranged.Start + 1)

	status := http.StatusOK
	headers := make(map[string]string)
	if uint64(size) < inode.Size {
		status = http.StatusPartialContent
		headers[rpc.HeaderContentRange] = fmt.Sprintf("bytes %d-%d/%d",
			ranged.Start, ranged.End, inode.Size)
	}

	body, err := d.makeBlockedReader(ctx, vol, inode.Inode, uint64(ranged.Start), ur.CipherKey)
	if err != nil {
		span.Warn(err)
		c.RespondError(err)
		return
	}
	body, err = d.cryptor.TransEncryptor(c.Request.Header.Get(headerCipherMaterial), body)
	if err != nil {
		span.Warn(err)
		c.RespondError(err)
		return
	}

	c.RespondWithReader(status, size, rpc.MIMEStream, body, headers)
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
	srcParentIno := root
	dstParentIno := root
	if srcName == "" {
		span.Error("invalid src=", args.Src)
		c.RespondError(sdk.ErrBadRequest)
		return
	}
	if srcDir != "" && srcDir != "/" {
		var srcParent *sdk.DirInfo
		srcParent, err = d.lookup(ctx, vol, root, srcDir.String())
		if err != nil {
			span.Warn("lookup src", srcDir, err)
			c.RespondError(err)
			return
		}
		srcParentIno = Inode(srcParent.Inode)
	}
	dstDir, dstName := args.Dst.Split()
	if dstName == "" {
		span.Error("invalid dst=", args.Dst)
		c.RespondError(sdk.ErrBadRequest)
		return
	}
	if dstDir != "" && dstDir != "/" {
		var dstParent *sdk.DirInfo
		dstParent, err = d.lookup(ctx, vol, root, dstDir.String())
		if err != nil {
			span.Warn("lookup dst", dstDir, err)
			c.RespondError(err)
			return
		}
		dstParentIno = Inode(dstParent.Inode)
	}

	err = vol.Rename(ctx, srcParentIno.Uint64(), dstParentIno.Uint64(), srcName, dstName)
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
		c.RespondError(err)
		return
	}

	inode, err := vol.GetInode(ctx, file.Inode)
	if err != nil {
		c.RespondError(err)
		return
	}
	body := makeFileReader(ctx, vol, inode.Inode, 0)

	// TODO: remove shared meta
	extend := make(map[string]string)
	if args.Meta {
		extend, err = vol.GetXAttrMap(ctx, inode.Inode)
		if err != nil {
			span.Warn(err)
			c.RespondError(err)
			return
		}
	}

	dir, filename := args.Dst.Split()
	dstParent, err := d.createDir(ctx, vol, root, dir.String(), true)
	if err != nil {
		span.Warn(err)
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
	if err == nil {
		d.out.Publish(ctx, makeOpLog(OpCopyFile, d.userID(c), string(args.Src), "dst", string(args.Dst)))
	} else {
		span.Error(err)
	}
	c.RespondError(err)
}
