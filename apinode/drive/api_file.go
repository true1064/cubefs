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
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/cubefs/cubefs/apinode/sdk"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
)

// ArgsFileUpload file upload argument.
type ArgsFileUpload struct {
	Path   FilePath `json:"path"`
	FileID FileID   `json:"fileId,omitempty"`
}

func (d *DriveNode) handleFileUpload(c *rpc.Context) {
	args := new(ArgsFileUpload)
	if d.checkError(c, nil, c.ParseArgs(args)) {
		return
	}
	ctx, span := d.ctxSpan(c)
	if d.checkError(c, func(err error) { span.Info(args.Path, err) }, args.Path.Clean()) {
		return
	}

	uid := d.userID(c)
	root, vol, err := d.getRootInoAndVolume(ctx, uid)
	ur, err1 := d.GetUserRouteInfo(ctx, uid)
	if d.checkError(c, func(err error) { span.Warn(err) }, err, err1) {
		return
	}
	if d.checkError(c, func(err error) { span.Warn(args.Path, err) },
		d.lookupID(ctx, vol, root, args.Path, args.FileID)) {
		return
	}

	dir, filename := args.Path.Split()
	info, err := d.createDir(ctx, vol, root, dir.String(), true)
	if d.checkError(c, func(err error) { span.Warn(root, dir, err) }, err) {
		return
	}

	hasher := md5.New()
	var reader io.Reader = io.TeeReader(c.Request.Body, hasher)
	if d.checkFunc(c, func(err error) { span.Warn(err) },
		func() error { reader, err = newCrc32Reader(c.Request.Header, reader, span.Warnf); return err },
		func() error { reader, err = d.cryptor.FileEncryptor(ur.CipherKey, reader); return err }) {
		return
	}

	extend, err := d.getProperties(c)
	if d.checkError(c, nil, err) {
		return
	}
	st := time.Now()
	inode, err := vol.UploadFile(ctx, &sdk.UploadFileReq{
		ParIno: info.Inode,
		Name:   filename,
		OldIno: args.FileID.Uint64(),
		Extend: extend,
		Body:   reader,
		Callback: func() error {
			extend[internalMetaMD5] = hex.EncodeToString(hasher.Sum(nil))
			return nil
		},
	})
	span.Info("to upload file", args, extend)
	span.AppendTrackLog("cfuu", st, err)
	if d.checkError(c, func(err error) { span.Error("upload file", err) }, err) {
		return
	}

	d.out.Publish(ctx, makeOpLog(OpUploadFile, d.requestID(c), uid, string(args.Path), "size", inode.Size))
	d.respData(c, inode2file(inode, filename, extend))
}

// ArgsFileWrite file write.
type ArgsFileWrite struct {
	Path   FilePath `json:"path"`
	FileID FileID   `json:"fileId,omitempty"`
}

func (d *DriveNode) handleFileWrite(c *rpc.Context) {
	args := new(ArgsFileWrite)
	if d.checkError(c, nil, c.ParseArgs(args)) {
		return
	}
	ctx, span := d.ctxSpan(c)
	if d.checkError(c, func(err error) { span.Info(args.Path, err) }, args.Path.Clean()) {
		return
	}

	uid := d.userID(c)
	root, vol, err := d.getRootInoAndVolume(ctx, uid)
	ur, err1 := d.GetUserRouteInfo(ctx, uid)
	if d.checkError(c, func(err error) { span.Warn(err) }, err, err1) {
		return
	}
	if d.checkError(c, func(err error) { span.Warn(args.Path, err) },
		d.lookupID(ctx, vol, root, args.Path, args.FileID)) {
		return
	}

	st := time.Now()
	inode, err := vol.GetInode(ctx, args.FileID.Uint64())
	span.AppendTrackLog("cfwi", st, err)
	if d.checkError(c, func(err error) { span.Warn(args.FileID, err) }, err) {
		return
	}

	ranged, err := parseRange(c.Request.Header.Get(headerRange), int64(inode.Size))
	if err == errOverSize {
		span.Warn(err)
		d.respError(c, sdk.ErrWriteOverSize)
		return
	} else if err != nil && err != errEndOfFile {
		span.Warn(err)
		d.respError(c, sdk.ErrBadRequest.Extend(err))
		return
	}

	var reader io.Reader = c.Request.Body
	if d.checkFunc(c, func(err error) { span.Warn(err) },
		func() error { reader, err = newCrc32Reader(c.Request.Header, reader, span.Warnf); return err }) {
		return
	}

	l, err := c.RequestLength()
	if err != nil {
		span.Warn(err)
		d.respError(c, sdk.ErrBadRequest.Extend(err))
		return
	}
	size := uint64(l)
	reader = newFixedReader(reader, int64(size))

	st = time.Now()
	first, firstN, err := d.blockReaderFirst(ctx, vol, inode, uint64(ranged.Start), ur.CipherKey)
	last, lastN, err1 := d.blockReaderLast(ctx, vol, inode, uint64(ranged.Start)+size, ur.CipherKey)
	span.AppendTrackLog("cfwr", st, err)
	if d.checkError(c, func(err error) { span.Warn(err) }, err, err1) {
		return
	}
	span.Infof("to write first(%d) size(%d) last(%d) with range[%d-]", firstN, size, lastN, ranged.Start)

	reader, err = d.cryptor.FileEncryptor(ur.CipherKey, io.MultiReader(first, reader, last))
	if d.checkError(c, func(err error) { span.Warn(err) }, err) {
		return
	}

	if err = d.out.Publish(ctx, makeOpLog(OpUpdateFile, d.requestID(c), uid, string(args.Path), "size", inode.Size)); err != nil {
		d.respError(c, err)
		return
	}
	wOffset, wSize := uint64(ranged.Start)-firstN, firstN+size+lastN
	span.Infof("write file:%d range-start:%d body-size:%d rewrite-offset:%d rewrite-size:%d",
		args.FileID, ranged.Start, size, wOffset, wSize)
	st = time.Now()
	defer func() { span.AppendTrackLog("cfww", st, nil) }()
	if d.checkError(c, func(err error) { span.Warn(err) },
		vol.WriteFile(ctx, args.FileID.Uint64(), wOffset, wSize, reader)) {
		return
	}
	c.Respond()
}

// ArgsFileDownload file download argument.
type ArgsFileDownload struct {
	Path FilePath `json:"path"`
}

func (d *DriveNode) downloadConfig(c *rpc.Context) {
	ctx, span := d.ctxSpan(c)
	file, err := d.lookup(ctx, d.vol, volumeRootIno, volumeConfigPath)
	if d.checkError(c, func(err error) { span.Warn("get config", d.volumeName, err) }, err) {
		return
	}

	inode, err := d.vol.GetInode(ctx, file.Inode)
	if d.checkError(c, func(err error) { span.Warn(err) }, err) {
		return
	}

	body, err := d.encryptResponse(c, makeFileReader(ctx, d.vol, inode.Inode, 0))
	if d.checkError(c, nil, err) {
		return
	}
	c.RespondWithReader(http.StatusOK, int(inode.Size), rpc.MIMEStream, body, nil)
}

func (d *DriveNode) handleFileDownload(c *rpc.Context) {
	args := new(ArgsFileDownload)
	if d.checkError(c, nil, c.ParseArgs(args)) {
		return
	}
	ctx, span := d.ctxSpan(c)
	if d.checkError(c, func(err error) { span.Info(err) }, args.Path.Clean()) {
		return
	}

	if c.Request.Header.Get(HeaderVolume) == "default" && args.Path.String() == volumeConfigPath {
		d.downloadConfig(c)
		return
	}

	uid := d.userID(c)
	root, vol, err := d.getRootInoAndVolume(ctx, uid)
	ur, err1 := d.GetUserRouteInfo(ctx, uid)
	if d.checkError(c, func(err error) { span.Warn(err) }, err, err1) {
		return
	}

	file, err := d.lookup(ctx, vol, root, args.Path.String())
	if d.checkError(c, func(err error) { span.Warn(args.Path, err) }, err) {
		return
	}

	st := time.Now()
	inode, err := vol.GetInode(ctx, file.Inode)
	span.AppendTrackLog("cfdi", st, err)
	if d.checkError(c, func(err error) { span.Warn(file.Inode, err) }, err) {
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
			if err == errEndOfFile {
				c.Respond()
				return
			}
			span.Warn(err)
			d.respError(c, sdk.ErrBadRequest.Extend(err))
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
	if d.checkError(c, func(err error) { span.Warn(err) }, err) {
		return
	}
	body, err = d.encryptResponse(c, body)
	if d.checkError(c, nil, err) {
		return
	}

	st = time.Now()
	defer func() { span.AppendTrackLog("cfdw", st, nil) }()
	span.Debug("download", args, ranged)
	c.RespondWithReader(status, size, rpc.MIMEStream, body, headers)
}

// ArgsFileRename rename file or dir.
type ArgsFileRename struct {
	Src FilePath `json:"src"`
	Dst FilePath `json:"dst"`
}

func (d *DriveNode) handleFileRename(c *rpc.Context) {
	args := new(ArgsFileRename)
	if d.checkError(c, nil, c.ParseArgs(args)) {
		return
	}
	ctx, span := d.ctxSpan(c)
	if d.checkError(c, func(err error) { span.Info(args, err) }, args.Src.Clean(), args.Dst.Clean()) {
		return
	}
	span.Info("to rename", args)

	root, vol, err := d.getRootInoAndVolume(ctx, d.userID(c))
	if d.checkError(c, func(err error) { span.Warn(err) }, err) {
		return
	}

	if args.Src.IsDir() {
		args.Src = args.Src[:len(args.Src)-1]
	}
	if args.Dst.IsDir() {
		args.Dst = args.Dst[:len(args.Dst)-1]
	}

	srcDir, srcName := args.Src.Split()
	dstDir, dstName := args.Dst.Split()
	if srcName == "" || dstName == "" {
		span.Errorf("invalid src=%s dst=%s", args.Src, args.Dst)
		d.respError(c, sdk.ErrBadRequest.Extend("invalid path"))
		return
	}
	srcParentIno := root
	if srcDir != "" && srcDir != "/" {
		var srcParent *sdk.DirInfo
		srcParent, err = d.lookup(ctx, vol, root, srcDir.String())
		if d.checkError(c, func(err error) { span.Warn("lookup src", srcDir, err) }, err) {
			return
		}
		srcParentIno = Inode(srcParent.Inode)
	}
	dstParentIno := root
	if dstDir != "" && dstDir != "/" {
		var dstParent *sdk.DirInfo
		dstParent, err = d.lookup(ctx, vol, root, dstDir.String())
		if d.checkError(c, func(err error) { span.Warn("lookup dst", srcDir, err) }, err) {
			return
		}
		dstParentIno = Inode(dstParent.Inode)
	}

	err = vol.Rename(ctx, srcParentIno.Uint64(), dstParentIno.Uint64(), srcName, dstName)
	if d.checkError(c, func(err error) { span.Error("rename error", args, err) }, err) {
		return
	}
	d.out.Publish(ctx, makeOpLog(OpRename, d.requestID(c), d.userID(c), string(args.Src), "dst", string(args.Dst)))
	c.Respond()
}

// ArgsFileCopy rename file or dir.
type ArgsFileCopy struct {
	Src  FilePath `json:"src"`
	Dst  FilePath `json:"dst"`
	Meta bool     `json:"meta,omitempty"`
}

func (d *DriveNode) handleFileCopy(c *rpc.Context) {
	args := new(ArgsFileCopy)
	if d.checkError(c, nil, c.ParseArgs(args)) {
		return
	}
	ctx, span := d.ctxSpan(c)
	if d.checkError(c, func(err error) { span.Warn(args, err) }, args.Src.Clean(), args.Dst.Clean()) {
		return
	}
	span.Info("to copy", args)

	root, vol, err := d.getRootInoAndVolume(ctx, d.userID(c))
	ur, err1 := d.GetUserRouteInfo(ctx, d.userID(c))
	if d.checkError(c, func(err error) { span.Warn(err) }, err, err1) {
		return
	}

	file, err := d.lookup(ctx, vol, root, args.Src.String())
	if d.checkError(c, func(err error) { span.Warn(err) }, err) {
		return
	}

	st := time.Now()
	inode, err := vol.GetInode(ctx, file.Inode)
	span.AppendTrackLog("cfci", st, err)
	if d.checkError(c, func(err error) { span.Warn(file.Inode, err) }, err) {
		return
	}
	hasher := md5.New()

	reader, err := d.makeBlockedReader(ctx, vol, inode.Inode, 0, ur.CipherKey)
	if d.checkError(c, func(err error) { span.Warn(args.Src, file.Inode, err) }, err) {
		return
	}
	reader = newFixedReader(io.TeeReader(reader, hasher), int64(inode.Size))
	reader, err = d.cryptor.FileEncryptor(ur.CipherKey, reader)
	if d.checkError(c, func(err error) { span.Warn(err) }, err) {
		return
	}

	st = time.Now()
	extend, err := vol.GetXAttrMap(ctx, inode.Inode)
	if d.checkError(c, func(err error) { span.Warn(err) }, err) {
		return
	}
	if !args.Meta {
		for k := range extend {
			if !strings.HasPrefix(k, internalMetaPrefix) {
				delete(extend, k)
			}
		}
	}

	dir, filename := args.Dst.Split()
	dstParent, err := d.createDir(ctx, vol, root, dir.String(), true)
	if d.checkError(c, func(err error) { span.Warn(root, dir, err) }, err) {
		return
	}

	if err = d.out.Publish(ctx, makeOpLog(OpCopyFile, d.requestID(c), d.userID(c), string(args.Src), "dst", string(args.Dst))); err != nil {
		d.respError(c, err)
		return
	}
	_, err = vol.UploadFile(ctx, &sdk.UploadFileReq{
		ParIno: dstParent.Inode,
		Name:   filename,
		OldIno: 0,
		Extend: extend,
		Body:   reader,
		Callback: func() error {
			newMd5 := hex.EncodeToString(hasher.Sum(nil))
			if oldMd5, ok := extend[internalMetaMD5]; ok {
				if oldMd5 != newMd5 {
					span.Errorf("copy md5 mismatch %s -> %s old:%s new:%s",
						args.Src.String(), args.Dst.String(), oldMd5, newMd5)
				}
			}
			extend[internalMetaMD5] = newMd5
			return nil
		},
	})
	span.AppendTrackLog("cfcc", st, err)
	if d.checkError(c, func(err error) { span.Error(err) }, err) {
		return
	}
	c.Respond()
}
