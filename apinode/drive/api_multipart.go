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
	"fmt"
	"io"
	"path"
	"time"

	"github.com/cubefs/cubefs/apinode/crypto"
	"github.com/cubefs/cubefs/apinode/sdk"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/util/bytespool"
)

// MPPart multipart part.
type MPPart struct {
	PartNumber uint16 `json:"partNumber"`
	Size       int    `json:"size"`
	Etag       string `json:"etag"`
}

// ArgsMPUploads multipart upload or complete argument.
type ArgsMPUploads struct {
	Path     FilePath `json:"path"`
	UploadID string   `json:"uploadId,omitempty"`
	FileID   uint64   `json:"fileId,omitempty"`
}

func (d *DriveNode) handleMultipartUploads(c *rpc.Context) {
	args := new(ArgsMPUploads)
	_, span := d.ctxSpan(c)
	if d.checkError(c, func(err error) { span.Error(err) }, c.ParseArgs(args)) {
		return
	}
	if d.checkError(c, func(err error) { span.Error(args.Path, err) }, args.Path.Clean()) {
		return
	}

	if args.UploadID == "" {
		d.multipartUploads(c, args)
	} else {
		d.multipartComplete(c, args)
	}
}

// RespMPuploads response uploads.
type RespMPuploads struct {
	UploadID string `json:"uploadId"`
}

func (d *DriveNode) multipartUploads(c *rpc.Context, args *ArgsMPUploads) {
	ctx, span := d.ctxSpan(c)
	ur, vol, err := d.getUserRouterAndVolume(ctx, d.userID(c))
	if d.checkError(c, func(err error) { span.Error(err) }, err, ur.CanWrite()) {
		return
	}
	root := ur.RootFileID

	extend, err := d.getProperties(c)
	if d.checkError(c, func(err error) { span.Error(err) }, err) {
		return
	}

	if d.checkError(c, func(err error) { span.Error("lookup error: ", err, args) },
		d.lookupFileID(ctx, vol, root, args.Path.String(), args.FileID)) {
		return
	}

	fullPath := multipartFullPath(d.userID(c), args.Path)
	uploadID, err := vol.InitMultiPart(ctx, fullPath, extend)
	if d.checkError(c, func(err error) { span.Error("multipart uploads", args, err) }, err) {
		return
	}
	span.Info("multipart init", args, uploadID, extend)
	d.respData(c, RespMPuploads{UploadID: uploadID})
}

func (d *DriveNode) requestParts(c *rpc.Context) (parts []MPPart, err error) {
	var size int
	size, err = c.RequestLength()
	if err != nil {
		return
	}
	if size > (8 << 20) {
		err = fmt.Errorf("body is too long %d", size)
		return
	}

	buf := bytespool.Alloc(size)
	defer bytespool.Free(buf)
	if _, err = io.ReadFull(c.Request.Body, buf); err != nil {
		return
	}

	err = json.Unmarshal(buf, &parts)
	return
}

func (d *DriveNode) multipartComplete(c *rpc.Context, args *ArgsMPUploads) {
	ctx, span := d.ctxSpan(c)
	ur, vol, err := d.getUserRouterAndVolume(ctx, d.userID(c))
	if d.checkError(c, func(err error) { span.Info(err) }, err, ur.CanWrite()) {
		return
	}
	root := ur.RootFileID
	if d.checkError(c, func(err error) { span.Error("lookup error: ", err, args) },
		d.lookupFileID(ctx, vol, root, args.Path.String(), args.FileID)) {
		return
	}

	parts, err := d.requestParts(c)
	if err != nil {
		span.Warn("multipart complete", args, err)
		d.respError(c, sdk.ErrBadRequest.Extend(err))
		return
	}

	reqParts := make(map[uint16]string, len(parts))
	sParts := make([]sdk.Part, 0, len(parts))
	for _, part := range parts {
		sParts = append(sParts, sdk.Part{
			ID:  part.PartNumber,
			MD5: part.Etag,
		})
		reqParts[part.PartNumber] = part.Etag
	}

	st := time.Now()
	fullPath := multipartFullPath(d.userID(c), args.Path)
	marker := uint64(0)
	for {
		sParts, next, _, perr := vol.ListMultiPart(ctx, fullPath, args.UploadID, 400, marker)
		if d.checkError(c, func(err error) { span.Error("multipart complete list", args, err) }, perr) {
			return
		}

		for idx, part := range sParts {
			// not the last part
			if !(next == 0 && idx == len(sParts)-1) && part.Size%crypto.BlockSize != 0 {
				if err = vol.AbortMultiPart(ctx, fullPath, args.UploadID); err != nil {
					span.Error("multipart comlete server abort", args.UploadID, err)
				}

				span.Warn("multipart complete size not supported", part.ID, part.Size)
				d.respError(c, sdk.ErrBadRequest.Extend("size not supported", part.ID, part.Size))
				return
			}
			if etag, ok := reqParts[part.ID]; ok {
				if etag != part.MD5 {
					span.Warn("multipart complete part etag mismatch", part.ID, etag)
					d.respError(c, sdk.ErrBadRequest.Extend("etag mismatch", part.ID, etag))
					return
				}
			}
		}

		if next == 0 {
			break
		}
		marker = next
	}
	span.AppendTrackLog("cmcl", st, nil)

	inode, fileID, err := vol.CompleteMultiPart(ctx, fullPath, args.UploadID, args.FileID, sParts)
	if d.checkError(c, func(err error) { span.Error("multipart complete", args, parts, err) }, err) {
		return
	}
	extend, err := vol.GetXAttrMap(ctx, inode.Inode)
	if d.checkError(c, func(err error) { span.Error("multipart complete, get properties", inode.Inode, err) }, err) {
		return
	}

	d.out.Publish(ctx, makeOpLog(OpMultiUploadFile, d.requestID(c), d.userID(c), args.Path.String(), "size", inode.Size))
	span.Info("multipart complete", args, parts)
	_, filename := args.Path.Split()
	d.respData(c, inode2file(inode, fileID, filename, extend))
}

// ArgsMPUpload multipart upload part argument.
type ArgsMPUpload struct {
	Path       FilePath `json:"path"`
	UploadID   string   `json:"uploadId"`
	PartNumber uint16   `json:"partNumber"`
}

func (d *DriveNode) handleMultipartPart(c *rpc.Context) {
	args := new(ArgsMPUpload)
	ctx, span := d.ctxSpan(c)

	if d.checkError(c, func(err error) { span.Error(err) }, c.ParseArgs(args)) {
		return
	}
	if d.checkError(c, func(err error) { span.Info(args.Path, err) }, args.Path.Clean()) {
		return
	}
	if args.PartNumber == 0 || args.PartNumber >= maxMultipartNumber {
		d.respError(c, sdk.ErrBadRequest.Extendf("exceeded part number %d", maxMultipartNumber))
		return
	}

	ur, vol, err := d.getUserRouterAndVolume(ctx, d.userID(c))
	if d.checkError(c, func(err error) { span.Warn(err) }, err, ur.CanWrite()) {
		return
	}

	var reader io.Reader = c.Request.Body
	if d.checkFunc(c, func(err error) { span.Warn(err) },
		func() error { reader, err = newCrc32Reader(c.Request.Header, reader, span.Warnf); return err },
		func() error { reader, err = d.cryptor.FileEncryptor(ur.CipherKey, reader); return err }) {
		return
	}

	fullPath := multipartFullPath(d.userID(c), args.Path)
	part, err := vol.UploadMultiPart(ctx, fullPath, args.UploadID, args.PartNumber, reader)
	if d.checkError(c, func(err error) { span.Error("multipart upload", args, err) }, err) {
		return
	}
	span.Info("multipart upload", args)
	d.respData(c, MPPart{
		PartNumber: args.PartNumber,
		Etag:       part.MD5,
		Size:       int(part.Size),
	})
}

// ArgsMPList multipart parts list argument.
type ArgsMPList struct {
	Path     FilePath `json:"path"`
	UploadID string   `json:"uploadId"`
	Marker   FileID   `json:"marker"`
	Limit    int      `json:"limit,omitempty"`
}

// RespMPList response of list parts.
type RespMPList struct {
	Parts []MPPart `json:"parts"`
	Next  FileID   `json:"next"`
}

func (d *DriveNode) handleMultipartList(c *rpc.Context) {
	args := new(ArgsMPList)
	ctx, span := d.ctxSpan(c)
	if d.checkError(c, func(err error) { span.Error(err) }, c.ParseArgs(args)) {
		return
	}
	if d.checkError(c, func(err error) { span.Info(args.Path, err) }, args.Path.Clean()) {
		return
	}
	if args.Limit <= 0 {
		args.Limit = 400
	}
	if args.Limit > maxMultipartNumber {
		args.Limit = maxMultipartNumber
	}

	_, vol, err := d.getUserRouterAndVolume(ctx, d.userID(c))
	if d.checkError(c, func(err error) { span.Warn(err) }, err) {
		return
	}

	fullPath := multipartFullPath(d.userID(c), args.Path)
	sParts, next, _, err := vol.ListMultiPart(ctx, fullPath, args.UploadID, uint64(args.Limit), args.Marker.Uint64())
	if d.checkError(c, func(err error) { span.Error("multipart list", args, err) }, err) {
		return
	}

	parts := make([]MPPart, 0, len(sParts))
	for _, part := range sParts {
		parts = append(parts, MPPart{
			PartNumber: part.ID,
			Size:       int(part.Size),
			Etag:       part.MD5,
		})
	}
	span.Info("multipart list", args, next, parts)
	d.respData(c, RespMPList{Parts: parts, Next: FileID(next)})
}

// ArgsMPAbort multipart abort argument.
type ArgsMPAbort struct {
	Path     FilePath `json:"path"`
	UploadID string   `json:"uploadId"`
}

func (d *DriveNode) handleMultipartAbort(c *rpc.Context) {
	args := new(ArgsMPAbort)
	if d.checkError(c, nil, c.ParseArgs(args)) {
		return
	}
	ctx, span := d.ctxSpan(c)
	if d.checkError(c, func(err error) { span.Info(args.Path, err) }, args.Path.Clean()) {
		return
	}

	_, vol, err := d.getUserRouterAndVolume(ctx, d.userID(c))
	if d.checkError(c, func(err error) { span.Warn(err) }, err) {
		return
	}

	fullPath := multipartFullPath(d.userID(c), args.Path)
	if d.checkFunc(c, func(err error) { span.Error("multipart abort", args, err) },
		func() error { return vol.AbortMultiPart(ctx, fullPath, args.UploadID) }) {
		return
	}
	span.Warn("multipart abort", args)
	c.Respond()
}

func multipartFullPath(uid UserID, p FilePath) string {
	root := getRootPath(uid)
	return path.Join(root, p.String())
}
