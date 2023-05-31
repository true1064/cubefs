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
	"io"
	"path"

	"github.com/cubefs/cubefs/apinode/crypto"
	"github.com/cubefs/cubefs/apinode/sdk"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/util/bytespool"
)

// MPPart multipart part.
type MPPart struct {
	PartNumber uint16 `json:"partNumber,omitempty"`
	Size       int    `json:"size,omitempty"`
	MD5        string `json:"md5,omitempty"`
}

// ArgsMPUploads multipart upload or complete argument.
type ArgsMPUploads struct {
	Path     FilePath `json:"path"`
	UploadID string   `json:"uploadId,omitempty"`
	XFileID  string   `json:"fileId,omitempty"`
	FileID   FileID   `json:"-"`
}

func (d *DriveNode) handleMultipartUploads(c *rpc.Context) {
	args := new(ArgsMPUploads)
	if d.checkError(c, nil, c.ParseArgs(args)) {
		return
	}

	t := d.encrypTransmitter(c)
	if d.checkFunc(c, nil,
		func() error { return decodeFileID(&args.FileID, args.XFileID, t) },
		func() error { return decodeHex(&args.UploadID, args.UploadID, t) },
		func() error { return args.Path.Clean(t) }) {
		return
	}

	if args.UploadID == "" {
		d.multipartUploads(c, args, t)
	} else {
		d.multipartComplete(c, args, t)
	}
}

// RespMPuploads response uploads.
type RespMPuploads struct {
	UploadID string `json:"uploadId"`
}

func (d *DriveNode) multipartUploads(c *rpc.Context, args *ArgsMPUploads, t crypto.Transmitter) {
	ctx, span := d.ctxSpan(c)
	_, vol, err := d.getRootInoAndVolume(ctx, d.userID(c))
	if d.checkError(c, func(err error) { span.Info(err) }, err) {
		return
	}

	extend, err := d.getProperties(c)
	if d.checkError(c, func(err error) { span.Info(err) }, err) {
		return
	}

	fullPath := multipartFullPath(d.userID(c), args.Path)
	uploadID, err := vol.InitMultiPart(ctx, fullPath, uint64(args.FileID), extend)
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

	t, fail := d.decryptTransmitter(c)
	if fail {
		err = sdk.ErrBadRequest
		return
	}
	reader := t.Transmit(c.Request.Body)

	buf := bytespool.Alloc(size)
	defer bytespool.Free(buf)
	if _, err = io.ReadFull(reader, buf); err != nil {
		return
	}

	err = json.Unmarshal(buf, &parts)
	return
}

func (d *DriveNode) multipartComplete(c *rpc.Context, args *ArgsMPUploads, t crypto.Transmitter) {
	ctx, span := d.ctxSpan(c)
	_, vol, err := d.getRootInoAndVolume(ctx, d.userID(c))
	if err != nil {
		c.RespondError(err)
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
			MD5: part.MD5,
		})
		reqParts[part.PartNumber] = part.MD5
	}

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
				span.Warn("multipart complete size not supported", part.ID, part.Size)
				d.respError(c, sdk.ErrBadRequest.Extend("size not supported", part.ID, part.Size))
				return
			}
			if md5, ok := reqParts[part.ID]; ok {
				if md5 != part.MD5 {
					span.Warn("multipart complete part md5 mismatch", part.ID, md5)
					d.respError(c, sdk.ErrBadRequest.Extend("md5 mismatch", part.ID, md5))
					return
				}
			}
		}

		if next == 0 {
			break
		}
		marker = next
	}

	inode, err := vol.CompleteMultiPart(ctx, fullPath, args.UploadID, uint64(args.FileID), sParts)
	if d.checkError(c, func(err error) { span.Error("multipart complete", args, parts, err) }, err) {
		return
	}
	extend, err := vol.GetXAttrMap(ctx, inode.Inode)
	if d.checkError(c, func(err error) { span.Error("multipart complete, get properties", inode.Inode, err) }, err) {
		return
	}

	d.out.Publish(ctx, makeOpLog(OpUploadFile, d.userID(c), args.Path.String(), "size", inode.Size))
	span.Info("multipart complete", args, parts)
	_, filename := args.Path.Split()
	d.respData(c, inode2file(inode, filename, extend))
}

// ArgsMPUpload multipart upload part argument.
type ArgsMPUpload struct {
	Path        FilePath `json:"path"`
	UploadID    string   `json:"uploadId"`
	XPartNumber string   `json:"partNumber"`
	PartNumber  uint16   `json:"-"`
}

func (d *DriveNode) handleMultipartPart(c *rpc.Context) {
	args := new(ArgsMPUpload)
	if d.checkError(c, nil, c.ParseArgs(args)) {
		return
	}
	ctx, span := d.ctxSpan(c)

	t, fail := d.decryptTransmitter(c)
	if fail {
		return
	}
	if d.checkFunc(c, func(err error) { span.Info(err) },
		func() error { return decodeHex(&args.UploadID, args.UploadID, t) },
		func() error { return decodeHex(&args.PartNumber, args.XPartNumber, t) },
		func() error { return args.Path.Clean(t) }) {
		return
	}
	if args.PartNumber == 0 {
		d.respError(c, sdk.ErrBadRequest.Extend("partNumber is 0"))
		return
	}

	_, vol, err := d.getRootInoAndVolume(ctx, d.userID(c))
	ur, err1 := d.GetUserRouteInfo(ctx, d.userID(c))
	if d.checkError(c, func(err error) { span.Warn(err) }, err, err1) {
		return
	}

	reader := t.Transmit(c.Request.Body)
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
		MD5:        part.MD5,
	})
}

// ArgsMPList multipart parts list argument.
type ArgsMPList struct {
	Path     FilePath `json:"path"`
	UploadID string   `json:"uploadId"`
	XMarker  string   `json:"marker"`
	Marker   FileID   `json:"-"`
	XCount   string   `json:"count,omitempty"`
	Count    int      `json:"-"`
}

// RespMPList response of list parts.
type RespMPList struct {
	Parts []MPPart `json:"parts"`
	Next  FileID   `json:"next"`
}

func (d *DriveNode) handleMultipartList(c *rpc.Context) {
	args := new(ArgsMPList)
	if d.checkError(c, nil, c.ParseArgs(args)) {
		return
	}
	ctx, span := d.ctxSpan(c)

	t := d.encrypTransmitter(c)
	if d.checkFunc(c, func(err error) { span.Info(err) },
		func() error { return decodeHex(&args.UploadID, args.UploadID, t) },
		func() error { return decodeFileID(&args.Marker, args.XMarker, t) },
		func() error { return decodeHex(&args.Count, args.XCount, t) },
		func() error { return args.Path.Clean(t) }) {
		return
	}
	if args.Count <= 0 {
		args.Count = 400
	}

	_, vol, err := d.getRootInoAndVolume(ctx, d.userID(c))
	if d.checkError(c, func(err error) { span.Warn(err) }, err) {
		return
	}

	fullPath := multipartFullPath(d.userID(c), args.Path)
	sParts, next, _, err := vol.ListMultiPart(ctx, fullPath, args.UploadID, uint64(args.Count), args.Marker.Uint64())
	if d.checkError(c, func(err error) { span.Error("multipart list", args, err) }, err) {
		return
	}

	parts := make([]MPPart, 0, len(sParts))
	for _, part := range sParts {
		parts = append(parts, MPPart{
			PartNumber: part.ID,
			Size:       int(part.Size),
			MD5:        part.MD5,
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

	t := d.encrypTransmitter(c)
	if d.checkFunc(c, func(err error) { span.Info(err) },
		func() error { return decodeHex(&args.UploadID, args.UploadID, t) },
		func() error { return args.Path.Clean(t) }) {
		return
	}

	_, vol, err := d.getRootInoAndVolume(ctx, d.userID(c))
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
