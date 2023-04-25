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
	FileID   FileID   `json:"fileId,omitempty"`
}

func (d *DriveNode) handleMultipartUploads(c *rpc.Context) {
	args := new(ArgsMPUploads)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	if err := args.Path.Clean(); err != nil {
		c.RespondError(err)
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
	_, vol, err := d.getRootInoAndVolume(ctx, d.userID(c))
	if err != nil {
		c.RespondError(err)
		return
	}

	extend := d.getProperties(c)
	uploadID, err := vol.InitMultiPart(ctx, args.Path.String(), uint64(args.FileID), extend)
	if err != nil {
		span.Error("multipart uploads", args, err)
		c.RespondError(err)
		return
	}
	c.RespondJSON(RespMPuploads{UploadID: uploadID})
}

func requestParts(c *rpc.Context) (parts []MPPart, err error) {
	var size int
	size, err = c.RequestLength()
	if err != nil {
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
	_, vol, err := d.getRootInoAndVolume(ctx, d.userID(c))
	if err != nil {
		c.RespondError(err)
		return
	}

	parts, err := requestParts(c)
	if err != nil {
		span.Warn("multipart complete", args, err)
		c.RespondError(sdk.ErrBadRequest)
		return
	}

	sParts := make([]sdk.Part, 0, len(parts))
	for _, part := range parts {
		sParts = append(sParts, sdk.Part{
			ID:  part.PartNumber,
			MD5: part.MD5,
		})
	}

	_, err = vol.CompleteMultiPart(ctx, args.Path.String(), args.UploadID, uint64(args.FileID), sParts)
	if err != nil {
		span.Error("multipart complete", args, parts, err)
	} else {
		span.Info("multipart complete", args, parts)
	}
	c.RespondError(err)
}

// ArgsMPUpload multipart upload part argument.
type ArgsMPUpload struct {
	Path       FilePath `json:"path"`
	UploadID   string   `json:"uploadId"`
	PartNumber uint16   `json:"partNumber"`
}

func (d *DriveNode) handleMultipartPart(c *rpc.Context) {
	args := new(ArgsMPUpload)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	if err := args.Path.Clean(); err != nil {
		c.RespondError(err)
		return
	}
	ctx, span := d.ctxSpan(c)
	_, vol, err := d.getRootInoAndVolume(ctx, d.userID(c))
	if err != nil {
		c.RespondError(err)
		return
	}

	reader, err := newCrc32Reader(c.Request.Header, c.Request.Body, span.Warnf)
	if err != nil {
		c.RespondError(err)
		return
	}

	part, err := vol.UploadMultiPart(ctx, args.Path.String(), args.UploadID, args.PartNumber, reader)
	if err != nil {
		span.Error("multipart upload", args, err)
		c.RespondError(err)
		return
	}
	span.Info("multipart upload", args)
	c.RespondJSON(MPPart{
		PartNumber: args.PartNumber,
		MD5:        part.MD5,
	})
}

// ArgsMPList multipart parts list argument.
type ArgsMPList struct {
	Path     FilePath `json:"path"`
	UploadID string   `json:"uploadId"`
	Marker   FileID   `json:"marker"`
	Count    int      `json:"count,omitempty"`
}

// RespMPList response of list parts.
type RespMPList struct {
	Parts []MPPart `json:"parts"`
	Next  FileID   `json:"next"`
}

func (d *DriveNode) handleMultipartList(c *rpc.Context) {
	args := new(ArgsMPList)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	if err := args.Path.Clean(); err != nil {
		c.RespondError(err)
		return
	}
	ctx, span := d.ctxSpan(c)
	_, vol, err := d.getRootInoAndVolume(ctx, d.userID(c))
	if err != nil {
		c.RespondError(err)
		return
	}

	if args.Count <= 0 {
		args.Count = 400
	}

	sParts, next, _, err := vol.ListMultiPart(ctx, args.Path.String(), args.UploadID, uint64(args.Count), args.Marker.Uint64())
	if err != nil {
		span.Error("multipart list", args, err)
		c.RespondError(err)
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
	c.RespondJSON(RespMPList{Parts: parts, Next: FileID(next)})
}

// ArgsMPAbort multipart abort argument.
type ArgsMPAbort struct {
	Path     FilePath `json:"path"`
	UploadID string   `json:"uploadId"`
}

func (d *DriveNode) handleMultipartAbort(c *rpc.Context) {
	args := new(ArgsMPAbort)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	if err := args.Path.Clean(); err != nil {
		c.RespondError(err)
		return
	}
	ctx, span := d.ctxSpan(c)
	_, vol, err := d.getRootInoAndVolume(ctx, d.userID(c))
	if err != nil {
		c.RespondError(err)
		return
	}

	if err = vol.AbortMultiPart(ctx, args.Path.String(), args.UploadID); err != nil {
		span.Error("multipart abort", args, err)
	} else {
		span.Warn("multipart abort", args)
	}
	c.RespondError(err)
}
