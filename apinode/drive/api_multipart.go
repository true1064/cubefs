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
	"encoding/json"
	"fmt"
	"io"
	"path"
	"sync"
	"time"

	"github.com/cubefs/cubefs/apinode/crypto"
	"github.com/cubefs/cubefs/apinode/sdk"
	"github.com/cubefs/cubefs/blobstore/common/resourcepool"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/util/bytespool"
)

var memPool *resourcepool.MemPool

func init() {
	memPool = resourcepool.NewMemPool(map[int]int{
		1 << 21: -1,
		1 << 22: -1,
		1 << 23: -1,
		1 << 24: -1,
		1 << 27: -1,
	})
}

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
	if d.checkError(c, func(err error) { span.Error(err) }, c.ParseArgs(args), args.Path.Clean(false)) {
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
	uid := d.userID(c, &args.Path)
	ur, vol, err := d.getUserRouterAndVolume(ctx, uid)
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

	fullPath := multipartFullPath(uid, args.Path)
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

func (d *DriveNode) checkParts(c *rpc.Context, vol sdk.IVolume, uid UserID, args *ArgsMPUploads) ([]sdk.Part, error) {
	ctx, span := d.ctxSpan(c)
	parts, err := d.requestParts(c)
	if err != nil {
		return nil, sdk.ErrBadRequest.Extend(err)
	}

	type indexEtag struct {
		Index int
		Etag  string
	}

	reqParts := make(map[uint16]indexEtag, len(parts))
	sParts := make([]sdk.Part, 0, len(parts))
	for idx, part := range parts {
		sParts = append(sParts, sdk.Part{
			ID:  part.PartNumber,
			MD5: part.Etag,
		})
		reqParts[part.PartNumber] = indexEtag{Index: idx, Etag: part.Etag}
	}

	fullPath := multipartFullPath(uid, args.Path)
	marker := uint64(0)
	for {
		listParts, next, _, perr := vol.ListMultiPart(ctx, fullPath, args.UploadID, 400, marker)
		if perr != nil {
			return nil, perr
		}

		for idx, part := range listParts {
			// not the last part
			if !(next == 0 && idx == len(listParts)-1) && part.Size%crypto.BlockSize != 0 {
				if err = vol.AbortMultiPart(ctx, fullPath, args.UploadID); err != nil {
					span.Error("multipart comlete server abort", args.UploadID, err)
				}
				return nil, sdk.ErrBadRequest.Extend("size not supported", part.ID, part.Size)
			}
			if ie, ok := reqParts[part.ID]; ok {
				sParts[ie.Index].Inode = part.Inode
				sParts[ie.Index].Size = part.Size
				if ie.Etag != part.MD5 {
					return nil, sdk.ErrBadRequest.Extend("etag mismatch", part.ID, ie.Etag)
				}
			}
		}

		if next == 0 {
			break
		}
		marker = next
	}

	for _, part := range sParts {
		if part.Inode == 0 {
			return nil, sdk.ErrBadRequest.Extend("inode mismatch", part.ID)
		}
	}
	return sParts, nil
}

func (d *DriveNode) calculatePartsMD5(c *rpc.Context, vol sdk.IVolume, parts []sdk.Part, key []byte) (string, error) {
	if len(parts) == 0 {
		return hex.EncodeToString(md5.New().Sum(nil)), nil
	}

	ctx, span := d.ctxSpan(c)
	solts := 8
	if n := len(parts); n < solts {
		solts = n
	}

	soltReaders := make([][]io.Reader, solts)
	soltSizes := make([][]int64, solts)
	l := (len(parts) / solts) + 1
	for solt := range soltReaders {
		soltReaders[solt] = make([]io.Reader, 0, l)
		soltSizes[solt] = make([]int64, 0, l)
	}

	for idx, part := range parts {
		solt := idx % solts
		var r io.Reader = &downReader{ctx: ctx, vol: vol, inode: part.Inode}
		soltReaders[solt] = append(soltReaders[solt], r)
		soltSizes[solt] = append(soltSizes[solt], int64(part.Size))
	}

	type partBuffer struct {
		buffer []byte
		err    error
	}
	readers := make([]io.Reader, 0, solts)
	buffers := make([]chan partBuffer, solts)
	for solt := range soltReaders {
		buffers[solt] = make(chan partBuffer, 1)
		er, err := d.cryptor.FileDecryptor(key, io.MultiReader(soltReaders[solt]...))
		if err != nil {
			return "", err
		}
		readers = append(readers, er)
	}

	var wg sync.WaitGroup
	wg.Add(solts + 1)

	closed := make(chan struct{})
	for solt := range soltReaders {
		go func(solt int) {
			defer func() {
				close(buffers[solt])
				wg.Done()
			}()

			for _, size := range soltSizes[solt] {
				select {
				case <-closed:
					return
				default:
				}

				p := partBuffer{}
				p.buffer, _ = memPool.Alloc(int(size))

				_, p.err = io.ReadFull(readers[solt], p.buffer)
				buffers[solt] <- p
				if p.err != nil {
					span.Warnf("read solt %d size %d %s", solt, size, p.err.Error())
					return
				}
			}
		}(solt)
	}

	hasher := md5.New()
	var err error
	go func() {
		has := true
		for has {
			has = false
			for solt := range buffers {
				p, ok := <-buffers[solt]
				if !ok {
					continue
				}
				has = true

				if p.err != nil {
					err = p.err
					close(closed)
				} else {
					hasher.Write(p.buffer)
				}
				memPool.Put(p.buffer)
			}
		}
		wg.Done()
	}()

	wg.Wait()

	if err != nil {
		return "", sdk.ErrInternalServerError.Extend(err)
	}
	return hex.EncodeToString(hasher.Sum(nil)), nil
}

func (d *DriveNode) multipartComplete(c *rpc.Context, args *ArgsMPUploads) {
	ctx, span := d.ctxSpan(c)
	uid := d.userID(c, &args.Path)
	ur, vol, err := d.getUserRouterAndVolume(ctx, uid)
	if d.checkError(c, func(err error) { span.Info(err) }, err, ur.CanWrite()) {
		return
	}
	root := ur.RootFileID

	fileInfo, err := d.lookup(ctx, vol, root, args.Path.String())
	if err == sdk.ErrNotFound {
		if args.FileID != 0 {
			span.Warn("not found file with id", args.FileID)
			d.respError(c, sdk.Conflicted(0))
			return
		}
	} else if err != nil {
		span.Error("lookup:", args, err)
		d.respError(c, err)
		return
	} else {
		xattrs, errAttr := vol.GetXAttrMap(ctx, fileInfo.Inode)
		inoInfo, errInode := vol.GetInode(ctx, fileInfo.Inode)
		if d.checkError(c, func(err error) { span.Error(err) }, errAttr, errInode) {
			return
		}
		if xattrs[internalMetaUploadID] == args.UploadID {
			d.respData(c, inode2file(inoInfo, fileInfo.FileId, fileInfo.Name, xattrs))
			return
		} else if fileInfo.FileId != args.FileID {
			span.Warn("fileid mismatch", args.FileID, fileInfo.FileId)
			d.respError(c, sdk.Conflicted(fileInfo.FileId))
			return
		}
	}

	compFile, err, _ := d.groupMulti.Do(args.UploadID, func() (interface{}, error) {
		st := time.Now()
		parts, gerr := d.checkParts(c, vol, uid, args)
		span.AppendTrackLog("cmcl", st, nil)
		if gerr != nil {
			span.Warn("multipart complete check", args, gerr)
			return nil, gerr
		}

		st = time.Now()
		fileMD5, gerr := d.calculatePartsMD5(c, vol, parts, ur.CipherKey)
		span.AppendTrackLog("cmcc", st, gerr)
		if gerr != nil {
			span.Warn("multipart complete calculate", args, gerr)
			return nil, gerr
		}
		if gerr = verifyMD5(c.Request.Header, fileMD5); gerr != nil {
			span.Warnf("multipart comlete", gerr)
			return nil, gerr
		}

		cmReq := &sdk.CompleteMultipartReq{
			FilePath:  multipartFullPath(uid, args.Path),
			UploadId:  args.UploadID,
			OldFileId: args.FileID,
			Parts:     parts,
			Extend: map[string]string{
				internalMetaMD5:      fileMD5,
				internalMetaUploadID: args.UploadID,
			},
		}
		inode, fileID, gerr := vol.CompleteMultiPart(ctx, cmReq)
		if gerr != nil {
			span.Error("multipart complete", args, parts, gerr)
			return nil, gerr
		}
		extend, gerr := vol.GetXAttrMap(ctx, inode.Inode)
		if gerr != nil {
			span.Error("multipart complete, get properties", inode.Inode, gerr)
			return nil, gerr
		}

		d.out.Publish(ctx, makeOpLog(OpMultiUploadFile, d.requestID(c), uid,
			args.Path.String(), "size", inode.Size))
		span.Info("multipart complete", fileMD5, args, parts)
		_, filename := args.Path.Split()
		return inode2file(inode, fileID, filename, extend), nil
	})
	if err != nil {
		d.respError(c, err)
		return
	}

	d.respData(c, compFile.(*FileInfo))
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
	if d.checkError(c, func(err error) { span.Error(err) }, c.ParseArgs(args), args.Path.Clean(false)) {
		return
	}
	if args.PartNumber == 0 || args.PartNumber >= maxMultipartNumber {
		d.respError(c, sdk.ErrBadRequest.Extendf("exceeded part number %d", maxMultipartNumber))
		return
	}

	uid := d.userID(c, &args.Path)
	ur, vol, err := d.getUserRouterAndVolume(ctx, uid)
	if d.checkError(c, func(err error) { span.Warn(err) }, err, ur.CanWrite()) {
		return
	}

	var reader io.Reader = c.Request.Body
	if d.checkFunc(c, func(err error) { span.Warn(err) },
		func() error { reader, err = newCrc32Reader(c.Request.Header, reader, span.Warnf); return err },
		func() error { reader, err = d.getFileEncryptor(ctx, ur.CipherKey, reader); return err }) {
		return
	}

	fullPath := multipartFullPath(uid, args.Path)
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
	if d.checkError(c, func(err error) { span.Error(err) }, c.ParseArgs(args), args.Path.Clean(false)) {
		return
	}
	if args.Limit <= 0 {
		args.Limit = 400
	}
	if args.Limit > maxMultipartNumber {
		args.Limit = maxMultipartNumber
	}

	uid := d.userID(c, &args.Path)
	_, vol, err := d.getUserRouterAndVolume(ctx, uid)
	if d.checkError(c, func(err error) { span.Warn(err) }, err) {
		return
	}

	fullPath := multipartFullPath(uid, args.Path)
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
	ctx, span := d.ctxSpan(c)
	if d.checkError(c, func(err error) { span.Error(err) }, c.ParseArgs(args), args.Path.Clean(false)) {
		return
	}

	uid := d.userID(c, &args.Path)
	_, vol, err := d.getUserRouterAndVolume(ctx, uid)
	if d.checkError(c, func(err error) { span.Warn(err) }, err) {
		return
	}

	fullPath := multipartFullPath(uid, args.Path)
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
