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
	"archive/tar"
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cubefs/cubefs/apinode/sdk"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
)

var bytesPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

// ArgsFileUpload file upload argument.
type ArgsFileUpload struct {
	Path   FilePath `json:"path"`
	FileID uint64   `json:"fileId,omitempty"`
}

func (d *DriveNode) handleFileUpload(c *rpc.Context) {
	args := new(ArgsFileUpload)
	ctx, span := d.ctxSpan(c)
	if d.checkError(c, func(err error) { span.Error(err) }, c.ParseArgs(args), args.Path.Clean(false)) {
		return
	}

	uid := d.userID(c, &args.Path)
	ur, vol, err := d.getUserRouterAndVolume(ctx, uid)
	if d.checkError(c, func(err error) { span.Warn(err) }, err, ur.CanWrite()) {
		return
	}
	root := ur.RootFileID

	dir, filename := args.Path.Split()
	ino, _, err := d.createDir(ctx, vol, root, dir.String(), true)
	if d.checkError(c, func(err error) { span.Warn(root, dir, err) }, err) {
		return
	}

	if d.checkError(c, func(err error) { span.Errorf("lookup %+v error: %v", args, err) },
		d.lookupFileID(ctx, vol, ino, filename, args.FileID)) {
		return
	}

	hasher := md5.New()
	var reader io.Reader = io.TeeReader(c.Request.Body, hasher)
	if d.checkFunc(c, func(err error) { span.Warn(err) },
		func() error { reader, err = newCrc32Reader(c.Request.Header, reader, span.Warnf); return err },
		func() error { reader, err = d.getFileEncryptor(ctx, ur.CipherKey, reader); return err }) {
		return
	}

	extend, err := d.getProperties(c)
	if d.checkError(c, func(err error) { span.Warn(err) }, err) {
		return
	}
	st := time.Now()
	inode, fileID, err := vol.UploadFile(ctx, &sdk.UploadFileReq{
		ParIno:    ino.Uint64(),
		Name:      filename,
		OldFileId: args.FileID,
		Extend:    extend,
		Body:      reader,
		Callback: func() error {
			fileMD5 := hex.EncodeToString(hasher.Sum(nil))
			if errMD5 := verifyMD5(c.Request.Header, fileMD5); errMD5 != nil {
				return errMD5
			}
			extend[internalMetaMD5] = fileMD5
			return nil
		},
	})
	span.Info("to upload file", args, extend)
	span.AppendTrackLog("cfuu", st, err)
	if d.checkError(c, func(err error) { span.Error("upload file", err) }, err) {
		return
	}

	d.out.Publish(ctx, makeOpLog(OpUploadFile, d.requestID(c), uid, string(args.Path), "size", inode.Size))
	d.respData(c, inode2file(inode, fileID, filename, extend))
}

// BatchUploadFileResult response of batch uploads.
type BatchUploadFileResult struct {
	Uploaded []FileInfo   `json:"uploaded"`
	Errors   []ErrorEntry `json:"error"`
}

type uploadFilePipe struct {
	hdr     *tar.Header
	content *bytes.Buffer
}

func (d *DriveNode) handleFileUploadBatch(c *rpc.Context) {
	ctx, span := d.ctxSpan(c)

	uid := d.userID(c, nil)
	ur, vol, errx := d.getUserRouterAndVolume(ctx, uid)
	if d.checkError(c, func(err error) { span.Warn(err) }, errx, ur.CanWrite()) {
		return
	}
	root := ur.RootFileID

	var respLock sync.Mutex
	resp := BatchUploadFileResult{
		Uploaded: []FileInfo{},
		Errors:   []ErrorEntry{},
	}

	tr := tar.NewReader(c.Request.Body)
	addErr := func(path string, e error) {
		rpcErr := rpc.Error2HTTPError(e)
		respLock.Lock()
		resp.Errors = append(resp.Errors, ErrorEntry{
			Path:    path,
			Status:  rpcErr.StatusCode(),
			Code:    rpcErr.ErrorCode(),
			Message: rpcErr.Error(),
		})
		respLock.Unlock()
	}

	const cc = 8
	filePipe := make(chan uploadFilePipe, cc)
	go func() {
		st := time.Now()
		defer func() {
			close(filePipe)
			span.AppendTrackLog("cfubr", st, nil)
		}()

		for {
			hdr, err := tr.Next()
			if err == io.EOF {
				return // end of archive
			}
			if err != nil {
				span.Warn(err)
				addErr("", sdk.ErrBadRequest.Extend("file header", err))
				return
			}

			if hdr.Size > 32<<20 { // 32M
				span.Errorf("skip over size file %s %d", hdr.Name, hdr.Size)
				addErr(hdr.Name, sdk.ErrBadRequest.Extend("file over size"))
				if _, err = io.CopyN(io.Discard, tr, hdr.Size); err != nil {
					return
				}
				continue
			}

			content := bytesPool.Get().(*bytes.Buffer)
			if _, err = io.CopyN(content, tr, hdr.Size); err != nil {
				addErr(hdr.Name, sdk.ErrBadRequest.Extend("read body"))
				return
			}
			filePipe <- uploadFilePipe{hdr: hdr, content: content}
		}
	}()

	uploadFile := func(tarFile uploadFilePipe) {
		hdr := tarFile.hdr
		content := tarFile.content
		defer func() {
			content.Reset()
			bytesPool.Put(content)
		}()

		args := ArgsFileUpload{Path: FilePath(hdr.Name)}
		if err := args.Path.Clean(true); err != nil {
			addErr(args.Path.String(), err)
			return
		}
		path := args.Path.String()

		if fileID := hdr.PAXRecords[PAXFileID]; fileID != "" {
			id, errx := strconv.ParseUint(fileID, 10, 64)
			if errx != nil {
				addErr(path, sdk.ErrBadRequest.Extend("parse fileid", errx))
				return
			}
			args.FileID = id
		}

		dir, filename := args.Path.Split()
		ino, _, err := d.createDir(ctx, vol, root, dir.String(), true)
		if err != nil {
			span.Warn(root, dir, err)
			addErr(path, err)
			return
		}

		if err = d.lookupFileID(ctx, vol, ino, filename, args.FileID); err != nil {
			span.Errorf("lookup %+v error: %v", args, err)
			addErr(path, err)
			return
		}

		hasher := md5.New()
		var reader io.Reader = io.TeeReader(newFixedReader(content, hdr.Size), hasher)
		paxHeader := make(http.Header)
		paxHeader.Set(HeaderCrc32, hdr.PAXRecords[PAXCrc32])
		paxHeader.Set(HeaderMD5, hdr.PAXRecords[PAXMD5])
		if reader, err = newCrc32Reader(paxHeader, reader, span.Warnf); err != nil {
			addErr(path, err)
			return
		}
		if reader, err = d.getFileEncryptor(ctx, ur.CipherKey, reader); err != nil {
			addErr(path, err)
			return
		}
		var extend map[string]string
		if extend, err = d.getPropertiesTar(hdr.PAXRecords); err != nil {
			addErr(path, err)
			return
		}

		inode, fileID, err := vol.UploadFile(ctx, &sdk.UploadFileReq{
			ParIno:    ino.Uint64(),
			Name:      filename,
			OldFileId: args.FileID,
			Extend:    extend,
			Body:      reader,
			Callback: func() error {
				fileMD5 := hex.EncodeToString(hasher.Sum(nil))
				if errMD5 := verifyMD5(paxHeader, fileMD5); errMD5 != nil {
					return errMD5
				}
				extend[internalMetaMD5] = fileMD5
				return nil
			},
		})
		span.Info("to uploads file", args, extend)
		if err != nil {
			span.Error("uploads file", err)
			addErr(path, err)
			return
		}

		d.out.Publish(ctx, makeOpLog(OpUploadFile, d.requestID(c), uid, args.Path.String(), "size", inode.Size))
		respLock.Lock()
		resp.Uploaded = append(resp.Uploaded, *inode2file(inode, fileID, args.Path.String(), extend))
		respLock.Unlock()
	}

	st := time.Now()
	var wg sync.WaitGroup
	wg.Add(cc)
	for range [cc]struct{}{} {
		go func() {
			defer wg.Done()
			for {
				tarFile, ok := <-filePipe
				if !ok {
					return
				}
				uploadFile(tarFile)
			}
		}()
	}
	wg.Wait()
	span.AppendTrackLog("cfubw", st, nil)
	d.respData(c, resp)
}

// ArgsFileWrite file write.
type ArgsFileWrite struct {
	Path   FilePath `json:"path"`
	FileID uint64   `json:"fileId,omitempty"`
}

func (d *DriveNode) handleFileWrite(c *rpc.Context) {
	args := new(ArgsFileWrite)
	ctx, span := d.ctxSpan(c)
	if d.checkError(c, func(err error) { span.Error(err) }, c.ParseArgs(args), args.Path.Clean(true)) {
		return
	}

	uid := d.userID(c, nil)
	ur, vol, err := d.getUserRouterAndVolume(ctx, uid)
	if d.checkError(c, func(err error) { span.Warn(err) }, err, ur.CanWrite()) {
		return
	}
	root := ur.RootFileID
	dirInfo, err := d.lookupFile(ctx, vol, root, args.Path.String())
	if err != nil {
		if err == sdk.ErrNotFound {
			err = sdk.Conflicted(0)
		}
		span.Warn("lookup file", args, err)
		d.respError(c, err)
		return
	}
	if dirInfo.FileId != args.FileID {
		span.Warn("fileid mismatch", args.FileID, dirInfo.FileId)
		d.respError(c, sdk.Conflicted(dirInfo.FileId))
		return
	}

	st := time.Now()
	inode, err := vol.GetInode(ctx, dirInfo.Inode)
	span.AppendTrackLog("cfwi", st, err)
	if d.checkError(c, func(err error) { span.Warn(args, err) }, err) {
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

	reader, err = d.getFileEncryptor(ctx, ur.CipherKey, io.MultiReader(first, reader, last))
	if d.checkError(c, func(err error) { span.Warn(err) }, err) {
		return
	}

	if d.checkError(c, func(err error) {
		span.Warn("delete old xattr", internalMetaMD5, err.Error())
	}, vol.DeleteXAttr(ctx, inode.Inode, internalMetaMD5)) {
		return
	}

	wOffset, wSize := uint64(ranged.Start)-firstN, firstN+size+lastN
	span.Infof("write file: %+v range-start: %d body-size: %d rewrite-offset: %d rewrite-size: %d",
		args, ranged.Start, size, wOffset, wSize)
	st = time.Now()
	if d.checkError(c, func(err error) { span.AppendTrackLog("cfww", st, err); span.Warn(err) },
		vol.WriteFile(ctx, inode.Inode, wOffset, wSize, reader)) {
		return
	}
	span.AppendTrackLog("cfww", st, nil)

	// inode.Size is not the real file size.
	d.out.Publish(ctx, makeOpLog(OpUpdateFile, d.requestID(c), uid, args.Path.String(), "size", inode.Size))
	c.Respond()
}

// ArgsFileVerify verify file content.
type ArgsFileVerify struct {
	Path FilePath `json:"path"`
}

func (d *DriveNode) handleFileVerify(c *rpc.Context) {
	args := new(ArgsFileVerify)
	ctx, span := d.ctxSpan(c)
	if d.checkError(c, func(err error) { span.Error(err) }, c.ParseArgs(args), args.Path.Clean(false)) {
		return
	}

	uid := d.userID(c, &args.Path)
	ur, vol, err := d.getUserRouterAndVolume(ctx, uid)
	if d.checkError(c, func(err error) { span.Warn(err) }, err) {
		return
	}
	root := ur.RootFileID

	file, err := d.lookupFile(ctx, vol, root, args.Path.String())
	if d.checkError(c, func(err error) { span.Warn(args.Path, err) }, err) {
		return
	}

	st := time.Now()
	inode, err := vol.GetInode(ctx, file.Inode)
	span.AppendTrackLog("cfvi", st, err)
	if d.checkError(c, func(err error) { span.Warn(file.Inode, err) }, err) {
		return
	}

	var ranged ranges
	ranged.End = int64(inode.Size) - 1
	if header := c.Request.Header.Get(headerRange); header != "" {
		ranged, err = parseRange(header, int64(inode.Size))
		if err != nil {
			span.Warn(err)
			d.respError(c, sdk.ErrBadRequest.Extend(err))
			return
		}
	}
	size := ranged.End - ranged.Start + 1
	if size <= 0 {
		c.Respond()
		return
	}

	checksum, err := parseChecksums(c.Request.Header)
	if d.checkError(c, func(err error) { span.Warn(err) }, err) {
		return
	}
	span.Info("to verify", args.Path, ranged, checksum)
	if len(checksum) == 0 {
		c.Respond()
		return
	}

	r, err := d.makeBlockedReader(ctx, vol, inode.Inode, uint64(ranged.Start), ur.CipherKey)
	if d.checkError(c, func(err error) { span.Warn(err) }, err) {
		return
	}
	r = io.TeeReader(newFixedReader(r, size), checksum.writer())
	if d.checkFunc(c, func(err error) { span.Error(err) },
		func() error { _, err = io.Copy(io.Discard, r); return err },
		func() error { return checksum.verify() }) {
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
	if d.checkError(c, func(err error) { span.Warn(err) }, err) {
		return
	}
	c.RespondWithReader(http.StatusOK, int(inode.Size), rpc.MIMEStream, body, nil)
}

func (d *DriveNode) handleFileDownload(c *rpc.Context) {
	args := new(ArgsFileDownload)
	ctx, span := d.ctxSpan(c)
	if d.checkError(c, func(err error) { span.Error(err) }, c.ParseArgs(args), args.Path.Clean(false)) {
		return
	}

	if c.Request.Header.Get(HeaderVolume) == "default" && args.Path.String() == volumeConfigPath {
		d.downloadConfig(c)
		return
	}

	uid := d.userID(c, &args.Path)
	ur, vol, err := d.getUserRouterAndVolume(ctx, uid)
	if d.checkError(c, func(err error) { span.Warn(err) }, err) {
		return
	}
	root := ur.RootFileID

	file, err := d.lookupFile(ctx, vol, root, args.Path.String())
	if d.checkError(c, func(err error) { span.Warn(args.Path, err) }, err) {
		return
	}

	md5Val, err := vol.GetXAttr(ctx, file.Inode, internalMetaMD5)
	needMD5 := err == nil && md5Val == ""

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
				c.Writer.Header().Set(rpc.HeaderContentRange,
					fmt.Sprintf("bytes %d-%d/%d", inode.Size, inode.Size, inode.Size))
				c.RespondStatus(http.StatusPartialContent)
				return
			}
			span.Warn(err)
			d.respError(c, sdk.ErrBadRequest.Extend(err))
			return
		}
	}
	size := int(ranged.End - ranged.Start + 1)

	var hasher hash.Hash
	status := http.StatusOK
	headers := make(map[string]string)
	if uint64(size) < inode.Size {
		status = http.StatusPartialContent
		headers[rpc.HeaderContentRange] = fmt.Sprintf("bytes %d-%d/%d",
			ranged.Start, ranged.End, inode.Size)
	} else if needMD5 {
		hasher = md5.New()
	}

	body, err := d.makeBlockedReader(ctx, vol, inode.Inode, uint64(ranged.Start), ur.CipherKey)
	if d.checkError(c, func(err error) { span.Warn(err) }, err) {
		return
	}
	if hasher != nil {
		body = io.TeeReader(body, hasher)
	}
	body, err = d.encryptResponse(c, body)
	if d.checkError(c, func(err error) { span.Warn(err) }, err) {
		return
	}

	span.Debug("download", args, ranged)

	c.Writer.Header().Set(rpc.HeaderContentType, rpc.MIMEStream)
	c.Writer.Header().Set(rpc.HeaderContentLength, strconv.Itoa(size))
	for key, val := range headers {
		c.Writer.Header().Set(key, val)
	}
	c.RespondStatus(status)

	st = time.Now()
	_, err = io.CopyN(c.Writer, body, int64(size))
	if err != nil {
		span.Info("download copy failed", args.Path, err)
	} else if hasher != nil {
		md5sum := hex.EncodeToString(hasher.Sum(nil))
		err = vol.SetXAttr(ctx, inode.Inode, internalMetaMD5, md5sum)
		span.Warn("download md5 feedback", args.Path, md5sum, err)
	}
	dur := time.Since(st).Nanoseconds() / 1e6
	span.AppendRPCTrackLog([]string{fmt.Sprintf("cfdw:%d", dur)})
}

// ArgsFileDownloadBatch files download.
type ArgsFileDownloadBatch []FilePath

type downloadFilePipe struct {
	hdr     *tar.Header
	content io.Reader
}

func (d *DriveNode) handleFileDownloadBatch(c *rpc.Context) {
	files := ArgsFileDownloadBatch{}
	ctx, span := d.ctxSpan(c)
	if d.checkError(c, func(err error) { span.Error(err) }, c.ParseArgs(&files)) {
		return
	}
	for idx := range files {
		if err := files[idx].Clean(true); err != nil {
			d.respError(c, err)
			return
		}
	}

	uid := d.userID(c, nil)
	ur, vol, err := d.getUserRouterAndVolume(ctx, uid)
	if d.checkError(c, func(err error) { span.Warn(err) }, err) {
		return
	}
	root := ur.RootFileID

	pipeR, pipeW := io.Pipe()
	body, err := d.encryptResponse(c, pipeR)
	if d.checkError(c, func(err error) { span.Warn(err) }, err) {
		return
	}

	c.Writer.Header().Set(rpc.HeaderContentType, "application/x-tar")
	c.RespondStatus(http.StatusOK)

	done := make(chan struct{})
	go func() { // write to client
		st := time.Now()
		if _, errx := io.Copy(c.Writer, body); errx != nil {
			span.Info("download copy failed", errx)
		}
		dur := time.Since(st).Nanoseconds() / 1e6
		span.AppendRPCTrackLog([]string{fmt.Sprintf("cfdb:%d", dur)})
		close(done)
	}()

	const cc = 8
	filePipe := make(chan downloadFilePipe, cc)

	downloadFile := func(path string) { // read a file to pipeline
		span.Debug("to download", path)
		file, errd := d.lookupFile(ctx, vol, root, path)
		if errd != nil {
			span.Warn(path, errd)
			return
		}

		inode, errd := vol.GetInode(ctx, file.Inode)
		if errd != nil {
			span.Warn(file.Inode, errd)
			return
		}
		hdr := &tar.Header{Name: path, Size: int64(inode.Size)}
		if inode.Size == 0 {
			filePipe <- downloadFilePipe{hdr: hdr, content: io.MultiReader()}
			return
		}

		fileBody, errd := d.makeBlockedReader(ctx, vol, inode.Inode, 0, ur.CipherKey)
		if errd != nil {
			span.Warn(errd)
			return
		}
		filePipe <- downloadFilePipe{hdr: hdr, content: fileBody}
	}

	var wg sync.WaitGroup
	wg.Add(cc)
	pathPipe := make(chan string)
	for range [cc]struct{}{} { // workers
		go func() {
			defer wg.Done()
			path, ok := <-pathPipe
			if !ok {
				return
			}
			downloadFile(path)
		}()
	}

	cancelCh := make(chan struct{})
	go func() {
		var canceled bool
		for _, path := range files {
			select {
			case <-cancelCh:
				canceled = true
			case pathPipe <- path.String():
			}
			if canceled {
				break
			}
		}
		close(pathPipe)
		wg.Wait()
		close(filePipe)
	}()

	tw := tar.NewWriter(pipeW)
	for tarFile := range filePipe {
		if err = tw.WriteHeader(tarFile.hdr); err != nil {
			span.Warn(err)
			close(cancelCh)
			break
		}
		if _, err = io.CopyN(tw, tarFile.content, tarFile.hdr.Size); err != nil {
			span.Warn("write body", err)
			close(cancelCh)
			break
		}
	}
	tw.Flush()
	pipeW.Close()
	<-done
}

// ArgsFileRename rename file or dir.
type ArgsFileRename struct {
	Src FilePath `json:"src"`
	Dst FilePath `json:"dst"`
}

func (d *DriveNode) handleFileRename(c *rpc.Context) {
	args := new(ArgsFileRename)
	ctx, span := d.ctxSpan(c)
	if d.checkError(c, func(err error) { span.Error(err) },
		c.ParseArgs(args), args.Src.Clean(false), args.Dst.Clean(false)) {
		return
	}
	span.Info("to rename", args)

	uid := d.userID(c, &args.Src)
	uidDst := d.userID(c, &args.Dst)
	if uid != uidDst {
		span.Warnf("rename between users", uid, uidDst)
		d.respError(c, sdk.ErrForbidden.Extend("rename between users"))
		return
	}

	ur, vol, err := d.getUserRouterAndVolume(ctx, d.userID(c))
	_, volSrc, errSrc := d.getUserRouterAndVolume(ctx, d.userID(c))
	if d.checkError(c, func(err error) { span.Warn(err) }, err, errSrc, ur.CanWrite()) {
		return
	}
	root := ur.RootFileID

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
		srcParent, err = d.lookup(ctx, volSrc, root, srcDir.String())
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
	dstInfo, err := d.lookup(ctx, vol, dstParentIno, dstName)
	if err == nil {
		span.Info("check dst exist:", args.Dst.String())
		d.respError(c, sdk.Conflicted(dstInfo.FileId))
		return
	} else if err != sdk.ErrNotFound {
		span.Warn("check dst", err)
		d.respError(c, err)
		return
	}

	if srcParentIno == dstParentIno && srcName == dstName {
		d.respError(c, sdk.ErrForbidden)
		return
	}

	span.Infof("parent ino of src(%d) dst(%d)", srcParentIno, dstParentIno)
	err = vol.Rename(ctx, args.Src.String(), args.Dst.String())
	if d.checkError(c, func(err error) { span.Error("rename error", args, err) }, err) {
		return
	}
	d.out.Publish(ctx, makeOpLog(OpRename, d.requestID(c), uid, args.Src.String(), "dst", args.Dst.String()))
	c.Respond()
}

// ArgsFileCopy copy file.
type ArgsFileCopy struct {
	Src  FilePath `json:"src"`
	Dst  FilePath `json:"dst"`
	Meta bool     `json:"meta,omitempty"`
}

func (d *DriveNode) handleFileCopy(c *rpc.Context) {
	args := new(ArgsFileCopy)
	ctx, span := d.ctxSpan(c)
	if d.checkError(c, func(err error) { span.Error(err) },
		c.ParseArgs(args), args.Src.Clean(false), args.Dst.Clean(false)) {
		return
	}
	span.Info("to copy", args)

	uid := d.userID(c, &args.Src)
	uidDst := d.userID(c, &args.Dst)
	if uid != uidDst {
		span.Warnf("copy between users", uid, uidDst)
		d.respError(c, sdk.ErrForbidden.Extend("copy between users"))
		return
	}

	dir, filename := args.Dst.Split()
	if filename == "" {
		span.Warn("invalid dst path", args.Dst)
		d.respError(c, sdk.ErrBadRequest.Extend("invalid path", args.Dst))
		return
	}

	ur, vol, err := d.getUserRouterAndVolume(ctx, d.userID(c))
	if d.checkError(c, func(err error) { span.Warn(err) }, err, errSrc, ur.CanWrite()) {
		return
	}
	root := ur.RootFileID

	file, err := d.lookupFile(ctx, volSrc, root, args.Src.String())
	if d.checkError(c, func(err error) { span.Warn(err) }, err) {
		return
	}
	dstParent, _, err := d.createDir(ctx, vol, root, dir.String(), true)
	if d.checkError(c, func(err error) { span.Warn(root, dir, err) }, err) {
		return
	}
	var oldFileID uint64
	dstFile, err := d.lookup(ctx, vol, dstParent, filename)
	if err == nil {
		if dstFile.IsDir() {
			span.Warn("args dst is dir:", args.Dst.String())
			d.respError(c, sdk.ErrNotFile)
			return
		}
		oldFileID = dstFile.Inode
	}

	st := time.Now()
	inode, err := volSrc.GetInode(ctx, file.Inode)
	span.AppendTrackLog("cfci", st, err)
	if d.checkError(c, func(err error) { span.Warn(file.Inode, err) }, err) {
		return
	}
	hasher := md5.New()

	reader, err := d.makeBlockedReader(ctx, volSrc, inode.Inode, 0, ur.CipherKey)
	if d.checkError(c, func(err error) { span.Warn(args.Src, file.Inode, err) }, err) {
		return
	}
	reader = newFixedReader(io.TeeReader(reader, hasher), int64(inode.Size))
	reader, err = d.getFileEncryptor(ctx, ur.CipherKey, reader)
	if d.checkError(c, func(err error) { span.Warn(err) }, err) {
		return
	}

	st = time.Now()
	extend, err := volSrc.GetXAttrMap(ctx, inode.Inode)
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

	_, _, err = vol.UploadFile(ctx, &sdk.UploadFileReq{
		ParIno:    dstParent.Uint64(),
		Name:      filename,
		OldFileId: oldFileID,
		Extend:    extend,
		Body:      reader,
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
	d.out.Publish(ctx, makeOpLog(OpCopyFile, d.requestID(c), uid, args.Src.String(), "dst", args.Dst.String()))
	c.Respond()
}
