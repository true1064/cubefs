package impl

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"io"
	"math"
	"os"
	"path"
	"strings"
	"syscall"

	"github.com/google/uuid"

	"github.com/cubefs/cubefs/apinode/sdk"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/data/stream"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/cubefs/cubefs/util"
)

var (
	newMetaWrapper  = newMetaOp
	newExtentClient = newDataOp
)

type volume struct {
	mw    sdk.MetaOp
	ec    sdk.DataOp
	name  string
	owner string
}

type DataOpImp struct {
	*stream.ExtentClient
}

func newDataOp(cfg *stream.ExtentConfig) (sdk.DataOp, error) {
	return stream.NewExtentClient(cfg)
}

func newVolume(ctx context.Context, name, owner, addr string) (sdk.IVolume, error) {
	span := trace.SpanFromContextSafe(ctx)

	addrList := strings.Split(addr, ",")
	metaCfg := &meta.MetaConfig{
		Volume:  name,
		Owner:   owner,
		Masters: addrList,
	}

	mw, err := newMetaWrapper(metaCfg)
	if err != nil {
		span.Errorf("init meta wrapper failed, name %s, owner %s, addr %s", name, owner, addr)
		return nil, sdk.ErrInternalServerError
	}

	ecCfg := &stream.ExtentConfig{
		Volume:       name,
		Masters:      addrList,
		FollowerRead: true,
		NearRead:     true,
		OnGetExtents: mw.GetExtents,
		OnTruncate:   mw.Truncate,
	}

	if mw1, ok := mw.(*metaOpImp); ok {
		ecCfg.OnAppendExtentKey = mw1.AppendExtentKey
	}

	ec, err := newExtentClient(ecCfg)
	if err != nil {
		span.Errorf("init extent client failed, name %s, owner %s, addr %s", name, owner, addr)
		return nil, sdk.ErrInternalServerError
	}

	if mw1, ok := mw.(*metaOpImp); ok {
		mw1.Client = ec.(*stream.ExtentClient)
	}

	v := &volume{
		mw:    mw,
		ec:    ec,
		owner: owner,
		name:  name,
	}

	return v, nil
}

func (v *volume) setAllocFunc(allocId func(ctx context.Context) (id uint64, err error)) {
	if mw1, ok := v.mw.(*metaOpImp); ok {
		mw1.allocId = allocId
	}
}

func (v *volume) NewInodeLock() sdk.InodeLockApi {
	uidByte, _ := uuid.New().MarshalBinary()
	m := md5.New()
	m.Write(uidByte)
	md5Val := hex.EncodeToString(m.Sum(nil))

	lk := &InodeLock{
		v:      v,
		id:     md5Val,
		status: 0,
	}
	return lk
}

func (v *volume) Info() *sdk.VolInfo {
	info := &sdk.VolInfo{
		Name: v.name,
	}
	return info
}

func (v *volume) Lookup(ctx context.Context, parentIno uint64, name string) (*sdk.DirInfo, error) {
	span := trace.SpanFromContextSafe(ctx)

	den, err := v.mw.LookupEx(parentIno, name)
	if err != nil {
		span.Errorf("look up file failed, parIno %d, name %s, err %s", parentIno, name, err.Error())
		return nil, syscallToErr(err)
	}

	return den, nil
}

func (v *volume) GetInode(ctx context.Context, ino uint64) (*proto.InodeInfo, error) {
	span := trace.SpanFromContextSafe(ctx)

	info, err := v.mw.InodeGet_ll(ino)
	if err != nil {
		span.Errorf("get inode info failed, ino %d, err %s", ino, err.Error())
		return nil, syscallToErr(err)
	}

	return info, nil
}

func (v *volume) BatchGetInodes(ctx context.Context, inos []uint64) ([]*proto.InodeInfo, error) {
	span := trace.SpanFromContextSafe(ctx)

	infos, err := v.mw.BatchInodeGetWith(inos)
	if err != nil {
		span.Errorf("batchGet Inodes failed, lenInodes: %d, err %s", len(inos), err.Error())
		return nil, syscallToErr(err)
	}

	return infos, nil
}

func (v *volume) Readdir(ctx context.Context, parIno uint64, marker string, count uint32) ([]sdk.DirInfo, error) {
	span := trace.SpanFromContextSafe(ctx)

	if count > 10000 {
		span.Errorf("count can't be over 10000, now %d", count)
		return nil, sdk.ErrBadRequest
	}

	if marker != "" {
		count++
	}

	dirs, err := v.mw.ReadDirLimit_ll(parIno, marker, uint64(count))
	if err != nil {
		span.Errorf("readdir failed, parentIno: %d, marker %s, count %s, err %s", parIno, marker, count, err.Error())
		return nil, syscallToErr(err)
	}

	cnt := len(dirs)
	if cnt == 0 {
		return []sdk.DirInfo{}, nil
	}

	if marker == "" {
		return dirs[:cnt], nil
	}

	if dirs[0].Name == marker && cnt == 1 {
		return []sdk.DirInfo{}, nil
	}

	if dirs[0].Name == marker {
		return dirs[1:], nil
	}

	// all dirs bigger than marker
	if cnt == int(count) {
		return dirs[:cnt-1], nil
	}
	return dirs, nil
}

func (v *volume) Rename(ctx context.Context, srcParIno, dstParIno uint64, srcName, destName string) error {
	span := trace.SpanFromContextSafe(ctx)
	if srcParIno == dstParIno && srcName == destName {
		span.Warnf("rename args can't be same for input and output, srcIno %d, dstIno %d, srcName %s, dstName %s",
			srcParIno, dstParIno, srcName, destName)
		return sdk.ErrBadRequest
	}

	err := v.mw.Rename_ll(srcParIno, srcName, dstParIno, destName, false)
	if err != nil {
		span.Errorf("rename file failed, srcIno %d, srcName %s, dstIno %d, dstName %s, err %s",
			srcParIno, srcName, dstParIno, destName, err.Error())
		return syscallToErr(err)
	}
	return nil
}

func (v *volume) ReadDirAll(ctx context.Context, ino uint64) ([]sdk.DirInfo, error) {
	span := trace.SpanFromContextSafe(ctx)
	marker := ""
	count := 1000
	total := make([]sdk.DirInfo, 0)

	for {
		dirs, err := v.Readdir(ctx, ino, marker, uint32(count))
		if err != nil {
			span.Errorf("readdir failed, ino %d, marker %s, count %s", ino, marker, count)
			return nil, syscallToErr(err)
		}

		total = append(total, dirs...)
		if len(dirs) < count {
			return total, nil
		}
		marker = dirs[len(dirs)-1].Name
	}
}

func (v *volume) getStatByIno(ctx context.Context, ino uint64) (info *sdk.StatFs, err error) {
	span := trace.SpanFromContextSafe(ctx)
	info = new(sdk.StatFs)
	entArr, err := v.ReadDirAll(ctx, ino)
	if err != nil {
		span.Errorf("readirAll failed, ino %d, err %s", ino, err.Error())
		return nil, syscallToErr(err)
	}

	if len(entArr) == 0 {
		return &sdk.StatFs{}, nil
	}

	var files, dirs int
	inoArr := make([]uint64, 0, len(entArr))
	for _, e := range entArr {
		if proto.IsDir(e.Type) {
			subStat, err1 := v.getStatByIno(ctx, e.Inode)
			if err1 != nil {
				return nil, syscallToErr(err1)
			}
			info.Add(subStat)
			dirs++
			continue
		}

		files++
		if proto.IsRegular(e.Type) {
			inoArr = append(inoArr, e.Inode)
		}
	}
	info.Files = files

	infos, err := v.BatchGetInodes(ctx, inoArr)
	if err != nil {
		span.Errorf("batch getInodes failed, err %s", err.Error())
		return nil, syscallToErr(err)
	}

	for _, e := range infos {
		info.Size += int(e.Size)
	}

	return info, nil
}

func (v *volume) StatFs(ctx context.Context, ino uint64) (*sdk.StatFs, error) {
	return v.getStatByIno(ctx, ino)
}

func (v *volume) SetAttr(ctx context.Context, req *sdk.SetAttrReq) error {
	span := trace.SpanFromContextSafe(ctx)

	err := v.mw.Setattr(req.Ino, req.Flag, req.Mode, req.Uid, req.Gid, int64(req.Atime), int64(req.Mtime))
	if err != nil {
		span.Errorf("setAttr failed, ino %d, flag %d")
		return syscallToErr(err)
	}

	return nil
}

func (v *volume) SetXAttr(ctx context.Context, ino uint64, key string, val string) error {
	span := trace.SpanFromContextSafe(ctx)

	err := v.mw.XAttrSet_ll(ino, []byte(key), []byte(val))
	if err != nil {
		span.Errorf("xSetAttr failed, ino %d, key %s, err %s", ino, key, err.Error())
		return syscallToErr(err)
	}

	return nil
}

func (v *volume) BatchSetXAttr(ctx context.Context, ino uint64, attrs map[string]string) error {
	span := trace.SpanFromContextSafe(ctx)

	err := v.mw.BatchSetXAttr_ll(ino, attrs)
	if err != nil {
		span.Errorf("batch setXAttr failed, ino %d, err %s", ino, err.Error())
		return syscallToErr(err)
	}

	return nil
}

func (v *volume) GetXAttr(ctx context.Context, ino uint64, key string) (string, error) {
	span := trace.SpanFromContextSafe(ctx)

	val, err := v.mw.XAttrGet_ll(ino, key)
	if err != nil {
		span.Errorf("XAttrGet failed, ino %d, key %s, err %s", ino, key, err.Error())
		return "", syscallToErr(err)
	}

	return val.XAttrs[key], nil
}

func (v *volume) ListXAttr(ctx context.Context, ino uint64) ([]string, error) {
	span := trace.SpanFromContextSafe(ctx)

	val, err := v.mw.XAttrsList_ll(ino)
	if err != nil {
		span.Errorf("ListXAttr failed, ino %d, err %s", ino, err.Error())
		return nil, syscallToErr(err)
	}

	return val, nil
}

func (v *volume) GetXAttrMap(ctx context.Context, ino uint64) (map[string]string, error) {
	span := trace.SpanFromContextSafe(ctx)

	val, err := v.mw.XAttrGetAll_ll(ino)
	if err != nil {
		span.Errorf("XAttrGetAll failed, ino %d, err %s", ino, err.Error())
		return nil, syscallToErr(err)
	}

	return val.XAttrs, nil
}

func (v *volume) DeleteXAttr(ctx context.Context, ino uint64, key string) error {
	span := trace.SpanFromContextSafe(ctx)

	err := v.mw.XAttrDel_ll(ino, key)
	if err != nil {
		span.Errorf("DeleteXAttr failed, ino %d, key %s, err %s", ino, key, err.Error())
		return syscallToErr(err)
	}

	return nil
}

func (v *volume) BatchDeleteXAttr(ctx context.Context, ino uint64, keys []string) error {
	span := trace.SpanFromContextSafe(ctx)
	err := v.mw.XBatchDelAttr_ll(ino, keys)
	if err != nil {
		span.Errorf("batchDelXAttr failed, ino %d, err %s", ctx, err.Error())
		return syscallToErr(err)
	}

	return nil
}

const (
	defaultDirMod   = os.ModeDir | 0o755
	defaultFileMode = 0o644
)

func (v *volume) Mkdir(ctx context.Context, parIno uint64, name string) (*sdk.InodeInfo, uint64, error) {
	span := trace.SpanFromContextSafe(ctx)

	info, id, err := v.mw.CreateFileEx(ctx, parIno, name, uint32(defaultDirMod))
	if err != nil {
		span.Errorf("create dir failed, parIno %d, name %s, err %s", parIno, name, err.Error())
		return nil, 0, syscallToErr(err)
	}

	return info, id, err
}

func (v *volume) CreateFile(ctx context.Context, parentIno uint64, name string) (*sdk.InodeInfo, uint64, error) {
	ifo, id, err := v.mw.CreateFileEx(ctx, parentIno, name, uint32(defaultFileMode))
	if err != nil {
		return nil, 0, syscallToErr(err)
	}
	return ifo, id, nil
}

func (v *volume) Delete(ctx context.Context, parIno uint64, name string, isDir bool) error {
	span := trace.SpanFromContextSafe(ctx)

	ifo, err := v.mw.Delete_ll(parIno, name, isDir)
	if err != nil {
		span.Errorf("delete file failed, ino %d, name %s, dir %v, err %s", parIno, name, isDir, err.Error())
		return syscallToErr(err)
	}

	if isDir || ifo == nil {
		return nil
	}

	err = v.mw.Evict(ifo.Inode)
	if err != nil {
		span.Errorf("evict file failed, ino %d, name %s, dir %v, err %s", parIno, name, isDir, err.Error())
		return syscallToErr(err)
	}

	return nil
}

func (v *volume) UploadFile(ctx context.Context, req *sdk.UploadFileReq) (*sdk.InodeInfo, uint64, error) {
	span := trace.SpanFromContextSafe(ctx)
	var oldIno uint64
	if req.OldFileId != 0 {
		// oldIno, mode, err := v.mw.Lookup_ll(req.ParIno, req.Name)
		den, err := v.mw.LookupEx(req.ParIno, req.Name)
		if err != nil && err != syscall.ENOENT {
			span.Warnf("lookup file failed, ino %d, name %s, err %s", req.ParIno, req.Name, err.Error())
			return nil, 0, syscallToErr(err)
		}

		if den == nil || den.FileId != req.OldFileId || proto.IsDir(den.Type) {
			span.Warnf("target file already exist but conflict, den %v, reqOld %d",
				den, req.OldFileId)
			return nil, 0, sdk.ErrConflict
		}
		oldIno = den.Inode
	}

	tmpInoInfo, err := v.mw.CreateInode(defaultFileMode)
	if err != nil {
		span.Errorf("create inode failed, err %s", err.Error())
		return nil, 0, syscallToErr(err)
	}

	tmpIno := tmpInoInfo.Inode

	defer func() {
		// remove inode once error not nil
		if err != nil {
			_, err1 := v.mw.InodeUnlink_ll(tmpIno)
			if err1 != nil {
				span.Errorf("unlink inode failed, ino %d, err %s", tmpIno, err1.Error())
			}

			err1 = v.mw.Evict(tmpIno)
			if err1 != nil {
				span.Errorf("evict inode failed, ino %d, err %s", tmpIno, err1.Error())
			}
		}
	}()

	err = v.ec.OpenStream(tmpIno)
	if err != nil {
		span.Errorf("open stream failed, ino %d, err %s", tmpIno, err.Error())
		return nil, 0, syscallToErr(err)
	}

	defer func() {
		err1 := v.ec.CloseStream(tmpIno)
		if err1 != nil {
			span.Warnf("close stream failed, ino %d, err %s", tmpIno, err1.Error())
		}
	}()

	_, err = v.writeAt(ctx, tmpIno, 0, -1, req.Body)
	if err != nil {
		span.Errorf("writeAt file failed, ino %s, err %s", tmpIno, err.Error())
		return nil, 0, err
	}

	if err = v.ec.Flush(tmpIno); err != nil {
		span.Errorf("flush file failed, ino %d, err %s", tmpIno, err.Error())
		return nil, 0, syscallToErr(err)
	}

	var finalIno *proto.InodeInfo
	if finalIno, err = v.mw.InodeGet_ll(tmpIno); err != nil {
		span.Errorf("get ino info failed, ino %d, err %s", tmpIno, err.Error())
		return nil, 0, syscallToErr(err)
	}

	if cb := req.Callback; cb != nil {
		if err = cb(); err != nil {
			span.Errorf("callback, ino %d, err %s", tmpIno, err.Error())
			return nil, 0, syscallToErr(err)
		}
	}

	if len(req.Extend) != 0 {
		err = v.mw.BatchSetXAttr_ll(tmpIno, req.Extend)
		if err != nil {
			span.Errorf("setXAttr failed, ino %d, err %s", tmpIno, err.Error())
			return nil, 0, syscallToErr(err)
		}
	}

	dirReq := &sdk.CreateDentryReq{
		ParentId: req.ParIno,
		Name:     req.Name,
		Inode:    tmpIno,
		OldIno:   oldIno,
		Mode:     defaultFileMode,
	}

	id, err := v.mw.CreateDentryEx(ctx, dirReq)
	if err != nil {
		span.Errorf("dentryCreateEx failed, parent %d, name %s, ino %d", req.ParIno, req.Name, req.OldFileId)
		return nil, 0, syscallToErr(err)
	}

	// return sdk.NewInode(finalIno, fileId), nil
	return finalIno, id, nil
}

func (v *volume) writeAt(ctx context.Context, ino uint64, off, size int, body io.Reader) (s int, err error) {
	span := trace.SpanFromContextSafe(ctx)

	if size < 0 {
		size = math.MaxInt64
	}

	total := 0
	wn := 0

	buf := make([]byte, util.BlockSize)
	for {
		n, err := body.Read(buf)
		if err != nil && err != io.EOF {
			span.Errorf("read file from body failed, err %s", err.Error())
			return 0, err
		}

		if n > 0 {
			wn, err = v.ec.Write(ino, off, buf[:n], 0)
			if err != nil {
				span.Errorf("write file failed, ino %d, off %d, err %s", ino, off, err.Error())
				return 0, syscallToErr(err)
			}
			off += wn
		}

		total += n
		if total >= size {
			return total, nil
		}

		if err == io.EOF {
			return total, nil
		}
	}
}

func (v *volume) WriteFile(ctx context.Context, ino, off, size uint64, body io.Reader) error {
	span := trace.SpanFromContextSafe(ctx)

	if err := v.ec.OpenStream(ino); err != nil {
		span.Errorf("open stream failed, ino %d, off %s, err %s", ino, off, err.Error())
		return syscallToErr(err)
	}

	defer func() {
		closeErr := v.ec.CloseStream(ino)
		if closeErr != nil {
			span.Errorf("close stream failed, ino %s, err %s", ino, closeErr.Error())
		}
	}()

	_, err := v.writeAt(ctx, ino, int(off), int(size), body)
	return err
}

func (v *volume) ReadFile(ctx context.Context, ino, off uint64, data []byte) (n int, err error) {
	span := trace.SpanFromContextSafe(ctx)

	if err = v.ec.OpenStream(ino); err != nil {
		span.Errorf("open stream failed, ino %d, off %s, err %s", ino, off, err.Error())
		return 0, syscallToErr(err)
	}

	defer func() {
		closeErr := v.ec.CloseStream(ino)
		if closeErr != nil {
			span.Errorf("close stream failed, ino %s, err %s", ino, closeErr.Error())
		}
	}()

	n, err = v.ec.Read(ino, data, int(off), len(data))
	if err != nil && err != io.EOF {
		span.Errorf("read file failed, ino %d, off %d, err %s", ino, off, err.Error())
		return 0, syscallToErr(err)
	}

	return n, nil
}

func (v *volume) InitMultiPart(ctx context.Context, filepath string, extend map[string]string) (string, error) {
	if !startWithSlash(filepath) {
		return "", sdk.ErrBadRequest
	}

	span := trace.SpanFromContextSafe(ctx)

	if _, name := path.Split(filepath); name == "" {
		span.Warnf("path is illegal, path %s", filepath)
		return "", sdk.ErrBadRequest
	}

	uploadId, err := v.mw.InitMultipart_ll(filepath, extend)
	if err != nil {
		span.Errorf("init multipart failed, path %s, err %s", filepath, err.Error())
		return "", syscallToErr(err)
	}

	return uploadId, nil
}

func (v *volume) UploadMultiPart(ctx context.Context, filepath, uploadId string, partNum uint16, read io.Reader) (part *sdk.Part, err error) {
	span := trace.SpanFromContextSafe(ctx)
	if !startWithSlash(filepath) {
		span.Warnf("input file path is not illegal, path %s", filepath)
		err = sdk.ErrBadRequest
		return
	}

	tmpInfo, err := v.mw.CreateInode(defaultFileMode)
	if err != nil {
		span.Errorf("create ino failed", err.Error())
		return nil, syscallToErr(err)
	}

	tmpIno := tmpInfo.Inode
	defer func() {
		if err != nil {
			_, err1 := v.mw.InodeUnlink_ll(tmpIno)
			if err1 != nil {
				span.Errorf("unlink ino failed, ino %d, err %s", tmpIno, err1.Error())
			}

			err1 = v.mw.Evict(tmpIno)
			if err1 != nil {
				span.Errorf("evict ino failed, ino %d, err %s", tmpIno, err1.Error())
			}
		}
	}()

	if err = v.ec.OpenStream(tmpIno); err != nil {
		span.Errorf("openStream failed, ino %d, err %s", tmpIno, err.Error())
		return nil, syscallToErr(err)
	}

	defer func() {
		if closeErr := v.ec.CloseStream(tmpIno); closeErr != nil {
			span.Errorf("closeStream failed, ino %d, err %s", tmpIno, err.Error())
		}
	}()

	h := md5.New()
	tee := io.TeeReader(read, h)

	size, err := v.writeAt(ctx, tmpIno, 0, -1, tee)
	if err != nil {
		span.Errorf("execute writeAt failed, ino %d, err %s", tmpIno, err.Error())
		return nil, err
	}

	md5Val := hex.EncodeToString(h.Sum(nil))

	part = &sdk.Part{
		Size: uint64(size),
		ID:   partNum,
		MD5:  md5Val,
	}

	if err = v.ec.Flush(tmpIno); err != nil {
		span.Errorf("execute flush failed, ino %d, err %s", tmpIno, err.Error())
		err = syscallToErr(err)
		return
	}

	_, _, err = v.mw.AddMultipartPart_ll(filepath, uploadId, partNum, uint64(size), md5Val, tmpInfo)
	if err != nil {
		span.Errorf("add multi part failed, path %s, uploadId %s, num %d, ino %d, err %s",
			filepath, uploadId, partNum, tmpIno, err.Error())
		err = syscallToErr(err)
		return
	}

	return
}

func (v *volume) ListMultiPart(ctx context.Context, filepath, uploadId string, count, marker uint64) (parts []*sdk.Part, next uint64, isTruncated bool, err error) {
	if !startWithSlash(filepath) {
		err = sdk.ErrBadRequest
		return
	}

	span := trace.SpanFromContextSafe(ctx)

	info, err := v.mw.GetMultipart_ll(filepath, uploadId)
	if err != nil {
		span.Errorf("get multipart failed, path %s, id %s, err %s", filepath, uploadId, err.Error())
		err = syscallToErr(err)
		return
	}

	sessParts := info.Parts
	total := len(sessParts)

	if uint64(total) < marker {
		span.Warnf("invalid marker, large than total parts cnt, marker %d, total %d", marker, total)
		err = sdk.ErrBadRequest
		return
	}

	next = marker + count
	isTruncated = true

	if uint64(total)-marker <= count {
		count = uint64(total) - marker
		next = 0
		isTruncated = false
	}

	parts = sessParts[marker : marker+count]

	return parts, next, isTruncated, nil
}

func startWithSlash(filepath string) bool {
	return strings.HasPrefix(filepath, "/")
}

func (v *volume) AbortMultiPart(ctx context.Context, filepath, uploadId string) error {
	if !startWithSlash(filepath) {
		return sdk.ErrBadRequest
	}

	span := trace.SpanFromContextSafe(ctx)

	multipartInfo, err := v.mw.GetMultipart_ll(filepath, uploadId)
	if err != nil {
		span.Errorf("get multipart failed, path %s, id %s, err %s", filepath, uploadId, err.Error())
		return syscallToErr(err)
	}

	for _, part := range multipartInfo.Parts {
		if _, err = v.mw.InodeUnlink_ll(part.Inode); err != nil {
			span.Errorf("execute inode unlink failed, ino %d, err %s", part.Inode, err.Error())
		}

		err = v.mw.Evict(part.Inode)
		if err != nil {
			span.Errorf("execute inode evict failed, ino %d, err %s", part.Inode, err.Error())
		}
	}

	if err = v.mw.RemoveMultipart_ll(filepath, uploadId); err != nil {
		span.Errorf("remove multipart failed, filepath %s, uploadId %s, err %s", filepath, uploadId, err.Error())
		return syscallToErr(err)
	}

	return nil
}

func (v *volume) CompleteMultiPart(ctx context.Context, filepath, uploadId string, oldFileId uint64, partsArg []sdk.Part) (ifo *sdk.InodeInfo, fileId uint64, err error) {
	if !startWithSlash(filepath) {
		return nil, 0, sdk.ErrBadRequest
	}

	span := trace.SpanFromContextSafe(ctx)

	for idx, part := range partsArg {
		if part.ID != uint16(idx+1) {
			return nil, 0, sdk.ErrInvalidPartOrder
		}
	}

	info, err := v.mw.GetMultipart_ll(filepath, uploadId)
	if err != nil {
		span.Errorf("get multipart info failed, path %s, uploadId %s, err %s", filepath, uploadId, err.Error())
		return nil, 0, syscallToErr(err)
	}

	if len(partsArg) != len(info.Parts) {
		span.Errorf("request part is not valid, path %s, uploadId %s", filepath, uploadId)
		return nil, 0, sdk.ErrInvalidPart
	}

	partArr := make([]*sdk.Part, 0, len(partsArg))
	for idx, part := range info.Parts {
		tmpPart := partsArg[idx]
		if tmpPart.MD5 != part.MD5 {
			span.Errorf("request part md5 is invalid, path %s, uploadId %s, num %d, md5 %s", filepath, uploadId, tmpPart.ID, tmpPart.MD5)
			return nil, 0, sdk.ErrInvalidPart
		}
		partArr = append(partArr, part)
	}

	completeInfo, err := v.mw.CreateInode(defaultFileMode)
	if err != nil {
		span.Errorf("inode create failed, path %s, err %s", filepath, err.Error())
		return nil, 0, syscallToErr(err)
	}
	cIno := completeInfo.Inode

	defer func() {
		if err != nil {
			if deleteErr := v.mw.InodeDelete_ll(cIno); deleteErr != nil {
				span.Errorf("delete ino failed, ino %d, err %s", cIno, deleteErr.Error())
			}
		}
	}()

	totalExtents := make([]proto.ExtentKey, 0)
	fileOffset := uint64(0)
	size := uint64(0)
	var eks []proto.ExtentKey

	for _, part := range partArr {
		_, _, eks, err = v.mw.GetExtents(part.Inode)
		if err != nil {
			span.Errorf("get part extent failed, ino %d, err %s", part.Inode, err.Error())
			return nil, 0, syscallToErr(err)
		}

		for _, ek := range eks {
			ek.FileOffset = fileOffset
			fileOffset += uint64(ek.Size)
			totalExtents = append(totalExtents, ek)
		}
		size += part.Size
	}

	err = v.mw.AppendExtentKeys(cIno, totalExtents)
	if err != nil {
		span.Errorf("append ino to complete ino failed, ino %d, err %s", cIno, err.Error())
		return nil, 0, syscallToErr(err)
	}

	dir, name := path.Split(filepath)
	parIno, err := v.mkdirByPath(ctx, dir)
	if err != nil {
		span.Errorf("mkdir dir failed, dir %s, err %s", dir, err.Error())
		return nil, 0, syscallToErr(err)
	}

	err = v.mw.RemoveMultipart_ll(filepath, uploadId)
	if err != nil {
		span.Errorf("remove multipart failed, path %s, id %s, err %s", filepath, uploadId, err.Error())
		return nil, 0, syscallToErr(err)
	}

	for _, part := range partArr {
		err1 := v.mw.InodeDelete_ll(part.Inode)
		if err1 != nil {
			span.Errorf("delete part ino failed, ino %d, err %s", part.Inode, err1.Error())
		}
	}

	extend := info.Extend
	attrs := make(map[string]string)
	if len(extend) > 0 {
		for key, value := range extend {
			attrs[key] = value
		}

		if err = v.mw.BatchSetXAttr_ll(cIno, attrs); err != nil {
			span.Errorf("batch setXAttr failed, ino %d", cIno, err.Error())
			return nil, 0, err
		}
	}

	dirReq := &sdk.CreateDentryReq{
		ParentId: parIno,
		Name:     name,
		Inode:    cIno,
		OldIno:   oldFileId,
		Mode:     defaultFileMode,
	}

	filId, err := v.mw.CreateDentryEx(ctx, dirReq)
	if err != nil {
		span.Errorf("final create dentry failed, parIno %d, name %s, childIno %d, err %s",
			parIno, name, cIno, err.Error())
		return nil, 0, syscallToErr(err)
	}

	var newIfo *proto.InodeInfo
	newIfo, err = v.mw.InodeGet_ll(completeInfo.Inode)
	if err != nil {
		span.Errorf("final get inode ifo failed, ino %d, err %s", completeInfo.Inode, err.Error())
		return nil, 0, syscallToErr(err)
	}

	return newIfo, filId, nil
}

func (v *volume) mkdirByPath(ctx context.Context, dir string) (ino uint64, err error) {
	span := trace.SpanFromContextSafe(ctx)

	parIno := proto.RootIno
	dir = strings.TrimSpace(dir)
	var childIno uint64
	var childMod uint32
	var info *sdk.InodeInfo

	defer func() {
		ino = parIno
	}()

	dirs := strings.Split(dir, "/")
	for _, name := range dirs {
		if name == "" {
			continue
		}

		childDen, err := v.mw.LookupEx(parIno, name)
		if err != nil && err != syscall.ENOENT {
			span.Errorf("lookup file failed, ino %d, name %s, err %s", parIno, name, err.Error())
			return 0, err
		}

		if err == syscall.ENOENT {
			info, _, err = v.mw.CreateFileEx(ctx, parIno, name, uint32(defaultDirMod))
			if err != nil && err == syscall.EEXIST {
				existDen, e := v.mw.LookupEx(parIno, name)
				if e != nil {
					span.Errorf("lookup exist ino failed, ino %d, name %s, err %s", parIno, name, err.Error())
					return 0, e
				}

				if proto.IsDir(existDen.Type) {
					parIno, err = existDen.Inode, nil
					continue
				}
			}
			if err != nil {
				span.Errorf("create dir failed, parent ino %d, name %s, err %s", parIno, name, err.Error())
				return 0, err
			}
			childIno, childMod = info.Inode, info.Mode
		} else {
			childIno, childMod = childDen.Inode, childDen.Type
		}

		if !proto.IsDir(childMod) {
			span.Errorf("target file exist but not dir, ino %d, name %v", childIno, name)
			err = syscall.EINVAL
			return 0, err
		}

		parIno = childIno
	}

	return
}

func syscallToErr(err error) *sdk.Error {
	if err == nil {
		return nil
	}

	if newErr, ok := err.(*sdk.Error); ok {
		return newErr
	}

	switch err {
	case syscall.EAGAIN:
		return sdk.ErrRetryAgain
	case syscall.EEXIST:
		return sdk.ErrExist
	case syscall.ENOENT:
		return sdk.ErrNotFound
	case syscall.ENOTEMPTY:
		return sdk.ErrNotEmpty
	case syscall.ENOMEM:
		return sdk.ErrFull
	case syscall.EINVAL:
		return sdk.ErrBadRequest
	case syscall.EPERM:
		return sdk.ErrForbidden
	case syscall.ENOTSUP:
		return sdk.ErrConflict
	case syscall.EBADF:
		return sdk.ErrBadFile
	default:
		return sdk.ErrInternalServerError
	}
}
