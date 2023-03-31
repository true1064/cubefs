package impl

import (
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"sort"
	"strings"
	"syscall"

	"github.com/cubefs/cubefs/apinode/sdk"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/data/stream"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/cubefs/cubefs/util"
)

type volume struct {
	mw    sdk.MetaOp
	ec    sdk.DataOp
	name  string
	owner string
}

func newVolume(ctx context.Context, name, owner string, addrs []string) (sdk.IVolume, error) {
	metaCfg := &meta.MetaConfig{
		Volume:  name,
		Owner:   owner,
		Masters: addrs,
	}
	mw, err := meta.NewMetaWrapper(metaCfg)
	if err != nil {
		fmt.Printf("init meta wrapper failed, err %s", err.Error())
		return nil, sdk.ErrInternalServerError
	}

	ecCfg := &stream.ExtentConfig{
		Volume:  name,
		Masters: addrs,
	}
	ec, err := stream.NewExtentClient(ecCfg)
	if err != nil {
		fmt.Printf("init extent client failed, err %s", err.Error())
		return nil, sdk.ErrInternalServerError
	}

	v := &volume{
		mw: mw,
		ec: ec,
	}

	return v, nil
}

func (v *volume) updateMasterAddr(addr string) {
	// v.mw.UpdateMasterAddr(addr)
	// v.ec.UpdateMasterAddr(addr)
}

func (v *volume) Info() *sdk.VolInfo {
	info := &sdk.VolInfo{
		Name: v.name,
	}
	return info
}

func (v *volume) Lookup(ctx context.Context, parentIno uint64, name string) (*sdk.DirInfo, error) {
	ino, mode, err := v.mw.Lookup_ll(parentIno, name)
	if err != nil {
		// todo log
		return nil, syscallToErr(err)
	}

	dir := &sdk.DirInfo{
		Name:  name,
		Type:  mode,
		Inode: ino,
	}
	return dir, nil
}

func (v *volume) GetInode(ctx context.Context, ino uint64) (*sdk.InodeInfo, error) {
	info, err := v.mw.InodeGet_ll(ino)
	if err != nil {
		// todo log
		return nil, syscallToErr(err)
	}

	return info, nil
}

func (v *volume) BatchGetInodes(ctx context.Context, inos []uint64) ([]*proto.InodeInfo, error) {
	infos, err := v.mw.BatchInodeGetWith(inos)
	if err != nil {
		// todo log
		return nil, syscallToErr(err)
	}

	return infos, nil
}

func (v *volume) Readdir(ctx context.Context, parIno uint64, marker string, count uint32) ([]sdk.DirInfo, error) {
	dirs, err := v.mw.ReadDirLimit_ll(parIno, marker, uint64(count))
	if err != nil {
		// todo log
		return nil, syscallToErr(err)
	}

	return dirs, nil
}

func (v *volume) StatFs(ctx context.Context, ino uint64) (*sdk.StatFs, error) {
	// TODO implement me
	return nil, nil
}

func (v *volume) SetAttr(ctx context.Context, ino uint64, flag, mode, uid, gid uint32, atime, mtime uint64) error {
	err := v.mw.Setattr(ino, flag, mode, uid, gid, int64(atime), int64(mtime))
	if err != nil {
		// todo log
		return syscallToErr(err)
	}

	return nil
}

func (v *volume) SetXAttr(ctx context.Context, ino uint64, key string, val string) error {
	err := v.mw.XAttrSet_ll(ino, []byte(key), []byte(val))
	if err != nil {
		// todo log
		return syscallToErr(err)
	}

	return nil
}

func (v *volume) BatchSetXAttr(ctx context.Context, ino uint64, attrs map[string]string) error {
	err := v.mw.BatchSetXAttr_ll(ino, attrs)
	if err != nil {
		// todo log
		return syscallToErr(err)
	}

	return nil
}

func (v *volume) GetXAttr(ctx context.Context, ino uint64, key string) (string, error) {
	val, err := v.mw.XAttrGet_ll(ino, key)
	if err != nil {
		// todo log
		return "", syscallToErr(err)
	}

	return val.XAttrs[key], nil
}

func (v *volume) ListXAttr(ctx context.Context, ino uint64) ([]string, error) {
	val, err := v.mw.XAttrsList_ll(ino)
	if err != nil {
		// todo log
		return nil, syscallToErr(err)
	}

	return val, nil
}

func (v *volume) GetXAttrMap(ctx context.Context, ino uint64) (map[string]string, error) {
	val, err := v.mw.XAttrGetAll_ll(ino)
	if err != nil {
		// todo log
		return nil, syscallToErr(err)
	}

	return val.XAttrs, nil
}

func (v *volume) DeleteXAttr(ctx context.Context, ino uint64, key string) error {
	err := v.mw.XAttrDel_ll(ino, key)
	if err != nil {
		// todo log
		return syscallToErr(err)
	}
	return nil
}

func (v *volume) BatchDeleteXAttr(ctx context.Context, ino uint64, keys []string) error {
	// TODO implement me
	panic("implement me")
}

func (v *volume) Mkdir(ctx context.Context, parIno uint64, name string) (*sdk.InodeInfo, error) {
	fmod := os.ModeDir | 0o755
	info, err := v.mw.Create_ll(parIno, name, uint32(fmod), 0, 0, nil)
	if err != nil {
		// todo log
		return nil, syscallToErr(err)
	}

	return info, err
}

func (v *volume) CreateFile(ctx context.Context, parentIno uint64, name string) (*sdk.InodeInfo, error) {
	mode := 0o755
	info, err := v.mw.Create_ll(parentIno, name, uint32(mode), 0, 0, nil)
	if err != nil {
		// todo log
		return nil, syscallToErr(err)
	}

	return info, nil
}

func (v *volume) Delete(ctx context.Context, parIno uint64, name string, isDir bool) error {
	_, err := v.mw.Delete_ll(parIno, name, isDir)
	if err != nil {
		// todo log
		return syscallToErr(err)
	}
	return nil
}

func (v *volume) Rename(ctx context.Context, srcParIno, dstParIno uint64, srcName, destName string) error {
	err := v.mw.Rename_ll(srcParIno, srcName, dstParIno, destName, false)
	if err != nil {
		// todo log
		return syscallToErr(err)
	}

	return nil
}

const defaultFileMode = 0o644

func (v *volume) UploadFile(req *sdk.UploadFileReq) (*sdk.InodeInfo, error) {
	oldIno, mode, err := v.mw.Lookup_ll(req.ParIno, req.Name)
	if err != nil {
		// todo log
		return nil, syscallToErr(err)
	}

	if proto.IsDir(mode) || oldIno != req.OldIno {
		return nil, sdk.ErrConflict
	}

	tmpInoInfo, err := v.mw.InodeCreate_ll(defaultFileMode, 0, 0, nil)
	if err != nil {
		return nil, syscallToErr(err)
	}

	tmpIno := tmpInoInfo.Inode

	defer func() {
		if err != nil {
			_, err1 := v.mw.InodeUnlink_ll(tmpIno)
			if err1 != nil {
				// todo log
			}
			err1 = v.mw.Evict(tmpIno)
			if err1 != nil {
				// todo log
			}
		}
	}()

	err = v.ec.OpenStream(tmpIno)
	if err != nil {
		// ino
		return nil, syscallToErr(err)
	}

	defer func() {
		err1 := v.ec.CloseStream(tmpIno)
		if err1 != nil {
			// todo log
		}
	}()

	_, err = v.writeAt(req.Ctx, tmpIno, 0, -1, req.Body)
	if err != nil {
		return nil, err
	}

	if err = v.ec.Flush(tmpIno); err != nil {
		return nil, syscallToErr(err)
	}

	var finalIno *proto.InodeInfo
	if finalIno, err = v.mw.InodeGet_ll(tmpIno); err != nil {
		return nil, syscallToErr(err)
	}

	if len(req.Extend) != 0 {
		err = v.mw.BatchSetXAttr_ll(tmpIno, req.Extend)
		if err != nil {
			return nil, syscallToErr(err)
		}
	}

	err = v.mw.DentryCreateEx_ll(req.ParIno, req.Name, tmpIno, req.OldIno, defaultFileMode)
	if err != nil {
		return nil, syscallToErr(err)
	}

	return finalIno, nil
}

func (v *volume) writeAt(ctx context.Context, ino uint64, off, size int, body io.Reader) (s int, err error) {
	if size < 0 {
		size = math.MaxInt64
	}

	total := 0
	wn := 0

	buf := make([]byte, util.BlockSize)
	for {
		n, err := body.Read(buf)
		if err != nil && err != io.EOF {
			// todo log
			return 0, sdk.ErrBadRequest
		}

		if n > 0 {
			wn, err = v.ec.Write(ino, off, buf[:n], 0)
			if err != nil {
				// todo add log
				return 0, syscallToErr(err)
			}
			off += wn
		}

		total += n
		if total >= size {
			return 0, nil
		}

		if err == io.EOF {
			return 0, nil
		}
	}
}

func (v *volume) WriteFile(ctx context.Context, ino, off, size uint64, body io.Reader) error {
	_, err := v.writeAt(ctx, ino, int(off), int(size), body)
	return err
}

func (v *volume) ReadFile(ctx context.Context, ino, off uint64, data []byte) (n int, err error) {
	if err = v.ec.OpenStream(ino); err != nil {
		// todo log
		return 0, syscallToErr(err)
	}

	defer func() {
		closeErr := v.ec.CloseStream(ino)
		if closeErr != nil {
		}
	}()

	n, err = v.ec.Read(ino, data, int(off), len(data))
	if err != nil && err != io.EOF {
		return 0, syscallToErr(err)
	}

	return n, nil
}

func (v *volume) findInodeByPath(filepath string) (uint64, error) {
	ino, err := v.mw.LookupPath(filepath)
	if err != nil {
		return 0, syscallToErr(err)
	}

	return ino, nil
}

func (v *volume) InitMultiPart(ctx context.Context, path string, oldIno uint64, extend map[string]string) (string, error) {
	// try check whether ino of path is equal to oldIno.
	if oldIno != 0 {
		ino, err := v.findInodeByPath(path)
		if err != nil && err != sdk.ErrNotFound {
			return "", err
		}

		if ino != oldIno {
			return "", sdk.ErrConflict
		}
	}

	uploadId, err := v.mw.InitMultipart_ll(path, extend)
	if err != nil {
		return "", syscallToErr(err)
	}

	return uploadId, nil
}

func (v *volume) GetMultiExtend(ctx context.Context, path, uploadId string) (extend map[string]string, err error) {
	// TODO implement me
	panic("implement me")
}

func (v *volume) UploadMultiPart(ctx context.Context, filepath, uploadId string, partNum uint16, read io.Reader) (err error) {
	tmpInfo, err := v.mw.InodeCreate_ll(defaultFileMode, 0, 0, nil)
	if err != nil {
		return syscallToErr(err)
	}

	tmpIno := tmpInfo.Inode
	defer func() {
		if err != nil {
			_, err1 := v.mw.InodeUnlink_ll(tmpIno)
			if err1 != nil {
			}
			err1 = v.mw.Evict(tmpIno)
			if err1 != nil {
				//
			}
		}
	}()

	if err = v.ec.OpenStream(tmpIno); err != nil {
		return syscallToErr(err)
	}

	defer func() {
		if closeErr := v.ec.CloseStream(tmpIno); closeErr != nil {
			// todo log
		}
	}()

	size, err := v.writeAt(ctx, tmpIno, 0, -1, read)
	if err != nil {
		return err
	}

	if err = v.ec.Flush(tmpIno); err != nil {
		err = syscallToErr(err)
		return
	}

	err = v.mw.AddMultipartPart_ll(filepath, uploadId, partNum, uint64(size), "", tmpInfo)
	if err != nil {
		err = syscallToErr(err)
		return
	}

	return
}

func (v *volume) ListMultiPart(ctx context.Context, filepath, uploadId string, count, marker uint64) (parts []*sdk.Part, next uint64, isTruncated bool, err error) {
	info, err := v.mw.GetMultipart_ll(filepath, uploadId)
	if err != nil {
		// todo log
		err = syscallToErr(err)
		return
	}

	sessParts := info.Parts
	total := len(sessParts)

	next = marker + count
	isTruncated = true

	if uint64(total)-marker < count {
		count = uint64(total) - marker
		next = 0
		isTruncated = false
	}

	parts = sessParts[marker : marker+count]

	return parts, next, isTruncated, nil
}

func (v *volume) AbortMultiPart(ctx context.Context, filepath, uploadId string) error {
	multipartInfo, err := v.mw.GetMultipart_ll(filepath, uploadId)
	if err != nil {
		return syscallToErr(err)
	}

	for _, part := range multipartInfo.Parts {
		if _, err = v.mw.InodeUnlink_ll(part.Inode); err != nil {
			// log msg
		}

		err = v.mw.Evict(part.Inode)
		if err != nil {
			// log msg
		}
	}

	if err = v.mw.RemoveMultipart_ll(filepath, uploadId); err != nil {
		return syscallToErr(err)
	}

	return nil
}

func (v *volume) CompleteMultiPart(ctx context.Context, filepath, uploadId string, oldIno uint64, parts []sdk.Part) (err error) {
	sort.SliceStable(parts, func(i, j int) bool {
		return parts[i].ID < parts[j].ID
	})

	completeInfo, err := v.mw.InodeCreate_ll(defaultFileMode, 0, 0, nil)
	if err != nil {
		return syscallToErr(err)
	}
	cIno := completeInfo.Inode

	defer func() {
		if err != nil {
			if deleteErr := v.mw.InodeDelete_ll(cIno); deleteErr != nil {
				// log msg
			}
		}
	}()

	totalExtents := make([]proto.ExtentKey, 0)
	fileOffset := uint64(0)
	size := uint64(0)
	var eks []proto.ExtentKey

	for _, part := range parts {
		_, _, eks, err = v.mw.GetExtents(part.Inode)
		if err != nil {
			// todo log
			return syscallToErr(err)
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
	}

	dir, name := path.Split(filepath)
	parIno, err := v.mkdirByPath(dir)
	if err != nil {
		return syscallToErr(err)
	}

	err = v.mw.RemoveMultipart_ll(filepath, uploadId)
	if err != nil {
		return
	}

	for _, part := range parts {
		err = v.mw.InodeDelete_ll(part.Inode)
		if err != nil {
			// todo log
		}
	}

	err = v.mw.DentryCreate_ll(parIno, name, cIno, defaultFileMode)
	if err != nil {
	}

	return nil
}

func (v *volume) mkdirByPath(dir string) (ino uint64, err error) {
	parIno := proto.RootIno
	dir = strings.TrimSpace(dir)
	var childIno uint64
	var childMod uint32
	var info *proto.InodeInfo

	dirs := strings.Split(dir, "/")
	for _, name := range dirs {
		childIno, childMod, err = v.mw.Lookup_ll(parIno, name)
		if err != nil && err != syscall.ENOENT {
			return 0, err
		}
		if err == syscall.ENOENT {
			info, err = v.mw.Create_ll(parIno, name, defaultFileMode, 0, 0, nil)
			if err != nil && err == syscall.EEXIST {
				existIno, mode, e := v.mw.Lookup_ll(parIno, name)
				if e != nil {
					return 0, err
				}
				if proto.IsDir(mode) {
					ino, err = existIno, nil
					continue
				}
			}
			if err != nil {
				return 0, err
			}
			childIno, childMod = info.Inode, info.Mode
		}

		if !proto.IsDir(childMod) {
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
