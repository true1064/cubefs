package sdk

import (
	"context"
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/data/stream"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/cubefs/cubefs/util"
	"io"
	"math"
	"os"
	"syscall"
)

type IVolume interface {
	//Info get vol simple info
	Info() *VolInfo
	// Lookup from target parentDir ino, parentIno 0 means lookup from root
	Lookup(ctx context.Context, parentIno uint64, name string) (*DirInfo, error)
	GetInode(ctx context.Context, ino uint64) (*InodeInfo, error)
	// BatchGetInodes maybe cost much time
	BatchGetInodes(ctx context.Context, inos []uint64) ([]*InodeInfo, error)
	Readdir(ctx context.Context, parIno uint64, marker string, count uint32) ([]DirInfo, error)
	StatFs(ctx context.Context, ino uint64) (*StatFs, error)
	// SetAttr set file mode, uid, gid, atime, mtime, valid=>(proto.AttrMode, proto.AttrUid)
	SetAttr(ctx context.Context, ino uint64, flag, mode, uid, gid uint32, atime, mtime uint64) error

	SetXAttr(ctx context.Context, ino uint64, key string, val string) error
	BatchSetXAttr(ctx context.Context, ino uint64, attrs map[string]string) error
	GetXAttr(ctx context.Context, ino uint64, key string) (string, error)
	ListXAttr(ctx context.Context, ino uint64) ([]string, error)
	GetXAttrMap(ctx context.Context, ino uint64) (map[string]string, error)
	DeleteXAttr(ctx context.Context, ino uint64, key string) error
	BatchDeleteXAttr(ctx context.Context, ino uint64, keys []string) error

	//Mkdir path
	Mkdir(ctx context.Context, parIno uint64, name string) (*InodeInfo, error)
	CreateFile(ctx context.Context, parentIno uint64, name string) (*InodeInfo, error)
	// Delete dir should be empty before delete
	Delete(ctx context.Context, parIno uint64, name string, isDir bool) error
	Rename(ctx context.Context, srcParIno, dstParIno uint64, srcName, destName string) error

	// UploadFile file
	UploadFile(req *UploadFileReq) (*InodeInfo, error)
	WriteFile(ctx context.Context, ino, off, size uint64, body io.Reader) error
	// ReadFile read() will make a rpc request to server, if n less than len(data), it means no more data
	ReadFile(ctx context.Context, ino, off uint64, data []byte) (n int, err error)

	InitMultiPart(ctx context.Context, path string, oldIno uint64, extend map[string]string) (string, error)
	GetMultiExtend(ctx context.Context, path, uploadId string) (extend map[string]string, err error)
	UploadMultiPart(ctx context.Context, filepath, uploadId string, partNum uint16, read io.Reader) error
	ListMultiPart(ctx context.Context, filepath, uploadId string, count, marker uint64) (parts []*Part, next uint64, isTruncated bool, err error)
	AbortMultiPart(ctx context.Context, filepath, uploadId string) error
	CompleteMultiPart(ctx context.Context, filepath, uploadId string, oldIno uint64, parts []Part) error
}

type volume struct {
	mw    *meta.MetaWrapper
	ec    *stream.ExtentClient
	name  string
	owner string
}

func newVolume(ctx context.Context, name, owner string, addrs []string) (IVolume, error) {

	metaCfg := &meta.MetaConfig{
		Volume:  name,
		Owner:   owner,
		Masters: addrs,
	}
	mw, err := meta.NewMetaWrapper(metaCfg)
	if err != nil {
		fmt.Sprintf("init meta wrapper failed, err %s", err.Error())
		return nil, ErrInternalServerError
	}

	ecCfg := &stream.ExtentConfig{
		Volume:  name,
		Masters: addrs,
	}
	ec, err := stream.NewExtentClient(ecCfg)
	if err != nil {
		fmt.Sprintf("init extent client failed, err %s", err.Error())
		return nil, ErrInternalServerError
	}

	v := &volume{
		mw: mw,
		ec: ec,
	}

	return v, nil
}

func (v *volume) updateMasterAddr(addr string) {
	v.mw.UpdateMasterAddr(addr)
	v.ec.UpdateMasterAddr(addr)
}

func (v *volume) Info() *VolInfo {
	info := &VolInfo{
		Name: v.name,
	}
	return info
}

func (v *volume) Lookup(ctx context.Context, parentIno uint64, name string) (*DirInfo, error) {
	ino, mode, err := v.mw.Lookup_ll(parentIno, name)
	if err != nil {
		// todo log
		return nil, syscallToErr(err)
	}

	dir := &DirInfo{
		Name:  name,
		Type:  mode,
		Inode: ino,
	}
	return dir, nil
}

func (v *volume) GetInode(ctx context.Context, ino uint64) (*InodeInfo, error) {
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

func (v *volume) Readdir(ctx context.Context, parIno uint64, marker string, count uint32) ([]DirInfo, error) {
	dirs, err := v.mw.ReadDirLimit_ll(parIno, marker, uint64(count))
	if err != nil {
		// todo log
		return nil, syscallToErr(err)
	}

	return dirs, nil
}

func (v *volume) StatFs(ctx context.Context, ino uint64) (*StatFs, error) {
	//TODO implement me
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
	//TODO implement me
	panic("implement me")
}

func (v *volume) Mkdir(ctx context.Context, parIno uint64, name string) (*InodeInfo, error) {
	fmod := os.ModeDir | 0755
	info, err := v.mw.Create_ll(parIno, name, uint32(fmod), 0, 0, nil)
	if err != nil {
		// todo log
		return nil, syscallToErr(err)
	}

	return info, err
}

func (v *volume) CreateFile(ctx context.Context, parentIno uint64, name string) (*InodeInfo, error) {
	mode := 0755
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

type UploadFileReq struct {
	ctx    context.Context
	ParIno uint64
	Name   string
	OldIno uint64
	Extend map[string]string
	Body   io.Reader
}

const defaultFileMode = 0644

func (v *volume) UploadFile(req *UploadFileReq) (*InodeInfo, error) {
	oldIno, mode, err := v.mw.Lookup_ll(req.ParIno, req.Name)
	if err != nil {
		// todo log
		return nil, syscallToErr(err)
	}

	if proto.IsDir(mode) || oldIno != req.OldIno {
		return nil, ErrConflict
	}

	tmpInoInfo, err := v.mw.InodeCreate_ll(defaultFileMode, 0, 0, nil)
	if err != nil {
		return nil, syscallToErr(err)
	}

	tmpIno := tmpInoInfo.Inode

	defer func() {
		if err != nil {
			_, err := v.mw.InodeUnlink_ll(tmpIno)
			if err != nil {
				// todo log
			}
			err = v.mw.Evict(tmpIno)
			if err != nil {
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

	err = v.writeAt(req.ctx, tmpIno, 0, -1, req.Body)
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

func (v *volume) writeAt(ctx context.Context, ino uint64, off, size int, body io.Reader) error {
	if size < 0 {
		size = math.MaxInt64
	}

	buf := make([]byte, util.BlockSize)
	for {
		n, err := body.Read(buf)
		if err != nil && err != io.EOF {
			// todo log
			return ErrBadRequest
		}

		if n > 0 {
			wn, err := v.ec.Write(ino, off, buf[:n], 0)
			if err != nil {
				// todo add log
				return syscallToErr(err)
			}
			off += wn
		}

		if err == io.EOF {
			return nil
		}
	}
}

func (v *volume) WriteFile(ctx context.Context, ino, off, size uint64, body io.Reader) error {
	return v.writeAt(ctx, ino, int(off), int(size), body)
}

//type reader struct {
//	ino  uint64
//	off  int
//	size int
//
//	v *volume
//
//	once sync.Once
//}
//
//func (r *reader) Read(data []byte) (n int, err error) {
//	n, err = r.v.ec.Read(r.ino, data, r.off, len(data))
//	if err != nil {
//		return 0, syscallToErr(err)
//	}
//
//	r.off += n
//
//	return 0, nil
//}
//
//func (r *reader) Close() error {
//	var err error
//	r.once.Do(func() {
//		err = r.v.ec.CloseStream(r.ino)
//		if err != nil {
//			// todo log
//			err = syscallToErr(err)
//		}
//	})
//
//	return err
//}

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

func (v *volume) InitMultiPart(ctx context.Context, path string, oldIno uint64, extend map[string]string) (string, error) {
	//TODO implement me
	panic("implement me")
}

func (v *volume) GetMultiExtend(ctx context.Context, path, uploadId string) (extend map[string]string, err error) {
	//TODO implement me
	panic("implement me")
}

func (v *volume) UploadMultiPart(ctx context.Context, filepath, uploadId string, partNum uint16, read io.Reader) error {
	//TODO implement me
	panic("implement me")
}

func (v *volume) ListMultiPart(ctx context.Context, filepath, uploadId string, count, marker uint64) (parts []*Part, next uint64, isTruncated bool, err error) {
	//TODO implement me
	panic("implement me")
}

func (v *volume) AbortMultiPart(ctx context.Context, filepath, uploadId string) error {
	//TODO implement me
	panic("implement me")
}

func (v *volume) CompleteMultiPart(ctx context.Context, filepath, uploadId string, oldIno uint64, parts []Part) error {
	//TODO implement me
	panic("implement me")
}

func syscallToErr(err error) *Error {
	if err == nil {
		return nil
	}

	switch err {
	case syscall.EAGAIN:
		return ErrRetryAgain
	case syscall.EEXIST:
		return ErrExist
	case syscall.ENOENT:
		return ErrNotFound
	case syscall.ENOMEM:
		return ErrFull
	case syscall.EINVAL:
		return ErrBadRequest
	case syscall.EPERM:
		return ErrForbidden
	case syscall.ENOTSUP:
		return ErrConflict
	case syscall.EBADF:
		return ErrBadFile
	default:
		return ErrInternalServerError
	}
}
