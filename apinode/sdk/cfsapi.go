package sdk

import (
	"context"

	"github.com/cubefs/cubefs/proto"
)

type DataOp interface {
	OpenStream(inode uint64) error
	Write(inode uint64, offset int, data []byte, flags int) (write int, err error)
	Read(inode uint64, data []byte, offset int, size int) (read int, err error)
	Flush(inode uint64) error
	CloseStream(inode uint64) error
}

type MetaOp interface {
	CreateInode(mode uint32) (*proto.InodeInfo, error)
	CreateFileEx(ctx context.Context, parentID uint64, name string, mode uint32) (*InodeInfo, uint64, error)
	Delete_ll(parentID uint64, name string, isDir bool) (*proto.InodeInfo, error)
	Truncate(inode, size uint64) error
	InodeUnlink_ll(inode uint64) (*proto.InodeInfo, error)
	Evict(inode uint64) error
	CreateDentryEx(ctx context.Context, req *CreateDentryReq) (uint64, error)
	Setattr(inode uint64, valid, mode, uid, gid uint32, atime, mtime int64) error
	InodeDelete_ll(inode uint64) error
	LookupPath(subdir string) (uint64, error)
	ReadDirLimit_ll(parentID uint64, from string, limit uint64) ([]proto.Dentry, error)
	BatchInodeGetWith(inodes []uint64) (batchInfos []*proto.InodeInfo, err error)
	LookupEx(parentId uint64, name string) (den *proto.Dentry, err error)
	GetExtents(inode uint64) (gen uint64, size uint64, extents []proto.ExtentKey, err error)
	InodeGet_ll(inode uint64) (*proto.InodeInfo, error)
	ReadDir_ll(parentID uint64) ([]proto.Dentry, error)
	// Link(parentID uint64, name string, ino uint64) (*proto.InodeInfo, error)
	Rename_ll(srcParentID uint64, srcName string, dstParentID uint64, dstName string, overwritten bool) (err error)
	AppendExtentKeys(inode uint64, eks []proto.ExtentKey) error
	BatchSetXAttr_ll(inode uint64, attrs map[string]string) error
	XAttrGetAll_ll(inode uint64) (*proto.XAttrInfo, error)
	SetInodeLock_ll(inode uint64, req *proto.InodeLockReq) error
	XAttrSetEx_ll(inode uint64, name, value []byte, overWrite bool) error
	XAttrGet_ll(inode uint64, name string) (*proto.XAttrInfo, error)
	XAttrDel_ll(inode uint64, name string) error
	XBatchDelAttr_ll(ino uint64, keys []string) error
	XAttrsList_ll(inode uint64) ([]string, error)
	InitMultipart_ll(path string, extend map[string]string) (multipartId string, err error)
	GetMultipart_ll(path, multipartId string) (info *proto.MultipartInfo, err error)
	AddMultipartPart_ll(path, multipartId string, partId uint16, size uint64, md5 string, inodeInfo *proto.InodeInfo) (oldInode uint64, updated bool, err error)
	RemoveMultipart_ll(path, multipartID string) (err error)
	ListMultipart_ll(prefix, delimiter, keyMarker string, multipartIdMarker string, maxUploads uint64) (sessionResponse []*proto.MultipartInfo, err error)
}

type IMaster interface {
	MasterApi
}

type MasterApi interface {
	GetClusterIP() (cp *proto.ClusterIP, err error)
	ListVols(keywords string) (volsInfo []*proto.VolInfo, err error)
	AllocFileId() (info *proto.FileId, err error)
}
