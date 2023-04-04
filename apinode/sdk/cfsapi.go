package sdk

import "github.com/cubefs/cubefs/proto"

type DataOp interface {
	OpenStream(inode uint64) error
	Write(inode uint64, offset int, data []byte, flags int) (write int, err error)
	Read(inode uint64, data []byte, offset int, size int) (read int, err error)
	Flush(inode uint64) error
	CloseStream(inode uint64) error
}

type MetaOp interface {
	Create_ll(parentID uint64, name string, mode, uid, gid uint32, target []byte) (*proto.InodeInfo, error)
	InodeCreate_ll(mode, uid, gid uint32, target []byte) (*proto.InodeInfo, error)
	Delete_ll(parentID uint64, name string, isDir bool) (*proto.InodeInfo, error)
	Truncate(inode, size uint64) error
	InodeUnlink_ll(inode uint64) (*proto.InodeInfo, error)
	Evict(inode uint64) error
	DentryCreateEx_ll(parentID uint64, name string, inode, oldIno uint64, mode uint32) error
	Setattr(inode uint64, valid, mode, uid, gid uint32, atime, mtime int64) error
	InodeDelete_ll(inode uint64) error
	DentryCreate_ll(parentID uint64, name string, inode uint64, mode uint32) error
	LookupPath(subdir string) (uint64, error)
	ReadDirLimit_ll(parentID uint64, from string, limit uint64) ([]proto.Dentry, error)
	BatchInodeGetWith(inodes []uint64) (batchInfos []*proto.InodeInfo, err error)
	Lookup_ll(parentID uint64, name string) (inode uint64, mode uint32, err error)
	GetExtents(inode uint64) (gen uint64, size uint64, extents []proto.ExtentKey, err error)
	InodeGet_ll(inode uint64) (*proto.InodeInfo, error)
	ReadDir_ll(parentID uint64) ([]proto.Dentry, error)
	Link(parentID uint64, name string, ino uint64) (*proto.InodeInfo, error)
	Rename_ll(srcParentID uint64, srcName string, dstParentID uint64, dstName string, overwritten bool) (err error)
	AppendExtentKeys(inode uint64, eks []proto.ExtentKey) error
	BatchSetXAttr_ll(inode uint64, attrs map[string]string) error
	XAttrGetAll_ll(inode uint64) (*proto.XAttrInfo, error)
	XAttrSet_ll(inode uint64, name, value []byte) error
	XAttrGet_ll(inode uint64, name string) (*proto.XAttrInfo, error)
	XAttrDel_ll(inode uint64, name string) error
	XBatchDelAttr_ll(ino uint64, keys []string) error
	XAttrsList_ll(inode uint64) ([]string, error)
	InitMultipart_ll(path string, extend map[string]string) (multipartId string, err error)
	GetMultipart_ll(path, multipartId string) (info *proto.MultipartInfo, err error)
	AddMultipartPart_ll(path, multipartId string, partId uint16, size uint64, md5 string, inodeInfo *proto.InodeInfo) (err error)
	RemoveMultipart_ll(path, multipartID string) (err error)
	ListMultipart_ll(prefix, delimiter, keyMarker string, multipartIdMarker string, maxUploads uint64) (sessionResponse []*proto.MultipartInfo, err error)
}

type IMaster interface {
	AdminApi
}

type AdminApi interface {
	GetClusterIP() (cp *proto.ClusterIP, err error)
	ListVols(keywords string) (volsInfo []*proto.VolInfo, err error)
}
