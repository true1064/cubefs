package sdk

import (
	"io"

	"github.com/cubefs/cubefs/proto"
)

type VolInfo struct {
	Name   string
	Weight int
}

type StatFs struct {
	Size  int
	Files int
}

func (sf *StatFs) Add(sub *StatFs) {
	sf.Size += sub.Size
	sf.Files += sub.Files
}

//type InodeInfo = proto.InodeInfo

type InodeInfo struct {
	*proto.InodeInfo
	FileId uint64
}

func NewInode(inode *proto.InodeInfo, fileId uint64) *InodeInfo {
	return &InodeInfo{
		InodeInfo: inode,
		FileId:    fileId,
	}
}

type DirInfo = proto.Dentry

type Part = proto.MultipartPartInfo

type UploadFileReq struct {
	ParIno    uint64
	Name      string
	OldFileId uint64
	Extend    map[string]string
	Body      io.Reader
	Callback  func() error
}

type SetAttrReq struct {
	Ino   uint64
	Flag  uint32 // valid=>(proto.AttrMode|proto.AttrUid|...)
	Mode  uint32
	Uid   uint32
	Gid   uint32
	Atime uint64 // seconds
	Mtime uint64
}

type ClusterInfo struct {
	Cid  string
	Addr string
}

type CreateDentryReq struct {
	ParentId uint64
	Inode    uint64
	Name     string
	Mode     uint32
	OldIno   uint64
	FileId   uint64
}
