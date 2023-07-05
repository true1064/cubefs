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

type InodeInfo = proto.InodeInfo

type DirInfo = proto.Dentry

type Part = proto.MultipartPartInfo

type UploadFileReq struct {
	ParIno   uint64
	Name     string
	OldIno   uint64
	Extend   map[string]string
	Body     io.Reader
	Callback func() error
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
