package sdk

import (
	"context"
	"io"

	"github.com/cubefs/cubefs/proto"
)

type VolInfo struct {
	Name   string
	Weight int
}

type StatFs struct {
	Size int
}

type InodeInfo = proto.InodeInfo

type DirInfo = proto.Dentry

type Part = proto.MultipartPartInfo

type UploadFileReq struct {
	Ctx    context.Context
	ParIno uint64
	Name   string
	OldIno uint64
	Extend map[string]string
	Body   io.Reader
}

type ClusterInfo struct {
	Cid  string
	Addr string
}
