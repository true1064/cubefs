package sdk

import (
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

type Part struct {
	PartNum      uint16
	MD5          string
	LastModified uint64
	Size         uint64
}
