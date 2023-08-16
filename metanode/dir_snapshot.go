package metanode

import "github.com/cubefs/cubefs/proto"

type SnapshotVer struct {
	OutVer string
	proto.VersionInfo
}

type DirSnapshotItem struct {
	Dir           string
	SnapshotInode uint64 // key for item
	RootInode     uint64
	Vers          []SnapshotVer
}

func (d *DirSnapshotItem) Marshal() (data []byte, err error) {
	return
}
