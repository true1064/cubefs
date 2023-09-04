package proto

import (
	"bytes"
	"fmt"
	"time"
)

type SnapshotMode uint8

const (
	_ SnapshotMode = iota
	ModeVol
	ModeDir
)

type SnapshotVerDelTaskRequest struct {
	MasterAddr string
	LcNodeAddr string
	Task       *SnapshotVerDelTask
}

type SnapshotVerDelTask struct {
	Mode           SnapshotMode
	Id             string
	VolName        string
	UpdateTime     int64
	VolVersionInfo *VersionInfo
	DirVersionInfo *DirVersionInfoTask
}

type SnapshotVerDelTaskResponse struct {
	ID                 string
	StartTime          *time.Time
	EndTime            *time.Time
	Done               bool
	Status             uint8
	Result             string
	SnapshotVerDelTask *SnapshotVerDelTask
	SnapshotStatistics
}

type SnapshotStatistics struct {
	VolName         string
	VerSeq          uint64
	TotalInodeNum   int64
	FileNum         int64
	DirNum          int64
	ErrorSkippedNum int64
}

type DirVersionInfoTask struct {
	MetaPartitionId uint64
	DirIno          uint64
	DelVer          DelVer
}

type DelVer struct {
	DelVel uint64         // the snapshot version to delete
	Vers   []*VersionInfo //Info of all snapshots of a directory
}

func (d *DelVer) Newest() bool {
	if len(d.Vers) == 0 {
		return false
	}

	lastV := d.Vers[len(d.Vers)-1]
	if lastV.Ver == d.DelVel {
		return true
	}

	return false
}

func (d *DelVer) String() string {
	buf := bytes.NewBuffer(make([]byte, 8))
	buf.WriteString(fmt.Sprintf("[ver %d", d.DelVel))

	if len(d.Vers) == 0 {
		buf.WriteString("]")
		return buf.String()
	}

	buf.WriteString(", vers: ")
	for _, v := range d.Vers {
		buf.WriteString(fmt.Sprintf("[v %d, status %d, del %d]", v.Ver, v.Status, v.DelTime))
	}

	buf.WriteString("]")
	return buf.String()
}

type DelDirVersionInfo struct {
	DirIno     uint64 // inodeId of the directory which has versions to delete
	SubRootIno uint64 // CFA-user's root directory
	DelVers    []DelVer
}

type MasterBatchDelDirVersionReq struct {
	Vol             string
	MetaPartitionId uint64
	DirInfos        []DelDirVersionInfo
}

type DirSnapshotVersionInfo struct {
	SnapVersion uint64
}

type CreateDirSnapShotReq struct {
	VolName     string                 `json:"vol"`
	PartitionID uint64                 `json:"pid"`
	Info        *CreateDirSnapShotInfo `json:"snapshot"`
}

type CreateDirSnapShotInfo struct {
	SnapshotDir   string `json:"snapshot_dir"`
	SnapshotInode uint64 `json:"snapshot_ino"`
	OutVer        string `json:"out_ver"`
	Ver           uint64 `json:"ver"`
	RootInode     uint64 `json:"rootInode"`
}
