package proto

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"
)

type SnapshotMode uint8

const (
	_ SnapshotMode = iota
	ModeVol
	ModeDir
)

const SnapshotMockVerName = ""

type SnapshotVerDelTaskRequest struct {
	MasterAddr string
	LcNodeAddr string
	Task       *SnapshotVerDelTask
}

type SnapshotVerDelTask struct {
	Mode           SnapshotMode
	Id             string
	VolName        string
	VolVersionInfo *VersionInfo
	DirVersionInfo *DirVersionInfoTask
}

type SnapshotVerDelTaskResponse struct {
	ID                 string
	LcNode             string
	StartTime          *time.Time
	EndTime            *time.Time
	UpdateTime         *time.Time
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
	DelVer uint64         // the snapshot version to delete
	Vers   []*VersionInfo //Info of all snapshots of a directory
}

func (d *DelVer) Newest() bool {
	if len(d.Vers) == 0 {
		return false
	}

	lastV := d.Vers[len(d.Vers)-1]
	if lastV.Ver == d.DelVer {
		return true
	}

	return false
}

func (d *DelVer) String() string {
	buf := bytes.NewBuffer(make([]byte, 0))
	buf.WriteString(fmt.Sprintf("[ver %d", d.DelVer))

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

type DirSnapshotInfo struct {
	SnapshotDir   string          `json:"snapshot_dir"`
	SnapshotInode uint64          `json:"snapshot_ino"`
	MaxVer        uint64          `json:"max_ver"`
	Deleted       bool            `json:"deleted"`
	Vers          []*ClientDirVer `json:"vers"`
}

func (d *DirSnapshotInfo) IsSnapshotInode(ino uint64) bool {
	return d.SnapshotInode == ino
}

func (d *DirSnapshotInfo) String() string {
	data, _ := json.Marshal(d)
	return string(data)
}

type DelDirVersionInfo struct {
	DirIno     uint64 // inodeId of the directory which has versions to delete
	SubRootIno uint64 // CFA-user's root directory
	DelVer     DelVer
}

func (d *DelDirVersionInfo) String() string {
	data, _ := json.Marshal(d)
	return string(data)
}

type MasterBatchDelDirVersionReq struct {
	Vol             string
	MetaPartitionId uint64
	DirInfos        []*DelDirVersionInfo
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
	SnapshotDir   string   `json:"snapshot_dir"`
	SnapshotInode uint64   `json:"snapshot_ino"`
	OutVer        string   `json:"out_ver"`
	Ver           uint64   `json:"ver"`
	RootInode     uint64   `json:"rootInode"`
	DirInodeArr   []uint64 `json:"dirInoArr"`
}

type MetaDirSnapVerBatchDelTask struct {
	DirVersionInfo *MetaBatchDelVerReq
}
