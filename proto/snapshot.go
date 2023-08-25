package proto

import (
	"fmt"
	"time"
)

type VerInfo struct {
	VolName string
	VerSeq  uint64
}

func (vi *VerInfo) Key() string {
	return fmt.Sprintf("%s_%d", vi.VolName, vi.VerSeq)
}

//snapshot version delete
type SnapshotVerDelTask struct {
	VerInfo
}

type SnapshotVerDelTaskRequest struct {
	MasterAddr string
	LcNodeAddr string
	Task       *SnapshotVerDelTask
}

type SnapshotStatistics struct {
	VerInfo
	TotalInodeNum   int64
	FileNum         int64
	DirNum          int64
	ErrorSkippedNum int64
}

type SnapshotVerDelTaskResponse struct {
	ID string
	SnapshotStatistics
	StartTime *time.Time
	EndTime   *time.Time
	Done      bool
	Status    uint8
	Result    string
}

type DelVer struct {
	DelVer     uint64        // the snapshot version to delete
	SubRootIno uint64        // CFA-user's root directory
	Vers       []VersionInfo //Info of all snapshots of a directory
}

type DelDirVersionInfo struct {
	DirIno  uint64
	DelVers []DelVer
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
	VolName     string           `json:"vol"`
	PartitionID uint64           `json:"pid"`
	Info        *DirSnapShotInfo `json:"snapshot"`
}

type DirSnapShotInfo struct {
	SnapshotDir   string `json:"snapshot_dir"`
	SnapshotInode uint64 `json:"snapshot_ino"`
	OutVer        string `json:"out_ver"`
	Ver           uint64 `json:"ver"`
	RootInode     uint64 `json:"rootInode"`
}
