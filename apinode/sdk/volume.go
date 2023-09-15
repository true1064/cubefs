package sdk

import (
	"context"
	"github.com/cubefs/cubefs/proto"
	"io"
)

type IVolume interface {
	IDirSnapshot
}

// can used as a distributed lock
type InodeLockApi interface {
	// Lock expireTime means lock will become invalid expireTime seconds
	Lock(ctx context.Context, inode uint64, expireTime int) error
	UnLock(ctx context.Context, inode uint64) error
}

type IDirSnapshot interface {
	Info() *VolInfo
	// GetDirSnapshot should be invoked when every rpc request from client.
	DirSnapShot
	NewInodeLock() InodeLockApi

	// Lookup from target parentDir ino, parentIno 0 means lookup from root
	Lookup(ctx context.Context, parentIno uint64, name string) (*DirInfo, error)
	GetInode(ctx context.Context, ino uint64) (*proto.InodeInfo, error)
	// BatchGetInodes maybe cost much time
	BatchGetInodes(ctx context.Context, inos []uint64) ([]*proto.InodeInfo, error)
	Readdir(ctx context.Context, parIno uint64, marker string, count uint32) ([]DirInfo, error)
	ReadDirAll(ctx context.Context, ino uint64) ([]DirInfo, error)
	StatFs(ctx context.Context, ino uint64) (*StatFs, error)
	SetAttr(ctx context.Context, req *SetAttrReq) error
	// Mkdir path
	Mkdir(ctx context.Context, parIno uint64, name string) (*InodeInfo, uint64, error)
	CreateFile(ctx context.Context, parentIno uint64, name string) (*InodeInfo, uint64, error)
	// Delete dir should be empty before delete
	Delete(ctx context.Context, parIno uint64, name string, isDir bool) error
	Rename(ctx context.Context, srcSubPath, dstSubPath string) error

	// UploadFile file
	UploadFile(ctx context.Context, req *UploadFileReq) (*InodeInfo, uint64, error)
	WriteFile(ctx context.Context, ino, off, size uint64, body io.Reader) error
	// ReadFile read() will make a rpc request to server, if n less than len(data), it means no more data
	ReadFile(ctx context.Context, ino, off uint64, data []byte) (n int, err error)
	MultiPart
	Xattr
}

type DirSnapShot interface {
	IsSnapshotInode(ctx context.Context, ino uint64) bool
	GetDirSnapshot(ctx context.Context, rootIno uint64) (IDirSnapshot, error)
	CreateDirSnapshot(ctx context.Context, ver, filePath string) error
	DeleteDirSnapshot(ctx context.Context, ver, filePath string) error
}

type MultiPart interface {
	InitMultiPart(ctx context.Context, path string, extend map[string]string) (string, error)
	UploadMultiPart(ctx context.Context, filepath, uploadId string, partNum uint16, read io.Reader) (*Part, error)
	ListMultiPart(ctx context.Context, filepath, uploadId string, count, marker uint64) (parts []*Part, next uint64, isTruncated bool, err error)
	AbortMultiPart(ctx context.Context, filepath, uploadId string) error
	CompleteMultiPart(ctx context.Context, filepath, uploadId string, oldIno uint64, parts []Part) (*InodeInfo, uint64, error)
}

type Xattr interface {
	SetXAttr(ctx context.Context, ino uint64, key string, val string) error
	SetXAttrNX(ctx context.Context, ino uint64, key string, val string) error
	BatchSetXAttr(ctx context.Context, ino uint64, attrs map[string]string) error
	GetXAttr(ctx context.Context, ino uint64, key string) (string, error)
	ListXAttr(ctx context.Context, ino uint64) ([]string, error)
	GetXAttrMap(ctx context.Context, ino uint64) (map[string]string, error)
	DeleteXAttr(ctx context.Context, ino uint64, key string) error
	BatchDeleteXAttr(ctx context.Context, ino uint64, keys []string) error
}
