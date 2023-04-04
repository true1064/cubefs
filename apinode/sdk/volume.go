package sdk

import (
	"context"
	"io"
)

type IVolume interface {
	// Info get vol simple info
	Info() *VolInfo
	// Lookup from target parentDir ino, parentIno 0 means lookup from root
	Lookup(ctx context.Context, parentIno uint64, name string) (*DirInfo, error)
	GetInode(ctx context.Context, ino uint64) (*InodeInfo, error)
	// BatchGetInodes maybe cost much time
	BatchGetInodes(ctx context.Context, inos []uint64) ([]*InodeInfo, error)
	Readdir(ctx context.Context, parIno uint64, marker string, count uint32) ([]DirInfo, error)
	ReadDirAll(ctx context.Context, ino uint64) ([]DirInfo, error)
	StatFs(ctx context.Context, ino uint64) (*StatFs, error)
	// SetAttr set file mode, uid, gid, atime, mtime,
	SetAttr(ctx context.Context, req *SetAttrReq) error
	SetXAttr(ctx context.Context, ino uint64, key string, val string) error
	BatchSetXAttr(ctx context.Context, ino uint64, attrs map[string]string) error
	GetXAttr(ctx context.Context, ino uint64, key string) (string, error)
	ListXAttr(ctx context.Context, ino uint64) ([]string, error)
	GetXAttrMap(ctx context.Context, ino uint64) (map[string]string, error)
	DeleteXAttr(ctx context.Context, ino uint64, key string) error
	BatchDeleteXAttr(ctx context.Context, ino uint64, keys []string) error

	// Mkdir path
	Mkdir(ctx context.Context, parIno uint64, name string) (*InodeInfo, error)
	CreateFile(ctx context.Context, parentIno uint64, name string) (*InodeInfo, error)
	// Delete dir should be empty before delete
	Delete(ctx context.Context, parIno uint64, name string, isDir bool) error
	Rename(ctx context.Context, srcParIno, dstParIno uint64, srcName, destName string) error

	// UploadFile file
	UploadFile(req *UploadFileReq) (*InodeInfo, error)
	WriteFile(ctx context.Context, ino, off, size uint64, body io.Reader) error
	// ReadFile read() will make a rpc request to server, if n less than len(data), it means no more data
	ReadFile(ctx context.Context, ino, off uint64, data []byte) (n int, err error)
	InitMultiPart(ctx context.Context, path string, oldIno uint64, extend map[string]string) (string, error)
	GetMultiExtend(ctx context.Context, path, uploadId string) (extend map[string]string, err error)
	UploadMultiPart(ctx context.Context, filepath, uploadId string, partNum uint16, read io.Reader) error
	ListMultiPart(ctx context.Context, filepath, uploadId string, count, marker uint64) (parts []*Part, next uint64, isTruncated bool, err error)
	AbortMultiPart(ctx context.Context, filepath, uploadId string) error
	CompleteMultiPart(ctx context.Context, filepath, uploadId string, oldIno uint64, parts []Part) error
}
