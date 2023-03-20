package sdk

import (
	"context"
	"io"
	"time"
)

type IVolume interface {
	//Info get vol simple info
	Info() *VolInfo
	// Lookup from target parentDir ino, parentIno 0 means lookup from root
	Lookup(ctx context.Context, parentIno uint64, path string) (*DirInfo, error)
	GetInode(ctx context.Context, ino uint64) (*InodeInfo, error)
	BatchGetInodes(ctx context.Context, inos []uint64) ([]*InodeInfo, error)
	Readdir(ctx context.Context, filePath, marker string, count uint32) ([]DirInfo, error)
	StatFs(ctx context.Context, filepath uint64) (*StatFs, error)

	SetXAttr(ctx context.Context, ino uint64, key string, val string) error
	BatchSetXAttr(ctx context.Context, ino uint64, attrs map[string]string) error
	GetXAttr(ctx context.Context, ino uint64, key string) (string, error)
	ListXAttr(ctx context.Context, ino uint64) ([]string, error)
	GetXAttrMap(ctx context.Context, ino uint64) (map[string]string, error)
	DeleteXAttr(ctx context.Context, ino uint64, key string) error
	BatchDeleteXAttr(ctx context.Context, ino uint64, keys []string) error

	//Mkdir path
	Mkdir(ctx context.Context, path string, recursive bool) (*InodeInfo, error)
	CreateFile(ctx context.Context, parentIno uint64, filename string) (*InodeInfo, error)
	Delete(ctx context.Context, filePath string) error
	Rename(ctx context.Context, src, dest string) error

	// UploadFile file
	UploadFile(ctx context.Context, filePath string, oldIno uint64, body io.Reader) (*InodeInfo, error)
	WriteFile(ctx context.Context, ino, off, size uint64, body io.Reader) error
	ReadFile(ctx context.Context, ino, off, size uint64) (body io.ReadCloser, err error)

	InitMultiPart(ctx context.Context, path string, oldIno uint64, extend map[string]string) (string, error)
	GetMultiExtend(ctx context.Context, path, uploadId string) (extend map[string]string, err error)
	UploadMultiPart(ctx context.Context, filepath, uploadId string, partNum uint16, read io.Reader) error
	ListMultiPart(ctx context.Context, filepath, uploadId string, count, marker uint64) (parts []*Part, next uint64, isTruncated bool, err error)
	AbortMultiPart(ctx context.Context, filepath, uploadId string) error
	CompleteMultiPart(ctx context.Context, filepath, uploadId string, oldIno uint64, parts []Part) error
}

type VolInfo struct {
	Name   string
	Weight int
}

type StatFs struct {
	Size int
}

type InodeInfo struct {
	Inode      uint64    `json:"ino"`
	Mode       uint32    `json:"mode"`
	Nlink      uint32    `json:"nlink"`
	Size       uint64    `json:"sz"`
	Uid        uint32    `json:"uid"`
	Gid        uint32    `json:"gid"`
	ModifyTime time.Time `json:"mt"`
	CreateTime time.Time `json:"ct"`
	AccessTime time.Time `json:"at"`
}

type DirInfo struct {
	Name  string
	Type  uint32
	Inode uint64
}

func (d *DirInfo) IsDir() bool {
	return true
}

type Part struct {
	PartNum      uint16
	MD5          string
	LastModified uint64
	Size         uint64
}
