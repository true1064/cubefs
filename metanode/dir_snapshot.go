package metanode

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"reflect"
	"sync"
)

type snapshotVer struct {
	OutVer  string
	Ver     uint64 // unixMicro of createTime used as version
	DelTime int64
	Status  uint8 // building,normal,deleted,abnormal
}

func newSnapshotVer(outVer string, ver uint64) *snapshotVer {
	return &snapshotVer{
		OutVer:  outVer,
		Ver:     ver,
		DelTime: 0,
		Status:  proto.VersionNormal,
	}
}

func (s *snapshotVer) buildVerInfo() *proto.VersionInfo {
	ifo := &proto.VersionInfo{
		Ver:     s.Ver,
		DelTime: s.DelTime,
		Status:  s.Status,
	}
	return ifo
}

type dirSnapshotItem struct {
	sync.RWMutex
	SnapshotInode uint64 // key for item
	Dir           string
	RootInode     uint64
	MaxVer        uint64
	Vers          []*snapshotVer
}

func newDirSnapItem(dirIno, rootIno uint64) *dirSnapshotItem {
	return &dirSnapshotItem{
		SnapshotInode: dirIno,
		RootInode:     rootIno,
	}
}

func (d *dirSnapshotItem) buildDirSnapshotIfo() *proto.DirSnapshotInfo {
	ifo := &proto.DirSnapshotInfo{
		SnapshotDir:   d.Dir,
		MaxVer:        d.MaxVer,
		SnapshotInode: d.SnapshotInode,
	}

	d.RLock()
	defer d.RUnlock()
	for _, v := range d.Vers {
		ifo.Vers = append(ifo.Vers, &proto.ClientDirVer{
			OutVer: v.OutVer,
			Ver:    v.buildVerInfo(),
		})
	}

	ifo.Vers = append(ifo.Vers, &proto.ClientDirVer{
		OutVer: proto.SnapshotMockVerName,
		Ver:    proto.GetMaxVersion(ifo.MaxVer),
	})
	return ifo
}

func (d *dirSnapshotItem) String() string {
	return fmt.Sprintf("snapshotIno(%d)_snapDir(%s)_rootIno(%d)_verCnt(%d)",
		d.SnapshotInode, d.Dir, d.RootInode, len(d.Vers))
}

func (d *dirSnapshotItem) equal(d1 *dirSnapshotItem) bool {
	if d.SnapshotInode != d1.SnapshotInode || d.Dir != d1.Dir || d.RootInode != d1.RootInode || d.MaxVer != d1.MaxVer {
		return false
	}

	if len(d.Vers) != len(d1.Vers) {
		return false
	}

	for idx, v := range d.Vers {
		v1 := d1.Vers[idx]
		if !reflect.DeepEqual(v, v1) {
			return false
		}
	}

	return true
}

// Less This method is necessary fot B-Tree item implementation.
func (d *dirSnapshotItem) Less(than BtreeItem) bool {
	d1, ok := than.(*dirSnapshotItem)
	if !ok {
		return false
	}

	if d.RootInode != d1.RootInode {
		return d.RootInode < d1.RootInode
	}
	return d.SnapshotInode < d1.SnapshotInode
}

func (d *dirSnapshotItem) Copy() BtreeItem {
	d1 := &dirSnapshotItem{}
	d1.RLock()
	d1.SnapshotInode = d.SnapshotInode
	d1.Dir = d.Dir
	d1.RootInode = d.RootInode
	d1.MaxVer = d.MaxVer

	if len(d.Vers) > 0 {
		d1.Vers = make([]*snapshotVer, 0, len(d.Vers))
		for _, v := range d.Vers {
			tmpV := *v
			d1.Vers = append(d1.Vers, &tmpV)
		}
	}
	d1.RUnlock()
	return d1
}

func (d *dirSnapshotItem) Marshal() (result []byte, err error) {
	keyBytes := d.MarshalKey()
	valBytes := d.MarshalValue()
	keyLen := uint32(len(keyBytes))
	valLen := uint32(len(valBytes))
	buff := bytes.NewBuffer(make([]byte, 0))
	buff.Grow(int(keyLen + valLen + 8))
	if err = binary.Write(buff, binary.BigEndian, keyLen); err != nil {
		return
	}
	if _, err = buff.Write(keyBytes); err != nil {
		return
	}
	if err = binary.Write(buff, binary.BigEndian, valLen); err != nil {
		return
	}
	if _, err = buff.Write(valBytes); err != nil {
		return
	}
	result = buff.Bytes()
	return
}

func (d *dirSnapshotItem) Unmarshal(raw []byte) (err error) {
	var (
		keyLen uint32
		valLen uint32
	)
	buff := bytes.NewBuffer(raw)
	if err = binary.Read(buff, binary.BigEndian, &keyLen); err != nil {
		return
	}
	keyBytes := make([]byte, keyLen)
	if _, err = buff.Read(keyBytes); err != nil {
		return
	}
	if err = d.UnmarshalKey(keyBytes); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &valLen); err != nil {
		return
	}
	valBytes := make([]byte, valLen)
	if _, err = buff.Read(valBytes); err != nil {
		return
	}
	err = d.UnmarshalValue(valBytes)
	return
}

func (d *dirSnapshotItem) MarshalKey() (k []byte) {
	k = make([]byte, 8)
	binary.BigEndian.PutUint64(k, d.SnapshotInode)
	return
}

func (d *dirSnapshotItem) UnmarshalKey(k []byte) (err error) {
	d.SnapshotInode = binary.BigEndian.Uint64(k)
	return
}

func (s *snapshotVer) Marshal() (k []byte) {
	buff := bytes.NewBuffer(make([]byte, 0))
	buff.Grow(30)

	outVerData := []byte(s.OutVer)
	if err := binary.Write(buff, binary.BigEndian, uint16(len(outVerData))); err != nil {
		panic(err)
	}
	if err := binary.Write(buff, binary.BigEndian, outVerData); err != nil {
		panic(err)
	}
	if err := binary.Write(buff, binary.BigEndian, s.Ver); err != nil {
		panic(err)
	}
	if err := binary.Write(buff, binary.BigEndian, s.DelTime); err != nil {
		panic(err)
	}
	if err := binary.Write(buff, binary.BigEndian, s.Status); err != nil {
		panic(err)
	}
	return buff.Bytes()
}

func (s *snapshotVer) Unmarshal(val []byte) (err error) {
	buff := bytes.NewBuffer(val)

	strSize := uint16(0)
	if err = binary.Read(buff, binary.BigEndian, &strSize); err != nil {
		return
	}

	outVerData := make([]byte, strSize)
	if err = binary.Read(buff, binary.BigEndian, &outVerData); err != nil {
		return
	}
	s.OutVer = string(outVerData)

	if err = binary.Read(buff, binary.BigEndian, &s.Ver); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &s.DelTime); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &s.Status); err != nil {
		return
	}
	return
}

func (d *dirSnapshotItem) MarshalValue() (k []byte) {

	buff := bytes.NewBuffer(make([]byte, 0))
	buff.Grow(32)
	dirData := []byte(d.Dir)

	if err := binary.Write(buff, binary.BigEndian, uint16(len(dirData))); err != nil {
		panic(err)
	}
	if err := binary.Write(buff, binary.BigEndian, dirData); err != nil {
		panic(err)
	}
	if err := binary.Write(buff, binary.BigEndian, &d.RootInode); err != nil {
		panic(err)
	}
	if err := binary.Write(buff, binary.BigEndian, &d.MaxVer); err != nil {
		panic(err)
	}

	verCnt := len(d.Vers)
	if err := binary.Write(buff, binary.BigEndian, uint16(verCnt)); err != nil {
		panic(err)
	}

	for _, v := range d.Vers {
		data := v.Marshal()
		if err := binary.Write(buff, binary.BigEndian, uint16(len(data))); err != nil {
			panic(err)
		}
		if err := binary.Write(buff, binary.BigEndian, data); err != nil {
			panic(err)
		}
	}

	k = buff.Bytes()
	return
}

func (d *dirSnapshotItem) UnmarshalValue(val []byte) (err error) {
	buff := bytes.NewBuffer(val)

	dirSize := uint16(0)
	if err = binary.Read(buff, binary.BigEndian, &dirSize); err != nil {
		return
	}
	dirData := make([]byte, dirSize)
	err = binary.Read(buff, binary.BigEndian, dirData)
	if err != nil {
		return
	}
	d.Dir = string(dirData)

	if err = binary.Read(buff, binary.BigEndian, &d.RootInode); err != nil {
		return
	}

	if err = binary.Read(buff, binary.BigEndian, &d.MaxVer); err != nil {
		return
	}

	verCnt := uint16(0)
	if err = binary.Read(buff, binary.BigEndian, &verCnt); err != nil {
		return
	}

	if verCnt > 0 {
		d.Vers = make([]*snapshotVer, 0, verCnt)
		for idx := 0; idx < int(verCnt); idx++ {
			vSize := uint16(0)
			if err = binary.Read(buff, binary.BigEndian, &vSize); err != nil {
				return
			}
			vData := make([]byte, vSize)
			if err = binary.Read(buff, binary.BigEndian, vData); err != nil {
				return
			}
			v := &snapshotVer{}
			err = v.Unmarshal(vData)
			if err != nil {
				return err
			}
			d.Vers = append(d.Vers, v)
		}
	}

	return
}

type BatchDelDirSnapInfo struct {
	Status int                `json:"status"`
	Items  []proto.DirVerItem `json:"items"`
}
