package metanode

import (
	"bytes"
	"encoding/binary"
	"reflect"
)

type SnapshotVer struct {
	OutVer  string
	Ver     uint64 // unixMicro of createTime used as version
	DelTime int64
	Status  uint8 // building,normal,deleted,abnormal
}

type DirSnapshotItem struct {
	SnapshotInode uint64 // key for item
	Dir           string
	RootInode     uint64
	Vers          []*SnapshotVer
}

func (d *DirSnapshotItem) equal(d1 *DirSnapshotItem) bool {
	if d.SnapshotInode != d1.SnapshotInode || d.Dir != d1.Dir || d.RootInode != d1.RootInode {
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

func (d *DirSnapshotItem) Marshal() (result []byte, err error) {
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

func (d *DirSnapshotItem) Unmarshal(raw []byte) (err error) {
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

func (d *DirSnapshotItem) MarshalKey() (k []byte) {
	k = make([]byte, 8)
	binary.BigEndian.PutUint64(k, d.SnapshotInode)
	return
}

func (d *DirSnapshotItem) UnmarshalKey(k []byte) (err error) {
	d.SnapshotInode = binary.BigEndian.Uint64(k)
	return
}

func (s *SnapshotVer) Marshal() (k []byte) {
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

func (s *SnapshotVer) Unmarshal(val []byte) (err error) {
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

func (d *DirSnapshotItem) MarshalValue() (k []byte) {

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

func (d *DirSnapshotItem) UnmarshalValue(val []byte) (err error) {
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

	verCnt := uint16(0)
	if err = binary.Read(buff, binary.BigEndian, &verCnt); err != nil {
		return
	}

	if verCnt > 0 {
		d.Vers = make([]*SnapshotVer, 0, verCnt)
		for idx := 0; idx < int(verCnt); idx++ {
			vSize := uint16(0)
			if err = binary.Read(buff, binary.BigEndian, &vSize); err != nil {
				return
			}
			vData := make([]byte, vSize)
			if err = binary.Read(buff, binary.BigEndian, vData); err != nil {
				return
			}
			v := &SnapshotVer{}
			err = v.Unmarshal(vData)
			if err != nil {
				return err
			}
			d.Vers = append(d.Vers, v)
		}
	}

	return
}
