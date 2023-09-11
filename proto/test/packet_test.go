package test

import (
	"encoding/binary"
	"github.com/cubefs/cubefs/proto"
	"github.com/stretchr/testify/require"
	"reflect"
	"testing"
)

func Test_MarshalVersionSlice(t *testing.T) {
	pkt := &proto.Packet{}
	pkt.VerSeq = 10
	pkt.MarshalVersionSlice()
	data, err := pkt.MarshalVersionSlice()
	require.NoError(t, err)
	require.Equal(t, len(data), 2)

	p1 := &proto.Packet{}
	err = p1.UnmarshalVersionSlice(0, []byte{})
	require.NoError(t, err)
}

func Test_MarshalHeader(t *testing.T) {
	p1 := &proto.Packet{
		Magic:              proto.ProtoMagic,
		ExtentType:         proto.DirVersionFlag,
		Opcode:             proto.OpMetaCreateInode,
		ResultCode:         proto.OpOk,
		RemainingFollowers: 3,
		CRC:                1010,
		Size:               1023,
		ArgLen:             10,
		PartitionID:        9,
		ExtentID:           1,
		ExtentOffset:       101,
		ReqID:              102,
		KernelOffset:       1024,
		VerSeq:             100,
	}

	data := make([]byte, 65)
	p1.MarshalHeader(data)

	p2 := &proto.Packet{}
	err := p2.UnmarshalHeader(data)
	require.NoError(t, err)
	require.False(t, reflect.DeepEqual(p1, p2))

	p2.VerSeq = binary.BigEndian.Uint64(data[57:])
	require.True(t, reflect.DeepEqual(p1, p2))
}
