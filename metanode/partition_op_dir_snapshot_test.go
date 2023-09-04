package metanode

import (
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/test/mocks"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestMetaPartition_ListAllDirSnapshot(t *testing.T) {
	mp := initDirVerMP()

	rootIno := uint64(time.Now().Unix())
	pkt := &Packet{}

	getResp := func(p *Packet) *proto.ListDirSnapshotResp {
		resp := &proto.ListDirSnapshotResp{}
		err := json.Unmarshal(p.Data, resp)
		require.NoError(t, err)
		return resp
	}

	err := mp.ListAllDirSnapshot(rootIno, pkt)
	require.NoError(t, err)
	resp := getResp(pkt)
	require.True(t, len(resp.Items) == 0)

	items := []proto.CreateDirSnapShotInfo{
		{"101", 10, "o1", 101, rootIno},
		{"101", 10, "o2", 102, rootIno},
		{"101", 12, "o-1", 121, rootIno},
		{"101", 12, "o-2", 122, rootIno},
		{"101", 12, "o-2", 121, rootIno - 1},
		{"101", 12, "o-2", 121, rootIno + 1},
	}

	for _, e := range items {
		resp := mp.fsmCreateDirSnapshot(&e)
		require.True(t, resp == proto.OpOk)
	}

	err = mp.ListAllDirSnapshot(rootIno, pkt)
	require.NoError(t, err)

	resp = getResp(pkt)
	require.Equal(t, len(resp.Items), 2)
	for _, e := range resp.Items {
		require.Equal(t, len(e.Vers), 2)
	}
}

var any = gomock.Any()

func TestMetaPartition_CreateDirSnapshot(t *testing.T) {
	mp := initDirVerMP()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRaft := mocks.NewMockRaftPartition(ctrl)
	mp.raftPartition = mockRaft

	err := fmt.Errorf("raft no leader")
	ifo := &proto.CreateDirSnapShotInfo{
		SnapshotDir: "10",
	}

	tcases := []struct {
		code       uint8
		err        error
		expectCode uint8
		expectErr  error
	}{
		{proto.OpOk, err, proto.OpAgain, err},
		{proto.OpOk, nil, proto.OpOk, nil},
		{proto.OpNotExistErr, nil, proto.OpNotExistErr, nil},
	}

	for _, c := range tcases {
		pkt := &Packet{}
		mockRaft.EXPECT().Submit(any).Return(c.code, c.err)
		err = mp.CreateDirSnapshot(ifo, pkt)
		require.Equal(t, c.expectErr, c.err)
		require.Equal(t, c.expectCode, pkt.ResultCode)
	}
}

func TestMetaPartition_DelDirSnapshot(t *testing.T) {
	mp := initDirVerMP()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRaft := mocks.NewMockRaftPartition(ctrl)
	mp.raftPartition = mockRaft

	err := fmt.Errorf("raft no leader")
	ifo := &proto.DirVerItem{
		RootIno: rootIno,
	}

	tcases := []struct {
		code       uint8
		err        error
		expectCode uint8
		expectErr  error
	}{
		{proto.OpOk, err, proto.OpAgain, err},
		{proto.OpOk, nil, proto.OpOk, nil},
		{proto.OpNotExistErr, nil, proto.OpNotExistErr, nil},
	}

	for _, c := range tcases {
		pkt := &Packet{}
		mockRaft.EXPECT().Submit(any).Return(c.code, c.err)
		err = mp.DelDirSnapshot(ifo, pkt)
		require.Equal(t, c.expectErr, c.err)
		require.Equal(t, c.expectCode, pkt.ResultCode)
	}
}
