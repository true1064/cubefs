package metanode

import (
	"github.com/cubefs/cubefs/proto"
	"github.com/stretchr/testify/require"
	"testing"
)

func initMetaPartition() *metaPartition {
	return &metaPartition{
		dirVerTree: NewBtree(),
	}
}

func Test_fsmCreateDirSnapshot(t *testing.T) {
	tcases := []struct {
		ifo      *proto.DirSnapShotInfo
		resp     uint8
		size     int
		treeSize int
	}{
		{ifo: &proto.DirSnapShotInfo{SnapshotDir: "test", SnapshotInode: 10, OutVer: "tt", Ver: 1, RootInode: 10},
			resp: proto.OpOk, size: 1, treeSize: 1},
		{ifo: &proto.DirSnapShotInfo{SnapshotDir: "test", SnapshotInode: 10, OutVer: "tt", Ver: 1, RootInode: 10},
			resp: proto.OpOk, size: 1, treeSize: 1},
		{ifo: &proto.DirSnapShotInfo{SnapshotDir: "test", SnapshotInode: 10, OutVer: "tt", Ver: 1, RootInode: 101},
			resp: proto.OpArgMismatchErr, size: 1, treeSize: 1},
		{ifo: &proto.DirSnapShotInfo{SnapshotDir: "test1", SnapshotInode: 10, OutVer: "tt", Ver: 2, RootInode: 10},
			resp: proto.OpArgMismatchErr, size: 1, treeSize: 1},
		{ifo: &proto.DirSnapShotInfo{SnapshotDir: "test", SnapshotInode: 10, OutVer: "tt1", Ver: 1, RootInode: 10},
			resp: proto.OpArgMismatchErr, size: 1, treeSize: 1},
		{ifo: &proto.DirSnapShotInfo{SnapshotDir: "test", SnapshotInode: 10, OutVer: "tt1", Ver: 2, RootInode: 10},
			resp: proto.OpOk, size: 2, treeSize: 1},
		{ifo: &proto.DirSnapShotInfo{SnapshotDir: "test", SnapshotInode: 101, OutVer: "tt1", Ver: 2, RootInode: 10},
			resp: proto.OpOk, size: 1, treeSize: 2},
	}

	mp := initMetaPartition()
	for _, c := range tcases {
		result := mp.fsmCreateDirSnapshot(c.ifo)
		require.True(t, result == c.resp)
		require.Equal(t, mp.dirVerTree.Len(), c.treeSize)
		item := mp.getDirSnapItem(c.ifo.SnapshotInode)
		require.Equal(t, len(item.Vers), c.size)
	}
}

func Test_fsmDelDirSnap(t *testing.T) {
	mp := initMetaPartition()
	ifo1 := &proto.DirSnapShotInfo{SnapshotDir: "test", SnapshotInode: 10, OutVer: "tt", Ver: 1, RootInode: 10}
	ifo2 := &proto.DirSnapShotInfo{SnapshotDir: "test", SnapshotInode: 10, OutVer: "tt2", Ver: 2, RootInode: 10}

	resp := mp.fsmCreateDirSnapshot(ifo1)
	require.Equal(t, resp, proto.OpOk)
	resp = mp.fsmCreateDirSnapshot(ifo2)
	require.Equal(t, resp, proto.OpOk)

	e := &proto.DirVerItem{}
	resp = mp.fsmDelDirSnap(e)
	require.Equal(t, resp, proto.OpNotExistErr)

	e.DirSnapIno = 10
	resp = mp.fsmDelDirSnap(e)
	require.Equal(t, resp, proto.OpNotExistErr)

	e.Ver = 2
	resp = mp.fsmDelDirSnap(e)
	require.Equal(t, resp, proto.OpOk)
	item := mp.getDirSnapItem(e.DirSnapIno)
	require.True(t, item != nil)
	for _, v := range item.Vers {
		if v.Ver == e.Ver {
			require.Equal(t, v.Status, uint8(proto.VersionMarkDelete))
		}
	}
}

func Test_fsmBatchDelDirSnapshot(t *testing.T) {
	// test mark deleting dir
	mp := initMetaPartition()
	ifo := &BatchDelDirSnapInfo{
		Status: proto.VersionNormal,
	}
	resp := mp.fsmBatchDelDirSnapshot(ifo)
	require.True(t, resp == proto.OpArgMismatchErr)

	ino1 := uint64(10)
	ino2 := uint64(20)
	items := []*proto.DirSnapShotInfo{
		{SnapshotDir: "test", SnapshotInode: ino1, OutVer: "tt", Ver: 1, RootInode: 10},
		{SnapshotDir: "test", SnapshotInode: ino1, OutVer: "tt2", Ver: 2, RootInode: 10},
		{SnapshotDir: "test", SnapshotInode: ino1, OutVer: "tt3", Ver: 3, RootInode: 10},
		{SnapshotDir: "test", SnapshotInode: ino2, OutVer: "tt1", Ver: 1, RootInode: 10},
		{SnapshotDir: "test", SnapshotInode: ino2, OutVer: "tt2", Ver: 2, RootInode: 10},
		{SnapshotDir: "test", SnapshotInode: ino2, OutVer: "tt3", Ver: 3, RootInode: 10},
	}

	for _, e := range items {
		resp = mp.fsmCreateDirSnapshot(e)
		require.Equal(t, resp, proto.OpOk)
	}

	for _, e := range items {
		resp = mp.fsmDelDirSnap(&proto.DirVerItem{DirSnapIno: e.SnapshotInode, Ver: e.Ver})
		require.Equal(t, resp, proto.OpOk)
	}

	ifo.Status = proto.VersionDeleting
	ifo.Items = []proto.DirVerItem{
		{DirSnapIno: ino1, Ver: 100},
		{DirSnapIno: ino1, Ver: 2},
		{DirSnapIno: ino2, Ver: 3},
	}

	resp = mp.fsmBatchDelDirSnapshot(ifo)
	require.Equal(t, resp, proto.OpOk)

	check := func(item *DirSnapshotItem, ver uint64, status int) bool {
		for _, v := range item.Vers {
			if v.Ver != ver {
				continue
			}
			return int(v.Status) == status
		}
		return false
	}

	dir1 := mp.getDirSnapItem(ino1)
	require.True(t, len(dir1.Vers) == 3)
	require.True(t, check(dir1, 2, proto.VersionDeleting))

	dir2 := mp.getDirSnapItem(ino2)
	t.Logf("dir2 items %v", dir2.Vers)
	require.True(t, len(dir2.Vers) == 3)
	require.True(t, check(dir2, 3, proto.VersionDeleting))

	update := func(dir *DirSnapshotItem, ver uint64, status int) {
		for _, v := range dir.Vers {
			if v.Ver == ver {
				v.Status = uint8(status)
			}
		}
	}

	update(dir2, 3, proto.VersionNormal)
	resp = mp.fsmBatchDelDirSnapshot(ifo)
	require.Equal(t, resp, proto.OpInternalErr)

	ifo.Status = proto.VersionDeleted
	resp = mp.fsmBatchDelDirSnapshot(ifo)
	require.Equal(t, resp, proto.OpInternalErr)
	update(dir2, 3, proto.VersionDeleting)

	resp = mp.fsmBatchDelDirSnapshot(ifo)
	require.Equal(t, resp, proto.OpOk)

	dir1 = mp.getDirSnapItem(ino1)
	require.Equal(t, len(dir1.Vers), 2)
	dir2 = mp.getDirSnapItem(ino2)
	require.Equal(t, len(dir2.Vers), 2)

	ifo.Status = proto.VersionDeleting
	ifo.Items = []proto.DirVerItem{
		{DirSnapIno: ino1, Ver: 1},
		{DirSnapIno: ino1, Ver: 3},
	}
	resp = mp.fsmBatchDelDirSnapshot(ifo)
	require.Equal(t, resp, proto.OpOk)

	ifo.Status = proto.VersionDeleted
	resp = mp.fsmBatchDelDirSnapshot(ifo)
	require.Equal(t, resp, proto.OpOk)
	dir1 = mp.getDirSnapItem(ino1)
	t.Logf("t %v", dir1)
	require.True(t, dir1 == nil)
}
