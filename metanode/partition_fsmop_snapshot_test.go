package metanode

import (
	"github.com/cubefs/cubefs/proto"
	"github.com/stretchr/testify/require"
	"testing"
)

func initDirVerMP() *metaPartition {
	return &metaPartition{
		dirVerTree: NewBtree(),
	}
}

func Test_fsmCreateDirSnapshot(t *testing.T) {
	tcases := []struct {
		ifo      *proto.CreateDirSnapShotInfo
		resp     uint8
		size     int
		treeSize int
	}{
		{ifo: &proto.CreateDirSnapShotInfo{SnapshotDir: "test", SnapshotInode: 10, OutVer: "tt", Ver: 1, RootInode: rootIno},
			resp: proto.OpOk, size: 1, treeSize: 1},
		{ifo: &proto.CreateDirSnapShotInfo{SnapshotDir: "test", SnapshotInode: 10, OutVer: "tt", Ver: 1, RootInode: rootIno},
			resp: proto.OpOk, size: 1, treeSize: 1},
		{ifo: &proto.CreateDirSnapShotInfo{SnapshotDir: "test1", SnapshotInode: 10, OutVer: "tt", Ver: 2, RootInode: rootIno},
			resp: proto.OpArgMismatchErr, size: 1, treeSize: 1},
		{ifo: &proto.CreateDirSnapShotInfo{SnapshotDir: "test", SnapshotInode: 10, OutVer: "tt1", Ver: 1, RootInode: rootIno},
			resp: proto.OpArgMismatchErr, size: 1, treeSize: 1},
		{ifo: &proto.CreateDirSnapShotInfo{SnapshotDir: "test", SnapshotInode: 10, OutVer: "tt1", Ver: 2, RootInode: rootIno},
			resp: proto.OpOk, size: 2, treeSize: 1},
		{ifo: &proto.CreateDirSnapShotInfo{SnapshotDir: "test", SnapshotInode: 101, OutVer: "tt1", Ver: 2, RootInode: rootIno},
			resp: proto.OpOk, size: 1, treeSize: 2},
	}

	mp := initDirVerMP()
	for _, c := range tcases {
		result := mp.fsmCreateDirSnapshot(c.ifo)
		require.True(t, result == c.resp)
		require.Equal(t, mp.dirVerTree.Len(), c.treeSize)
		item := mp.getDirSnapItem(c.ifo.SnapshotInode, rootIno)
		require.Equal(t, len(item.Vers), c.size)
	}
}

func Test_fsmDelDirSnap(t *testing.T) {
	mp := initDirVerMP()
	ifo1 := &proto.CreateDirSnapShotInfo{SnapshotDir: "test", SnapshotInode: 10, OutVer: "tt", Ver: 1, RootInode: rootIno}
	ifo2 := &proto.CreateDirSnapShotInfo{SnapshotDir: "test", SnapshotInode: 10, OutVer: "tt2", Ver: 2, RootInode: rootIno}

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

	e.Ver = 1
	resp = mp.fsmDelDirSnap(e)
	require.Equal(t, resp, proto.OpNotExistErr)

	e.RootIno = rootIno
	resp = mp.fsmDelDirSnap(e)
	require.Equal(t, resp, proto.OpOk)

	item := mp.getDirSnapItem(e.DirSnapIno, rootIno)
	require.True(t, item != nil)
	for _, v := range item.Vers {
		if v.Ver == e.Ver {
			require.Equal(t, v.Status, uint8(proto.VersionMarkDelete))
		}
	}
}

var rootIno = uint64(3)

func Test_fsmBatchDelDirSnapshot(t *testing.T) {
	// test mark deleting dir
	mp := initDirVerMP()
	ifo := &BatchDelDirSnapInfo{
		Status: proto.VersionNormal,
	}
	resp := mp.fsmBatchDelDirSnapshot(ifo)
	require.True(t, resp == proto.OpArgMismatchErr)

	ino1 := uint64(10)
	ino2 := uint64(20)
	items := []*proto.CreateDirSnapShotInfo{
		{SnapshotDir: "test", SnapshotInode: ino1, OutVer: "tt", Ver: 1, RootInode: rootIno},
		{SnapshotDir: "test", SnapshotInode: ino1, OutVer: "tt2", Ver: 2, RootInode: rootIno},
		{SnapshotDir: "test", SnapshotInode: ino1, OutVer: "tt3", Ver: 3, RootInode: rootIno},
		{SnapshotDir: "test", SnapshotInode: ino2, OutVer: "tt1", Ver: 1, RootInode: rootIno},
		{SnapshotDir: "test", SnapshotInode: ino2, OutVer: "tt2", Ver: 2, RootInode: rootIno},
		{SnapshotDir: "test", SnapshotInode: ino2, OutVer: "tt3", Ver: 3, RootInode: rootIno},
	}

	for _, e := range items {
		resp = mp.fsmCreateDirSnapshot(e)
		require.Equal(t, resp, proto.OpOk)
	}

	for _, e := range items {
		resp = mp.fsmDelDirSnap(&proto.DirVerItem{DirSnapIno: e.SnapshotInode, Ver: e.Ver, RootIno: rootIno})
		require.Equal(t, resp, proto.OpOk)
	}

	ifo.Status = proto.VersionDeleting
	ifo.Items = []proto.DirVerItem{
		{DirSnapIno: ino1, Ver: 100, RootIno: rootIno},
		{DirSnapIno: ino1, Ver: 2, RootIno: rootIno},
		{DirSnapIno: ino2, Ver: 3, RootIno: rootIno},
	}

	resp = mp.fsmBatchDelDirSnapshot(ifo)
	require.Equal(t, resp, proto.OpOk)

	check := func(item *dirSnapshotItem, ver uint64, status int) bool {
		for _, v := range item.Vers {
			if v.Ver != ver {
				continue
			}
			return int(v.Status) == status
		}
		return false
	}

	dir1 := mp.getDirSnapItem(ino1, rootIno)
	require.True(t, len(dir1.Vers) == 3)
	require.True(t, check(dir1, 2, proto.VersionDeleting))

	dir2 := mp.getDirSnapItem(ino2, rootIno)
	t.Logf("dir2 items %v", dir2.Vers)
	require.True(t, len(dir2.Vers) == 3)
	require.True(t, check(dir2, 3, proto.VersionDeleting))

	update := func(dir *dirSnapshotItem, ver uint64, status int) {
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

	dir1 = mp.getDirSnapItem(ino1, rootIno)
	require.Equal(t, len(dir1.Vers), 2)
	dir2 = mp.getDirSnapItem(ino2, rootIno)
	require.Equal(t, len(dir2.Vers), 2)

	ifo.Status = proto.VersionDeleting
	ifo.Items = []proto.DirVerItem{
		{DirSnapIno: ino1, Ver: 1, RootIno: rootIno},
		{DirSnapIno: ino1, Ver: 3, RootIno: rootIno},
	}
	resp = mp.fsmBatchDelDirSnapshot(ifo)
	require.Equal(t, resp, proto.OpOk)

	ifo.Status = proto.VersionDeleted
	resp = mp.fsmBatchDelDirSnapshot(ifo)
	require.Equal(t, resp, proto.OpOk)
	dir1 = mp.getDirSnapItem(ino1, rootIno)
	t.Logf("t %v", dir1)
	require.True(t, dir1 == nil)
}
