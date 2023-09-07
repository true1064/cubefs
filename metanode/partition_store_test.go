package metanode

import (
	"github.com/cubefs/cubefs/proto"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"os"
	"testing"
)

func initStoreMp() *metaPartition {
	mp := &metaPartition{
		dirVerTree: NewBtree(),
		config: &MetaPartitionConfig{
			VolName:     "mockVol",
			PartitionId: 10,
		},
	}
	return mp
}

type Info = proto.CreateDirSnapShotInfo

func Test_persistDirSnapshot(t *testing.T) {
	mp := initStoreMp()

	d1, d2 := "test", "test1"
	ino1, ino2 := uint64(10), uint64(11)

	tcases := []*proto.CreateDirSnapShotInfo{
		{SnapshotDir: d1, SnapshotInode: ino1, OutVer: "tt", Ver: 1, RootInode: rootIno},
		{SnapshotDir: d1, SnapshotInode: ino1, OutVer: "tx", Ver: 2, RootInode: rootIno},
		{SnapshotDir: d1, SnapshotInode: ino1, OutVer: "ty", Ver: 3, RootInode: rootIno},
		{SnapshotDir: d2, SnapshotInode: ino2, OutVer: "tx", Ver: 1, RootInode: rootIno},
		{SnapshotDir: d2, SnapshotInode: ino2, OutVer: "ty", Ver: 2, RootInode: rootIno},
		{SnapshotDir: d2, SnapshotInode: ino2, OutVer: "tz", Ver: 3, RootInode: rootIno},
	}

	for _, c := range tcases {
		resp := mp.fsmCreateDirSnapshot(c)
		require.Equal(t, resp, proto.OpOk)
	}

	tmpDir, err := ioutil.TempDir("", "snap")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	sm := &storeMsg{
		dirVerTree: mp.dirVerTree,
	}
	_, err = mp.storeDirSnapshot(tmpDir, sm)
	require.NoError(t, err)

	mp2 := initStoreMp()
	err = mp2.loadDirSnapshot(tmpDir)
	require.NoError(t, err)

	mp.dirVerTree.Ascend(func(i BtreeItem) bool {
		d1 := i.(*dirSnapshotItem)
		d2 := mp2.getDirSnapItem(d1.SnapshotInode, d1.RootInode)
		require.True(t, d1.equal(d2))
		return true
	})
}
