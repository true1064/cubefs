package metanode

import (
	"github.com/cubefs/cubefs/proto"
	"github.com/stretchr/testify/require"
	"reflect"
	"testing"
)

func TestSnapshotVer_marshal(t *testing.T) {
	tcases := []string{
		"", "testxxx", "1212",
	}
	sv := &SnapshotVer{
		Ver:     1010,
		DelTime: 1020,
		Status:  proto.VersionDeleting,
	}

	for _, s := range tcases {
		sv.OutVer = s
		k := sv.Marshal()
		nsv := &SnapshotVer{}
		err := nsv.Unmarshal(k)
		require.NoError(t, err)
		require.True(t, reflect.DeepEqual(sv, nsv))
	}
}

func TestDirSnapshotItem_Marshal(t *testing.T) {
	ds := &DirSnapshotItem{
		SnapshotInode: 10,
		RootInode:     1024,
	}

	tcases := []struct {
		s  string
		vs []*SnapshotVer
	}{
		{s: "/", vs: []*SnapshotVer{}},
		{s: "", vs: []*SnapshotVer{}},
		{s: "/test/txx/name", vs: []*SnapshotVer{}},
		{s: "/test/txx/name", vs: []*SnapshotVer{
			{DelTime: 10, Status: proto.VersionNormal},
			{Ver: 102, Status: proto.VersionDeleted},
		}},
	}

	for _, c := range tcases {
		ds.Dir = c.s
		ds.Vers = c.vs
		data, err := ds.Marshal()
		require.NoError(t, err)
		nds := &DirSnapshotItem{}
		err = nds.Unmarshal(data)
		require.NoError(t, err)
		require.True(t, ds.equal(nds))
	}
}
