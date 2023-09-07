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
	sv := &snapshotVer{
		Ver:     1010,
		DelTime: 1020,
		Status:  proto.VersionDeleting,
	}

	for _, s := range tcases {
		sv.OutVer = s
		k := sv.Marshal()
		nsv := &snapshotVer{}
		err := nsv.Unmarshal(k)
		require.NoError(t, err)
		require.True(t, reflect.DeepEqual(sv, nsv))
	}
}

func TestDirSnapshotItem_Marshal(t *testing.T) {
	ds := &dirSnapshotItem{
		SnapshotInode: 10,
		RootInode:     1024,
		MaxVer:        10,
	}

	tcases := []struct {
		s  string
		vs []*snapshotVer
	}{
		{s: "/", vs: []*snapshotVer{}},
		{s: "", vs: []*snapshotVer{}},
		{s: "/test/txx/name", vs: []*snapshotVer{}},
		{s: "/test/txx/name", vs: []*snapshotVer{
			{DelTime: 10, Status: proto.VersionNormal},
			{Ver: 102, Status: proto.VersionDeleted},
		}},
	}

	for _, c := range tcases {
		ds.Dir = c.s
		ds.Vers = c.vs
		data, err := ds.Marshal()
		require.NoError(t, err)
		nds := &dirSnapshotItem{}
		err = nds.Unmarshal(data)
		require.NoError(t, err)
		require.True(t, ds.equal(nds))
		cds := ds.Copy()
		require.True(t, ds.equal(cds.(*dirSnapshotItem)))
	}
}

func TestDirSnapshotItem_Less(t *testing.T) {
	tcases := []struct {
		a      uint64
		b      uint64
		expect bool
	}{
		{0, 10, true},
		{10, 10, false},
		{11, 1, false},
	}

	for _, c := range tcases {
		d1 := &dirSnapshotItem{
			SnapshotInode: c.a,
		}
		d2 := &dirSnapshotItem{
			SnapshotInode: c.b,
		}
		require.Equal(t, d1.Less(d2), c.expect)
	}
}
