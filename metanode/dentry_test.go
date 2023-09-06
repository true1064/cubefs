package metanode

import (
	"github.com/stretchr/testify/require"
	"reflect"
	"testing"
)

func TestDentry_Marshal(t *testing.T) {
	dentry := &Dentry{
		ParentId: 11,
		Name:     "test",
		Inode:    101,
		Type:     0x644,
		FileId:   1110,
	}

	snap := &DentryMultiSnap{
		VerSeq: 10,
		dentryList: DentryBatch{
			{Inode: 10, Type: 0x655, multiSnap: NewDentrySnap(10), Name: dentry.Name, ParentId: dentry.ParentId},
		},
	}

	data, err := dentry.Marshal()
	require.NoError(t, err)
	d1 := &Dentry{}
	err = d1.Unmarshal(data)
	require.NoError(t, err)
	require.True(t, reflect.DeepEqual(dentry, d1))

	dentry.multiSnap = snap

	data, err = dentry.Marshal()
	require.NoError(t, err)

	newDentry := &Dentry{}
	err = newDentry.Unmarshal(data)
	require.NoError(t, err)
	require.True(t, reflect.DeepEqual(dentry, newDentry))

	// test unmarshal from old data
	valData := dentry.MarshalValue()
	tmpData := valData[:12]
	err = newDentry.UnmarshalValue(tmpData)
	require.NoError(t, err)
	require.Equal(t, dentry.Inode, newDentry.Inode)

	// old data from snapshot
	tmpData = valData[:44]
	err = newDentry.UnmarshalValue(tmpData)
	require.NoError(t, err)
	require.Equal(t, dentry.Inode, newDentry.Inode)
	require.Equal(t, dentry.getSnapListLen(), newDentry.getSnapListLen())
	require.Equal(t, dentry.getSeqFiled(), newDentry.getSeqFiled())
}
