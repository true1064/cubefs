package metanode

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
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
			{Inode: 10, Type: 0x655, multiSnap: NewDentrySnap(10), Name: dentry.Name, ParentId: dentry.ParentId, FileId: 11},
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
	err = newDentry.UnmarshalValue(valData)
	require.NoError(t, err)
	require.Equal(t, dentry.Inode, newDentry.Inode)
	require.Equal(t, dentry.getSnapListLen(), newDentry.getSnapListLen())
	require.Equal(t, dentry.getSeqFiled(), newDentry.getSeqFiled())
}

func Test_addVersion(t *testing.T) {
	dentry := &Dentry{
		Inode: 10,
		FileId: 11,
	}
	dentry.setVerSeq(11)

	dentry.addVersion(12)
	dentry.FileId = 13
	t.Logf("new dentry, dentry %v", dentry)
}

func Test_DentryEx_Serialization(t *testing.T) {
	dentryEx := &DentryEx{
		Dentry: &Dentry{
			ParentId: 11,
			Name:     "test",
			Inode:    101,
			Type:     0x644,
			FileId:   1110,
		},
		OldIno: 12,
	}

	data, err := dentryEx.Marshal()
	require.NoError(t, err)

	newDentryEx := &DentryEx{}
	err = newDentryEx.UnMarshal(data)
	require.NoError(t, err)
	require.True(t, reflect.DeepEqual(dentryEx, newDentryEx))
}