package metanode

import (
	"github.com/cubefs/cubefs/proto"
	"github.com/stretchr/testify/require"
	"reflect"
	"testing"
)

func Test_InodeDirVer_Marshal(t *testing.T) {
	i := &InodeDirVer{
		Ino: NewInode(100, 200),
	}

	items := []*proto.VersionInfo{
		{Ver: 11},
		{Ver: 12},
		{Ver: 13},
	}

	data, err := i.Marshal()
	require.NoError(t, err)
	i1 := &InodeDirVer{}
	err = i1.Unmarshal(data)
	require.NoError(t, err)
	require.True(t, reflect.DeepEqual(i, i1))

	i.DirVerList = items
	data, err = i.Marshal()
	require.NoError(t, err)

	i2 := &InodeDirVer{}
	err = i2.Unmarshal(data)
	require.NoError(t, err)
	for idx, v := range i.DirVerList {
		v2 := i2.DirVerList[idx]
		require.True(t, reflect.DeepEqual(v, v2))
	}

}
