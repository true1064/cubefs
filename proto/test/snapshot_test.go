package test

import (
	"encoding/json"
	"github.com/cubefs/cubefs/proto"
	"github.com/stretchr/testify/require"
	"reflect"
	"testing"
)

func Test_DelVerMarshal(t *testing.T) {
	v := &proto.DelVer{
		DelVer: 10,
		Vers: []*proto.VersionInfo{
			{Ver: 10, DelTime: 11, Status: proto.VersionInit},
			{Ver: 11, DelTime: 12, Status: proto.VersionNormal},
			{Ver: 12, DelTime: 13, Status: proto.VersionMarkDelete},
		},
	}

	data, err := json.Marshal(v)
	require.NoError(t, err)

	v1 := &proto.DelVer{}
	err = json.Unmarshal(data, v1)
	require.NoError(t, err)

	for idx, e := range v.Vers {
		e1 := v1.Vers[idx]
		require.True(t, reflect.DeepEqual(e, e1))
	}
}
