package impl

import (
	"context"
	"testing"

	"github.com/cubefs/cubefs/apinode/sdk"
	"github.com/cubefs/cubefs/apinode/testing/mocks"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestCluster_initMasterCli(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cId := "testC1"

	testCases := []struct {
		addr      string
		result    error
		cIp       *proto.ClusterIP
		returnErr error
	}{
		{"addr1", sdk.ErrBadRequest, &proto.ClusterIP{Cluster: "xxx"}, nil},
		{"addr2", sdk.ErrNoMaster, nil, master.ErrNoValidMaster},
		{"addr3", nil, &proto.ClusterIP{Cluster: cId}, nil},
	}

	_, ctx := trace.StartSpanFromContext(context.TODO(), "")

	for _, tc := range testCases {
		newMaster = func(addr string) sdk.IMaster {
			m := mocks.NewMockMaster(ctrl)
			m.EXPECT().GetClusterIP().Return(tc.cIp, tc.returnErr)
			return m
		}

		_, err := initMasterCli(ctx, cId, tc.addr)
		require.True(t, err == tc.result)
	}
}

func TestCluster_updateVols(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	m := mocks.NewMockMaster(ctrl)
	m.EXPECT().GetClusterIP().Return(&proto.ClusterIP{Cluster: "cid1"}, nil)

	newMaster = func(addr string) sdk.IMaster {
		return m
	}

	_, ctx := trace.StartSpanFromContext(context.TODO(), "")
	cl, err := newClusterIn(ctx, "addr1", "cid1")
	require.NoError(t, err)

	m.EXPECT().ListVols("").Return(nil, master.ErrNoValidMaster)
	err = cl.updateVols(ctx)
	require.True(t, err == sdk.ErrNoMaster)

	volsInfo := []*proto.VolInfo{
		{Name: "v1", Owner: "o1"},
		{Name: "v1", Owner: "o2"},
		{Name: "v2", Owner: "o2"},
		{Name: "v3", Owner: "o3"},
	}
	m.EXPECT().ListVols("").Return(volsInfo, nil).AnyTimes()

	cl.newVol = func(ctx context.Context, name, owner, addr string) (iVolume sdk.IVolume, err error) {
		if "v3" == name {
			return nil, sdk.ErrInternalServerError
		}

		v := mocks.NewMockIVolume(ctrl)
		v.EXPECT().Info().Return(&sdk.VolInfo{})
		return v, nil
	}
	err = cl.updateVols(ctx)
	require.True(t, err == sdk.ErrInternalServerError)
	require.True(t, len(cl.ListVols()) == 2)
}

func TestNewCluster(t *testing.T) {
	//ctrl := gomock.NewController(t)
	//defer ctrl.Finish()
	//
	//cId := "testC1"
	//errAddr := "errAddr"
	//
	//newMaster = func(addr string) sdk.IMaster {
	//	m := mocks.NewMockMaster(ctrl)
	//	if addr == errAddr {
	//		m.EXPECT().GetClusterIP().AnyTimes().Return(proto.ClusterIP{Cluster: "xxx"}, nil)
	//	} else {
	//		m.EXPECT().GetClusterIP().AnyTimes().Return(proto.ClusterIP{Cluster: cId}, nil)
	//	}
	//	return m
	//}
	//
	//_, ctx := trace.StartSpanFromContext(context.TODO(), "")
	//_, err := newCluster(ctx, errAddr, "c1")
	//require.True(t, err == sdk.ErrBadRequest)
}
