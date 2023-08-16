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
			m.EXPECT().AllocFileId().Return(nil, nil).AnyTimes()
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
	m.EXPECT().AllocFileId().Return(nil, nil).AnyTimes()

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

func TestCluster_AllocFileId(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	m := mocks.NewMockMaster(ctrl)
	newMaster = func(addr string) sdk.IMaster {
		return m
	}

	m.EXPECT().GetClusterIP().Return(&proto.ClusterIP{Cluster: "cid1"}, nil).AnyTimes()
	_, ctx := trace.StartSpanFromContext(context.TODO(), "")

	begin, end := uint64(10), uint64(100)
	expect := &proto.FileId{Begin: begin, End: end}
	m.EXPECT().AllocFileId().Return(expect, sdk.ErrInternalServerError)
	_, err := newClusterIn(ctx, "addr1", "cid1")
	require.Error(t, err)

	m.EXPECT().AllocFileId().Return(expect, nil)
	cl, err := newClusterIn(ctx, "addr1", "cid1")
	require.NoError(t, err)
	require.True(t, cl.fileId.Begin == begin)

	for idx := begin; idx < end; idx++ {
		gotId, errx := cl.allocFileId(ctx)
		require.NoError(t, errx)
		require.True(t, gotId == idx+1)
	}

	m.EXPECT().AllocFileId().Return(expect, sdk.ErrInternalServerError)
	_, err = cl.allocFileId(ctx)
	require.Error(t, err)

	newBegin := uint64(1001)
	m.EXPECT().AllocFileId().Return(&proto.FileId{Begin: newBegin}, nil)
	gotFile, err := cl.allocFileId(ctx)
	require.NoError(t, err)
	require.Equal(t, gotFile, newBegin+1)
}
