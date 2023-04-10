package drive

import (
	"context"
	"fmt"
	"testing"

	"github.com/cubefs/cubefs/apinode/testing/mocks"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/singleflight"
)

func TestGetUserRouteInfo(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	urm, _ := NewUserRouteMgr()
	mockVol := mocks.NewMockIVolume(ctrl)
	mockClusterMgr := mocks.NewMockClusterManager(ctrl)
	d := &DriveNode{
		vol:         mockVol,
		userRouter:  urm,
		clusterMgr:  mockClusterMgr,
		groupRouter: &singleflight.Group{},
	}

	mockVol.EXPECT().Lookup(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, fmt.Errorf("look up error"))
	ur, err := d.GetUserRouteInfo(context.TODO(), "test")
	require.Nil(t, ur)
	require.Equal(t, err.Error(), "look up error")
}
