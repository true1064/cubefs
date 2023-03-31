package impl

import (
	"context"
	"testing"

	"github.com/cubefs/cubefs/apinode/sdk"
	"github.com/cubefs/cubefs/apinode/testing/mocks"
	gomock "github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

var any = gomock.Any()

func TestAddCluster(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mgr := newClusterMgr()
	mgr.create = func(context context.Context, cId, addr string) (sdk.ICluster, error) {
		if addr == "addr1" {
			return nil, sdk.ErrNoMaster
		}

		cluster := mocks.NewMockICluster(ctrl)
		cluster.EXPECT().UpdateAddr(any, "addr3").Return(sdk.ErrNoMaster).AnyTimes()
		cluster.EXPECT().UpdateAddr(any, any).Return(nil).AnyTimes()
		return cluster, nil
	}

	testcases := []struct {
		cid    string
		addr   string
		result error
	}{
		{"c1", "addr1", sdk.ErrNoMaster},
		{"c1", "addr2", nil},
		{"c2", "addr2", nil},
		{"c2", "addr2", nil},
		{"c2", "addr4", nil},
		{"c2", "addr3", sdk.ErrNoMaster},
	}

	ctx := context.TODO()
	for _, tc := range testcases {
		err := mgr.AddCluster(ctx, tc.cid, tc.addr)
		require.Equal(t, tc.result, err)

		if tc.result != nil {
			continue
		}

		tmpC := mgr.GetCluster(tc.cid).(*mocks.MockICluster)
		tmpC.EXPECT().Addr().Return(tc.addr).AnyTimes()
	}
}
