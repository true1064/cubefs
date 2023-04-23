package impl

import (
	"context"
	"testing"

	"github.com/cubefs/cubefs/apinode/sdk"
	"github.com/cubefs/cubefs/apinode/testing/mocks"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

var any = gomock.Any()

func TestClusterMgr(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mgr := newClusterMgr()
	mgr.create = func(context context.Context, cId, addr string) (sdk.ICluster, error) {
		span := trace.SpanFromContextSafe(context)
		if addr == "addr1" {
			span.Error("addr return error", sdk.ErrNoMaster)
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

	for _, tc := range testcases {
		span, ctx := trace.StartSpanFromContext(context.TODO(), "")
		span.Debugf("start execute case, cid %s, addr %s, result %v", tc.cid, tc.addr, tc.result)
		err := mgr.AddCluster(ctx, tc.cid, tc.addr)
		require.Equal(t, tc.result, err)

		if tc.result != nil {
			continue
		}

		tmpC := mgr.GetCluster(tc.cid).(*mocks.MockICluster)
		tmpC.EXPECT().Addr().Return(tc.addr).AnyTimes()
		tmpC.EXPECT().Info().AnyTimes()
	}

	cs := mgr.ListCluster()
	require.True(t, len(cs) == 2)
}
