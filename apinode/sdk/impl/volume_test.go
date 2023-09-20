package impl

import (
	"context"
	"fmt"
	"testing"

	"github.com/cubefs/cubefs/apinode/sdk"
	"github.com/cubefs/cubefs/apinode/testing/mocks"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/sdk/data/stream"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func Test_newVolume(t *testing.T) {
	tests := []struct {
		name    string
		owner   string
		addr    string
		want    sdk.IVolume
		wantErr error
	}{
		{"v1", "o1", "addr1", nil, sdk.ErrInternalServerError},
		{"v2", "o1", "addr1", nil, sdk.ErrInternalServerError},
		{"v3", "o1", "addr1", &volume{name: "v3", owner: "o1"}, nil},
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	newExtentClient = func(cfg *stream.ExtentConfig) (op DataOp, err error) {
		if cfg.Volume == "v1" {
			return nil, master.ErrNoValidMaster
		}
		return mocks.NewMockDataOp(ctrl), nil
	}

	newMetaWrapper = func(config *meta.MetaConfig) (op MetaOp, err error) {
		if config.Volume == "v2" {
			return nil, fmt.Errorf("no vaild volume")
		}
		return mocks.NewMockMetaOp(ctrl), err
	}

	_, ctx := trace.StartSpanFromContext(context.TODO(), "")
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := newVolume(ctx, tt.name, tt.owner, tt.addr)
			require.True(t, err == tt.wantErr)
			if tt.want != nil {
				// t.Errorf("got name %s, want name %s", got.Info().Name, tt.want.Info().Name)
				require.True(t, got.Info().Name == tt.want.Info().Name)
			}
		})
	}
}
