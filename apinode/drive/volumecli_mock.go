// Copyright 2023 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package drive

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/cubefs/cubefs/apinode/sdk/impl"

	"github.com/cubefs/cubefs/apinode/sdk"
	"github.com/cubefs/cubefs/apinode/testing/mocks"
	"github.com/golang/mock/gomock"
)

func MockVolumeCli(tb testing.TB) sdk.IVolume {
	volumeCli := mocks.NewMockIVolume(gomock.NewController(tb))
	ca := make(map[string]VolumeAlloc)
	ca["cid1"] = map[string]int{"vid1": 10, "vid2": 8}
	ca["cid2"] = map[string]int{"vid11": 10, "vid12": 10}
	b, _ := json.Marshal(ca)
	volumeCli.EXPECT().Lookup(gomock.Any(), gomock.Any(), gomock.Any()).Return(&sdk.DirInfo{}, nil).AnyTimes()
	volumeCli.EXPECT().GetXAttr(gomock.Any(), gomock.Any(), "clusters").Return(string(b), nil).AnyTimes()
	volumeCli.EXPECT().SetXAttr(gomock.Any(), gomock.Any(), "clusters", gomock.Any()).Return(nil).AnyTimes()
	return volumeCli
}

func MockClusterCli(tb testing.TB) sdk.ClusterManager {
	ctx := context.Background()
	nc := impl.NewClusterMgr()
	err := nc.AddCluster(ctx, "cid1", "host1")
	if err != nil {
		tb.Log(err)
	}
	clusterCli := mocks.NewMockClusterManager(gomock.NewController(tb))
	clusterCli.EXPECT().GetCluster(gomock.Any()).Return(nc.GetCluster("cid"))
	return clusterCli
}
