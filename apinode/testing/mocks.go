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

// Package testing for mocking interfaces with `go generate`
package testing

//go:generate mockgen -destination=./mocks/api_sdk.go -package=mocks -mock_names IVolume=MockIVolume,ICluster=MockICluster,ClusterManager=MockClusterManager,IMaster=MockMaster github.com/cubefs/cubefs/apinode/sdk IVolume,ICluster,ClusterManager,IMaster
//go:generate mockgen -destination=./mocks/api_cfsop.go -package=mocks -mock_names DataOp=MockDataOp,MetaOp=MockMetaOp github.com/cubefs/cubefs/apinode/sdk DataOp,MetaOp
//go:generate mockgen -destination=./mocks/api_crypto.go -package=mocks -mock_names Cryptor=MockCryptor github.com/cubefs/cubefs/apinode/crypto Cryptor

import (
	// add package to go.mod for `go generate`
	_ "github.com/golang/mock/mockgen/model"
)
