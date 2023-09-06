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

package util_test

import (
	"os"
	"testing"

	"github.com/cubefs/cubefs/util"
	"github.com/stretchr/testify/require"
)

func TestGetMemInfo(t *testing.T) {
	total, used, err := util.GetMemInfo()
	require.NoError(t, err)
	require.NotEqual(t, total, 0)
	require.LessOrEqual(t, used, total)
}

func TestGetProcessMemory(t *testing.T) {
	used, err := util.GetProcessMemory(os.Getpid())
	require.NoError(t, err)
	require.NotEqual(t, 0, used)
}
