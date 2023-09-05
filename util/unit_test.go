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
	"testing"

	"github.com/cubefs/cubefs/util"
)

func TestIsIpv4(t *testing.T) {
	if !util.IsIPV4Addr("127.0.0.1:80") {
		t.Errorf("127.0.0.1 is a ipv4 addr")
		return
	}
	if util.IsIPV4("[1fff:0:a88:85a3::ac1f]:80") {
		t.Errorf("1fff:0:a88:85a3::ac1f is not a ipv4 addr")
		return
	}
}
