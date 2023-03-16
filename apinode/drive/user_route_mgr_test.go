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
	"strconv"
	"testing"
)

func TestDriveNode_CreateUserSpace(t *testing.T) {
	dn := DriveNode{}
	dn.CreateUserRoute(context.Background(), "178")
}

func TestHash(t *testing.T) {
	mmap := make(map[int]map[int]int)
	for i := range [100000]struct{}{} {
		rs := strconv.Itoa(i)
		l1, l2 := hash(rs)
		if entry, ok := mmap[l1]; ok {
			if _, ok := entry[l2]; ok {
				entry[l2]++
				continue
			}
			entry[l2] = 1
			continue
		}
		mmap[l1] = make(map[int]int)
		mmap[l1][l2] = 1
	}
	l1Nums := make([]int, 0)
	l2Nums := make([]int, 0)
	for l1, l2map := range mmap {
		l1Nums = append(l1Nums, l1)
		for l2, count := range l2map {
			if l1 == 0 {
				l2Nums = append(l2Nums, l2)
			}
			t.Log(count)
		}
	}
	t.Log(l1Nums, l2Nums)
}
