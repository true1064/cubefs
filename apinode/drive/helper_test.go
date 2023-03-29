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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseRange(t *testing.T) {
	cases := []struct {
		header string
		size   int64
		err    bool
		ranges ranges
	}{
		{"bytes 1-111", 0, true, ranges{}},
		{"bytes=-1-", 0, true, ranges{}},
		{"bytes=1--20", 0, true, ranges{}},
		{"bytes=-", 0, true, ranges{}},
		{"bytes=x-x", 0, true, ranges{}},
		{"bytes=111-10", 1024, true, ranges{}},
		{"bytes=-1025", 1024, true, ranges{}},

		{"bytes=0-", 1024, false, ranges{0, 1023}},
		{"bytes=1000-", 1024, false, ranges{1000, 1023}},
		{"bytes=-1024", 1024, false, ranges{0, 1023}},
		{"bytes=-100", 1024, false, ranges{924, 1023}},
		{"bytes=1-100", 1024, false, ranges{1, 100}},
		{"bytes=0-102400", 1024, false, ranges{0, 1023}},
	}

	for _, cs := range cases {
		r, err := parseRange(cs.header, cs.size)
		if cs.err {
			require.Error(t, err, cs.header)
		} else {
			require.Equal(t, cs.ranges, r, cs.header)
		}
	}
}
