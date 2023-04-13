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
	"bytes"
	"crypto/rand"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"testing"

	"github.com/cubefs/cubefs/apinode/sdk"
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

func TestCrc32Reader(t *testing.T) {
	count := 0
	logger := func(string, ...interface{}) {
		count++
	}
	{
		reader := bytes.NewReader(nil)
		r, err := newCrc32Reader(nil, reader, logger)
		require.NoError(t, err)
		require.NotNil(t, r)
		require.Equal(t, 0, count)
	}
	{
		reader := bytes.NewReader(nil)
		header := make(http.Header)
		header.Add(headerCrc32, "")
		r, err := newCrc32Reader(header, reader, logger)
		require.NoError(t, err)
		require.NotNil(t, r)
	}
	{
		reader := bytes.NewReader(nil)
		header := make(http.Header)
		header.Add(headerCrc32, "-abc")
		_, err := newCrc32Reader(header, reader, logger)
		require.Error(t, err)
		require.Equal(t, 1, count)
	}
	{
		buff := make([]byte, 128)
		rand.Read(buff)
		hasher := crc32.NewIEEE()
		hasher.Write(buff)
		reader := bytes.NewReader(buff)
		header := make(http.Header)
		header.Add(headerCrc32, fmt.Sprintf("%d", hasher.Sum32()))
		r, err := newCrc32Reader(header, reader, logger)
		require.NoError(t, err)
		b, err := io.ReadAll(r)
		require.NoError(t, err)
		require.Equal(t, buff, b)
	}
	{
		buff := make([]byte, 128)
		rand.Read(buff)
		hasher := crc32.NewIEEE()
		hasher.Write(buff)
		reader := bytes.NewReader(buff)
		header := make(http.Header)
		header.Add(headerCrc32, fmt.Sprintf("%d", hasher.Sum32()+1))
		r, err := newCrc32Reader(header, reader, logger)
		require.NoError(t, err)
		_, err = io.ReadAll(r)
		require.ErrorIs(t, sdk.ErrMismatchChecksum, err)
		require.Equal(t, 2, count)
	}
}
