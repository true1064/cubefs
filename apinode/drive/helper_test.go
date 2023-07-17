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
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/stretchr/testify/require"
)

func TestHelperParseRange(t *testing.T) {
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
		{"bytes=1025-10000", 1024, true, ranges{1025, 0}},
		{"bytes=1025-", 1024, true, ranges{1025, 0}},
		{"bytes=1025-10000", 1025, true, ranges{1025, 0}},

		{"bytes=-1024", 1024, true, ranges{}},
		{"bytes=-100", 1024, true, ranges{}},

		{"bytes=0-", 1024, false, ranges{0, 1023}},
		{"bytes=1000-", 1024, false, ranges{1000, 1023}},
		{"bytes=1-100", 1024, false, ranges{1, 100}},
		{"bytes=0-102400", 1024, false, ranges{0, 1023}},
	}

	for _, cs := range cases {
		r, err := parseRange(cs.header, cs.size)
		if cs.err {
			require.Error(t, err, cs.header)
		}
		require.Equal(t, cs.ranges, r, cs.header)
	}

	{
		r, err := parseRange("bytes=1000-", 1)
		require.ErrorIs(t, err, errOverSize)
		require.Equal(t, int64(1000), r.Start)
	}
}

func TestHelperCrc32Reader(t *testing.T) {
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
		header.Add(HeaderCrc32, "")
		r, err := newCrc32Reader(header, reader, logger)
		require.NoError(t, err)
		require.NotNil(t, r)
	}
	{
		reader := bytes.NewReader(nil)
		header := make(http.Header)
		header.Add(HeaderCrc32, "-abc")
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
		header.Add(HeaderCrc32, fmt.Sprintf("%d", hasher.Sum32()))
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
		header.Add(HeaderCrc32, fmt.Sprintf("%d", hasher.Sum32()+1))
		r, err := newCrc32Reader(header, reader, logger)
		require.NoError(t, err)
		_, err = io.ReadAll(r)
		require.Error(t, err)
		require.Equal(t, 2, count)
	}
}

func TestHelperFixedReader(t *testing.T) {
	for _, size := range []int64{-100, -1, 0} {
		r := newFixedReader(nil, size)
		buff, err := io.ReadAll(r)
		require.NoError(t, err)
		require.Equal(t, 0, len(buff))
	}
	data := make([]byte, 1024)
	for _, size := range []int64{0, 1, 10, 1023, 1024} {
		r := newFixedReader(bytes.NewReader(data), size)
		buff, err := io.ReadAll(r)
		require.NoError(t, err)
		require.Equal(t, int(size), len(buff))
	}
	for _, size := range []int64{1025, 10000, 1 << 20} {
		r := newFixedReader(bytes.NewReader(data), size)
		_, err := io.ReadAll(r)
		require.ErrorIs(t, io.ErrUnexpectedEOF, err)
	}
}

func TestHelperChecksums(t *testing.T) {
	data := []byte("checksums")
	valCrc32 := 185260686
	valCrc64 := 2162653388653163224
	valMD5 := "42b8d9faf974b929ed89a56df591d9a7"
	valSHA1 := "1da7765e3ca71ed76395bc56dae69d64732beff8"
	valSHA256 := "d3beb16ca27a9fc332b55f526e1c8da6db0b2f58d50c9d27d59e15e23a4e35a8"

	for _, cs := range []struct {
		header       []string
		parseStatus  int
		verifyStatus int
	}{
		{[]string{"sha224", "1"}, sdk.ErrBadRequest.Status, 0},
		{[]string{"crc32", "invalid"}, sdk.ErrBadRequest.Status, 0},
		{[]string{"crc64", "-100"}, sdk.ErrBadRequest.Status, 0},
		{[]string{"sha1", "invalid"}, sdk.ErrBadRequest.Status, 0},
		{[]string{"sha1", valSHA1 + "x"}, sdk.ErrBadRequest.Status, 0},
		{[]string{"sha1", valSHA1 + "0a"}, sdk.ErrBadRequest.Status, 0},

		{[]string{"crc32", fmt.Sprint(valCrc32 + 1)}, 0, sdk.ErrMismatchChecksum.Status},
		{[]string{"crc64", fmt.Sprint(valCrc64 - 100)}, 0, sdk.ErrMismatchChecksum.Status},
		{[]string{"md5", "42b8d9faf974b929ed89a56df591d9a0"}, 0, sdk.ErrMismatchChecksum.Status},

		{[]string{}, 0, 0},
		{[]string{"", "1"}, 0, 0},
		{[]string{"md5", valMD5}, 0, 0},
		{[]string{"sha1", valSHA1, "sha256", valSHA256}, 0, 0},
		{[]string{
			"crc32", fmt.Sprint(valCrc32), "crc64", fmt.Sprint(valCrc64),
			"md5", valMD5, "sha1", valSHA1, "sha256", valSHA256,
		}, 0, 0},
	} {
		h := make(http.Header)
		for ii := 0; ii < len(cs.header); ii += 2 {
			h.Set(ChecksumPrefix+cs.header[ii], cs.header[ii+1])
		}
		s, err := parseChecksums(h)
		if cs.parseStatus > 0 {
			require.Equal(t, cs.parseStatus, err.(rpc.HTTPError).StatusCode())
			continue
		} else {
			require.NoError(t, err)
		}

		t.Log(s)

		s.writer().Write(data)
		err = s.verify()
		if cs.verifyStatus > 0 {
			require.Equal(t, cs.verifyStatus, err.(rpc.HTTPError).StatusCode())
		} else {
			require.NoError(t, err)
		}
	}
}
