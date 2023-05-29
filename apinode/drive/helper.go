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
	"errors"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/cubefs/cubefs/apinode/sdk"
)

var errOverSize = errors.New("start out of size")

// returns bytes contains the End byte.
type ranges struct {
	Start, End int64
}

// parseRange parse request range header.
//   Range: bytes=1-1023
//   Range: bytes=-1023
//   Range: bytes=1-
func parseRange(header string, size int64) (ranges, error) {
	index := strings.Index(header, "=")
	if index == -1 {
		return ranges{}, fmt.Errorf("invalid range header %s", header)
	}

	arr := strings.Split(strings.TrimSpace(header[index+1:]), "-")
	if len(arr) != 2 {
		return ranges{}, fmt.Errorf("invalid range header %s", header)
	}

	start, startErr := strconv.ParseInt(arr[0], 10, 64)
	end, endErr := strconv.ParseInt(arr[1], 10, 64)
	if startErr != nil && endErr != nil {
		return ranges{}, fmt.Errorf("invalid range header %s", header)
	}

	// -nnn or nnn-
	if startErr != nil {
		start = size - end
		end = size - 1
	} else if endErr != nil {
		if start >= size {
			return ranges{Start: start}, errOverSize
		}
		end = size - 1
	}

	if end >= size {
		end = size - 1
	}

	if start > end || start < 0 {
		return ranges{}, fmt.Errorf("invalid range header %s", header)
	}

	return ranges{Start: start, End: end}, nil
}

type (
	logFunc     func(format string, v ...interface{})
	crc32Reader struct {
		crc32  uint32
		reader io.Reader
		hasher hash.Hash32
		logger logFunc
	}
)

func newCrc32Reader(header http.Header, reader io.Reader, logger logFunc) (io.Reader, error) {
	val := header.Get(headerCrc32)
	if val == "" {
		return reader, nil
	}
	i, err := strconv.Atoi(val)
	if err != nil || i < 0 {
		logger("invalid checksum %s", val)
		return reader, sdk.ErrBadRequest
	}
	return &crc32Reader{
		crc32:  uint32(i),
		reader: reader,
		hasher: crc32.NewIEEE(),
		logger: logger,
	}, nil
}

func (r *crc32Reader) Read(p []byte) (n int, err error) {
	n, err = r.reader.Read(p)
	if n > 0 {
		r.hasher.Write(p[:n])
	}
	if err == io.EOF {
		if actual := r.hasher.Sum32(); actual != r.crc32 {
			r.logger("mismatch checksum wont=%d actual=%d", r.crc32, actual)
			err = sdk.ErrMismatchChecksum
			return
		}
	}
	return
}
