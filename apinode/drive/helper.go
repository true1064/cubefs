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
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"hash/crc32"
	"hash/crc64"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/cubefs/cubefs/apinode/sdk"
)

var (
	errOverSize  = errors.New("start out of size")
	errEndOfFile = errors.New("start end of file")
)

// returns bytes contains the End byte.
type ranges struct {
	Start, End int64
}

// parseRange parse request range header.
//
//	Range: bytes=1-1023
//	Range: bytes=1-
//	Range: bytes=-1023 // hide this
//
//	return errEndOfFile if start is equal size of file.
//	return errOverSize if start is greater than size of file.
func parseRange(header string, size int64) (ranges, error) {
	err := fmt.Errorf("invalid range header %s", header)

	index := strings.Index(header, "=")
	if index == -1 {
		return ranges{}, err
	}

	arr := strings.Split(strings.TrimSpace(header[index+1:]), "-")
	if len(arr) != 2 {
		return ranges{}, err
	}

	start, startErr := strconv.ParseInt(arr[0], 10, 64)
	end, endErr := strconv.ParseInt(arr[1], 10, 64)
	if startErr != nil {
		return ranges{}, err
	}

	if start == size {
		return ranges{Start: start}, errEndOfFile
	} else if start > size {
		return ranges{Start: start}, errOverSize
	}

	// nnn-
	if endErr != nil {
		end = size - 1
	}

	if end >= size {
		end = size - 1
	}

	if start > end || start < 0 {
		return ranges{}, err
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
	val := header.Get(HeaderCrc32)
	if val == "" {
		return reader, nil
	}
	i, err := strconv.Atoi(val)
	if err != nil || i < 0 {
		logger("invalid checksum %s", val)
		return reader, sdk.ErrBadRequest.Extend(val)
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
			err = sdk.ErrMismatchChecksum.Extend(r.crc32)
			return
		}
	}
	return
}

// A fixedReader reads fixed amount of data from R.
// Read returns EOF only N == 0.
// Return io.ErrUnexpectedEOF when the underlying R returns EOF and N > 0.
type fixedReader struct {
	R io.Reader // underlying reader
	N int64     // fixed bytes remaining
}

func (r *fixedReader) Read(p []byte) (n int, err error) {
	if r.N <= 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > r.N {
		p = p[0:r.N]
	}
	n, err = r.R.Read(p)
	r.N -= int64(n)
	if err == io.EOF && r.N > 0 {
		err = io.ErrUnexpectedEOF
	}
	return
}

func newFixedReader(r io.Reader, n int64) io.Reader { return &fixedReader{R: r, N: n} }

type checksums map[string]struct {
	hasher   hash.Hash
	checksum []byte
}

func (s checksums) writer() io.Writer {
	writers := make([]io.Writer, 0, len(s))
	for _, hasher := range s {
		writers = append(writers, hasher.hasher)
	}
	return io.MultiWriter(writers...)
}

func (s checksums) verify() error {
	be := binary.BigEndian
	he := hex.EncodeToString
	for key, hasher := range s {
		actual := hasher.hasher.Sum(nil)
		expect := hasher.checksum
		if bytes.Equal(expect, actual) {
			continue
		}
		msg := ""
		switch key {
		case "crc32":
			msg = fmt.Sprintf("%s: expect(%d) actual(%d)", key, be.Uint32(expect), be.Uint32(actual))
		case "crc64":
			msg = fmt.Sprintf("%s: expect(%d) actual(%d)", key, be.Uint64(expect), be.Uint64(actual))
		default:
			msg = fmt.Sprintf("%s: expect(%s) actual(%s)", key, he(expect), he(actual))
		}
		return sdk.ErrMismatchChecksum.Extend(msg)
	}
	return nil
}

func (s checksums) String() string {
	b := strings.Builder{}
	be := binary.BigEndian
	he := hex.EncodeToString
	for key, hasher := range s {
		b.WriteString(" ")
		expect := hasher.checksum
		switch key {
		case "crc32":
			b.WriteString(fmt.Sprintf("[%s -> %d]", key, be.Uint32(expect)))
		case "crc64":
			b.WriteString(fmt.Sprintf("[%s -> %d]", key, be.Uint64(expect)))
		default:
			b.WriteString(fmt.Sprintf("[%s -> %s]", key, he(expect)))
		}
	}
	return b.String()
}

func parseChecksums(header http.Header) (checksums, error) {
	s := make(checksums)
	for key := range header {
		key = strings.ToLower(key)
		if len(key) <= len(ChecksumPrefix) || !strings.HasPrefix(key, ChecksumPrefix) {
			continue
		}
		val := header.Get(key)
		key = key[len(ChecksumPrefix):]

		var hasher hash.Hash
		var checksum []byte
		switch key {
		case "crc32", "crc64":
			crc, err := strconv.Atoi(val)
			if err != nil || crc < 0 {
				return nil, sdk.ErrBadRequest.Extend(key, val)
			}

			if key == "crc32" {
				hasher = crc32.NewIEEE()
				checksum = binary.BigEndian.AppendUint32(nil, uint32(crc))[:hasher.Size()]
			} else {
				hasher = crc64.New(crc64.MakeTable(crc64.ISO))
				checksum = binary.BigEndian.AppendUint64(nil, uint64(crc))[:hasher.Size()]
			}

		case "md5", "sha1", "sha256":
			cs, err := hex.DecodeString(val)
			if err != nil {
				return nil, sdk.ErrBadRequest.Extend(key, val)
			}

			switch key {
			case "md5":
				hasher = md5.New()
			case "sha1":
				hasher = sha1.New()
			case "sha256":
				hasher = sha256.New()
			}

			if len(cs) != hasher.Size() {
				return nil, sdk.ErrBadRequest.Extend(key, val)
			}
			checksum = cs[:]

		default:
			return nil, sdk.ErrBadRequest.Extend("not supported", key)
		}

		s[key] = struct {
			hasher   hash.Hash
			checksum []byte
		}{hasher, checksum}
	}
	return s, nil
}
