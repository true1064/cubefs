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
	"io"

	"github.com/cubefs/cubefs/apinode/crypto"
	"github.com/cubefs/cubefs/apinode/sdk"
)

type downReader struct {
	ctx    context.Context
	vol    sdk.IVolume
	err    error
	inode  uint64
	offset uint64
}

func (r *downReader) Read(p []byte) (n int, err error) {
	if r.err != nil {
		err = r.err
		return
	}
	n, err = r.vol.ReadFile(r.ctx, r.inode, r.offset, p)
	if err != nil {
		r.err = err
	}
	if r.err == nil && n < len(p) {
		r.err = io.EOF
		err = r.err
	}
	r.offset += uint64(n)
	return
}

func makeFileReader(ctx context.Context, vol sdk.IVolume, ino, off uint64) io.Reader {
	return &downReader{ctx: ctx, vol: vol, inode: ino, offset: off}
}

func (d *DriveNode) makeBlockedReader(ctx context.Context, vol sdk.IVolume, ino, off uint64, cipherKey []byte) (io.Reader, error) {
	remain := off % crypto.BlockSize
	off = off - remain
	var r io.Reader = &downReader{ctx: ctx, vol: vol, inode: ino, offset: off}
	r, err := d.cryptor.FileDecryptor(cipherKey, r)
	if err != nil {
		return nil, err
	}
	if remain > 0 {
		if _, err := io.CopyN(io.Discard, r, int64(remain)); err != nil {
			return nil, err
		}
	}
	return r, nil
}

func (d *DriveNode) blockReader(ctx context.Context, vol sdk.IVolume, inode *sdk.InodeInfo, off uint64,
	cipherKey []byte) (io.Reader, error) {
	remain := off % crypto.BlockSize
	off = off - remain
	if inode.Size <= off {
		return io.LimitReader(nil, 0), nil
	}

	r, err := d.cryptor.FileDecryptor(cipherKey, makeFileReader(ctx, vol, inode.Inode, off))
	if err != nil {
		return nil, err
	}
	return io.LimitReader(r, crypto.BlockSize), nil
}

func (d *DriveNode) blockReaderFirst(ctx context.Context, vol sdk.IVolume, inode *sdk.InodeInfo, off uint64,
	cipherKey []byte) (io.Reader, error) {
	remain := off % crypto.BlockSize
	r, err := d.blockReader(ctx, vol, inode, off, cipherKey)
	if err != nil {
		return nil, err
	}
	return io.LimitReader(r, int64(remain)), nil
}

func (d *DriveNode) blockReaderLast(ctx context.Context, vol sdk.IVolume, inode *sdk.InodeInfo, off uint64,
	cipherKey []byte) (io.Reader, error) {
	remain := off % crypto.BlockSize
	r, err := d.blockReader(ctx, vol, inode, off, cipherKey)
	if err != nil {
		return nil, err
	}
	if remain > 0 {
		if _, err := io.CopyN(io.Discard, r, int64(remain)); err != nil {
			return nil, err
		}
	}
	return io.LimitReader(r, crypto.BlockSize-int64(remain)), nil
}
