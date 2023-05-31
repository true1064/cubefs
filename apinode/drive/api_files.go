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
	"github.com/cubefs/cubefs/apinode/sdk"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
)

// ArgsMkDir make dir argument.
type ArgsMkDir struct {
	Path       FilePath `json:"path"`
	XRecursive string   `json:"recursive,omitempty"`
	Recursive  bool     `json:"-"`
}

// POST /v1/files/mkdir?path=/abc&recursive=bool
func (d *DriveNode) handleMkDir(c *rpc.Context) {
	ctx, span := d.ctxSpan(c)
	args := new(ArgsMkDir)
	if d.checkError(c, nil, c.ParseArgs(args)) {
		return
	}

	t := d.encrypTransmitter(c)
	if d.checkFunc(c, func(err error) { span.Info(err) },
		func() error { return decodeHex(&args.Recursive, args.XRecursive, t) },
		func() error { return args.Path.Clean(t) }) {
		return
	}

	uid := d.userID(c)
	rootIno, vol, err := d.getRootInoAndVolume(ctx, uid)
	if d.checkError(c, func(err error) { span.Warn(err) }, err) {
		return
	}

	span.Info("to makedir", args)
	_, err = d.createDir(ctx, vol, rootIno, args.Path.String(), args.Recursive)
	if d.checkError(c, func(err error) {
		span.Errorf("create dir %s error: %s, uid=%s recursive=%v", args.Path, err.Error(), uid, args.Recursive)
	}, err) {
		return
	}
	d.out.Publish(ctx, makeOpLog(OpCreateDir, uid, args.Path.String()))
	c.Respond()
}

// ArgsDelete file delete argument.
type ArgsDelete struct {
	Path FilePath `json:"path"`
}

func (d *DriveNode) handleFilesDelete(c *rpc.Context) {
	args := new(ArgsDelete)
	if d.checkError(c, nil, c.ParseArgs(args)) {
		return
	}
	ctx, span := d.ctxSpan(c)

	t := d.encrypTransmitter(c)
	if d.checkFunc(c, func(err error) { span.Info(err) },
		func() error { return args.Path.Clean(t) }) {
		return
	}
	if args.Path.IsDir() {
		args.Path = args.Path[:len(args.Path)-1]
	}
	span.Info("to delete", args)

	root, vol, err := d.getRootInoAndVolume(ctx, d.userID(c))
	if d.checkError(c, func(err error) { span.Warn(err) }, err) {
		return
	}

	dir, name := args.Path.Split()
	parentIno := root
	if dir != "" && dir != "/" {
		parent, errx := d.lookup(ctx, vol, root, dir.String())
		if errx != nil {
			span.Warn(errx)
			d.respError(c, errx)
			return
		}
		parentIno = Inode(parent.Inode)
	}

	var file *sdk.DirInfo
	if d.checkFunc(c, func(err error) { span.Error(err) },
		func() error { file, err = d.lookup(ctx, vol, parentIno, name); return err },
		func() error { return vol.Delete(ctx, parentIno.Uint64(), name, file.IsDir()) }) {
		return
	}

	op := OpDeleteFile
	if file.IsDir() {
		op = OpDeleteDir
	}
	d.out.Publish(ctx, makeOpLog(op, d.userID(c), args.Path.String()))
	c.Respond()
}
