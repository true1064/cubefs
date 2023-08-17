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
	"path/filepath"
	"strings"
	"sync"

	"github.com/cubefs/cubefs/apinode/sdk"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/util/task"
	"github.com/cubefs/cubefs/blobstore/util/taskpool"
	"github.com/cubefs/cubefs/util"
)

// ArgsMkDir make dir argument.
type ArgsMkDir struct {
	Path      FilePath `json:"path"`
	Recursive bool     `json:"recursive,omitempty"`
}

// POST /v1/files/mkdir?path=/abc&recursive=bool
func (d *DriveNode) handleMkDir(c *rpc.Context) {
	ctx, span := d.ctxSpan(c)
	args := new(ArgsMkDir)
	if d.checkError(c, nil, c.ParseArgs(args)) {
		return
	}
	if d.checkError(c, func(err error) { span.Info(args.Path, err) }, args.Path.Clean()) {
		return
	}

	uid := d.userID(c)
	ur, vol, err := d.getUserRouterAndVolume(ctx, uid)
	if d.checkError(c, func(err error) { span.Warn(err) }, err) {
		return
	}
	root := ur.RootFileID

	span.Info("to makedir", args)
	_, _, err = d.createDir(ctx, vol, root, args.Path.String(), args.Recursive)
	if d.checkError(c, func(err error) {
		span.Errorf("create dir %s error: %s, uid=%s recursive=%v", args.Path, err.Error(), uid, args.Recursive)
	}, err) {
		return
	}
	d.out.Publish(ctx, makeOpLog(OpCreateDir, d.requestID(c), uid, args.Path.String()))
	c.Respond()
}

// ArgsDelete file delete argument.
type ArgsDelete struct {
	Path      FilePath `json:"path"`
	Recursive bool     `json:"recursive,omitempty"`
}

func (d *DriveNode) handleFilesDelete(c *rpc.Context) {
	args := new(ArgsDelete)
	if d.checkError(c, nil, c.ParseArgs(args)) {
		return
	}
	ctx, span := d.ctxSpan(c)
	if d.checkError(c, func(err error) { span.Info(args.Path, err) }, args.Path.Clean()) {
		return
	}
	span.Info("to delete", args)

	if args.Recursive {
		d.recursivelyDelete(c, args.Path)
		return
	}

	ur, vol, err := d.getUserRouterAndVolume(ctx, d.userID(c))
	if d.checkError(c, func(err error) { span.Warn(err) }, err) {
		return
	}
	root := ur.RootFileID

	var info *sdk.DirInfo
	if d.checkFunc(c, func(err error) { span.Error(err) },
		func() error { info, err = deleteFile(ctx, vol, root, args.Path.String()); return err }) {
		return
	}

	op := OpDeleteFile
	if info.IsDir() {
		op = OpDeleteDir
	}
	d.out.Publish(ctx, makeOpLog(op, d.requestID(c), d.userID(c), args.Path.String()))
	c.Respond()
}

type delDir struct {
	parent Inode
	name   string
}

func (d *DriveNode) recursivelyDelete(c *rpc.Context, path FilePath) {
	ctx, span := d.ctxSpan(c)

	ur, vol, err := d.getUserRouterAndVolume(ctx, d.userID(c))
	if d.checkError(c, func(err error) { span.Warn(err) }, err) {
		return
	}
	root := ur.RootFileID
	_, err = d.lookupDir(ctx, vol, root, path.String())
	if d.checkError(c, func(err error) { span.Warn(err) }, err) {
		return
	}

	dirName, name := path.Split()
	ino := root
	if dirName != "" && dirName != "/" {
		parent, errx := d.lookup(ctx, vol, root, dirName.String())
		if d.checkError(c, func(err error) { span.Warn(err) }, errx) {
			return
		}
		ino = Inode(parent.Inode)
	}

	nextLayer := []delDir{{parent: ino, name: name}}
	layerDirs := make([][]delDir, 0, 4)
	layerDirs = append(layerDirs, nextLayer[:])

	for len(nextLayer) > 0 {
		thisLayer := make([]delDir, 0, 4)
		for ii := range nextLayer {
			dir := nextLayer[ii]

			var dirInfo *sdk.DirInfo
			dirInfo, err = d.lookup(ctx, vol, dir.parent, dir.name)
			if d.checkError(c, func(err error) { span.Warn(err) }, err) {
				return
			}
			if !dirInfo.IsDir() {
				span.Info("sub name is file:", dir)
				d.respError(c, sdk.ErrNotEmpty)
				return
			}

			var files []FileInfo
			files, err = d.listDir(ctx, dirInfo.Inode, vol, "", 10000)
			if d.checkError(c, func(err error) { span.Warn(err) }, err) {
				return
			}
			if len(files) >= 10000 {
				span.Error("too many dirs in dir:", dirInfo)
				d.respError(c, sdk.ErrInternalServerError.Extend("too many dirs"))
				return
			}

			for _, file := range files {
				if !file.IsDir() {
					span.Info("sub name was not dir:", file)
					d.respError(c, sdk.ErrNotEmpty)
					return
				}

				dd := delDir{parent: Inode(dirInfo.Inode), name: file.Name}
				thisLayer = append(thisLayer, dd)
			}
		}
		if len(thisLayer) > 0 {
			layerDirs = append(layerDirs, thisLayer[:])
		}
		nextLayer = thisLayer
	}

	span.Debug("to delete dirs", layerDirs)
	for ii := len(layerDirs) - 1; ii >= 0; ii-- {
		tasks := make([]func() error, 0, len(layerDirs[ii]))
		for _, dir := range layerDirs[ii] {
			taskDir := dir
			tasks = append(tasks, func() error {
				return vol.Delete(ctx, taskDir.parent.Uint64(), taskDir.name, true)
			})
		}
		if d.checkError(c, func(err error) { span.Error(err) }, task.Run(ctx, tasks...)) {
			return
		}
	}

	d.out.Publish(ctx, makeOpLog(OpDeleteDir, d.requestID(c), d.userID(c), path.String()))
	c.Respond()
}

type ArgsBatchDelete struct {
	Paths []FilePath `json:"paths"`
}

type ErrorEntry struct {
	Path    string `json:"path"`
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type BatchDeleteResult struct {
	Deleted []string     `json:"deleted"`
	Errors  []ErrorEntry `json:"error"`
}

func (d *DriveNode) handleBatchDelete(c *rpc.Context) {
	args := new(ArgsBatchDelete)
	if d.checkError(c, nil, c.ParseArgs(args)) {
		return
	}
	ctx, span := d.ctxSpan(c)
	ur, vol, err := d.getUserRouterAndVolume(ctx, d.userID(c))
	if d.checkError(c, func(err error) { span.Warnf("get root inode and volume return error: %v", err) }, err) {
		return
	}
	root := ur.RootFileID

	type result struct {
		path string
		err  error
	}

	var wg sync.WaitGroup
	ch := make(chan result, len(args.Paths))
	pool := taskpool.New(util.Min(len(args.Paths), maxTaskPoolSize), maxTaskPoolSize)
	defer pool.Close()
	wg.Add(len(args.Paths))
	for _, path := range args.Paths {
		name := path
		pool.Run(func() {
			defer wg.Done()
			arg := name.String()
			if err := name.Clean(); err != nil {
				span.Infof("invalid path %s", arg)
				ch <- result{arg, err}
				return
			}
			_, err := deleteFile(ctx, vol, root, name.String())
			if err == sdk.ErrNotFound {
				span.Infof("delete file %s rootIno %d return error: not found, ignore this error", arg, root)
				err = nil
			} else {
				span.Debugf("delete file %s rootIno %d return success", arg, root)
			}
			ch <- result{arg, err}
			if err != nil {
				span.Errorf("delete %s error: %v, uid=%s", name, err, d.userID(c))
			}
		})
	}

	wg.Wait()
	close(ch)

	res := &BatchDeleteResult{
		Deleted: []string{},
		Errors:  []ErrorEntry{},
	}
	for r := range ch {
		if r.err == nil {
			res.Deleted = append(res.Deleted, r.path)
		} else {
			res.Errors = append(res.Errors, ErrorEntry{
				Path:    r.path,
				Code:    rpc.Error2HTTPError(r.err).StatusCode(),
				Message: r.err.Error(),
			})
		}
	}

	c.RespondJSON(res)
}

func deleteFile(ctx context.Context, vol sdk.IVolume, root Inode, path string) (*sdk.DirInfo, error) {
	path = filepath.Clean(path)
	if path == "" || path == "/" {
		return nil, sdk.ErrInvalidPath
	}
	ino := root
	dir, file := filepath.Split(path)
	names := strings.Split(dir, "/")
	for _, name := range names {
		if name == "" {
			continue
		}
		info, err := vol.Lookup(ctx, ino.Uint64(), name)
		if err != nil {
			return nil, err
		}
		ino = Inode(info.Inode)
	}

	info, err := vol.Lookup(ctx, ino.Uint64(), file)
	if err != nil {
		return nil, err
	}
	return info, vol.Delete(ctx, ino.Uint64(), file, info.IsDir())
}
