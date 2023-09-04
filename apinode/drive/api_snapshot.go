package drive

import (
	"fmt"
	"path/filepath"

	"github.com/cubefs/cubefs/apinode/sdk"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
)

type ArgsSnapshot struct {
	Path    FilePath `json:"path"`
	Version string   `json:"ver"`
}

func (d *DriveNode) handleCreateSnapshot(c *rpc.Context) {
	args := &ArgsSnapshot{}
	ctx, span := d.ctxSpan(c)

	if d.checkError(c, func(err error) { span.Error(err) }, c.ParseArgs(args)) {
		return
	}
	if d.checkError(c, func(err error) { span.Info(args.Path, err) }, args.Path.Clean()) {
		return
	}

	uid := d.userID(c)
	ur, vol, err := d.getUserRouterAndVolume(ctx, uid)
	if d.checkError(c, func(err error) { span.Warn(err) }, err, ur.CanWrite()) {
		return
	}
	root := ur.RootFileID
	path := filepath.Join(args.Path.String(), fmt.Sprintf(".cfa_snapshot_%s", args.Version))
	_, _, err = d.createDir(ctx, vol, root, path, false)
	if d.checkError(c, func(err error) {
		span.Errorf("create dir %s snapshot error: %s, uid=%s", args.Path, err.Error(), uid)
	}, err) {
		return
	}
	d.out.Publish(ctx, makeOpLog(OpCreateDir, d.requestID(c), uid, path))
	c.Respond()
}

func (d *DriveNode) handleDeleteSnapshot(c *rpc.Context) {
	args := &ArgsSnapshot{}
	ctx, span := d.ctxSpan(c)

	if d.checkError(c, func(err error) { span.Error(err) }, c.ParseArgs(args)) {
		return
	}
	if d.checkError(c, func(err error) { span.Info(args.Path, err) }, args.Path.Clean()) {
		return
	}

	uid := d.userID(c)
	ur, vol, err := d.getUserRouterAndVolume(ctx, uid)
	if d.checkError(c, func(err error) { span.Warn(err) }, err, ur.CanWrite()) {
		return
	}
	root := ur.RootFileID
	path := filepath.Join(args.Path.String(), fmt.Sprintf(".cfa_snapshot_%s", args.Version))

	var info *sdk.DirInfo
	if d.checkFunc(c, func(err error) { span.Error(err) },
		func() error { info, err = deleteFile(ctx, vol, root, path); return err }) {
		return
	}

	op := OpDeleteFile
	if info.IsDir() {
		op = OpDeleteDir
	}
	d.out.Publish(ctx, makeOpLog(op, d.requestID(c), d.userID(c), path))
	c.Respond()
}
