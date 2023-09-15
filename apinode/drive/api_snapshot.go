package drive

import (
	"github.com/cubefs/cubefs/blobstore/common/rpc"
)

type ArgsSnapshot struct {
	Path    FilePath `json:"path"`
	Version string   `json:"ver"`
}

func (d *DriveNode) handleCreateSnapshot(c *rpc.Context) {
	args := new(ArgsSnapshot)
	ctx, span := d.ctxSpan(c)
	if d.checkError(c, func(err error) { span.Error(err) }, c.ParseArgs(args), args.Path.Clean()) {
		return
	}

	uid := d.userID(c)
	ur, vol, err := d.getUserRouterAndVolume(ctx, uid)
	if d.checkError(c, func(err error) { span.Warn(err) }, err, ur.CanWrite()) {
		return
	}
	if d.checkError(c, func(err error) {
		span.Errorf("uid=%s create snapshot %s error: %s", uid, args.Path, err.Error())
	}, vol.CreateDirSnapshot(ctx, args.Version, args.Path.String())) {
		return
	}
	c.Respond()
}

func (d *DriveNode) handleDeleteSnapshot(c *rpc.Context) {
	args := new(ArgsSnapshot)
	ctx, span := d.ctxSpan(c)

	if d.checkError(c, func(err error) { span.Info(args, err) },
		c.ParseArgs(args), args.Path.Clean()) {
		return
	}

	uid := d.userID(c)
	ur, vol, err := d.getUserRouterAndVolume(ctx, uid)
	if d.checkError(c, func(err error) { span.Warn(err) }, err, ur.CanWrite()) {
		return
	}
	if d.checkError(c, func(err error) {
		span.Errorf("uid=%s delete snapshot %s error: %s", uid, args.Path, err.Error())
	}, vol.DeleteDirSnapshot(ctx, args.Version, args.Path.String())) {
		return
	}
	c.Respond()
}
