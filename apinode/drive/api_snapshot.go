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
	if d.checkError(c, func(err error) { span.Error(err) }, c.ParseArgs(args), args.Path.Clean(true)) {
		return
	}

	uid := d.userID(c, nil)
	ur, vol, err := d.getUserRouterAndVolume(ctx, uid)
	if d.checkError(c, func(err error) { span.Warn(err) }, err, ur.CanWrite()) {
		return
	}
	if d.checkError(c, func(err error) {
		span.Errorf("uid=%v create snapshot %s error: %s", uid, args.Path, err.Error())
	}, vol.CreateDirSnapshot(ctx, args.Version, args.Path.String())) {
		return
	}
	c.Respond()
}

func (d *DriveNode) handleDeleteSnapshot(c *rpc.Context) {
	args := new(ArgsSnapshot)
	ctx, span := d.ctxSpan(c)
	if d.checkError(c, func(err error) { span.Error(err) }, c.ParseArgs(args), args.Path.Clean(true)) {
		return
	}

	uid := d.userID(c, nil)
	ur, vol, err := d.getUserRouterAndVolume(ctx, uid)
	if d.checkError(c, func(err error) { span.Warn(err) }, err, ur.CanWrite()) {
		return
	}
	if d.checkError(c, func(err error) {
		span.Errorf("uid=%v delete snapshot %s error: %s", uid, args.Path, err.Error())
	}, vol.DeleteDirSnapshot(ctx, args.Version, args.Path.String())) {
		return
	}
	c.Respond()
}
