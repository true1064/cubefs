package drive

import (
	"context"
	"fmt"
	"net/http"

	"github.com/cubefs/cubefs/apinode/sdk"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
)

const (
	readOnlyPerm  = "rd"
	readWritePerm = "rw"
)

func (d *DriveNode) handlerShare(c *rpc.Context) {
	ctx, span := d.ctxSpan(c)
	args := new(ArgsShare)
	if err := c.ParseArgs(args); err != nil {
		span.Errorf("parse args error: %v", err)
		c.RespondStatus(http.StatusBadRequest)
		return
	}

	uid := string(d.userID(c))
	if uid == "" {
		span.Error("uid is empty")
		c.RespondStatus(http.StatusBadRequest)
		return
	}

	path, perm := args.Path, args.Perm
	if path == "" || perm == "" {
		span.Error("invalid path or perm")
		c.RespondStatus(http.StatusBadRequest)
		return
	}
}

func (d *DriveNode) handlerUnShare(c *rpc.Context) {
}

func (d *DriveNode) verifyPerm(ctx context.Context, vol sdk.Volume, path string, uid string, perm string) error {
	inodeInfo, err := vol.Lookup(ctx, path)
	if err != nil {
		return err
	}
	val, err := vol.GetXAttr(ctx, inodeInfo.Inode, fmt.Sprintf("shared-%s", uid))
	if err != nil {
		return err
	}
	if val != readOnlyPerm && val != readWritePerm {
		return sdk.ErrForbidden
	}
	if perm != val {
		return sdk.ErrForbidden
	}
	return nil
}
