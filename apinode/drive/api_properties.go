package drive

import (
	"net/http"
	"strings"

	"github.com/cubefs/cubefs/blobstore/common/rpc"
)

const (
	userPropertyPrefix = "x-cfa-meta-"
)

type GetPropertiesResult struct {
	ID         uint64            `json:"id"`
	Name       string            `json:"name"`
	Type       string            `json:"type"`
	Size       int64             `json:"size"`
	Ctime      int64             `json:"ctime"`
	Mtime      int64             `json:"mtime"`
	Atime      int64             `json:"atime"`
	Properties map[string]string `json:"properties"`
}

func (d *DriveNode) handleSetProperties(c *rpc.Context) {
	ctx, span := d.ctxSpan(c)
	uid := string(d.userID(c))

	args := new(ArgsSetProperties)
	if err := c.ParseArgs(args); err != nil {
		span.Errorf("parse args error: %v", err)
		c.RespondStatus(http.StatusBadRequest)
		return
	}

	rootIno, vol, err := d.getRootInoAndVolume(ctx, uid)
	if err != nil {
		span.Errorf("get user router error: %v, uid=%s", err, uid)
		c.RespondError(err)
		return
	}

	dirInfo, err := d.lookup(ctx, vol, rootIno, args.Path)
	if err != nil {
		span.Errorf("lookup path=%s error: %v", args.Path, err)
		c.RespondError(err)
		return
	}
	xattrs := map[string]string{}
	for key, values := range c.Request.Header {
		if !strings.HasPrefix(key, userPropertyPrefix) {
			continue
		}
		xattrs[strings.TrimPrefix(key, userPropertyPrefix)] = values[0]
	}
	if err = vol.BatchSetXAttr(ctx, dirInfo.Inode, xattrs); err != nil {
		span.Errorf("batch set xattr path=%s error: %v", args.Path, err)
		c.RespondError(err)
		return
	}
	c.Respond()
}

func (d *DriveNode) handleGetProperties(c *rpc.Context) {
	ctx, span := d.ctxSpan(c)
	uid := string(d.userID(c))

	args := new(ArgsSetProperties)
	if err := c.ParseArgs(args); err != nil {
		span.Errorf("parse args error: %v", err)
		c.RespondStatus(http.StatusBadRequest)
		return
	}

	rootIno, vol, err := d.getRootInoAndVolume(ctx, uid)
	if err != nil {
		span.Errorf("get user router error: %v, uid=%s", err, uid)
		c.RespondError(err)
		return
	}

	dirInfo, err := d.lookup(ctx, vol, rootIno, args.Path)
	if err != nil {
		span.Errorf("lookup path=%s error: %v", args.Path, err)
		c.RespondError(err)
		return
	}
	xattrs, err := vol.GetXAttrMap(ctx, dirInfo.Inode)
	if err != nil {
		span.Errorf("get xattr path=%s error: %v", args.Path, err)
		c.RespondError(err)
		return
	}
	inoInfo, err := vol.GetInode(ctx, dirInfo.Inode)
	if err != nil {
		span.Errorf("get inode path=%s error: %v", args.Path, err)
		c.RespondError(err)
		return
	}
	res := GetPropertiesResult{
		ID:         inoInfo.Inode,
		Name:       dirInfo.Name,
		Type:       "file",
		Size:       int64(inoInfo.Size),
		Ctime:      inoInfo.CreateTime.Unix(),
		Mtime:      inoInfo.ModifyTime.Unix(),
		Atime:      inoInfo.AccessTime.Unix(),
		Properties: xattrs,
	}
	if dirInfo.IsDir() {
		res.Type = "folder"
	}
	c.RespondJSON(res)
}
