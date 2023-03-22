package drive

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/cubefs/blobstore/util/taskpool"
	"github.com/cubefs/cubefs/apinode/sdk"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/util"
)

const (
	readOnlyPerm   = "rd"
	readWritePerm  = "rw"
	sharedPrefix   = "x-cfa-shared-"
	sharedFilePath = "/.usr/share"
)

type userPerm struct {
	uid  string
	perm string
}

type ListShareResult struct {
	Files []SharedFileInfo `json:"files"`
}

func (d *DriveNode) handleShare(c *rpc.Context) {
	ctx, span := d.ctxSpan(c)
	args := new(ArgsShare)
	if err := c.ParseArgs(args); err != nil {
		span.Errorf("parse args error: %v", err)
		c.RespondStatus(http.StatusBadRequest)
		return
	}
	uid := string(d.userID(c))

	var (
		perms  []userPerm
		xattrs map[string]string
	)
	for _, s := range strings.Split(args.Perm, ",") {
		p := strings.Split(s, ":")
		if len(p) != 2 {
			span.Errorf("invalid perm=%s", args.Perm)
			c.RespondStatus(http.StatusBadRequest)
			return
		}
		if p[1] != readOnlyPerm && p[1] != readWritePerm {
			span.Errorf("invalid perm=%s", args.Perm)
			c.RespondStatus(http.StatusBadRequest)
			return
		}
		if p[0] == string(uid) {
			continue
		}
		perms = append(perms, userPerm{p[0], p[1]})
		xattrs[fmt.Sprintf("%s%s", sharedPrefix, p[0])] = p[1]
	}
	n := len(perms)
	if n == 0 {
		span.Errorf("empty perm %s", args.Perm)
		c.RespondStatus(http.StatusBadRequest)
		return
	}

	rootIno, vol, err := d.getRootInoAndVolume(string(uid))
	if err != nil {
		span.Errorf("get filepath and volume error: %v", err)
		c.RespondError(err)
		return
	}
	dirInfo, err := d.lookup(ctx, vol, rootIno, args.Path)
	if err != nil {
		span.Errorf("lookup path error: %v, path=%s", err, args.Path)
		c.RespondError(err)
		return
	}
	if err = vol.BatchSetXAttr(ctx, dirInfo.Inode, xattrs); err != nil {
		span.Errorf("batch setxattr error: %v", err)
		c.RespondError(err)
		return
	}
	pool := taskpool.New(util.Min(n, maxTaskPoolSize), n)
	defer pool.Close()
	var (
		wg    sync.WaitGroup
		errCh chan error
	)
	errCh = make(chan error, n)
	wg.Add(n)

	for i := 0; i < len(perms); i++ {
		perm := perms[i]
		pool.Run(func() {
			defer wg.Done()
			rootIno, vol, err := d.getRootInoAndVolume(perm.uid)
			if err != nil {
				errCh <- err
				return
			}
			dirInfo, err := d.lookup(ctx, vol, rootIno, sharedFilePath)
			if err != nil {
				errCh <- err
				return
			}
			if err = vol.SetXAttr(ctx, dirInfo.Inode, fmt.Sprintf("%s-%s", uid, args.Path), perm.perm); err != nil {
				errCh <- err
			} else {
				errCh <- nil
			}
			return
		})
	}
	wg.Wait()
	for err := range errCh {
		if err != nil {
			span.Errorf("set xattr error: %v", err)
			c.RespondError(err)
			return
		}
	}
	c.Respond()
}

func (d *DriveNode) handleUnShare(c *rpc.Context) {
	ctx, span := d.ctxSpan(c)
	args := new(ArgsUnShare)
	if err := c.ParseArgs(args); err != nil {
		span.Errorf("parse args error: %v", err)
		c.RespondStatus(http.StatusBadRequest)
		return
	}
	uid := string(d.userID(c))

	rootIno, vol, err := d.getRootInoAndVolume(uid)
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
	var users []string
	if args.Users == "" {
		xattrs, err := vol.GetXAttrMap(ctx, dirInfo.Inode)
		if err != nil {
			span.Errorf("get xattr error: %v, path: %s", err, args.Path)
			c.RespondError(err)
			return
		}

		for k, _ := range xattrs {
			if strings.HasPrefix(k, sharedPrefix) {
				users = append(users, strings.TrimPrefix(k, sharedPrefix))
			}
		}
	} else {
		users = strings.Split(args.Users, ",")
	}
	n := len(users)
	if n == 0 {
		// don't need to unshare anything
		c.Respond()
		return
	}
	pool := taskpool.New(util.Min(n, maxTaskPoolSize), n)
	errCh := make(chan error, n)
	var (
		wg     sync.WaitGroup
		delKey []string
	)
	wg.Add(n)
	for i := 0; i < n; i++ {
		user := users[i]
		delKey = append(delKey, fmt.Sprintf("%s%s", sharedPrefix, user))
		pool.Run(func() {
			defer wg.Done()
			rootIno, vol, err := d.getRootInoAndVolume(user)
			if err != nil {
				if err == sdk.ErrNotFound {
					errCh <- nil
				} else {
					errCh <- err
				}
				return
			}
			info, err := d.lookup(ctx, vol, rootIno, sharedFilePath)
			if err != nil {
				errCh <- err
				return
			}
			err = vol.DeleteXAttr(ctx, info.Inode, fmt.Sprintf("%s-%s", uid, args.Path))
			errCh <- err
			return
		})
	}
	wg.Wait()

	for err := range errCh {
		if err != nil {
			c.RespondError(err)
			return
		}
	}

	if err := vol.BatchDeleteXAttr(ctx, dirInfo.Inode, delKey); err != nil {
		span.Errorf("batch delete xattr error: %v", err)
		c.RespondError(err)
		return
	}
	c.Respond()
}

func (d *DriveNode) handleListShare(c *rpc.Context) {
	ctx, span := d.ctxSpan(c)
	uid := string(d.userID(c))

	rootIno, vol, err := d.getRootInoAndVolume(uid)
	if err != nil {
		span.Errorf("get user router error: %v, uid=%s", err, uid)
		c.RespondError(err)
		return
	}

	dirInfo, err := d.lookup(ctx, vol, rootIno, sharedFilePath)
	if err != nil {
		span.Errorf("lookup path=%s error: %v", sharedFilePath, err)
		c.RespondError(err)
		return
	}
	xattrs, err := vol.GetXAttrMap(ctx, dirInfo.Inode)
	if err != nil {
		span.Errorf("get xattr map path=%s error: %v", sharedFilePath, err)
		c.RespondError(err)
		return
	}

	var (
		sharedFileInfos []SharedFileInfo
		wg              sync.WaitGroup
	)
	for k, perm := range xattrs {
		s := strings.SplitN(k, "-", 2)
		if len(s) != 2 {
			continue
		}
		if perm != readOnlyPerm && perm != readWritePerm {
			continue
		}
		sharedFileInfos = append(sharedFileInfos, SharedFileInfo{
			Path:  s[1],
			Owner: s[0],
			Perm:  perm,
		})
	}
	n := len(sharedFileInfos)
	pool := taskpool.New(util.Min(n, maxTaskPoolSize), n)
	defer pool.Close()
	wg.Add(n)
	for i := 0; i < n; i++ {
		fileInfo := &sharedFileInfos[i]
		pool.Run(func() {
			defer wg.Done()
			rootIno, vol, err := d.getRootInoAndVolume(fileInfo.Owner)
			if err != nil {
				span.Errorf("get user(%s) info error: %v", fileInfo.Owner, err)
				return
			}
			info, err := d.lookup(ctx, vol, rootIno, fileInfo.Path)
			if err != nil {
				span.Errorf("lookup path=%s error: %v, owner=%s", fileInfo.Path, err, fileInfo.Owner)
				return
			}
			inoInfo, err := vol.GetInode(ctx, info.Inode)
			if err != nil {
				span.Errorf("get inode path=%s error: %v", fileInfo.Path, err)
				return
			}
			fileInfo.ID = info.Inode
			fileInfo.Type = "file"
			if info.IsDir() {
				fileInfo.Type = "folder"
			}
			fileInfo.Size = int64(inoInfo.Size)
			fileInfo.Ctime = inoInfo.CreateTime.Unix()
			fileInfo.Atime = inoInfo.AccessTime.Unix()
			fileInfo.Mtime = inoInfo.ModifyTime.Unix()
		})
	}
	wg.Wait()

	var res ListShareResult
	for _, f := range sharedFileInfos {
		if f.ID == 0 {
			continue
		}
		res.Files = append(res.Files, f)
	}
	c.RespondJSON(res)
}

func (d *DriveNode) verifyPerm(ctx context.Context, vol sdk.IVolume, ino uint64, uid string, perm string) error {
	val, err := vol.GetXAttr(ctx, ino, fmt.Sprintf("%s%s", sharedPrefix, uid))
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
