package impl

import (
	"context"
	"github.com/cubefs/cubefs/apinode/sdk"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/proto"
	"syscall"
)

type InodeLock struct {
	v      *volume
	id     string
	status int
}

func (l *InodeLock) Lock(ctx context.Context, inode uint64, expireTime int) error {
	span := trace.SpanFromContextSafe(ctx)
	req := &proto.InodeLockReq{
		PartitionId: 0,
		LockType:    proto.InodeLockStatus,
		Id:          l.id,
		ExpireTime:  expireTime,
		Inode:       inode,
	}

	err := l.v.mw.SetInodeLock_ll(inode, req)
	if err == nil {
		return nil
	}

	if err == syscall.EEXIST || err == syscall.EINVAL {
		span.Warnf("set inode lock failed, inode %d, req %v, err %s", inode, req, err.Error())
		return sdk.ErrConflict
	}

	span.Errorf("set inode lock failed, unknown error, inode %d, req %v, err %s", inode, req, err.Error())
	return sdk.ErrInternalServerError
}

func (l *InodeLock) UnLock(ctx context.Context, inode uint64) error {
	span := trace.SpanFromContextSafe(ctx)
	req := &proto.InodeLockReq{
		PartitionId: 0,
		LockType:    proto.InodeUnLockStatus,
		Id:          l.id,
		Inode:       inode,
	}

	err := l.v.mw.SetInodeLock_ll(inode, req)
	if err == nil {
		return nil
	}

	if err == syscall.EINVAL {
		span.Warnf("unlock inode lock failed, maybe already be used by other instance, ino %d, id %s, err %s", inode, l.id, err.Error())
		return sdk.ErrConflict
	}

	span.Errorf("unlock inode lock failed, ino %d, id %s, err %s", inode, l.id, err.Error())
	return sdk.ErrInternalServerError
}
