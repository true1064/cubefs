package impl

import (
	"context"
	"github.com/cubefs/cubefs/apinode/sdk"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/meta"
)

type metaOpImp struct {
	*meta.MetaWrapper
	allocId func(ctx context.Context) (id uint64, err error)
}

func newMetaOp(config *meta.MetaConfig) (sdk.MetaOp, error) {
	mw, err := meta.NewMetaWrapper(config)
	m := &metaOpImp{
		MetaWrapper: mw,
	}
	return m, err
}

func (m *metaOpImp) CreateDentryEx(ctx context.Context, req *sdk.CreateDentryReq) (uint64, error) {
	span := trace.SpanFromContextSafe(ctx)
	fileId, err := m.allocId(ctx)
	if err != nil {
		span.Errorf("alloc id failed, err %s", err.Error())
		return 0, err
	}

	createReq := &proto.CreateDentryRequest{
		ParentID: req.ParentId,
		Name:     req.Name,
		Inode:    req.Inode,
		OldIno:   req.OldIno,
		Mode:     req.Mode,
		FileId:   fileId,
	}

	err = m.DentryCreateEx_ll(createReq)
	if err != nil {
		span.Errorf("create dentry failed, req %v, err %s", req, err.Error())
		return 0, err
	}

	return fileId, nil
}

func (m *metaOpImp) LookupEx(parentId uint64, name string) (den *proto.Dentry, err error) {
	return m.LookupEx_ll(parentId, name)
}

func (m *metaOpImp) CreateInode(mode uint32) (*proto.InodeInfo, error) {
	return m.InodeCreate_ll(mode, 0, 0, nil, nil)
}

func (m *metaOpImp) CreateFileEx(ctx context.Context, parentID uint64, name string, mode uint32) (*sdk.InodeInfo, uint64, error) {
	span := trace.SpanFromContextSafe(ctx)
	ifo, err := m.CreateInode(mode)
	if err != nil {
		span.Errorf("create inode failed, err %s", err.Error())
		return nil, 0, err
	}
	span.Debugf("create inode success, %v", ifo.String())

	req := &sdk.CreateDentryReq{
		ParentId: parentID,
		Name:     name,
		Inode:    ifo.Inode,
		OldIno:   0,
		Mode:     mode,
	}

	fileId, err := m.CreateDentryEx(ctx, req)
	if err != nil {
		span.Errorf("create dentry failed, req %s, err %s", req, err.Error())
		return nil, 0, err
	}

	//return sdk.NewInode(ifo, fileId), nil
	return ifo, fileId, nil
}
