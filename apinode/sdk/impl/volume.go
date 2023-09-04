package impl

import (
	"context"
	"strings"
	"time"

	"github.com/cubefs/cubefs/apinode/sdk"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/sdk/data/stream"
	"github.com/cubefs/cubefs/sdk/meta"
)

var (
	newMetaWrapper  = newMetaOp
	newExtentClient = newDataOp
)

type volume struct {
	mw      MetaOp
	ec      DataOp
	name    string
	owner   string
	allocId func(ctx context.Context) (id uint64, err error)
	sdk.IDirSnapshot
}

type metaOpImp struct {
	*meta.MetaWrapper
}

type DataOpImp struct {
	*stream.ExtentClient
}

func newDataOp(cfg *stream.ExtentConfig) (DataOp, error) {
	ec, err := stream.NewExtentClient(cfg)
	if ec != nil {
		return nil, err
	}

	dop := &DataOpImp{
		ExtentClient: ec,
	}
	return dop, nil
}

func newMetaOp(config *meta.MetaConfig) (MetaOp, error) {
	mw, err := meta.NewMetaWrapper(config)
	if err != nil {
		return nil, err
	}

	return &metaOpImp{MetaWrapper: mw}, nil
}

func newVolume(ctx context.Context, name, owner, addr string) (sdk.IVolume, error) {
	span := trace.SpanFromContextSafe(ctx)

	addrList := strings.Split(addr, ",")
	metaCfg := &meta.MetaConfig{
		Volume:  name,
		Owner:   owner,
		Masters: addrList,
	}

	mw, err := newMetaWrapper(metaCfg)
	if err != nil {
		span.Errorf("init meta wrapper failed, name %s, owner %s, addr %s", name, owner, addr)
		return nil, sdk.ErrInternalServerError
	}

	ecCfg := &stream.ExtentConfig{
		Volume:       name,
		Masters:      addrList,
		FollowerRead: true,
		NearRead:     true,
		OnGetExtents: mw.GetExtents,
		OnTruncate:   mw.Truncate,
	}

	if mw1, ok := mw.(*metaOpImp); ok {
		ecCfg.OnAppendExtentKey = mw1.AppendExtentKey
	}

	ec, err := newExtentClient(ecCfg)
	if err != nil {
		span.Errorf("init extent client failed, name %s, owner %s, addr %s", name, owner, addr)
		return nil, sdk.ErrInternalServerError
	}

	if mw1, ok := mw.(*metaOpImp); ok {
		mw1.Client = ec.(*stream.ExtentClient)
	}

	v := &volume{
		mw:    mw,
		ec:    ec,
		owner: owner,
		name:  name,
	}

	return v, nil
}

// TODO invoke master api to get inner ver id
func (v *volume) allocVerId() (uint64, error) {
	return uint64(time.Now().UnixMicro()), nil
}

func (v *volume) GetDirSnapshot(ctx context.Context, rootIno uint64) (sdk.IDirSnapshot, error) {
	span := trace.SpanFromContext(ctx)

	items, err := v.mw.ListAllDirSnapshot(rootIno)
	if err != nil {
		span.Errorf("list dir snapshot failed, ino %d, err %s", rootIno, err.Error())
		return nil, syscallToErr(err)
	}

	nmw := newSnapMetaOp(v.mw, items, rootIno)
	nmw.allocId = v.allocId
	verEc := newExtentClientVer(v.ec, nmw)

	dirSnap := &dirSnapshotOp{
		v:  v,
		mw: nmw,
		ec: verEc,
	}
	return dirSnap, nil
}

func (v *volume) Info() *sdk.VolInfo {
	info := &sdk.VolInfo{
		Name: v.name,
	}
	return info
}
