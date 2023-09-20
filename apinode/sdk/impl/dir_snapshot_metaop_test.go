package impl

import (
	"context"
	"reflect"
	"testing"

	"github.com/cubefs/cubefs/apinode/sdk"
	"github.com/cubefs/cubefs/apinode/testing/mocks"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func Test_newSnapMetaOp(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockMeta := mocks.NewMockMetaOp(ctrl)

	items := []*proto.DirSnapshotInfo{
		{SnapshotInode: 10},
	}

	rootIno := uint64(11)
	mw1 := newSnapMetaOp(mockMeta, items, rootIno)
	require.True(t, mw1.rootIno == rootIno && mw1.isNewest)
	require.True(t, len(mw1.snapShotItems) == 1)
	_, ok := mw1.sm.(*mocks.MockMetaOp)
	require.True(t, ok)

	metaOp := &metaOpImp{
		SnapShotMetaWrapper: &meta.SnapShotMetaWrapper{},
	}
	mw2 := newSnapMetaOp(metaOp, items, rootIno)
	_, ok = mw2.sm.(*meta.SnapShotMetaWrapper)
	require.True(t, ok)
}

func Test_getVerStr(t *testing.T) {
	mw := &snapMetaOpImp{}
	str := mw.getVerStr()
	t.Logf("no version get str, str %s", str)
	mw.ver = &proto.DelVer{
		DelVer: 100,
		Vers: []*proto.VersionInfo{
			{Ver: 10, Status: proto.VersionInit, DelTime: 11},
		},
	}
	str = mw.getVerStr()
	t.Logf("no version get str, str %s", str)
}

func Test_buildByClientVerItems(t *testing.T) {

	clientItems := []*proto.ClientDirVer{
		{OutVer: "tt", Ver: &proto.VersionInfo{Ver: 10, Status: proto.VersionNormal, DelTime: 10}},
		{OutVer: "tt1", Ver: &proto.VersionInfo{Ver: 11, Status: proto.VersionNormal, DelTime: 12}},
	}

	items := buildByClientVerItems(clientItems)
	for idx, e := range clientItems {
		require.True(t, reflect.DeepEqual(e.Ver, items[idx]))
	}
}

func Test_isSnapshotName(t *testing.T) {
	type args struct {
		name string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isSnapshotName(tt.args.name); got != tt.want {
				t.Errorf("isSnapshotName() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_newDirDentry(t *testing.T) {
	type args struct {
		dirIno uint64
		name   string
	}
	tests := []struct {
		name    string
		args    args
		wantDen *proto.Dentry
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotDen := newDirDentry(tt.args.dirIno, tt.args.name); !reflect.DeepEqual(gotDen, tt.wantDen) {
				t.Errorf("newDirDentry() = %v, want %v", gotDen, tt.wantDen)
			}
		})
	}
}

func Test_newSnapMetaOp1(t *testing.T) {
	type args struct {
		mop     MetaOp
		items   []*proto.DirSnapshotInfo
		rootIno uint64
	}
	tests := []struct {
		name string
		args args
		want *snapMetaOpImp
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := newSnapMetaOp(tt.args.mop, tt.args.items, tt.args.rootIno); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("newSnapMetaOp() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_snapMetaOpImp_AddMultipartPart_ll(t *testing.T) {
	type fields struct {
		sm            MetaOp
		allocId       func(ctx context.Context) (id uint64, err error)
		snapShotItems []*proto.DirSnapshotInfo
		hasSetVer     bool
		ver           *proto.DelVer
		snapIno       uint64
		isNewest      bool
		rootIno       uint64
	}
	type args struct {
		path        string
		multipartId string
		partId      uint16
		size        uint64
		md5         string
		inodeInfo   *proto.InodeInfo
	}
	tests := []struct {
		name         string
		fields       fields
		args         args
		wantOldInode uint64
		wantUpdated  bool
		wantErr      bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &snapMetaOpImp{
				sm:            tt.fields.sm,
				allocId:       tt.fields.allocId,
				snapShotItems: tt.fields.snapShotItems,
				hasSetVer:     tt.fields.hasSetVer,
				ver:           tt.fields.ver,
				snapIno:       tt.fields.snapIno,
				isNewest:      tt.fields.isNewest,
				rootIno:       tt.fields.rootIno,
			}
			gotOldInode, gotUpdated, err := m.AddMultipartPart_ll(tt.args.path, tt.args.multipartId, tt.args.partId, tt.args.size, tt.args.md5, tt.args.inodeInfo)
			if (err != nil) != tt.wantErr {
				t.Errorf("AddMultipartPart_ll() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotOldInode != tt.wantOldInode {
				t.Errorf("AddMultipartPart_ll() gotOldInode = %v, want %v", gotOldInode, tt.wantOldInode)
			}
			if gotUpdated != tt.wantUpdated {
				t.Errorf("AddMultipartPart_ll() gotUpdated = %v, want %v", gotUpdated, tt.wantUpdated)
			}
		})
	}
}

func Test_snapMetaOpImp_AppendExtentKeys(t *testing.T) {
	type fields struct {
		sm            MetaOp
		allocId       func(ctx context.Context) (id uint64, err error)
		snapShotItems []*proto.DirSnapshotInfo
		hasSetVer     bool
		ver           *proto.DelVer
		snapIno       uint64
		isNewest      bool
		rootIno       uint64
	}
	type args struct {
		inode uint64
		eks   []proto.ExtentKey
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &snapMetaOpImp{
				sm:            tt.fields.sm,
				allocId:       tt.fields.allocId,
				snapShotItems: tt.fields.snapShotItems,
				hasSetVer:     tt.fields.hasSetVer,
				ver:           tt.fields.ver,
				snapIno:       tt.fields.snapIno,
				isNewest:      tt.fields.isNewest,
				rootIno:       tt.fields.rootIno,
			}
			if err := m.AppendExtentKeys(tt.args.inode, tt.args.eks); (err != nil) != tt.wantErr {
				t.Errorf("AppendExtentKeys() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_snapMetaOpImp_BatchSetXAttr(t *testing.T) {
	type fields struct {
		sm            MetaOp
		allocId       func(ctx context.Context) (id uint64, err error)
		snapShotItems []*proto.DirSnapshotInfo
		hasSetVer     bool
		ver           *proto.DelVer
		snapIno       uint64
		isNewest      bool
		rootIno       uint64
	}
	type args struct {
		inode uint64
		attrs map[string]string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &snapMetaOpImp{
				sm:            tt.fields.sm,
				allocId:       tt.fields.allocId,
				snapShotItems: tt.fields.snapShotItems,
				hasSetVer:     tt.fields.hasSetVer,
				ver:           tt.fields.ver,
				snapIno:       tt.fields.snapIno,
				isNewest:      tt.fields.isNewest,
				rootIno:       tt.fields.rootIno,
			}
			if err := m.BatchSetXAttr(tt.args.inode, tt.args.attrs); (err != nil) != tt.wantErr {
				t.Errorf("BatchSetXAttr() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_snapMetaOpImp_CreateDentryEx(t *testing.T) {
	type fields struct {
		sm            MetaOp
		allocId       func(ctx context.Context) (id uint64, err error)
		snapShotItems []*proto.DirSnapshotInfo
		hasSetVer     bool
		ver           *proto.DelVer
		snapIno       uint64
		isNewest      bool
		rootIno       uint64
	}
	type args struct {
		ctx context.Context
		req *sdk.CreateDentryReq
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    uint64
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &snapMetaOpImp{
				sm:            tt.fields.sm,
				allocId:       tt.fields.allocId,
				snapShotItems: tt.fields.snapShotItems,
				hasSetVer:     tt.fields.hasSetVer,
				ver:           tt.fields.ver,
				snapIno:       tt.fields.snapIno,
				isNewest:      tt.fields.isNewest,
				rootIno:       tt.fields.rootIno,
			}
			got, err := m.CreateDentryEx(tt.args.ctx, tt.args.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("CreateDentryEx() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("CreateDentryEx() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_snapMetaOpImp_CreateFileEx(t *testing.T) {
	type fields struct {
		sm            MetaOp
		allocId       func(ctx context.Context) (id uint64, err error)
		snapShotItems []*proto.DirSnapshotInfo
		hasSetVer     bool
		ver           *proto.DelVer
		snapIno       uint64
		isNewest      bool
		rootIno       uint64
	}
	type args struct {
		ctx      context.Context
		parentID uint64
		name     string
		mode     uint32
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *sdk.InodeInfo
		want1   uint64
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &snapMetaOpImp{
				sm:            tt.fields.sm,
				allocId:       tt.fields.allocId,
				snapShotItems: tt.fields.snapShotItems,
				hasSetVer:     tt.fields.hasSetVer,
				ver:           tt.fields.ver,
				snapIno:       tt.fields.snapIno,
				isNewest:      tt.fields.isNewest,
				rootIno:       tt.fields.rootIno,
			}
			got, got1, err := m.CreateFileEx(tt.args.ctx, tt.args.parentID, tt.args.name, tt.args.mode)
			if (err != nil) != tt.wantErr {
				t.Errorf("CreateFileEx() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("CreateFileEx() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("CreateFileEx() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func Test_snapMetaOpImp_CreateInode(t *testing.T) {
	type fields struct {
		sm            MetaOp
		allocId       func(ctx context.Context) (id uint64, err error)
		snapShotItems []*proto.DirSnapshotInfo
		hasSetVer     bool
		ver           *proto.DelVer
		snapIno       uint64
		isNewest      bool
		rootIno       uint64
	}
	type args struct {
		mode uint32
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *proto.InodeInfo
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &snapMetaOpImp{
				sm:            tt.fields.sm,
				allocId:       tt.fields.allocId,
				snapShotItems: tt.fields.snapShotItems,
				hasSetVer:     tt.fields.hasSetVer,
				ver:           tt.fields.ver,
				snapIno:       tt.fields.snapIno,
				isNewest:      tt.fields.isNewest,
				rootIno:       tt.fields.rootIno,
			}
			got, err := m.CreateInode(tt.args.mode)
			if (err != nil) != tt.wantErr {
				t.Errorf("CreateInode() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("CreateInode() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_snapMetaOpImp_Delete(t *testing.T) {
	type fields struct {
		sm            MetaOp
		allocId       func(ctx context.Context) (id uint64, err error)
		snapShotItems []*proto.DirSnapshotInfo
		hasSetVer     bool
		ver           *proto.DelVer
		snapIno       uint64
		isNewest      bool
		rootIno       uint64
	}
	type args struct {
		parentID uint64
		name     string
		isDir    bool
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *proto.InodeInfo
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &snapMetaOpImp{
				sm:            tt.fields.sm,
				allocId:       tt.fields.allocId,
				snapShotItems: tt.fields.snapShotItems,
				hasSetVer:     tt.fields.hasSetVer,
				ver:           tt.fields.ver,
				snapIno:       tt.fields.snapIno,
				isNewest:      tt.fields.isNewest,
				rootIno:       tt.fields.rootIno,
			}
			got, err := m.Delete(tt.args.parentID, tt.args.name, tt.args.isDir)
			if (err != nil) != tt.wantErr {
				t.Errorf("Delete() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Delete() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_snapMetaOpImp_Evict(t *testing.T) {
	type fields struct {
		sm            MetaOp
		allocId       func(ctx context.Context) (id uint64, err error)
		snapShotItems []*proto.DirSnapshotInfo
		hasSetVer     bool
		ver           *proto.DelVer
		snapIno       uint64
		isNewest      bool
		rootIno       uint64
	}
	type args struct {
		inode uint64
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &snapMetaOpImp{
				sm:            tt.fields.sm,
				allocId:       tt.fields.allocId,
				snapShotItems: tt.fields.snapShotItems,
				hasSetVer:     tt.fields.hasSetVer,
				ver:           tt.fields.ver,
				snapIno:       tt.fields.snapIno,
				isNewest:      tt.fields.isNewest,
				rootIno:       tt.fields.rootIno,
			}
			if err := m.Evict(tt.args.inode); (err != nil) != tt.wantErr {
				t.Errorf("Evict() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_snapMetaOpImp_GetMultipart_ll(t *testing.T) {
	type fields struct {
		sm            MetaOp
		allocId       func(ctx context.Context) (id uint64, err error)
		snapShotItems []*proto.DirSnapshotInfo
		hasSetVer     bool
		ver           *proto.DelVer
		snapIno       uint64
		isNewest      bool
		rootIno       uint64
	}
	type args struct {
		path        string
		multipartId string
	}
	tests := []struct {
		name     string
		fields   fields
		args     args
		wantInfo *proto.MultipartInfo
		wantErr  bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &snapMetaOpImp{
				sm:            tt.fields.sm,
				allocId:       tt.fields.allocId,
				snapShotItems: tt.fields.snapShotItems,
				hasSetVer:     tt.fields.hasSetVer,
				ver:           tt.fields.ver,
				snapIno:       tt.fields.snapIno,
				isNewest:      tt.fields.isNewest,
				rootIno:       tt.fields.rootIno,
			}
			gotInfo, err := m.GetMultipart_ll(tt.args.path, tt.args.multipartId)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetMultipart_ll() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotInfo, tt.wantInfo) {
				t.Errorf("GetMultipart_ll() gotInfo = %v, want %v", gotInfo, tt.wantInfo)
			}
		})
	}
}

func Test_snapMetaOpImp_InitMultipart_ll(t *testing.T) {
	type fields struct {
		sm            MetaOp
		allocId       func(ctx context.Context) (id uint64, err error)
		snapShotItems []*proto.DirSnapshotInfo
		hasSetVer     bool
		ver           *proto.DelVer
		snapIno       uint64
		isNewest      bool
		rootIno       uint64
	}
	type args struct {
		path   string
		extend map[string]string
	}
	tests := []struct {
		name            string
		fields          fields
		args            args
		wantMultipartId string
		wantErr         bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &snapMetaOpImp{
				sm:            tt.fields.sm,
				allocId:       tt.fields.allocId,
				snapShotItems: tt.fields.snapShotItems,
				hasSetVer:     tt.fields.hasSetVer,
				ver:           tt.fields.ver,
				snapIno:       tt.fields.snapIno,
				isNewest:      tt.fields.isNewest,
				rootIno:       tt.fields.rootIno,
			}
			gotMultipartId, err := m.InitMultipart_ll(tt.args.path, tt.args.extend)
			if (err != nil) != tt.wantErr {
				t.Errorf("InitMultipart_ll() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotMultipartId != tt.wantMultipartId {
				t.Errorf("InitMultipart_ll() gotMultipartId = %v, want %v", gotMultipartId, tt.wantMultipartId)
			}
		})
	}
}

func Test_snapMetaOpImp_InodeDelete(t *testing.T) {
	type fields struct {
		sm            MetaOp
		allocId       func(ctx context.Context) (id uint64, err error)
		snapShotItems []*proto.DirSnapshotInfo
		hasSetVer     bool
		ver           *proto.DelVer
		snapIno       uint64
		isNewest      bool
		rootIno       uint64
	}
	type args struct {
		inode uint64
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &snapMetaOpImp{
				sm:            tt.fields.sm,
				allocId:       tt.fields.allocId,
				snapShotItems: tt.fields.snapShotItems,
				hasSetVer:     tt.fields.hasSetVer,
				ver:           tt.fields.ver,
				snapIno:       tt.fields.snapIno,
				isNewest:      tt.fields.isNewest,
				rootIno:       tt.fields.rootIno,
			}
			if err := m.InodeDelete(tt.args.inode); (err != nil) != tt.wantErr {
				t.Errorf("InodeDelete() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_snapMetaOpImp_InodeDeleteVer(t *testing.T) {
	type fields struct {
		sm            MetaOp
		allocId       func(ctx context.Context) (id uint64, err error)
		snapShotItems []*proto.DirSnapshotInfo
		hasSetVer     bool
		ver           *proto.DelVer
		snapIno       uint64
		isNewest      bool
		rootIno       uint64
	}
	type args struct {
		inode uint64
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &snapMetaOpImp{
				sm:            tt.fields.sm,
				allocId:       tt.fields.allocId,
				snapShotItems: tt.fields.snapShotItems,
				hasSetVer:     tt.fields.hasSetVer,
				ver:           tt.fields.ver,
				snapIno:       tt.fields.snapIno,
				isNewest:      tt.fields.isNewest,
				rootIno:       tt.fields.rootIno,
			}
			if err := m.InodeDeleteVer(tt.args.inode); (err != nil) != tt.wantErr {
				t.Errorf("InodeDeleteVer() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_snapMetaOpImp_InodeGet(t *testing.T) {
	type fields struct {
		sm            MetaOp
		allocId       func(ctx context.Context) (id uint64, err error)
		snapShotItems []*proto.DirSnapshotInfo
		hasSetVer     bool
		ver           *proto.DelVer
		snapIno       uint64
		isNewest      bool
		rootIno       uint64
	}
	type args struct {
		inode uint64
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *proto.InodeInfo
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &snapMetaOpImp{
				sm:            tt.fields.sm,
				allocId:       tt.fields.allocId,
				snapShotItems: tt.fields.snapShotItems,
				hasSetVer:     tt.fields.hasSetVer,
				ver:           tt.fields.ver,
				snapIno:       tt.fields.snapIno,
				isNewest:      tt.fields.isNewest,
				rootIno:       tt.fields.rootIno,
			}
			got, err := m.InodeGet(tt.args.inode)
			if (err != nil) != tt.wantErr {
				t.Errorf("InodeGet() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("InodeGet() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_snapMetaOpImp_InodeUnlink(t *testing.T) {
	type fields struct {
		sm            MetaOp
		allocId       func(ctx context.Context) (id uint64, err error)
		snapShotItems []*proto.DirSnapshotInfo
		hasSetVer     bool
		ver           *proto.DelVer
		snapIno       uint64
		isNewest      bool
		rootIno       uint64
	}
	type args struct {
		inode uint64
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *proto.InodeInfo
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &snapMetaOpImp{
				sm:            tt.fields.sm,
				allocId:       tt.fields.allocId,
				snapShotItems: tt.fields.snapShotItems,
				hasSetVer:     tt.fields.hasSetVer,
				ver:           tt.fields.ver,
				snapIno:       tt.fields.snapIno,
				isNewest:      tt.fields.isNewest,
				rootIno:       tt.fields.rootIno,
			}
			got, err := m.InodeUnlink(tt.args.inode)
			if (err != nil) != tt.wantErr {
				t.Errorf("InodeUnlink() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("InodeUnlink() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_snapMetaOpImp_ListAllDirSnapshot(t *testing.T) {
	type fields struct {
		sm            MetaOp
		allocId       func(ctx context.Context) (id uint64, err error)
		snapShotItems []*proto.DirSnapshotInfo
		hasSetVer     bool
		ver           *proto.DelVer
		snapIno       uint64
		isNewest      bool
		rootIno       uint64
	}
	type args struct {
		subRootIno uint64
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []*proto.DirSnapshotInfo
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &snapMetaOpImp{
				sm:            tt.fields.sm,
				allocId:       tt.fields.allocId,
				snapShotItems: tt.fields.snapShotItems,
				hasSetVer:     tt.fields.hasSetVer,
				ver:           tt.fields.ver,
				snapIno:       tt.fields.snapIno,
				isNewest:      tt.fields.isNewest,
				rootIno:       tt.fields.rootIno,
			}
			got, err := m.ListAllDirSnapshot(tt.args.subRootIno)
			if (err != nil) != tt.wantErr {
				t.Errorf("ListAllDirSnapshot() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ListAllDirSnapshot() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_snapMetaOpImp_LookupEx(t *testing.T) {
	type fields struct {
		sm            MetaOp
		allocId       func(ctx context.Context) (id uint64, err error)
		snapShotItems []*proto.DirSnapshotInfo
		hasSetVer     bool
		ver           *proto.DelVer
		snapIno       uint64
		isNewest      bool
		rootIno       uint64
	}
	type args struct {
		ctx      context.Context
		parentId uint64
		name     string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantDen *proto.Dentry
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &snapMetaOpImp{
				sm:            tt.fields.sm,
				allocId:       tt.fields.allocId,
				snapShotItems: tt.fields.snapShotItems,
				hasSetVer:     tt.fields.hasSetVer,
				ver:           tt.fields.ver,
				snapIno:       tt.fields.snapIno,
				isNewest:      tt.fields.isNewest,
				rootIno:       tt.fields.rootIno,
			}
			gotDen, err := m.LookupEx(tt.args.ctx, tt.args.parentId, tt.args.name)
			if (err != nil) != tt.wantErr {
				t.Errorf("LookupEx() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotDen, tt.wantDen) {
				t.Errorf("LookupEx() gotDen = %v, want %v", gotDen, tt.wantDen)
			}
		})
	}
}

func Test_snapMetaOpImp_ReadDirLimit(t *testing.T) {
	type fields struct {
		sm            MetaOp
		allocId       func(ctx context.Context) (id uint64, err error)
		snapShotItems []*proto.DirSnapshotInfo
		hasSetVer     bool
		ver           *proto.DelVer
		snapIno       uint64
		isNewest      bool
		rootIno       uint64
	}
	type args struct {
		dirIno uint64
		from   string
		limit  uint64
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []proto.Dentry
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &snapMetaOpImp{
				sm:            tt.fields.sm,
				allocId:       tt.fields.allocId,
				snapShotItems: tt.fields.snapShotItems,
				hasSetVer:     tt.fields.hasSetVer,
				ver:           tt.fields.ver,
				snapIno:       tt.fields.snapIno,
				isNewest:      tt.fields.isNewest,
				rootIno:       tt.fields.rootIno,
			}
			got, err := m.ReadDirLimit(tt.args.dirIno, tt.args.from, tt.args.limit)
			if (err != nil) != tt.wantErr {
				t.Errorf("ReadDirLimit() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ReadDirLimit() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_snapMetaOpImp_RemoveMultipart_ll(t *testing.T) {
	type fields struct {
		sm            MetaOp
		allocId       func(ctx context.Context) (id uint64, err error)
		snapShotItems []*proto.DirSnapshotInfo
		hasSetVer     bool
		ver           *proto.DelVer
		snapIno       uint64
		isNewest      bool
		rootIno       uint64
	}
	type args struct {
		path        string
		multipartID string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &snapMetaOpImp{
				sm:            tt.fields.sm,
				allocId:       tt.fields.allocId,
				snapShotItems: tt.fields.snapShotItems,
				hasSetVer:     tt.fields.hasSetVer,
				ver:           tt.fields.ver,
				snapIno:       tt.fields.snapIno,
				isNewest:      tt.fields.isNewest,
				rootIno:       tt.fields.rootIno,
			}
			if err := m.RemoveMultipart_ll(tt.args.path, tt.args.multipartID); (err != nil) != tt.wantErr {
				t.Errorf("RemoveMultipart_ll() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_snapMetaOpImp_Rename(t *testing.T) {
	type fields struct {
		sm            MetaOp
		allocId       func(ctx context.Context) (id uint64, err error)
		snapShotItems []*proto.DirSnapshotInfo
		hasSetVer     bool
		ver           *proto.DelVer
		snapIno       uint64
		isNewest      bool
		rootIno       uint64
	}
	type args struct {
		ctx context.Context
		src string
		dst string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &snapMetaOpImp{
				sm:            tt.fields.sm,
				allocId:       tt.fields.allocId,
				snapShotItems: tt.fields.snapShotItems,
				hasSetVer:     tt.fields.hasSetVer,
				ver:           tt.fields.ver,
				snapIno:       tt.fields.snapIno,
				isNewest:      tt.fields.isNewest,
				rootIno:       tt.fields.rootIno,
			}
			if err := m.Rename(tt.args.ctx, tt.args.src, tt.args.dst); (err != nil) != tt.wantErr {
				t.Errorf("Rename() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_snapMetaOpImp_SetInodeLock(t *testing.T) {
	type fields struct {
		sm            MetaOp
		allocId       func(ctx context.Context) (id uint64, err error)
		snapShotItems []*proto.DirSnapshotInfo
		hasSetVer     bool
		ver           *proto.DelVer
		snapIno       uint64
		isNewest      bool
		rootIno       uint64
	}
	type args struct {
		inode uint64
		req   *proto.InodeLockReq
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &snapMetaOpImp{
				sm:            tt.fields.sm,
				allocId:       tt.fields.allocId,
				snapShotItems: tt.fields.snapShotItems,
				hasSetVer:     tt.fields.hasSetVer,
				ver:           tt.fields.ver,
				snapIno:       tt.fields.snapIno,
				isNewest:      tt.fields.isNewest,
				rootIno:       tt.fields.rootIno,
			}
			if err := m.SetInodeLock(tt.args.inode, tt.args.req); (err != nil) != tt.wantErr {
				t.Errorf("SetInodeLock() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_snapMetaOpImp_Setattr(t *testing.T) {
	type fields struct {
		sm            MetaOp
		allocId       func(ctx context.Context) (id uint64, err error)
		snapShotItems []*proto.DirSnapshotInfo
		hasSetVer     bool
		ver           *proto.DelVer
		snapIno       uint64
		isNewest      bool
		rootIno       uint64
	}
	type args struct {
		inode uint64
		valid uint32
		mode  uint32
		uid   uint32
		gid   uint32
		atime int64
		mtime int64
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &snapMetaOpImp{
				sm:            tt.fields.sm,
				allocId:       tt.fields.allocId,
				snapShotItems: tt.fields.snapShotItems,
				hasSetVer:     tt.fields.hasSetVer,
				ver:           tt.fields.ver,
				snapIno:       tt.fields.snapIno,
				isNewest:      tt.fields.isNewest,
				rootIno:       tt.fields.rootIno,
			}
			if err := m.Setattr(tt.args.inode, tt.args.valid, tt.args.mode, tt.args.uid, tt.args.gid, tt.args.atime, tt.args.mtime); (err != nil) != tt.wantErr {
				t.Errorf("Setattr() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_snapMetaOpImp_Truncate(t *testing.T) {
	type fields struct {
		sm            MetaOp
		allocId       func(ctx context.Context) (id uint64, err error)
		snapShotItems []*proto.DirSnapshotInfo
		hasSetVer     bool
		ver           *proto.DelVer
		snapIno       uint64
		isNewest      bool
		rootIno       uint64
	}
	type args struct {
		inode uint64
		size  uint64
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &snapMetaOpImp{
				sm:            tt.fields.sm,
				allocId:       tt.fields.allocId,
				snapShotItems: tt.fields.snapShotItems,
				hasSetVer:     tt.fields.hasSetVer,
				ver:           tt.fields.ver,
				snapIno:       tt.fields.snapIno,
				isNewest:      tt.fields.isNewest,
				rootIno:       tt.fields.rootIno,
			}
			if err := m.Truncate(tt.args.inode, tt.args.size); (err != nil) != tt.wantErr {
				t.Errorf("Truncate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_snapMetaOpImp_XAttrDel_ll(t *testing.T) {
	type fields struct {
		sm            MetaOp
		allocId       func(ctx context.Context) (id uint64, err error)
		snapShotItems []*proto.DirSnapshotInfo
		hasSetVer     bool
		ver           *proto.DelVer
		snapIno       uint64
		isNewest      bool
		rootIno       uint64
	}
	type args struct {
		inode uint64
		name  string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &snapMetaOpImp{
				sm:            tt.fields.sm,
				allocId:       tt.fields.allocId,
				snapShotItems: tt.fields.snapShotItems,
				hasSetVer:     tt.fields.hasSetVer,
				ver:           tt.fields.ver,
				snapIno:       tt.fields.snapIno,
				isNewest:      tt.fields.isNewest,
				rootIno:       tt.fields.rootIno,
			}
			if err := m.XAttrDel_ll(tt.args.inode, tt.args.name); (err != nil) != tt.wantErr {
				t.Errorf("XAttrDel_ll() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_snapMetaOpImp_XAttrGetAll(t *testing.T) {
	type fields struct {
		sm            MetaOp
		allocId       func(ctx context.Context) (id uint64, err error)
		snapShotItems []*proto.DirSnapshotInfo
		hasSetVer     bool
		ver           *proto.DelVer
		snapIno       uint64
		isNewest      bool
		rootIno       uint64
	}
	type args struct {
		inode uint64
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *proto.XAttrInfo
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &snapMetaOpImp{
				sm:            tt.fields.sm,
				allocId:       tt.fields.allocId,
				snapShotItems: tt.fields.snapShotItems,
				hasSetVer:     tt.fields.hasSetVer,
				ver:           tt.fields.ver,
				snapIno:       tt.fields.snapIno,
				isNewest:      tt.fields.isNewest,
				rootIno:       tt.fields.rootIno,
			}
			got, err := m.XAttrGetAll(tt.args.inode)
			if (err != nil) != tt.wantErr {
				t.Errorf("XAttrGetAll() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("XAttrGetAll() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_snapMetaOpImp_XAttrGet_ll(t *testing.T) {
	type fields struct {
		sm            MetaOp
		allocId       func(ctx context.Context) (id uint64, err error)
		snapShotItems []*proto.DirSnapshotInfo
		hasSetVer     bool
		ver           *proto.DelVer
		snapIno       uint64
		isNewest      bool
		rootIno       uint64
	}
	type args struct {
		inode uint64
		name  string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *proto.XAttrInfo
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &snapMetaOpImp{
				sm:            tt.fields.sm,
				allocId:       tt.fields.allocId,
				snapShotItems: tt.fields.snapShotItems,
				hasSetVer:     tt.fields.hasSetVer,
				ver:           tt.fields.ver,
				snapIno:       tt.fields.snapIno,
				isNewest:      tt.fields.isNewest,
				rootIno:       tt.fields.rootIno,
			}
			got, err := m.XAttrGet_ll(tt.args.inode, tt.args.name)
			if (err != nil) != tt.wantErr {
				t.Errorf("XAttrGet_ll() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("XAttrGet_ll() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_snapMetaOpImp_XAttrSet(t *testing.T) {
	type fields struct {
		sm            MetaOp
		allocId       func(ctx context.Context) (id uint64, err error)
		snapShotItems []*proto.DirSnapshotInfo
		hasSetVer     bool
		ver           *proto.DelVer
		snapIno       uint64
		isNewest      bool
		rootIno       uint64
	}
	type args struct {
		inode     uint64
		name      []byte
		value     []byte
		overwrite bool
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &snapMetaOpImp{
				sm:            tt.fields.sm,
				allocId:       tt.fields.allocId,
				snapShotItems: tt.fields.snapShotItems,
				hasSetVer:     tt.fields.hasSetVer,
				ver:           tt.fields.ver,
				snapIno:       tt.fields.snapIno,
				isNewest:      tt.fields.isNewest,
				rootIno:       tt.fields.rootIno,
			}
			if err := m.XAttrSet(tt.args.inode, tt.args.name, tt.args.value, tt.args.overwrite); (err != nil) != tt.wantErr {
				t.Errorf("XAttrSet() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_snapMetaOpImp_XAttrsList_ll(t *testing.T) {
	type fields struct {
		sm            MetaOp
		allocId       func(ctx context.Context) (id uint64, err error)
		snapShotItems []*proto.DirSnapshotInfo
		hasSetVer     bool
		ver           *proto.DelVer
		snapIno       uint64
		isNewest      bool
		rootIno       uint64
	}
	type args struct {
		inode uint64
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []string
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &snapMetaOpImp{
				sm:            tt.fields.sm,
				allocId:       tt.fields.allocId,
				snapShotItems: tt.fields.snapShotItems,
				hasSetVer:     tt.fields.hasSetVer,
				ver:           tt.fields.ver,
				snapIno:       tt.fields.snapIno,
				isNewest:      tt.fields.isNewest,
				rootIno:       tt.fields.rootIno,
			}
			got, err := m.XAttrsList_ll(tt.args.inode)
			if (err != nil) != tt.wantErr {
				t.Errorf("XAttrsList_ll() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("XAttrsList_ll() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_snapMetaOpImp_XBatchDelAttr_ll(t *testing.T) {
	type fields struct {
		sm            MetaOp
		allocId       func(ctx context.Context) (id uint64, err error)
		snapShotItems []*proto.DirSnapshotInfo
		hasSetVer     bool
		ver           *proto.DelVer
		snapIno       uint64
		isNewest      bool
		rootIno       uint64
	}
	type args struct {
		ino  uint64
		keys []string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &snapMetaOpImp{
				sm:            tt.fields.sm,
				allocId:       tt.fields.allocId,
				snapShotItems: tt.fields.snapShotItems,
				hasSetVer:     tt.fields.hasSetVer,
				ver:           tt.fields.ver,
				snapIno:       tt.fields.snapIno,
				isNewest:      tt.fields.isNewest,
				rootIno:       tt.fields.rootIno,
			}
			if err := m.XBatchDelAttr_ll(tt.args.ino, tt.args.keys); (err != nil) != tt.wantErr {
				t.Errorf("XBatchDelAttr_ll() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_snapMetaOpImp_checkSnapshotIno(t *testing.T) {
	type fields struct {
		sm            MetaOp
		allocId       func(ctx context.Context) (id uint64, err error)
		snapShotItems []*proto.DirSnapshotInfo
		hasSetVer     bool
		ver           *proto.DelVer
		snapIno       uint64
		isNewest      bool
		rootIno       uint64
	}
	type args struct {
		dirIno uint64
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &snapMetaOpImp{
				sm:            tt.fields.sm,
				allocId:       tt.fields.allocId,
				snapShotItems: tt.fields.snapShotItems,
				hasSetVer:     tt.fields.hasSetVer,
				ver:           tt.fields.ver,
				snapIno:       tt.fields.snapIno,
				isNewest:      tt.fields.isNewest,
				rootIno:       tt.fields.rootIno,
			}
			m.checkSnapshotIno(tt.args.dirIno)
		})
	}
}

func Test_snapMetaOpImp_getSnapshotInodes(t *testing.T) {
	type fields struct {
		sm            MetaOp
		allocId       func(ctx context.Context) (id uint64, err error)
		snapShotItems []*proto.DirSnapshotInfo
		hasSetVer     bool
		ver           *proto.DelVer
		snapIno       uint64
		isNewest      bool
		rootIno       uint64
	}
	tests := []struct {
		name   string
		fields fields
		want   []uint64
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &snapMetaOpImp{
				sm:            tt.fields.sm,
				allocId:       tt.fields.allocId,
				snapShotItems: tt.fields.snapShotItems,
				hasSetVer:     tt.fields.hasSetVer,
				ver:           tt.fields.ver,
				snapIno:       tt.fields.snapIno,
				isNewest:      tt.fields.isNewest,
				rootIno:       tt.fields.rootIno,
			}
			if got := m.getSnapshotInodes(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getSnapshotInodes() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_snapMetaOpImp_getVerStr(t *testing.T) {
	type fields struct {
		sm            MetaOp
		allocId       func(ctx context.Context) (id uint64, err error)
		snapShotItems []*proto.DirSnapshotInfo
		hasSetVer     bool
		ver           *proto.DelVer
		snapIno       uint64
		isNewest      bool
		rootIno       uint64
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &snapMetaOpImp{
				sm:            tt.fields.sm,
				allocId:       tt.fields.allocId,
				snapShotItems: tt.fields.snapShotItems,
				hasSetVer:     tt.fields.hasSetVer,
				ver:           tt.fields.ver,
				snapIno:       tt.fields.snapIno,
				isNewest:      tt.fields.isNewest,
				rootIno:       tt.fields.rootIno,
			}
			if got := m.getVerStr(); got != tt.want {
				t.Errorf("getVerStr() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_snapMetaOpImp_getVersionNames(t *testing.T) {
	type fields struct {
		sm            MetaOp
		allocId       func(ctx context.Context) (id uint64, err error)
		snapShotItems []*proto.DirSnapshotInfo
		hasSetVer     bool
		ver           *proto.DelVer
		snapIno       uint64
		isNewest      bool
		rootIno       uint64
	}
	type args struct {
		dirIno uint64
	}
	tests := []struct {
		name      string
		fields    fields
		args      args
		wantNames []string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &snapMetaOpImp{
				sm:            tt.fields.sm,
				allocId:       tt.fields.allocId,
				snapShotItems: tt.fields.snapShotItems,
				hasSetVer:     tt.fields.hasSetVer,
				ver:           tt.fields.ver,
				snapIno:       tt.fields.snapIno,
				isNewest:      tt.fields.isNewest,
				rootIno:       tt.fields.rootIno,
			}
			if gotNames := m.getVersionNames(tt.args.dirIno); !reflect.DeepEqual(gotNames, tt.wantNames) {
				t.Errorf("getVersionNames() = %v, want %v", gotNames, tt.wantNames)
			}
		})
	}
}

func Test_snapMetaOpImp_isSnapshotDir(t *testing.T) {
	type fields struct {
		sm            MetaOp
		allocId       func(ctx context.Context) (id uint64, err error)
		snapShotItems []*proto.DirSnapshotInfo
		hasSetVer     bool
		ver           *proto.DelVer
		snapIno       uint64
		isNewest      bool
		rootIno       uint64
	}
	type args struct {
		ctx      context.Context
		parentId uint64
		name     string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    bool
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &snapMetaOpImp{
				sm:            tt.fields.sm,
				allocId:       tt.fields.allocId,
				snapShotItems: tt.fields.snapShotItems,
				hasSetVer:     tt.fields.hasSetVer,
				ver:           tt.fields.ver,
				snapIno:       tt.fields.snapIno,
				isNewest:      tt.fields.isNewest,
				rootIno:       tt.fields.rootIno,
			}
			got, err := m.isSnapshotDir(tt.args.ctx, tt.args.parentId, tt.args.name)
			if (err != nil) != tt.wantErr {
				t.Errorf("isSnapshotDir() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("isSnapshotDir() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_snapMetaOpImp_isSnapshotInode(t *testing.T) {
	type fields struct {
		sm            MetaOp
		allocId       func(ctx context.Context) (id uint64, err error)
		snapShotItems []*proto.DirSnapshotInfo
		hasSetVer     bool
		ver           *proto.DelVer
		snapIno       uint64
		isNewest      bool
		rootIno       uint64
	}
	type args struct {
		dirIno uint64
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
		want1  *proto.DelVer
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &snapMetaOpImp{
				sm:            tt.fields.sm,
				allocId:       tt.fields.allocId,
				snapShotItems: tt.fields.snapShotItems,
				hasSetVer:     tt.fields.hasSetVer,
				ver:           tt.fields.ver,
				snapIno:       tt.fields.snapIno,
				isNewest:      tt.fields.isNewest,
				rootIno:       tt.fields.rootIno,
			}
			got, got1 := m.isSnapshotInode(tt.args.dirIno)
			if got != tt.want {
				t.Errorf("isSnapshotInode() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("isSnapshotInode() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func Test_snapMetaOpImp_lookupSubDirVer(t *testing.T) {
	type fields struct {
		sm            MetaOp
		allocId       func(ctx context.Context) (id uint64, err error)
		snapShotItems []*proto.DirSnapshotInfo
		hasSetVer     bool
		ver           *proto.DelVer
		snapIno       uint64
		isNewest      bool
		rootIno       uint64
	}
	type args struct {
		parIno  uint64
		subPath string
	}
	tests := []struct {
		name         string
		fields       fields
		args         args
		wantChildIno uint64
		wantV        *proto.DelVer
		wantErr      bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &snapMetaOpImp{
				sm:            tt.fields.sm,
				allocId:       tt.fields.allocId,
				snapShotItems: tt.fields.snapShotItems,
				hasSetVer:     tt.fields.hasSetVer,
				ver:           tt.fields.ver,
				snapIno:       tt.fields.snapIno,
				isNewest:      tt.fields.isNewest,
				rootIno:       tt.fields.rootIno,
			}
			gotChildIno, gotV, err := m.lookupSubDirVer(tt.args.parIno, tt.args.subPath)
			if (err != nil) != tt.wantErr {
				t.Errorf("lookupSubDirVer() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotChildIno != tt.wantChildIno {
				t.Errorf("lookupSubDirVer() gotChildIno = %v, want %v", gotChildIno, tt.wantChildIno)
			}
			if !reflect.DeepEqual(gotV, tt.wantV) {
				t.Errorf("lookupSubDirVer() gotV = %v, want %v", gotV, tt.wantV)
			}
		})
	}
}

func Test_snapMetaOpImp_newestVer(t *testing.T) {
	type fields struct {
		sm            MetaOp
		allocId       func(ctx context.Context) (id uint64, err error)
		snapShotItems []*proto.DirSnapshotInfo
		hasSetVer     bool
		ver           *proto.DelVer
		snapIno       uint64
		isNewest      bool
		rootIno       uint64
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &snapMetaOpImp{
				sm:            tt.fields.sm,
				allocId:       tt.fields.allocId,
				snapShotItems: tt.fields.snapShotItems,
				hasSetVer:     tt.fields.hasSetVer,
				ver:           tt.fields.ver,
				snapIno:       tt.fields.snapIno,
				isNewest:      tt.fields.isNewest,
				rootIno:       tt.fields.rootIno,
			}
			if got := m.newestVer(); got != tt.want {
				t.Errorf("newestVer() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_snapMetaOpImp_resetDirVer(t *testing.T) {
	type fields struct {
		sm            MetaOp
		allocId       func(ctx context.Context) (id uint64, err error)
		snapShotItems []*proto.DirSnapshotInfo
		hasSetVer     bool
		ver           *proto.DelVer
		snapIno       uint64
		isNewest      bool
		rootIno       uint64
	}
	type args struct {
		ctx context.Context
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &snapMetaOpImp{
				sm:            tt.fields.sm,
				allocId:       tt.fields.allocId,
				snapShotItems: tt.fields.snapShotItems,
				hasSetVer:     tt.fields.hasSetVer,
				ver:           tt.fields.ver,
				snapIno:       tt.fields.snapIno,
				isNewest:      tt.fields.isNewest,
				rootIno:       tt.fields.rootIno,
			}
			m.resetDirVer(tt.args.ctx)
		})
	}
}

func Test_snapMetaOpImp_versionExist(t *testing.T) {
	type fields struct {
		sm            MetaOp
		allocId       func(ctx context.Context) (id uint64, err error)
		snapShotItems []*proto.DirSnapshotInfo
		hasSetVer     bool
		ver           *proto.DelVer
		snapIno       uint64
		isNewest      bool
		rootIno       uint64
	}
	type args struct {
		dirIno uint64
		outVer string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
		want1  *proto.VersionInfo
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &snapMetaOpImp{
				sm:            tt.fields.sm,
				allocId:       tt.fields.allocId,
				snapShotItems: tt.fields.snapShotItems,
				hasSetVer:     tt.fields.hasSetVer,
				ver:           tt.fields.ver,
				snapIno:       tt.fields.snapIno,
				isNewest:      tt.fields.isNewest,
				rootIno:       tt.fields.rootIno,
			}
			got, got1 := m.versionExist(tt.args.dirIno, tt.args.outVer)
			if got != tt.want {
				t.Errorf("versionExist() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("versionExist() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func Test_versionName(t *testing.T) {
	type args struct {
		ver string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := versionName(tt.args.ver); got != tt.want {
				t.Errorf("versionName() = %v, want %v", got, tt.want)
			}
		})
	}
}
