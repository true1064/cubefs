package impl

import (
	"context"
	"fmt"
	"github.com/cubefs/cubefs/apinode/sdk"
	"github.com/cubefs/cubefs/apinode/testing/mocks"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/data/stream"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"io"
	"reflect"
	"testing"
)

func Test_newVolume(t *testing.T) {
	tests := []struct {
		name    string
		owner   string
		addr    string
		want    sdk.IVolume
		wantErr error
	}{
		{"v1", "o1", "addr1", nil, sdk.ErrInternalServerError},
		{"v2", "o1", "addr1", nil, sdk.ErrInternalServerError},
		{"v3", "o1", "addr1", &volume{name: "v3", owner: "o1"}, nil},
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	newExtentClient = func(cfg *stream.ExtentConfig) (op sdk.DataOp, err error) {
		if cfg.Volume == "v1" {
			return nil, master.ErrNoValidMaster
		}
		return mocks.NewMockDataOp(ctrl), nil
	}

	newMetaWrapper = func(config *meta.MetaConfig) (op sdk.MetaOp, err error) {
		if config.Volume == "v2" {
			return nil, fmt.Errorf("no vaild volume")
		}
		return mocks.NewMockMetaOp(ctrl), err
	}

	_, ctx := trace.StartSpanFromContext(context.TODO(), "")
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := newVolume(ctx, tt.name, tt.owner, tt.addr)
			require.True(t, err == tt.wantErr)
			if tt.want != nil {
				//t.Errorf("got name %s, want name %s", got.Info().Name, tt.want.Info().Name)
				require.True(t, got.Info().Name == tt.want.Info().Name)
			}
		})
	}
}

func Test_syscallToErr(t *testing.T) {
	type args struct {
		err error
	}
	tests := []struct {
		name string
		args args
		want *sdk.Error
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := syscallToErr(tt.args.err); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("syscallToErr() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_volume_AbortMultiPart(t *testing.T) {
	type fields struct {
		mw    sdk.MetaOp
		ec    sdk.DataOp
		name  string
		owner string
	}
	type args struct {
		ctx      context.Context
		filepath string
		uploadId string
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
			v := &volume{
				mw:    tt.fields.mw,
				ec:    tt.fields.ec,
				name:  tt.fields.name,
				owner: tt.fields.owner,
			}
			if err := v.AbortMultiPart(tt.args.ctx, tt.args.filepath, tt.args.uploadId); (err != nil) != tt.wantErr {
				t.Errorf("AbortMultiPart() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_volume_BatchDeleteXAttr(t *testing.T) {
	type fields struct {
		mw    sdk.MetaOp
		ec    sdk.DataOp
		name  string
		owner string
	}
	type args struct {
		ctx  context.Context
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
			v := &volume{
				mw:    tt.fields.mw,
				ec:    tt.fields.ec,
				name:  tt.fields.name,
				owner: tt.fields.owner,
			}
			if err := v.BatchDeleteXAttr(tt.args.ctx, tt.args.ino, tt.args.keys); (err != nil) != tt.wantErr {
				t.Errorf("BatchDeleteXAttr() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_volume_BatchGetInodes(t *testing.T) {
	type fields struct {
		mw    sdk.MetaOp
		ec    sdk.DataOp
		name  string
		owner string
	}
	type args struct {
		ctx  context.Context
		inos []uint64
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []*proto.InodeInfo
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &volume{
				mw:    tt.fields.mw,
				ec:    tt.fields.ec,
				name:  tt.fields.name,
				owner: tt.fields.owner,
			}
			got, err := v.BatchGetInodes(tt.args.ctx, tt.args.inos)
			if (err != nil) != tt.wantErr {
				t.Errorf("BatchGetInodes() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("BatchGetInodes() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_volume_BatchSetXAttr(t *testing.T) {
	type fields struct {
		mw    sdk.MetaOp
		ec    sdk.DataOp
		name  string
		owner string
	}
	type args struct {
		ctx   context.Context
		ino   uint64
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
			v := &volume{
				mw:    tt.fields.mw,
				ec:    tt.fields.ec,
				name:  tt.fields.name,
				owner: tt.fields.owner,
			}
			if err := v.BatchSetXAttr(tt.args.ctx, tt.args.ino, tt.args.attrs); (err != nil) != tt.wantErr {
				t.Errorf("BatchSetXAttr() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_volume_CompleteMultiPart(t *testing.T) {
	type fields struct {
		mw    sdk.MetaOp
		ec    sdk.DataOp
		name  string
		owner string
	}
	type args struct {
		ctx      context.Context
		filepath string
		uploadId string
		oldIno   uint64
		partsArg []sdk.Part
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
			v := &volume{
				mw:    tt.fields.mw,
				ec:    tt.fields.ec,
				name:  tt.fields.name,
				owner: tt.fields.owner,
			}
			if err := v.CompleteMultiPart(tt.args.ctx, tt.args.filepath, tt.args.uploadId, tt.args.oldIno, tt.args.partsArg); (err != nil) != tt.wantErr {
				t.Errorf("CompleteMultiPart() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_volume_CreateFile(t *testing.T) {
	type fields struct {
		mw    sdk.MetaOp
		ec    sdk.DataOp
		name  string
		owner string
	}
	type args struct {
		ctx       context.Context
		parentIno uint64
		name      string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *sdk.InodeInfo
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &volume{
				mw:    tt.fields.mw,
				ec:    tt.fields.ec,
				name:  tt.fields.name,
				owner: tt.fields.owner,
			}
			got, err := v.CreateFile(tt.args.ctx, tt.args.parentIno, tt.args.name)
			if (err != nil) != tt.wantErr {
				t.Errorf("CreateFile() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("CreateFile() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_volume_Delete(t *testing.T) {
	type fields struct {
		mw    sdk.MetaOp
		ec    sdk.DataOp
		name  string
		owner string
	}
	type args struct {
		ctx    context.Context
		parIno uint64
		name   string
		isDir  bool
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
			v := &volume{
				mw:    tt.fields.mw,
				ec:    tt.fields.ec,
				name:  tt.fields.name,
				owner: tt.fields.owner,
			}
			if err := v.Delete(tt.args.ctx, tt.args.parIno, tt.args.name, tt.args.isDir); (err != nil) != tt.wantErr {
				t.Errorf("Delete() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_volume_DeleteXAttr(t *testing.T) {
	type fields struct {
		mw    sdk.MetaOp
		ec    sdk.DataOp
		name  string
		owner string
	}
	type args struct {
		ctx context.Context
		ino uint64
		key string
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
			v := &volume{
				mw:    tt.fields.mw,
				ec:    tt.fields.ec,
				name:  tt.fields.name,
				owner: tt.fields.owner,
			}
			if err := v.DeleteXAttr(tt.args.ctx, tt.args.ino, tt.args.key); (err != nil) != tt.wantErr {
				t.Errorf("DeleteXAttr() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_volume_GetInode(t *testing.T) {
	type fields struct {
		mw    sdk.MetaOp
		ec    sdk.DataOp
		name  string
		owner string
	}
	type args struct {
		ctx context.Context
		ino uint64
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *sdk.InodeInfo
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &volume{
				mw:    tt.fields.mw,
				ec:    tt.fields.ec,
				name:  tt.fields.name,
				owner: tt.fields.owner,
			}
			got, err := v.GetInode(tt.args.ctx, tt.args.ino)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetInode() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetInode() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_volume_GetXAttr(t *testing.T) {
	type fields struct {
		mw    sdk.MetaOp
		ec    sdk.DataOp
		name  string
		owner string
	}
	type args struct {
		ctx context.Context
		ino uint64
		key string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    string
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &volume{
				mw:    tt.fields.mw,
				ec:    tt.fields.ec,
				name:  tt.fields.name,
				owner: tt.fields.owner,
			}
			got, err := v.GetXAttr(tt.args.ctx, tt.args.ino, tt.args.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetXAttr() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("GetXAttr() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_volume_GetXAttrMap(t *testing.T) {
	type fields struct {
		mw    sdk.MetaOp
		ec    sdk.DataOp
		name  string
		owner string
	}
	type args struct {
		ctx context.Context
		ino uint64
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    map[string]string
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &volume{
				mw:    tt.fields.mw,
				ec:    tt.fields.ec,
				name:  tt.fields.name,
				owner: tt.fields.owner,
			}
			got, err := v.GetXAttrMap(tt.args.ctx, tt.args.ino)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetXAttrMap() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetXAttrMap() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_volume_Info(t *testing.T) {
	type fields struct {
		mw    sdk.MetaOp
		ec    sdk.DataOp
		name  string
		owner string
	}
	tests := []struct {
		name   string
		fields fields
		want   *sdk.VolInfo
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &volume{
				mw:    tt.fields.mw,
				ec:    tt.fields.ec,
				name:  tt.fields.name,
				owner: tt.fields.owner,
			}
			if got := v.Info(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Info() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_volume_InitMultiPart(t *testing.T) {
	type fields struct {
		mw    sdk.MetaOp
		ec    sdk.DataOp
		name  string
		owner string
	}
	type args struct {
		ctx    context.Context
		path   string
		oldIno uint64
		extend map[string]string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    string
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &volume{
				mw:    tt.fields.mw,
				ec:    tt.fields.ec,
				name:  tt.fields.name,
				owner: tt.fields.owner,
			}
			got, err := v.InitMultiPart(tt.args.ctx, tt.args.path, tt.args.oldIno, tt.args.extend)
			if (err != nil) != tt.wantErr {
				t.Errorf("InitMultiPart() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("InitMultiPart() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_volume_ListMultiPart(t *testing.T) {
	type fields struct {
		mw    sdk.MetaOp
		ec    sdk.DataOp
		name  string
		owner string
	}
	type args struct {
		ctx      context.Context
		filepath string
		uploadId string
		count    uint64
		marker   uint64
	}
	tests := []struct {
		name            string
		fields          fields
		args            args
		wantParts       []*sdk.Part
		wantNext        uint64
		wantIsTruncated bool
		wantErr         bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &volume{
				mw:    tt.fields.mw,
				ec:    tt.fields.ec,
				name:  tt.fields.name,
				owner: tt.fields.owner,
			}
			gotParts, gotNext, gotIsTruncated, err := v.ListMultiPart(tt.args.ctx, tt.args.filepath, tt.args.uploadId, tt.args.count, tt.args.marker)
			if (err != nil) != tt.wantErr {
				t.Errorf("ListMultiPart() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotParts, tt.wantParts) {
				t.Errorf("ListMultiPart() gotParts = %v, want %v", gotParts, tt.wantParts)
			}
			if gotNext != tt.wantNext {
				t.Errorf("ListMultiPart() gotNext = %v, want %v", gotNext, tt.wantNext)
			}
			if gotIsTruncated != tt.wantIsTruncated {
				t.Errorf("ListMultiPart() gotIsTruncated = %v, want %v", gotIsTruncated, tt.wantIsTruncated)
			}
		})
	}
}

func Test_volume_ListXAttr(t *testing.T) {
	type fields struct {
		mw    sdk.MetaOp
		ec    sdk.DataOp
		name  string
		owner string
	}
	type args struct {
		ctx context.Context
		ino uint64
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
			v := &volume{
				mw:    tt.fields.mw,
				ec:    tt.fields.ec,
				name:  tt.fields.name,
				owner: tt.fields.owner,
			}
			got, err := v.ListXAttr(tt.args.ctx, tt.args.ino)
			if (err != nil) != tt.wantErr {
				t.Errorf("ListXAttr() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ListXAttr() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_volume_Lookup(t *testing.T) {
	type fields struct {
		mw    sdk.MetaOp
		ec    sdk.DataOp
		name  string
		owner string
	}
	type args struct {
		ctx       context.Context
		parentIno uint64
		name      string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *sdk.DirInfo
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &volume{
				mw:    tt.fields.mw,
				ec:    tt.fields.ec,
				name:  tt.fields.name,
				owner: tt.fields.owner,
			}
			got, err := v.Lookup(tt.args.ctx, tt.args.parentIno, tt.args.name)
			if (err != nil) != tt.wantErr {
				t.Errorf("Lookup() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Lookup() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_volume_Mkdir(t *testing.T) {
	type fields struct {
		mw    sdk.MetaOp
		ec    sdk.DataOp
		name  string
		owner string
	}
	type args struct {
		ctx    context.Context
		parIno uint64
		name   string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *sdk.InodeInfo
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &volume{
				mw:    tt.fields.mw,
				ec:    tt.fields.ec,
				name:  tt.fields.name,
				owner: tt.fields.owner,
			}
			got, err := v.Mkdir(tt.args.ctx, tt.args.parIno, tt.args.name)
			if (err != nil) != tt.wantErr {
				t.Errorf("Mkdir() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Mkdir() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_volume_NewInodeLock(t *testing.T) {
	type fields struct {
		mw    sdk.MetaOp
		ec    sdk.DataOp
		name  string
		owner string
	}
	tests := []struct {
		name   string
		fields fields
		want   sdk.InodeLockApi
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &volume{
				mw:    tt.fields.mw,
				ec:    tt.fields.ec,
				name:  tt.fields.name,
				owner: tt.fields.owner,
			}
			if got := v.NewInodeLock(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewInodeLock() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_volume_ReadDirAll(t *testing.T) {
	type fields struct {
		mw    sdk.MetaOp
		ec    sdk.DataOp
		name  string
		owner string
	}
	type args struct {
		ctx context.Context
		ino uint64
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []sdk.DirInfo
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &volume{
				mw:    tt.fields.mw,
				ec:    tt.fields.ec,
				name:  tt.fields.name,
				owner: tt.fields.owner,
			}
			got, err := v.ReadDirAll(tt.args.ctx, tt.args.ino)
			if (err != nil) != tt.wantErr {
				t.Errorf("ReadDirAll() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ReadDirAll() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_volume_ReadFile(t *testing.T) {
	type fields struct {
		mw    sdk.MetaOp
		ec    sdk.DataOp
		name  string
		owner string
	}
	type args struct {
		ctx  context.Context
		ino  uint64
		off  uint64
		data []byte
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantN   int
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &volume{
				mw:    tt.fields.mw,
				ec:    tt.fields.ec,
				name:  tt.fields.name,
				owner: tt.fields.owner,
			}
			gotN, err := v.ReadFile(tt.args.ctx, tt.args.ino, tt.args.off, tt.args.data)
			if (err != nil) != tt.wantErr {
				t.Errorf("ReadFile() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotN != tt.wantN {
				t.Errorf("ReadFile() gotN = %v, want %v", gotN, tt.wantN)
			}
		})
	}
}

func Test_volume_Readdir(t *testing.T) {
	type fields struct {
		mw    sdk.MetaOp
		ec    sdk.DataOp
		name  string
		owner string
	}
	type args struct {
		ctx    context.Context
		parIno uint64
		marker string
		count  uint32
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []sdk.DirInfo
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &volume{
				mw:    tt.fields.mw,
				ec:    tt.fields.ec,
				name:  tt.fields.name,
				owner: tt.fields.owner,
			}
			got, err := v.Readdir(tt.args.ctx, tt.args.parIno, tt.args.marker, tt.args.count)
			if (err != nil) != tt.wantErr {
				t.Errorf("Readdir() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Readdir() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_volume_Rename(t *testing.T) {
	type fields struct {
		mw    sdk.MetaOp
		ec    sdk.DataOp
		name  string
		owner string
	}
	type args struct {
		ctx       context.Context
		srcParIno uint64
		dstParIno uint64
		srcName   string
		destName  string
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
			v := &volume{
				mw:    tt.fields.mw,
				ec:    tt.fields.ec,
				name:  tt.fields.name,
				owner: tt.fields.owner,
			}
			if err := v.Rename(tt.args.ctx, tt.args.srcParIno, tt.args.dstParIno, tt.args.srcName, tt.args.destName); (err != nil) != tt.wantErr {
				t.Errorf("Rename() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_volume_SetAttr(t *testing.T) {
	type fields struct {
		mw    sdk.MetaOp
		ec    sdk.DataOp
		name  string
		owner string
	}
	type args struct {
		ctx context.Context
		req *sdk.SetAttrReq
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
			v := &volume{
				mw:    tt.fields.mw,
				ec:    tt.fields.ec,
				name:  tt.fields.name,
				owner: tt.fields.owner,
			}
			if err := v.SetAttr(tt.args.ctx, tt.args.req); (err != nil) != tt.wantErr {
				t.Errorf("SetAttr() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_volume_SetXAttr(t *testing.T) {
	type fields struct {
		mw    sdk.MetaOp
		ec    sdk.DataOp
		name  string
		owner string
	}
	type args struct {
		ctx context.Context
		ino uint64
		key string
		val string
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
			v := &volume{
				mw:    tt.fields.mw,
				ec:    tt.fields.ec,
				name:  tt.fields.name,
				owner: tt.fields.owner,
			}
			if err := v.SetXAttr(tt.args.ctx, tt.args.ino, tt.args.key, tt.args.val); (err != nil) != tt.wantErr {
				t.Errorf("SetXAttr() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_volume_StatFs(t *testing.T) {
	type fields struct {
		mw    sdk.MetaOp
		ec    sdk.DataOp
		name  string
		owner string
	}
	type args struct {
		ctx context.Context
		ino uint64
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *sdk.StatFs
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &volume{
				mw:    tt.fields.mw,
				ec:    tt.fields.ec,
				name:  tt.fields.name,
				owner: tt.fields.owner,
			}
			got, err := v.StatFs(tt.args.ctx, tt.args.ino)
			if (err != nil) != tt.wantErr {
				t.Errorf("StatFs() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("StatFs() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_volume_UploadFile(t *testing.T) {
	type fields struct {
		mw    sdk.MetaOp
		ec    sdk.DataOp
		name  string
		owner string
	}
	type args struct {
		req *sdk.UploadFileReq
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *sdk.InodeInfo
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &volume{
				mw:    tt.fields.mw,
				ec:    tt.fields.ec,
				name:  tt.fields.name,
				owner: tt.fields.owner,
			}
			got, err := v.UploadFile(tt.args.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("UploadFile() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("UploadFile() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_volume_UploadMultiPart(t *testing.T) {
	type fields struct {
		mw    sdk.MetaOp
		ec    sdk.DataOp
		name  string
		owner string
	}
	type args struct {
		ctx      context.Context
		filepath string
		uploadId string
		partNum  uint16
		read     io.Reader
	}
	tests := []struct {
		name     string
		fields   fields
		args     args
		wantPart *sdk.Part
		wantErr  bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &volume{
				mw:    tt.fields.mw,
				ec:    tt.fields.ec,
				name:  tt.fields.name,
				owner: tt.fields.owner,
			}
			gotPart, err := v.UploadMultiPart(tt.args.ctx, tt.args.filepath, tt.args.uploadId, tt.args.partNum, tt.args.read)
			if (err != nil) != tt.wantErr {
				t.Errorf("UploadMultiPart() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotPart, tt.wantPart) {
				t.Errorf("UploadMultiPart() gotPart = %v, want %v", gotPart, tt.wantPart)
			}
		})
	}
}

func Test_volume_WriteFile(t *testing.T) {
	type fields struct {
		mw    sdk.MetaOp
		ec    sdk.DataOp
		name  string
		owner string
	}
	type args struct {
		ctx  context.Context
		ino  uint64
		off  uint64
		size uint64
		body io.Reader
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
			v := &volume{
				mw:    tt.fields.mw,
				ec:    tt.fields.ec,
				name:  tt.fields.name,
				owner: tt.fields.owner,
			}
			if err := v.WriteFile(tt.args.ctx, tt.args.ino, tt.args.off, tt.args.size, tt.args.body); (err != nil) != tt.wantErr {
				t.Errorf("WriteFile() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_volume_getStatByIno(t *testing.T) {
	type fields struct {
		mw    sdk.MetaOp
		ec    sdk.DataOp
		name  string
		owner string
	}
	type args struct {
		ctx context.Context
		ino uint64
	}
	tests := []struct {
		name     string
		fields   fields
		args     args
		wantInfo *sdk.StatFs
		wantErr  bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &volume{
				mw:    tt.fields.mw,
				ec:    tt.fields.ec,
				name:  tt.fields.name,
				owner: tt.fields.owner,
			}
			gotInfo, err := v.getStatByIno(tt.args.ctx, tt.args.ino)
			if (err != nil) != tt.wantErr {
				t.Errorf("getStatByIno() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotInfo, tt.wantInfo) {
				t.Errorf("getStatByIno() gotInfo = %v, want %v", gotInfo, tt.wantInfo)
			}
		})
	}
}

func Test_volume_mkdirByPath(t *testing.T) {
	type fields struct {
		mw    sdk.MetaOp
		ec    sdk.DataOp
		name  string
		owner string
	}
	type args struct {
		ctx context.Context
		dir string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantIno uint64
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &volume{
				mw:    tt.fields.mw,
				ec:    tt.fields.ec,
				name:  tt.fields.name,
				owner: tt.fields.owner,
			}
			gotIno, err := v.mkdirByPath(tt.args.ctx, tt.args.dir)
			if (err != nil) != tt.wantErr {
				t.Errorf("mkdirByPath() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotIno != tt.wantIno {
				t.Errorf("mkdirByPath() gotIno = %v, want %v", gotIno, tt.wantIno)
			}
		})
	}
}

func Test_volume_writeAt(t *testing.T) {
	type fields struct {
		mw    sdk.MetaOp
		ec    sdk.DataOp
		name  string
		owner string
	}
	type args struct {
		ctx  context.Context
		ino  uint64
		off  int
		size int
		body io.Reader
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantS   int
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &volume{
				mw:    tt.fields.mw,
				ec:    tt.fields.ec,
				name:  tt.fields.name,
				owner: tt.fields.owner,
			}
			gotS, err := v.writeAt(tt.args.ctx, tt.args.ino, tt.args.off, tt.args.size, tt.args.body)
			if (err != nil) != tt.wantErr {
				t.Errorf("writeAt() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotS != tt.wantS {
				t.Errorf("writeAt() gotS = %v, want %v", gotS, tt.wantS)
			}
		})
	}
}
