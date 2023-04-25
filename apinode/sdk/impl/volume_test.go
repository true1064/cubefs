package impl

import (
	"context"
	"fmt"
	"io"
	"reflect"
	"syscall"
	"testing"

	"github.com/cubefs/cubefs/apinode/sdk"
	"github.com/cubefs/cubefs/apinode/testing/mocks"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/data/stream"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
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
				// t.Errorf("got name %s, want name %s", got.Info().Name, tt.want.Info().Name)
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
		{"t1", args{sdk.ErrInternalServerError}, sdk.ErrInternalServerError},
		{"t2", args{syscall.EAGAIN}, sdk.ErrRetryAgain},
		{"t3", args{syscall.EEXIST}, sdk.ErrExist},
		{"t4", args{syscall.ENOENT}, sdk.ErrNotFound},
		{"t5", args{syscall.ENOMEM}, sdk.ErrFull},
		{"t6", args{syscall.EINVAL}, sdk.ErrBadRequest},
		{"t7", args{syscall.EPERM}, sdk.ErrForbidden},
		{"t8", args{syscall.ENOTSUP}, sdk.ErrConflict},
		{"t9", args{syscall.EBADF}, sdk.ErrBadFile},
		{"t10", args{fmt.Errorf("unkonwn error")}, sdk.ErrInternalServerError},
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
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockMeta := mocks.NewMockMetaOp(ctrl)
	v := &volume{
		mw: mockMeta,
	}
	_, ctx := trace.StartSpanFromContext(context.TODO(), "")
	filePath := "/filepath"
	uploadId := "uploadId"
	var err error
	{
		// path is not illegal
		err = v.AbortMultiPart(ctx, "test", "")
		require.True(t, err == sdk.ErrBadRequest)
	}
	{
		// getMultiPart failed
		mockMeta.EXPECT().GetMultipart_ll(gomock.Any(), gomock.Any()).Return(nil, syscall.EEXIST)
		err = v.AbortMultiPart(ctx, filePath, uploadId)
		require.True(t, err != nil)
	}

	partInfo := &proto.MultipartInfo{
		Parts: []*proto.MultipartPartInfo{
			{Inode: 1},
		},
	}

	{
		// remove multipart, inode unlink, evict failed
		mockMeta.EXPECT().GetMultipart_ll(gomock.Any(), gomock.Any()).Return(partInfo, nil).AnyTimes()
		mockMeta.EXPECT().RemoveMultipart_ll(filePath, uploadId).Return(syscall.ENOENT)
		mockMeta.EXPECT().InodeUnlink_ll(gomock.Any()).Return(nil, syscall.ENOENT)
		mockMeta.EXPECT().Evict(gomock.Any()).Return(syscall.ENOENT)
		err = v.AbortMultiPart(ctx, filePath, uploadId)
		require.True(t, err != nil)
	}

	{
		mockMeta.EXPECT().RemoveMultipart_ll(filePath, uploadId).Return(nil)
		mockMeta.EXPECT().InodeUnlink_ll(gomock.Any()).Return(nil, nil)
		mockMeta.EXPECT().Evict(gomock.Any()).Return(nil)
		err = v.AbortMultiPart(ctx, filePath, uploadId)
		require.True(t, err == nil)
	}
}

func Test_volume_BatchDeleteXAttr(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockMeta := mocks.NewMockMetaOp(ctrl)
	v := &volume{
		mw: mockMeta,
	}
	_, ctx := trace.StartSpanFromContext(context.TODO(), "")
	tmpIno := uint64(10)
	keys := []string{"k1", "k2"}
	var err error

	{
		mockMeta.EXPECT().XBatchDelAttr_ll(gomock.Any(), gomock.Any()).Return(syscall.ENOTSUP)
		err = v.BatchDeleteXAttr(ctx, tmpIno, keys)
		require.Error(t, err)
	}
	{
		mockMeta.EXPECT().XBatchDelAttr_ll(gomock.Any(), gomock.Any()).Return(nil)
		err = v.BatchDeleteXAttr(ctx, tmpIno, keys)
		require.NoError(t, err)
	}
}

func Test_volume_BatchGetInodes(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockMeta := mocks.NewMockMetaOp(ctrl)
	v := &volume{
		mw: mockMeta,
	}

	_, ctx := trace.StartSpanFromContext(context.TODO(), "")
	keys := []uint64{1, 10}
	var err error

	{
		mockMeta.EXPECT().BatchInodeGetWith(gomock.Any()).Return(nil, syscall.ENOTSUP)
		_, err = v.BatchGetInodes(ctx, keys)
		require.Error(t, err)
	}
	{
		mockMeta.EXPECT().BatchInodeGetWith(gomock.Any()).Return([]*proto.InodeInfo{}, nil)
		_, err = v.BatchGetInodes(ctx, keys)
		require.True(t, err == nil)
	}
}

func Test_volume_BatchSetXAttr(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockMeta := mocks.NewMockMetaOp(ctrl)
	v := &volume{
		mw: mockMeta,
	}

	_, ctx := trace.StartSpanFromContext(context.TODO(), "")
	attrs := map[string]string{
		"k1": "v1",
		"k2": "v2",
	}
	var err error

	{
		mockMeta.EXPECT().BatchSetXAttr_ll(gomock.Any(), gomock.Any()).Return(syscall.ENOTSUP)
		err = v.BatchSetXAttr(ctx, 10, attrs)
		require.Error(t, err)
	}
	{
		mockMeta.EXPECT().BatchSetXAttr_ll(gomock.Any(), gomock.Any()).Return(nil)
		err = v.BatchSetXAttr(ctx, 10, attrs)
		require.NoError(t, err)
	}
}

func Test_volume_CompleteMultiPart(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockMeta := mocks.NewMockMetaOp(ctrl)
	v := &volume{
		mw: mockMeta,
	}

	_, ctx := trace.StartSpanFromContext(context.TODO(), "")
	var err error

	{
		// path not illegal
		_, err = v.CompleteMultiPart(ctx, "test", "", 0, nil)
		require.True(t, err == sdk.ErrBadRequest)
	}

	filePath := "/completePath"
	uploadId := "uploadId"
	parts := []sdk.Part{
		{ID: 3},
	}
	{
		// order is not valid
		_, err = v.CompleteMultiPart(ctx, filePath, uploadId, 0, parts)
		require.True(t, err == sdk.ErrInvalidPartOrder)
	}

	parts = []sdk.Part{
		{ID: 1, MD5: "md1"},
	}
	resultPart := &proto.MultipartInfo{
		Parts: []*proto.MultipartPartInfo{
			{Inode: 1, MD5: "md2"},
		},
	}
	{
		mockMeta.EXPECT().GetMultipart_ll(filePath, uploadId).Return(resultPart, nil)
		// md5 not equal
		_, err = v.CompleteMultiPart(ctx, filePath, uploadId, 0, parts)
		require.True(t, err == sdk.ErrInvalidPart)
	}
	resultPart = &proto.MultipartInfo{
		Parts: []*proto.MultipartPartInfo{
			{Inode: 1, MD5: "md1"},
		},
	}
	mockMeta.EXPECT().GetMultipart_ll(filePath, uploadId).Return(resultPart, nil).AnyTimes()
	any := gomock.Any()
	{
		// inode create failed
		mockMeta.EXPECT().InodeCreate_ll(any, any, any, any).Return(nil, syscall.EAGAIN)
		_, err = v.CompleteMultiPart(ctx, filePath, uploadId, 0, parts)
		require.True(t, err == syscallToErr(syscall.EAGAIN))
	}
	newIno := uint64(10)
	mockMeta.EXPECT().InodeCreate_ll(any, any, any, any).Return(&proto.InodeInfo{Inode: newIno}, nil).AnyTimes()
	{
		// delete error (*proto.InodeInfo, error)
		mockMeta.EXPECT().InodeDelete_ll(newIno).Return(syscall.ENOENT).AnyTimes()
		// getExtents error (gen uint64, size uint64, extents []proto.ExtentKey, err error)
		mockMeta.EXPECT().GetExtents(uint64(1)).Return(uint64(0), uint64(0), nil, syscall.ENOENT)
		_, err = v.CompleteMultiPart(ctx, filePath, uploadId, 0, parts)
		require.True(t, err == syscallToErr(syscall.ENOENT))
	}
	exts := []proto.ExtentKey{
		{FileOffset: 10},
	}
	mockMeta.EXPECT().GetExtents(uint64(1)).Return(uint64(0), uint64(0), exts, nil).AnyTimes()
	{
		// append extent failed
		mockMeta.EXPECT().AppendExtentKeys(any, any).Return(syscall.ENOTSUP)
		_, err = v.CompleteMultiPart(ctx, filePath, uploadId, 0, parts)
		require.Equal(t, err, syscallToErr(syscall.ENOTSUP))
	}
	mockMeta.EXPECT().AppendExtentKeys(newIno, any).Return(nil).AnyTimes()
	{
		// remove multipart failed
		mockMeta.EXPECT().RemoveMultipart_ll(any, any).Return(syscall.ENOTSUP)
		_, err = v.CompleteMultiPart(ctx, filePath, uploadId, 0, parts)
		require.Equal(t, err, syscallToErr(syscall.ENOTSUP))
	}
	mockMeta.EXPECT().RemoveMultipart_ll(any, any).Return(nil).AnyTimes()
	mockMeta.EXPECT().InodeDelete_ll(any).Return(syscall.ENOENT).AnyTimes()
	{
		mockMeta.EXPECT().DentryCreateEx_ll(any, any, any, any, any).Return(syscall.EEXIST)
		_, err = v.CompleteMultiPart(ctx, filePath, uploadId, 0, parts)
		require.Equal(t, err, syscallToErr(syscall.EEXIST))
	}
	mockMeta.EXPECT().DentryCreateEx_ll(any, any, newIno, uint64(0), any).Return(nil)
	_, err = v.CompleteMultiPart(ctx, filePath, uploadId, 0, parts)
	require.True(t, err == nil)
}

func Test_volume_CreateFile(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockMeta := mocks.NewMockMetaOp(ctrl)
	v := &volume{
		mw: mockMeta,
	}

	_, ctx := trace.StartSpanFromContext(context.TODO(), "")
	fileName := "tmpCreateFile"
	var err error
	var ifo *proto.InodeInfo
	parIno := uint64(10)

	{
		// create failed (parentID uint64, name string, mode, uid, gid uint32, target []byte) (*proto.InodeInfo, error)
		mockMeta.EXPECT().Create_ll(parIno, fileName, any, any, any, nil).Return(nil, syscall.EEXIST)
		ifo, err = v.CreateFile(ctx, parIno, fileName)
		require.Equal(t, err, syscallToErr(syscall.EEXIST))
	}
	{
		tmIfo := &proto.InodeInfo{Inode: 10}
		mockMeta.EXPECT().Create_ll(parIno, fileName, any, any, any, nil).Return(tmIfo, nil)
		ifo, err = v.CreateFile(ctx, parIno, fileName)
		require.NoError(t, err)
		require.True(t, ifo.Inode == tmIfo.Inode)
	}
}

func Test_volume_Delete(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockMeta := mocks.NewMockMetaOp(ctrl)
	v := &volume{
		mw: mockMeta,
	}

	_, ctx := trace.StartSpanFromContext(context.TODO(), "")
	fileName := "tmpDeleteFile"
	var err error
	parIno := uint64(10)

	{
		// delete failed Delete_ll(parentID uint64, name string, isDir bool) (*proto.InodeInfo, error)
		mockMeta.EXPECT().Delete_ll(parIno, fileName, false).Return(nil, syscall.ENOENT)
		err = v.Delete(ctx, parIno, fileName, false)
		require.Equal(t, err, syscallToErr(syscall.ENOENT))
	}
	{
		mockMeta.EXPECT().Delete_ll(parIno, fileName, false).Return(nil, nil)
		err = v.Delete(ctx, parIno, fileName, false)
		require.NoError(t, err)
	}
}

func Test_volume_DeleteXAttr(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockMeta := mocks.NewMockMetaOp(ctrl)
	v := &volume{
		mw: mockMeta,
	}

	_, ctx := trace.StartSpanFromContext(context.TODO(), "")
	key := "tmpDeleteXAttr"
	var err error
	parIno := uint64(10)
	{
		// delete failed XAttrDel_ll(inode uint64, name string) error
		mockMeta.EXPECT().XAttrDel_ll(parIno, key).Return(syscall.ENOENT)
		err = v.DeleteXAttr(ctx, parIno, key)
		require.Equal(t, err, syscallToErr(syscall.ENOENT))
	}
	{
		mockMeta.EXPECT().XAttrDel_ll(parIno, key).Return(nil)
		err = v.DeleteXAttr(ctx, parIno, key)
		require.NoError(t, err)
	}
}

func Test_volume_GetInode(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockMeta := mocks.NewMockMetaOp(ctrl)
	v := &volume{
		mw: mockMeta,
	}

	_, ctx := trace.StartSpanFromContext(context.TODO(), "")
	var err error
	parIno := uint64(10)
	{
		// inoGet failed InodeGet_ll(inode uint64) (*proto.InodeInfo, error)
		mockMeta.EXPECT().InodeGet_ll(parIno).Return(nil, syscall.ENOENT)
		_, err = v.GetInode(ctx, parIno)
		require.Equal(t, err, syscallToErr(syscall.ENOENT))
	}
	{
		tmpIfo := &proto.InodeInfo{
			Inode: 10,
		}
		mockMeta.EXPECT().InodeGet_ll(parIno).Return(tmpIfo, nil)
		ifo, err := v.GetInode(ctx, parIno)
		require.NoError(t, err)
		require.True(t, ifo.Inode == tmpIfo.Inode)
	}
}

func Test_volume_GetXAttr(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockMeta := mocks.NewMockMetaOp(ctrl)
	v := &volume{
		mw: mockMeta,
	}

	_, ctx := trace.StartSpanFromContext(context.TODO(), "")
	var err error
	parIno := uint64(10)
	key, val := "k1", "v1"
	{
		// inoGet failed XAttrGet_ll(inode uint64, name string) (*proto.XAttrInfo, error)
		mockMeta.EXPECT().XAttrGet_ll(parIno, key).Return(nil, syscall.ENOENT)
		_, err = v.GetXAttr(ctx, parIno, key)
		require.Equal(t, err, syscallToErr(syscall.ENOENT))
	}
	{
		tmpXAttr := &proto.XAttrInfo{
			XAttrs: map[string]string{key: val},
		}
		mockMeta.EXPECT().XAttrGet_ll(parIno, key).Return(tmpXAttr, nil)
		newVal, err := v.GetXAttr(ctx, parIno, key)
		require.NoError(t, err)
		require.True(t, newVal == val)
	}
}

func Test_volume_GetXAttrMap(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockMeta := mocks.NewMockMetaOp(ctrl)
	v := &volume{
		mw: mockMeta,
	}

	_, ctx := trace.StartSpanFromContext(context.TODO(), "")
	var err error
	parIno := uint64(10)
	key, val := "k1", "v1"
	{
		// XAttrGetAll_ll failed XAttrGetAll_ll(inode uint64) (*proto.XAttrInfo, error)
		mockMeta.EXPECT().XAttrGetAll_ll(parIno).Return(nil, syscall.ENOENT)
		_, err = v.GetXAttrMap(ctx, parIno)
		require.Equal(t, err, syscallToErr(syscall.ENOENT))
	}
	{
		tmpXAttr := &proto.XAttrInfo{
			XAttrs: map[string]string{key: val},
		}
		mockMeta.EXPECT().XAttrGetAll_ll(parIno).Return(tmpXAttr, nil)
		newMap, err := v.GetXAttrMap(ctx, parIno)
		require.NoError(t, err)
		require.True(t, newMap[key] == val)
	}
}

func Test_volume_Info(t *testing.T) {
	name := "tmpInfoName"
	v := &volume{
		name: name,
	}
	require.True(t, v.Info().Name == name)
}

func Test_volume_InitMultiPart(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockMeta := mocks.NewMockMetaOp(ctrl)
	v := &volume{
		mw: mockMeta,
	}
	_, ctx := trace.StartSpanFromContext(context.TODO(), "")

	var err error
	filePath := "/tmpInitMultiPart"
	oldIno := uint64(10)
	var uploadId string

	{
		// path not valid
		uploadId, err = v.InitMultiPart(ctx, "testInit", oldIno, nil)
		require.Equal(t, err, sdk.ErrBadRequest)
	}

	{
		// failed LookupPath(subdir string) (uint64, error)
		mockMeta.EXPECT().LookupPath(filePath).Return(uint64(0), syscall.EAGAIN)
		uploadId, err = v.InitMultiPart(ctx, filePath, oldIno, nil)
		require.Equal(t, err, syscallToErr(syscall.EAGAIN))
	}
	{
		// lookup ino not equal id
		mockMeta.EXPECT().LookupPath(filePath).Return(uint64(5), nil)
		uploadId, err = v.InitMultiPart(ctx, filePath, oldIno, nil)
		require.Equal(t, err, sdk.ErrConflict)
	}
	mockMeta.EXPECT().LookupPath(filePath).Return(oldIno, nil).AnyTimes()
	{
		// failed InitMultipart_ll(path string, extend map[string]string) (multipartId string, err error)
		mockMeta.EXPECT().InitMultipart_ll(filePath, nil).Return("", syscall.EAGAIN)
		uploadId, err = v.InitMultiPart(ctx, filePath, oldIno, nil)
		require.Equal(t, err, syscallToErr(syscall.EAGAIN))
	}

	tmpUploadId := "testUploadId"
	mockMeta.EXPECT().InitMultipart_ll(filePath, nil).Return(tmpUploadId, nil)
	uploadId, err = v.InitMultiPart(ctx, filePath, oldIno, nil)
	require.Equal(t, uploadId, tmpUploadId)

	t.Logf("get uploadId %s", uploadId)
}

func Test_volume_ListMultiPart(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockMeta := mocks.NewMockMetaOp(ctrl)
	v := &volume{
		mw: mockMeta,
	}
	span, ctx := trace.StartSpanFromContext(context.TODO(), "")

	var err error
	filePath := "/tmpInitMultiPart"
	var uploadId string
	count := uint64(3)
	marker := uint64(0)
	var parts []*sdk.Part

	{
		// path not valid
		_, _, _, err = v.ListMultiPart(ctx, uploadId, filePath, count, marker)
		require.Equal(t, err, sdk.ErrBadRequest)
	}

	{
		// failed GetMultipart_ll(path, multipartId string) (info *proto.MultipartInfo, err error)
		mockMeta.EXPECT().GetMultipart_ll(filePath, uploadId).Return(nil, syscall.ENOENT)
		_, _, _, err = v.ListMultiPart(ctx, filePath, uploadId, count, marker)
		require.Equal(t, err, syscallToErr(syscall.ENOENT))
	}

	partsResult := &proto.MultipartInfo{
		Parts: []*proto.MultipartPartInfo{
			{ID: 1, MD5: "xxt"},
			{ID: 2, MD5: "xx2"},
			{ID: 3, MD5: "xx3"},
			{ID: 4, MD5: "xx4"},
			{ID: 5, MD5: "xx5"},
		},
	}
	mockMeta.EXPECT().GetMultipart_ll(filePath, uploadId).Return(partsResult, nil).AnyTimes()
	parts, next, trunc, err := v.ListMultiPart(ctx, filePath, uploadId, count, marker)
	span.Infof("listMultiPart failed, len(%d), next %d, trunc %v, err %v", len(parts), next, trunc, err)
	require.Equal(t, err, nil)
	require.True(t, len(parts) == int(count) && next == count+marker && trunc)
	for idx, p := range parts {
		pt := partsResult.Parts[idx]
		require.True(t, pt.MD5 == p.MD5 && pt.ID == p.ID)
	}

	newMarker := uint64(3)
	parts, next, trunc, err = v.ListMultiPart(ctx, filePath, uploadId, count, newMarker)
	span.Infof("listMultiPart failed, len(%d), next %d, trunc %v, err %v", len(parts), next, trunc, err)
	require.Equal(t, err, nil)
	require.True(t, len(parts) == 2 && next == 0 && !trunc)
	for idx, p := range parts {
		pt := partsResult.Parts[idx+int(newMarker)]
		require.Equal(t, pt.ID, p.ID)
		require.Equal(t, pt.MD5, p.MD5)
	}
}

func Test_volume_ListXAttr(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockMeta := mocks.NewMockMetaOp(ctrl)
	v := &volume{
		mw: mockMeta,
	}

	_, ctx := trace.StartSpanFromContext(context.TODO(), "")
	var err error
	parIno := uint64(10)
	keys := []string{"k1", "k2"}
	{
		// failed XAttrsList_ll(inode uint64) ([]string, error)
		mockMeta.EXPECT().XAttrsList_ll(parIno).Return(nil, syscall.ENOENT)
		_, err = v.ListXAttr(ctx, parIno)
		require.Equal(t, err, syscallToErr(syscall.ENOENT))
	}
	{
		mockMeta.EXPECT().XAttrsList_ll(parIno).Return(keys, nil)
		resultKeys, err := v.ListXAttr(ctx, parIno)
		require.NoError(t, err)
		for idx, k := range keys {
			require.True(t, k == resultKeys[idx])
		}
	}
}

func Test_volume_Lookup(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockMeta := mocks.NewMockMetaOp(ctrl)
	v := &volume{
		mw: mockMeta,
	}

	_, ctx := trace.StartSpanFromContext(context.TODO(), "")
	var err error
	parIno := uint64(10)
	name := "tmpLookName"
	tmpIno, mode := uint64(100), uint32(defaultFileMode)
	{
		// failed Lookup_ll(parentID uint64, name string) (inode uint64, mode uint32, err error)
		mockMeta.EXPECT().Lookup_ll(parIno, name).Return(tmpIno, mode, syscall.ENOENT)
		_, err = v.Lookup(ctx, parIno, name)
		require.Equal(t, err, syscallToErr(syscall.ENOENT))
	}
	{
		mockMeta.EXPECT().Lookup_ll(parIno, name).Return(tmpIno, mode, nil)
		info, err := v.Lookup(ctx, parIno, name)
		require.NoError(t, err)
		require.True(t, tmpIno == info.Inode && mode == info.Type)
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
			got, err := v.UploadFile(nil, tt.args.req)
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
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockMeta := mocks.NewMockMetaOp(ctrl)
	v := &volume{
		mw: mockMeta,
	}

	_, ctx := trace.StartSpanFromContext(context.TODO(), "")
	var err error
	var ino uint64
	tmpDirs := []string{
		" ", "/",
	}

	for _, dir := range tmpDirs {
		ino, err = v.mkdirByPath(ctx, dir)
		require.True(t, err == nil)
		require.True(t, ino == proto.RootIno)
	}

	defIno := uint64(0)
	defMode := uint32(0)
	tmpDir := "/test/createDir"
	// lookup failed (parentID uint64, name string) (inode uint64, mode uint32, err error)
	mockMeta.EXPECT().Lookup_ll(any, any).Return(defIno, defMode, syscall.EAGAIN)
	_, err = v.mkdirByPath(ctx, tmpDir)
	require.True(t, err != nil)

	// lookup success but not dir
	mockMeta.EXPECT().Lookup_ll(any, any).Return(defIno, defMode, nil)
	_, err = v.mkdirByPath(ctx, tmpDir)
	require.True(t, err != nil)

	// lookup success
	mockMeta.EXPECT().Lookup_ll(any, any).Return(defIno, defMode, nil)

	// lookup no entry
	mockMeta.EXPECT().Lookup_ll(any, any).Return(defIno, defMode, syscall.ENOENT).AnyTimes()
	// create failed, (parentID uint64, name string, mode, uid, gid uint32, target []byte) (*proto.InodeInfo, error)
	mockMeta.EXPECT().Create_ll(any, any, any, any, any, any).Return(nil, syscall.ENOMEM).AnyTimes()
	_, err = v.mkdirByPath(ctx, tmpDir)
	require.True(t, err != nil)

	{
		// create return exist
		mockMeta.EXPECT().Create_ll(any, any, any, any, any, any).Return(nil, syscall.EEXIST).AnyTimes()
		{
			// lookup return no entry
			mockMeta.EXPECT().Lookup_ll(any, any).Return(defIno, defMode, syscall.ENOENT).AnyTimes()
			_, err = v.mkdirByPath(ctx, tmpDir)
			require.True(t, err != nil)
		}
		// lookup return not dir
		mockMeta.EXPECT().Lookup_ll(any, any).Return(defIno, defMode, nil).AnyTimes()
		_, err = v.mkdirByPath(ctx, tmpDir)
		require.Error(t, err)
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
