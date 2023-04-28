package impl

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"reflect"
	"syscall"
	"testing"

	"github.com/cubefs/cubefs/util"

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
		_, err = v.CreateFile(ctx, parIno, fileName)
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
		_, err = v.InitMultiPart(ctx, "testInit", oldIno, nil)
		require.Equal(t, err, sdk.ErrBadRequest)
	}

	{
		// failed LookupPath(subdir string) (uint64, error)
		mockMeta.EXPECT().LookupPath(filePath).Return(uint64(0), syscall.EAGAIN)
		_, err = v.InitMultiPart(ctx, filePath, oldIno, nil)
		require.Equal(t, err, syscallToErr(syscall.EAGAIN))
	}
	{
		// lookup ino not equal id
		mockMeta.EXPECT().LookupPath(filePath).Return(uint64(5), nil)
		_, err = v.InitMultiPart(ctx, filePath, oldIno, nil)
		require.Equal(t, err, sdk.ErrConflict)
	}
	mockMeta.EXPECT().LookupPath(filePath).Return(oldIno, nil).AnyTimes()
	{
		// failed InitMultipart_ll(path string, extend map[string]string) (multipartId string, err error)
		mockMeta.EXPECT().InitMultipart_ll(filePath, nil).Return("", syscall.EAGAIN)
		_, err = v.InitMultiPart(ctx, filePath, oldIno, nil)
		require.Equal(t, err, syscallToErr(syscall.EAGAIN))
	}

	tmpUploadId := "testUploadId"
	mockMeta.EXPECT().InitMultipart_ll(filePath, nil).Return(tmpUploadId, nil)
	uploadId, err = v.InitMultiPart(ctx, filePath, oldIno, nil)
	require.NoError(t, err)
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
	{
		// failed Create_ll(parentID uint64, name string, mode, uid, gid uint32, target []byte) (*proto.InodeInfo, error)
		mockMeta.EXPECT().Create_ll(parIno, name, any, any, any, nil).Return(nil, syscall.EEXIST)
		_, err = v.Mkdir(ctx, parIno, name)
		require.Equal(t, err, syscallToErr(syscall.EEXIST))
	}
	{
		inoIfo := &proto.InodeInfo{
			Inode: 10,
		}
		mockMeta.EXPECT().Create_ll(parIno, name, any, any, any, nil).Return(inoIfo, nil)
		newIfo, err := v.Mkdir(ctx, parIno, name)
		require.NoError(t, err)
		require.True(t, inoIfo.Inode == newIfo.Inode)
	}
}

func Test_volume_NewInodeLock(t *testing.T) {
	v := &volume{}
	v.NewInodeLock()
}

func Test_volume_ReadDirAll(t *testing.T) {
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
		// failed // ReadDirLimit_ll(parentID uint64, from string, limit uint64) ([]proto.Dentry, error)
		mockMeta.EXPECT().ReadDirLimit_ll(parIno, any, any).Return(nil, syscall.ENOENT)
		_, err = v.ReadDirAll(ctx, parIno)
		require.Equal(t, err, syscallToErr(syscall.ENOENT))
	}
	{
		dirs := []proto.Dentry{
			{Inode: 11},
			{Inode: 12},
		}
		mockMeta.EXPECT().ReadDirLimit_ll(parIno, any, any).Return(dirs, nil)
		_, err = v.ReadDirAll(ctx, parIno)
		require.NoError(t, err)
	}
}

func Test_volume_ReadFile(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockMeta := mocks.NewMockMetaOp(ctrl)
	mockData := mocks.NewMockDataOp(ctrl)
	v := &volume{
		mw: mockMeta,
		ec: mockData,
	}

	_, ctx := trace.StartSpanFromContext(context.TODO(), "")
	var err error
	parIno := uint64(10)
	data := make([]byte, 10)
	{
		// failed OpenStream(inode uint64) error
		mockData.EXPECT().OpenStream(parIno).Return(syscall.ENOENT)
		_, err = v.ReadFile(ctx, parIno, uint64(0), data)
		require.Equal(t, err, syscallToErr(syscall.ENOENT))
	}
	mockData.EXPECT().OpenStream(parIno).Return(nil).AnyTimes()
	{
		// close&read failed
		mockData.EXPECT().CloseStream(parIno).Return(syscall.ENOENT)
		// Read(inode uint64, data []byte, offset int, size int) (read int, err error)
		mockData.EXPECT().Read(parIno, any, any, any).Return(0, syscall.EBADF)
		_, err = v.ReadFile(ctx, parIno, uint64(0), data)
		require.Equal(t, err, syscallToErr(syscall.EBADF))
	}
	mockData.EXPECT().CloseStream(parIno).Return(nil)
	{
		cnt := 10
		mockData.EXPECT().Read(parIno, any, any, any).Return(cnt, nil)
		size, err := v.ReadFile(ctx, parIno, uint64(0), data)
		require.NoError(t, err)
		require.True(t, size == cnt)
	}
}

func Test_volume_Readdir(t *testing.T) {
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
		_, err = v.Readdir(ctx, parIno, "", 10001)
		require.Equal(t, err, sdk.ErrBadRequest)
	}
	{
		// failed // ReadDirLimit_ll(parentID uint64, from string, limit uint64) ([]proto.Dentry, error)
		mockMeta.EXPECT().ReadDirLimit_ll(parIno, any, any).Return(nil, syscall.ENOENT)
		_, err = v.Readdir(ctx, parIno, "", 2)
		require.Equal(t, err, syscallToErr(syscall.ENOENT))
	}
	{
		dirs := []proto.Dentry{
			{Inode: 11},
			{Inode: 12},
		}
		mockMeta.EXPECT().ReadDirLimit_ll(parIno, any, any).Return(dirs, nil)
		result, err := v.Readdir(ctx, parIno, "", 2)
		require.NoError(t, err)
		require.True(t, len(result) == 2)
	}
}

func Test_volume_Rename(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockMeta := mocks.NewMockMetaOp(ctrl)
	v := &volume{
		mw: mockMeta,
	}

	_, ctx := trace.StartSpanFromContext(context.TODO(), "")
	var err error
	srcIno, destIno, srcName, destName := uint64(10), uint64(20), "src_name", "dst_name"
	{
		// failed Rename_ll(srcParentID uint64, srcName string, dstParentID uint64, dstName string, overwritten bool) (err error)
		mockMeta.EXPECT().Rename_ll(srcIno, srcName, destIno, destName, false).Return(syscall.ENOENT)
		// Rename(ctx context.Context, srcParIno, dstParIno uint64, srcName, destName string) error {
		err = v.Rename(ctx, srcIno, destIno, srcName, destName)
		require.Equal(t, err, syscallToErr(syscall.ENOENT))
	}
	{
		mockMeta.EXPECT().Rename_ll(srcIno, srcName, destIno, destName, false).Return(nil)
		err = v.Rename(ctx, srcIno, destIno, srcName, destName)
		require.NoError(t, err)
	}
}

func Test_volume_SetAttr(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockMeta := mocks.NewMockMetaOp(ctrl)
	v := &volume{
		mw: mockMeta,
	}

	_, ctx := trace.StartSpanFromContext(context.TODO(), "")
	var err error
	attr := &sdk.SetAttrReq{}
	{
		mockMeta.EXPECT().Setattr(any, any, any, any, any, any, any).Return(syscall.ENOENT)
		err = v.SetAttr(ctx, attr)
		require.Equal(t, err, syscallToErr(syscall.ENOENT))
	}
	{
		mockMeta.EXPECT().Setattr(any, any, any, any, any, any, any).Return(nil)
		err = v.SetAttr(ctx, attr)
		require.NoError(t, err)
	}
}

func Test_volume_SetXAttr(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockMeta := mocks.NewMockMetaOp(ctrl)
	v := &volume{
		mw: mockMeta,
	}

	_, ctx := trace.StartSpanFromContext(context.TODO(), "")
	var err error
	ino := uint64(10)
	key, val := "k1", "v1"
	{
		// XAttrSet_ll(inode uint64, name, value []byte) error
		mockMeta.EXPECT().XAttrSet_ll(ino, key, []byte(val)).Return(syscall.ENOENT)
		err = v.SetXAttr(ctx, ino, key, val)
		require.Equal(t, err, syscallToErr(syscall.ENOENT))
	}
	{
		mockMeta.EXPECT().XAttrSet_ll(ino, key, []byte(val)).Return(nil)
		err = v.SetXAttr(ctx, ino, key, val)
		require.NoError(t, err)
	}
}

func Test_volume_UploadFile(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockMeta := mocks.NewMockMetaOp(ctrl)
	mockData := mocks.NewMockDataOp(ctrl)
	v := &volume{
		mw: mockMeta,
		ec: mockData,
	}

	_, ctx := trace.StartSpanFromContext(context.TODO(), "")
	var err error
	req := &sdk.UploadFileReq{
		ParIno: 1,
		OldIno: 10,
		Name:   "upload_test",
		Body:   bytes.NewBufferString("test hello world"),
		Extend: map[string]string{"k1": "v1"},
	}

	lookCase := []struct {
		ino       uint64
		mode      uint32
		returnErr error
		wantErr   error
	}{
		{0, 0, syscall.EAGAIN, syscallToErr(syscall.EAGAIN)},
		{0, 0, syscall.ENOENT, sdk.ErrConflict},
		{req.OldIno, uint32(defaultDirMod), nil, sdk.ErrConflict},
	}
	for _, lc := range lookCase {
		mockMeta.EXPECT().Lookup_ll(req.ParIno, req.Name).Return(lc.ino, lc.mode, lc.returnErr)
		_, err = v.UploadFile(ctx, req)
		require.Equal(t, err, lc.wantErr)
	}

	mockMeta.EXPECT().Lookup_ll(req.ParIno, req.Name).Return(req.OldIno, uint32(defaultFileMode), nil).AnyTimes()
	{
		// failed InodeCreate_ll(mode, uid, gid uint32, target []byte) (*proto.InodeInfo, error)
		mockMeta.EXPECT().InodeCreate_ll(any, any, any, nil).Return(nil, syscall.EAGAIN)
		_, err = v.UploadFile(ctx, req)
		require.Equal(t, err, syscallToErr(syscall.EAGAIN))
	}
	ifo := &sdk.InodeInfo{
		Inode: 10,
	}
	mockMeta.EXPECT().InodeCreate_ll(any, any, any, nil).Return(ifo, nil).AnyTimes()
	{
		// unlink, evict, open stream failed
		mockMeta.EXPECT().InodeUnlink_ll(ifo.Inode).Return(nil, syscall.ENOENT)
		mockMeta.EXPECT().Evict(ifo.Inode).Return(syscall.EAGAIN)
		mockData.EXPECT().OpenStream(ifo.Inode).Return(syscall.ENOENT)
		_, err = v.UploadFile(ctx, req)
		require.Equal(t, err, syscallToErr(syscall.ENOENT))
	}

	mockMeta.EXPECT().InodeUnlink_ll(ifo.Inode).Return(nil, nil).AnyTimes()
	mockMeta.EXPECT().Evict(ifo.Inode).Return(nil).AnyTimes()
	mockData.EXPECT().OpenStream(ifo.Inode).Return(nil).AnyTimes()

	{
		// write failed Write(inode uint64, offset int, data []byte, flags int) (write int, err error)
		mockData.EXPECT().CloseStream(ifo.Inode).Return(syscall.EAGAIN)
		mockData.EXPECT().Write(ifo.Inode, 0, any, any).Return(0, syscall.EBADF)
		_, err = v.UploadFile(ctx, req)
		require.Equal(t, err, syscallToErr(syscall.EBADF))
	}

	mockData.EXPECT().CloseStream(ifo.Inode).Return(nil).AnyTimes()
	mockData.EXPECT().Write(ifo.Inode, 0, any, any).Return(10, nil).AnyTimes()
	{
		// flush failed
		mockData.EXPECT().Flush(ifo.Inode).Return(syscall.EBADF)
		_, err = v.UploadFile(ctx, req)
		require.Equal(t, err, syscallToErr(syscall.EBADF))
	}

	mockData.EXPECT().Flush(ifo.Inode).Return(nil).AnyTimes()
	{
		// InodeGet_ll(inode uint64) (*proto.InodeInfo, error)
		mockMeta.EXPECT().InodeGet_ll(ifo.Inode).Return(nil, syscall.ENOENT)
		_, err = v.UploadFile(ctx, req)
		require.Equal(t, err, syscallToErr(syscall.ENOENT))
	}
	mockMeta.EXPECT().InodeGet_ll(ifo.Inode).Return(ifo, nil).AnyTimes()
	// BatchSetXAttr_ll(inode uint64, attrs map[string]string) error
	{
		mockMeta.EXPECT().BatchSetXAttr_ll(ifo.Inode, any).Return(syscall.ENOENT)
		_, err = v.UploadFile(ctx, req)
		require.Equal(t, err, syscallToErr(syscall.ENOENT))
	}
	mockMeta.EXPECT().BatchSetXAttr_ll(ifo.Inode, any).Return(nil).AnyTimes()
	{
		// DentryCreateEx_ll(parentID uint64, name string, inode, oldIno uint64, mode uint32) error
		mockMeta.EXPECT().DentryCreateEx_ll(req.ParIno, req.Name, ifo.Inode, req.OldIno, any).Return(syscall.EINVAL)
		_, err = v.UploadFile(ctx, req)
		require.Equal(t, err, syscallToErr(syscall.EINVAL))
	}

	mockMeta.EXPECT().DentryCreateEx_ll(req.ParIno, req.Name, ifo.Inode, req.OldIno, any).Return(nil)
	_, err = v.UploadFile(ctx, req)
	require.NoError(t, err)
}

func Test_volume_UploadMultiPart(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockMeta := mocks.NewMockMetaOp(ctrl)
	mockData := mocks.NewMockDataOp(ctrl)
	v := &volume{
		mw: mockMeta,
		ec: mockData,
	}

	_, ctx := trace.StartSpanFromContext(context.TODO(), "")
	var err error

	filePath, uploadId, partNum := "/uploadFilePath", "uploadId", uint16(2)

	data := []byte("test upload")
	body := bytes.NewBuffer(data)

	hash := md5.New()
	hash.Write(data)
	tag := hex.EncodeToString(hash.Sum(nil))

	{
		_, err = v.UploadMultiPart(ctx, "filePath", uploadId, partNum, body)
		require.Equal(t, err, sdk.ErrBadRequest)
	}
	{
		// failed InodeCreate_ll(mode, uid, gid uint32, target []byte) (*proto.InodeInfo, error)
		mockMeta.EXPECT().InodeCreate_ll(any, any, any, nil).Return(nil, syscall.EAGAIN)
		_, err = v.UploadMultiPart(ctx, filePath, uploadId, partNum, body)
		require.Equal(t, err, syscallToErr(syscall.EAGAIN))
	}
	ifo := &sdk.InodeInfo{
		Inode: 10,
	}
	mockMeta.EXPECT().InodeCreate_ll(any, any, any, nil).Return(ifo, nil).AnyTimes()
	{
		// unlink, evict, open stream failed
		mockMeta.EXPECT().InodeUnlink_ll(ifo.Inode).Return(nil, syscall.ENOENT)
		mockMeta.EXPECT().Evict(ifo.Inode).Return(syscall.EAGAIN)
		mockData.EXPECT().OpenStream(ifo.Inode).Return(syscall.ENOENT)
		_, err = v.UploadMultiPart(ctx, filePath, uploadId, partNum, body)
		require.Equal(t, err, syscallToErr(syscall.ENOENT))
	}

	mockMeta.EXPECT().InodeUnlink_ll(ifo.Inode).Return(nil, nil).AnyTimes()
	mockMeta.EXPECT().Evict(ifo.Inode).Return(nil).AnyTimes()
	mockData.EXPECT().OpenStream(ifo.Inode).Return(nil).AnyTimes()

	{
		// write failed Write(inode uint64, offset int, data []byte, flags int) (write int, err error)
		mockData.EXPECT().CloseStream(ifo.Inode).Return(syscall.EAGAIN)
		mockData.EXPECT().Write(ifo.Inode, 0, any, any).Return(0, syscall.EBADF)
		_, err = v.UploadMultiPart(ctx, filePath, uploadId, partNum, body)
		require.Equal(t, err, syscallToErr(syscall.EBADF))
	}

	mockData.EXPECT().CloseStream(ifo.Inode).Return(nil).AnyTimes()
	mockData.EXPECT().Write(ifo.Inode, 0, any, any).Return(10, nil).AnyTimes()
	{
		// flush failed
		mockData.EXPECT().Flush(ifo.Inode).Return(syscall.EBADF)
		_, err = v.UploadMultiPart(ctx, filePath, uploadId, partNum, body)
		require.Equal(t, err, syscallToErr(syscall.EBADF))
	}

	mockData.EXPECT().Flush(ifo.Inode).Return(nil).AnyTimes()
	{
		// failed AddMultipartPart_ll(path, multipartId string, partId uint16, size uint64, md5 string, inodeInfo *proto.InodeInfo) (err error)
		mockMeta.EXPECT().AddMultipartPart_ll(filePath, uploadId, partNum, any, tag, any).Return(syscall.EINVAL)
		_, err = v.UploadMultiPart(ctx, filePath, uploadId, partNum, body)
		require.Equal(t, err, syscallToErr(syscall.EINVAL))
	}

	mockMeta.EXPECT().AddMultipartPart_ll(filePath, uploadId, partNum, any, tag, any).Return(nil)
	_, err = v.UploadMultiPart(ctx, filePath, uploadId, partNum, body)
	require.NoError(t, err)
}

func Test_volume_WriteFile(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockMeta := mocks.NewMockMetaOp(ctrl)
	mockData := mocks.NewMockDataOp(ctrl)
	v := &volume{
		mw: mockMeta,
		ec: mockData,
	}

	ino, off, size := uint64(10), uint64(0), uint64(100)
	body := bytes.NewBufferString("test write file")

	_, ctx := trace.StartSpanFromContext(context.TODO(), "")
	var err error
	{
		// failed OpenStream(inode uint64) error
		mockData.EXPECT().OpenStream(ino).Return(syscall.ENOENT)
		err = v.WriteFile(ctx, ino, off, size, body)
		require.Equal(t, err, syscallToErr(syscall.ENOENT))
	}
	mockData.EXPECT().OpenStream(ino).Return(nil).AnyTimes()
	{
		// close failed, write failed
		mockData.EXPECT().CloseStream(ino).Return(syscall.EBADF)
		mockData.EXPECT().Write(ino, any, any, any).Return(10, syscall.EBADF)
		err = v.WriteFile(ctx, ino, off, size, body)
		require.Equal(t, err, syscallToErr(syscall.EBADF))
	}

	body = bytes.NewBufferString("test write file")
	mockData.EXPECT().CloseStream(ino).Return(nil)
	mockData.EXPECT().Write(ino, any, any, any).Return(10, nil)
	err = v.WriteFile(ctx, ino, off, size, body)
	require.NoError(t, err)
}

func Test_volume_getStatByIno(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockMeta := mocks.NewMockMetaOp(ctrl)
	v := &volume{
		mw: mockMeta,
	}

	ino := uint64(10)
	_, ctx := trace.StartSpanFromContext(context.TODO(), "")
	var err error
	var st *sdk.StatFs
	{
		// failed ReadDirLimit_ll(parentID uint64, from string, limit uint64) ([]proto.Dentry, error)
		mockMeta.EXPECT().ReadDirLimit_ll(ino, any, any).Return(nil, syscall.ENOENT)
		_, err = v.getStatByIno(ctx, ino)
		require.Equal(t, err, syscallToErr(syscall.ENOENT))
	}
	{
		// return empty
		mockMeta.EXPECT().ReadDirLimit_ll(ino, any, any).Return([]proto.Dentry{}, nil)
		st, err = v.getStatByIno(ctx, ino)
		require.NoError(t, err)
		require.Equal(t, 0, st.Size)
	}
	{
		mockMeta.EXPECT().ReadDirLimit_ll(ino, any, any).Return([]proto.Dentry{}, nil)
		st, err = v.getStatByIno(ctx, ino)
		require.NoError(t, err)
		require.Equal(t, 0, st.Size)
	}

	dirs := []proto.Dentry{
		{Name: "d1", Inode: 11, Type: uint32(defaultDirMod)},
		{Name: "f1", Inode: 12, Type: 0},
		{Name: "f2", Inode: 13, Type: 0},
	}

	mockMeta.EXPECT().ReadDirLimit_ll(ino, any, any).Return(dirs, nil).AnyTimes()
	{
		// subDir read dir failed
		mockMeta.EXPECT().ReadDirLimit_ll(uint64(11), any, any).Return(nil, syscall.EBADF)
		_, err = v.getStatByIno(ctx, ino)
		require.Equal(t, err, syscallToErr(syscall.EBADF))
	}

	mockMeta.EXPECT().ReadDirLimit_ll(uint64(11), any, any).Return([]proto.Dentry{}, nil).AnyTimes()
	{
		// failed BatchInodeGetWith(inodes []uint64) (batchInfos []*proto.InodeInfo, err error)
		mockMeta.EXPECT().BatchInodeGetWith(any).Return(nil, syscall.ENOENT)
		_, err = v.getStatByIno(ctx, ino)
		require.Equal(t, err, syscallToErr(syscall.ENOENT))
	}

	inoInfos := []*sdk.InodeInfo{
		{Size: 101, Inode: 12},
		{Size: 103, Inode: 13},
	}
	mockMeta.EXPECT().BatchInodeGetWith(any).Return(inoInfos, nil)
	st, err = v.getStatByIno(ctx, ino)
	require.NoError(t, err)
	require.True(t, st.Size == 204)
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

	{
		tmpDirs := []string{
			" ", "/",
		}

		for _, dir := range tmpDirs {
			ino, err = v.mkdirByPath(ctx, dir)
			require.True(t, err == nil)
			require.True(t, ino == proto.RootIno)
		}
	}

	defIno := uint64(0)
	defMode := uint32(0)
	tmpDir := "/test/createDir"
	{
		// lookup failed (parentID uint64, name string) (inode uint64, mode uint32, err error)
		mockMeta.EXPECT().Lookup_ll(any, any).Return(defIno, defMode, syscall.EAGAIN)
		_, err = v.mkdirByPath(ctx, tmpDir)
		require.True(t, err != nil)
	}
	{
		// lookup success but not dir
		mockMeta.EXPECT().Lookup_ll(any, any).Return(defIno, defMode, nil)
		_, err = v.mkdirByPath(ctx, tmpDir)
		require.True(t, err != nil)
	}
	// lookup no entry
	mockMeta.EXPECT().Lookup_ll(any, any).Return(defIno, defMode, syscall.ENOENT).AnyTimes()
	{
		// create failed, (parentID uint64, name string, mode, uid, gid uint32, target []byte) (*proto.InodeInfo, error)
		mockMeta.EXPECT().Create_ll(any, any, any, any, any, any).Return(nil, syscall.ENOMEM).AnyTimes()
		_, err = v.mkdirByPath(ctx, tmpDir)
		require.True(t, err != nil)
	}
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

type mockReader struct{}

func (mr *mockReader) Read(data []byte) (int, error) {
	return 0, syscall.EBADF
}

func Test_volume_writeAt(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockMeta := mocks.NewMockMetaOp(ctrl)
	mockData := mocks.NewMockDataOp(ctrl)
	v := &volume{
		mw: mockMeta,
		ec: mockData,
	}

	_, ctx := trace.StartSpanFromContext(context.TODO(), "")
	var err error
	ino, off, size := uint64(1020), 0, 1024*512

	data := make([]byte, 1024*1024)
	body := bytes.NewReader(data)
	{
		_, err = v.writeAt(ctx, ino, off, size, &mockReader{})
		require.Equal(t, err, sdk.ErrBadRequest)
	}
	{
		// failed Write(inode uint64, offset int, data []byte, flags int) (write int, err error)
		mockData.EXPECT().Write(ino, any, any, any).Return(0, syscall.ENOENT)
		_, err = v.writeAt(ctx, ino, off, size, body)
		require.Equal(t, err, syscallToErr(syscall.ENOENT))
	}

	body = bytes.NewReader(data)
	mockData.EXPECT().Write(ino, any, any, any).Return(util.BlockSize, nil).AnyTimes()
	{
		total, err := v.writeAt(ctx, ino, off, size, body)
		require.NoError(t, err)
		require.Equal(t, total, size)
	}

	body = bytes.NewReader(data)
	{
		total, err := v.writeAt(ctx, ino, off, len(data)*2, body)
		require.NoError(t, err)
		require.Equal(t, total, len(data))
	}
}
