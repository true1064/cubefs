// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/cubefs/cubefs/apinode/sdk/impl (interfaces: DataOp,MetaOp,IMaster)

// Package mocks is a generated GoMock package.
package mocks

import (
	reflect "reflect"

	proto "github.com/cubefs/cubefs/proto"
	gomock "github.com/golang/mock/gomock"
)

// MockDataOp is a mock of DataOp interface.
type MockDataOp struct {
	ctrl     *gomock.Controller
	recorder *MockDataOpMockRecorder
}

// MockDataOpMockRecorder is the mock recorder for MockDataOp.
type MockDataOpMockRecorder struct {
	mock *MockDataOp
}

// NewMockDataOp creates a new mock instance.
func NewMockDataOp(ctrl *gomock.Controller) *MockDataOp {
	mock := &MockDataOp{ctrl: ctrl}
	mock.recorder = &MockDataOpMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockDataOp) EXPECT() *MockDataOpMockRecorder {
	return m.recorder
}

// CloseStream mocks base method.
func (m *MockDataOp) CloseStream(arg0 uint64) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CloseStream", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// CloseStream indicates an expected call of CloseStream.
func (mr *MockDataOpMockRecorder) CloseStream(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CloseStream", reflect.TypeOf((*MockDataOp)(nil).CloseStream), arg0)
}

// Flush mocks base method.
func (m *MockDataOp) Flush(arg0 uint64) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Flush", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// Flush indicates an expected call of Flush.
func (mr *MockDataOpMockRecorder) Flush(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Flush", reflect.TypeOf((*MockDataOp)(nil).Flush), arg0)
}

// OpenStream mocks base method.
func (m *MockDataOp) OpenStream(arg0 uint64) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "OpenStream", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// OpenStream indicates an expected call of OpenStream.
func (mr *MockDataOpMockRecorder) OpenStream(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "OpenStream", reflect.TypeOf((*MockDataOp)(nil).OpenStream), arg0)
}

// Read mocks base method.
func (m *MockDataOp) Read(arg0 uint64, arg1 []byte, arg2, arg3 int) (int, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Read", arg0, arg1, arg2, arg3)
	ret0, _ := ret[0].(int)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Read indicates an expected call of Read.
func (mr *MockDataOpMockRecorder) Read(arg0, arg1, arg2, arg3 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Read", reflect.TypeOf((*MockDataOp)(nil).Read), arg0, arg1, arg2, arg3)
}

// Write mocks base method.
func (m *MockDataOp) Write(arg0 uint64, arg1 int, arg2 []byte, arg3 int, arg4 func() error) (int, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Write", arg0, arg1, arg2, arg3, arg4)
	ret0, _ := ret[0].(int)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Write indicates an expected call of Write.
func (mr *MockDataOpMockRecorder) Write(arg0, arg1, arg2, arg3, arg4 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Write", reflect.TypeOf((*MockDataOp)(nil).Write), arg0, arg1, arg2, arg3, arg4)
}

// MockMetaOp is a mock of MetaOp interface.
type MockMetaOp struct {
	ctrl     *gomock.Controller
	recorder *MockMetaOpMockRecorder
}

// MockMetaOpMockRecorder is the mock recorder for MockMetaOp.
type MockMetaOpMockRecorder struct {
	mock *MockMetaOp
}

// NewMockMetaOp creates a new mock instance.
func NewMockMetaOp(ctrl *gomock.Controller) *MockMetaOp {
	mock := &MockMetaOp{ctrl: ctrl}
	mock.recorder = &MockMetaOpMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockMetaOp) EXPECT() *MockMetaOpMockRecorder {
	return m.recorder
}

// AddMultipartPart_ll mocks base method.
func (m *MockMetaOp) AddMultipartPart_ll(arg0, arg1 string, arg2 uint16, arg3 uint64, arg4 string, arg5 *proto.InodeInfo) (uint64, bool, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "AddMultipartPart_ll", arg0, arg1, arg2, arg3, arg4, arg5)
	ret0, _ := ret[0].(uint64)
	ret1, _ := ret[1].(bool)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// AddMultipartPart_ll indicates an expected call of AddMultipartPart_ll.
func (mr *MockMetaOpMockRecorder) AddMultipartPart_ll(arg0, arg1, arg2, arg3, arg4, arg5 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AddMultipartPart_ll", reflect.TypeOf((*MockMetaOp)(nil).AddMultipartPart_ll), arg0, arg1, arg2, arg3, arg4, arg5)
}

// AppendExtentKeys mocks base method.
func (m *MockMetaOp) AppendExtentKeys(arg0 uint64, arg1 []proto.ExtentKey) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "AppendExtentKeys", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// AppendExtentKeys indicates an expected call of AppendExtentKeys.
func (mr *MockMetaOpMockRecorder) AppendExtentKeys(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AppendExtentKeys", reflect.TypeOf((*MockMetaOp)(nil).AppendExtentKeys), arg0, arg1)
}

// BatchGetXAttrEx mocks base method.
func (m *MockMetaOp) BatchGetXAttrEx(arg0 []uint64, arg1 []string, arg2 bool) ([]*proto.XAttrInfo, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "BatchGetXAttrEx", arg0, arg1, arg2)
	ret0, _ := ret[0].([]*proto.XAttrInfo)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// BatchGetXAttrEx indicates an expected call of BatchGetXAttrEx.
func (mr *MockMetaOpMockRecorder) BatchGetXAttrEx(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "BatchGetXAttrEx", reflect.TypeOf((*MockMetaOp)(nil).BatchGetXAttrEx), arg0, arg1, arg2)
}

// BatchInodeGetWith mocks base method.
func (m *MockMetaOp) BatchInodeGetWith(arg0 []uint64) ([]*proto.InodeInfo, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "BatchInodeGetWith", arg0)
	ret0, _ := ret[0].([]*proto.InodeInfo)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// BatchInodeGetWith indicates an expected call of BatchInodeGetWith.
func (mr *MockMetaOpMockRecorder) BatchInodeGetWith(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "BatchInodeGetWith", reflect.TypeOf((*MockMetaOp)(nil).BatchInodeGetWith), arg0)
}

// BatchSetXAttr_ll mocks base method.
func (m *MockMetaOp) BatchSetXAttr_ll(arg0 uint64, arg1 map[string]string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "BatchSetXAttr_ll", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// BatchSetXAttr_ll indicates an expected call of BatchSetXAttr_ll.
func (mr *MockMetaOpMockRecorder) BatchSetXAttr_ll(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "BatchSetXAttr_ll", reflect.TypeOf((*MockMetaOp)(nil).BatchSetXAttr_ll), arg0, arg1)
}

// CreateDirSnapshot mocks base method.
func (m *MockMetaOp) CreateDirSnapshot(arg0 *proto.CreateDirSnapShotInfo) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CreateDirSnapshot", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// CreateDirSnapshot indicates an expected call of CreateDirSnapshot.
func (mr *MockMetaOpMockRecorder) CreateDirSnapshot(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CreateDirSnapshot", reflect.TypeOf((*MockMetaOp)(nil).CreateDirSnapshot), arg0)
}

// DeleteDirSnapshot mocks base method.
func (m *MockMetaOp) DeleteDirSnapshot(arg0 *proto.DirVerItem) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DeleteDirSnapshot", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// DeleteDirSnapshot indicates an expected call of DeleteDirSnapshot.
func (mr *MockMetaOpMockRecorder) DeleteDirSnapshot(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DeleteDirSnapshot", reflect.TypeOf((*MockMetaOp)(nil).DeleteDirSnapshot), arg0)
}

// Delete_ll mocks base method.
func (m *MockMetaOp) Delete_ll(arg0 uint64, arg1 string, arg2 bool) (*proto.InodeInfo, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Delete_ll", arg0, arg1, arg2)
	ret0, _ := ret[0].(*proto.InodeInfo)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Delete_ll indicates an expected call of Delete_ll.
func (mr *MockMetaOpMockRecorder) Delete_ll(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Delete_ll", reflect.TypeOf((*MockMetaOp)(nil).Delete_ll), arg0, arg1, arg2)
}

// DentryCreateEx_ll mocks base method.
func (m *MockMetaOp) DentryCreateEx_ll(arg0 *proto.CreateDentryRequest) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DentryCreateEx_ll", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// DentryCreateEx_ll indicates an expected call of DentryCreateEx_ll.
func (mr *MockMetaOpMockRecorder) DentryCreateEx_ll(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DentryCreateEx_ll", reflect.TypeOf((*MockMetaOp)(nil).DentryCreateEx_ll), arg0)
}

// Evict mocks base method.
func (m *MockMetaOp) Evict(arg0 uint64) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Evict", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// Evict indicates an expected call of Evict.
func (mr *MockMetaOpMockRecorder) Evict(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Evict", reflect.TypeOf((*MockMetaOp)(nil).Evict), arg0)
}

// GetExtents mocks base method.
func (m *MockMetaOp) GetExtents(arg0 uint64) (uint64, uint64, []proto.ExtentKey, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetExtents", arg0)
	ret0, _ := ret[0].(uint64)
	ret1, _ := ret[1].(uint64)
	ret2, _ := ret[2].([]proto.ExtentKey)
	ret3, _ := ret[3].(error)
	return ret0, ret1, ret2, ret3
}

// GetExtents indicates an expected call of GetExtents.
func (mr *MockMetaOpMockRecorder) GetExtents(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetExtents", reflect.TypeOf((*MockMetaOp)(nil).GetExtents), arg0)
}

// GetMultipart_ll mocks base method.
func (m *MockMetaOp) GetMultipart_ll(arg0, arg1 string) (*proto.MultipartInfo, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetMultipart_ll", arg0, arg1)
	ret0, _ := ret[0].(*proto.MultipartInfo)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetMultipart_ll indicates an expected call of GetMultipart_ll.
func (mr *MockMetaOpMockRecorder) GetMultipart_ll(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetMultipart_ll", reflect.TypeOf((*MockMetaOp)(nil).GetMultipart_ll), arg0, arg1)
}

// GetVerInfo mocks base method.
func (m *MockMetaOp) GetVerInfo() *proto.DelVer {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetVerInfo")
	ret0, _ := ret[0].(*proto.DelVer)
	return ret0
}

// GetVerInfo indicates an expected call of GetVerInfo.
func (mr *MockMetaOpMockRecorder) GetVerInfo() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetVerInfo", reflect.TypeOf((*MockMetaOp)(nil).GetVerInfo))
}

// InitMultipart_ll mocks base method.
func (m *MockMetaOp) InitMultipart_ll(arg0 string, arg1 map[string]string) (string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "InitMultipart_ll", arg0, arg1)
	ret0, _ := ret[0].(string)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// InitMultipart_ll indicates an expected call of InitMultipart_ll.
func (mr *MockMetaOpMockRecorder) InitMultipart_ll(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "InitMultipart_ll", reflect.TypeOf((*MockMetaOp)(nil).InitMultipart_ll), arg0, arg1)
}

// InodeCreate_ll mocks base method.
func (m *MockMetaOp) InodeCreate_ll(arg0 uint64, arg1, arg2, arg3 uint32, arg4 []byte, arg5 []uint64) (*proto.InodeInfo, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "InodeCreate_ll", arg0, arg1, arg2, arg3, arg4, arg5)
	ret0, _ := ret[0].(*proto.InodeInfo)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// InodeCreate_ll indicates an expected call of InodeCreate_ll.
func (mr *MockMetaOpMockRecorder) InodeCreate_ll(arg0, arg1, arg2, arg3, arg4, arg5 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "InodeCreate_ll", reflect.TypeOf((*MockMetaOp)(nil).InodeCreate_ll), arg0, arg1, arg2, arg3, arg4, arg5)
}

// InodeDelete_ll mocks base method.
func (m *MockMetaOp) InodeDelete_ll(arg0 uint64) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "InodeDelete_ll", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// InodeDelete_ll indicates an expected call of InodeDelete_ll.
func (mr *MockMetaOpMockRecorder) InodeDelete_ll(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "InodeDelete_ll", reflect.TypeOf((*MockMetaOp)(nil).InodeDelete_ll), arg0)
}

// InodeGet_ll mocks base method.
func (m *MockMetaOp) InodeGet_ll(arg0 uint64) (*proto.InodeInfo, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "InodeGet_ll", arg0)
	ret0, _ := ret[0].(*proto.InodeInfo)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// InodeGet_ll indicates an expected call of InodeGet_ll.
func (mr *MockMetaOpMockRecorder) InodeGet_ll(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "InodeGet_ll", reflect.TypeOf((*MockMetaOp)(nil).InodeGet_ll), arg0)
}

// InodeUnlink_ll mocks base method.
func (m *MockMetaOp) InodeUnlink_ll(arg0 uint64) (*proto.InodeInfo, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "InodeUnlink_ll", arg0)
	ret0, _ := ret[0].(*proto.InodeInfo)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// InodeUnlink_ll indicates an expected call of InodeUnlink_ll.
func (mr *MockMetaOpMockRecorder) InodeUnlink_ll(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "InodeUnlink_ll", reflect.TypeOf((*MockMetaOp)(nil).InodeUnlink_ll), arg0)
}

// ListAllDirSnapshot mocks base method.
func (m *MockMetaOp) ListAllDirSnapshot(arg0 uint64) ([]*proto.DirSnapshotInfo, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ListAllDirSnapshot", arg0)
	ret0, _ := ret[0].([]*proto.DirSnapshotInfo)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ListAllDirSnapshot indicates an expected call of ListAllDirSnapshot.
func (mr *MockMetaOpMockRecorder) ListAllDirSnapshot(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListAllDirSnapshot", reflect.TypeOf((*MockMetaOp)(nil).ListAllDirSnapshot), arg0)
}

// LookupEx_ll mocks base method.
func (m *MockMetaOp) LookupEx_ll(arg0 uint64, arg1 string) (*proto.Dentry, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "LookupEx_ll", arg0, arg1)
	ret0, _ := ret[0].(*proto.Dentry)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// LookupEx_ll indicates an expected call of LookupEx_ll.
func (mr *MockMetaOpMockRecorder) LookupEx_ll(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "LookupEx_ll", reflect.TypeOf((*MockMetaOp)(nil).LookupEx_ll), arg0, arg1)
}

// LookupPath mocks base method.
func (m *MockMetaOp) LookupPath(arg0 string) (uint64, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "LookupPath", arg0)
	ret0, _ := ret[0].(uint64)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// LookupPath indicates an expected call of LookupPath.
func (mr *MockMetaOpMockRecorder) LookupPath(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "LookupPath", reflect.TypeOf((*MockMetaOp)(nil).LookupPath), arg0)
}

// ReadDirLimit_ll mocks base method.
func (m *MockMetaOp) ReadDirLimit_ll(arg0 uint64, arg1 string, arg2 uint64) ([]proto.Dentry, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ReadDirLimit_ll", arg0, arg1, arg2)
	ret0, _ := ret[0].([]proto.Dentry)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ReadDirLimit_ll indicates an expected call of ReadDirLimit_ll.
func (mr *MockMetaOpMockRecorder) ReadDirLimit_ll(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ReadDirLimit_ll", reflect.TypeOf((*MockMetaOp)(nil).ReadDirLimit_ll), arg0, arg1, arg2)
}

// ReadDir_ll mocks base method.
func (m *MockMetaOp) ReadDir_ll(arg0 uint64) ([]proto.Dentry, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ReadDir_ll", arg0)
	ret0, _ := ret[0].([]proto.Dentry)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ReadDir_ll indicates an expected call of ReadDir_ll.
func (mr *MockMetaOpMockRecorder) ReadDir_ll(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ReadDir_ll", reflect.TypeOf((*MockMetaOp)(nil).ReadDir_ll), arg0)
}

// RemoveMultipart_ll mocks base method.
func (m *MockMetaOp) RemoveMultipart_ll(arg0, arg1 string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RemoveMultipart_ll", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// RemoveMultipart_ll indicates an expected call of RemoveMultipart_ll.
func (mr *MockMetaOpMockRecorder) RemoveMultipart_ll(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RemoveMultipart_ll", reflect.TypeOf((*MockMetaOp)(nil).RemoveMultipart_ll), arg0, arg1)
}

// Rename_ll mocks base method.
func (m *MockMetaOp) Rename_ll(arg0 uint64, arg1 string, arg2 uint64, arg3 string, arg4 bool) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Rename_ll", arg0, arg1, arg2, arg3, arg4)
	ret0, _ := ret[0].(error)
	return ret0
}

// Rename_ll indicates an expected call of Rename_ll.
func (mr *MockMetaOpMockRecorder) Rename_ll(arg0, arg1, arg2, arg3, arg4 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Rename_ll", reflect.TypeOf((*MockMetaOp)(nil).Rename_ll), arg0, arg1, arg2, arg3, arg4)
}

// SetInodeLock_ll mocks base method.
func (m *MockMetaOp) SetInodeLock_ll(arg0 uint64, arg1 *proto.InodeLockReq) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SetInodeLock_ll", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// SetInodeLock_ll indicates an expected call of SetInodeLock_ll.
func (mr *MockMetaOpMockRecorder) SetInodeLock_ll(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetInodeLock_ll", reflect.TypeOf((*MockMetaOp)(nil).SetInodeLock_ll), arg0, arg1)
}

// SetRenameVerInfo mocks base method.
func (m *MockMetaOp) SetRenameVerInfo(arg0, arg1 *proto.DelVer) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "SetRenameVerInfo", arg0, arg1)
}

// SetRenameVerInfo indicates an expected call of SetRenameVerInfo.
func (mr *MockMetaOpMockRecorder) SetRenameVerInfo(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetRenameVerInfo", reflect.TypeOf((*MockMetaOp)(nil).SetRenameVerInfo), arg0, arg1)
}

// SetVerInfoEx mocks base method.
func (m *MockMetaOp) SetVerInfoEx(arg0 *proto.DelVer, arg1 uint64) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "SetVerInfoEx", arg0, arg1)
}

// SetVerInfoEx indicates an expected call of SetVerInfoEx.
func (mr *MockMetaOpMockRecorder) SetVerInfoEx(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetVerInfoEx", reflect.TypeOf((*MockMetaOp)(nil).SetVerInfoEx), arg0, arg1)
}

// Setattr mocks base method.
func (m *MockMetaOp) Setattr(arg0 uint64, arg1, arg2, arg3, arg4 uint32, arg5, arg6 int64) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Setattr", arg0, arg1, arg2, arg3, arg4, arg5, arg6)
	ret0, _ := ret[0].(error)
	return ret0
}

// Setattr indicates an expected call of Setattr.
func (mr *MockMetaOpMockRecorder) Setattr(arg0, arg1, arg2, arg3, arg4, arg5, arg6 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Setattr", reflect.TypeOf((*MockMetaOp)(nil).Setattr), arg0, arg1, arg2, arg3, arg4, arg5, arg6)
}

// Truncate mocks base method.
func (m *MockMetaOp) Truncate(arg0, arg1 uint64) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Truncate", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// Truncate indicates an expected call of Truncate.
func (mr *MockMetaOpMockRecorder) Truncate(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Truncate", reflect.TypeOf((*MockMetaOp)(nil).Truncate), arg0, arg1)
}

// XAttrDel_ll mocks base method.
func (m *MockMetaOp) XAttrDel_ll(arg0 uint64, arg1 string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "XAttrDel_ll", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// XAttrDel_ll indicates an expected call of XAttrDel_ll.
func (mr *MockMetaOpMockRecorder) XAttrDel_ll(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "XAttrDel_ll", reflect.TypeOf((*MockMetaOp)(nil).XAttrDel_ll), arg0, arg1)
}

// XAttrGetAll_ll mocks base method.
func (m *MockMetaOp) XAttrGetAll_ll(arg0 uint64) (*proto.XAttrInfo, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "XAttrGetAll_ll", arg0)
	ret0, _ := ret[0].(*proto.XAttrInfo)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// XAttrGetAll_ll indicates an expected call of XAttrGetAll_ll.
func (mr *MockMetaOpMockRecorder) XAttrGetAll_ll(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "XAttrGetAll_ll", reflect.TypeOf((*MockMetaOp)(nil).XAttrGetAll_ll), arg0)
}

// XAttrGet_ll mocks base method.
func (m *MockMetaOp) XAttrGet_ll(arg0 uint64, arg1 string) (*proto.XAttrInfo, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "XAttrGet_ll", arg0, arg1)
	ret0, _ := ret[0].(*proto.XAttrInfo)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// XAttrGet_ll indicates an expected call of XAttrGet_ll.
func (mr *MockMetaOpMockRecorder) XAttrGet_ll(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "XAttrGet_ll", reflect.TypeOf((*MockMetaOp)(nil).XAttrGet_ll), arg0, arg1)
}

// XAttrSetEx_ll mocks base method.
func (m *MockMetaOp) XAttrSetEx_ll(arg0 uint64, arg1, arg2 []byte, arg3 bool) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "XAttrSetEx_ll", arg0, arg1, arg2, arg3)
	ret0, _ := ret[0].(error)
	return ret0
}

// XAttrSetEx_ll indicates an expected call of XAttrSetEx_ll.
func (mr *MockMetaOpMockRecorder) XAttrSetEx_ll(arg0, arg1, arg2, arg3 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "XAttrSetEx_ll", reflect.TypeOf((*MockMetaOp)(nil).XAttrSetEx_ll), arg0, arg1, arg2, arg3)
}

// XAttrsList_ll mocks base method.
func (m *MockMetaOp) XAttrsList_ll(arg0 uint64) ([]string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "XAttrsList_ll", arg0)
	ret0, _ := ret[0].([]string)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// XAttrsList_ll indicates an expected call of XAttrsList_ll.
func (mr *MockMetaOpMockRecorder) XAttrsList_ll(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "XAttrsList_ll", reflect.TypeOf((*MockMetaOp)(nil).XAttrsList_ll), arg0)
}

// XBatchDelAttr_ll mocks base method.
func (m *MockMetaOp) XBatchDelAttr_ll(arg0 uint64, arg1 []string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "XBatchDelAttr_ll", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// XBatchDelAttr_ll indicates an expected call of XBatchDelAttr_ll.
func (mr *MockMetaOpMockRecorder) XBatchDelAttr_ll(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "XBatchDelAttr_ll", reflect.TypeOf((*MockMetaOp)(nil).XBatchDelAttr_ll), arg0, arg1)
}

// MockMaster is a mock of IMaster interface.
type MockMaster struct {
	ctrl     *gomock.Controller
	recorder *MockMasterMockRecorder
}

// MockMasterMockRecorder is the mock recorder for MockMaster.
type MockMasterMockRecorder struct {
	mock *MockMaster
}

// NewMockMaster creates a new mock instance.
func NewMockMaster(ctrl *gomock.Controller) *MockMaster {
	mock := &MockMaster{ctrl: ctrl}
	mock.recorder = &MockMasterMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockMaster) EXPECT() *MockMasterMockRecorder {
	return m.recorder
}

// AllocDirSnapshotVersion mocks base method.
func (m *MockMaster) AllocDirSnapshotVersion(arg0 string) (*proto.DirSnapshotVersionInfo, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "AllocDirSnapshotVersion", arg0)
	ret0, _ := ret[0].(*proto.DirSnapshotVersionInfo)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// AllocDirSnapshotVersion indicates an expected call of AllocDirSnapshotVersion.
func (mr *MockMasterMockRecorder) AllocDirSnapshotVersion(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AllocDirSnapshotVersion", reflect.TypeOf((*MockMaster)(nil).AllocDirSnapshotVersion), arg0)
}

// AllocFileId mocks base method.
func (m *MockMaster) AllocFileId() (*proto.FileId, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "AllocFileId")
	ret0, _ := ret[0].(*proto.FileId)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// AllocFileId indicates an expected call of AllocFileId.
func (mr *MockMasterMockRecorder) AllocFileId() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AllocFileId", reflect.TypeOf((*MockMaster)(nil).AllocFileId))
}

// GetClusterIP mocks base method.
func (m *MockMaster) GetClusterIP() (*proto.ClusterIP, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetClusterIP")
	ret0, _ := ret[0].(*proto.ClusterIP)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetClusterIP indicates an expected call of GetClusterIP.
func (mr *MockMasterMockRecorder) GetClusterIP() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetClusterIP", reflect.TypeOf((*MockMaster)(nil).GetClusterIP))
}

// ListVols mocks base method.
func (m *MockMaster) ListVols(arg0 string) ([]*proto.VolInfo, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ListVols", arg0)
	ret0, _ := ret[0].([]*proto.VolInfo)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ListVols indicates an expected call of ListVols.
func (mr *MockMasterMockRecorder) ListVols(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListVols", reflect.TypeOf((*MockMaster)(nil).ListVols), arg0)
}