// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/cubefs/cubefs/apinode/sdk (interfaces: IVolume,ICluster,ClusterManager)

// Package mocks is a generated GoMock package.
package mocks

import (
	context "context"
	io "io"
	reflect "reflect"

	sdk "github.com/cubefs/cubefs/apinode/sdk"
	proto "github.com/cubefs/cubefs/proto"
	gomock "github.com/golang/mock/gomock"
)

// MockIVolume is a mock of IVolume interface.
type MockIVolume struct {
	ctrl     *gomock.Controller
	recorder *MockIVolumeMockRecorder
}

// MockIVolumeMockRecorder is the mock recorder for MockIVolume.
type MockIVolumeMockRecorder struct {
	mock *MockIVolume
}

// NewMockIVolume creates a new mock instance.
func NewMockIVolume(ctrl *gomock.Controller) *MockIVolume {
	mock := &MockIVolume{ctrl: ctrl}
	mock.recorder = &MockIVolumeMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockIVolume) EXPECT() *MockIVolumeMockRecorder {
	return m.recorder
}

// AbortMultiPart mocks base method.
func (m *MockIVolume) AbortMultiPart(arg0 context.Context, arg1, arg2 string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "AbortMultiPart", arg0, arg1, arg2)
	ret0, _ := ret[0].(error)
	return ret0
}

// AbortMultiPart indicates an expected call of AbortMultiPart.
func (mr *MockIVolumeMockRecorder) AbortMultiPart(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AbortMultiPart", reflect.TypeOf((*MockIVolume)(nil).AbortMultiPart), arg0, arg1, arg2)
}

// BatchDeleteXAttr mocks base method.
func (m *MockIVolume) BatchDeleteXAttr(arg0 context.Context, arg1 uint64, arg2 []string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "BatchDeleteXAttr", arg0, arg1, arg2)
	ret0, _ := ret[0].(error)
	return ret0
}

// BatchDeleteXAttr indicates an expected call of BatchDeleteXAttr.
func (mr *MockIVolumeMockRecorder) BatchDeleteXAttr(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "BatchDeleteXAttr", reflect.TypeOf((*MockIVolume)(nil).BatchDeleteXAttr), arg0, arg1, arg2)
}

// BatchGetInodes mocks base method.
func (m *MockIVolume) BatchGetInodes(arg0 context.Context, arg1 []uint64) ([]*proto.InodeInfo, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "BatchGetInodes", arg0, arg1)
	ret0, _ := ret[0].([]*proto.InodeInfo)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// BatchGetInodes indicates an expected call of BatchGetInodes.
func (mr *MockIVolumeMockRecorder) BatchGetInodes(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "BatchGetInodes", reflect.TypeOf((*MockIVolume)(nil).BatchGetInodes), arg0, arg1)
}

// BatchGetXAttr mocks base method.
func (m *MockIVolume) BatchGetXAttr(arg0 context.Context, arg1 []uint64) ([]*proto.XAttrInfo, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "BatchGetXAttr", arg0, arg1)
	ret0, _ := ret[0].([]*proto.XAttrInfo)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// BatchGetXAttr indicates an expected call of BatchGetXAttr.
func (mr *MockIVolumeMockRecorder) BatchGetXAttr(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "BatchGetXAttr", reflect.TypeOf((*MockIVolume)(nil).BatchGetXAttr), arg0, arg1)
}

// BatchSetXAttr mocks base method.
func (m *MockIVolume) BatchSetXAttr(arg0 context.Context, arg1 uint64, arg2 map[string]string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "BatchSetXAttr", arg0, arg1, arg2)
	ret0, _ := ret[0].(error)
	return ret0
}

// BatchSetXAttr indicates an expected call of BatchSetXAttr.
func (mr *MockIVolumeMockRecorder) BatchSetXAttr(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "BatchSetXAttr", reflect.TypeOf((*MockIVolume)(nil).BatchSetXAttr), arg0, arg1, arg2)
}

// CompleteMultiPart mocks base method.
func (m *MockIVolume) CompleteMultiPart(arg0 context.Context, arg1 *sdk.CompleteMultipartReq) (*proto.InodeInfo, uint64, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CompleteMultiPart", arg0, arg1)
	ret0, _ := ret[0].(*proto.InodeInfo)
	ret1, _ := ret[1].(uint64)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// CompleteMultiPart indicates an expected call of CompleteMultiPart.
func (mr *MockIVolumeMockRecorder) CompleteMultiPart(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CompleteMultiPart", reflect.TypeOf((*MockIVolume)(nil).CompleteMultiPart), arg0, arg1)
}

// CreateDirSnapshot mocks base method.
func (m *MockIVolume) CreateDirSnapshot(arg0 context.Context, arg1, arg2 string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CreateDirSnapshot", arg0, arg1, arg2)
	ret0, _ := ret[0].(error)
	return ret0
}

// CreateDirSnapshot indicates an expected call of CreateDirSnapshot.
func (mr *MockIVolumeMockRecorder) CreateDirSnapshot(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CreateDirSnapshot", reflect.TypeOf((*MockIVolume)(nil).CreateDirSnapshot), arg0, arg1, arg2)
}

// CreateFile mocks base method.
func (m *MockIVolume) CreateFile(arg0 context.Context, arg1 uint64, arg2 string) (*proto.InodeInfo, uint64, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CreateFile", arg0, arg1, arg2)
	ret0, _ := ret[0].(*proto.InodeInfo)
	ret1, _ := ret[1].(uint64)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// CreateFile indicates an expected call of CreateFile.
func (mr *MockIVolumeMockRecorder) CreateFile(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CreateFile", reflect.TypeOf((*MockIVolume)(nil).CreateFile), arg0, arg1, arg2)
}

// Delete mocks base method.
func (m *MockIVolume) Delete(arg0 context.Context, arg1 uint64, arg2 string, arg3 bool) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Delete", arg0, arg1, arg2, arg3)
	ret0, _ := ret[0].(error)
	return ret0
}

// Delete indicates an expected call of Delete.
func (mr *MockIVolumeMockRecorder) Delete(arg0, arg1, arg2, arg3 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Delete", reflect.TypeOf((*MockIVolume)(nil).Delete), arg0, arg1, arg2, arg3)
}

// DeleteDirSnapshot mocks base method.
func (m *MockIVolume) DeleteDirSnapshot(arg0 context.Context, arg1, arg2 string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DeleteDirSnapshot", arg0, arg1, arg2)
	ret0, _ := ret[0].(error)
	return ret0
}

// DeleteDirSnapshot indicates an expected call of DeleteDirSnapshot.
func (mr *MockIVolumeMockRecorder) DeleteDirSnapshot(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DeleteDirSnapshot", reflect.TypeOf((*MockIVolume)(nil).DeleteDirSnapshot), arg0, arg1, arg2)
}

// DeleteXAttr mocks base method.
func (m *MockIVolume) DeleteXAttr(arg0 context.Context, arg1 uint64, arg2 string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DeleteXAttr", arg0, arg1, arg2)
	ret0, _ := ret[0].(error)
	return ret0
}

// DeleteXAttr indicates an expected call of DeleteXAttr.
func (mr *MockIVolumeMockRecorder) DeleteXAttr(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DeleteXAttr", reflect.TypeOf((*MockIVolume)(nil).DeleteXAttr), arg0, arg1, arg2)
}

// GetDirSnapshot mocks base method.
func (m *MockIVolume) GetDirSnapshot(arg0 context.Context, arg1 uint64) (sdk.IDirSnapshot, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetDirSnapshot", arg0, arg1)
	ret0, _ := ret[0].(sdk.IDirSnapshot)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetDirSnapshot indicates an expected call of GetDirSnapshot.
func (mr *MockIVolumeMockRecorder) GetDirSnapshot(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetDirSnapshot", reflect.TypeOf((*MockIVolume)(nil).GetDirSnapshot), arg0, arg1)
}

// GetInode mocks base method.
func (m *MockIVolume) GetInode(arg0 context.Context, arg1 uint64) (*proto.InodeInfo, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetInode", arg0, arg1)
	ret0, _ := ret[0].(*proto.InodeInfo)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetInode indicates an expected call of GetInode.
func (mr *MockIVolumeMockRecorder) GetInode(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetInode", reflect.TypeOf((*MockIVolume)(nil).GetInode), arg0, arg1)
}

// GetXAttr mocks base method.
func (m *MockIVolume) GetXAttr(arg0 context.Context, arg1 uint64, arg2 string) (string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetXAttr", arg0, arg1, arg2)
	ret0, _ := ret[0].(string)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetXAttr indicates an expected call of GetXAttr.
func (mr *MockIVolumeMockRecorder) GetXAttr(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetXAttr", reflect.TypeOf((*MockIVolume)(nil).GetXAttr), arg0, arg1, arg2)
}

// GetXAttrMap mocks base method.
func (m *MockIVolume) GetXAttrMap(arg0 context.Context, arg1 uint64) (map[string]string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetXAttrMap", arg0, arg1)
	ret0, _ := ret[0].(map[string]string)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetXAttrMap indicates an expected call of GetXAttrMap.
func (mr *MockIVolumeMockRecorder) GetXAttrMap(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetXAttrMap", reflect.TypeOf((*MockIVolume)(nil).GetXAttrMap), arg0, arg1)
}

// Info mocks base method.
func (m *MockIVolume) Info() *sdk.VolInfo {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Info")
	ret0, _ := ret[0].(*sdk.VolInfo)
	return ret0
}

// Info indicates an expected call of Info.
func (mr *MockIVolumeMockRecorder) Info() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Info", reflect.TypeOf((*MockIVolume)(nil).Info))
}

// InitMultiPart mocks base method.
func (m *MockIVolume) InitMultiPart(arg0 context.Context, arg1 string, arg2 map[string]string) (string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "InitMultiPart", arg0, arg1, arg2)
	ret0, _ := ret[0].(string)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// InitMultiPart indicates an expected call of InitMultiPart.
func (mr *MockIVolumeMockRecorder) InitMultiPart(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "InitMultiPart", reflect.TypeOf((*MockIVolume)(nil).InitMultiPart), arg0, arg1, arg2)
}

// IsSnapshotInode mocks base method.
func (m *MockIVolume) IsSnapshotInode(arg0 context.Context, arg1 uint64) bool {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "IsSnapshotInode", arg0, arg1)
	ret0, _ := ret[0].(bool)
	return ret0
}

// IsSnapshotInode indicates an expected call of IsSnapshotInode.
func (mr *MockIVolumeMockRecorder) IsSnapshotInode(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "IsSnapshotInode", reflect.TypeOf((*MockIVolume)(nil).IsSnapshotInode), arg0, arg1)
}

// ListMultiPart mocks base method.
func (m *MockIVolume) ListMultiPart(arg0 context.Context, arg1, arg2 string, arg3, arg4 uint64) ([]*proto.MultipartPartInfo, uint64, bool, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ListMultiPart", arg0, arg1, arg2, arg3, arg4)
	ret0, _ := ret[0].([]*proto.MultipartPartInfo)
	ret1, _ := ret[1].(uint64)
	ret2, _ := ret[2].(bool)
	ret3, _ := ret[3].(error)
	return ret0, ret1, ret2, ret3
}

// ListMultiPart indicates an expected call of ListMultiPart.
func (mr *MockIVolumeMockRecorder) ListMultiPart(arg0, arg1, arg2, arg3, arg4 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListMultiPart", reflect.TypeOf((*MockIVolume)(nil).ListMultiPart), arg0, arg1, arg2, arg3, arg4)
}

// ListXAttr mocks base method.
func (m *MockIVolume) ListXAttr(arg0 context.Context, arg1 uint64) ([]string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ListXAttr", arg0, arg1)
	ret0, _ := ret[0].([]string)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ListXAttr indicates an expected call of ListXAttr.
func (mr *MockIVolumeMockRecorder) ListXAttr(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListXAttr", reflect.TypeOf((*MockIVolume)(nil).ListXAttr), arg0, arg1)
}

// Lookup mocks base method.
func (m *MockIVolume) Lookup(arg0 context.Context, arg1 uint64, arg2 string) (*proto.Dentry, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Lookup", arg0, arg1, arg2)
	ret0, _ := ret[0].(*proto.Dentry)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Lookup indicates an expected call of Lookup.
func (mr *MockIVolumeMockRecorder) Lookup(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Lookup", reflect.TypeOf((*MockIVolume)(nil).Lookup), arg0, arg1, arg2)
}

// Mkdir mocks base method.
func (m *MockIVolume) Mkdir(arg0 context.Context, arg1 uint64, arg2 string) (*proto.InodeInfo, uint64, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Mkdir", arg0, arg1, arg2)
	ret0, _ := ret[0].(*proto.InodeInfo)
	ret1, _ := ret[1].(uint64)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// Mkdir indicates an expected call of Mkdir.
func (mr *MockIVolumeMockRecorder) Mkdir(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Mkdir", reflect.TypeOf((*MockIVolume)(nil).Mkdir), arg0, arg1, arg2)
}

// NewInodeLock mocks base method.
func (m *MockIVolume) NewInodeLock() sdk.InodeLockApi {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "NewInodeLock")
	ret0, _ := ret[0].(sdk.InodeLockApi)
	return ret0
}

// NewInodeLock indicates an expected call of NewInodeLock.
func (mr *MockIVolumeMockRecorder) NewInodeLock() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "NewInodeLock", reflect.TypeOf((*MockIVolume)(nil).NewInodeLock))
}

// ReadDirAll mocks base method.
func (m *MockIVolume) ReadDirAll(arg0 context.Context, arg1 uint64, arg2 string) ([]proto.Dentry, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ReadDirAll", arg0, arg1, arg2)
	ret0, _ := ret[0].([]proto.Dentry)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ReadDirAll indicates an expected call of ReadDirAll.
func (mr *MockIVolumeMockRecorder) ReadDirAll(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ReadDirAll", reflect.TypeOf((*MockIVolume)(nil).ReadDirAll), arg0, arg1, arg2)
}

// ReadFile mocks base method.
func (m *MockIVolume) ReadFile(arg0 context.Context, arg1, arg2 uint64, arg3 []byte) (int, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ReadFile", arg0, arg1, arg2, arg3)
	ret0, _ := ret[0].(int)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ReadFile indicates an expected call of ReadFile.
func (mr *MockIVolumeMockRecorder) ReadFile(arg0, arg1, arg2, arg3 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ReadFile", reflect.TypeOf((*MockIVolume)(nil).ReadFile), arg0, arg1, arg2, arg3)
}

// Readdir mocks base method.
func (m *MockIVolume) Readdir(arg0 context.Context, arg1 uint64, arg2 string, arg3 uint32) ([]proto.Dentry, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Readdir", arg0, arg1, arg2, arg3)
	ret0, _ := ret[0].([]proto.Dentry)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Readdir indicates an expected call of Readdir.
func (mr *MockIVolumeMockRecorder) Readdir(arg0, arg1, arg2, arg3 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Readdir", reflect.TypeOf((*MockIVolume)(nil).Readdir), arg0, arg1, arg2, arg3)
}

// Rename mocks base method.
func (m *MockIVolume) Rename(arg0 context.Context, arg1, arg2 string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Rename", arg0, arg1, arg2)
	ret0, _ := ret[0].(error)
	return ret0
}

// Rename indicates an expected call of Rename.
func (mr *MockIVolumeMockRecorder) Rename(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Rename", reflect.TypeOf((*MockIVolume)(nil).Rename), arg0, arg1, arg2)
}

// SetAttr mocks base method.
func (m *MockIVolume) SetAttr(arg0 context.Context, arg1 *sdk.SetAttrReq) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SetAttr", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// SetAttr indicates an expected call of SetAttr.
func (mr *MockIVolumeMockRecorder) SetAttr(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetAttr", reflect.TypeOf((*MockIVolume)(nil).SetAttr), arg0, arg1)
}

// SetXAttr mocks base method.
func (m *MockIVolume) SetXAttr(arg0 context.Context, arg1 uint64, arg2, arg3 string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SetXAttr", arg0, arg1, arg2, arg3)
	ret0, _ := ret[0].(error)
	return ret0
}

// SetXAttr indicates an expected call of SetXAttr.
func (mr *MockIVolumeMockRecorder) SetXAttr(arg0, arg1, arg2, arg3 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetXAttr", reflect.TypeOf((*MockIVolume)(nil).SetXAttr), arg0, arg1, arg2, arg3)
}

// SetXAttrNX mocks base method.
func (m *MockIVolume) SetXAttrNX(arg0 context.Context, arg1 uint64, arg2, arg3 string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SetXAttrNX", arg0, arg1, arg2, arg3)
	ret0, _ := ret[0].(error)
	return ret0
}

// SetXAttrNX indicates an expected call of SetXAttrNX.
func (mr *MockIVolumeMockRecorder) SetXAttrNX(arg0, arg1, arg2, arg3 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetXAttrNX", reflect.TypeOf((*MockIVolume)(nil).SetXAttrNX), arg0, arg1, arg2, arg3)
}

// StatFs mocks base method.
func (m *MockIVolume) StatFs(arg0 context.Context, arg1 uint64) (*sdk.StatFs, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "StatFs", arg0, arg1)
	ret0, _ := ret[0].(*sdk.StatFs)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// StatFs indicates an expected call of StatFs.
func (mr *MockIVolumeMockRecorder) StatFs(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "StatFs", reflect.TypeOf((*MockIVolume)(nil).StatFs), arg0, arg1)
}

// UploadFile mocks base method.
func (m *MockIVolume) UploadFile(arg0 context.Context, arg1 *sdk.UploadFileReq) (*proto.InodeInfo, uint64, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UploadFile", arg0, arg1)
	ret0, _ := ret[0].(*proto.InodeInfo)
	ret1, _ := ret[1].(uint64)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// UploadFile indicates an expected call of UploadFile.
func (mr *MockIVolumeMockRecorder) UploadFile(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UploadFile", reflect.TypeOf((*MockIVolume)(nil).UploadFile), arg0, arg1)
}

// UploadMultiPart mocks base method.
func (m *MockIVolume) UploadMultiPart(arg0 context.Context, arg1, arg2 string, arg3 uint16, arg4 io.Reader) (*proto.MultipartPartInfo, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UploadMultiPart", arg0, arg1, arg2, arg3, arg4)
	ret0, _ := ret[0].(*proto.MultipartPartInfo)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// UploadMultiPart indicates an expected call of UploadMultiPart.
func (mr *MockIVolumeMockRecorder) UploadMultiPart(arg0, arg1, arg2, arg3, arg4 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UploadMultiPart", reflect.TypeOf((*MockIVolume)(nil).UploadMultiPart), arg0, arg1, arg2, arg3, arg4)
}

// WriteFile mocks base method.
func (m *MockIVolume) WriteFile(arg0 context.Context, arg1, arg2, arg3 uint64, arg4 io.Reader) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "WriteFile", arg0, arg1, arg2, arg3, arg4)
	ret0, _ := ret[0].(error)
	return ret0
}

// WriteFile indicates an expected call of WriteFile.
func (mr *MockIVolumeMockRecorder) WriteFile(arg0, arg1, arg2, arg3, arg4 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "WriteFile", reflect.TypeOf((*MockIVolume)(nil).WriteFile), arg0, arg1, arg2, arg3, arg4)
}

// MockICluster is a mock of ICluster interface.
type MockICluster struct {
	ctrl     *gomock.Controller
	recorder *MockIClusterMockRecorder
}

// MockIClusterMockRecorder is the mock recorder for MockICluster.
type MockIClusterMockRecorder struct {
	mock *MockICluster
}

// NewMockICluster creates a new mock instance.
func NewMockICluster(ctrl *gomock.Controller) *MockICluster {
	mock := &MockICluster{ctrl: ctrl}
	mock.recorder = &MockIClusterMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockICluster) EXPECT() *MockIClusterMockRecorder {
	return m.recorder
}

// Addr mocks base method.
func (m *MockICluster) Addr() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Addr")
	ret0, _ := ret[0].(string)
	return ret0
}

// Addr indicates an expected call of Addr.
func (mr *MockIClusterMockRecorder) Addr() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Addr", reflect.TypeOf((*MockICluster)(nil).Addr))
}

// GetVol mocks base method.
func (m *MockICluster) GetVol(arg0 string) sdk.IVolume {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetVol", arg0)
	ret0, _ := ret[0].(sdk.IVolume)
	return ret0
}

// GetVol indicates an expected call of GetVol.
func (mr *MockIClusterMockRecorder) GetVol(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetVol", reflect.TypeOf((*MockICluster)(nil).GetVol), arg0)
}

// Info mocks base method.
func (m *MockICluster) Info() *sdk.ClusterInfo {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Info")
	ret0, _ := ret[0].(*sdk.ClusterInfo)
	return ret0
}

// Info indicates an expected call of Info.
func (mr *MockIClusterMockRecorder) Info() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Info", reflect.TypeOf((*MockICluster)(nil).Info))
}

// ListVols mocks base method.
func (m *MockICluster) ListVols() []*sdk.VolInfo {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ListVols")
	ret0, _ := ret[0].([]*sdk.VolInfo)
	return ret0
}

// ListVols indicates an expected call of ListVols.
func (mr *MockIClusterMockRecorder) ListVols() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListVols", reflect.TypeOf((*MockICluster)(nil).ListVols))
}

// UpdateAddr mocks base method.
func (m *MockICluster) UpdateAddr(arg0 context.Context, arg1 string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UpdateAddr", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// UpdateAddr indicates an expected call of UpdateAddr.
func (mr *MockIClusterMockRecorder) UpdateAddr(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UpdateAddr", reflect.TypeOf((*MockICluster)(nil).UpdateAddr), arg0, arg1)
}

// MockClusterManager is a mock of ClusterManager interface.
type MockClusterManager struct {
	ctrl     *gomock.Controller
	recorder *MockClusterManagerMockRecorder
}

// MockClusterManagerMockRecorder is the mock recorder for MockClusterManager.
type MockClusterManagerMockRecorder struct {
	mock *MockClusterManager
}

// NewMockClusterManager creates a new mock instance.
func NewMockClusterManager(ctrl *gomock.Controller) *MockClusterManager {
	mock := &MockClusterManager{ctrl: ctrl}
	mock.recorder = &MockClusterManagerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockClusterManager) EXPECT() *MockClusterManagerMockRecorder {
	return m.recorder
}

// AddCluster mocks base method.
func (m *MockClusterManager) AddCluster(arg0 context.Context, arg1, arg2 string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "AddCluster", arg0, arg1, arg2)
	ret0, _ := ret[0].(error)
	return ret0
}

// AddCluster indicates an expected call of AddCluster.
func (mr *MockClusterManagerMockRecorder) AddCluster(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AddCluster", reflect.TypeOf((*MockClusterManager)(nil).AddCluster), arg0, arg1, arg2)
}

// GetCluster mocks base method.
func (m *MockClusterManager) GetCluster(arg0 string) sdk.ICluster {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetCluster", arg0)
	ret0, _ := ret[0].(sdk.ICluster)
	return ret0
}

// GetCluster indicates an expected call of GetCluster.
func (mr *MockClusterManagerMockRecorder) GetCluster(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetCluster", reflect.TypeOf((*MockClusterManager)(nil).GetCluster), arg0)
}

// ListCluster mocks base method.
func (m *MockClusterManager) ListCluster() []*sdk.ClusterInfo {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ListCluster")
	ret0, _ := ret[0].([]*sdk.ClusterInfo)
	return ret0
}

// ListCluster indicates an expected call of ListCluster.
func (mr *MockClusterManagerMockRecorder) ListCluster() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListCluster", reflect.TypeOf((*MockClusterManager)(nil).ListCluster))
}
