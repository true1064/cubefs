// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/cubefs/cubefs/apinode/crypto (interfaces: TransCipher,FileCipher)

// Package mocks is a generated GoMock package.
package mocks

import (
	io "io"
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
)

// MockTransCipher is a mock of TransCipher interface.
type MockTransCipher struct {
	ctrl     *gomock.Controller
	recorder *MockTransCipherMockRecorder
}

// MockTransCipherMockRecorder is the mock recorder for MockTransCipher.
type MockTransCipherMockRecorder struct {
	mock *MockTransCipher
}

// NewMockTransCipher creates a new mock instance.
func NewMockTransCipher(ctrl *gomock.Controller) *MockTransCipher {
	mock := &MockTransCipher{ctrl: ctrl}
	mock.recorder = &MockTransCipherMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockTransCipher) EXPECT() *MockTransCipherMockRecorder {
	return m.recorder
}

// Decrypt mocks base method.
func (m *MockTransCipher) Decrypt(arg0 []byte) ([]byte, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Decrypt", arg0)
	ret0, _ := ret[0].([]byte)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Decrypt indicates an expected call of Decrypt.
func (mr *MockTransCipherMockRecorder) Decrypt(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Decrypt", reflect.TypeOf((*MockTransCipher)(nil).Decrypt), arg0)
}

// Decryptor mocks base method.
func (m *MockTransCipher) Decryptor(arg0 io.Reader) io.Reader {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Decryptor", arg0)
	ret0, _ := ret[0].(io.Reader)
	return ret0
}

// Decryptor indicates an expected call of Decryptor.
func (mr *MockTransCipherMockRecorder) Decryptor(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Decryptor", reflect.TypeOf((*MockTransCipher)(nil).Decryptor), arg0)
}

// Encrypt mocks base method.
func (m *MockTransCipher) Encrypt(arg0 []byte) ([]byte, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Encrypt", arg0)
	ret0, _ := ret[0].([]byte)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Encrypt indicates an expected call of Encrypt.
func (mr *MockTransCipherMockRecorder) Encrypt(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Encrypt", reflect.TypeOf((*MockTransCipher)(nil).Encrypt), arg0)
}

// Encryptor mocks base method.
func (m *MockTransCipher) Encryptor(arg0 io.Reader) io.Reader {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Encryptor", arg0)
	ret0, _ := ret[0].(io.Reader)
	return ret0
}

// Encryptor indicates an expected call of Encryptor.
func (mr *MockTransCipherMockRecorder) Encryptor(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Encryptor", reflect.TypeOf((*MockTransCipher)(nil).Encryptor), arg0)
}

// MockFileCipher is a mock of FileCipher interface.
type MockFileCipher struct {
	ctrl     *gomock.Controller
	recorder *MockFileCipherMockRecorder
}

// MockFileCipherMockRecorder is the mock recorder for MockFileCipher.
type MockFileCipherMockRecorder struct {
	mock *MockFileCipher
}

// NewMockFileCipher creates a new mock instance.
func NewMockFileCipher(ctrl *gomock.Controller) *MockFileCipher {
	mock := &MockFileCipher{ctrl: ctrl}
	mock.recorder = &MockFileCipherMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockFileCipher) EXPECT() *MockFileCipherMockRecorder {
	return m.recorder
}

// Decrypt mocks base method.
func (m *MockFileCipher) Decrypt(arg0 []byte) ([]byte, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Decrypt", arg0)
	ret0, _ := ret[0].([]byte)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Decrypt indicates an expected call of Decrypt.
func (mr *MockFileCipherMockRecorder) Decrypt(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Decrypt", reflect.TypeOf((*MockFileCipher)(nil).Decrypt), arg0)
}

// Decryptor mocks base method.
func (m *MockFileCipher) Decryptor(arg0 io.Reader) io.Reader {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Decryptor", arg0)
	ret0, _ := ret[0].(io.Reader)
	return ret0
}

// Decryptor indicates an expected call of Decryptor.
func (mr *MockFileCipherMockRecorder) Decryptor(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Decryptor", reflect.TypeOf((*MockFileCipher)(nil).Decryptor), arg0)
}

// Encrypt mocks base method.
func (m *MockFileCipher) Encrypt(arg0 []byte) ([]byte, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Encrypt", arg0)
	ret0, _ := ret[0].([]byte)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Encrypt indicates an expected call of Encrypt.
func (mr *MockFileCipherMockRecorder) Encrypt(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Encrypt", reflect.TypeOf((*MockFileCipher)(nil).Encrypt), arg0)
}

// Encryptor mocks base method.
func (m *MockFileCipher) Encryptor(arg0 io.Reader) io.Reader {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Encryptor", arg0)
	ret0, _ := ret[0].(io.Reader)
	return ret0
}

// Encryptor indicates an expected call of Encryptor.
func (mr *MockFileCipherMockRecorder) Encryptor(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Encryptor", reflect.TypeOf((*MockFileCipher)(nil).Encryptor), arg0)
}

// Key mocks base method.
func (m *MockFileCipher) Key() []byte {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Key")
	ret0, _ := ret[0].([]byte)
	return ret0
}

// Key indicates an expected call of Key.
func (mr *MockFileCipherMockRecorder) Key() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Key", reflect.TypeOf((*MockFileCipher)(nil).Key))
}
