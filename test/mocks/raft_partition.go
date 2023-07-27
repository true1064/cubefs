// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/cubefs/cubefs/raftstore (interfaces: Partition)

// Package mocks is a generated GoMock package.
package mocks

import (
	reflect "reflect"

	raft "github.com/cubefs/cubefs/depends/tiglabs/raft"
	proto "github.com/cubefs/cubefs/depends/tiglabs/raft/proto"
	gomock "github.com/golang/mock/gomock"
)

// MockRaftPartition is a mock of Partition interface.
type MockRaftPartition struct {
	ctrl     *gomock.Controller
	recorder *MockRaftPartitionMockRecorder
}

// MockRaftPartitionMockRecorder is the mock recorder for MockRaftPartition.
type MockRaftPartitionMockRecorder struct {
	mock *MockRaftPartition
}

// NewMockRaftPartition creates a new mock instance.
func NewMockRaftPartition(ctrl *gomock.Controller) *MockRaftPartition {
	mock := &MockRaftPartition{ctrl: ctrl}
	mock.recorder = &MockRaftPartitionMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockRaftPartition) EXPECT() *MockRaftPartitionMockRecorder {
	return m.recorder
}

// AppliedIndex mocks base method.
func (m *MockRaftPartition) AppliedIndex() uint64 {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "AppliedIndex")
	ret0, _ := ret[0].(uint64)
	return ret0
}

// AppliedIndex indicates an expected call of AppliedIndex.
func (mr *MockRaftPartitionMockRecorder) AppliedIndex() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AppliedIndex", reflect.TypeOf((*MockRaftPartition)(nil).AppliedIndex))
}

// ChangeMember mocks base method.
func (m *MockRaftPartition) ChangeMember(arg0 proto.ConfChangeType, arg1 proto.Peer, arg2 []byte) (interface{}, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ChangeMember", arg0, arg1, arg2)
	ret0, _ := ret[0].(interface{})
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ChangeMember indicates an expected call of ChangeMember.
func (mr *MockRaftPartitionMockRecorder) ChangeMember(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ChangeMember", reflect.TypeOf((*MockRaftPartition)(nil).ChangeMember), arg0, arg1, arg2)
}

// CommittedIndex mocks base method.
func (m *MockRaftPartition) CommittedIndex() uint64 {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CommittedIndex")
	ret0, _ := ret[0].(uint64)
	return ret0
}

// CommittedIndex indicates an expected call of CommittedIndex.
func (mr *MockRaftPartitionMockRecorder) CommittedIndex() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CommittedIndex", reflect.TypeOf((*MockRaftPartition)(nil).CommittedIndex))
}

// Delete mocks base method.
func (m *MockRaftPartition) Delete() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Delete")
	ret0, _ := ret[0].(error)
	return ret0
}

// Delete indicates an expected call of Delete.
func (mr *MockRaftPartitionMockRecorder) Delete() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Delete", reflect.TypeOf((*MockRaftPartition)(nil).Delete))
}

// IsOfflinePeer mocks base method.
func (m *MockRaftPartition) IsOfflinePeer() bool {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "IsOfflinePeer")
	ret0, _ := ret[0].(bool)
	return ret0
}

// IsOfflinePeer indicates an expected call of IsOfflinePeer.
func (mr *MockRaftPartitionMockRecorder) IsOfflinePeer() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "IsOfflinePeer", reflect.TypeOf((*MockRaftPartition)(nil).IsOfflinePeer))
}

// IsRaftLeader mocks base method.
func (m *MockRaftPartition) IsRaftLeader() bool {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "IsRaftLeader")
	ret0, _ := ret[0].(bool)
	return ret0
}

// IsRaftLeader indicates an expected call of IsRaftLeader.
func (mr *MockRaftPartitionMockRecorder) IsRaftLeader() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "IsRaftLeader", reflect.TypeOf((*MockRaftPartition)(nil).IsRaftLeader))
}

// LeaderTerm mocks base method.
func (m *MockRaftPartition) LeaderTerm() (uint64, uint64) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "LeaderTerm")
	ret0, _ := ret[0].(uint64)
	ret1, _ := ret[1].(uint64)
	return ret0, ret1
}

// LeaderTerm indicates an expected call of LeaderTerm.
func (mr *MockRaftPartitionMockRecorder) LeaderTerm() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "LeaderTerm", reflect.TypeOf((*MockRaftPartition)(nil).LeaderTerm))
}

// Status mocks base method.
func (m *MockRaftPartition) Status() *raft.Status {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Status")
	ret0, _ := ret[0].(*raft.Status)
	return ret0
}

// Status indicates an expected call of Status.
func (mr *MockRaftPartitionMockRecorder) Status() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Status", reflect.TypeOf((*MockRaftPartition)(nil).Status))
}

// Stop mocks base method.
func (m *MockRaftPartition) Stop() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Stop")
	ret0, _ := ret[0].(error)
	return ret0
}

// Stop indicates an expected call of Stop.
func (mr *MockRaftPartitionMockRecorder) Stop() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Stop", reflect.TypeOf((*MockRaftPartition)(nil).Stop))
}

// Submit mocks base method.
func (m *MockRaftPartition) Submit(arg0 []byte) (interface{}, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Submit", arg0)
	ret0, _ := ret[0].(interface{})
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Submit indicates an expected call of Submit.
func (mr *MockRaftPartitionMockRecorder) Submit(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Submit", reflect.TypeOf((*MockRaftPartition)(nil).Submit), arg0)
}

// Truncate mocks base method.
func (m *MockRaftPartition) Truncate(arg0 uint64) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Truncate", arg0)
}

// Truncate indicates an expected call of Truncate.
func (mr *MockRaftPartitionMockRecorder) Truncate(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Truncate", reflect.TypeOf((*MockRaftPartition)(nil).Truncate), arg0)
}

// TryToLeader mocks base method.
func (m *MockRaftPartition) TryToLeader(arg0 uint64) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "TryToLeader", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// TryToLeader indicates an expected call of TryToLeader.
func (mr *MockRaftPartitionMockRecorder) TryToLeader(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "TryToLeader", reflect.TypeOf((*MockRaftPartition)(nil).TryToLeader), arg0)
}
