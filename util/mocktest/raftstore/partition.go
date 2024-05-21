// Code generated by MockGen. DO NOT EDIT.
// Source: raftstore/partition.go

// Package raftstoremock is a generated GoMock package.
package raftstoremock

import (
	reflect "reflect"

	proto "github.com/cubefs/cubefs/depends/tiglabs/raft/proto"
	raftstore "github.com/cubefs/cubefs/raftstore"
	gomock "github.com/golang/mock/gomock"
)

// MockPartition is a mock of Partition interface.
type MockPartition struct {
	ctrl     *gomock.Controller
	recorder *MockPartitionMockRecorder
}

func (m *MockPartition) IsRestoring() bool {
	return true
}

// MockPartitionMockRecorder is the mock recorder for MockPartition.
type MockPartitionMockRecorder struct {
	mock *MockPartition
}

// NewMockPartition creates a new mock instance.
func NewMockPartition(ctrl *gomock.Controller) *MockPartition {
	mock := &MockPartition{ctrl: ctrl}
	mock.recorder = &MockPartitionMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockPartition) EXPECT() *MockPartitionMockRecorder {
	return m.recorder
}

// AppliedIndex mocks base method.
func (m *MockPartition) AppliedIndex() uint64 {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "AppliedIndex")
	ret0, _ := ret[0].(uint64)
	return ret0
}

// AppliedIndex indicates an expected call of AppliedIndex.
func (mr *MockPartitionMockRecorder) AppliedIndex() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AppliedIndex", reflect.TypeOf((*MockPartition)(nil).AppliedIndex))
}

// ChangeMember mocks base method.
func (m *MockPartition) ChangeMember(changeType proto.ConfChangeType, peer proto.Peer, context []byte) (interface{}, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ChangeMember", changeType, peer, context)
	ret0, _ := ret[0].(interface{})
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ChangeMember indicates an expected call of ChangeMember.
func (mr *MockPartitionMockRecorder) ChangeMember(changeType, peer, context interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ChangeMember", reflect.TypeOf((*MockPartition)(nil).ChangeMember), changeType, peer, context)
}

// CommittedIndex mocks base method.
func (m *MockPartition) CommittedIndex() uint64 {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CommittedIndex")
	ret0, _ := ret[0].(uint64)
	return ret0
}

// CommittedIndex indicates an expected call of CommittedIndex.
func (mr *MockPartitionMockRecorder) CommittedIndex() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CommittedIndex", reflect.TypeOf((*MockPartition)(nil).CommittedIndex))
}

// IsRestoring indicates an expected call of IsRestoring.
func (mr *MockPartitionMockRecorder) IsRestoring() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "IsRestoring", reflect.TypeOf((*MockPartition)(nil).IsRestoring))
}

// Delete mocks base method.
func (m *MockPartition) Delete() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Delete")
	ret0, _ := ret[0].(error)
	return ret0
}

// Delete indicates an expected call of Delete.
func (mr *MockPartitionMockRecorder) Delete() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Delete", reflect.TypeOf((*MockPartition)(nil).Delete))
}

// IsOfflinePeer mocks base method.
func (m *MockPartition) IsOfflinePeer() bool {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "IsOfflinePeer")
	ret0, _ := ret[0].(bool)
	return ret0
}

// IsOfflinePeer indicates an expected call of IsOfflinePeer.
func (mr *MockPartitionMockRecorder) IsOfflinePeer() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "IsOfflinePeer", reflect.TypeOf((*MockPartition)(nil).IsOfflinePeer))
}

// IsRaftLeader mocks base method.
func (m *MockPartition) IsRaftLeader() bool {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "IsRaftLeader")
	ret0, _ := ret[0].(bool)
	return ret0
}

// IsRaftLeader indicates an expected call of IsRaftLeader.
func (mr *MockPartitionMockRecorder) IsRaftLeader() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "IsRaftLeader", reflect.TypeOf((*MockPartition)(nil).IsRaftLeader))
}

// LeaderTerm mocks base method.
func (m *MockPartition) LeaderTerm() (uint64, uint64) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "LeaderTerm")
	ret0, _ := ret[0].(uint64)
	ret1, _ := ret[1].(uint64)
	return ret0, ret1
}

// LeaderTerm indicates an expected call of LeaderTerm.
func (mr *MockPartitionMockRecorder) LeaderTerm() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "LeaderTerm", reflect.TypeOf((*MockPartition)(nil).LeaderTerm))
}

// Status mocks base method.
func (m *MockPartition) Status() *raftstore.PartitionStatus {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Status")
	ret0, _ := ret[0].(*raftstore.PartitionStatus)
	return ret0
}

// Status indicates an expected call of Status.
func (mr *MockPartitionMockRecorder) Status() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Status", reflect.TypeOf((*MockPartition)(nil).Status))
}

// Stop mocks base method.
func (m *MockPartition) Stop() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Stop")
	ret0, _ := ret[0].(error)
	return ret0
}

// Stop indicates an expected call of Stop.
func (mr *MockPartitionMockRecorder) Stop() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Stop", reflect.TypeOf((*MockPartition)(nil).Stop))
}

// Submit mocks base method.
func (m *MockPartition) Submit(cmd []byte) (interface{}, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Submit", cmd)
	ret0, _ := ret[0].(interface{})
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Submit indicates an expected call of Submit.
func (mr *MockPartitionMockRecorder) Submit(cmd interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Submit", reflect.TypeOf((*MockPartition)(nil).Submit), cmd)
}

// Truncate mocks base method.
func (m *MockPartition) Truncate(index uint64) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Truncate", index)
}

// Truncate indicates an expected call of Truncate.
func (mr *MockPartitionMockRecorder) Truncate(index interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Truncate", reflect.TypeOf((*MockPartition)(nil).Truncate), index)
}

// TryToLeader mocks base method.
func (m *MockPartition) TryToLeader(nodeID uint64) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "TryToLeader", nodeID)
	ret0, _ := ret[0].(error)
	return ret0
}

// TryToLeader indicates an expected call of TryToLeader.
func (mr *MockPartitionMockRecorder) TryToLeader(nodeID interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "TryToLeader", reflect.TypeOf((*MockPartition)(nil).TryToLeader), nodeID)
}
