// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/filecoin-project/curio/lib/paths (interfaces: Store)

// Package mocks is a generated GoMock package.
package mocks

import (
	context "context"
	reflect "reflect"

	abi "github.com/filecoin-project/go-state-types/abi"
	fsutil "github.com/filecoin-project/lotus/storage/sealer/fsutil"
	storiface "github.com/filecoin-project/lotus/storage/sealer/storiface"
	gomock "github.com/golang/mock/gomock"
	cid "github.com/ipfs/go-cid"
)

// MockStore is a mock of Store interface.
type MockStore struct {
	ctrl     *gomock.Controller
	recorder *MockStoreMockRecorder
}

// MockStoreMockRecorder is the mock recorder for MockStore.
type MockStoreMockRecorder struct {
	mock *MockStore
}

// NewMockStore creates a new mock instance.
func NewMockStore(ctrl *gomock.Controller) *MockStore {
	mock := &MockStore{ctrl: ctrl}
	mock.recorder = &MockStoreMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockStore) EXPECT() *MockStoreMockRecorder {
	return m.recorder
}

// AcquireSector mocks base method.
func (m *MockStore) AcquireSector(arg0 context.Context, arg1 storiface.SectorRef, arg2, arg3 storiface.SectorFileType, arg4 storiface.PathType, arg5 storiface.AcquireMode, arg6 ...storiface.AcquireOption) (storiface.SectorPaths, storiface.SectorPaths, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{arg0, arg1, arg2, arg3, arg4, arg5}
	for _, a := range arg6 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "AcquireSector", varargs...)
	ret0, _ := ret[0].(storiface.SectorPaths)
	ret1, _ := ret[1].(storiface.SectorPaths)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// AcquireSector indicates an expected call of AcquireSector.
func (mr *MockStoreMockRecorder) AcquireSector(arg0, arg1, arg2, arg3, arg4, arg5 interface{}, arg6 ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{arg0, arg1, arg2, arg3, arg4, arg5}, arg6...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AcquireSector", reflect.TypeOf((*MockStore)(nil).AcquireSector), varargs...)
}

// FsStat mocks base method.
func (m *MockStore) FsStat(arg0 context.Context, arg1 storiface.ID) (fsutil.FsStat, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "FsStat", arg0, arg1)
	ret0, _ := ret[0].(fsutil.FsStat)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// FsStat indicates an expected call of FsStat.
func (mr *MockStoreMockRecorder) FsStat(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "FsStat", reflect.TypeOf((*MockStore)(nil).FsStat), arg0, arg1)
}

// GeneratePoRepVanillaProof mocks base method.
func (m *MockStore) GeneratePoRepVanillaProof(arg0 context.Context, arg1 storiface.SectorRef, arg2, arg3 cid.Cid, arg4 abi.SealRandomness, arg5 abi.InteractiveSealRandomness) ([]byte, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GeneratePoRepVanillaProof", arg0, arg1, arg2, arg3, arg4, arg5)
	ret0, _ := ret[0].([]byte)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GeneratePoRepVanillaProof indicates an expected call of GeneratePoRepVanillaProof.
func (mr *MockStoreMockRecorder) GeneratePoRepVanillaProof(arg0, arg1, arg2, arg3, arg4, arg5 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GeneratePoRepVanillaProof", reflect.TypeOf((*MockStore)(nil).GeneratePoRepVanillaProof), arg0, arg1, arg2, arg3, arg4, arg5)
}

// GenerateSingleVanillaProof mocks base method.
func (m *MockStore) GenerateSingleVanillaProof(arg0 context.Context, arg1 abi.ActorID, arg2 storiface.PostSectorChallenge, arg3 abi.RegisteredPoStProof) ([]byte, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GenerateSingleVanillaProof", arg0, arg1, arg2, arg3)
	ret0, _ := ret[0].([]byte)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GenerateSingleVanillaProof indicates an expected call of GenerateSingleVanillaProof.
func (mr *MockStoreMockRecorder) GenerateSingleVanillaProof(arg0, arg1, arg2, arg3 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GenerateSingleVanillaProof", reflect.TypeOf((*MockStore)(nil).GenerateSingleVanillaProof), arg0, arg1, arg2, arg3)
}

// MoveStorage mocks base method.
func (m *MockStore) MoveStorage(arg0 context.Context, arg1 storiface.SectorRef, arg2 storiface.SectorFileType, arg3 ...storiface.AcquireOption) error {
	m.ctrl.T.Helper()
	varargs := []interface{}{arg0, arg1, arg2}
	for _, a := range arg3 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "MoveStorage", varargs...)
	ret0, _ := ret[0].(error)
	return ret0
}

// MoveStorage indicates an expected call of MoveStorage.
func (mr *MockStoreMockRecorder) MoveStorage(arg0, arg1, arg2 interface{}, arg3 ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{arg0, arg1, arg2}, arg3...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "MoveStorage", reflect.TypeOf((*MockStore)(nil).MoveStorage), varargs...)
}

// Remove mocks base method.
func (m *MockStore) Remove(arg0 context.Context, arg1 abi.SectorID, arg2 storiface.SectorFileType, arg3 bool, arg4 []storiface.ID) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Remove", arg0, arg1, arg2, arg3, arg4)
	ret0, _ := ret[0].(error)
	return ret0
}

// Remove indicates an expected call of Remove.
func (mr *MockStoreMockRecorder) Remove(arg0, arg1, arg2, arg3, arg4 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Remove", reflect.TypeOf((*MockStore)(nil).Remove), arg0, arg1, arg2, arg3, arg4)
}

// RemoveCopies mocks base method.
func (m *MockStore) RemoveCopies(arg0 context.Context, arg1 abi.SectorID, arg2 storiface.SectorFileType) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RemoveCopies", arg0, arg1, arg2)
	ret0, _ := ret[0].(error)
	return ret0
}

// RemoveCopies indicates an expected call of RemoveCopies.
func (mr *MockStoreMockRecorder) RemoveCopies(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RemoveCopies", reflect.TypeOf((*MockStore)(nil).RemoveCopies), arg0, arg1, arg2)
}

// Reserve mocks base method.
func (m *MockStore) Reserve(arg0 context.Context, arg1 storiface.SectorRef, arg2 storiface.SectorFileType, arg3 storiface.SectorPaths, arg4 map[storiface.SectorFileType]int, arg5 float64) (func(), error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Reserve", arg0, arg1, arg2, arg3, arg4, arg5)
	ret0, _ := ret[0].(func())
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Reserve indicates an expected call of Reserve.
func (mr *MockStoreMockRecorder) Reserve(arg0, arg1, arg2, arg3, arg4, arg5 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Reserve", reflect.TypeOf((*MockStore)(nil).Reserve), arg0, arg1, arg2, arg3, arg4, arg5)
}
