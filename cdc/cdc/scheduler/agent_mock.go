// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package scheduler

import (
	"testing"

	"github.com/pingcap/log"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/tikv/migration/cdc/cdc/model"
	cdcContext "github.com/tikv/migration/cdc/pkg/context"
	"go.uber.org/zap"
)

type MockProcessorMessenger struct {
	mock.Mock
}

func (m *MockProcessorMessenger) FinishKeySpanOperation(ctx cdcContext.Context, keyspanID model.KeySpanID) (bool, error) {
	args := m.Called(ctx, keyspanID)
	return args.Bool(0), args.Error(1)
}

func (m *MockProcessorMessenger) SyncTaskStatuses(ctx cdcContext.Context, running, adding, removing []model.KeySpanID) (bool, error) {
	args := m.Called(ctx, running, adding, removing)
	return args.Bool(0), args.Error(1)
}

func (m *MockProcessorMessenger) SendCheckpoint(ctx cdcContext.Context, checkpointTs model.Ts, resolvedTs model.Ts) (bool, error) {
	args := m.Called(ctx, checkpointTs, resolvedTs)
	return args.Bool(0), args.Error(1)
}

func (m *MockProcessorMessenger) Barrier(ctx cdcContext.Context) (done bool) {
	args := m.Called(ctx)
	return args.Bool(0)
}

func (m *MockProcessorMessenger) OnOwnerChanged(ctx cdcContext.Context, newOwnerCaptureID model.CaptureID) {
	m.Called(ctx, newOwnerCaptureID)
}

func (m *MockProcessorMessenger) Close() error {
	args := m.Called()
	return args.Error(0)
}

type MockCheckpointSender struct {
	lastSentCheckpointTs model.Ts
	lastSentResolvedTs   model.Ts
}

func (s *MockCheckpointSender) SendCheckpoint(_ cdcContext.Context, provider checkpointProviderFunc) error {
	checkpointTs, resolvedTs, ok := provider()
	if !ok {
		return nil
	}
	s.lastSentCheckpointTs = checkpointTs
	s.lastSentResolvedTs = resolvedTs
	return nil
}

func (s *MockCheckpointSender) LastSentCheckpointTs() model.Ts {
	return s.lastSentCheckpointTs
}

type MockKeySpanExecutor struct {
	mock.Mock

	t *testing.T

	Adding, Running, Removing map[model.KeySpanID]struct{}
}

func NewMockKeySpanExecutor(t *testing.T) *MockKeySpanExecutor {
	return &MockKeySpanExecutor{
		t:        t,
		Adding:   map[model.KeySpanID]struct{}{},
		Running:  map[model.KeySpanID]struct{}{},
		Removing: map[model.KeySpanID]struct{}{},
	}
}

func (e *MockKeySpanExecutor) AddKeySpan(ctx cdcContext.Context, keyspanID model.KeySpanID, Start []byte, End []byte) (bool, error) {
	log.Info("AddKeySpan", zap.Uint64("keyspan-id", keyspanID))
	require.NotContains(e.t, e.Adding, keyspanID)
	require.NotContains(e.t, e.Running, keyspanID)
	require.NotContains(e.t, e.Removing, keyspanID)
	args := e.Called(ctx, keyspanID)
	if args.Bool(0) {
		// If the mock return value indicates a success, then we record the added keyspan.
		e.Adding[keyspanID] = struct{}{}
	}
	return args.Bool(0), args.Error(1)
}

func (e *MockKeySpanExecutor) RemoveKeySpan(ctx cdcContext.Context, keyspanID model.KeySpanID) (bool, error) {
	log.Info("RemoveKeySpan", zap.Uint64("keyspan-id", keyspanID))
	args := e.Called(ctx, keyspanID)
	require.Contains(e.t, e.Running, keyspanID)
	require.NotContains(e.t, e.Removing, keyspanID)
	delete(e.Running, keyspanID)
	e.Removing[keyspanID] = struct{}{}
	return args.Bool(0), args.Error(1)
}

func (e *MockKeySpanExecutor) IsAddKeySpanFinished(ctx cdcContext.Context, keyspanID model.KeySpanID) bool {
	_, ok := e.Running[keyspanID]
	return ok
}

func (e *MockKeySpanExecutor) IsRemoveKeySpanFinished(ctx cdcContext.Context, keyspanID model.KeySpanID) bool {
	_, ok := e.Removing[keyspanID]
	return !ok
}

func (e *MockKeySpanExecutor) GetAllCurrentKeySpans() []model.KeySpanID {
	var ret []model.KeySpanID
	for keyspanID := range e.Adding {
		ret = append(ret, keyspanID)
	}
	for keyspanID := range e.Running {
		ret = append(ret, keyspanID)
	}
	for keyspanID := range e.Removing {
		ret = append(ret, keyspanID)
	}

	return ret
}

func (e *MockKeySpanExecutor) GetCheckpoint() (checkpointTs, resolvedTs model.Ts) {
	args := e.Called()
	return args.Get(0).(model.Ts), args.Get(1).(model.Ts)
}
