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
	"fmt"
	"testing"

	"github.com/pingcap/log"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/tikv/migration/cdc/cdc/model"
	"github.com/tikv/migration/cdc/cdc/scheduler/util"
	cdcContext "github.com/tikv/migration/cdc/pkg/context"
	"go.uber.org/zap"
)

var _ ScheduleDispatcherCommunicator = (*mockScheduleDispatcherCommunicator)(nil)

type mockScheduleDispatcherCommunicator struct {
	mock.Mock
	addKeySpanRecords    map[model.CaptureID][]model.KeySpanID
	removeKeySpanRecords map[model.CaptureID][]model.KeySpanID
}

func NewMockScheduleDispatcherCommunicator() *mockScheduleDispatcherCommunicator {
	return &mockScheduleDispatcherCommunicator{
		addKeySpanRecords:    map[model.CaptureID][]model.KeySpanID{},
		removeKeySpanRecords: map[model.CaptureID][]model.KeySpanID{},
	}
}

func (m *mockScheduleDispatcherCommunicator) Reset() {
	m.addKeySpanRecords = map[model.CaptureID][]model.KeySpanID{}
	m.removeKeySpanRecords = map[model.CaptureID][]model.KeySpanID{}
	m.Mock.ExpectedCalls = nil
	m.Mock.Calls = nil
}

func (m *mockScheduleDispatcherCommunicator) DispatchKeySpan(
	ctx cdcContext.Context,
	changeFeedID model.ChangeFeedID,
	keyspanID model.KeySpanID,
	captureID model.CaptureID,
	isDelete bool,
) (done bool, err error) {
	log.Info("dispatch keyspan called",
		zap.String("changefeed-id", changeFeedID),
		zap.Uint64("keyspan-id", keyspanID),
		zap.String("capture-id", captureID),
		zap.Bool("is-delete", isDelete))
	if !isDelete {
		m.addKeySpanRecords[captureID] = append(m.addKeySpanRecords[captureID], keyspanID)
	} else {
		m.removeKeySpanRecords[captureID] = append(m.removeKeySpanRecords[captureID], keyspanID)
	}
	args := m.Called(ctx, changeFeedID, keyspanID, captureID, isDelete)
	return args.Bool(0), args.Error(1)
}

func (m *mockScheduleDispatcherCommunicator) Announce(
	ctx cdcContext.Context,
	changeFeedID model.ChangeFeedID,
	captureID model.CaptureID,
) (done bool, err error) {
	args := m.Called(ctx, changeFeedID, captureID)
	return args.Bool(0), args.Error(1)
}

// read-only variable
var defaultMockCaptureInfos = map[model.CaptureID]*model.CaptureInfo{
	"capture-1": {
		ID:            "capture-1",
		AdvertiseAddr: "fakeip:1",
	},
	"capture-2": {
		ID:            "capture-2",
		AdvertiseAddr: "fakeip:2",
	},
}

func TestDispatchKeySpan(t *testing.T) {
	t.Parallel()

	ctx := cdcContext.NewBackendContext4Test(false)
	communicator := NewMockScheduleDispatcherCommunicator()
	dispatcher := NewBaseScheduleDispatcher("cf-1", communicator, 1000)

	communicator.On("Announce", mock.Anything, "cf-1", "capture-1").Return(true, nil)
	communicator.On("Announce", mock.Anything, "cf-1", "capture-2").Return(true, nil)
	checkpointTs, resolvedTs, err := dispatcher.Tick(ctx, 1000, []model.KeySpanID{1, 2, 3}, defaultMockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)
	communicator.AssertExpectations(t)

	dispatcher.OnAgentSyncTaskStatuses("capture-1", []model.KeySpanID{}, []model.KeySpanID{}, []model.KeySpanID{})
	dispatcher.OnAgentSyncTaskStatuses("capture-2", []model.KeySpanID{}, []model.KeySpanID{}, []model.KeySpanID{})

	communicator.Reset()
	// Injects a dispatch keyspan failure
	communicator.On("DispatchKeySpan", mock.Anything, "cf-1", mock.Anything, mock.Anything, false).
		Return(false, nil)
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1000, []model.KeySpanID{1, 2, 3}, defaultMockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)
	communicator.AssertExpectations(t)

	communicator.Reset()
	communicator.On("DispatchKeySpan", mock.Anything, "cf-1", model.KeySpanID(1), mock.Anything, false).
		Return(true, nil)
	communicator.On("DispatchKeySpan", mock.Anything, "cf-1", model.KeySpanID(2), mock.Anything, false).
		Return(true, nil)
	communicator.On("DispatchKeySpan", mock.Anything, "cf-1", model.KeySpanID(3), mock.Anything, false).
		Return(true, nil)
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1000, []model.KeySpanID{1, 2, 3}, defaultMockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)
	communicator.AssertExpectations(t)
	require.NotEqual(t, 0, len(communicator.addKeySpanRecords["capture-1"]))
	require.NotEqual(t, 0, len(communicator.addKeySpanRecords["capture-2"]))
	require.Equal(t, 0, len(communicator.removeKeySpanRecords["capture-1"]))
	require.Equal(t, 0, len(communicator.removeKeySpanRecords["capture-2"]))

	dispatcher.OnAgentCheckpoint("capture-1", 2000, 2000)
	dispatcher.OnAgentCheckpoint("capture-1", 2001, 2001)

	communicator.ExpectedCalls = nil
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1000, []model.KeySpanID{1, 2, 3}, defaultMockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)

	communicator.AssertExpectations(t)

	for captureID, keyspans := range communicator.addKeySpanRecords {
		for _, keyspanID := range keyspans {
			dispatcher.OnAgentFinishedKeySpanOperation(captureID, keyspanID)
		}
	}

	communicator.Reset()
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1000, []model.KeySpanID{1, 2, 3}, defaultMockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, model.Ts(1000), checkpointTs)
	require.Equal(t, model.Ts(1000), resolvedTs)

	dispatcher.OnAgentCheckpoint("capture-1", 1100, 1400)
	dispatcher.OnAgentCheckpoint("capture-2", 1200, 1300)
	communicator.Reset()
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1000, []model.KeySpanID{1, 2, 3}, defaultMockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, model.Ts(1100), checkpointTs)
	require.Equal(t, model.Ts(1300), resolvedTs)
}

func TestSyncCaptures(t *testing.T) {
	t.Parallel()

	ctx := cdcContext.NewBackendContext4Test(false)
	communicator := NewMockScheduleDispatcherCommunicator()
	dispatcher := NewBaseScheduleDispatcher("cf-1", communicator, 1000)
	dispatcher.captureStatus = map[model.CaptureID]*captureStatus{} // empty capture status
	communicator.On("Announce", mock.Anything, "cf-1", "capture-1").Return(false, nil)
	communicator.On("Announce", mock.Anything, "cf-1", "capture-2").Return(false, nil)

	checkpointTs, resolvedTs, err := dispatcher.Tick(ctx, 1500, []model.KeySpanID{1, 2, 3, 4, 5}, defaultMockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)

	communicator.Reset()
	communicator.On("Announce", mock.Anything, "cf-1", "capture-1").Return(true, nil)
	communicator.On("Announce", mock.Anything, "cf-1", "capture-2").Return(true, nil)
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1500, []model.KeySpanID{1, 2, 3, 4, 5}, defaultMockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)

	dispatcher.OnAgentSyncTaskStatuses("capture-1", []model.KeySpanID{1, 2, 3}, []model.KeySpanID{4, 5}, []model.KeySpanID{6, 7})
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1500, []model.KeySpanID{1, 2, 3, 4, 5}, defaultMockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)

	communicator.Reset()
	dispatcher.OnAgentFinishedKeySpanOperation("capture-1", 4)
	dispatcher.OnAgentFinishedKeySpanOperation("capture-1", 5)
	dispatcher.OnAgentSyncTaskStatuses("capture-2", []model.KeySpanID(nil), []model.KeySpanID(nil), []model.KeySpanID(nil))
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1500, []model.KeySpanID{1, 2, 3, 4, 5}, defaultMockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)

	communicator.Reset()
	dispatcher.OnAgentFinishedKeySpanOperation("capture-1", 6)
	dispatcher.OnAgentFinishedKeySpanOperation("capture-1", 7)
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1500, []model.KeySpanID{1, 2, 3, 4, 5}, defaultMockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, model.Ts(1500), checkpointTs)
	require.Equal(t, model.Ts(1500), resolvedTs)
}

func TestSyncUnknownCapture(t *testing.T) {
	t.Parallel()

	mockCaptureInfos := map[model.CaptureID]*model.CaptureInfo{}

	ctx := cdcContext.NewBackendContext4Test(false)
	communicator := NewMockScheduleDispatcherCommunicator()
	dispatcher := NewBaseScheduleDispatcher("cf-1", communicator, 1000)
	dispatcher.captureStatus = map[model.CaptureID]*captureStatus{} // empty capture status

	// Sends a sync from an unknown capture
	dispatcher.OnAgentSyncTaskStatuses("capture-1", []model.KeySpanID{1, 2, 3}, []model.KeySpanID{4, 5}, []model.KeySpanID{6, 7})

	// We expect the `Sync` to be ignored.
	checkpointTs, resolvedTs, err := dispatcher.Tick(ctx, 1500, []model.KeySpanID{1, 2, 3, 4, 5}, mockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)
}

func TestRemoveKeySpan(t *testing.T) {
	t.Parallel()

	ctx := cdcContext.NewBackendContext4Test(false)
	communicator := NewMockScheduleDispatcherCommunicator()
	dispatcher := NewBaseScheduleDispatcher("cf-1", communicator, 1000)
	dispatcher.captureStatus = map[model.CaptureID]*captureStatus{
		"capture-1": {
			SyncStatus:   captureSyncFinished,
			CheckpointTs: 1500,
			ResolvedTs:   1500,
		},
		"capture-2": {
			SyncStatus:   captureSyncFinished,
			CheckpointTs: 1500,
			ResolvedTs:   1500,
		},
	}
	dispatcher.keyspans.AddKeySpanRecord(&util.KeySpanRecord{
		KeySpanID: 1,
		CaptureID: "capture-1",
		Status:    util.RunningKeySpan,
	})
	dispatcher.keyspans.AddKeySpanRecord(&util.KeySpanRecord{
		KeySpanID: 2,
		CaptureID: "capture-2",
		Status:    util.RunningKeySpan,
	})
	dispatcher.keyspans.AddKeySpanRecord(&util.KeySpanRecord{
		KeySpanID: 3,
		CaptureID: "capture-1",
		Status:    util.RunningKeySpan,
	})

	checkpointTs, resolvedTs, err := dispatcher.Tick(ctx, 1500, []model.KeySpanID{1, 2, 3}, defaultMockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, model.Ts(1500), checkpointTs)
	require.Equal(t, model.Ts(1500), resolvedTs)

	// Inject a dispatch keyspan failure
	communicator.On("DispatchKeySpan", mock.Anything, "cf-1", model.KeySpanID(3), "capture-1", true).
		Return(false, nil)
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1500, []model.KeySpanID{1, 2}, defaultMockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)
	communicator.AssertExpectations(t)

	communicator.Reset()
	communicator.On("DispatchKeySpan", mock.Anything, "cf-1", model.KeySpanID(3), "capture-1", true).
		Return(true, nil)
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1500, []model.KeySpanID{1, 2}, defaultMockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)
	communicator.AssertExpectations(t)

	dispatcher.OnAgentFinishedKeySpanOperation("capture-1", 3)
	communicator.Reset()
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1500, []model.KeySpanID{1, 2}, defaultMockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, model.Ts(1500), checkpointTs)
	require.Equal(t, model.Ts(1500), resolvedTs)
}

func TestCaptureGone(t *testing.T) {
	t.Parallel()

	mockCaptureInfos := map[model.CaptureID]*model.CaptureInfo{
		"capture-1": {
			ID:            "capture-1",
			AdvertiseAddr: "fakeip:1",
		},
		// capture-2 is gone
	}

	ctx := cdcContext.NewBackendContext4Test(false)
	communicator := NewMockScheduleDispatcherCommunicator()
	dispatcher := NewBaseScheduleDispatcher("cf-1", communicator, 1000)
	dispatcher.captureStatus = map[model.CaptureID]*captureStatus{
		"capture-1": {
			SyncStatus:   captureSyncFinished,
			CheckpointTs: 1500,
			ResolvedTs:   1500,
		},
		"capture-2": {
			SyncStatus:   captureSyncFinished,
			CheckpointTs: 1500,
			ResolvedTs:   1500,
		},
	}
	dispatcher.keyspans.AddKeySpanRecord(&util.KeySpanRecord{
		KeySpanID: 1,
		CaptureID: "capture-1",
		Status:    util.RunningKeySpan,
	})
	dispatcher.keyspans.AddKeySpanRecord(&util.KeySpanRecord{
		KeySpanID: 2,
		CaptureID: "capture-2",
		Status:    util.RunningKeySpan,
	})
	dispatcher.keyspans.AddKeySpanRecord(&util.KeySpanRecord{
		KeySpanID: 3,
		CaptureID: "capture-1",
		Status:    util.RunningKeySpan,
	})

	communicator.On("DispatchKeySpan", mock.Anything, "cf-1", model.KeySpanID(2), "capture-1", false).
		Return(true, nil)
	checkpointTs, resolvedTs, err := dispatcher.Tick(ctx, 1500, []model.KeySpanID{1, 2, 3}, mockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)
	communicator.AssertExpectations(t)
}

func TestCaptureRestarts(t *testing.T) {
	t.Parallel()

	ctx := cdcContext.NewBackendContext4Test(false)
	communicator := NewMockScheduleDispatcherCommunicator()
	dispatcher := NewBaseScheduleDispatcher("cf-1", communicator, 1000)
	dispatcher.captureStatus = map[model.CaptureID]*captureStatus{
		"capture-1": {
			SyncStatus:   captureSyncFinished,
			CheckpointTs: 1500,
			ResolvedTs:   1500,
		},
		"capture-2": {
			SyncStatus:   captureSyncFinished,
			CheckpointTs: 1500,
			ResolvedTs:   1500,
		},
	}
	dispatcher.keyspans.AddKeySpanRecord(&util.KeySpanRecord{
		KeySpanID: 1,
		CaptureID: "capture-1",
		Status:    util.RunningKeySpan,
	})
	dispatcher.keyspans.AddKeySpanRecord(&util.KeySpanRecord{
		KeySpanID: 2,
		CaptureID: "capture-2",
		Status:    util.RunningKeySpan,
	})
	dispatcher.keyspans.AddKeySpanRecord(&util.KeySpanRecord{
		KeySpanID: 3,
		CaptureID: "capture-1",
		Status:    util.RunningKeySpan,
	})

	dispatcher.OnAgentSyncTaskStatuses("capture-2", []model.KeySpanID{}, []model.KeySpanID{}, []model.KeySpanID{})
	communicator.On("DispatchKeySpan", mock.Anything, "cf-1", model.KeySpanID(2), "capture-2", false).
		Return(true, nil)
	checkpointTs, resolvedTs, err := dispatcher.Tick(ctx, 1500, []model.KeySpanID{1, 2, 3}, defaultMockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)
	communicator.AssertExpectations(t)
}

func TestCaptureGoneWhileMovingKeySpan(t *testing.T) {
	t.Parallel()

	mockCaptureInfos := map[model.CaptureID]*model.CaptureInfo{
		"capture-1": {
			ID:            "capture-1",
			AdvertiseAddr: "fakeip:1",
		},
		"capture-2": {
			ID:            "capture-2",
			AdvertiseAddr: "fakeip:2",
		},
	}

	ctx := cdcContext.NewBackendContext4Test(false)
	communicator := NewMockScheduleDispatcherCommunicator()
	dispatcher := NewBaseScheduleDispatcher("cf-1", communicator, 1000)
	dispatcher.captureStatus = map[model.CaptureID]*captureStatus{
		"capture-1": {
			SyncStatus:   captureSyncFinished,
			CheckpointTs: 1300,
			ResolvedTs:   1600,
		},
		"capture-2": {
			SyncStatus:   captureSyncFinished,
			CheckpointTs: 1500,
			ResolvedTs:   1550,
		},
	}
	dispatcher.keyspans.AddKeySpanRecord(&util.KeySpanRecord{
		KeySpanID: 1,
		CaptureID: "capture-1",
		Status:    util.RunningKeySpan,
	})
	dispatcher.keyspans.AddKeySpanRecord(&util.KeySpanRecord{
		KeySpanID: 2,
		CaptureID: "capture-2",
		Status:    util.RunningKeySpan,
	})
	dispatcher.keyspans.AddKeySpanRecord(&util.KeySpanRecord{
		KeySpanID: 3,
		CaptureID: "capture-1",
		Status:    util.RunningKeySpan,
	})

	dispatcher.MoveKeySpan(1, "capture-2")
	communicator.On("DispatchKeySpan", mock.Anything, "cf-1", model.KeySpanID(1), "capture-1", true).
		Return(true, nil)
	checkpointTs, resolvedTs, err := dispatcher.Tick(ctx, 1300, []model.KeySpanID{1, 2, 3}, mockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)
	communicator.AssertExpectations(t)

	delete(mockCaptureInfos, "capture-2")
	dispatcher.OnAgentFinishedKeySpanOperation("capture-1", 1)
	communicator.Reset()
	communicator.On("DispatchKeySpan", mock.Anything, "cf-1", model.KeySpanID(1), mock.Anything, false).
		Return(true, nil)
	communicator.On("DispatchKeySpan", mock.Anything, "cf-1", model.KeySpanID(2), mock.Anything, false).
		Return(true, nil)
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1300, []model.KeySpanID{1, 2, 3}, mockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)
	communicator.AssertExpectations(t)
}

func TestRebalance(t *testing.T) {
	t.Parallel()

	mockCaptureInfos := map[model.CaptureID]*model.CaptureInfo{
		"capture-1": {
			ID:            "capture-1",
			AdvertiseAddr: "fakeip:1",
		},
		"capture-2": {
			ID:            "capture-2",
			AdvertiseAddr: "fakeip:2",
		},
		"capture-3": {
			ID:            "capture-3",
			AdvertiseAddr: "fakeip:3",
		},
	}

	ctx := cdcContext.NewBackendContext4Test(false)
	communicator := NewMockScheduleDispatcherCommunicator()
	dispatcher := NewBaseScheduleDispatcher("cf-1", communicator, 1000)
	dispatcher.captureStatus = map[model.CaptureID]*captureStatus{
		"capture-1": {
			SyncStatus:   captureSyncFinished,
			CheckpointTs: 1300,
			ResolvedTs:   1600,
		},
		"capture-2": {
			SyncStatus:   captureSyncFinished,
			CheckpointTs: 1500,
			ResolvedTs:   1550,
		},
		"capture-3": {
			SyncStatus:   captureSyncFinished,
			CheckpointTs: 1400,
			ResolvedTs:   1650,
		},
	}
	for i := 1; i <= 6; i++ {
		dispatcher.keyspans.AddKeySpanRecord(&util.KeySpanRecord{
			KeySpanID: model.KeySpanID(i),
			CaptureID: fmt.Sprintf("capture-%d", (i+1)%2+1),
			Status:    util.RunningKeySpan,
		})
	}

	dispatcher.Rebalance()
	communicator.On("DispatchKeySpan", mock.Anything, "cf-1", mock.Anything, mock.Anything, true).
		Return(false, nil)
	checkpointTs, resolvedTs, err := dispatcher.Tick(ctx, 1300, []model.KeySpanID{1, 2, 3, 4, 5, 6}, mockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)
	communicator.AssertExpectations(t)
	communicator.AssertNumberOfCalls(t, "DispatchKeySpan", 1)

	communicator.Reset()
	communicator.On("DispatchKeySpan", mock.Anything, "cf-1", mock.Anything, mock.Anything, true).
		Return(true, nil)
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1300, []model.KeySpanID{1, 2, 3, 4, 5, 6}, mockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)
	communicator.AssertNumberOfCalls(t, "DispatchKeySpan", 2)
	communicator.AssertExpectations(t)
}

func TestIgnoreEmptyCapture(t *testing.T) {
	t.Parallel()

	mockCaptureInfos := map[model.CaptureID]*model.CaptureInfo{
		"capture-1": {
			ID:            "capture-1",
			AdvertiseAddr: "fakeip:1",
		},
		"capture-2": {
			ID:            "capture-2",
			AdvertiseAddr: "fakeip:2",
		},
		"capture-3": {
			ID:            "capture-3",
			AdvertiseAddr: "fakeip:3",
		},
	}

	ctx := cdcContext.NewBackendContext4Test(false)
	communicator := NewMockScheduleDispatcherCommunicator()
	dispatcher := NewBaseScheduleDispatcher("cf-1", communicator, 1000)
	dispatcher.captureStatus = map[model.CaptureID]*captureStatus{
		"capture-1": {
			SyncStatus:   captureSyncFinished,
			CheckpointTs: 1300,
			ResolvedTs:   1600,
		},
		"capture-2": {
			SyncStatus:   captureSyncFinished,
			CheckpointTs: 1500,
			ResolvedTs:   1550,
		},
		"capture-3": {
			SyncStatus:   captureSyncFinished,
			CheckpointTs: 900,
			ResolvedTs:   1650,
		},
	}
	for i := 1; i <= 6; i++ {
		dispatcher.keyspans.AddKeySpanRecord(&util.KeySpanRecord{
			KeySpanID: model.KeySpanID(i),
			CaptureID: fmt.Sprintf("capture-%d", (i+1)%2+1),
			Status:    util.RunningKeySpan,
		})
	}

	checkpointTs, resolvedTs, err := dispatcher.Tick(ctx, 1300, []model.KeySpanID{1, 2, 3, 4, 5, 6}, mockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, model.Ts(1300), checkpointTs)
	require.Equal(t, model.Ts(1550), resolvedTs)
	communicator.AssertExpectations(t)
}

func TestIgnoreDeadCapture(t *testing.T) {
	t.Parallel()

	ctx := cdcContext.NewBackendContext4Test(false)
	communicator := NewMockScheduleDispatcherCommunicator()
	dispatcher := NewBaseScheduleDispatcher("cf-1", communicator, 1000)
	dispatcher.captureStatus = map[model.CaptureID]*captureStatus{
		"capture-1": {
			SyncStatus:   captureSyncFinished,
			CheckpointTs: 1300,
			ResolvedTs:   1600,
		},
		"capture-2": {
			SyncStatus:   captureSyncFinished,
			CheckpointTs: 1500,
			ResolvedTs:   1550,
		},
	}
	for i := 1; i <= 6; i++ {
		dispatcher.keyspans.AddKeySpanRecord(&util.KeySpanRecord{
			KeySpanID: model.KeySpanID(i),
			CaptureID: fmt.Sprintf("capture-%d", (i+1)%2+1),
			Status:    util.RunningKeySpan,
		})
	}

	// A dead capture sends very old watermarks.
	// They should be ignored.
	dispatcher.OnAgentCheckpoint("capture-3", 1000, 1000)
	checkpointTs, resolvedTs, err := dispatcher.Tick(ctx, 1300, []model.KeySpanID{1, 2, 3, 4, 5, 6}, defaultMockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, model.Ts(1300), checkpointTs)
	require.Equal(t, model.Ts(1550), resolvedTs)
	communicator.AssertExpectations(t)
}

func TestIgnoreUnsyncedCaptures(t *testing.T) {
	t.Parallel()

	ctx := cdcContext.NewBackendContext4Test(false)
	communicator := NewMockScheduleDispatcherCommunicator()
	dispatcher := NewBaseScheduleDispatcher("cf-1", communicator, 1000)
	dispatcher.captureStatus = map[model.CaptureID]*captureStatus{
		"capture-1": {
			SyncStatus:   captureSyncFinished,
			CheckpointTs: 1300,
			ResolvedTs:   1600,
		},
		"capture-2": {
			SyncStatus:   captureSyncSent, // not synced
			CheckpointTs: 1400,
			ResolvedTs:   1500,
		},
	}

	for i := 1; i <= 6; i++ {
		dispatcher.keyspans.AddKeySpanRecord(&util.KeySpanRecord{
			KeySpanID: model.KeySpanID(i),
			CaptureID: fmt.Sprintf("capture-%d", (i+1)%2+1),
			Status:    util.RunningKeySpan,
		})
	}

	dispatcher.OnAgentCheckpoint("capture-2", 1000, 1000)
	checkpointTs, resolvedTs, err := dispatcher.Tick(ctx, 1300, []model.KeySpanID{1, 2, 3, 4, 5, 6}, defaultMockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)

	communicator.Reset()
	dispatcher.OnAgentSyncTaskStatuses("capture-2", []model.KeySpanID{2, 4, 6}, []model.KeySpanID{}, []model.KeySpanID{})
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1300, []model.KeySpanID{1, 2, 3, 4, 5, 6}, defaultMockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, model.Ts(1300), checkpointTs)
	require.Equal(t, model.Ts(1500), resolvedTs)
	communicator.AssertExpectations(t)
}

func TestRebalanceWhileAddingKeySpan(t *testing.T) {
	t.Parallel()

	ctx := cdcContext.NewBackendContext4Test(false)
	communicator := NewMockScheduleDispatcherCommunicator()
	dispatcher := NewBaseScheduleDispatcher("cf-1", communicator, 1000)
	dispatcher.captureStatus = map[model.CaptureID]*captureStatus{
		"capture-1": {
			SyncStatus:   captureSyncFinished,
			CheckpointTs: 1300,
			ResolvedTs:   1600,
		},
		"capture-2": {
			SyncStatus:   captureSyncFinished,
			CheckpointTs: 1500,
			ResolvedTs:   1550,
		},
	}
	for i := 1; i <= 6; i++ {
		dispatcher.keyspans.AddKeySpanRecord(&util.KeySpanRecord{
			KeySpanID: model.KeySpanID(i),
			CaptureID: "capture-1",
			Status:    util.RunningKeySpan,
		})
	}

	communicator.On("DispatchKeySpan", mock.Anything, "cf-1", model.KeySpanID(7), "capture-2", false).
		Return(true, nil)
	checkpointTs, resolvedTs, err := dispatcher.Tick(ctx, 1300, []model.KeySpanID{1, 2, 3, 4, 5, 6, 7}, defaultMockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)
	communicator.AssertExpectations(t)

	dispatcher.Rebalance()
	communicator.Reset()
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1300, []model.KeySpanID{1, 2, 3, 4, 5, 6, 7}, defaultMockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)
	communicator.AssertExpectations(t)

	dispatcher.OnAgentFinishedKeySpanOperation("capture-2", model.KeySpanID(7))
	communicator.Reset()
	communicator.On("DispatchKeySpan", mock.Anything, "cf-1", mock.Anything, mock.Anything, true).
		Return(true, nil)
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1300, []model.KeySpanID{1, 2, 3, 4, 5, 6, 7}, defaultMockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)
	communicator.AssertNumberOfCalls(t, "DispatchKeySpan", 2)
	communicator.AssertExpectations(t)
}

func TestManualMoveKeySpanWhileAddingKeySpan(t *testing.T) {
	t.Parallel()

	ctx := cdcContext.NewBackendContext4Test(false)
	communicator := NewMockScheduleDispatcherCommunicator()
	dispatcher := NewBaseScheduleDispatcher("cf-1", communicator, 1000)
	dispatcher.captureStatus = map[model.CaptureID]*captureStatus{
		"capture-1": {
			SyncStatus:   captureSyncFinished,
			CheckpointTs: 1300,
			ResolvedTs:   1600,
		},
		"capture-2": {
			SyncStatus:   captureSyncFinished,
			CheckpointTs: 1500,
			ResolvedTs:   1550,
		},
	}
	dispatcher.keyspans.AddKeySpanRecord(&util.KeySpanRecord{
		KeySpanID: 2,
		CaptureID: "capture-1",
		Status:    util.RunningKeySpan,
	})
	dispatcher.keyspans.AddKeySpanRecord(&util.KeySpanRecord{
		KeySpanID: 3,
		CaptureID: "capture-1",
		Status:    util.RunningKeySpan,
	})

	communicator.On("DispatchKeySpan", mock.Anything, "cf-1", model.KeySpanID(1), "capture-2", false).
		Return(true, nil)
	checkpointTs, resolvedTs, err := dispatcher.Tick(ctx, 1300, []model.KeySpanID{1, 2, 3}, defaultMockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)

	dispatcher.MoveKeySpan(1, "capture-1")
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1300, []model.KeySpanID{1, 2, 3}, defaultMockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)
	communicator.AssertExpectations(t)

	dispatcher.OnAgentFinishedKeySpanOperation("capture-2", 1)
	communicator.Reset()
	communicator.On("DispatchKeySpan", mock.Anything, "cf-1", model.KeySpanID(1), "capture-2", true).
		Return(true, nil)
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1300, []model.KeySpanID{1, 2, 3}, defaultMockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)
	communicator.AssertExpectations(t)

	dispatcher.OnAgentFinishedKeySpanOperation("capture-2", 1)
	communicator.Reset()
	communicator.On("DispatchKeySpan", mock.Anything, "cf-1", model.KeySpanID(1), "capture-1", false).
		Return(true, nil)
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1300, []model.KeySpanID{1, 2, 3}, defaultMockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)
	communicator.AssertExpectations(t)
}

func TestAutoRebalanceOnCaptureOnline(t *testing.T) {
	// This test case tests the following scenario:
	// 1. Capture-1 and Capture-2 are online.
	// 2. Owner dispatches three keyspans to these two captures.
	// 3. While the pending dispatches are in progress, Capture-3 goes online.
	// 4. Capture-1 and Capture-2 finish the dispatches.
	//
	// We expect that the workload is eventually balanced by migrating
	// a keyspan to Capture-3.

	t.Parallel()

	ctx := cdcContext.NewBackendContext4Test(false)
	communicator := NewMockScheduleDispatcherCommunicator()
	dispatcher := NewBaseScheduleDispatcher("cf-1", communicator, 1000)

	captureList := map[model.CaptureID]*model.CaptureInfo{
		"capture-1": {
			ID:            "capture-1",
			AdvertiseAddr: "fakeip:1",
		},
		"capture-2": {
			ID:            "capture-2",
			AdvertiseAddr: "fakeip:2",
		},
	}

	communicator.On("Announce", mock.Anything, "cf-1", "capture-1").Return(true, nil)
	communicator.On("Announce", mock.Anything, "cf-1", "capture-2").Return(true, nil)
	checkpointTs, resolvedTs, err := dispatcher.Tick(ctx, 1000, []model.KeySpanID{1, 2, 3}, captureList)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)
	communicator.AssertExpectations(t)

	dispatcher.OnAgentSyncTaskStatuses("capture-1", []model.KeySpanID{}, []model.KeySpanID{}, []model.KeySpanID{})
	dispatcher.OnAgentSyncTaskStatuses("capture-2", []model.KeySpanID{}, []model.KeySpanID{}, []model.KeySpanID{})

	communicator.Reset()
	communicator.On("DispatchKeySpan", mock.Anything, "cf-1", model.KeySpanID(1), mock.Anything, false).
		Return(true, nil)
	communicator.On("DispatchKeySpan", mock.Anything, "cf-1", model.KeySpanID(2), mock.Anything, false).
		Return(true, nil)
	communicator.On("DispatchKeySpan", mock.Anything, "cf-1", model.KeySpanID(3), mock.Anything, false).
		Return(true, nil)
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1000, []model.KeySpanID{1, 2, 3}, captureList)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)
	communicator.AssertExpectations(t)
	require.NotEqual(t, 0, len(communicator.addKeySpanRecords["capture-1"]))
	require.NotEqual(t, 0, len(communicator.addKeySpanRecords["capture-2"]))
	require.Equal(t, 0, len(communicator.removeKeySpanRecords["capture-1"]))
	require.Equal(t, 0, len(communicator.removeKeySpanRecords["capture-2"]))

	dispatcher.OnAgentCheckpoint("capture-1", 2000, 2000)
	dispatcher.OnAgentCheckpoint("capture-1", 2001, 2001)

	communicator.ExpectedCalls = nil
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1000, []model.KeySpanID{1, 2, 3}, captureList)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)

	communicator.AssertExpectations(t)

	captureList["capture-3"] = &model.CaptureInfo{
		ID:            "capture-3",
		AdvertiseAddr: "fakeip:3",
	}
	communicator.ExpectedCalls = nil
	communicator.On("Announce", mock.Anything, "cf-1", "capture-3").Return(true, nil)
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1000, []model.KeySpanID{1, 2, 3}, captureList)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)
	communicator.AssertExpectations(t)

	communicator.ExpectedCalls = nil
	dispatcher.OnAgentSyncTaskStatuses("capture-3", []model.KeySpanID{}, []model.KeySpanID{}, []model.KeySpanID{})
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1000, []model.KeySpanID{1, 2, 3}, captureList)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)
	communicator.AssertExpectations(t)

	for captureID, keyspans := range communicator.addKeySpanRecords {
		for _, keyspanID := range keyspans {
			dispatcher.OnAgentFinishedKeySpanOperation(captureID, keyspanID)
		}
	}

	communicator.Reset()
	var removeKeySpanFromCapture model.CaptureID
	communicator.On("DispatchKeySpan", mock.Anything, "cf-1", mock.Anything, mock.Anything, true).
		Return(true, nil).Run(func(args mock.Arguments) {
		removeKeySpanFromCapture = args.Get(3).(model.CaptureID)
	})
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1000, []model.KeySpanID{1, 2, 3}, captureList)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)
	communicator.AssertExpectations(t)

	removedKeySpanID := communicator.removeKeySpanRecords[removeKeySpanFromCapture][0]

	dispatcher.OnAgentFinishedKeySpanOperation(removeKeySpanFromCapture, removedKeySpanID)
	dispatcher.OnAgentCheckpoint("capture-1", 1100, 1400)
	dispatcher.OnAgentCheckpoint("capture-2", 1200, 1300)
	communicator.ExpectedCalls = nil
	communicator.On("DispatchKeySpan", mock.Anything, "cf-1", removedKeySpanID, "capture-3", false).
		Return(true, nil)
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1000, []model.KeySpanID{1, 2, 3}, captureList)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)
	communicator.AssertExpectations(t)
}
