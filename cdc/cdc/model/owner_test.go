// Copyright 2020 PingCAP, Inc.
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

package model

import (
	"math"
	"testing"

	"github.com/pingcap/check"
	"github.com/stretchr/testify/require"
)

func TestAdminJobType(t *testing.T) {
	t.Parallel()

	names := map[AdminJobType]string{
		AdminNone:         "noop",
		AdminStop:         "stop changefeed",
		AdminResume:       "resume changefeed",
		AdminRemove:       "remove changefeed",
		AdminFinish:       "finish changefeed",
		AdminJobType(100): "unknown",
	}
	for job, name := range names {
		require.Equal(t, name, job.String())
	}

	isStopped := map[AdminJobType]bool{
		AdminNone:   false,
		AdminStop:   true,
		AdminResume: false,
		AdminRemove: true,
		AdminFinish: true,
	}
	for job, stopped := range isStopped {
		require.Equal(t, stopped, job.IsStopState())
	}
}

func TestDDLStateString(t *testing.T) {
	t.Parallel()

	names := map[ChangeFeedDDLState]string{
		ChangeFeedSyncDML:          "SyncDML",
		ChangeFeedWaitToExecDDL:    "WaitToExecDDL",
		ChangeFeedExecDDL:          "ExecDDL",
		ChangeFeedDDLExecuteFailed: "DDLExecuteFailed",
		ChangeFeedDDLState(100):    "Unknown",
	}
	for state, name := range names {
		require.Equal(t, name, state.String())
	}
}

func TestTaskPositionMarshal(t *testing.T) {
	t.Parallel()

	pos := &TaskPosition{
		ResolvedTs:   420875942036766723,
		CheckPointTs: 420875940070686721,
	}
	expected := `{"checkpoint-ts":420875940070686721,"resolved-ts":420875942036766723,"count":0,"error":null}`

	data, err := pos.Marshal()
	require.Nil(t, err)
	require.Equal(t, expected, data)
	require.Equal(t, expected, pos.String())

	newPos := &TaskPosition{}
	err = newPos.Unmarshal([]byte(data))
	require.Nil(t, err)
	require.Equal(t, pos, newPos)
}

func TestChangeFeedStatusMarshal(t *testing.T) {
	t.Parallel()

	status := &ChangeFeedStatus{
		ResolvedTs:   420875942036766723,
		CheckpointTs: 420875940070686721,
	}
	expected := `{"resolved-ts":420875942036766723,"checkpoint-ts":420875940070686721,"admin-job-type":0}`

	data, err := status.Marshal()
	require.Nil(t, err)
	require.Equal(t, expected, data)

	newStatus := &ChangeFeedStatus{}
	err = newStatus.Unmarshal([]byte(data))
	require.Nil(t, err)
	require.Equal(t, status, newStatus)
}

func TestKeySpanOperationState(t *testing.T) {
	t.Parallel()

	processedMap := map[uint64]bool{
		OperDispatched: false,
		OperProcessed:  true,
		OperFinished:   true,
	}
	appliedMap := map[uint64]bool{
		OperDispatched: false,
		OperProcessed:  false,
		OperFinished:   true,
	}
	o := &KeySpanOperation{}

	for status, processed := range processedMap {
		o.Status = status
		require.Equal(t, processed, o.KeySpanProcessed())
	}
	for status, applied := range appliedMap {
		o.Status = status
		require.Equal(t, applied, o.KeySpanApplied())
	}

	// test clone nil operation. no-nil clone will be tested in `TestShouldBeDeepCopy`
	var nilKeySpanOper *KeySpanOperation
	require.Nil(t, nilKeySpanOper.Clone())
}

func TestTaskWorkloadMarshal(t *testing.T) {
	t.Parallel()

	workload := &TaskWorkload{
		12: WorkloadInfo{Workload: uint64(1)},
		15: WorkloadInfo{Workload: uint64(3)},
	}
	expected := `{"12":{"workload":1},"15":{"workload":3}}`

	data, err := workload.Marshal()
	require.Nil(t, err)
	require.Equal(t, expected, data)

	newWorkload := &TaskWorkload{}
	err = newWorkload.Unmarshal([]byte(data))
	require.Nil(t, err)
	require.Equal(t, workload, newWorkload)

	workload = nil
	data, err = workload.Marshal()
	require.Nil(t, err)
	require.Equal(t, "{}", data)
}

type taskStatusSuite struct{}

var _ = check.Suite(&taskStatusSuite{})

func TestShouldBeDeepCopy(t *testing.T) {
	t.Parallel()

	info := TaskStatus{

		KeySpans: map[KeySpanID]*KeySpanReplicaInfo{
			1: {StartTs: 100},
			2: {StartTs: 100},
			3: {StartTs: 100},
			4: {StartTs: 100},
		},
		Operation: map[KeySpanID]*KeySpanOperation{
			5: {
				Delete: true, BoundaryTs: 6,
			},
			6: {
				Delete: false, BoundaryTs: 7,
			},
		},
		AdminJobType: AdminStop,
	}

	clone := info.Clone()
	assertIsSnapshot := func() {
		require.Equal(t, map[KeySpanID]*KeySpanReplicaInfo{
			1: {StartTs: 100},
			2: {StartTs: 100},
			3: {StartTs: 100},
			4: {StartTs: 100},
		}, clone.KeySpans)
		require.Equal(t, map[KeySpanID]*KeySpanOperation{
			5: {
				Delete: true, BoundaryTs: 6,
			},
			6: {
				Delete: false, BoundaryTs: 7,
			},
		}, clone.Operation)
		require.Equal(t, AdminStop, clone.AdminJobType)
	}

	assertIsSnapshot()

	info.KeySpans[7] = &KeySpanReplicaInfo{StartTs: 100}
	info.Operation[7] = &KeySpanOperation{Delete: true, BoundaryTs: 7}

	info.Operation[5].BoundaryTs = 8
	info.KeySpans[1].StartTs = 200

	assertIsSnapshot()
}

func TestProcSnapshot(t *testing.T) {
	t.Parallel()

	info := TaskStatus{
		KeySpans: map[KeySpanID]*KeySpanReplicaInfo{
			10: {StartTs: 100},
		},
	}
	cfID := "changefeed-1"
	captureID := "capture-1"
	snap := info.Snapshot(cfID, captureID, 200)
	require.Equal(t, cfID, snap.CfID)
	require.Equal(t, captureID, snap.CaptureID)
	require.Equal(t, 1, len(snap.KeySpans))
	require.Equal(t, &KeySpanReplicaInfo{StartTs: 200}, snap.KeySpans[10])
}

func TestTaskStatusMarshal(t *testing.T) {
	t.Parallel()

	status := &TaskStatus{
		KeySpans: map[KeySpanID]*KeySpanReplicaInfo{
			1: {StartTs: 420875942036766723},
		},
	}
	expected := `{"keyspans":{"1":{"start-ts":420875942036766723,"Start":null,"End":null}},"operation":null,"admin-job-type":0}`

	data, err := status.Marshal()
	require.Nil(t, err)
	require.Equal(t, expected, data)
	require.Equal(t, expected, status.String())

	newStatus := &TaskStatus{}
	err = newStatus.Unmarshal([]byte(data))
	require.Nil(t, err)
	require.Equal(t, status, newStatus)
}

func TestAddKeySpan(t *testing.T) {
	t.Parallel()

	ts := uint64(420875942036766723)
	expected := &TaskStatus{
		KeySpans: map[KeySpanID]*KeySpanReplicaInfo{
			1: {StartTs: ts},
		},
		Operation: map[KeySpanID]*KeySpanOperation{
			1: {
				BoundaryTs: ts,
				Status:     OperDispatched,
			},
		},
	}
	status := &TaskStatus{}
	status.AddKeySpan(1, &KeySpanReplicaInfo{StartTs: ts}, ts)
	require.Equal(t, expected, status)

	// add existing keyspan does nothing
	status.AddKeySpan(1, &KeySpanReplicaInfo{StartTs: 1}, 1)
	require.Equal(t, expected, status)
}

func TestTaskStatusApplyState(t *testing.T) {
	t.Parallel()

	ts1 := uint64(420875042036766723)
	ts2 := uint64(420876783269969921)
	status := &TaskStatus{}
	status.AddKeySpan(1, &KeySpanReplicaInfo{StartTs: ts1}, ts1)
	status.AddKeySpan(2, &KeySpanReplicaInfo{StartTs: ts2}, ts2)
	require.True(t, status.SomeOperationsUnapplied())
	require.Equal(t, ts1, status.AppliedTs())

	status.Operation[1].Status = OperFinished
	status.Operation[2].Status = OperFinished
	require.False(t, status.SomeOperationsUnapplied())
	require.Equal(t, uint64(math.MaxUint64), status.AppliedTs())
}

func TestMoveKeySpan(t *testing.T) {
	t.Parallel()

	info := TaskStatus{
		KeySpans: map[KeySpanID]*KeySpanReplicaInfo{
			1: {StartTs: 100},
			2: {StartTs: 200},
		},
	}

	replicaInfo, found := info.RemoveKeySpan(2, 300, true)
	require.True(t, found)
	require.Equal(t, &KeySpanReplicaInfo{StartTs: 200}, replicaInfo)
	require.NotNil(t, info.KeySpans[uint64(1)])
	require.Nil(t, info.KeySpans[uint64(2)])
	expectedFlag := uint64(1) // OperFlagMoveKeySpan
	require.Equal(t, map[uint64]*KeySpanOperation{
		uint64(2): {
			Delete:     true,
			Flag:       expectedFlag,
			BoundaryTs: 300,
			Status:     OperDispatched,
		},
	}, info.Operation)
}

func TestShouldReturnRemovedKeySpan(t *testing.T) {
	t.Parallel()

	info := TaskStatus{
		KeySpans: map[KeySpanID]*KeySpanReplicaInfo{
			1: {StartTs: 100},
			2: {StartTs: 200},
			3: {StartTs: 300},
			4: {StartTs: 400},
		},
	}

	replicaInfo, found := info.RemoveKeySpan(2, 666, false)
	require.True(t, found)
	require.Equal(t, &KeySpanReplicaInfo{StartTs: 200}, replicaInfo)
}

func TestShouldHandleKeySpanNotFound(t *testing.T) {
	t.Parallel()

	info := TaskStatus{}
	_, found := info.RemoveKeySpan(404, 666, false)
	require.False(t, found)

	info = TaskStatus{
		KeySpans: map[KeySpanID]*KeySpanReplicaInfo{
			1: {StartTs: 100},
		},
	}
	_, found = info.RemoveKeySpan(404, 666, false)
	require.False(t, found)
}
