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

package util

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tikv/migration/cdc/cdc/model"
)

func TestKeySpanSetBasics(t *testing.T) {
	ts := NewKeySpanSet()
	ok := ts.AddKeySpanRecord(&KeySpanRecord{
		KeySpanID: 1,
		CaptureID: "capture-1",
		Status:    AddingKeySpan,
	})
	require.True(t, ok)

	ok = ts.AddKeySpanRecord(&KeySpanRecord{
		KeySpanID: 1,
		CaptureID: "capture-2",
		Status:    AddingKeySpan,
	})
	// Adding a duplicate keyspan record should fail
	require.False(t, ok)

	record, ok := ts.GetKeySpanRecord(1)
	require.True(t, ok)
	require.Equal(t, &KeySpanRecord{
		KeySpanID: 1,
		CaptureID: "capture-1",
		Status:    AddingKeySpan,
	}, record)

	ok = ts.RemoveKeySpanRecord(1)
	require.True(t, ok)

	ok = ts.RemoveKeySpanRecord(2)
	require.False(t, ok)
}

func TestKeySpanSetCaptures(t *testing.T) {
	ts := NewKeySpanSet()
	ok := ts.AddKeySpanRecord(&KeySpanRecord{
		KeySpanID: 1,
		CaptureID: "capture-1",
		Status:    AddingKeySpan,
	})
	require.True(t, ok)

	ok = ts.AddKeySpanRecord(&KeySpanRecord{
		KeySpanID: 2,
		CaptureID: "capture-1",
		Status:    AddingKeySpan,
	})
	require.True(t, ok)

	ok = ts.AddKeySpanRecord(&KeySpanRecord{
		KeySpanID: 3,
		CaptureID: "capture-2",
		Status:    AddingKeySpan,
	})
	require.True(t, ok)

	ok = ts.AddKeySpanRecord(&KeySpanRecord{
		KeySpanID: 4,
		CaptureID: "capture-2",
		Status:    AddingKeySpan,
	})
	require.True(t, ok)

	ok = ts.AddKeySpanRecord(&KeySpanRecord{
		KeySpanID: 5,
		CaptureID: "capture-3",
		Status:    AddingKeySpan,
	})
	require.True(t, ok)

	require.Equal(t, 2, ts.CountKeySpanByCaptureID("capture-1"))
	require.Equal(t, 2, ts.CountKeySpanByCaptureID("capture-2"))
	require.Equal(t, 1, ts.CountKeySpanByCaptureID("capture-3"))

	ok = ts.AddKeySpanRecord(&KeySpanRecord{
		KeySpanID: 6,
		CaptureID: "capture-3",
		Status:    AddingKeySpan,
	})
	require.True(t, ok)
	require.Equal(t, 2, ts.CountKeySpanByCaptureID("capture-3"))

	captures := ts.GetDistinctCaptures()
	require.Len(t, captures, 3)
	require.Contains(t, captures, "capture-1")
	require.Contains(t, captures, "capture-2")
	require.Contains(t, captures, "capture-3")

	ok = ts.RemoveKeySpanRecord(3)
	require.True(t, ok)
	ok = ts.RemoveKeySpanRecord(4)
	require.True(t, ok)

	captures = ts.GetDistinctCaptures()
	require.Len(t, captures, 2)
	require.Contains(t, captures, "capture-1")
	require.Contains(t, captures, "capture-3")

	captureToKeySpanMap := ts.GetAllKeySpansGroupedByCaptures()
	require.Equal(t, map[model.CaptureID]map[model.KeySpanID]*KeySpanRecord{
		"capture-1": {
			1: &KeySpanRecord{
				KeySpanID: 1,
				CaptureID: "capture-1",
				Status:    AddingKeySpan,
			},
			2: &KeySpanRecord{
				KeySpanID: 2,
				CaptureID: "capture-1",
				Status:    AddingKeySpan,
			},
		},
		"capture-3": {
			5: &KeySpanRecord{
				KeySpanID: 5,
				CaptureID: "capture-3",
				Status:    AddingKeySpan,
			},
			6: &KeySpanRecord{
				KeySpanID: 6,
				CaptureID: "capture-3",
				Status:    AddingKeySpan,
			},
		},
	}, captureToKeySpanMap)

	removed := ts.RemoveKeySpanRecordByCaptureID("capture-3")
	require.Len(t, removed, 2)
	require.Contains(t, removed, &KeySpanRecord{
		KeySpanID: 5,
		CaptureID: "capture-3",
		Status:    AddingKeySpan,
	})
	require.Contains(t, removed, &KeySpanRecord{
		KeySpanID: 6,
		CaptureID: "capture-3",
		Status:    AddingKeySpan,
	})

	_, ok = ts.GetKeySpanRecord(5)
	require.False(t, ok)
	_, ok = ts.GetKeySpanRecord(6)
	require.False(t, ok)

	allKeySpans := ts.GetAllKeySpans()
	require.Equal(t, map[model.KeySpanID]*KeySpanRecord{
		1: {
			KeySpanID: 1,
			CaptureID: "capture-1",
			Status:    AddingKeySpan,
		},
		2: {
			KeySpanID: 2,
			CaptureID: "capture-1",
			Status:    AddingKeySpan,
		},
	}, allKeySpans)

	ok = ts.RemoveKeySpanRecord(1)
	require.True(t, ok)
	ok = ts.RemoveKeySpanRecord(2)
	require.True(t, ok)

	captureToKeySpanMap = ts.GetAllKeySpansGroupedByCaptures()
	require.Len(t, captureToKeySpanMap, 0)
}

func TestCountKeySpanByStatus(t *testing.T) {
	ts := NewKeySpanSet()
	ok := ts.AddKeySpanRecord(&KeySpanRecord{
		KeySpanID: 1,
		CaptureID: "capture-1",
		Status:    AddingKeySpan,
	})
	require.True(t, ok)

	ok = ts.AddKeySpanRecord(&KeySpanRecord{
		KeySpanID: 2,
		CaptureID: "capture-1",
		Status:    RunningKeySpan,
	})
	require.True(t, ok)

	ok = ts.AddKeySpanRecord(&KeySpanRecord{
		KeySpanID: 3,
		CaptureID: "capture-2",
		Status:    RemovingKeySpan,
	})
	require.True(t, ok)

	ok = ts.AddKeySpanRecord(&KeySpanRecord{
		KeySpanID: 4,
		CaptureID: "capture-2",
		Status:    AddingKeySpan,
	})
	require.True(t, ok)

	ok = ts.AddKeySpanRecord(&KeySpanRecord{
		KeySpanID: 5,
		CaptureID: "capture-3",
		Status:    RunningKeySpan,
	})
	require.True(t, ok)

	require.Equal(t, 2, ts.CountKeySpanByStatus(AddingKeySpan))
	require.Equal(t, 2, ts.CountKeySpanByStatus(RunningKeySpan))
	require.Equal(t, 1, ts.CountKeySpanByStatus(RemovingKeySpan))
}

func TestUpdateKeySpanRecord(t *testing.T) {
	ts := NewKeySpanSet()
	ok := ts.AddKeySpanRecord(&KeySpanRecord{
		KeySpanID: 4,
		CaptureID: "capture-2",
		Status:    AddingKeySpan,
	})
	require.True(t, ok)

	ok = ts.AddKeySpanRecord(&KeySpanRecord{
		KeySpanID: 5,
		CaptureID: "capture-3",
		Status:    AddingKeySpan,
	})
	require.True(t, ok)

	ok = ts.UpdateKeySpanRecord(&KeySpanRecord{
		KeySpanID: 5,
		CaptureID: "capture-3",
		Status:    RunningKeySpan,
	})
	require.True(t, ok)

	rec, ok := ts.GetKeySpanRecord(5)
	require.True(t, ok)
	require.Equal(t, RunningKeySpan, rec.Status)
	require.Equal(t, RunningKeySpan, ts.GetAllKeySpansGroupedByCaptures()["capture-3"][5].Status)

	ok = ts.UpdateKeySpanRecord(&KeySpanRecord{
		KeySpanID: 4,
		CaptureID: "capture-3",
		Status:    RunningKeySpan,
	})
	require.True(t, ok)
	rec, ok = ts.GetKeySpanRecord(4)
	require.True(t, ok)
	require.Equal(t, RunningKeySpan, rec.Status)
	require.Equal(t, "capture-3", rec.CaptureID)
	require.Equal(t, RunningKeySpan, ts.GetAllKeySpansGroupedByCaptures()["capture-3"][4].Status)
}
