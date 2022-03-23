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

	"github.com/stretchr/testify/require"
	"github.com/tikv/migration/cdc/cdc/model"
	"github.com/tikv/migration/cdc/pkg/context"
)

func TestMoveKeySpanManagerBasics(t *testing.T) {
	m := newMoveKeySpanManager()

	// Test 1: Add a keyspan.
	m.Add(1, "capture-1")
	_, ok := m.GetTargetByKeySpanID(1)
	require.False(t, ok)

	// Test 2: Add a keyspan again.
	m.Add(2, "capture-2")
	_, ok = m.GetTargetByKeySpanID(2)
	require.False(t, ok)

	// Test 3: Add a keyspan with the same ID.
	ok = m.Add(2, "capture-2-1")
	require.False(t, ok)

	ctx := context.NewBackendContext4Test(false)
	// Test 4: Remove one keyspan
	var removedKeySpan model.KeySpanID
	ok, err := m.DoRemove(ctx, func(ctx context.Context, keyspanID model.KeySpanID, _ model.CaptureID) (result removeKeySpanResult, err error) {
		if removedKeySpan != 0 {
			return removeKeySpanResultUnavailable, nil
		}
		removedKeySpan = keyspanID
		return removeKeySpanResultOK, nil
	})
	require.NoError(t, err)
	require.False(t, ok)
	require.Containsf(t, []model.KeySpanID{1, 2}, removedKeySpan, "removedKeySpan: %d", removedKeySpan)

	// Test 5: Check removed keyspan's target
	target, ok := m.GetTargetByKeySpanID(removedKeySpan)
	require.True(t, ok)
	require.Equal(t, fmt.Sprintf("capture-%d", removedKeySpan), target)

	// Test 6: Remove another keyspan
	var removedKeySpan1 model.KeySpanID
	_, err = m.DoRemove(ctx, func(ctx context.Context, keyspanID model.KeySpanID, _ model.CaptureID) (result removeKeySpanResult, err error) {
		if removedKeySpan1 != 0 {
			require.Fail(t, "Should not have been called twice")
		}
		removedKeySpan1 = keyspanID
		return removeKeySpanResultOK, nil
	})
	require.NoError(t, err)

	// Test 7: Mark keyspan done
	m.MarkDone(1)
	_, ok = m.GetTargetByKeySpanID(1)
	require.False(t, ok)
}

func TestMoveKeySpanManagerCaptureRemoved(t *testing.T) {
	m := newMoveKeySpanManager()

	ok := m.Add(1, "capture-1")
	require.True(t, ok)

	ok = m.Add(2, "capture-2")
	require.True(t, ok)

	ok = m.Add(3, "capture-1")
	require.True(t, ok)

	ok = m.Add(4, "capture-2")
	require.True(t, ok)

	m.OnCaptureRemoved("capture-2")

	ctx := context.NewBackendContext4Test(false)
	var count int
	ok, err := m.DoRemove(ctx,
		func(ctx context.Context, keyspanID model.KeySpanID, target model.CaptureID) (result removeKeySpanResult, err error) {
			require.NotEqual(t, model.KeySpanID(2), keyspanID)
			require.NotEqual(t, model.KeySpanID(4), keyspanID)
			require.Equal(t, "capture-1", target)
			count++
			return removeKeySpanResultOK, nil
		},
	)
	require.NoError(t, err)
	require.True(t, ok)
}

func TestMoveKeySpanManagerGiveUp(t *testing.T) {
	m := newMoveKeySpanManager()

	ok := m.Add(1, "capture-1")
	require.True(t, ok)

	ok = m.Add(2, "capture-2")
	require.True(t, ok)

	ctx := context.NewBackendContext4Test(false)
	ok, err := m.DoRemove(ctx,
		func(ctx context.Context, keyspanID model.KeySpanID, target model.CaptureID) (result removeKeySpanResult, err error) {
			if keyspanID == 1 {
				return removeKeySpanResultOK, nil
			}
			return removeKeySpanResultGiveUp, nil
		},
	)
	require.NoError(t, err)
	require.True(t, ok)

	target, ok := m.GetTargetByKeySpanID(1)
	require.True(t, ok)
	require.Equal(t, "capture-1", target)

	_, ok = m.GetTargetByKeySpanID(2)
	require.False(t, ok)
}
