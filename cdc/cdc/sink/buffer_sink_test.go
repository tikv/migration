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

package sink

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tikv/migration/cdc/cdc/model"
)

func TestKeySpanIsNotFlushed(t *testing.T) {
	t.Parallel()

	b := bufferSink{changeFeedCheckpointTs: 1}
	require.Equal(t, uint64(1), b.getKeySpanCheckpointTs(2))
	b.UpdateChangeFeedCheckpointTs(3)
	require.Equal(t, uint64(3), b.getKeySpanCheckpointTs(2))
}

func TestFlushRawKVEntry(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()

	backendSink := newBlackHoleSink(ctx, make(map[string]string))
	b := newBufferSink(backendSink, 1, make(chan drawbackMsg))
	go b.run(ctx, make(chan error))

	require.Nil(t, b.EmitChangedEvents(ctx))
	require.Nil(t, b.EmitChangedEvents(ctx, []*model.RawKVEntry{
		{KeySpanID: 1, CRTs: 2},
		{KeySpanID: 1, CRTs: 2},
		{KeySpanID: 1, CRTs: 3},
		{KeySpanID: 1, CRTs: 5},
		{KeySpanID: 1, CRTs: 6},
	}...))
	require.Equal(t, 5, len(b.buffer[uint64(1)]))

	require.Nil(t, b.EmitChangedEvents(ctx, []*model.RawKVEntry{
		{KeySpanID: 2, CRTs: 3},
		{KeySpanID: 2, CRTs: 3},
		{KeySpanID: 2, CRTs: 5},
		{KeySpanID: 2, CRTs: 6},
		{KeySpanID: 2, CRTs: 6},
	}...))
	require.Equal(t, 5, len(b.buffer[uint64(2)]))

	_, err := b.FlushChangedEvents(ctx, 1, 4)
	require.Nil(t, err)
	retry := 0
	for retry < 5 {
		if backendSink.accumulated == 3 && len(b.buffer[uint64(1)]) == 2 {
			break
		}
		time.Sleep(100 * time.Millisecond)
		retry++
	}
	require.Less(t, retry, 5)

	_, err = b.FlushChangedEvents(ctx, 2, 4)
	require.Nil(t, err)
	retry = 0
	for retry < 5 {
		if backendSink.accumulated == 5 && len(b.buffer[uint64(2)]) == 3 {
			break
		}
		time.Sleep(100 * time.Millisecond)
		retry++
	}
	require.Less(t, retry, 5)

	_, err = b.FlushChangedEvents(ctx, 1, 6)
	require.Nil(t, err)
	retry = 0
	for retry < 5 {
		if backendSink.accumulated == 7 && len(b.buffer[uint64(1)]) == 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
		retry++
	}
	require.Less(t, retry, 5)

	_, err = b.FlushChangedEvents(ctx, 2, 6)
	require.Nil(t, err)
	retry = 0
	for retry < 5 {
		if backendSink.accumulated == 10 && len(b.buffer[uint64(2)]) == 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
		retry++
	}
	require.Less(t, retry, 5)
}

func TestFlushKeySpan(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	b := newBufferSink(newBlackHoleSink(ctx, make(map[string]string)), 5, make(chan drawbackMsg))
	go b.run(ctx, make(chan error))

	require.Equal(t, uint64(5), b.getKeySpanCheckpointTs(2))
	require.Nil(t, b.EmitChangedEvents(ctx))
	require.Nil(t, b.EmitChangedEvents(ctx, []*model.RawKVEntry{
		{KeySpanID: 1},
		{KeySpanID: 2},
		{KeySpanID: 3},
		{KeySpanID: 4},
		{KeySpanID: 1},
		{KeySpanID: 2},
		{KeySpanID: 3},
		{KeySpanID: 4},
	}...))
	checkpoint, err := b.FlushChangedEvents(ctx, 1, 7)
	require.True(t, checkpoint <= 7)
	require.Nil(t, err)
	checkpoint, err = b.FlushChangedEvents(ctx, 2, 6)
	require.True(t, checkpoint <= 6)
	require.Nil(t, err)
	checkpoint, err = b.FlushChangedEvents(ctx, 3, 8)
	require.True(t, checkpoint <= 8)
	require.Nil(t, err)
	time.Sleep(200 * time.Millisecond)
	require.Equal(t, uint64(7), b.getKeySpanCheckpointTs(1))
	require.Equal(t, uint64(6), b.getKeySpanCheckpointTs(2))
	require.Equal(t, uint64(8), b.getKeySpanCheckpointTs(3))
	require.Equal(t, uint64(5), b.getKeySpanCheckpointTs(4))
	b.UpdateChangeFeedCheckpointTs(6)
	require.Equal(t, uint64(7), b.getKeySpanCheckpointTs(1))
	require.Equal(t, uint64(6), b.getKeySpanCheckpointTs(2))
	require.Equal(t, uint64(8), b.getKeySpanCheckpointTs(3))
	require.Equal(t, uint64(6), b.getKeySpanCheckpointTs(4))
}

func TestFlushFailed(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.TODO())
	b := newBufferSink(newBlackHoleSink(ctx, make(map[string]string)), 5, make(chan drawbackMsg))
	go b.run(ctx, make(chan error))

	checkpoint, err := b.FlushChangedEvents(ctx, 3, 8)
	require.True(t, checkpoint <= 8)
	require.Nil(t, err)
	time.Sleep(200 * time.Millisecond)
	require.Equal(t, uint64(8), b.getKeySpanCheckpointTs(3))
	cancel()
	checkpoint, _ = b.FlushChangedEvents(ctx, 3, 18)
	require.Equal(t, uint64(8), checkpoint)
	checkpoint, _ = b.FlushChangedEvents(ctx, 1, 18)
	require.Equal(t, uint64(5), checkpoint)
	time.Sleep(200 * time.Millisecond)
	require.Equal(t, uint64(8), b.getKeySpanCheckpointTs(3))
	require.Equal(t, uint64(5), b.getKeySpanCheckpointTs(1))
}

type benchSink struct {
	Sink
}

func (b *benchSink) FlushRowChangedEvents(
	ctx context.Context, keyspanID model.KeySpanID, resolvedTs uint64,
) (uint64, error) {
	return 0, nil
}

func BenchmarkRun(b *testing.B) {
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()

	state := runState{
		metricFlushDuration:   flushRowChangedDuration.WithLabelValues(b.Name(), b.Name(), "Flush"),
		metricEmitRowDuration: flushRowChangedDuration.WithLabelValues(b.Name(), b.Name(), "EmitRow"),
		metricTotalRows:       bufferSinkTotalRowsCountCounter.WithLabelValues(b.Name(), b.Name()),
	}

	for exp := 0; exp < 9; exp++ {
		count := int(math.Pow(4, float64(exp)))
		s := newBufferSink(&benchSink{}, 5, make(chan drawbackMsg))
		s.flushTsChan = make(chan flushMsg, count)
		for i := 0; i < count; i++ {
			s.buffer[uint64(i)] = []*model.RawKVEntry{}
		}
		b.ResetTimer()

		b.Run(fmt.Sprintf("%d keyspan(s)", count), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				for j := 0; j < count; j++ {
					s.flushTsChan <- flushMsg{keyspanID: uint64(0)}
				}
				for len(s.flushTsChan) != 0 {
					keepRun, err := s.runOnce(ctx, &state)
					if err != nil || !keepRun {
						b.Fatal(keepRun, err)
					}
				}
			}
		})
	}
}
