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
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/migration/cdc/cdc/model"
	"github.com/tikv/migration/cdc/pkg/util"
	"go.uber.org/zap"
)

const maxFlushBatchSize = 512

// bufferSink buffers emitted events and checkpoints and flush asynchronously.
// Note that it is a thread-safe Sink implementation.
type bufferSink struct {
	Sink
	changeFeedCheckpointTs uint64
	keyspanCheckpointTsMap sync.Map
	buffer                 map[model.KeySpanID][]*model.RawKVEntry
	bufferMu               sync.Mutex
	flushTsChan            chan flushMsg
	drawbackChan           chan drawbackMsg
}

var _ Sink = (*bufferSink)(nil)

func newBufferSink(
	backendSink Sink, checkpointTs model.Ts, drawbackChan chan drawbackMsg,
) *bufferSink {
	sink := &bufferSink{
		Sink: backendSink,
		// buffer shares the same flow control with keyspan sink
		buffer:                 make(map[model.KeySpanID][]*model.RawKVEntry),
		changeFeedCheckpointTs: checkpointTs,
		flushTsChan:            make(chan flushMsg, maxFlushBatchSize),
		drawbackChan:           drawbackChan,
	}
	return sink
}

type runState struct {
	batch [maxFlushBatchSize]flushMsg

	metricFlushDuration   prometheus.Observer
	metricEmitRowDuration prometheus.Observer
	metricTotalRows       prometheus.Counter
}

func (b *bufferSink) run(ctx context.Context, errCh chan error) {
	changefeedID := util.ChangefeedIDFromCtx(ctx)
	advertiseAddr := util.CaptureAddrFromCtx(ctx)
	state := runState{
		metricFlushDuration:   flushRowChangedDuration.WithLabelValues(advertiseAddr, changefeedID, "Flush"),
		metricEmitRowDuration: flushRowChangedDuration.WithLabelValues(advertiseAddr, changefeedID, "EmitRow"),
		metricTotalRows:       bufferSinkTotalRowsCountCounter.WithLabelValues(advertiseAddr, changefeedID),
	}
	defer func() {
		flushRowChangedDuration.DeleteLabelValues(advertiseAddr, changefeedID, "Flush")
		flushRowChangedDuration.DeleteLabelValues(advertiseAddr, changefeedID, "EmitRow")
		bufferSinkTotalRowsCountCounter.DeleteLabelValues(advertiseAddr, changefeedID)
	}()

	for {
		keepRun, err := b.runOnce(ctx, &state)
		if err != nil && errors.Cause(err) != context.Canceled {
			errCh <- err
			return
		}
		if !keepRun {
			return
		}
	}
}

func (b *bufferSink) runOnce(ctx context.Context, state *runState) (bool, error) {
	batchSize, batch := 0, state.batch
	push := func(event flushMsg) {
		batch[batchSize] = event
		batchSize++
	}
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case drawback := <-b.drawbackChan:
		b.bufferMu.Lock()
		delete(b.buffer, drawback.keyspanID)
		b.bufferMu.Unlock()
		close(drawback.callback)
	case event := <-b.flushTsChan:
		push(event)
	RecvBatch:
		for batchSize < maxFlushBatchSize {
			select {
			case event := <-b.flushTsChan:
				push(event)
			default:
				break RecvBatch
			}
		}
	}

	b.bufferMu.Lock()
	startEmit := time.Now()
	// find all rows before resolvedTs and emit to backend sink
	for i := 0; i < batchSize; i++ {
		keyspanID, resolvedTs := batch[i].keyspanID, batch[i].resolvedTs
		rawKVEntries := b.buffer[keyspanID]

		i := sort.Search(len(rawKVEntries), func(i int) bool {
			return rawKVEntries[i].CRTs > resolvedTs
		})
		if i == 0 {
			continue
		}
		state.metricTotalRows.Add(float64(i))

		err := b.Sink.EmitChangedEvents(ctx, rawKVEntries...)
		if err != nil {
			b.bufferMu.Unlock()
			return false, errors.Trace(err)
		}
		// put remaining rawKVEntries back to buffer
		// append to a new, fixed slice to avoid lazy GC
		b.buffer[keyspanID] = append(make([]*model.RawKVEntry, 0, len(rawKVEntries[i:])), rawKVEntries[i:]...)
	}
	b.bufferMu.Unlock()
	state.metricEmitRowDuration.Observe(time.Since(startEmit).Seconds())

	startFlush := time.Now()
	for i := 0; i < batchSize; i++ {
		keyspanID, resolvedTs := batch[i].keyspanID, batch[i].resolvedTs
		checkpointTs, err := b.Sink.FlushChangedEvents(ctx, keyspanID, resolvedTs)
		if err != nil {
			return false, errors.Trace(err)
		}
		b.keyspanCheckpointTsMap.Store(keyspanID, checkpointTs)
	}
	now := time.Now()
	state.metricFlushDuration.Observe(now.Sub(startFlush).Seconds())
	if now.Sub(startEmit) > time.Second {
		log.Warn("flush row changed events too slow",
			zap.Int("batchSize", batchSize),
			zap.Duration("duration", now.Sub(startEmit)),
			util.ZapFieldChangefeed(ctx))
	}

	return true, nil
}

func (b *bufferSink) EmitChangedEvents(ctx context.Context, rawKVEntries ...*model.RawKVEntry) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		if len(rawKVEntries) == 0 {
			return nil
		}
		keyspanID := rawKVEntries[0].KeySpanID
		b.bufferMu.Lock()
		b.buffer[keyspanID] = append(b.buffer[keyspanID], rawKVEntries...)
		b.bufferMu.Unlock()
	}
	return nil
}

func (b *bufferSink) FlushChangedEvents(ctx context.Context, keyspanID model.KeySpanID, resolvedTs uint64) (uint64, error) {
	select {
	case <-ctx.Done():
		return b.getKeySpanCheckpointTs(keyspanID), ctx.Err()
	case b.flushTsChan <- flushMsg{
		keyspanID:  keyspanID,
		resolvedTs: resolvedTs,
	}:
	}
	return b.getKeySpanCheckpointTs(keyspanID), nil
}

type flushMsg struct {
	keyspanID  model.KeySpanID
	resolvedTs uint64
}

func (b *bufferSink) getKeySpanCheckpointTs(keyspanID model.KeySpanID) uint64 {
	checkPoints, ok := b.keyspanCheckpointTsMap.Load(keyspanID)
	if ok {
		return checkPoints.(uint64)
	}
	return atomic.LoadUint64(&b.changeFeedCheckpointTs)
}

// UpdateChangeFeedCheckpointTs update the changeFeedCheckpointTs every processor tick
func (b *bufferSink) UpdateChangeFeedCheckpointTs(checkpointTs uint64) {
	atomic.StoreUint64(&b.changeFeedCheckpointTs, checkpointTs)
}
