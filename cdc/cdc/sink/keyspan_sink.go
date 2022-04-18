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

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/tikv/migration/cdc/cdc/model"
	"go.uber.org/zap"
)

type keyspanSink struct {
	keyspanID model.KeySpanID
	manager   *Manager
	buffer    []*model.RawKVEntry
}

var _ Sink = (*keyspanSink)(nil)

func (t *keyspanSink) EmitChangedEvents(ctx context.Context, rawKVEntries ...*model.RawKVEntry) error {
	t.buffer = append(t.buffer, rawKVEntries...)
	t.manager.metricsKeySpanSinkTotalEvents.Add(float64(len(rawKVEntries)))
	return nil
}

// FlushRowChangedEvents flushes sorted rows to sink manager, note the resolvedTs
// is required to be no more than global resolvedTs, keyspan barrierTs and keyspan
// redo log watermarkTs.
func (t *keyspanSink) FlushChangedEvents(ctx context.Context, keyspanID model.KeySpanID, resolvedTs uint64) (uint64, error) {
	if keyspanID != t.keyspanID {
		log.Panic("inconsistent keyspan sink",
			zap.Uint64("keyspanID", keyspanID), zap.Uint64("sinkKeySpanID", t.keyspanID))
	}
	resolvedRawKVEntries := t.buffer
	t.buffer = []*model.RawKVEntry{}

	err := t.manager.bufSink.EmitChangedEvents(ctx, resolvedRawKVEntries...)
	if err != nil {
		return t.manager.getCheckpointTs(keyspanID), errors.Trace(err)
	}
	return t.flushResolvedTs(ctx, resolvedTs)
}

func (t *keyspanSink) flushResolvedTs(ctx context.Context, resolvedTs uint64) (uint64, error) {
	return t.manager.flushBackendSink(ctx, t.keyspanID, resolvedTs)
}

func (t *keyspanSink) EmitCheckpointTs(ctx context.Context, ts uint64) error {
	// the keyspan sink doesn't receive the checkpoint event
	return nil
}

// Close once the method is called, no more events can be written to this keyspan sink
func (t *keyspanSink) Close(ctx context.Context) error {
	return t.manager.destroyKeySpanSink(ctx, t.keyspanID)
}

// Barrier is not used in keyspan sink
func (t *keyspanSink) Barrier(ctx context.Context, keyspanID model.KeySpanID) error {
	return nil
}
