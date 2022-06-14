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

package sink

import (
	"context"
	"sync/atomic"

	"github.com/pingcap/log"
	"github.com/tikv/migration/cdc/cdc/model"
	"go.uber.org/zap"
)

// newBlackHoleSink creates a block hole sink
func newBlackHoleSink(ctx context.Context, opts map[string]string) *blackHoleSink {
	return &blackHoleSink{
		statistics: NewStatistics(ctx, "blackhole", opts),
	}
}

type blackHoleSink struct {
	statistics      *Statistics
	accumulated     uint64
	lastAccumulated uint64
}

func (b *blackHoleSink) EmitChangedEvents(ctx context.Context, rawKVEntries ...*model.RawKVEntry) error {
	for _, rawKVEntry := range rawKVEntries {
		log.Debug("BlockHoleSink: EmitRowChangedEvents", zap.Any("row", rawKVEntry))
	}
	rowsCount := len(rawKVEntries)
	atomic.AddUint64(&b.accumulated, uint64(rowsCount))
	b.statistics.AddRowsCount(rowsCount)
	return nil
}

func (b *blackHoleSink) FlushChangedEvents(ctx context.Context, _ model.KeySpanID, resolvedTs uint64) (uint64, error) {
	log.Debug("BlockHoleSink: FlushRowChangedEvents", zap.Uint64("resolvedTs", resolvedTs))
	err := b.statistics.RecordBatchExecution(func() (int, error) {
		// TODO: add some random replication latency
		accumulated := atomic.LoadUint64(&b.accumulated)
		batchSize := accumulated - b.lastAccumulated
		b.lastAccumulated = accumulated
		return int(batchSize), nil
	})
	b.statistics.PrintStatus(ctx)
	return resolvedTs, err
}

func (b *blackHoleSink) EmitCheckpointTs(ctx context.Context, ts uint64) error {
	log.Debug("BlockHoleSink: Checkpoint Event", zap.Uint64("ts", ts))
	return nil
}

func (b *blackHoleSink) Close(ctx context.Context) error {
	return nil
}

func (b *blackHoleSink) Barrier(ctx context.Context, keyspanID model.KeySpanID) error {
	return nil
}
