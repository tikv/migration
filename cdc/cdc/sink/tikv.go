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
	"net/url"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/twmb/murmur3"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	tikvconfig "github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/rawkv"
	"github.com/tikv/migration/cdc/cdc/model"
	"github.com/tikv/migration/cdc/pkg/config"
	"github.com/tikv/migration/cdc/pkg/notify"
)

const (
	defaultConcurrency       uint32 = 4
	defaultTikvByteSizeLimit int64  = 4 * 1024 * 1024 // 4MB
)

type tikvSink struct {
	workerNum   uint32
	workerInput []chan struct {
		rawKVEntry *model.RawKVEntry
		resolvedTs uint64
	}
	workerResolvedTs []uint64
	checkpointTs     uint64
	resolvedNotifier *notify.Notifier
	resolvedReceiver *notify.Receiver

	config *tikvconfig.Config
	pdAddr []string
	opts   map[string]string

	statistics *Statistics
}

func createTiKVSink(
	ctx context.Context,
	config *tikvconfig.Config,
	pdAddr []string,
	opts map[string]string,
	errCh chan error,
) (*tikvSink, error) {
	workerNum := defaultConcurrency
	if s, ok := opts["concurrency"]; ok {
		c, _ := strconv.Atoi(s)
		workerNum = uint32(c)
	}
	workerInput := make([]chan struct {
		rawKVEntry *model.RawKVEntry
		resolvedTs uint64
	}, workerNum)
	for i := 0; i < int(workerNum); i++ {
		workerInput[i] = make(chan struct {
			rawKVEntry *model.RawKVEntry
			resolvedTs uint64
		}, 12800)
	}

	notifier := new(notify.Notifier)
	resolvedReceiver, err := notifier.NewReceiver(50 * time.Millisecond)
	if err != nil {
		return nil, err
	}
	k := &tikvSink{
		workerNum:        workerNum,
		workerInput:      workerInput,
		workerResolvedTs: make([]uint64, workerNum),
		resolvedNotifier: notifier,
		resolvedReceiver: resolvedReceiver,

		config: config,
		pdAddr: pdAddr,
		opts:   opts,

		statistics: NewStatistics(ctx, "TiKVSink", opts),
	}

	go func() {
		if err := k.run(ctx); err != nil && errors.Cause(err) != context.Canceled {
			select {
			case <-ctx.Done():
				return
			case errCh <- err:
			default:
				log.Error("error channel is full", zap.Error(err))
			}
		}
	}()
	return k, nil
}

func (k *tikvSink) dispatch(entry *model.RawKVEntry) uint32 {
	hasher := murmur3.New32()
	hasher.Write(entry.Key)
	return hasher.Sum32() % k.workerNum
}

func (k *tikvSink) EmitChangedEvents(ctx context.Context, rawKVEntries ...*model.RawKVEntry) error {
	// log.Debug("(rawkv)tikvSink::EmitRowChangedEvents", zap.Any("events", events))
	rowsCount := 0
	for _, rawKVEntry := range rawKVEntries {
		workerIdx := k.dispatch(rawKVEntry)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case k.workerInput[workerIdx] <- struct {
			rawKVEntry *model.RawKVEntry
			resolvedTs uint64
		}{rawKVEntry: rawKVEntry}:
		}
		rowsCount++
	}
	k.statistics.AddRowsCount(rowsCount)
	return nil
}

func (k *tikvSink) FlushChangedEvents(ctx context.Context, keyspanID model.KeySpanID, resolvedTs uint64) (uint64, error) {
	log.Debug("(rawkv)tikvSink::FlushRowChangedEvents", zap.Uint64("resolvedTs", resolvedTs), zap.Uint64("checkpointTs", k.checkpointTs))
	if resolvedTs <= k.checkpointTs {
		return k.checkpointTs, nil
	}

	for i := 0; i < int(k.workerNum); i++ {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case k.workerInput[i] <- struct {
			rawKVEntry *model.RawKVEntry
			resolvedTs uint64
		}{resolvedTs: resolvedTs}:
		}
	}

	// waiting for all row events are sent to TiKV
flushLoop:
	for {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-k.resolvedReceiver.C:
			for i := 0; i < int(k.workerNum); i++ {
				if resolvedTs > atomic.LoadUint64(&k.workerResolvedTs[i]) {
					continue flushLoop
				}
			}
			break flushLoop
		}
	}
	k.checkpointTs = resolvedTs
	k.statistics.PrintStatus(ctx)
	return k.checkpointTs, nil
}

func (k *tikvSink) EmitCheckpointTs(ctx context.Context, ts uint64) error {
	return nil
}

func (k *tikvSink) EmitDDLEvent(ctx context.Context, ddl *model.DDLEvent) error {
	return nil
}

// Initialize registers Avro schemas for all tables
func (k *tikvSink) Initialize(ctx context.Context, tableInfo []*model.SimpleTableInfo) error {
	// No longer need it for now
	return nil
}

func (k *tikvSink) Close(ctx context.Context) error {
	return nil
}

func (k *tikvSink) Barrier(cxt context.Context, keyspanID model.KeySpanID) error {
	// Barrier does nothing because FlushRowChangedEvents in mq sink has flushed
	// all buffered events forcedlly.
	return nil
}

func (k *tikvSink) run(ctx context.Context) error {
	defer k.resolvedReceiver.Stop()
	wg, ctx := errgroup.WithContext(ctx)
	for i := uint32(0); i < k.workerNum; i++ {
		workerIdx := i
		wg.Go(func() error {
			return k.runWorker(ctx, workerIdx)
		})
	}
	return wg.Wait()
}

type innerBatch struct {
	OpType model.OpType
	Keys   [][]byte
	Values [][]byte
}

type tikvBatcher struct {
	Batches  map[model.OpType]*innerBatch
	count    int
	byteSize int64
}

func (b *tikvBatcher) Count() int {
	return b.count
}

func (b *tikvBatcher) ByteSize() int64 {
	return b.byteSize
}

func (b *tikvBatcher) Append(entry *model.RawKVEntry) {
	log.Debug("(rawkv)tikvBatch::Append", zap.Any("event", entry))

	opType := entry.OpType
	_, ok := b.Batches[opType]
	if !ok {
		b.Batches[opType] = &innerBatch{
			OpType: opType,
			Keys:   [][]byte{entry.Key},
			Values: [][]byte{entry.Value},
		}
	}
	b.Batches[opType].Keys = append(b.Batches[opType].Keys, entry.Key[1:])
	b.Batches[opType].Values = append(b.Batches[opType].Values, entry.Value[1:])

	b.count += 1
	b.byteSize += int64(len(entry.Key) + len(entry.Value))
}

func (b *tikvBatcher) Reset() {
	b.Batches = map[model.OpType]*innerBatch{}
	b.count = 0
	b.byteSize = 0
}

func (k *tikvSink) runWorker(ctx context.Context, workerIdx uint32) error {
	log.Info("(rawkv)tikvSink worker start", zap.Uint32("workerIdx", workerIdx))
	input := k.workerInput[workerIdx]

	cli, err := rawkv.NewClient(ctx, k.pdAddr, k.config.Security)
	if err != nil {
		return err
	}
	defer cli.Close()

	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()

	batcher := tikvBatcher{
		Batches: map[model.OpType]*innerBatch{},
	}

	flushToTiKV := func() error {
		return k.statistics.RecordBatchExecution(func() (int, error) {
			log.Debug("(rawkv)tikvSink::flushToTiKV", zap.Any("batches", batcher.Batches))
			thisBatchSize := batcher.Count()
			if thisBatchSize == 0 {
				return 0, nil
			}

			for _, batch := range batcher.Batches {
				var err error
				if batch.OpType == model.OpTypePut {
					err = cli.BatchPut(ctx, batch.Keys, batch.Values, nil)
				} else if batch.OpType == model.OpTypeDelete {
					err = cli.BatchDelete(ctx, batch.Keys)
				}
				if err != nil {
					return 0, err
				}
				log.Debug("(rawkv)TiKVSink flushed", zap.Int("thisBatchSize", thisBatchSize), zap.Any("batch", batch))
			}
			batcher.Reset()
			return thisBatchSize, nil
		})
	}
	for {
		var e struct {
			rawKVEntry *model.RawKVEntry
			resolvedTs uint64
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
			if err := flushToTiKV(); err != nil {
				return errors.Trace(err)
			}
			continue
		case e = <-input:
		}
		if e.rawKVEntry == nil {
			if e.resolvedTs != 0 {
				log.Debug("(rawkv)tikvSink::runWorker push workerResolvedTs", zap.Uint32("workerIdx", workerIdx), zap.Uint64("event.resolvedTs", e.resolvedTs))
				if err := flushToTiKV(); err != nil {
					return errors.Trace(err)
				}

				atomic.StoreUint64(&k.workerResolvedTs[workerIdx], e.resolvedTs)
				k.resolvedNotifier.Notify()
			}
			continue
		}
		log.Debug("(rawkv)tikvSink::runWorker append event", zap.Uint32("workerIdx", workerIdx), zap.Any("event", e.rawKVEntry))
		batcher.Append(e.rawKVEntry)

		if batcher.ByteSize() >= defaultTikvByteSizeLimit {
			if err := flushToTiKV(); err != nil {
				return errors.Trace(err)
			}
		}
	}
}

func parseTiKVUri(sinkURI *url.URL, opts map[string]string) (*tikvconfig.Config, []string, error) {
	config := tikvconfig.DefaultConfig()

	var pdAddr []string
	if sinkURI.Opaque != "" {
		pdAddr = append(pdAddr, "http://"+sinkURI.Opaque)
	} else {
		pdAddr = append(pdAddr, "http://127.0.0.1:2379")
	}

	s := sinkURI.Query().Get("concurrency")
	if s != "" {
		_, err := strconv.Atoi(s)
		if err != nil {
			return nil, nil, err
		}
		opts["concurrency"] = s
	}

	return &config, pdAddr, nil
}

func newTiKVSink(ctx context.Context, sinkURI *url.URL, _ *config.ReplicaConfig, opts map[string]string, errCh chan error) (*tikvSink, error) {
	config, pdAddr, err := parseTiKVUri(sinkURI, opts)
	if err != nil {
		return nil, errors.Trace(err)
	}

	sink, err := createTiKVSink(ctx, config, pdAddr, opts, errCh)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return sink, nil
}
