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
	log.Debug("tikvSink::FlushRowChangedEvents", zap.Uint64("resolvedTs", resolvedTs), zap.Uint64("checkpointTs", k.checkpointTs))

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
	TTL    []uint64
}

type tikvBatcher struct {
	Batches        map[model.OpType]*innerBatch
	BatchesWithTTL map[model.OpType]*innerBatch
	count          int
	byteSize       int64
}

func NewTiKVBatcher() *tikvBatcher {
	tikvBatcher := &tikvBatcher{}
	tikvBatcher.Batches[model.OpTypePut] = &innerBatch{
		OpType: model.OpTypePut,
		Keys:   [][]byte{},
		Values: [][]byte{},
	}
	tikvBatcher.Batches[model.OpTypeDelete] = &innerBatch{
		OpType: model.OpTypePut,
		Keys:   [][]byte{},
		Values: [][]byte{},
	}
	tikvBatcher.BatchesWithTTL[model.OpTypePut] = &innerBatch{
		OpType: model.OpTypePut,
		Keys:   [][]byte{},
		Values: [][]byte{},
	}
	return tikvBatcher
}

func (b *tikvBatcher) Count() int {
	return b.count
}

func (b *tikvBatcher) ByteSize() int64 {
	return b.byteSize
}

func (b *tikvBatcher) Append(entry *model.RawKVEntry) {
	log.Debug("tikvBatch::Append", zap.Any("event", entry))

	var batcher *innerBatch
	var ok bool
	if entry.ExpiredTs == 0 {
		batcher, ok = b.Batches[entry.OpType]
		if !ok {
			log.Info("Unknow operate type, support put and delete", zap.Int("OpType", int(entry.OpType)))
			return
		}
	} else {
		batcher, ok = b.BatchesWithTTL[entry.OpType]
		if !ok {
			log.Info("Unknow operate type, only support put", zap.Int("OpType", int(entry.OpType)))
			return
		}
		// Maybe lose precision
		batcher.TTL = append(batcher.TTL, entry.ExpiredTs-uint64(time.Now().Unix()))
	}

	key, value := entry.Key[1:], entry.Value
	batcher.Keys = append(batcher.Keys, key)
	batcher.Values = append(batcher.Values, value)

	b.count += 1
	b.byteSize += int64(len(key) + len(value))
}

func (b *tikvBatcher) Reset() {
	b.Batches[model.OpTypePut] = &innerBatch{
		OpType: model.OpTypePut,
		Keys:   [][]byte{},
		Values: [][]byte{},
	}
	b.Batches[model.OpTypeDelete] = &innerBatch{
		OpType: model.OpTypePut,
		Keys:   [][]byte{},
		Values: [][]byte{},
	}
	b.BatchesWithTTL[model.OpTypePut] = &innerBatch{
		OpType: model.OpTypePut,
		Keys:   [][]byte{},
		Values: [][]byte{},
	}
	b.count = 0
	b.byteSize = 0
}

func (k *tikvSink) runWorker(ctx context.Context, workerIdx uint32) error {
	log.Info("tikvSink worker start", zap.Uint32("workerIdx", workerIdx))
	input := k.workerInput[workerIdx]

	cli, err := rawkv.NewClient(ctx, k.pdAddr, k.config.Security)
	if err != nil {
		return err
	}
	defer cli.Close()

	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()

	batcher := NewTiKVBatcher()

	flushToTiKV := func() error {
		return k.statistics.RecordBatchExecution(func() (int, error) {
			log.Debug("tikvSink::flushToTiKV", zap.Any("batches", batcher.Batches))
			thisBatchSize := batcher.Count()
			if thisBatchSize == 0 {
				return 0, nil
			}

			var err error
			for _, batch := range batcher.Batches {
				if batch.OpType == model.OpTypePut {
					err = cli.BatchPut(ctx, batch.Keys, batch.Values, nil)
				} else if batch.OpType == model.OpTypeDelete {
					err = cli.BatchDelete(ctx, batch.Keys)
				}
				if err != nil {
					return 0, err
				}
			}

			for _, batch := range batcher.BatchesWithExpiredTs {
				err = cli.BatchPutWithTTL(ctx, batch.Keys, batch.Values, batch.TTL)
			}
			log.Debug("TiKVSink flushed", zap.Int("thisBatchSize", thisBatchSize))
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
				log.Debug("tikvSink::runWorker push workerResolvedTs", zap.Uint32("workerIdx", workerIdx), zap.Uint64("event.resolvedTs", e.resolvedTs))
				if err := flushToTiKV(); err != nil {
					return errors.Trace(err)
				}

				atomic.StoreUint64(&k.workerResolvedTs[workerIdx], e.resolvedTs)
				k.resolvedNotifier.Notify()
			}
			continue
		}
		log.Debug("tikvSink::runWorker append event", zap.Uint32("workerIdx", workerIdx), zap.Any("event", e.rawKVEntry))
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
