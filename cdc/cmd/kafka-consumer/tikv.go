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

package main

import (
	"context"
	"math"
	"net/url"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/tikv/migration/cdc/cdc/model"
	"github.com/tikv/migration/cdc/cdc/sink"
	"github.com/tikv/migration/cdc/pkg/config"

	"github.com/tikv/client-go/v2/rawkv"
	pd "github.com/tikv/pd/client"
)

const (
	defaultPDErrorRetry        int    = math.MaxInt
	defaultTiKVBatchBytesLimit uint64 = 40 * 1024 * 1024 // 40MB
)

var _ sink.Sink = (*tikvSimpleSink)(nil)

// tikvSimpleSink is a sink that sends events to downstream TiKV cluster.
// The reason why we need this sink other than `cdc/sink/tikv.tikvSink` is that we need Kafka message offset to handle TiKV errors, which is not provided by `tikvSink`.
type tikvSimpleSink struct {
	client  *rawkv.Client
	batcher *sink.TikvBatcher
}

func newSimpleTiKVSink(ctx context.Context, sinkURI *url.URL, _ *config.ReplicaConfig, opts map[string]string, _ chan error) (*tikvSimpleSink, error) {
	config, pdAddrs, err := sink.ParseTiKVUri(sinkURI, opts)
	if err != nil {
		return nil, errors.Trace(err)
	}

	client, err := rawkv.NewClientWithOpts(ctx, pdAddrs,
		rawkv.WithSecurity(config.Security),
		rawkv.WithAPIVersion(kvrpcpb.APIVersion_V2),
		rawkv.WithPDOptions(pd.WithMaxErrorRetry(defaultPDErrorRetry)),
	)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &tikvSimpleSink{
		client:  client,
		batcher: sink.NewTiKVBatcher(nil),
	}, nil
}

func (s *tikvSimpleSink) EmitChangedEvents(ctx context.Context, rawKVEntries ...*model.RawKVEntry) error {
	s.batcher.Reset()

	flushToTiKV := func() error {
		if s.batcher.IsEmpty() {
			return nil
		}

		var err error
		for _, batch := range s.batcher.Batches {
			if batch.OpType == model.OpTypePut {
				err = s.client.BatchPutWithTTL(ctx, batch.Keys, batch.Values, batch.TTLs)
			} else if batch.OpType == model.OpTypeDelete {
				err = s.client.BatchDelete(ctx, batch.Keys)
			} else {
				err = errors.Errorf("unexpected OpType: %v", batch.OpType)
			}
			if err != nil {
				return errors.Trace(err)
			}
		}
		s.batcher.Reset()
		return nil
	}

	for _, entry := range rawKVEntries {
		err := s.batcher.Append(entry)
		if err != nil {
			return errors.Trace(err)
		}

		if s.batcher.ByteSize() >= defaultTiKVBatchBytesLimit {
			if err := flushToTiKV(); err != nil {
				return errors.Trace(err)
			}
		}
	}

	return errors.Trace(flushToTiKV())
}

func (s *tikvSimpleSink) FlushChangedEvents(ctx context.Context, _ model.KeySpanID, resolvedTs uint64) (uint64, error) {
	return resolvedTs, nil
}

func (s *tikvSimpleSink) EmitCheckpointTs(ctx context.Context, ts uint64) error {
	return nil
}

func (s *tikvSimpleSink) Close(ctx context.Context) error {
	s.batcher.Reset()
	return errors.Trace(s.client.Close())
}

func (s *tikvSimpleSink) Barrier(ctx context.Context, keyspanID model.KeySpanID) error {
	return nil
}

func registerSimpleTiKVSink(schema string) {
	initFunc := func(ctx context.Context, changefeedID model.ChangeFeedID, sinkURI *url.URL,
		config *config.ReplicaConfig, opts map[string]string, errCh chan error,
	) (sink.Sink, error) {
		return newSimpleTiKVSink(ctx, sinkURI, config, opts, errCh)
	}
	checkerFunc := func(ctx context.Context, changefeedID model.ChangeFeedID, sinkURI *url.URL,
		config *config.ReplicaConfig, opts map[string]string, errCh chan error,
	) (sink.Sink, error) {
		_, _, err := sink.ParseTiKVUri(sinkURI, opts)
		return nil, err
	}
	sink.RegisterSink(schema, initFunc, checkerFunc)
}
