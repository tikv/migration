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
	"net/url"
	"strings"

	"github.com/tikv/migration/cdc/cdc/model"
	"github.com/tikv/migration/cdc/pkg/config"
	cerror "github.com/tikv/migration/cdc/pkg/errors"
)

// Sink options keys
const (
	OptChangefeedID = "_changefeed_id"
	OptCaptureAddr  = "_capture_addr"
)

// Sink is an abstraction for anything that a changefeed may emit into.
type Sink interface {
	// EmitChangedEvents sends Changed Event to Sink
	// EmitChangedEvents may write rows to downstream directly;
	//
	// EmitChangedEvents is thread-safe.
	// FIXME: some sink implementation, they should be.
	EmitChangedEvents(ctx context.Context, rawKVEntries ...*model.RawKVEntry) error

	// FlushChangedEvents flushes each row which of commitTs less than or
	// equal to `resolvedTs` into downstream.
	// TiKV-CDC guarantees that all the Events whose commitTs is less than or
	// equal to `resolvedTs` are sent to Sink through `EmitRowChangedEvents`
	//
	// FlushChangedEvents is thread-safe.
	// FIXME: some sink implementation, they should be.
	FlushChangedEvents(ctx context.Context, keyspanID model.KeySpanID, resolvedTs uint64) (uint64, error)

	// EmitCheckpointTs sends CheckpointTs to Sink.
	// TiCDC guarantees that all Events **in the cluster** which of commitTs
	// less than or equal `checkpointTs` are sent to downstream successfully.
	//
	// EmitCheckpointTs is thread-safe.
	// FIXME: some sink implementation, they should be.
	EmitCheckpointTs(ctx context.Context, ts uint64) error

	// Close closes the Sink.
	//
	// Close is thread-safe and idempotent.
	Close(ctx context.Context) error

	// Barrier is a synchronous function to wait all events to be flushed
	// in underlying sink.
	// Note once Barrier is called, the resolved ts won't be pushed until
	// the Barrier call returns.
	//
	// Barrier is thread-safe.
	Barrier(ctx context.Context, keyspanID model.KeySpanID) error
}

var sinkIniterMap = make(map[string]sinkInitFunc)
var sinkUriCheckerMap = make(map[string]sinkInitFunc)

type sinkInitFunc func(context.Context, model.ChangeFeedID, *url.URL, *config.ReplicaConfig, map[string]string, chan error) (Sink, error)

func init() {
	// register blackhole sink
	sinkIniterMap["blackhole"] = func(ctx context.Context, changefeedID model.ChangeFeedID, sinkURI *url.URL,
		config *config.ReplicaConfig, opts map[string]string, errCh chan error,
	) (Sink, error) {
		return newBlackHoleSink(ctx, opts), nil
	}

	// register tikv sink
	sinkIniterMap["tikv"] = func(ctx context.Context, changefeedID model.ChangeFeedID, sinkURI *url.URL,
		config *config.ReplicaConfig, opts map[string]string, errCh chan error,
	) (Sink, error) {
		return newTiKVSink(ctx, sinkURI, config, opts, errCh)
	}
	sinkUriCheckerMap["tikv"] = func(ctx context.Context, changefeedID model.ChangeFeedID, sinkURI *url.URL,
		config *config.ReplicaConfig, opts map[string]string, errCh chan error,
	) (Sink, error) {
		_, _, err := parseTiKVUri(sinkURI, opts)
		return nil, err
	}

	// register kafka sink
	sinkIniterMap["kafka"] = func(ctx context.Context, changefeedID model.ChangeFeedID, sinkURI *url.URL,
		config *config.ReplicaConfig, opts map[string]string, errCh chan error,
	) (Sink, error) {
		return newKafkaSaramaSink(ctx, sinkURI, config, opts, errCh)
	}
	sinkUriCheckerMap["kafka"] = func(ctx context.Context, changefeedID model.ChangeFeedID, sinkURI *url.URL,
		config *config.ReplicaConfig, opts map[string]string, errCh chan error,
	) (Sink, error) {
		_, _, err := parseKafkaSinkConfig(sinkURI, config, opts)
		return nil, err
	}
	sinkIniterMap["kafka+ssl"] = sinkIniterMap["kafka"]
	sinkUriCheckerMap["kafka+ssl"] = sinkUriCheckerMap["kafka"]
}

// New creates a new sink with the sink-uri
func New(ctx context.Context, changefeedID model.ChangeFeedID, sinkURIStr string, config *config.ReplicaConfig, opts map[string]string, errCh chan error) (Sink, error) {
	// parse sinkURI as a URI
	sinkURI, err := url.Parse(sinkURIStr)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrSinkURIInvalid, err)
	}
	if newSink, ok := sinkIniterMap[strings.ToLower(sinkURI.Scheme)]; ok {
		return newSink(ctx, changefeedID, sinkURI, config, opts, errCh)
	}

	return nil, cerror.ErrSinkURIInvalid.GenWithStack("the sink scheme (%s) is not supported", sinkURI.Scheme)
}

// Validate sink if given valid parameters.
func Validate(ctx context.Context, sinkURIStr string, cfg *config.ReplicaConfig, opts map[string]string) error {
	errCh := make(chan error)

	// parse sinkURI as a URI
	sinkURI, err := url.Parse(sinkURIStr)
	if err != nil {
		return cerror.WrapError(cerror.ErrSinkURIInvalid, err)
	}
	scheme := strings.ToLower(sinkURI.Scheme)
	newSink, ok := sinkUriCheckerMap[scheme]
	if !ok {
		newSink, ok = sinkIniterMap[scheme]
		if !ok {
			return cerror.ErrSinkURIInvalid.GenWithStack("the sink scheme (%s) is not supported", sinkURI.Scheme)
		}
	}

	s, err := newSink(ctx, "sink-verify", sinkURI, cfg, opts, errCh)
	if err != nil {
		return err
	}
	if s != nil {
		err = s.Close(ctx)
		if err != nil {
			return err
		}
	}
	select {
	case err = <-errCh:
		if err != nil {
			return err
		}
	default:
	}
	return nil
}
