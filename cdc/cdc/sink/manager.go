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
	"sync"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/migration/cdc/cdc/model"
	"go.uber.org/zap"
)

// Manager manages keyspan sinks, maintains the relationship between keyspan sinks
// and backendSink.
// Manager is thread-safe.
type Manager struct {
	bufSink                *bufferSink
	keyspanCheckpointTsMap sync.Map
	keyspanSinks           map[model.KeySpanID]*keyspanSink
	keyspanSinksMu         sync.Mutex
	changeFeedCheckpointTs uint64

	drawbackChan chan drawbackMsg

	captureAddr                   string
	changefeedID                  model.ChangeFeedID
	metricsKeySpanSinkTotalEvents prometheus.Counter
}

// NewManager creates a new Sink manager
func NewManager(
	ctx context.Context, backendSink Sink, errCh chan error, checkpointTs model.Ts,
	captureAddr string, changefeedID model.ChangeFeedID,
) *Manager {
	drawbackChan := make(chan drawbackMsg, 16)
	bufSink := newBufferSink(backendSink, checkpointTs, drawbackChan)
	go bufSink.run(ctx, errCh)
	return &Manager{
		bufSink:                       bufSink,
		changeFeedCheckpointTs:        checkpointTs,
		keyspanSinks:                  make(map[model.KeySpanID]*keyspanSink),
		drawbackChan:                  drawbackChan,
		captureAddr:                   captureAddr,
		changefeedID:                  changefeedID,
		metricsKeySpanSinkTotalEvents: keyspanSinkTotalEventsCountCounter.WithLabelValues(captureAddr, changefeedID),
	}
}

// CreateKeySpanSink creates a keyspan sink
func (m *Manager) CreateKeySpanSink(keyspanID model.KeySpanID, checkpointTs model.Ts) Sink {
	m.keyspanSinksMu.Lock()
	defer m.keyspanSinksMu.Unlock()
	if _, exist := m.keyspanSinks[keyspanID]; exist {
		log.Panic("the keyspan sink already exists", zap.Uint64("keyspanID", uint64(keyspanID)))
	}
	sink := &keyspanSink{
		keyspanID: keyspanID,
		manager:   m,
		buffer:    make([]*model.RawKVEntry, 0, 128),
	}
	m.keyspanSinks[keyspanID] = sink
	return sink
}

// Close closes the Sink manager and backend Sink, this method can be reentrantly called
func (m *Manager) Close(ctx context.Context) error {
	m.keyspanSinksMu.Lock()
	defer m.keyspanSinksMu.Unlock()
	keyspanSinkTotalEventsCountCounter.DeleteLabelValues(m.captureAddr, m.changefeedID)
	if m.bufSink != nil {
		return m.bufSink.Close(ctx)
	}
	return nil
}

func (m *Manager) flushBackendSink(ctx context.Context, keyspanID model.KeySpanID, resolvedTs uint64) (model.Ts, error) {
	checkpointTs, err := m.bufSink.FlushChangedEvents(ctx, keyspanID, resolvedTs)
	if err != nil {
		return m.getCheckpointTs(keyspanID), errors.Trace(err)
	}
	m.keyspanCheckpointTsMap.Store(keyspanID, checkpointTs)
	return checkpointTs, nil
}

func (m *Manager) destroyKeySpanSink(ctx context.Context, keyspanID model.KeySpanID) error {
	m.keyspanSinksMu.Lock()
	delete(m.keyspanSinks, keyspanID)
	m.keyspanSinksMu.Unlock()
	callback := make(chan struct{})
	select {
	case <-ctx.Done():
		return ctx.Err()
	case m.drawbackChan <- drawbackMsg{keyspanID: keyspanID, callback: callback}:
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-callback:
	}
	return m.bufSink.Barrier(ctx, keyspanID)
}

func (m *Manager) getCheckpointTs(keyspanID model.KeySpanID) uint64 {
	checkPoints, ok := m.keyspanCheckpointTsMap.Load(keyspanID)
	if ok {
		return checkPoints.(uint64)
	}
	// cannot find keyspan level checkpointTs because of no keyspan level resolvedTs flush task finished successfully,
	// for example: first time to flush resolvedTs but cannot get the flush lock, return changefeed level checkpointTs is safe
	return atomic.LoadUint64(&m.changeFeedCheckpointTs)
}

func (m *Manager) UpdateChangeFeedCheckpointTs(checkpointTs uint64) {
	atomic.StoreUint64(&m.changeFeedCheckpointTs, checkpointTs)
	if m.bufSink != nil {
		m.bufSink.UpdateChangeFeedCheckpointTs(checkpointTs)
	}
}

type drawbackMsg struct {
	keyspanID model.KeySpanID
	callback  chan struct{}
}
