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
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/tikv/migration/cdc/cdc/model"
	"github.com/tikv/migration/cdc/pkg/util/testleak"
)

type managerSuite struct{}

var _ = check.Suite(&managerSuite{})

type checkSink struct {
	*check.C
	entries        map[model.KeySpanID][]*model.RawKVEntry
	rowsMu         sync.Mutex
	lastResolvedTs map[model.KeySpanID]uint64
}

func newCheckSink(c *check.C) *checkSink {
	return &checkSink{
		C:              c,
		entries:        make(map[model.KeySpanID][]*model.RawKVEntry),
		lastResolvedTs: make(map[model.KeySpanID]uint64),
	}
}

func (c *checkSink) EmitChangedEvents(ctx context.Context, entries ...*model.RawKVEntry) error {
	c.rowsMu.Lock()
	defer c.rowsMu.Unlock()
	for _, entry := range entries {
		c.entries[entry.KeySpanID] = append(c.entries[entry.KeySpanID], entry)
	}
	return nil
}

func (c *checkSink) FlushChangedEvents(ctx context.Context, keyspanID model.KeySpanID, resolvedTs uint64) (uint64, error) {
	c.rowsMu.Lock()
	defer c.rowsMu.Unlock()
	var newEntries []*model.RawKVEntry
	entries := c.entries[keyspanID]
	newEntries = append(newEntries, entries...)

	c.Assert(c.lastResolvedTs[keyspanID], check.LessEqual, resolvedTs)
	c.lastResolvedTs[keyspanID] = resolvedTs
	c.entries[keyspanID] = newEntries

	return c.lastResolvedTs[keyspanID], nil
}

func (c *checkSink) EmitCheckpointTs(ctx context.Context, ts uint64) error {
	panic("unreachable")
}

func (c *checkSink) Close(ctx context.Context) error {
	return nil
}

func (c *checkSink) Barrier(ctx context.Context, keyspanID model.KeySpanID) error {
	return nil
}

func (s *managerSuite) TestManagerRandom(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 16)
	manager := NewManager(ctx, newCheckSink(c), errCh, 0, "", "")
	defer manager.Close(ctx)
	goroutineNum := 10
	rowNum := 100
	var wg sync.WaitGroup
	keyspanSinks := make([]Sink, goroutineNum)
	for i := 0; i < goroutineNum; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			keyspanSinks[i] = manager.CreateKeySpanSink(model.KeySpanID(i), 0)
		}()
	}
	wg.Wait()
	for i := 0; i < goroutineNum; i++ {
		i := i
		keyspanSink := keyspanSinks[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx := context.Background()
			var lastResolvedTs uint64
			for j := 1; j < rowNum; j++ {
				if rand.Intn(10) == 0 {
					resolvedTs := lastResolvedTs + uint64(rand.Intn(j-int(lastResolvedTs)))
					_, err := keyspanSink.FlushChangedEvents(ctx, model.KeySpanID(i), resolvedTs)
					c.Assert(err, check.IsNil)
					lastResolvedTs = resolvedTs
				} else {
					err := keyspanSink.EmitChangedEvents(ctx, &model.RawKVEntry{
						KeySpanID: uint64(i),
					})
					c.Assert(err, check.IsNil)
				}
			}
			_, err := keyspanSink.FlushChangedEvents(ctx, model.KeySpanID(i), uint64(rowNum))
			c.Assert(err, check.IsNil)
		}()
	}
	wg.Wait()
	cancel()
	time.Sleep(1 * time.Second)
	close(errCh)
	for err := range errCh {
		c.Assert(err, check.IsNil)
	}
}

func (s *managerSuite) TestManagerAddRemoveKeySpan(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 16)
	manager := NewManager(ctx, newCheckSink(c), errCh, 0, "", "")
	defer manager.Close(ctx)
	goroutineNum := 200
	var wg sync.WaitGroup
	const ExitSignal = uint64(math.MaxUint64)

	var maxResolvedTs uint64
	keyspanSinks := make([]Sink, 0, goroutineNum)
	keyspanCancels := make([]context.CancelFunc, 0, goroutineNum)
	runKeySpanSink := func(ctx context.Context, index uint64, sink Sink, startTs uint64) {
		defer wg.Done()
		lastResolvedTs := startTs
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			resolvedTs := atomic.LoadUint64(&maxResolvedTs)
			if resolvedTs == ExitSignal {
				return
			}
			if resolvedTs == lastResolvedTs {
				time.Sleep(10 * time.Millisecond)
				continue
			}
			for i := lastResolvedTs + 1; i <= resolvedTs; i++ {
				err := sink.EmitChangedEvents(ctx, &model.RawKVEntry{
					KeySpanID: index,
				})
				c.Assert(err, check.IsNil)
			}
			_, err := sink.FlushChangedEvents(ctx, sink.(*keyspanSink).keyspanID, resolvedTs)
			if err != nil {
				c.Assert(errors.Cause(err), check.Equals, context.Canceled)
			}
			lastResolvedTs = resolvedTs
		}
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		// add three keyspan and then remote one keyspan
		for i := 0; i < goroutineNum; i++ {
			if i%4 != 3 {
				// add keyspan
				keyspan := manager.CreateKeySpanSink(model.KeySpanID(i), maxResolvedTs)
				ctx, cancel := context.WithCancel(ctx)
				keyspanCancels = append(keyspanCancels, cancel)
				keyspanSinks = append(keyspanSinks, keyspan)

				atomic.AddUint64(&maxResolvedTs, 20)
				wg.Add(1)
				go runKeySpanSink(ctx, uint64(i), keyspan, maxResolvedTs)
			} else {
				// remove keyspan
				keyspan := keyspanSinks[0]
				// note when a keyspan is removed, no more data can be sent to the
				// backend sink, so we cancel the context of this keyspan sink.
				keyspanCancels[0]()
				c.Assert(keyspan.Close(ctx), check.IsNil)
				keyspanSinks = keyspanSinks[1:]
				keyspanCancels = keyspanCancels[1:]
			}
			time.Sleep(10 * time.Millisecond)
		}
		atomic.StoreUint64(&maxResolvedTs, ExitSignal)
	}()

	wg.Wait()
	cancel()
	time.Sleep(1 * time.Second)
	close(errCh)
	for err := range errCh {
		c.Assert(err, check.IsNil)
	}
}

func (s *managerSuite) TestManagerDestroyKeySpanSink(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 16)
	manager := NewManager(ctx, newCheckSink(c), errCh, 0, "", "")
	defer manager.Close(ctx)

	keyspanID := uint64(49)
	keyspanSink := manager.CreateKeySpanSink(keyspanID, 100)
	err := keyspanSink.EmitChangedEvents(ctx, &model.RawKVEntry{
		KeySpanID: keyspanID,
	})
	c.Assert(err, check.IsNil)
	_, err = keyspanSink.FlushChangedEvents(ctx, keyspanID, 110)
	c.Assert(err, check.IsNil)
	err = manager.destroyKeySpanSink(ctx, keyspanID)
	c.Assert(err, check.IsNil)
}

// Run the benchmark
// go test -benchmem -run='^$' -bench '^(BenchmarkManagerFlushing)$' github.com/tikv/migration/cdc/cdc/sink
func BenchmarkManagerFlushing(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 16)
	manager := NewManager(ctx, newCheckSink(nil), errCh, 0, "", "")

	// Init keyspan sinks.
	goroutineNum := 2000
	rowNum := 2000
	var wg sync.WaitGroup
	keyspanSinks := make([]Sink, goroutineNum)
	for i := 0; i < goroutineNum; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			keyspanSinks[i] = manager.CreateKeySpanSink(model.KeySpanID(i), 0)
		}()
	}
	wg.Wait()

	// Concurrent emit events.
	for i := 0; i < goroutineNum; i++ {
		i := i
		keyspanSink := keyspanSinks[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 1; j < rowNum; j++ {
				err := keyspanSink.EmitChangedEvents(context.Background(), &model.RawKVEntry{
					KeySpanID: uint64(i),
				})
				if err != nil {
					b.Error(err)
				}
			}
		}()
	}
	wg.Wait()

	// All keyspans are flushed concurrently, except keyspan 0.
	for i := 1; i < goroutineNum; i++ {
		i := i
		tblSink := keyspanSinks[i]
		go func() {
			for j := 1; j < rowNum; j++ {
				if j%2 == 0 {
					_, err := tblSink.FlushChangedEvents(context.Background(), tblSink.(*keyspanSink).keyspanID, uint64(j))
					if err != nil {
						b.Error(err)
					}
				}
			}
		}()
	}

	b.ResetTimer()
	// KeySpan 0 flush.
	tblSink := keyspanSinks[0]
	for i := 0; i < b.N; i++ {
		_, err := tblSink.FlushChangedEvents(context.Background(), tblSink.(*keyspanSink).keyspanID, uint64(rowNum))
		if err != nil {
			b.Error(err)
		}
	}
	b.StopTimer()

	cancel()
	_ = manager.Close(ctx)
	close(errCh)
	for err := range errCh {
		if err != nil {
			b.Error(err)
		}
	}
}

type errorSink struct {
	*check.C
}

func (e *errorSink) EmitChangedEvents(ctx context.Context, rows ...*model.RawKVEntry) error {
	return errors.New("error in emit row changed events")
}

func (e *errorSink) FlushChangedEvents(ctx context.Context, keyspanID model.KeySpanID, resolvedTs uint64) (uint64, error) {
	return 0, errors.New("error in flush row changed events")
}

func (e *errorSink) EmitCheckpointTs(ctx context.Context, ts uint64) error {
	panic("unreachable")
}

func (e *errorSink) Close(ctx context.Context) error {
	return nil
}

func (e *errorSink) Barrier(ctx context.Context, keyspanID model.KeySpanID) error {
	return nil
}

func (s *managerSuite) TestManagerError(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 16)
	manager := NewManager(ctx, &errorSink{C: c}, errCh, 0, "", "")
	defer manager.Close(ctx)
	sink := manager.CreateKeySpanSink(1, 0)
	err := sink.EmitChangedEvents(ctx, &model.RawKVEntry{
		KeySpanID: 1,
	})
	c.Assert(err, check.IsNil)
	_, err = sink.FlushChangedEvents(ctx, 1, 2)
	c.Assert(err, check.IsNil)
	err = <-errCh
	c.Assert(err.Error(), check.Equals, "error in emit row changed events")
}
