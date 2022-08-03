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

package memory

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/tikv/migration/cdc/cdc/model"
	cerror "github.com/tikv/migration/cdc/pkg/errors"
	"github.com/tikv/migration/cdc/pkg/util/testleak"
)

type mockEntrySorterSuite struct{}

var _ = check.Suite(&mockEntrySorterSuite{})

func TestSuite(t *testing.T) {
	check.TestingT(t)
}

func (s *mockEntrySorterSuite) TestEntrySorter(c *check.C) {
	defer testleak.AfterTest(c)()
	testCases := []struct {
		input      []*model.RawKVEntry
		resolvedTs uint64
		expect     []*model.RawKVEntry
	}{
		{
			input: []*model.RawKVEntry{
				{Key: []byte("key1"), CRTs: 1, OpType: model.OpTypePut},
				{Key: []byte("key2"), CRTs: 2, OpType: model.OpTypePut},
				{Key: []byte("key1"), CRTs: 4, OpType: model.OpTypeDelete},
				{Key: []byte("key2"), CRTs: 2, OpType: model.OpTypeDelete},
			},
			resolvedTs: 0,
			expect: []*model.RawKVEntry{
				{CRTs: 0, OpType: model.OpTypeResolved},
			},
		},
		{
			input: []*model.RawKVEntry{
				{Key: []byte("key3"), CRTs: 3, OpType: model.OpTypePut},
				{Key: []byte("key4"), CRTs: 2, OpType: model.OpTypePut},
				{Key: []byte("key5"), CRTs: 5, OpType: model.OpTypePut},
			},
			resolvedTs: 3,
			expect: []*model.RawKVEntry{
				{Key: []byte("key1"), CRTs: 1, OpType: model.OpTypePut},
				{Key: []byte("key2"), CRTs: 2, OpType: model.OpTypePut},
				{Key: []byte("key2"), CRTs: 2, OpType: model.OpTypeDelete},
				{Key: []byte("key4"), CRTs: 2, OpType: model.OpTypePut},
				{Key: []byte("key3"), CRTs: 3, OpType: model.OpTypePut},
				{CRTs: 3, OpType: model.OpTypeResolved},
			},
		},
		{
			input:      []*model.RawKVEntry{},
			resolvedTs: 3,
			expect:     []*model.RawKVEntry{{CRTs: 3, OpType: model.OpTypeResolved}},
		},
		{
			input: []*model.RawKVEntry{
				{Key: []byte("key6"), CRTs: 7, OpType: model.OpTypePut},
			},
			resolvedTs: 6,
			expect: []*model.RawKVEntry{
				{Key: []byte("key1"), CRTs: 4, OpType: model.OpTypeDelete},
				{Key: []byte("key5"), CRTs: 5, OpType: model.OpTypePut},
				{CRTs: 6, OpType: model.OpTypeResolved},
			},
		},
		{
			input:      []*model.RawKVEntry{{Key: []byte("key3"), CRTs: 7, OpType: model.OpTypeDelete}},
			resolvedTs: 6,
			expect: []*model.RawKVEntry{
				{CRTs: 6, OpType: model.OpTypeResolved},
			},
		},
		{
			input:      []*model.RawKVEntry{{Key: []byte("key4"), CRTs: 7, OpType: model.OpTypeDelete}},
			resolvedTs: 8,
			expect: []*model.RawKVEntry{
				{Key: []byte("key6"), CRTs: 7, OpType: model.OpTypePut},
				{Key: []byte("key3"), CRTs: 7, OpType: model.OpTypeDelete},
				{Key: []byte("key4"), CRTs: 7, OpType: model.OpTypeDelete},
				{CRTs: 8, OpType: model.OpTypeResolved},
			},
		},
		{
			input:      []*model.RawKVEntry{},
			resolvedTs: 15,
			expect: []*model.RawKVEntry{
				{CRTs: 15, OpType: model.OpTypeResolved},
			},
		},
	}
	es := NewEntrySorter()
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := es.Run(ctx)
		c.Assert(errors.Cause(err), check.Equals, context.Canceled)
	}()
	for _, tc := range testCases {
		for _, entry := range tc.input {
			es.AddEntry(ctx, model.NewPolymorphicEvent(entry))
		}
		es.AddEntry(ctx, model.NewResolvedPolymorphicEvent(0, tc.resolvedTs, 0))
		for i := 0; i < len(tc.expect); i++ {
			e := <-es.Output()
			c.Check(e.RawKV, check.DeepEquals, tc.expect[i])
		}
	}
	cancel()
	wg.Wait()
}

func (s *mockEntrySorterSuite) TestEntrySorterNonBlocking(c *check.C) {
	defer testleak.AfterTest(c)()
	testCases := []struct {
		input      []*model.RawKVEntry
		resolvedTs uint64
		expect     []*model.RawKVEntry
	}{
		{
			input: []*model.RawKVEntry{
				{Key: []byte("key1"), CRTs: 1, OpType: model.OpTypePut},
				{Key: []byte("key2"), CRTs: 2, OpType: model.OpTypePut},
				{Key: []byte("key1"), CRTs: 4, OpType: model.OpTypeDelete},
				{Key: []byte("key2"), CRTs: 2, OpType: model.OpTypeDelete},
			},
			resolvedTs: 0,
			expect: []*model.RawKVEntry{
				{CRTs: 0, OpType: model.OpTypeResolved},
			},
		},
		{
			input: []*model.RawKVEntry{
				{Key: []byte("key3"), CRTs: 3, OpType: model.OpTypePut},
				{Key: []byte("key4"), CRTs: 2, OpType: model.OpTypePut},
				{Key: []byte("key5"), CRTs: 5, OpType: model.OpTypePut},
			},
			resolvedTs: 3,
			expect: []*model.RawKVEntry{
				{Key: []byte("key1"), CRTs: 1, OpType: model.OpTypePut},
				{Key: []byte("key2"), CRTs: 2, OpType: model.OpTypePut},
				{Key: []byte("key2"), CRTs: 2, OpType: model.OpTypeDelete},
				{Key: []byte("key4"), CRTs: 2, OpType: model.OpTypePut},
				{Key: []byte("key3"), CRTs: 3, OpType: model.OpTypePut},
				{CRTs: 3, OpType: model.OpTypeResolved},
			},
		},
		{
			input:      []*model.RawKVEntry{},
			resolvedTs: 3,
			expect:     []*model.RawKVEntry{{CRTs: 3, OpType: model.OpTypeResolved}},
		},
		{
			input: []*model.RawKVEntry{
				{Key: []byte("key6"), CRTs: 7, OpType: model.OpTypePut},
			},
			resolvedTs: 6,
			expect: []*model.RawKVEntry{
				{Key: []byte("key1"), CRTs: 4, OpType: model.OpTypeDelete},
				{Key: []byte("key5"), CRTs: 5, OpType: model.OpTypePut},
				{CRTs: 6, OpType: model.OpTypeResolved},
			},
		},
		{
			input:      []*model.RawKVEntry{{Key: []byte("key3"), CRTs: 7, OpType: model.OpTypeDelete}},
			resolvedTs: 6,
			expect: []*model.RawKVEntry{
				{CRTs: 6, OpType: model.OpTypeResolved},
			},
		},
		{
			input:      []*model.RawKVEntry{{Key: []byte("key4"), CRTs: 7, OpType: model.OpTypeDelete}},
			resolvedTs: 8,
			expect: []*model.RawKVEntry{
				{Key: []byte("key6"), CRTs: 7, OpType: model.OpTypePut},
				{Key: []byte("key3"), CRTs: 7, OpType: model.OpTypeDelete},
				{Key: []byte("key4"), CRTs: 7, OpType: model.OpTypeDelete},
				{CRTs: 8, OpType: model.OpTypeResolved},
			},
		},
		{
			input:      []*model.RawKVEntry{},
			resolvedTs: 15,
			expect: []*model.RawKVEntry{
				{CRTs: 15, OpType: model.OpTypeResolved},
			},
		},
	}
	es := NewEntrySorter()
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := es.Run(ctx)
		c.Assert(errors.Cause(err), check.Equals, context.Canceled)
	}()
	for _, tc := range testCases {
		for _, entry := range tc.input {
			added, err := es.TryAddEntry(ctx, model.NewPolymorphicEvent(entry))
			c.Assert(added, check.IsTrue)
			c.Assert(err, check.IsNil)
		}
		added, err := es.TryAddEntry(ctx, model.NewResolvedPolymorphicEvent(0, tc.resolvedTs, 0))
		c.Assert(added, check.IsTrue)
		c.Assert(err, check.IsNil)
		for i := 0; i < len(tc.expect); i++ {
			e := <-es.Output()
			c.Check(e.RawKV, check.DeepEquals, tc.expect[i])
		}
	}
	cancel()
	wg.Wait()
}

func (s *mockEntrySorterSuite) TestEntrySorterRandomly(c *check.C) {
	defer testleak.AfterTest(c)()
	es := NewEntrySorter()
	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := es.Run(ctx)
		c.Assert(errors.Cause(err), check.Equals, context.Canceled)
	}()

	maxTs := uint64(1000000)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for resolvedTs := uint64(1); resolvedTs <= maxTs; resolvedTs += 400 {
			var (
				opType1 model.OpType
				opType2 model.OpType
			)
			if rand.Intn(2) == 0 {
				opType1 = model.OpTypePut
				opType2 = model.OpTypePut
			} else {
				opType1 = model.OpTypePut
				opType2 = model.OpTypeDelete
			}
			CRTs := make([]uint64, 500)
			for i := 0; i < 500; i++ {
				CRTs[i] = uint64(int64(resolvedTs) + rand.Int63n(int64(maxTs-resolvedTs)))
				entry := &model.RawKVEntry{
					Key:    []byte(fmt.Sprintf("key%d-%d", resolvedTs, i)),
					Value:  []byte("value1"),
					CRTs:   CRTs[i],
					OpType: opType1,
				}
				es.AddEntry(ctx, model.NewPolymorphicEvent(entry))
			}
			for i := 0; i < 500; i++ {
				entry := &model.RawKVEntry{
					Key:    []byte(fmt.Sprintf("key%d-%d", resolvedTs, i)),
					Value:  []byte("value2"),
					CRTs:   CRTs[i],
					OpType: opType2,
				}
				es.AddEntry(ctx, model.NewPolymorphicEvent(entry))
			}
			es.AddEntry(ctx, model.NewResolvedPolymorphicEvent(0, resolvedTs, 0))
		}
		es.AddEntry(ctx, model.NewResolvedPolymorphicEvent(0, maxTs, 0))
	}()
	var (
		lastTs     uint64
		lastEntry  *model.PolymorphicEvent
		resolvedTs uint64
	)
	for entry := range es.Output() {
		c.Assert(entry.CRTs, check.GreaterEqual, lastTs)
		c.Assert(entry.CRTs, check.Greater, resolvedTs)
		lastTs = entry.CRTs
		if entry.RawKV.OpType == model.OpTypeResolved {
			resolvedTs = entry.CRTs
		} else {
			if lastEntry != nil && bytes.Equal(lastEntry.RawKV.Key, entry.RawKV.Key) {
				c.Assert(lastEntry.RawKV.Value, check.BytesEquals, []byte("value1"))
				c.Assert(entry.RawKV.Value, check.BytesEquals, []byte("value2"))
			}
			lastEntry = entry
		}
		if resolvedTs == maxTs {
			break
		}
	}
	cancel()
	wg.Wait()
}

func (s *mockEntrySorterSuite) TestEventLess(c *check.C) {
	defer testleak.AfterTest(c)()
	testCases := []struct {
		i        *model.PolymorphicEvent
		j        *model.PolymorphicEvent
		expected bool
	}{
		{
			&model.PolymorphicEvent{
				CRTs: 1,
			},
			&model.PolymorphicEvent{
				CRTs: 2,
			},
			true,
		},
		{
			&model.PolymorphicEvent{
				CRTs: 2,
				RawKV: &model.RawKVEntry{
					OpType: model.OpTypeDelete,
				},
			},
			&model.PolymorphicEvent{
				CRTs: 2,
				RawKV: &model.RawKVEntry{
					OpType: model.OpTypeDelete,
				},
			},
			false,
		},
		{
			&model.PolymorphicEvent{
				CRTs: 2,
				RawKV: &model.RawKVEntry{
					OpType: model.OpTypeResolved,
				},
			},
			&model.PolymorphicEvent{
				CRTs: 2,
				RawKV: &model.RawKVEntry{
					OpType: model.OpTypeResolved,
				},
			},
			false,
		},
		{
			&model.PolymorphicEvent{
				CRTs: 2,
				RawKV: &model.RawKVEntry{
					OpType: model.OpTypeResolved,
				},
			},
			&model.PolymorphicEvent{
				CRTs: 2,
				RawKV: &model.RawKVEntry{
					OpType: model.OpTypeDelete,
				},
			},
			false,
		},
		{
			&model.PolymorphicEvent{
				CRTs: 2,
				RawKV: &model.RawKVEntry{
					OpType: model.OpTypeDelete,
				},
			},
			&model.PolymorphicEvent{
				CRTs: 2,
				RawKV: &model.RawKVEntry{
					OpType: model.OpTypeResolved,
				},
			},
			true,
		},
		{
			&model.PolymorphicEvent{
				CRTs: 2,
				RawKV: &model.RawKVEntry{
					OpType: model.OpTypePut,
				},
			},
			&model.PolymorphicEvent{
				CRTs: 2,
				RawKV: &model.RawKVEntry{
					OpType: model.OpTypeResolved,
				},
			},
			true,
		},
		{
			&model.PolymorphicEvent{
				CRTs: 2,
				RawKV: &model.RawKVEntry{
					OpType: model.OpTypeResolved,
				},
			},
			&model.PolymorphicEvent{
				CRTs: 2,
				RawKV: &model.RawKVEntry{
					OpType: model.OpTypePut,
				},
			},
			false,
		},
		{
			&model.PolymorphicEvent{
				CRTs: 3,
				RawKV: &model.RawKVEntry{
					OpType: model.OpTypeDelete,
				},
			},
			&model.PolymorphicEvent{
				CRTs: 2,
				RawKV: &model.RawKVEntry{
					OpType: model.OpTypeResolved,
				},
			},
			false,
		},
	}

	for _, tc := range testCases {
		c.Assert(eventLess(tc.i, tc.j), check.Equals, tc.expected)
	}
}

func (s *mockEntrySorterSuite) TestMergeEvents(c *check.C) {
	defer testleak.AfterTest(c)()
	events1 := []*model.PolymorphicEvent{
		{
			CRTs: 1,
			RawKV: &model.RawKVEntry{
				OpType: model.OpTypeDelete,
			},
		},
		{
			CRTs: 2,
			RawKV: &model.RawKVEntry{
				OpType: model.OpTypePut,
			},
		},
		{
			CRTs: 3,
			RawKV: &model.RawKVEntry{
				OpType: model.OpTypePut,
			},
		},
		{
			CRTs: 4,
			RawKV: &model.RawKVEntry{
				OpType: model.OpTypePut,
			},
		},
		{
			CRTs: 5,
			RawKV: &model.RawKVEntry{
				OpType: model.OpTypeDelete,
			},
		},
	}
	events2 := []*model.PolymorphicEvent{
		{
			CRTs: 3,
			RawKV: &model.RawKVEntry{
				OpType: model.OpTypeResolved,
			},
		},
		{
			CRTs: 4,
			RawKV: &model.RawKVEntry{
				OpType: model.OpTypePut,
			},
		},
		{
			CRTs: 4,
			RawKV: &model.RawKVEntry{
				OpType: model.OpTypeResolved,
			},
		},
		{
			CRTs: 7,
			RawKV: &model.RawKVEntry{
				OpType: model.OpTypePut,
			},
		},
		{
			CRTs: 9,
			RawKV: &model.RawKVEntry{
				OpType: model.OpTypeDelete,
			},
		},
	}

	var outputResults []*model.PolymorphicEvent
	output := func(event *model.PolymorphicEvent) {
		outputResults = append(outputResults, event)
	}

	expectedResults := append(events1, events2...)
	sort.Slice(expectedResults, func(i, j int) bool {
		return eventLess(expectedResults[i], expectedResults[j])
	})

	mergeEvents(events1, events2, output)
	c.Assert(outputResults, check.DeepEquals, expectedResults)
}

func (s *mockEntrySorterSuite) TestEntrySorterClosed(c *check.C) {
	defer testleak.AfterTest(c)()
	es := NewEntrySorter()
	atomic.StoreInt32(&es.closed, 1)
	added, err := es.TryAddEntry(context.TODO(), model.NewResolvedPolymorphicEvent(0, 1, 0))
	c.Assert(added, check.IsFalse)
	c.Assert(cerror.ErrSorterClosed.Equal(err), check.IsTrue)
}

func BenchmarkSorter(b *testing.B) {
	es := NewEntrySorter()
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := es.Run(ctx)
		if errors.Cause(err) != context.Canceled {
			panic(errors.Annotate(err, "unexpected error"))
		}
	}()

	maxTs := uint64(10000000)
	b.ResetTimer()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for resolvedTs := uint64(1); resolvedTs <= maxTs; resolvedTs += 400 {
			var opType model.OpType
			if rand.Intn(2) == 0 {
				opType = model.OpTypePut
			} else {
				opType = model.OpTypeDelete
			}
			for i := 0; i < 100000; i++ {
				entry := &model.RawKVEntry{
					CRTs:   uint64(int64(resolvedTs) + rand.Int63n(1000)),
					OpType: opType,
				}
				es.AddEntry(ctx, model.NewPolymorphicEvent(entry))
			}
			es.AddEntry(ctx, model.NewResolvedPolymorphicEvent(0, resolvedTs, 0))
		}
		es.AddEntry(ctx, model.NewResolvedPolymorphicEvent(0, maxTs, 0))
	}()
	var resolvedTs uint64
	for entry := range es.Output() {
		if entry.RawKV.OpType == model.OpTypeResolved {
			resolvedTs = entry.CRTs
		}
		if resolvedTs == maxTs {
			break
		}
	}
	cancel()
	wg.Wait()
}
