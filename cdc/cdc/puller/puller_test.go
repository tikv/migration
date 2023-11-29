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

package puller

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/migration/cdc/cdc/kv"
	"github.com/tikv/migration/cdc/cdc/model"
	cerrors "github.com/tikv/migration/cdc/pkg/errors"
	"github.com/tikv/migration/cdc/pkg/regionspan"
	"github.com/tikv/migration/cdc/pkg/retry"
	"github.com/tikv/migration/cdc/pkg/security"
	"github.com/tikv/migration/cdc/pkg/txnutil"
	"github.com/tikv/migration/cdc/pkg/util/testleak"
	pd "github.com/tikv/pd/client"
)

type pullerSuite struct{}

var _ = check.Suite(&pullerSuite{})

func TestSuite(t *testing.T) {
	check.TestingT(t)
}

type mockPdClientForPullerTest struct {
	pd.Client
	clusterID uint64
}

func (mc *mockPdClientForPullerTest) GetClusterID(ctx context.Context) uint64 {
	return mc.clusterID
}

type mockCDCKVClient struct {
	expectations chan model.RegionFeedEvent
}

type mockInjectedPuller struct {
	Puller
	cli *mockCDCKVClient
}

func newMockCDCKVClient(
	ctx context.Context,
	pd pd.Client,
	kvStorage tikv.Storage,
	grpcPool kv.GrpcPool,
	regionCache *tikv.RegionCache,
) kv.CDCKVClient {
	return &mockCDCKVClient{
		expectations: make(chan model.RegionFeedEvent, 1024),
	}
}

func (mc *mockCDCKVClient) EventFeed(
	ctx context.Context,
	span regionspan.ComparableSpan,
	ts uint64,
	enableOldValue bool,
	lockResolver txnutil.LockResolver,
	isPullerInit kv.PullerInitialization,
	eventCh chan<- model.RegionFeedEvent,
) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ev, ok := <-mc.expectations:
			if !ok {
				return nil
			}
			eventCh <- ev
		}
	}
}

func (mc *mockCDCKVClient) Close() error {
	close(mc.expectations)
	if len(mc.expectations) > 0 {
		buf := bytes.NewBufferString("mockCDCKVClient: not all expectations were satisfied! Still waiting\n")
		for e := range mc.expectations {
			_, _ = buf.WriteString(fmt.Sprintf("%s", e.GetValue()))
		}
		return errors.New(buf.String())
	}
	return nil
}

func (mc *mockCDCKVClient) Returns(ev model.RegionFeedEvent) {
	mc.expectations <- ev
}

func (s *pullerSuite) newPullerForTest(
	c *check.C,
	spans []regionspan.Span,
	checkpointTs uint64,
) (*mockInjectedPuller, context.CancelFunc, *sync.WaitGroup, tikv.Storage) {
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	store, err := mockstore.NewMockStore()
	c.Assert(err, check.IsNil)

	tikvStorage, ok := store.(tikv.Storage)
	if !ok {
		panic("can't create puller for non-tikv storage")
	}

	enableOldValue := true
	backupNewCDCKVClient := kv.NewCDCKVClient
	kv.NewCDCKVClient = newMockCDCKVClient
	defer func() {
		kv.NewCDCKVClient = backupNewCDCKVClient
	}()
	pdCli := &mockPdClientForPullerTest{clusterID: uint64(1)}
	grpcPool := kv.NewGrpcPoolImpl(ctx, &security.Credential{})
	defer grpcPool.Close()
	regionCache := tikv.NewRegionCache(pdCli)
	defer regionCache.Close()
	plr := NewPuller(ctx, pdCli, grpcPool, regionCache, tikvStorage, checkpointTs, spans, enableOldValue)
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := plr.Run(ctx)
		if err != nil {
			c.Assert(errors.Cause(err), check.Equals, context.Canceled)
		}
	}()
	c.Assert(err, check.IsNil)
	mockPlr := &mockInjectedPuller{
		Puller: plr,
		cli:    plr.(*pullerImpl).kvCli.(*mockCDCKVClient),
	}
	return mockPlr, cancel, &wg, tikvStorage
}

func (s *pullerSuite) TestPullerResolvedForward(c *check.C) {
	defer testleak.AfterTest(c)()
	spans := []regionspan.Span{
		{Start: []byte("t_a"), End: []byte("t_e")},
	}
	checkpointTs := uint64(996)
	plr, cancel, wg, store := s.newPullerForTest(c, spans, checkpointTs)

	plr.cli.Returns(model.RegionFeedEvent{
		Resolved: &model.ResolvedSpan{
			Span:       regionspan.ToComparableSpan(regionspan.Span{Start: []byte("t_a"), End: []byte("t_c")}),
			ResolvedTs: uint64(1001),
		},
	})
	plr.cli.Returns(model.RegionFeedEvent{
		Resolved: &model.ResolvedSpan{
			Span:       regionspan.ToComparableSpan(regionspan.Span{Start: []byte("t_c"), End: []byte("t_d")}),
			ResolvedTs: uint64(1002),
		},
	})
	plr.cli.Returns(model.RegionFeedEvent{
		Resolved: &model.ResolvedSpan{
			Span:       regionspan.ToComparableSpan(regionspan.Span{Start: []byte("t_d"), End: []byte("t_e")}),
			ResolvedTs: uint64(1000),
		},
	})
	ev := <-plr.Output()
	c.Assert(ev.OpType, check.Equals, model.OpTypeResolved)
	c.Assert(ev.CRTs, check.Equals, uint64(1000))
	c.Assert(plr.IsInitialized(), check.IsTrue)
	err := retry.Do(context.Background(), func() error {
		ts := plr.GetResolvedTs()
		if ts != uint64(1000) {
			return errors.Errorf("resolved ts %d of puller does not forward to 1000", ts)
		}
		return nil
	}, retry.WithBackoffBaseDelay(10), retry.WithMaxTries(10), retry.WithIsRetryableErr(cerrors.IsRetryableError))

	c.Assert(err, check.IsNil)

	store.Close()
	cancel()
	wg.Wait()
}

func (s *pullerSuite) TestPullerRawKV(c *check.C) {
	defer testleak.AfterTest(c)()
	spans := []regionspan.Span{
		{Start: []byte("c"), End: []byte("e")},
	}
	checkpointTs := uint64(996)
	plr, cancel, wg, store := s.newPullerForTest(c, spans, checkpointTs)

	plr.cli.Returns(model.RegionFeedEvent{
		Val: &model.RawKVEntry{
			OpType: model.OpTypePut,
			Key:    []byte("a"),
			Value:  []byte("test-value"),
			CRTs:   uint64(1002),
		},
	})
	plr.cli.Returns(model.RegionFeedEvent{
		Val: &model.RawKVEntry{
			OpType: model.OpTypePut,
			Key:    []byte("d"),
			Value:  []byte("test-value"),
			CRTs:   uint64(1003),
		},
	})
	var ev *model.RawKVEntry
	ev = <-plr.Output()
	c.Assert(ev.OpType, check.Equals, model.OpTypePut)
	c.Assert(ev.Key, check.DeepEquals, []byte("a"))
	ev = <-plr.Output()
	c.Assert(ev.OpType, check.Equals, model.OpTypePut)
	c.Assert(ev.Key, check.DeepEquals, []byte("d"))

	store.Close()
	cancel()
	wg.Wait()
}

func (s *pullerSuite) TestAssignSequence(c *check.C) {
	defer testleak.AfterTest(c)()
	spans := []regionspan.Span{
		{Start: []byte("t_a"), End: []byte("t_z")},
	}
	checkpointTs := uint64(999)
	plr, cancel, wg, store := s.newPullerForTest(c, spans, checkpointTs)
	go func() {
		for i := 1; i <= 100; i++ {
			plr.cli.Returns(model.RegionFeedEvent{
				Val: &model.RawKVEntry{
					OpType: model.OpTypePut,
					Key:    []byte("t_b"),
					Value:  []byte("test-value"),
					CRTs:   checkpointTs + uint64(i),
				},
			})
		}
	}()

	seq := uint64(0)
	for seq < uint64(100) {
		ev := <-plr.Output()
		c.Assert(ev.Sequence, check.Equals, seq)
		seq += 1
	}

	store.Close()
	cancel()
	wg.Wait()
}
