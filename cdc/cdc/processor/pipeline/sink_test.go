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

package pipeline

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/migration/cdc/cdc/model"
	"github.com/tikv/migration/cdc/pkg/config"
	cdcContext "github.com/tikv/migration/cdc/pkg/context"
	cerrors "github.com/tikv/migration/cdc/pkg/errors"
	"github.com/tikv/migration/cdc/pkg/pipeline"
)

type mockSink struct {
	received []struct {
		resolvedTs model.Ts
		rawKVEntry *model.RawKVEntry
	}
}

// mockFlowController is created because a real keyspanFlowController cannot be used
// we are testing sinkNode by itself.
type mockFlowController struct{}

func (c *mockFlowController) Consume(commitTs uint64, size uint64, blockCallBack func() error) error {
	return nil
}

func (c *mockFlowController) Release(resolvedTs uint64) {
}

func (c *mockFlowController) Abort() {
}

func (c *mockFlowController) GetConsumption() uint64 {
	return 0
}

func (s *mockSink) EmitChangedEvents(ctx context.Context, RawKVEntries ...*model.RawKVEntry) error {
	for _, rawKVEntry := range RawKVEntries {
		s.received = append(s.received, struct {
			resolvedTs model.Ts
			rawKVEntry *model.RawKVEntry
		}{rawKVEntry: rawKVEntry})
	}
	return nil
}

func (s *mockSink) FlushChangedEvents(ctx context.Context, _ model.KeySpanID, resolvedTs uint64) (uint64, error) {
	s.received = append(s.received, struct {
		resolvedTs model.Ts
		rawKVEntry *model.RawKVEntry
	}{resolvedTs: resolvedTs})
	return resolvedTs, nil
}

func (s *mockSink) EmitCheckpointTs(ctx context.Context, ts uint64) error {
	panic("unreachable")
}

func (s *mockSink) Close(ctx context.Context) error {
	return nil
}

func (s *mockSink) Barrier(ctx context.Context, keyspanID model.KeySpanID) error {
	return nil
}

func (s *mockSink) Check(t *testing.T, expected []struct {
	resolvedTs model.Ts
	rawKVEntry *model.RawKVEntry
},
) {
	require.Equal(t, expected, s.received)
}

func (s *mockSink) Reset() {
	s.received = s.received[:0]
}

type mockCloseControlSink struct {
	mockSink
	closeCh chan interface{}
}

func (s *mockCloseControlSink) Close(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.closeCh:
		return nil
	}
}

func TestStatus(t *testing.T) {
	ctx := cdcContext.NewContext(context.Background(), &cdcContext.GlobalVars{})
	ctx = cdcContext.WithChangefeedVars(ctx, &cdcContext.ChangefeedVars{
		ID: "changefeed-id-test-status",
		Info: &model.ChangeFeedInfo{
			StartTs: oracle.GoTimeToTS(time.Now()),
			Config:  config.GetDefaultReplicaConfig(),
		},
	})

	// test stop at targetTs
	node := newSinkNode(1, &mockSink{}, 0, 10, &mockFlowController{})
	require.Nil(t, node.Init(pipeline.MockNodeContext4Test(ctx, pipeline.Message{}, nil)))
	require.Equal(t, KeySpanStatusInitializing, node.Status())

	require.Nil(t, node.Receive(pipeline.MockNodeContext4Test(ctx, pipeline.BarrierMessage(20), nil)))
	require.Equal(t, KeySpanStatusInitializing, node.Status())

	require.Nil(t, node.Receive(pipeline.MockNodeContext4Test(ctx,
		pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 1, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}}), nil)))
	require.Equal(t, KeySpanStatusInitializing, node.Status())

	require.Nil(t, node.Receive(pipeline.MockNodeContext4Test(ctx,
		pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 2, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}}), nil)))
	require.Equal(t, KeySpanStatusInitializing, node.Status())

	require.Nil(t, node.Receive(pipeline.MockNodeContext4Test(ctx,
		pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 2, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved}}), nil)))
	require.Equal(t, KeySpanStatusRunning, node.Status())

	err := node.Receive(pipeline.MockNodeContext4Test(ctx,
		pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 15, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved}}), nil))
	require.True(t, cerrors.ErrKeySpanProcessorStoppedSafely.Equal(err))
	require.Equal(t, KeySpanStatusStopped, node.Status())
	require.Equal(t, uint64(10), node.CheckpointTs())

	// test the stop at ts command
	node = newSinkNode(1, &mockSink{}, 0, 10, &mockFlowController{})
	require.Nil(t, node.Init(pipeline.MockNodeContext4Test(ctx, pipeline.Message{}, nil)))
	require.Equal(t, KeySpanStatusInitializing, node.Status())

	require.Nil(t, node.Receive(pipeline.MockNodeContext4Test(ctx, pipeline.BarrierMessage(20), nil)))
	require.Equal(t, KeySpanStatusInitializing, node.Status())

	require.Nil(t, node.Receive(pipeline.MockNodeContext4Test(ctx,
		pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 2, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved}}), nil)))
	require.Equal(t, KeySpanStatusRunning, node.Status())

	err = node.Receive(pipeline.MockNodeContext4Test(ctx,
		pipeline.CommandMessage(&pipeline.Command{Tp: pipeline.CommandTypeStop}), nil))
	require.True(t, cerrors.ErrKeySpanProcessorStoppedSafely.Equal(err))
	require.Equal(t, KeySpanStatusStopped, node.Status())

	err = node.Receive(pipeline.MockNodeContext4Test(ctx,
		pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 7, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved}}), nil))
	require.True(t, cerrors.ErrKeySpanProcessorStoppedSafely.Equal(err))
	require.Equal(t, KeySpanStatusStopped, node.Status())
	require.Equal(t, uint64(2), node.CheckpointTs())

	// test the stop at ts command is after then resolvedTs and checkpointTs is greater than stop ts
	node = newSinkNode(1, &mockSink{}, 0, 10, &mockFlowController{})
	require.Nil(t, node.Init(pipeline.MockNodeContext4Test(ctx, pipeline.Message{}, nil)))
	require.Equal(t, KeySpanStatusInitializing, node.Status())

	require.Nil(t, node.Receive(pipeline.MockNodeContext4Test(ctx, pipeline.BarrierMessage(20), nil)))
	require.Equal(t, KeySpanStatusInitializing, node.Status())

	require.Nil(t, node.Receive(pipeline.MockNodeContext4Test(ctx,
		pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 7, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved}}), nil)))
	require.Equal(t, KeySpanStatusRunning, node.Status())

	err = node.Receive(pipeline.MockNodeContext4Test(ctx,
		pipeline.CommandMessage(&pipeline.Command{Tp: pipeline.CommandTypeStop}), nil))
	require.True(t, cerrors.ErrKeySpanProcessorStoppedSafely.Equal(err))
	require.Equal(t, KeySpanStatusStopped, node.Status())

	err = node.Receive(pipeline.MockNodeContext4Test(ctx,
		pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 7, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved}}), nil))
	require.True(t, cerrors.ErrKeySpanProcessorStoppedSafely.Equal(err))
	require.Equal(t, KeySpanStatusStopped, node.Status())
	require.Equal(t, uint64(7), node.CheckpointTs())
}

// TestStopStatus tests the keyspan status of a pipeline is not set to stopped
// until the underlying sink is closed
func TestStopStatus(t *testing.T) {
	ctx := cdcContext.NewContext(context.Background(), &cdcContext.GlobalVars{})
	ctx = cdcContext.WithChangefeedVars(ctx, &cdcContext.ChangefeedVars{
		ID: "changefeed-id-test-status",
		Info: &model.ChangeFeedInfo{
			StartTs: oracle.GoTimeToTS(time.Now()),
			Config:  config.GetDefaultReplicaConfig(),
		},
	})

	closeCh := make(chan interface{}, 1)
	node := newSinkNode(1, &mockCloseControlSink{mockSink: mockSink{}, closeCh: closeCh}, 0, 100, &mockFlowController{})
	require.Nil(t, node.Init(pipeline.MockNodeContext4Test(ctx, pipeline.Message{}, nil)))
	require.Equal(t, KeySpanStatusInitializing, node.Status())
	require.Nil(t, node.Receive(pipeline.MockNodeContext4Test(ctx,
		pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 2, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved}}), nil)))
	require.Equal(t, KeySpanStatusRunning, node.Status())

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		// This will block until sink Close returns
		err := node.Receive(pipeline.MockNodeContext4Test(ctx,
			pipeline.CommandMessage(&pipeline.Command{Tp: pipeline.CommandTypeStop}), nil))
		require.True(t, cerrors.ErrKeySpanProcessorStoppedSafely.Equal(err))
		require.Equal(t, KeySpanStatusStopped, node.Status())
	}()
	// wait to ensure stop message is sent to the sink node
	time.Sleep(time.Millisecond * 50)
	require.Equal(t, KeySpanStatusRunning, node.Status())
	closeCh <- struct{}{}
	wg.Wait()
}

func TestManyTs(t *testing.T) {
	ctx := cdcContext.NewContext(context.Background(), &cdcContext.GlobalVars{})
	ctx = cdcContext.WithChangefeedVars(ctx, &cdcContext.ChangefeedVars{
		ID: "changefeed-id-test-many-ts",
		Info: &model.ChangeFeedInfo{
			StartTs: oracle.GoTimeToTS(time.Now()),
			Config:  config.GetDefaultReplicaConfig(),
		},
	})
	sink := &mockSink{}
	node := newSinkNode(1, sink, 0, 10, &mockFlowController{})
	require.Nil(t, node.Init(pipeline.MockNodeContext4Test(ctx, pipeline.Message{}, nil)))
	require.Equal(t, KeySpanStatusInitializing, node.Status())

	require.Nil(t, node.Receive(pipeline.MockNodeContext4Test(ctx,
		pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{
			CRTs: 1,
			RawKV: &model.RawKVEntry{
				Key:    []byte{1},
				Value:  []byte{1},
				OpType: model.OpTypePut,
			},
		}), nil)))
	require.Equal(t, KeySpanStatusInitializing, node.Status())

	require.Nil(t, node.Receive(pipeline.MockNodeContext4Test(ctx,
		pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{
			CRTs: 2,
			RawKV: &model.RawKVEntry{
				Key:    []byte{2},
				Value:  []byte{2},
				OpType: model.OpTypePut,
			},
		}), nil)))
	require.Equal(t, KeySpanStatusInitializing, node.Status())

	require.Nil(t, node.Receive(pipeline.MockNodeContext4Test(ctx,
		pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 2, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved}}), nil)))
	require.Equal(t, KeySpanStatusRunning, node.Status())
	sink.Check(t, nil)

	require.Nil(t, node.Receive(pipeline.MockNodeContext4Test(ctx, pipeline.BarrierMessage(1), nil)))
	require.Equal(t, KeySpanStatusRunning, node.Status())

	sink.Check(t, []struct {
		resolvedTs model.Ts
		rawKVEntry *model.RawKVEntry
	}{
		{
			rawKVEntry: &model.RawKVEntry{
				Key:    []byte{1},
				Value:  []byte{1},
				OpType: model.OpTypePut,
			},
		},
		{
			rawKVEntry: &model.RawKVEntry{
				Key:    []byte{2},
				Value:  []byte{2},
				OpType: model.OpTypePut,
			},
		},
		{resolvedTs: 1},
	})
	sink.Reset()
	require.Equal(t, uint64(2), node.ResolvedTs())
	require.Equal(t, uint64(1), node.CheckpointTs())

	require.Nil(t, node.Receive(pipeline.MockNodeContext4Test(ctx, pipeline.BarrierMessage(5), nil)))
	require.Equal(t, KeySpanStatusRunning, node.Status())
	sink.Check(t, []struct {
		resolvedTs model.Ts
		rawKVEntry *model.RawKVEntry
	}{
		{resolvedTs: 2},
	})
	sink.Reset()
	require.Equal(t, uint64(2), node.ResolvedTs())
	require.Equal(t, uint64(2), node.CheckpointTs())
}

type flushFlowController struct {
	mockFlowController
	releaseCounter int
}

func (c *flushFlowController) Release(resolvedTs uint64) {
	c.releaseCounter++
}

type flushSink struct {
	mockSink
}

// use to simulate the situation that resolvedTs return from sink manager
// fall back
var fallBackResolvedTs = uint64(10)

func (s *flushSink) FlushChangedEvents(ctx context.Context, _ model.KeySpanID, resolvedTs uint64) (uint64, error) {
	if resolvedTs == fallBackResolvedTs {
		return 0, nil
	}
	return resolvedTs, nil
}

// TestFlushSinkReleaseFlowController tests sinkNode.flushSink method will always
// call flowController.Release to release the memory quota of the keyspan to avoid
// deadlock if there is no error occur
func TestFlushSinkReleaseFlowController(t *testing.T) {
	ctx := cdcContext.NewContext(context.Background(), &cdcContext.GlobalVars{})
	cfg := config.GetDefaultReplicaConfig()
	cfg.EnableOldValue = false
	ctx = cdcContext.WithChangefeedVars(ctx, &cdcContext.ChangefeedVars{
		ID: "changefeed-id-test-flushSink",
		Info: &model.ChangeFeedInfo{
			StartTs: oracle.GoTimeToTS(time.Now()),
			Config:  cfg,
		},
	})
	flowController := &flushFlowController{}
	sink := &flushSink{}
	// sNode is a sinkNode
	sNode := newSinkNode(1, sink, 0, 10, flowController)
	require.Nil(t, sNode.Init(pipeline.MockNodeContext4Test(ctx, pipeline.Message{}, nil)))
	sNode.barrierTs = 10

	err := sNode.flushSink(context.Background(), uint64(8))
	require.Nil(t, err)
	require.Equal(t, uint64(8), sNode.checkpointTs)
	require.Equal(t, 1, flowController.releaseCounter)
	// resolvedTs will fall back in this call
	err = sNode.flushSink(context.Background(), uint64(10))
	require.Nil(t, err)
	require.Equal(t, uint64(8), sNode.checkpointTs)
	require.Equal(t, 2, flowController.releaseCounter)
}
