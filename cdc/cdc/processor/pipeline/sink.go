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
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/tikv/migration/cdc/cdc/model"
	"github.com/tikv/migration/cdc/cdc/sink"
	"github.com/tikv/migration/cdc/pkg/config"
	cerror "github.com/tikv/migration/cdc/pkg/errors"
	"github.com/tikv/migration/cdc/pkg/pipeline"
	"go.uber.org/zap"
)

const (
	defaultSyncResolvedBatch = 64
)

// KeySpanStatus is status of the keyspan pipeline
type KeySpanStatus int32

// KeySpanStatus for keyspan pipeline
const (
	KeySpanStatusInitializing KeySpanStatus = iota
	KeySpanStatusRunning
	KeySpanStatusStopped
)

func (s KeySpanStatus) String() string {
	switch s {
	case KeySpanStatusInitializing:
		return "Initializing"
	case KeySpanStatusRunning:
		return "Running"
	case KeySpanStatusStopped:
		return "Stopped"
	}
	return "Unknown"
}

// Load KeySpanStatus with THREAD-SAFE
func (s *KeySpanStatus) Load() KeySpanStatus {
	return KeySpanStatus(atomic.LoadInt32((*int32)(s)))
}

// Store KeySpanStatus with THREAD-SAFE
func (s *KeySpanStatus) Store(new KeySpanStatus) {
	atomic.StoreInt32((*int32)(s), int32(new))
}

type sinkNode struct {
	sink      sink.Sink
	status    KeySpanStatus
	keyspanID model.KeySpanID

	resolvedTs   model.Ts
	checkpointTs model.Ts
	targetTs     model.Ts
	barrierTs    model.Ts

	eventBuffer []*model.PolymorphicEvent
	rawKVBuffer []*model.RawKVEntry

	flowController keyspanFlowController

	replicaConfig      *config.ReplicaConfig
	isKeySpanActorMode bool
}

func newSinkNode(keyspanID model.KeySpanID, sink sink.Sink, startTs model.Ts, targetTs model.Ts, flowController keyspanFlowController) *sinkNode {
	return &sinkNode{
		keyspanID:    keyspanID,
		sink:         sink,
		status:       KeySpanStatusInitializing,
		targetTs:     targetTs,
		resolvedTs:   startTs,
		checkpointTs: startTs,
		barrierTs:    startTs,

		flowController: flowController,
	}
}

func (n *sinkNode) ResolvedTs() model.Ts   { return atomic.LoadUint64(&n.resolvedTs) }
func (n *sinkNode) CheckpointTs() model.Ts { return atomic.LoadUint64(&n.checkpointTs) }
func (n *sinkNode) SaveCheckpointTs(checkpointTs model.Ts) {
	atomic.StoreUint64(&n.checkpointTs, checkpointTs)
}
func (n *sinkNode) Status() KeySpanStatus { return n.status.Load() }

func (n *sinkNode) Init(ctx pipeline.NodeContext) error {
	n.replicaConfig = ctx.ChangefeedVars().Info.Config
	return n.InitWithReplicaConfig(false, ctx.ChangefeedVars().Info.Config)
}

func (n *sinkNode) InitWithReplicaConfig(isKeySpanActorMode bool, replicaConfig *config.ReplicaConfig) error {
	n.replicaConfig = replicaConfig
	n.isKeySpanActorMode = isKeySpanActorMode
	return nil
}

// stop is called when sink receives a stop command or checkpointTs reaches targetTs.
// In this method, the builtin keyspan sink will be closed by calling `Close`, and
// no more events can be sent to this sink node afterwards.
func (n *sinkNode) stop(ctx context.Context) (err error) {
	// keyspan stopped status must be set after underlying sink is closed
	defer n.status.Store(KeySpanStatusStopped)
	err = n.sink.Close(ctx)
	if err != nil {
		return
	}
	log.Info("sink is closed", zap.Uint64("keyspanID", n.keyspanID))
	err = cerror.ErrKeySpanProcessorStoppedSafely.GenWithStackByArgs()
	return
}

func (n *sinkNode) flushSink(ctx context.Context, resolvedTs model.Ts) (err error) {
	defer func() {
		if err != nil {
			n.status.Store(KeySpanStatusStopped)
			return
		}
		if n.CheckpointTs() >= n.targetTs {
			err = n.stop(ctx)
		}
	}()
	requestResolvedTs := resolvedTs
	if resolvedTs > n.barrierTs {
		resolvedTs = n.barrierTs
	}
	if resolvedTs > n.targetTs {
		resolvedTs = n.targetTs
	}
	log.Debug("sink.flushSink", zap.Uint64("requestResolvedTs", requestResolvedTs), zap.Uint64("resultResolvedTs", resolvedTs), zap.Uint64("barrierTs", n.barrierTs),
		zap.Uint64("targetTs", n.targetTs), zap.Uint64("checkpointTs", n.CheckpointTs()))
	if resolvedTs <= n.CheckpointTs() {
		return nil
	}
	if err := n.emitRow2Sink(ctx); err != nil {
		return errors.Trace(err)
	}
	checkpointTs, err := n.sink.FlushChangedEvents(ctx, n.keyspanID, resolvedTs)
	log.Debug("sinkNode.sink.FlushChangedEvents", zap.Uint64("keyspanID", n.keyspanID), zap.Uint64("resolvedTs", resolvedTs), zap.Uint64("checkpointTs", checkpointTs),
		zap.Uint64("n.CheckpointTs()", n.CheckpointTs()), zap.Error(err))
	if err != nil {
		return errors.Trace(err)
	}

	// we must call flowController.Release immediately after we call
	// FlushChangedEvents to prevent deadlock cause by checkpointTs
	// fall back
	n.flowController.Release(checkpointTs)

	// the checkpointTs may fall back in some situation such as:
	//   1. This keyspan is newly added to the processor
	//   2. There is one keyspan in the processor that has a smaller
	//   checkpointTs than this one
	if checkpointTs <= n.CheckpointTs() {
		return nil
	}
	n.SaveCheckpointTs(checkpointTs) // TODO: compare_and_swap ?

	return nil
}

func (n *sinkNode) emitEvent(ctx context.Context, event *model.PolymorphicEvent) error {
	if event == nil {
		log.Warn("skip emit nil event", zap.Any("event", event))
		return nil
	}
	log.Debug("sinkNode.emitEvent", zap.Any("event", event))

	n.eventBuffer = append(n.eventBuffer, event)

	if len(n.eventBuffer) >= defaultSyncResolvedBatch {
		if err := n.emitRow2Sink(ctx); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// clear event buffer and row buffer.
// Also, it dereferences data that are held by buffers.
func (n *sinkNode) clearBuffers() {
	// Do not hog memory.
	if cap(n.rawKVBuffer) > defaultSyncResolvedBatch {
		n.rawKVBuffer = make([]*model.RawKVEntry, 0, defaultSyncResolvedBatch)
	} else {
		for i := range n.rawKVBuffer {
			n.rawKVBuffer[i] = nil
		}
		n.rawKVBuffer = n.rawKVBuffer[:0]
	}

	if cap(n.eventBuffer) > defaultSyncResolvedBatch {
		n.eventBuffer = make([]*model.PolymorphicEvent, 0, defaultSyncResolvedBatch)
	} else {
		for i := range n.eventBuffer {
			n.eventBuffer[i] = nil
		}
		n.eventBuffer = n.eventBuffer[:0]
	}
}

func (n *sinkNode) emitRow2Sink(ctx context.Context) error {
	for _, ev := range n.eventBuffer {
		n.rawKVBuffer = append(n.rawKVBuffer, ev.RawKV)
	}
	failpoint.Inject("ProcessorSyncResolvedPreEmit", func() {
		log.Info("Prepare to panic for ProcessorSyncResolvedPreEmit")
		time.Sleep(10 * time.Second)
		panic("ProcessorSyncResolvedPreEmit")
	})
	if len(n.rawKVBuffer) > 0 {
		log.Debug("sinkNode.emitRow2Sink", zap.Any("n.rawKVBuffer", n.rawKVBuffer))
	}
	err := n.sink.EmitChangedEvents(ctx, n.rawKVBuffer...)
	if err != nil {
		return errors.Trace(err)
	}
	n.clearBuffers()
	return nil
}

// Receive receives the message from the previous node
func (n *sinkNode) Receive(ctx pipeline.NodeContext) error {
	_, err := n.HandleMessage(ctx, ctx.Message())
	return err
}

func (n *sinkNode) HandleMessage(ctx context.Context, msg pipeline.Message) (bool, error) {
	log.Debug("sinkNode.HandleMessage", zap.Any("msg", msg))
	if n.status == KeySpanStatusStopped {
		return false, cerror.ErrKeySpanProcessorStoppedSafely.GenWithStackByArgs()
	}
	switch msg.Tp {
	case pipeline.MessageTypePolymorphicEvent:
		event := msg.PolymorphicEvent
		if event.RawKV.OpType == model.OpTypeResolved {
			if n.status == KeySpanStatusInitializing {
				n.status.Store(KeySpanStatusRunning)
			}
			failpoint.Inject("ProcessorSyncResolvedError", func() {
				failpoint.Return(false, errors.New("processor sync resolved injected error"))
			})
			log.Debug("sinkNode.flushSink", zap.Uint64("msg.PolymorphicEvent.CRTs", msg.PolymorphicEvent.CRTs))
			if err := n.flushSink(ctx, msg.PolymorphicEvent.CRTs); err != nil {
				return false, errors.Trace(err)
			}
			atomic.StoreUint64(&n.resolvedTs, msg.PolymorphicEvent.CRTs)
			return true, nil
		}
		if err := n.emitEvent(ctx, event); err != nil {
			return false, errors.Trace(err)
		}
	case pipeline.MessageTypeTick:
		if err := n.flushSink(ctx, n.resolvedTs); err != nil {
			return false, errors.Trace(err)
		}
	case pipeline.MessageTypeCommand:
		if msg.Command.Tp == pipeline.CommandTypeStop {
			if err := n.stop(ctx); err != nil {
				return false, errors.Trace(err)
			}
		}
	case pipeline.MessageTypeBarrier:
		n.barrierTs = msg.BarrierTs
		if err := n.flushSink(ctx, n.resolvedTs); err != nil {
			return false, errors.Trace(err)
		}
	}
	return true, nil
}

func (n *sinkNode) Destroy(ctx pipeline.NodeContext) error {
	n.status.Store(KeySpanStatusStopped)
	n.flowController.Abort()
	return n.sink.Close(ctx)
}
