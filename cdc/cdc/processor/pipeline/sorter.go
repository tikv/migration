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
	"github.com/tikv/migration/cdc/cdc/sorter"
	"github.com/tikv/migration/cdc/cdc/sorter/memory"
	"github.com/tikv/migration/cdc/cdc/sorter/unified"
	"github.com/tikv/migration/cdc/pkg/config"
	cerror "github.com/tikv/migration/cdc/pkg/errors"
	"github.com/tikv/migration/cdc/pkg/pipeline"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	flushMemoryMetricsDuration = time.Second * 5
)

type sorterNode struct {
	sorter sorter.EventSorter

	keyspanID   model.KeySpanID
	keyspanName string // quoted keyspan, used in metircs only

	// for per-keyspan flow control
	flowController keyspanFlowController

	eg     *errgroup.Group
	cancel context.CancelFunc

	// The latest resolved ts that sorter has received.
	resolvedTs model.Ts

	// The latest barrier ts that sorter has received.
	barrierTs model.Ts

	replConfig *config.ReplicaConfig
}

func newSorterNode(
	keyspanName string, keyspanID model.KeySpanID, startTs model.Ts,
	flowController keyspanFlowController, replConfig *config.ReplicaConfig,
) *sorterNode {
	return &sorterNode{
		keyspanName:    keyspanName,
		keyspanID:      keyspanID,
		flowController: flowController,
		resolvedTs:     startTs,
		barrierTs:      startTs,
		replConfig:     replConfig,
	}
}

func (n *sorterNode) Init(ctx pipeline.NodeContext) error {
	wg := errgroup.Group{}
	return n.StartActorNode(ctx, &wg)
}

func (n *sorterNode) StartActorNode(ctx pipeline.NodeContext, eg *errgroup.Group) error {
	n.eg = eg
	stdCtx, cancel := context.WithCancel(ctx)
	n.cancel = cancel
	var eventSorter sorter.EventSorter
	sortEngine := ctx.ChangefeedVars().Info.Engine
	switch sortEngine {
	case model.SortInMemory:
		eventSorter = memory.NewEntrySorter()
	case model.SortUnified, model.SortInFile /* `file` becomes an alias of `unified` for backward compatibility */ :
		if sortEngine == model.SortInFile {
			log.Warn("File sorter is obsolete and replaced by unified sorter. Please revise your changefeed settings",
				zap.String("changefeed-id", ctx.ChangefeedVars().ID), zap.String("keyspan-name", n.keyspanName))
		}

		sortDir := config.GetGlobalServerConfig().Sorter.SortDir
		var err error
		eventSorter, err = unified.NewUnifiedSorter(sortDir, ctx.ChangefeedVars().ID, n.keyspanName, n.keyspanID, ctx.GlobalVars().CaptureInfo.AdvertiseAddr)
		if err != nil {
			return errors.Trace(err)
		}
	default:
		return cerror.ErrUnknownSortEngine.GenWithStackByArgs(sortEngine)
	}
	failpoint.Inject("ProcessorAddKeySpanError", func() {
		failpoint.Return(errors.New("processor add keyspan injected error"))
	})
	n.eg.Go(func() error {
		ctx.Throw(errors.Trace(eventSorter.Run(stdCtx)))
		return nil
	})
	n.eg.Go(func() error {
		// Since the flowController is implemented by `Cond`, it is not cancelable
		// by a context. We need to listen on cancellation and aborts the flowController
		// manually.
		<-stdCtx.Done()
		n.flowController.Abort()
		return nil
	})
	n.eg.Go(func() error {
		lastSentResolvedTs := uint64(0)
		lastSendResolvedTsTime := time.Now() // the time at which we last sent a resolved-ts.
		lastCRTs := uint64(0)                // the commit-ts of the last row changed we sent.

		metricsKeySpanMemoryHistogram := keyspanMemoryHistogram.WithLabelValues(ctx.ChangefeedVars().ID, ctx.GlobalVars().CaptureInfo.AdvertiseAddr)
		metricsTicker := time.NewTicker(flushMemoryMetricsDuration)
		defer metricsTicker.Stop()

		for {
			select {
			case <-stdCtx.Done():
				return nil
			case <-metricsTicker.C:
				metricsKeySpanMemoryHistogram.Observe(float64(n.flowController.GetConsumption()))
			case msg, ok := <-eventSorter.Output():
				if !ok {
					// sorter output channel closed
					return nil
				}
				if msg == nil || msg.RawKV == nil {
					log.Panic("unexpected empty msg", zap.Reflect("msg", msg))
				}
				if msg.RawKV.OpType != model.OpTypeResolved {
					commitTs := msg.CRTs
					// We interpolate a resolved-ts if none has been sent for some time.
					if time.Since(lastSendResolvedTsTime) > resolvedTsInterpolateInterval {
						// checks the condition: cur_event_commit_ts > prev_event_commit_ts > last_resolved_ts
						// If this is true, it implies that (1) the last transaction has finished, and we are processing
						// the first event in a new transaction, (2) a resolved-ts is safe to be sent, but it has not yet.
						// This means that we can interpolate prev_event_commit_ts as a resolved-ts, improving the frequency
						// at which the sink flushes.
						if lastCRTs > lastSentResolvedTs && commitTs > lastCRTs {
							lastSentResolvedTs = lastCRTs
							lastSendResolvedTsTime = time.Now()
							ctx.SendToNextNode(pipeline.PolymorphicEventMessage(model.NewResolvedPolymorphicEvent(0, lastCRTs, n.keyspanID)))
						}
					}

					// We calculate memory consumption by PolymorphicEvent size.
					size := uint64(msg.RawKV.ApproximateDataSize())
					// NOTE we allow the quota to be exceeded if blocking means interrupting a transaction.
					// Otherwise the pipeline would deadlock.
					err := n.flowController.Consume(commitTs, size, func() error {
						if lastCRTs > lastSentResolvedTs {
							// If we are blocking, we send a Resolved Event here to elicit a sink-flush.
							// Not sending a Resolved Event here will very likely deadlock the pipeline.
							lastSentResolvedTs = lastCRTs
							lastSendResolvedTsTime = time.Now()
							ctx.SendToNextNode(pipeline.PolymorphicEventMessage(model.NewResolvedPolymorphicEvent(0, lastCRTs, n.keyspanID)))
						}
						return nil
					})
					if err != nil {
						if cerror.ErrFlowControllerAborted.Equal(err) {
							log.Info("flow control cancelled for keyspan",
								zap.Uint64("keyspanID", n.keyspanID),
								zap.String("keyspanName", n.keyspanName))
						} else {
							ctx.Throw(err)
						}
						return nil
					}
					lastCRTs = commitTs
				} else {
					// handle OpTypeResolved
					if msg.CRTs < lastSentResolvedTs {
						continue
					}
					lastSentResolvedTs = msg.CRTs
					lastSendResolvedTsTime = time.Now()
				}
				ctx.SendToNextNode(pipeline.PolymorphicEventMessage(msg))
			}
		}
	})
	n.sorter = eventSorter
	return nil
}

// Receive receives the message from the previous node
func (n *sorterNode) Receive(ctx pipeline.NodeContext) error {
	_, err := n.TryHandleDataMessage(ctx, ctx.Message())
	return err
}

func (n *sorterNode) TryHandleDataMessage(ctx context.Context, msg pipeline.Message) (bool, error) {
	switch msg.Tp {
	case pipeline.MessageTypePolymorphicEvent:
		rawKV := msg.PolymorphicEvent.RawKV
		if rawKV != nil && rawKV.OpType == model.OpTypeResolved {
			// Puller resolved ts should not fall back.
			resolvedTs := rawKV.CRTs
			oldResolvedTs := atomic.SwapUint64(&n.resolvedTs, resolvedTs)
			if oldResolvedTs > resolvedTs {
				log.Panic("resolved ts regression",
					zap.Uint64("keyspanID", n.keyspanID),
					zap.Uint64("resolvedTs", resolvedTs),
					zap.Uint64("oldResolvedTs", oldResolvedTs))
			}
			atomic.StoreUint64(&n.resolvedTs, rawKV.CRTs)
		}
		n.sorter.AddEntry(ctx, msg.PolymorphicEvent)
		return true, nil
	case pipeline.MessageTypeBarrier:
		if msg.BarrierTs > n.barrierTs {
			n.barrierTs = msg.BarrierTs
		}
		fallthrough
	default:
		ctx.(pipeline.NodeContext).SendToNextNode(msg)
		return true, nil
	}
}

func (n *sorterNode) Destroy(ctx pipeline.NodeContext) error {
	defer keyspanMemoryHistogram.DeleteLabelValues(ctx.ChangefeedVars().ID, ctx.GlobalVars().CaptureInfo.AdvertiseAddr)
	n.cancel()
	return n.eg.Wait()
}

func (n *sorterNode) ResolvedTs() model.Ts {
	return atomic.LoadUint64(&n.resolvedTs)
}
