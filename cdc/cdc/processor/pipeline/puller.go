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
	"strconv"

	"github.com/pingcap/errors"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/migration/cdc/cdc/model"
	"github.com/tikv/migration/cdc/cdc/puller"
	"github.com/tikv/migration/cdc/pkg/pipeline"
	"github.com/tikv/migration/cdc/pkg/regionspan"
	"github.com/tikv/migration/cdc/pkg/util"
	"golang.org/x/sync/errgroup"
)

type pullerNode struct {
	keyspanID   model.KeySpanID
	keyspanName string
	keyspan     regionspan.Span
	replicaInfo *model.KeySpanReplicaInfo
	eventFilter *util.KvFilter
	cancel      context.CancelFunc
	wg          *errgroup.Group
}

func newPullerNode(
	keyspanID model.KeySpanID, replicaInfo *model.KeySpanReplicaInfo, filterConfig *util.KvFilterConfig,
) pipeline.Node {
	keyspan := regionspan.Span{Start: replicaInfo.Start, End: replicaInfo.End}
	var filter *util.KvFilter
	if filterConfig != nil {
		filter = util.CreateFilter(filterConfig)
	}
	return &pullerNode{
		keyspanID:   keyspanID,
		keyspanName: keyspan.Name(),
		keyspan:     keyspan,
		replicaInfo: replicaInfo,
		eventFilter: filter,
	}
}

func (n *pullerNode) keyspans() []regionspan.Span {
	return []regionspan.Span{n.keyspan}
}

func (n *pullerNode) Init(ctx pipeline.NodeContext) error {
	return n.InitWithWaitGroup(ctx, new(errgroup.Group))
}

func (n *pullerNode) InitWithWaitGroup(ctx pipeline.NodeContext, wg *errgroup.Group) error {
	n.wg = wg
	metricKeySpanResolvedTsGauge := keyspanResolvedTsGauge.WithLabelValues(ctx.ChangefeedVars().ID, ctx.GlobalVars().CaptureInfo.AdvertiseAddr, strconv.FormatUint(n.keyspanID, 10))
	ctxC, cancel := context.WithCancel(ctx)
	ctxC = util.PutKeySpanInfoInCtx(ctxC, n.keyspanID, n.keyspanName)
	ctxC = util.PutCaptureAddrInCtx(ctxC, ctx.GlobalVars().CaptureInfo.AdvertiseAddr)
	ctxC = util.PutChangefeedIDInCtx(ctxC, ctx.ChangefeedVars().ID)
	ctxC = util.PutEventFilterInCtx(ctxC, n.eventFilter)

	plr := puller.NewPuller(ctxC, ctx.GlobalVars().PDClient, ctx.GlobalVars().GrpcPool, ctx.GlobalVars().RegionCache, ctx.GlobalVars().KVStorage,
		n.replicaInfo.StartTs, n.keyspans(), true)
	n.wg.Go(func() error {
		ctx.Throw(errors.Trace(plr.Run(ctxC)))
		return nil
	})
	n.wg.Go(func() error {
		for {
			select {
			case <-ctxC.Done():
				return nil
			case rawKV := <-plr.Output():
				if rawKV == nil {
					continue
				}
				rawKV.KeySpanID = n.keyspanID
				if rawKV.OpType == model.OpTypeResolved {
					metricKeySpanResolvedTsGauge.Set(float64(oracle.ExtractPhysical(rawKV.CRTs)))
				}
				pEvent := model.NewPolymorphicEvent(rawKV)
				ctx.SendToNextNode(pipeline.PolymorphicEventMessage(pEvent))
			}
		}
	})
	n.cancel = cancel
	return nil
}

// Receive receives the message from the previous node
func (n *pullerNode) Receive(ctx pipeline.NodeContext) error {
	// just forward any messages to the next node
	ctx.SendToNextNode(ctx.Message())
	return nil
}

func (n *pullerNode) Destroy(ctx pipeline.NodeContext) error {
	keyspanResolvedTsGauge.DeleteLabelValues(ctx.ChangefeedVars().ID, ctx.GlobalVars().CaptureInfo.AdvertiseAddr, strconv.FormatUint(n.keyspanID, 10))
	n.cancel()
	return n.wg.Wait()
}
