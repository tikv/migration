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
	"time"

	"github.com/pingcap/log"
	"github.com/tikv/migration/cdc/cdc/model"
	"github.com/tikv/migration/cdc/cdc/sink"
	"github.com/tikv/migration/cdc/cdc/sink/common"
	serverConfig "github.com/tikv/migration/cdc/pkg/config"
	cdcContext "github.com/tikv/migration/cdc/pkg/context"
	cerror "github.com/tikv/migration/cdc/pkg/errors"
	"github.com/tikv/migration/cdc/pkg/pipeline"
	"github.com/tikv/migration/cdc/pkg/regionspan"
	"go.uber.org/zap"
)

const (
	// TODO determine a reasonable default value
	// This is part of sink performance optimization
	resolvedTsInterpolateInterval = 200 * time.Millisecond
)

// KeySpanPipeline is a pipeline which capture the change log from tikv in a keyspan
type KeySpanPipeline interface {
	// ID returns the ID of source keyspan and mark keyspan
	ID() (keyspanID uint64)
	// Name returns the quoted schema and keyspan name
	Name() string
	// ResolvedTs returns the resolved ts in this keyspan pipeline
	ResolvedTs() model.Ts
	// CheckpointTs returns the checkpoint ts in this keyspan pipeline
	CheckpointTs() model.Ts
	// UpdateBarrierTs updates the barrier ts in this keyspan pipeline
	UpdateBarrierTs(ts model.Ts)
	// AsyncStop tells the pipeline to stop, and returns true is the pipeline is already stopped.
	AsyncStop(targetTs model.Ts) bool
	// Workload returns the workload of this keyspan
	Workload() model.WorkloadInfo
	// Status returns the status of this keyspan pipeline
	Status() KeySpanStatus
	// Cancel stops this keyspan pipeline immediately and destroy all resources created by this keyspan pipeline
	Cancel()
	// Wait waits for keyspan pipeline destroyed
	Wait()
}

type keyspanPipelineImpl struct {
	p *pipeline.Pipeline

	keyspanID   uint64
	keyspanName string
	keyspan     regionspan.Span

	sorterNode *sorterNode
	sinkNode   *sinkNode
	cancel     context.CancelFunc

	replConfig *serverConfig.ReplicaConfig
}

// TODO find a better name or avoid using an interface
// We use an interface here for ease in unit testing.
type changefeedFlowController interface {
	Consume(commitTs uint64, size uint64, blockCallBack func() error) error
	Release(resolvedTs uint64)
	Abort()
	GetConsumption() uint64
}

// ResolvedTs returns the resolved ts in this keyspan pipeline
func (t *keyspanPipelineImpl) ResolvedTs() model.Ts {
	return t.sorterNode.ResolvedTs()
}

// CheckpointTs returns the checkpoint ts in this keyspan pipeline
func (t *keyspanPipelineImpl) CheckpointTs() model.Ts {
	return t.sinkNode.CheckpointTs()
}

// UpdateBarrierTs updates the barrier ts in this keyspan pipeline
func (t *keyspanPipelineImpl) UpdateBarrierTs(ts model.Ts) {
	err := t.p.SendToFirstNode(pipeline.BarrierMessage(ts))
	if err != nil && !cerror.ErrSendToClosedPipeline.Equal(err) && !cerror.ErrPipelineTryAgain.Equal(err) {
		log.Panic("unexpect error from send to first node", zap.Error(err))
	}
}

// AsyncStop tells the pipeline to stop, and returns true is the pipeline is already stopped.
func (t *keyspanPipelineImpl) AsyncStop(targetTs model.Ts) bool {
	err := t.p.SendToFirstNode(pipeline.CommandMessage(&pipeline.Command{
		Tp: pipeline.CommandTypeStop,
	}))
	log.Info("send async stop signal to keyspan", zap.Uint64("keyspanID", t.keyspanID), zap.Uint64("targetTs", targetTs))
	if err != nil {
		if cerror.ErrPipelineTryAgain.Equal(err) {
			return false
		}
		if cerror.ErrSendToClosedPipeline.Equal(err) {
			return true
		}
		log.Panic("unexpect error from send to first node", zap.Error(err))
	}
	return true
}

var workload = model.WorkloadInfo{Workload: 1}

// Workload returns the workload of this keyspan
func (t *keyspanPipelineImpl) Workload() model.WorkloadInfo {
	// TODO(leoppro) calculate the workload of this keyspan
	// We temporarily set the value to constant 1
	return workload
}

// Status returns the status of this keyspan pipeline
func (t *keyspanPipelineImpl) Status() KeySpanStatus {
	return t.sinkNode.Status()
}

// ID returns the ID of source keyspan and mark keyspan
// TODO: Maybe tikv cdc don't need markKeySpanID.
func (t *keyspanPipelineImpl) ID() (keyspanID uint64) {
	return t.keyspanID
}

// Name returns the quoted schema and keyspan name
func (t *keyspanPipelineImpl) Name() string {
	return t.keyspanName
}

// Cancel stops this keyspan pipeline immediately and destroy all resources created by this keyspan pipeline
func (t *keyspanPipelineImpl) Cancel() {
	t.cancel()
}

// Wait waits for keyspan pipeline destroyed
func (t *keyspanPipelineImpl) Wait() {
	t.p.Wait()
}

// Assume 1KB per row in upstream TiDB, it takes about 250 MB (1024*4*64) for
// replicating 1024 keyspans in the worst case.
const defaultOutputChannelSize = 64

// There are 4 runners in keyspan pipeline: header, puller, sorter, sink.
const defaultRunnersSize = 4

// NewKeySpanPipeline creates a keyspan pipeline
func NewKeySpanPipeline(ctx cdcContext.Context,
	keyspanID model.KeySpanID,
	replicaInfo *model.KeySpanReplicaInfo,
	sink sink.Sink,
	targetTs model.Ts,
) KeySpanPipeline {
	ctx, cancel := cdcContext.WithCancel(ctx)
	replConfig := ctx.ChangefeedVars().Info.Config
	keyspan := regionspan.Span{Start: replicaInfo.Start, End: replicaInfo.End}
	keyspanName := keyspan.Name()
	keyspanPipeline := &keyspanPipelineImpl{
		keyspanID:   keyspanID,
		keyspanName: keyspanName,
		keyspan:     keyspan,
		cancel:      cancel,
		replConfig:  replConfig,
	}

	perChangefeedMemoryQuota := serverConfig.GetGlobalServerConfig().PerChangefeedMemoryQuota

	log.Debug("creating keyspan flow controller",
		zap.String("changefeed-id", ctx.ChangefeedVars().ID),
		zap.String("keyspan-name", keyspanName),
		zap.Uint64("keyspan-id", keyspanID),
		zap.Uint64("quota", perChangefeedMemoryQuota))

	flowController := common.NewChangefeedFlowController(perChangefeedMemoryQuota)
	runnerSize := defaultRunnersSize

	p := pipeline.NewPipeline(ctx, 500*time.Millisecond, runnerSize, defaultOutputChannelSize)

	sorterNode := newSorterNode(keyspanName, keyspanID, replicaInfo.StartTs, flowController, replConfig)

	sinkNode := newSinkNode(keyspanID, sink, replicaInfo.StartTs, targetTs, flowController)

	p.AppendNode(ctx, "puller", newPullerNode(keyspanID, replicaInfo, replConfig.Filter))
	p.AppendNode(ctx, "sorter", sorterNode)
	p.AppendNode(ctx, "sink", sinkNode)

	keyspanPipeline.p = p
	keyspanPipeline.sorterNode = sorterNode
	keyspanPipeline.sinkNode = sinkNode
	return keyspanPipeline
}
