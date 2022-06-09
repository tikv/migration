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
	"go.uber.org/zap"
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
	keyspanName string // quoted schema and keyspan, used in metircs only

	// sorterNode *sorterNode
	sinkNode *sinkNode
	cancel   context.CancelFunc

	replConfig *serverConfig.ReplicaConfig
}

// TODO find a better name or avoid using an interface
// We use an interface here for ease in unit testing.
type keyspanFlowController interface {
	Consume(commitTs uint64, size uint64, blockCallBack func() error) error
	Release(resolvedTs uint64)
	Abort()
	GetConsumption() uint64
}

// ResolvedTs returns the resolved ts in this keyspan pipeline
func (t *keyspanPipelineImpl) ResolvedTs() model.Ts {
	// TODO: after TiCDC introduces p2p based resolved ts mechanism, TiCDC nodes
	// will be able to cooperate replication status directly. Then we will add
	// another replication barrier for consistent replication instead of reusing
	// the global resolved-ts.

	return 0
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

// There are 4 or 5 runners in keyspan pipeline: header, puller,
// sink, cyclic if cyclic replication is enabled
const defaultRunnersSize = 3

// NewKeySpanPipeline creates a keyspan pipeline
// TODO(leoppro): implement a mock kvclient to test the keyspan pipeline
func NewKeySpanPipeline(ctx cdcContext.Context,
	// mounter entry.Mounter,
	keyspanID model.KeySpanID,
	replicaInfo *model.KeySpanReplicaInfo,
	sink sink.Sink,
	targetTs model.Ts) KeySpanPipeline {
	ctx, cancel := cdcContext.WithCancel(ctx)
	replConfig := ctx.ChangefeedVars().Info.Config
	keyspanPipeline := &keyspanPipelineImpl{
		keyspanID:   keyspanID,
		cancel:      cancel,
		replConfig:  replConfig,
		keyspanName: string(replicaInfo.Start) + "-" + string(replicaInfo.End),
	}

	perKeySpanMemoryQuota := serverConfig.GetGlobalServerConfig().PerKeySpanMemoryQuota

	log.Debug("creating keyspan flow controller",
		zap.String("changefeed-id", ctx.ChangefeedVars().ID),
		zap.Uint64("quota", perKeySpanMemoryQuota))

	flowController := common.NewKeySpanFlowController(perKeySpanMemoryQuota)
	// config := ctx.ChangefeedVars().Info.Config
	// cyclicEnabled := config.Cyclic != nil && config.Cyclic.IsEnabled()
	runnerSize := defaultRunnersSize

	p := pipeline.NewPipeline(ctx, 500*time.Millisecond, runnerSize, defaultOutputChannelSize)
	sinkNode := newSinkNode(keyspanID, sink, replicaInfo.StartTs, targetTs, flowController)

	p.AppendNode(ctx, "puller", newPullerNode(keyspanID, replicaInfo))
	p.AppendNode(ctx, "sink", sinkNode)

	keyspanPipeline.p = p
	keyspanPipeline.sinkNode = sinkNode
	return keyspanPipeline
}
