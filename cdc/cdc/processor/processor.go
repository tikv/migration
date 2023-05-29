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

package processor

import (
	"context"
	"fmt"
	"io"
	"math"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/migration/cdc/cdc/model"
	keyspanpipeline "github.com/tikv/migration/cdc/cdc/processor/pipeline"
	"github.com/tikv/migration/cdc/cdc/sink"
	cdcContext "github.com/tikv/migration/cdc/pkg/context"
	cerror "github.com/tikv/migration/cdc/pkg/errors"
	"github.com/tikv/migration/cdc/pkg/orchestrator"
	"github.com/tikv/migration/cdc/pkg/util"
	"go.uber.org/zap"
)

type processor struct {
	changefeedID model.ChangeFeedID
	captureInfo  *model.CaptureInfo
	changefeed   *orchestrator.ChangefeedReactorState

	keyspans map[model.KeySpanID]keyspanpipeline.KeySpanPipeline

	sinkManager   *sink.Manager
	lastRedoFlush time.Time

	initialized bool
	errCh       chan error
	cancel      context.CancelFunc
	wg          sync.WaitGroup

	lazyInit              func(ctx cdcContext.Context) error
	createKeySpanPipeline func(ctx cdcContext.Context, keyspanID model.KeySpanID, replicaInfo *model.KeySpanReplicaInfo) (keyspanpipeline.KeySpanPipeline, error)

	metricResolvedTsGauge       prometheus.Gauge
	metricResolvedTsLagGauge    prometheus.Gauge
	metricCheckpointTsGauge     prometheus.Gauge
	metricCheckpointTsLagGauge  prometheus.Gauge
	metricSyncKeySpanNumGauge   prometheus.Gauge
	metricProcessorErrorCounter prometheus.Counter
}

// newProcessor creates a new processor
func newProcessor(ctx cdcContext.Context) *processor {
	changefeedID := ctx.ChangefeedVars().ID
	advertiseAddr := ctx.GlobalVars().CaptureInfo.AdvertiseAddr
	p := &processor{
		keyspans:      make(map[model.KeySpanID]keyspanpipeline.KeySpanPipeline),
		errCh:         make(chan error, 1),
		changefeedID:  changefeedID,
		captureInfo:   ctx.GlobalVars().CaptureInfo,
		cancel:        func() {},
		lastRedoFlush: time.Now(),

		metricResolvedTsGauge:       resolvedTsGauge.WithLabelValues(changefeedID, advertiseAddr),
		metricResolvedTsLagGauge:    resolvedTsLagGauge.WithLabelValues(changefeedID, advertiseAddr),
		metricCheckpointTsGauge:     checkpointTsGauge.WithLabelValues(changefeedID, advertiseAddr),
		metricCheckpointTsLagGauge:  checkpointTsLagGauge.WithLabelValues(changefeedID, advertiseAddr),
		metricSyncKeySpanNumGauge:   syncKeySpanNumGauge.WithLabelValues(changefeedID, advertiseAddr),
		metricProcessorErrorCounter: processorErrorCounter.WithLabelValues(changefeedID, advertiseAddr),
		// metricSchemaStorageGcTsGauge: processorSchemaStorageGcTsGauge.WithLabelValues(changefeedID, advertiseAddr),
	}
	p.createKeySpanPipeline = p.createKeySpanPipelineImpl
	p.lazyInit = p.lazyInitImpl
	return p
}

var processorIgnorableError = []*errors.Error{
	cerror.ErrAdminStopProcessor,
	cerror.ErrReactorFinished,
	cerror.ErrRedoWriterStopped,
}

// isProcessorIgnorableError returns true if the error means the processor exits
// normally, caused by changefeed pause, remove, etc.
func isProcessorIgnorableError(err error) bool {
	if err == nil {
		return true
	}
	if errors.Cause(err) == context.Canceled {
		return true
	}
	for _, e := range processorIgnorableError {
		if e.Equal(err) {
			return true
		}
	}
	return false
}

// Tick implements the `orchestrator.State` interface
// the `state` parameter is sent by the etcd worker, the `state` must be a snapshot of KVs in etcd
// The main logic of processor is in this function, including the calculation of many kinds of ts, maintain keyspan pipeline, error handling, etc.
func (p *processor) Tick(ctx cdcContext.Context, state *orchestrator.ChangefeedReactorState) (orchestrator.ReactorState, error) {
	p.changefeed = state
	state.CheckCaptureAlive(ctx.GlobalVars().CaptureInfo.ID)
	ctx = cdcContext.WithChangefeedVars(ctx, &cdcContext.ChangefeedVars{
		ID:   state.ID,
		Info: state.Info,
	})
	_, err := p.tick(ctx, state)
	if err == nil {
		return state, nil
	}
	if isProcessorIgnorableError(err) {
		log.Info("processor exited", cdcContext.ZapFieldCapture(ctx), cdcContext.ZapFieldChangefeed(ctx))
		return state, cerror.ErrReactorFinished.GenWithStackByArgs()
	}
	p.metricProcessorErrorCounter.Inc()
	// record error information in etcd
	var code string
	if rfcCode, ok := cerror.RFCCode(err); ok {
		code = string(rfcCode)
	} else {
		code = string(cerror.ErrProcessorUnknown.RFCCode())
	}
	state.PatchTaskPosition(p.captureInfo.ID, func(position *model.TaskPosition) (*model.TaskPosition, bool, error) {
		if position == nil {
			position = &model.TaskPosition{}
		}
		position.Error = &model.RunningError{
			Addr:    ctx.GlobalVars().CaptureInfo.AdvertiseAddr,
			Code:    code,
			Message: err.Error(),
		}
		return position, true, nil
	})
	log.Error("run processor failed",
		cdcContext.ZapFieldChangefeed(ctx),
		cdcContext.ZapFieldCapture(ctx),
		zap.Error(err))
	return state, cerror.ErrReactorFinished.GenWithStackByArgs()
}

func (p *processor) tick(ctx cdcContext.Context, state *orchestrator.ChangefeedReactorState) (nextState orchestrator.ReactorState, err error) {
	p.changefeed = state
	if !p.checkChangefeedNormal() {
		return nil, cerror.ErrAdminStopProcessor.GenWithStackByArgs()
	}
	// we should skip this tick after create a task position
	if p.createTaskPosition() {
		return p.changefeed, nil
	}
	if err := p.handleErrorCh(ctx); err != nil {
		return nil, errors.Trace(err)
	}
	if err := p.lazyInit(ctx); err != nil {
		return nil, errors.Trace(err)
	}
	// sink manager will return this checkpointTs to sink node if sink node resolvedTs flush failed
	p.sinkManager.UpdateChangeFeedCheckpointTs(state.Info.GetCheckpointTs(state.Status))
	if err := p.handleKeySpanOperation(ctx); err != nil {
		return nil, errors.Trace(err)
	}
	if err := p.checkKeySpansNum(ctx); err != nil {
		return nil, errors.Trace(err)
	}
	// it is no need to check the err here, because we will use
	// local time when an error return, which is acceptable
	pdTime, _ := ctx.GlobalVars().TimeAcquirer.CurrentTimeFromCached()

	p.handlePosition(oracle.GetPhysical(pdTime))
	p.pushResolvedTs2KeySpan()

	// If we wrote to the key while there are many keyspans (>10000), we would risk burdening Etcd.
	//
	// The keys will still exist but will no longer be written to
	// if we do not call handleWorkload.
	p.handleWorkload()

	return p.changefeed, nil
}

// checkChangefeedNormal checks if the changefeed is runnable.
func (p *processor) checkChangefeedNormal() bool {
	// check the state in this tick, make sure that the admin job type of the changefeed is not stopped
	if p.changefeed.Info.AdminJobType.IsStopState() || p.changefeed.Status.AdminJobType.IsStopState() {
		return false
	}
	// add a patch to check the changefeed is runnable when applying the patches in the etcd worker.
	p.changefeed.CheckChangefeedNormal()
	return true
}

// createTaskPosition will create a new task position if a task position does not exist.
// task position not exist only when the processor is running first in the first tick.
func (p *processor) createTaskPosition() (skipThisTick bool) {
	if _, exist := p.changefeed.TaskPositions[p.captureInfo.ID]; exist {
		return false
	}
	if p.initialized {
		log.Warn("position is nil, maybe position info is removed unexpected", zap.Any("state", p.changefeed))
	}
	checkpointTs := p.changefeed.Info.GetCheckpointTs(p.changefeed.Status)
	p.changefeed.PatchTaskPosition(p.captureInfo.ID, func(position *model.TaskPosition) (*model.TaskPosition, bool, error) {
		if position == nil {
			return &model.TaskPosition{
				CheckPointTs: checkpointTs,
				ResolvedTs:   checkpointTs,
			}, true, nil
		}
		return position, false, nil
	})
	return true
}

// lazyInitImpl create instances at the first tick.
func (p *processor) lazyInitImpl(ctx cdcContext.Context) error {
	if p.initialized {
		return nil
	}
	ctx, cancel := cdcContext.WithCancel(ctx)
	p.cancel = cancel

	// We don't close this error channel, since it is only safe to close channel
	// in sender, and this channel will be used in many modules including sink,
	// redo log manager, etc. Let runtime GC to recycle it.
	errCh := make(chan error, 16)
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		// there are some other objects need errCh, such as sink and sink manager
		// but we can't ensure that all the producer of errCh are non-blocking
		// It's very tricky that create a goroutine to receive the local errCh
		// TODO(leoppro): we should using `pkg/cdcContext.Context` instead of standard cdcContext and handle error by `pkg/cdcContext.Context.Throw`
		for {
			select {
			case <-ctx.Done():
				return
			case err := <-errCh:
				if err == nil {
					return
				}
				p.sendError(err)
			}
		}
	}()

	var err error
	stdCtx := util.PutChangefeedIDInCtx(ctx, p.changefeed.ID)
	stdCtx = util.PutCaptureAddrInCtx(stdCtx, p.captureInfo.AdvertiseAddr)

	opts := make(map[string]string, len(p.changefeed.Info.Opts)+2)
	for k, v := range p.changefeed.Info.Opts {
		opts[k] = v
	}

	// TODO(neil) find a better way to let sink know cyclic is enabled.
	// TODO: it's useless for tikv cdc.
	opts[sink.OptChangefeedID] = p.changefeed.ID
	opts[sink.OptCaptureAddr] = ctx.GlobalVars().CaptureInfo.AdvertiseAddr
	s, err := sink.New(stdCtx, p.changefeed.ID, p.changefeed.Info.SinkURI, p.changefeed.Info.Config, opts, errCh)
	if err != nil {
		return errors.Trace(err)
	}
	checkpointTs := p.changefeed.Info.GetCheckpointTs(p.changefeed.Status)
	captureAddr := ctx.GlobalVars().CaptureInfo.AdvertiseAddr
	p.sinkManager = sink.NewManager(stdCtx, s, errCh, checkpointTs, captureAddr, p.changefeedID)

	p.initialized = true
	log.Info("run processor", cdcContext.ZapFieldCapture(ctx), cdcContext.ZapFieldChangefeed(ctx))
	return nil
}

// handleErrorCh listen the error channel and throw the error if it is not expected.
func (p *processor) handleErrorCh(ctx cdcContext.Context) error {
	var err error
	select {
	case err = <-p.errCh:
	default:
		return nil
	}
	if !isProcessorIgnorableError(err) {
		log.Error("error on running processor",
			cdcContext.ZapFieldCapture(ctx),
			cdcContext.ZapFieldChangefeed(ctx),
			zap.Error(err))
		return err
	}
	log.Info("processor exited", cdcContext.ZapFieldCapture(ctx), cdcContext.ZapFieldChangefeed(ctx))
	return cerror.ErrReactorFinished
}

// handleKeySpanOperation handles the operation of `TaskStatus`(add keyspan operation and remove keyspan operation)
func (p *processor) handleKeySpanOperation(ctx cdcContext.Context) error {
	patchOperation := func(keyspanID model.KeySpanID, fn func(operation *model.KeySpanOperation) error) {
		p.changefeed.PatchTaskStatus(p.captureInfo.ID, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
			if status == nil || status.Operation == nil {
				log.Error("Operation not found, may be remove by other patch", zap.Uint64("keyspanID", keyspanID), zap.Any("status", status))
				return nil, false, cerror.ErrTaskStatusNotExists.GenWithStackByArgs()
			}
			opt := status.Operation[keyspanID]
			if opt == nil {
				log.Error("Operation not found, may be remove by other patch", zap.Uint64("keyspanID", keyspanID), zap.Any("status", status))
				return nil, false, cerror.ErrTaskStatusNotExists.GenWithStackByArgs()
			}
			if err := fn(opt); err != nil {
				return nil, false, errors.Trace(err)
			}
			return status, true, nil
		})
	}
	taskStatus := p.changefeed.TaskStatuses[p.captureInfo.ID]
	for keyspanID, opt := range taskStatus.Operation {
		if opt.KeySpanApplied() {
			continue
		}
		globalCheckpointTs := p.changefeed.Status.CheckpointTs
		if opt.Delete {
			keyspan, exist := p.keyspans[keyspanID]
			if !exist {
				log.Warn("keyspan which will be deleted is not found",
					cdcContext.ZapFieldChangefeed(ctx), zap.Uint64("keyspanID", keyspanID))
				patchOperation(keyspanID, func(operation *model.KeySpanOperation) error {
					operation.Status = model.OperFinished
					return nil
				})
				continue
			}
			switch opt.Status {
			case model.OperDispatched:
				if opt.BoundaryTs < globalCheckpointTs {
					log.Warn("the BoundaryTs of remove keyspan operation is smaller than global checkpoint ts", zap.Uint64("globalCheckpointTs", globalCheckpointTs), zap.Any("operation", opt))
				}
				if !keyspan.AsyncStop(opt.BoundaryTs) {
					// We use a Debug log because it is conceivable for the pipeline to block for a legitimate reason,
					// and we do not want to alarm the user.
					log.Debug("AsyncStop has failed, possible due to a full pipeline",
						zap.Uint64("checkpointTs", keyspan.CheckpointTs()), zap.Uint64("keyspanID", keyspanID))
					continue
				}
				patchOperation(keyspanID, func(operation *model.KeySpanOperation) error {
					operation.Status = model.OperProcessed
					return nil
				})
			case model.OperProcessed:
				if keyspan.Status() != keyspanpipeline.KeySpanStatusStopped {
					log.Debug("the keyspan is still not stopped", zap.Uint64("checkpointTs", keyspan.CheckpointTs()), zap.Uint64("keyspanID", keyspanID))
					continue
				}
				patchOperation(keyspanID, func(operation *model.KeySpanOperation) error {
					operation.BoundaryTs = keyspan.CheckpointTs()
					operation.Status = model.OperFinished
					return nil
				})
				p.removeKeySpan(keyspan, keyspanID)
				log.Debug("Operation done signal received",
					cdcContext.ZapFieldChangefeed(ctx),
					zap.Uint64("keyspanID", keyspanID),
					zap.Reflect("operation", opt))
			default:
				log.Panic("unreachable")
			}
		} else {
			switch opt.Status {
			case model.OperDispatched:
				replicaInfo, exist := taskStatus.KeySpans[keyspanID]
				if !exist {
					return cerror.ErrProcessorKeySpanNotFound.GenWithStack("replicaInfo of keyspan(%d)", keyspanID)
				}
				if replicaInfo.StartTs != opt.BoundaryTs {
					log.Warn("the startTs and BoundaryTs of add keyspan operation should be always equaled", zap.Any("replicaInfo", replicaInfo))
				}

				if !p.checkRelatedKeyspans(opt.RelatedKeySpans) {
					continue
				}
				err := p.addKeySpan(ctx, keyspanID, replicaInfo)
				if err != nil {
					return errors.Trace(err)
				}
				patchOperation(keyspanID, func(operation *model.KeySpanOperation) error {
					operation.Status = model.OperProcessed
					return nil
				})
			case model.OperProcessed:
				keyspan, exist := p.keyspans[keyspanID]
				if !exist {
					log.Warn("keyspan which was added is not found",
						cdcContext.ZapFieldChangefeed(ctx), zap.Uint64("keyspanID", keyspanID))
					patchOperation(keyspanID, func(operation *model.KeySpanOperation) error {
						operation.Status = model.OperDispatched
						return nil
					})
					continue
				}
				localResolvedTs := p.changefeed.TaskPositions[p.captureInfo.ID].ResolvedTs
				globalResolvedTs := p.changefeed.Status.ResolvedTs
				if keyspan.ResolvedTs() >= localResolvedTs && localResolvedTs >= globalResolvedTs {
					patchOperation(keyspanID, func(operation *model.KeySpanOperation) error {
						operation.Status = model.OperFinished
						return nil
					})
					log.Debug("Operation done signal received",
						cdcContext.ZapFieldChangefeed(ctx),
						zap.Uint64("keyspanID", keyspanID),
						zap.Reflect("operation", opt))
				}
			default:
				log.Panic("unreachable")
			}
		}
	}
	return nil
}

func (p *processor) checkRelatedKeyspans(relatedKeySpans []model.KeySpanLocation) bool {
	for _, location := range relatedKeySpans {
		if taskStatus, ok := p.changefeed.TaskStatuses[location.CaptureID]; ok {
			if operation, ok := taskStatus.Operation[location.KeySpanID]; ok && operation.Status != model.OperFinished {
				return false
			}
		}
	}
	return true
}

func (p *processor) sendError(err error) {
	if err == nil {
		return
	}
	select {
	case p.errCh <- err:
	default:
		if !isProcessorIgnorableError(err) {
			log.Error("processor receives redundant error", zap.Error(err))
		}
	}
}

// checkKeySpansNum if the number of keyspan pipelines is equal to the number of TaskStatus in etcd state.
// if the keyspan number is not right, create or remove the odd keyspans.
func (p *processor) checkKeySpansNum(ctx cdcContext.Context) error {
	taskStatus := p.changefeed.TaskStatuses[p.captureInfo.ID]
	if len(p.keyspans) == len(taskStatus.KeySpans) {
		return nil
	}
	// check if a keyspan should be listen but not
	// this only could be happened in the first tick.
	for keyspanID, replicaInfo := range taskStatus.KeySpans {
		if _, exist := p.keyspans[keyspanID]; exist {
			continue
		}
		opt := taskStatus.Operation
		// TODO(leoppro): check if the operation is a undone add operation
		if opt != nil && opt[keyspanID] != nil {
			continue
		}
		log.Info("start to listen to the keyspan immediately", zap.Uint64("keyspanID", keyspanID), zap.Any("replicaInfo", replicaInfo))
		if replicaInfo.StartTs < p.changefeed.Status.CheckpointTs {
			replicaInfo.StartTs = p.changefeed.Status.CheckpointTs
		}
		err := p.addKeySpan(ctx, keyspanID, replicaInfo)
		if err != nil {
			return errors.Trace(err)
		}
	}
	// check if a keyspan should be removed but still exist
	// this shouldn't be happened in any time.
	for keyspanID, keyspanPipeline := range p.keyspans {
		if _, exist := taskStatus.KeySpans[keyspanID]; exist {
			continue
		}
		opt := taskStatus.Operation
		if opt != nil && opt[keyspanID] != nil && opt[keyspanID].Delete {
			// keyspan will be removed by normal logic
			continue
		}
		p.removeKeySpan(keyspanPipeline, keyspanID)
		log.Warn("the keyspan was forcibly deleted", zap.Uint64("keyspanID", keyspanID), zap.Any("taskStatus", taskStatus))
	}
	return nil
}

// handlePosition calculates the local resolved ts and local checkpoint ts
func (p *processor) handlePosition(currentTs int64) {
	minResolvedTs := uint64(math.MaxUint64)

	for _, keyspan := range p.keyspans {
		ts := keyspan.ResolvedTs()
		if ts < minResolvedTs {
			minResolvedTs = ts
		}
	}

	minCheckpointTs := minResolvedTs
	for _, keyspan := range p.keyspans {
		ts := keyspan.CheckpointTs()
		if ts < minCheckpointTs {
			minCheckpointTs = ts
		}
	}

	resolvedPhyTs := oracle.ExtractPhysical(minResolvedTs)
	p.metricResolvedTsLagGauge.Set(float64(currentTs-resolvedPhyTs) / 1e3)
	p.metricResolvedTsGauge.Set(float64(resolvedPhyTs))

	checkpointPhyTs := oracle.ExtractPhysical(minCheckpointTs)
	p.metricCheckpointTsLagGauge.Set(float64(currentTs-checkpointPhyTs) / 1e3)
	p.metricCheckpointTsGauge.Set(float64(checkpointPhyTs))

	// minResolvedTs and minCheckpointTs may less than global resolved ts and global checkpoint ts when a new keyspan added, the startTs of the new keyspan is less than global checkpoint ts.
	if minResolvedTs != p.changefeed.TaskPositions[p.captureInfo.ID].ResolvedTs ||
		minCheckpointTs != p.changefeed.TaskPositions[p.captureInfo.ID].CheckPointTs {
		p.changefeed.PatchTaskPosition(p.captureInfo.ID, func(position *model.TaskPosition) (*model.TaskPosition, bool, error) {
			failpoint.Inject("ProcessorUpdatePositionDelaying", nil)
			if position == nil {
				// when the captureInfo is deleted, the old owner will delete task status, task position, task workload in non-atomic
				// so processor may see a intermediate state, for example the task status is exist but task position is deleted.
				log.Warn("task position is not exist, skip to update position", zap.String("changefeed", p.changefeed.ID))
				return nil, false, nil
			}
			position.CheckPointTs = minCheckpointTs
			position.ResolvedTs = minResolvedTs
			return position, true, nil
		})
	}
}

// handleWorkload calculates the workload of all keyspans
func (p *processor) handleWorkload() {
	p.changefeed.PatchTaskWorkload(p.captureInfo.ID, func(workloads model.TaskWorkload) (model.TaskWorkload, bool, error) {
		changed := false
		if workloads == nil {
			workloads = make(model.TaskWorkload)
		}
		for keyspanID := range workloads {
			if _, exist := p.keyspans[keyspanID]; !exist {
				delete(workloads, keyspanID)
				changed = true
			}
		}
		for keyspanID, keyspan := range p.keyspans {
			if workloads[keyspanID] != keyspan.Workload() {
				workloads[keyspanID] = keyspan.Workload()
				changed = true
			}
		}
		return workloads, changed, nil
	})
}

// pushResolvedTs2KeySpan sends global resolved ts to all the keyspan pipelines.
func (p *processor) pushResolvedTs2KeySpan() {
	resolvedTs := p.changefeed.Status.ResolvedTs
	for _, keyspan := range p.keyspans {
		keyspan.UpdateBarrierTs(resolvedTs)
	}
}

// addKeySpan creates a new keyspan pipeline and adds it to the `p.keyspans`
func (p *processor) addKeySpan(ctx cdcContext.Context, keyspanID model.KeySpanID, replicaInfo *model.KeySpanReplicaInfo) error {
	if replicaInfo.StartTs == 0 {
		replicaInfo.StartTs = p.changefeed.Status.CheckpointTs
	}

	if keyspan, ok := p.keyspans[keyspanID]; ok {
		if keyspan.Status() == keyspanpipeline.KeySpanStatusStopped {
			log.Warn("The same keyspan exists but is stopped. Cancel it and continue.", cdcContext.ZapFieldChangefeed(ctx), zap.Uint64("ID", keyspanID))
			p.removeKeySpan(keyspan, keyspanID)
		} else {
			log.Warn("Ignore existing keyspan", cdcContext.ZapFieldChangefeed(ctx), zap.Uint64("ID", keyspanID))
			return nil
		}
	}

	globalCheckpointTs := p.changefeed.Status.CheckpointTs

	if replicaInfo.StartTs < globalCheckpointTs {
		log.Warn("addKeySpan: startTs < checkpoint",
			cdcContext.ZapFieldChangefeed(ctx),
			zap.Uint64("keyspanID", keyspanID),
			zap.Uint64("checkpoint", globalCheckpointTs),
			zap.Uint64("startTs", replicaInfo.StartTs))
	}
	keyspan, err := p.createKeySpanPipeline(ctx, keyspanID, replicaInfo)
	if err != nil {
		return errors.Trace(err)
	}
	p.keyspans[keyspanID] = keyspan
	return nil
}

func (p *processor) createKeySpanPipelineImpl(ctx cdcContext.Context, keyspanID model.KeySpanID, replicaInfo *model.KeySpanReplicaInfo) (keyspanpipeline.KeySpanPipeline, error) {
	ctx = cdcContext.WithErrorHandler(ctx, func(err error) error {
		if cerror.ErrKeySpanProcessorStoppedSafely.Equal(err) ||
			errors.Cause(errors.Cause(err)) == context.Canceled {
			return nil
		}
		p.sendError(err)
		return nil
	})

	sink := p.sinkManager.CreateKeySpanSink(keyspanID, replicaInfo.StartTs)
	keyspan := keyspanpipeline.NewKeySpanPipeline(
		ctx,
		keyspanID,
		replicaInfo,
		sink,
		p.changefeed.Info.GetTargetTs(),
	)
	p.wg.Add(1)
	p.metricSyncKeySpanNumGauge.Inc()
	go func() {
		keyspan.Wait()
		p.wg.Done()
		p.metricSyncKeySpanNumGauge.Dec()
		log.Debug("KeySpan pipeline exited", zap.Uint64("keyspanID", keyspanID),
			cdcContext.ZapFieldChangefeed(ctx),
			zap.String("name", keyspan.Name()),
			zap.Any("replicaInfo", replicaInfo))
	}()

	log.Info("Add keyspan pipeline", zap.Uint64("keyspanID", keyspanID),
		cdcContext.ZapFieldChangefeed(ctx),
		zap.String("name", keyspan.Name()),
		zap.Any("replicaInfo", replicaInfo),
		zap.Uint64("globalResolvedTs", p.changefeed.Status.ResolvedTs))

	return keyspan, nil
}

func (p *processor) removeKeySpan(keyspan keyspanpipeline.KeySpanPipeline, keyspanID model.KeySpanID) {
	keyspan.Cancel()
	keyspan.Wait()
	delete(p.keyspans, keyspanID)
}

func (p *processor) Close() error {
	if !p.initialized {
		return nil
	}

	for _, tbl := range p.keyspans {
		tbl.Cancel()
	}
	for _, tbl := range p.keyspans {
		tbl.Wait()
	}
	p.cancel()
	p.wg.Wait()
	// mark keyspans share the same cdcContext with its original keyspan, don't need to cancel
	failpoint.Inject("processorStopDelay", nil)
	resolvedTsGauge.DeleteLabelValues(p.changefeedID, p.captureInfo.AdvertiseAddr)
	resolvedTsLagGauge.DeleteLabelValues(p.changefeedID, p.captureInfo.AdvertiseAddr)
	checkpointTsGauge.DeleteLabelValues(p.changefeedID, p.captureInfo.AdvertiseAddr)
	checkpointTsLagGauge.DeleteLabelValues(p.changefeedID, p.captureInfo.AdvertiseAddr)
	syncKeySpanNumGauge.DeleteLabelValues(p.changefeedID, p.captureInfo.AdvertiseAddr)
	processorErrorCounter.DeleteLabelValues(p.changefeedID, p.captureInfo.AdvertiseAddr)
	// processorSchemaStorageGcTsGauge.DeleteLabelValues(p.changefeedID, p.captureInfo.AdvertiseAddr)
	if p.sinkManager != nil {
		// pass a canceled context is ok here, since we don't need to wait Close
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := p.sinkManager.Close(ctx); err != nil {
			return errors.Trace(err)
		}
	}

	p.initialized = false
	return nil
}

// WriteDebugInfo write the debug info to Writer
func (p *processor) WriteDebugInfo(w io.Writer) {
	fmt.Fprintf(w, "%+v\n", *p.changefeed)
	for keyspanID, keyspanPipeline := range p.keyspans {
		fmt.Fprintf(w, "keyspanID: %d, keyspanName: %s, resolvedTs: %d, checkpointTs: %d, status: %s\n",
			keyspanID, keyspanPipeline.Name(), keyspanPipeline.ResolvedTs(), keyspanPipeline.CheckpointTs(), keyspanPipeline.Status())
	}
}
