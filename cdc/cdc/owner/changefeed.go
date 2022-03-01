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

package owner

import (
	"context"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/migration/cdc/cdc/model"
	schedulerv2 "github.com/tikv/migration/cdc/cdc/scheduler"
	cdcContext "github.com/tikv/migration/cdc/pkg/context"
	cerror "github.com/tikv/migration/cdc/pkg/errors"
	"github.com/tikv/migration/cdc/pkg/orchestrator"
	"github.com/tikv/migration/cdc/pkg/txnutil/gc"
	"github.com/tikv/migration/cdc/pkg/util"
	"go.uber.org/zap"
)

const (
	defaultErrChSize = 1024
)

type changefeed struct {
	id    model.ChangeFeedID
	state *orchestrator.ChangefeedReactorState

	scheduler        scheduler
	feedStateManager *feedStateManager
	gcManager        gc.Manager

	initialized bool
	// isRemoved is true if the changefeed is removed
	isRemoved bool

	errCh chan error
	// cancel the running goroutine start by `DDLPuller`
	cancel context.CancelFunc

	// The changefeed will start some backend goroutines in the function `initialize`,
	// such as DDLPuller, DDLSink, etc.
	// `wg` is used to manage those backend goroutines.
	wg sync.WaitGroup

	metricsChangefeedCheckpointTsGauge    prometheus.Gauge
	metricsChangefeedCheckpointTsLagGauge prometheus.Gauge
	metricsChangefeedResolvedTsGauge      prometheus.Gauge
	metricsChangefeedResolvedTsLagGauge   prometheus.Gauge

	newScheduler func(ctx cdcContext.Context, startTs uint64) (scheduler, error)
}

func newChangefeed(id model.ChangeFeedID, gcManager gc.Manager) *changefeed {
	c := &changefeed{
		id: id,
		// The scheduler will be created lazily.
		scheduler:        nil,
		feedStateManager: new(feedStateManager),
		gcManager:        gcManager,

		errCh:  make(chan error, defaultErrChSize),
		cancel: func() {},
	}
	c.newScheduler = newScheduler
	return c
}

func newChangefeed4Test(id model.ChangeFeedID, gcManager gc.Manager) *changefeed {
	c := newChangefeed(id, gcManager)
	return c
}

func (c *changefeed) Tick(ctx cdcContext.Context, state *orchestrator.ChangefeedReactorState, captures map[model.CaptureID]*model.CaptureInfo) {
	ctx = cdcContext.WithErrorHandler(ctx, func(err error) error {
		c.errCh <- errors.Trace(err)
		return nil
	})
	state.CheckCaptureAlive(ctx.GlobalVars().CaptureInfo.ID)
	if err := c.tick(ctx, state, captures); err != nil {
		log.Error("an error occurred in Owner", zap.String("changefeedID", c.state.ID), zap.Error(err))
		var code string
		if rfcCode, ok := cerror.RFCCode(err); ok {
			code = string(rfcCode)
		} else {
			code = string(cerror.ErrOwnerUnknown.RFCCode())
		}
		c.feedStateManager.handleError(&model.RunningError{
			Addr:    util.CaptureAddrFromCtx(ctx),
			Code:    code,
			Message: err.Error(),
		})
		c.releaseResources(ctx)
	}
}

func (c *changefeed) checkStaleCheckpointTs(ctx cdcContext.Context, checkpointTs uint64) error {
	state := c.state.Info.State
	if state == model.StateNormal || state == model.StateStopped || state == model.StateError {
		failpoint.Inject("InjectChangefeedFastFailError", func() error {
			return cerror.ErrGCTTLExceeded.FastGen("InjectChangefeedFastFailError")
		})
		if err := c.gcManager.CheckStaleCheckpointTs(ctx, c.id, checkpointTs); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (c *changefeed) tick(ctx cdcContext.Context, state *orchestrator.ChangefeedReactorState, captures map[model.CaptureID]*model.CaptureInfo) error {
	c.state = state
	c.feedStateManager.Tick(state)

	checkpointTs := c.state.Info.GetCheckpointTs(c.state.Status)
	// check stale checkPointTs must be called before `feedStateManager.ShouldRunning()`
	// to ensure an error or stopped changefeed also be checked
	if err := c.checkStaleCheckpointTs(ctx, checkpointTs); err != nil {
		return errors.Trace(err)
	}

	if !c.feedStateManager.ShouldRunning() {
		c.isRemoved = c.feedStateManager.ShouldRemoved()
		c.releaseResources(ctx)
		return nil
	}

	if !c.preflightCheck(captures) {
		return nil
	}
	if err := c.initialize(ctx); err != nil {
		return errors.Trace(err)
	}

	select {
	case err := <-c.errCh:
		return errors.Trace(err)
	default:
	}

	pdCli := ctx.GlobalVars().PDClient
	regions, err := pdCli.ScanRegions(context.Background(), nil, nil, 10000)
	if err != nil {
		return errors.Trace(err)
	}

	keyspanIDs := make([]model.KeySpanID, 0, len(regions))
	for _, region := range regions {
		keyspanIDs = append(keyspanIDs, region.Meta.Id)
	}

	newCheckpointTs, newResolvedTs, err := c.scheduler.Tick(ctx, c.state, keyspanIDs, captures)
	if err != nil {
		return errors.Trace(err)
	}
	// CheckpointCannotProceed implies that not all tables are being replicated normally,
	// so in that case there is no need to advance the global watermarks.
	if newCheckpointTs != schedulerv2.CheckpointCannotProceed {
		pdTime, _ := ctx.GlobalVars().TimeAcquirer.CurrentTimeFromCached()
		currentTs := oracle.GetPhysical(pdTime)
		c.updateStatus(currentTs, newCheckpointTs, newResolvedTs)
	}
	return nil
}

func (c *changefeed) initialize(ctx cdcContext.Context) error {
	if c.initialized {
		return nil
	}
	// clean the errCh
	// When the changefeed is resumed after being stopped, the changefeed instance will be reused,
	// So we should make sure that the errCh is empty when the changefeed is restarting
LOOP:
	for {
		select {
		case <-c.errCh:
		default:
			break LOOP
		}
	}
	checkpointTs := c.state.Info.GetCheckpointTs(c.state.Status)
	log.Info("initialize changefeed", zap.String("changefeed", c.state.ID),
		zap.Stringer("info", c.state.Info),
		zap.Uint64("checkpoint ts", checkpointTs))
	failpoint.Inject("NewChangefeedNoRetryError", func() {
		failpoint.Return(cerror.ErrStartTsBeforeGC.GenWithStackByArgs(checkpointTs-300, checkpointTs))
	})
	failpoint.Inject("NewChangefeedRetryError", func() {
		failpoint.Return(errors.New("failpoint injected retriable error"))
	})
	if c.state.Info.Config.CheckGCSafePoint {
		// Check TiDB GC safepoint does not exceed the checkpoint.
		//
		// We update TTL to 10 minutes,
		//  1. to delete the service GC safepoint effectively,
		//  2. in case owner update TiCDC service GC safepoint fails.
		//
		// Also it unblocks TiDB GC, because the service GC safepoint is set to
		// 1 hour TTL during creating changefeed.
		//
		// See more gc doc.
		ensureTTL := int64(10 * 60)
		err := gc.EnsureChangefeedStartTsSafety(
			ctx, ctx.GlobalVars().PDClient, c.state.ID, ensureTTL, checkpointTs)
		if err != nil {
			return errors.Trace(err)
		}
	}

	var err error

	// init metrics
	c.metricsChangefeedCheckpointTsGauge = changefeedCheckpointTsGauge.WithLabelValues(c.id)
	c.metricsChangefeedCheckpointTsLagGauge = changefeedCheckpointTsLagGauge.WithLabelValues(c.id)
	c.metricsChangefeedResolvedTsGauge = changefeedResolvedTsGauge.WithLabelValues(c.id)
	c.metricsChangefeedResolvedTsLagGauge = changefeedResolvedTsLagGauge.WithLabelValues(c.id)

	// create scheduler
	c.scheduler, err = c.newScheduler(ctx, checkpointTs)
	if err != nil {
		return errors.Trace(err)
	}

	c.initialized = true
	return nil
}

func (c *changefeed) releaseResources(ctx cdcContext.Context) {
	if !c.initialized {
		return
	}
	log.Info("close changefeed", zap.String("changefeed", c.state.ID),
		zap.Stringer("info", c.state.Info), zap.Bool("isRemoved", c.isRemoved))
	c.cancel()
	c.cancel = func() {}
	_, cancel := context.WithCancel(context.Background())
	cancel()
	c.wg.Wait()
	c.scheduler.Close(ctx)

	changefeedCheckpointTsGauge.DeleteLabelValues(c.id)
	changefeedCheckpointTsLagGauge.DeleteLabelValues(c.id)
	c.metricsChangefeedCheckpointTsGauge = nil
	c.metricsChangefeedCheckpointTsLagGauge = nil

	changefeedResolvedTsGauge.DeleteLabelValues(c.id)
	changefeedResolvedTsLagGauge.DeleteLabelValues(c.id)
	c.metricsChangefeedResolvedTsGauge = nil
	c.metricsChangefeedResolvedTsLagGauge = nil

	c.initialized = false
}

// preflightCheck makes sure that the metadata in Etcd is complete enough to run the tick.
// If the metadata is not complete, such as when the ChangeFeedStatus is nil,
// this function will reconstruct the lost metadata and skip this tick.
func (c *changefeed) preflightCheck(captures map[model.CaptureID]*model.CaptureInfo) (ok bool) {
	ok = true
	if c.state.Status == nil {
		c.state.PatchStatus(func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
			if status == nil {
				status = &model.ChangeFeedStatus{
					// the changefeed status is nil when the changefeed is just created.
					ResolvedTs:   c.state.Info.StartTs,
					CheckpointTs: c.state.Info.StartTs,
					AdminJobType: model.AdminNone,
				}
				return status, true, nil
			}
			return status, false, nil
		})
		ok = false
	}
	for captureID := range captures {
		if _, exist := c.state.TaskStatuses[captureID]; !exist {
			c.state.PatchTaskStatus(captureID, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
				if status == nil {
					status = new(model.TaskStatus)
					return status, true, nil
				}
				return status, false, nil
			})
			ok = false
		}
	}
	for captureID := range c.state.TaskStatuses {
		if _, exist := captures[captureID]; !exist {
			c.state.PatchTaskStatus(captureID, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
				return nil, status != nil, nil
			})
			ok = false
		}
	}

	for captureID := range c.state.TaskPositions {
		if _, exist := captures[captureID]; !exist {
			c.state.PatchTaskPosition(captureID, func(position *model.TaskPosition) (*model.TaskPosition, bool, error) {
				return nil, position != nil, nil
			})
			ok = false
		}
	}
	for captureID := range c.state.Workloads {
		if _, exist := captures[captureID]; !exist {
			c.state.PatchTaskWorkload(captureID, func(workload model.TaskWorkload) (model.TaskWorkload, bool, error) {
				return nil, workload != nil, nil
			})
			ok = false
		}
	}
	return
}

func (c *changefeed) updateStatus(currentTs int64, checkpointTs, resolvedTs model.Ts) {
	c.state.PatchStatus(func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
		changed := false
		if status == nil {
			return nil, changed, nil
		}
		if status.ResolvedTs != resolvedTs {
			status.ResolvedTs = resolvedTs
			changed = true
		}
		if status.CheckpointTs != checkpointTs {
			status.CheckpointTs = checkpointTs
			changed = true
		}
		return status, changed, nil
	})
	phyCkpTs := oracle.ExtractPhysical(checkpointTs)
	c.metricsChangefeedCheckpointTsGauge.Set(float64(phyCkpTs))
	c.metricsChangefeedCheckpointTsLagGauge.Set(float64(currentTs-phyCkpTs) / 1e3)

	phyRTs := oracle.ExtractPhysical(resolvedTs)
	c.metricsChangefeedResolvedTsGauge.Set(float64(phyRTs))
	c.metricsChangefeedResolvedTsLagGauge.Set(float64(currentTs-phyRTs) / 1e3)
}

func (c *changefeed) Close(ctx cdcContext.Context) {
	c.releaseResources(ctx)
}

func (c *changefeed) GetInfoProvider() schedulerv2.InfoProvider {
	if provider, ok := c.scheduler.(schedulerv2.InfoProvider); ok {
		return provider
	}
	return nil
}
