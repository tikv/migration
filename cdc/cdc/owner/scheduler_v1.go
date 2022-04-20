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
	"math"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/migration/cdc/cdc/model"
	schedulerv2 "github.com/tikv/migration/cdc/cdc/scheduler"
	cdcContext "github.com/tikv/migration/cdc/pkg/context"
	cerror "github.com/tikv/migration/cdc/pkg/errors"
	"github.com/tikv/migration/cdc/pkg/orchestrator"
	"github.com/tikv/migration/cdc/pkg/regionspan"
	"go.uber.org/zap"
)

type schedulerJobType string

const (
	schedulerJobTypeAddKeySpan    schedulerJobType = "ADD"
	schedulerJobTypeRemoveKeySpan schedulerJobType = "REMOVE"
)

type schedulerJob struct {
	Tp        schedulerJobType
	KeySpanID model.KeySpanID
	Start     []byte
	End       []byte
	// if the operation is a delete operation, boundaryTs is checkpoint ts
	// if the operation is an add operation, boundaryTs is start ts
	BoundaryTs    uint64
	TargetCapture model.CaptureID

	RelatedKeySpans []model.KeySpanID
}

type moveKeySpanJob struct {
	keyspanID model.KeySpanID
	target    model.CaptureID
}

type oldScheduler struct {
	state             *orchestrator.ChangefeedReactorState
	currentKeySpanIDs []model.KeySpanID
	currentKeySpans   map[model.KeySpanID]regionspan.Span
	captures          map[model.CaptureID]*model.CaptureInfo

	moveKeySpanTargets    map[model.KeySpanID]model.CaptureID
	moveKeySpanJobQueue   []*moveKeySpanJob
	needRebalanceNextTick bool
	lastTickCaptureCount  int

	updateCurrentKeySpans func(ctx cdcContext.Context) ([]model.KeySpanID, map[model.KeySpanID]regionspan.Span, error)
}

func newSchedulerV1() scheduler {
	return &schedulerV1CompatWrapper{&oldScheduler{
		moveKeySpanTargets:    make(map[model.KeySpanID]model.CaptureID),
		updateCurrentKeySpans: updateCurrentKeySpansImpl,
	}}
}

// Tick is the main function of scheduler. It dispatches keyspans to captures and handles move-keyspan and rebalance events.
// Tick returns a bool representing whether the changefeed's state can be updated in this tick.
// The state can be updated only if all the keyspans which should be listened to have been dispatched to captures and no operations have been sent to captures in this tick.
func (s *oldScheduler) Tick(
	ctx cdcContext.Context,
	state *orchestrator.ChangefeedReactorState,
	// currentKeySpans []model.KeySpanID,
	captures map[model.CaptureID]*model.CaptureInfo,
) (shouldUpdateState bool, err error) {

	s.state = state
	currentKeySpanIDs, currentKeySpans, err := s.updateCurrentKeySpans(ctx)
	if err != nil {
		return false, errors.Trace(err)
	}

	relatedKeySpans := s.computeRelatedKeySpans(currentKeySpans)
	s.currentKeySpanIDs, s.currentKeySpans = currentKeySpanIDs, currentKeySpans

	s.cleanUpFinishedOperations()
	pendingJob, err := s.syncKeySpansWithCurrentKeySpans(relatedKeySpans)
	if err != nil {
		return false, errors.Trace(err)
	}
	s.dispatchToTargetCaptures(pendingJob)
	if len(pendingJob) != 0 {
		log.Debug("scheduler:generated pending job to be executed", zap.Any("pendingJob", pendingJob))
	}
	s.handleJobs(pendingJob)

	// only if the pending job list is empty and no keyspan is being rebalanced or moved,
	// can the global resolved ts and checkpoint ts be updated
	shouldUpdateState = len(pendingJob) == 0
	shouldUpdateState = s.rebalance() && shouldUpdateState
	shouldUpdateStateInMoveKeySpan, err := s.handleMoveKeySpanJob()
	if err != nil {
		return false, errors.Trace(err)
	}
	shouldUpdateState = shouldUpdateStateInMoveKeySpan && shouldUpdateState
	s.lastTickCaptureCount = len(captures)
	return shouldUpdateState, nil
}

func (s *oldScheduler) computeRelatedKeySpans(currentKeySpans map[model.KeySpanID]regionspan.Span) map[model.KeySpanID][]model.KeySpanID {
	oldKeySpans := s.currentKeySpans

	newKeySpans := []model.KeySpanID{}
	needRemovedKeySpans := []model.KeySpanID{}

	for keyspanID := range oldKeySpans {
		if _, ok := currentKeySpans[keyspanID]; !ok {
			needRemovedKeySpans = append(needRemovedKeySpans, keyspanID)
		}
	}

	for keyspanID := range currentKeySpans {
		if _, ok := oldKeySpans[keyspanID]; !ok {
			newKeySpans = append(newKeySpans, keyspanID)
		}
	}

	relatedKeySpans := map[model.KeySpanID][]model.KeySpanID{}
	for _, keyspanID := range newKeySpans {
		relatedKeySpans[keyspanID] = needRemovedKeySpans
	}

	return relatedKeySpans
}

func (s *oldScheduler) MoveKeySpan(keyspanID model.KeySpanID, target model.CaptureID) {
	s.moveKeySpanJobQueue = append(s.moveKeySpanJobQueue, &moveKeySpanJob{
		keyspanID: keyspanID,
		target:    target,
	})
}

// handleMoveKeySpanJob handles the move keyspan job add be MoveKeySpan function
func (s *oldScheduler) handleMoveKeySpanJob() (shouldUpdateState bool, err error) {
	shouldUpdateState = true
	if len(s.moveKeySpanJobQueue) == 0 {
		return
	}
	keyspan2CaptureIndex, err := s.keyspan2CaptureIndex()
	if err != nil {
		return false, errors.Trace(err)
	}
	for _, job := range s.moveKeySpanJobQueue {
		source, exist := keyspan2CaptureIndex[job.keyspanID]
		if !exist {
			return
		}
		s.moveKeySpanTargets[job.keyspanID] = job.target
		job := job
		shouldUpdateState = false
		// for all move keyspan job, here just remove the keyspan from the source capture.
		// and the keyspan removed by this function will be added to target capture by syncKeySpansWithCurrentKeySpans in the next tick.
		s.state.PatchTaskStatus(source, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
			if status == nil {
				// the capture may be down, just skip remove this keyspan
				return status, false, nil
			}
			if status.Operation != nil && status.Operation[job.keyspanID] != nil {
				// skip removing this keyspan to avoid the remove operation created by the rebalance function interfering with the operation created by another function
				return status, false, nil
			}
			status.RemoveKeySpan(job.keyspanID, s.state.Status.CheckpointTs, false)
			return status, true, nil
		})
	}
	s.moveKeySpanJobQueue = nil
	return
}

func (s *oldScheduler) Rebalance() {
	s.needRebalanceNextTick = true
}

func (s *oldScheduler) keyspan2CaptureIndex() (map[model.KeySpanID]model.CaptureID, error) {
	keyspan2CaptureIndex := make(map[model.KeySpanID]model.CaptureID)
	for captureID, taskStatus := range s.state.TaskStatuses {
		for keyspanID := range taskStatus.KeySpans {
			if preCaptureID, exist := keyspan2CaptureIndex[keyspanID]; exist && preCaptureID != captureID {
				return nil, cerror.ErrKeySpanListenReplicated.GenWithStackByArgs(keyspanID, preCaptureID, captureID)
			}
			keyspan2CaptureIndex[keyspanID] = captureID
		}
		for keyspanID := range taskStatus.Operation {
			if preCaptureID, exist := keyspan2CaptureIndex[keyspanID]; exist && preCaptureID != captureID {
				return nil, cerror.ErrKeySpanListenReplicated.GenWithStackByArgs(keyspanID, preCaptureID, captureID)
			}
			keyspan2CaptureIndex[keyspanID] = captureID
		}
	}
	return keyspan2CaptureIndex, nil
}

// dispatchToTargetCaptures sets the TargetCapture of scheduler jobs
// If the TargetCapture of a job is not set, it chooses a capture with the minimum workload(minimum number of keyspans)
// and sets the TargetCapture to the capture.
func (s *oldScheduler) dispatchToTargetCaptures(pendingJobs []*schedulerJob) {
	workloads := make(map[model.CaptureID]uint64)

	for captureID := range s.captures {
		workloads[captureID] = 0
		taskWorkload := s.state.Workloads[captureID]
		if taskWorkload == nil {
			continue
		}
		for _, workload := range taskWorkload {
			workloads[captureID] += workload.Workload
		}
	}

	for _, pendingJob := range pendingJobs {
		if pendingJob.TargetCapture == "" {
			target, exist := s.moveKeySpanTargets[pendingJob.KeySpanID]
			if !exist {
				continue
			}
			pendingJob.TargetCapture = target
			delete(s.moveKeySpanTargets, pendingJob.KeySpanID)
			continue
		}
		switch pendingJob.Tp {
		case schedulerJobTypeAddKeySpan:
			workloads[pendingJob.TargetCapture] += 1
		case schedulerJobTypeRemoveKeySpan:
			workloads[pendingJob.TargetCapture] -= 1
		default:
			log.Panic("Unreachable, please report a bug",
				zap.String("changefeed", s.state.ID), zap.Any("job", pendingJob))
		}
	}

	getMinWorkloadCapture := func() model.CaptureID {
		minCapture := ""
		minWorkLoad := uint64(math.MaxUint64)
		for captureID, workload := range workloads {
			if workload < minWorkLoad {
				minCapture = captureID
				minWorkLoad = workload
			}
		}

		if minCapture == "" {
			log.Panic("Unreachable, no capture is found")
		}
		return minCapture
	}

	for _, pendingJob := range pendingJobs {
		if pendingJob.TargetCapture != "" {
			continue
		}
		minCapture := getMinWorkloadCapture()
		pendingJob.TargetCapture = minCapture
		workloads[minCapture] += 1
	}
}

// syncKeySpansWithCurrentKeySpans iterates all current keyspans to check whether it should be listened or not.
// this function will return schedulerJob to make sure all keyspans will be listened.
func (s *oldScheduler) syncKeySpansWithCurrentKeySpans(relatedKeySpans map[model.KeySpanID][]model.KeySpanID) ([]*schedulerJob, error) {
	var pendingJob []*schedulerJob
	allKeySpanListeningNow, err := s.keyspan2CaptureIndex()
	if err != nil {
		return nil, errors.Trace(err)
	}
	globalCheckpointTs := s.state.Status.CheckpointTs
	for _, keyspanID := range s.currentKeySpanIDs {
		if _, exist := allKeySpanListeningNow[keyspanID]; exist {
			delete(allKeySpanListeningNow, keyspanID)
			continue
		}
		// For each keyspan which should be listened but is not, add an adding-keyspan job to the pending job list
		pendingJob = append(pendingJob, &schedulerJob{
			Tp:              schedulerJobTypeAddKeySpan,
			KeySpanID:       keyspanID,
			Start:           s.currentKeySpans[keyspanID].Start,
			End:             s.currentKeySpans[keyspanID].End,
			BoundaryTs:      globalCheckpointTs,
			RelatedKeySpans: relatedKeySpans[keyspanID],
		})
	}
	// The remaining keyspans are the keyspans which should be not listened
	keyspansThatShouldNotBeListened := allKeySpanListeningNow
	for keyspanID, captureID := range keyspansThatShouldNotBeListened {
		opts := s.state.TaskStatuses[captureID].Operation
		if opts != nil && opts[keyspanID] != nil && opts[keyspanID].Delete {
			// the keyspan is being removed, skip
			continue
		}
		pendingJob = append(pendingJob, &schedulerJob{
			Tp:            schedulerJobTypeRemoveKeySpan,
			KeySpanID:     keyspanID,
			BoundaryTs:    globalCheckpointTs,
			TargetCapture: captureID,
		})
	}
	return pendingJob, nil
}

func (s *oldScheduler) handleJobs(jobs []*schedulerJob) {
	for _, job := range jobs {
		job := job
		s.state.PatchTaskStatus(job.TargetCapture, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
			switch job.Tp {
			case schedulerJobTypeAddKeySpan:
				if status == nil {
					// if task status is not found, we can just skip adding the adding-keyspan operation, since this keyspan will be added in the next tick
					log.Warn("task status of the capture is not found, may be the capture is already down. specify a new capture and redo the job", zap.Any("job", job))
					return status, false, nil
				}
				status.AddKeySpan(job.KeySpanID, &model.KeySpanReplicaInfo{
					StartTs: job.BoundaryTs,
					Start:   job.Start,
					End:     job.End,
				}, job.BoundaryTs, job.RelatedKeySpans)
			case schedulerJobTypeRemoveKeySpan:
				failpoint.Inject("OwnerRemoveKeySpanError", func() {
					// just skip removing this keyspan
					failpoint.Return(status, false, nil)
				})
				if status == nil {
					log.Warn("Task status of the capture is not found. Maybe the capture is already down. Specify a new capture and redo the job", zap.Any("job", job))
					return status, false, nil
				}
				status.RemoveKeySpan(job.KeySpanID, job.BoundaryTs, false)
			default:
				log.Panic("Unreachable, please report a bug", zap.Any("job", job))
			}
			return status, true, nil
		})
	}
}

// cleanUpFinishedOperations clean up the finished operations.
func (s *oldScheduler) cleanUpFinishedOperations() {
	for captureID := range s.state.TaskStatuses {
		s.state.PatchTaskStatus(captureID, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
			if status == nil {
				log.Warn("task status of the capture is not found, may be the key in etcd was deleted", zap.String("captureID", captureID), zap.String("changeFeedID", s.state.ID))
				return status, false, nil
			}

			changed := false
			if status == nil {
				return nil, changed, nil
			}
			for keyspanID, operation := range status.Operation {
				if operation.Status == model.OperFinished {
					delete(status.Operation, keyspanID)
					changed = true
				}
			}
			return status, changed, nil
		})
	}
}

func (s *oldScheduler) rebalance() (shouldUpdateState bool) {
	if !s.shouldRebalance() {
		// if no keyspan is rebalanced, we can update the resolved ts and checkpoint ts
		return true
	}
	// we only support rebalance by keyspan number for now
	return s.rebalanceByKeySpanNum()
}

func (s *oldScheduler) shouldRebalance() bool {
	if s.needRebalanceNextTick {
		s.needRebalanceNextTick = false
		return true
	}
	if s.lastTickCaptureCount != len(s.captures) {
		// a new capture online and no keyspan distributed to the capture
		// or some captures offline
		return true
	}
	// TODO periodic trigger rebalance
	return false
}

// rebalanceByKeySpanNum removes keyspans from captures replicating an above-average number of keyspans.
// the removed keyspan will be dispatched again by syncKeySpansWithCurrentKeySpans function
func (s *oldScheduler) rebalanceByKeySpanNum() (shouldUpdateState bool) {
	totalKeySpanNum := len(s.currentKeySpans)
	captureNum := len(s.captures)
	upperLimitPerCapture := int(math.Ceil(float64(totalKeySpanNum) / float64(captureNum)))
	shouldUpdateState = true

	log.Info("Start rebalancing",
		zap.String("changefeed", s.state.ID),
		zap.Int("keyspan-num", totalKeySpanNum),
		zap.Int("capture-num", captureNum),
		zap.Int("target-limit", upperLimitPerCapture))

	for captureID, taskStatus := range s.state.TaskStatuses {
		keyspanNum2Remove := len(taskStatus.KeySpans) - upperLimitPerCapture
		if keyspanNum2Remove <= 0 {
			continue
		}

		// here we pick `keyspanNum2Remove` keyspans to delete,
		// and then the removed keyspans will be dispatched by `syncKeySpansWithCurrentKeySpans` function in the next tick
		for keyspanID := range taskStatus.KeySpans {
			keyspanID := keyspanID
			if keyspanNum2Remove <= 0 {
				break
			}
			shouldUpdateState = false
			s.state.PatchTaskStatus(captureID, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
				if status == nil {
					// the capture may be down, just skip remove this keyspan
					return status, false, nil
				}
				if status.Operation != nil && status.Operation[keyspanID] != nil {
					// skip remove this keyspan to avoid the remove operation created by rebalance function to influence the operation created by other function
					return status, false, nil
				}
				status.RemoveKeySpan(keyspanID, s.state.Status.CheckpointTs, false)
				log.Info("Rebalance: Move keyspan",
					zap.Uint64("keyspan-id", keyspanID),
					zap.String("capture", captureID),
					zap.String("changefeed-id", s.state.ID))
				return status, true, nil
			})
			keyspanNum2Remove--
		}
	}
	return
}

func updateCurrentKeySpansImpl(ctx cdcContext.Context) ([]model.KeySpanID, map[model.KeySpanID]regionspan.Span, error) {
	limit := -1
	tikvRequestMaxBackoff := 20000
	bo := tikv.NewBackoffer(ctx, tikvRequestMaxBackoff)

	regionCache := ctx.GlobalVars().RegionCache
	regions, err := regionCache.BatchLoadRegionsWithKeyRange(bo, []byte{regionspan.RawKvStartKey}, []byte{regionspan.RawKvEndKey}, limit)
	if err != nil {
		return nil, nil, err
	}

	currentKeySpans := map[model.KeySpanID]regionspan.Span{}
	currentKeySpansID := make([]model.KeySpanID, 0, len(regions))
	for i, region := range regions {
		startKey := region.StartKey()
		endKey := region.EndKey()

		if i == 0 {
			startKey = []byte{regionspan.RawKvStartKey}
		}
		if i == len(regions)-1 {
			endKey = []byte{regionspan.RawKvEndKey}
		}

		keyspan := regionspan.Span{Start: startKey, End: endKey}
		id := region.GetID()

		currentKeySpans[id] = keyspan
		currentKeySpansID = append(currentKeySpansID, id)
	}

	return currentKeySpansID, currentKeySpans, nil
}

// schedulerV1CompatWrapper is used to wrap the old scheduler to
// support the compatibility with the new scheduler.
// It incorporates watermark calculations into the scheduler, which
// is the same design as the new scheduler.
type schedulerV1CompatWrapper struct {
	inner *oldScheduler
}

func (w *schedulerV1CompatWrapper) Tick(
	ctx cdcContext.Context,
	state *orchestrator.ChangefeedReactorState,
	// currentKeySpans []model.KeySpanID,
	captures map[model.CaptureID]*model.CaptureInfo,
) (newCheckpointTs, newResolvedTs model.Ts, err error) {

	shouldUpdateState, err := w.inner.Tick(ctx, state, captures)
	if err != nil {
		return schedulerv2.CheckpointCannotProceed, schedulerv2.CheckpointCannotProceed, err
	}

	if !shouldUpdateState {
		return schedulerv2.CheckpointCannotProceed, schedulerv2.CheckpointCannotProceed, nil
	}

	checkpointTs, resolvedTs := w.calculateWatermarks(state)
	return checkpointTs, resolvedTs, nil
}

func (w *schedulerV1CompatWrapper) MoveKeySpan(keyspanID model.KeySpanID, target model.CaptureID) {
	w.inner.MoveKeySpan(keyspanID, target)
}

func (w *schedulerV1CompatWrapper) Rebalance() {
	w.inner.Rebalance()
}

func (w *schedulerV1CompatWrapper) Close(_ cdcContext.Context) {
	// No-op for the old scheduler
}

func (w *schedulerV1CompatWrapper) calculateWatermarks(
	state *orchestrator.ChangefeedReactorState,
) (newCheckpointTs, newResolvedTs model.Ts) {
	resolvedTs := model.Ts(math.MaxUint64)

	for _, position := range state.TaskPositions {
		if resolvedTs > position.ResolvedTs {
			resolvedTs = position.ResolvedTs
		}
	}
	for _, taskStatus := range state.TaskStatuses {
		for _, opt := range taskStatus.Operation {
			if resolvedTs > opt.BoundaryTs {
				resolvedTs = opt.BoundaryTs
			}
		}
	}
	checkpointTs := resolvedTs
	for _, position := range state.TaskPositions {
		if checkpointTs > position.CheckPointTs {
			checkpointTs = position.CheckPointTs
		}
	}

	return checkpointTs, resolvedTs
}
