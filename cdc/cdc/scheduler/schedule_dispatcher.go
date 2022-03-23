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

package scheduler

import (
	"math"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/tikv/migration/cdc/cdc/model"
	"github.com/tikv/migration/cdc/cdc/scheduler/util"
	"github.com/tikv/migration/cdc/pkg/context"
	"go.uber.org/zap"
)

const (
	// CheckpointCannotProceed is a placeholder indicating that the
	// Owner should not advance the global checkpoint TS just yet.
	CheckpointCannotProceed = model.Ts(0)
)

// ScheduleDispatcher is an interface for a keyspan scheduler used in Owner.
type ScheduleDispatcher interface {
	// Tick is called periodically to update the SchedulerDispatcher on the latest state of replication.
	// This function should NOT be assumed to be thread-safe. No concurrent calls allowed.
	Tick(
		ctx context.Context,
		checkpointTs model.Ts, // Latest global checkpoint of the changefeed
		currentKeySpans []model.KeySpanID, // All keyspans that SHOULD be replicated (or started) at the current checkpoint.
		captures map[model.CaptureID]*model.CaptureInfo, // All captures that are alive according to the latest Etcd states.
	) (newCheckpointTs, newResolvedTs model.Ts, err error)

	// MoveKeySpan requests that a keyspan be moved to target.
	// It should be thread-safe.
	MoveKeySpan(keyspanID model.KeySpanID, target model.CaptureID)

	// Rebalance triggers a rebalance operation.
	// It should be thread-safe
	Rebalance()
}

// ScheduleDispatcherCommunicator is an interface for the BaseScheduleDispatcher to
// send commands to Processors. The owner of a BaseScheduleDispatcher should provide
// an implementation of ScheduleDispatcherCommunicator to supply BaseScheduleDispatcher
// some methods to specify its behavior.
type ScheduleDispatcherCommunicator interface {
	// DispatchKeySpan should send a dispatch command to the Processor.
	DispatchKeySpan(ctx context.Context,
		changeFeedID model.ChangeFeedID,
		keyspanID model.KeySpanID,
		captureID model.CaptureID,
		isDelete bool, // True when we want to remove a keyspan from the capture.
	) (done bool, err error)

	// Announce announces to the specified capture that the current node has become the Owner.
	Announce(ctx context.Context,
		changeFeedID model.ChangeFeedID,
		captureID model.CaptureID) (done bool, err error)
}

const (
	// captureCountUninitialized is a placeholder for an unknown total capture count.
	captureCountUninitialized = -1
)

// BaseScheduleDispatcher implements the basic logic of a ScheduleDispatcher.
// For it to be directly useful to the Owner, the Owner should implement it own
// ScheduleDispatcherCommunicator.
type BaseScheduleDispatcher struct {
	mu            sync.Mutex
	keyspans      *util.KeySpanSet                       // information of all actually running kespans
	captures      map[model.CaptureID]*model.CaptureInfo // basic information of all captures
	captureStatus map[model.CaptureID]*captureStatus     // more information on the captures
	checkpointTs  model.Ts                               // current checkpoint-ts

	moveKeySpanManager moveKeySpanManager
	balancer           balancer

	lastTickCaptureCount int
	needRebalance        bool

	// read only fields
	changeFeedID model.ChangeFeedID
	communicator ScheduleDispatcherCommunicator
	logger       *zap.Logger
}

// NewBaseScheduleDispatcher creates a new BaseScheduleDispatcher.
func NewBaseScheduleDispatcher(
	changeFeedID model.ChangeFeedID,
	communicator ScheduleDispatcherCommunicator,
	checkpointTs model.Ts,
) *BaseScheduleDispatcher {
	// logger is just the global logger with the `changefeed-id` field attached.
	logger := log.L().With(zap.String("changefeed-id", changeFeedID))

	return &BaseScheduleDispatcher{
		keyspans:             util.NewKeySpanSet(),
		captureStatus:        map[model.CaptureID]*captureStatus{},
		moveKeySpanManager:   newMoveKeySpanManager(),
		balancer:             newKeySpanNumberRebalancer(logger),
		changeFeedID:         changeFeedID,
		logger:               logger,
		communicator:         communicator,
		checkpointTs:         checkpointTs,
		lastTickCaptureCount: captureCountUninitialized,
	}
}

type captureStatus struct {
	// SyncStatus indicates what we know about the capture's internal state.
	// We need to know this before we can make decision whether to
	// dispatch a keyspan.
	SyncStatus captureSyncStatus

	// Watermark fields
	CheckpointTs model.Ts
	ResolvedTs   model.Ts
	// We can add more fields here in the future, such as: RedoLogTs.
}

type captureSyncStatus int32

const (
	// captureUninitialized indicates that we have not sent an `Announce` to the capture yet,
	// nor has it sent us a `Sync` message.
	captureUninitialized = captureSyncStatus(iota) + 1
	// captureSyncSent indicates that we have sent a `Sync` message to the capture, but have had
	// no response yet.
	captureSyncSent
	// captureSyncFinished indicates that the capture has been fully initialized and is ready to
	// accept `DispatchKeySpan` messages.
	captureSyncFinished
)

// Tick implements the interface ScheduleDispatcher.
func (s *BaseScheduleDispatcher) Tick(
	ctx context.Context,
	checkpointTs model.Ts,
	// currentKeySpans are keyspans that SHOULD be running given the current checkpoint-ts.
	// It is maintained by the caller of this function.
	currentKeySpans []model.KeySpanID,
	captures map[model.CaptureID]*model.CaptureInfo,
) (newCheckpointTs, resolvedTs model.Ts, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Update the internal capture list with information from the Owner
	// (from Etcd in the current implementation).
	s.captures = captures

	// We trigger an automatic rebalance if the capture count has changed.
	// This logic is the same as in the older implementation of scheduler.
	// TODO a better criterion is needed.
	// NOTE: We need to check whether the capture count has changed in every tick,
	// and set needRebalance to true if it has. If we miss a capture count change,
	// the workload may never be balanced until user manually triggers a rebalance.
	if s.lastTickCaptureCount != captureCountUninitialized &&
		s.lastTickCaptureCount != len(captures) {

		s.needRebalance = true
	}
	s.lastTickCaptureCount = len(captures)

	// Checks for checkpoint regression as a safety measure.
	if s.checkpointTs > checkpointTs {
		s.logger.Panic("checkpointTs regressed",
			zap.Uint64("old", s.checkpointTs),
			zap.Uint64("new", checkpointTs))
	}
	// Updates the internally maintained last checkpoint-ts.
	s.checkpointTs = checkpointTs

	// Makes sure that captures have all been synchronized before proceeding.
	done, err := s.syncCaptures(ctx)
	if err != nil {
		return CheckpointCannotProceed, CheckpointCannotProceed, errors.Trace(err)
	}
	if !done {
		// Returns early if not all captures have synced their states with us.
		// We need to know all captures' status in order to proceed.
		// This is crucial for ensuring that no keyspan is double-scheduled.
		return CheckpointCannotProceed, CheckpointCannotProceed, nil
	}

	s.descheduleKeySpansFromDownCaptures()

	shouldReplicateKeySpanSet := make(map[model.KeySpanID]struct{})
	for _, keyspanID := range currentKeySpans {
		shouldReplicateKeySpanSet[keyspanID] = struct{}{}
	}

	// findDiffKeySpans compares the keyspans that should be running and
	// the keyspans that are actually running.
	// Note: keyspans that are being added and removed are considered
	// "running" for the purpose of comparison, and we do not interrupt
	// these operations.
	toAdd, toRemove := s.findDiffKeySpans(shouldReplicateKeySpanSet)

	for _, keyspanID := range toAdd {
		ok, err := s.addKeySpan(ctx, keyspanID)
		if err != nil {
			return CheckpointCannotProceed, CheckpointCannotProceed, errors.Trace(err)
		}
		if !ok {
			return CheckpointCannotProceed, CheckpointCannotProceed, nil
		}
	}

	for _, keyspanID := range toRemove {
		record, ok := s.keyspans.GetKeySpanRecord(keyspanID)
		if !ok {
			s.logger.Panic("keyspan not found", zap.Uint64("keyspan-id", keyspanID))
		}
		if record.Status != util.RunningKeySpan {
			// another operation is in progress
			continue
		}

		ok, err := s.removeKeySpan(ctx, keyspanID)
		if err != nil {
			return CheckpointCannotProceed, CheckpointCannotProceed, errors.Trace(err)
		}
		if !ok {
			return CheckpointCannotProceed, CheckpointCannotProceed, nil
		}
	}

	checkAllTasksNormal := func() bool {
		return s.keyspans.CountKeySpanByStatus(util.RunningKeySpan) == len(currentKeySpans) &&
			s.keyspans.CountKeySpanByStatus(util.AddingKeySpan) == 0 &&
			s.keyspans.CountKeySpanByStatus(util.RemovingKeySpan) == 0
	}
	if !checkAllTasksNormal() {
		return CheckpointCannotProceed, CheckpointCannotProceed, nil
	}

	// handleMoveKeySpanJobs tries to execute user-specified manual move keyspan jobs.
	ok, err := s.handleMoveKeySpanJobs(ctx)
	if err != nil {
		return CheckpointCannotProceed, CheckpointCannotProceed, errors.Trace(err)
	}
	if !ok {
		return CheckpointCannotProceed, CheckpointCannotProceed, nil
	}
	if !checkAllTasksNormal() {
		return CheckpointCannotProceed, CheckpointCannotProceed, nil
	}

	if s.needRebalance {
		ok, err := s.rebalance(ctx)
		if err != nil {
			return CheckpointCannotProceed, CheckpointCannotProceed, errors.Trace(err)
		}
		if !ok {
			return CheckpointCannotProceed, CheckpointCannotProceed, nil
		}
		s.needRebalance = false
	}
	if !checkAllTasksNormal() {
		return CheckpointCannotProceed, CheckpointCannotProceed, nil
	}

	newCheckpointTs, resolvedTs = s.calculateTs()
	return
}

func (s *BaseScheduleDispatcher) calculateTs() (checkpointTs, resolvedTs model.Ts) {
	checkpointTs = math.MaxUint64
	resolvedTs = math.MaxUint64

	for captureID, status := range s.captureStatus {
		if s.keyspans.CountKeySpanByCaptureID(captureID) == 0 {
			// the checkpoint (as well as resolved-ts) from a capture
			// that is not replicating any keyspan is meaningless.
			continue
		}
		if status.ResolvedTs < resolvedTs {
			resolvedTs = status.ResolvedTs
		}
		if status.CheckpointTs < checkpointTs {
			checkpointTs = status.CheckpointTs
		}
	}
	return
}

func (s *BaseScheduleDispatcher) syncCaptures(ctx context.Context) (capturesAllSynced bool, err error) {
	for captureID := range s.captureStatus {
		if _, ok := s.captures[captureID]; !ok {
			// removes expired captures from the captureSynced map
			delete(s.captureStatus, captureID)
			s.logger.Debug("syncCaptures: remove offline capture",
				zap.String("capture-id", captureID))
		}
	}
	for captureID := range s.captures {
		if _, ok := s.captureStatus[captureID]; !ok {
			s.captureStatus[captureID] = &captureStatus{
				SyncStatus:   captureUninitialized,
				CheckpointTs: s.checkpointTs,
				ResolvedTs:   s.checkpointTs,
			}
		}
	}

	finishedCount := 0
	for captureID, status := range s.captureStatus {
		switch status.SyncStatus {
		case captureUninitialized:
			done, err := s.communicator.Announce(ctx, s.changeFeedID, captureID)
			if err != nil {
				return false, errors.Trace(err)
			}
			if done {
				s.captureStatus[captureID].SyncStatus = captureSyncSent
				s.logger.Info("syncCaptures: sent sync request",
					zap.String("capture-id", captureID))
			}
		case captureSyncFinished:
			finishedCount++
		case captureSyncSent:
			continue
		default:
			panic("unreachable")
		}
	}
	s.logger.Debug("syncCaptures: size of captures, size of sync finished captures",
		zap.Int("size of caputres", len(s.captureStatus)),
		zap.Int("size of finished captures", finishedCount))

	return finishedCount == len(s.captureStatus), nil
}

// descheduleKeySpansFromDownCaptures removes keyspans from `s.keyspans` that are
// associated with a capture that no longer exists.
// `s.captures` MUST be updated before calling this method.
func (s *BaseScheduleDispatcher) descheduleKeySpansFromDownCaptures() {
	for _, captureID := range s.keyspans.GetDistinctCaptures() {
		// If the capture is not in the current list of captures, it means that
		// the capture has been removed from the system.
		if _, ok := s.captures[captureID]; !ok {
			// Remove records for all keyspan previously replicated by the
			// gone capture.
			removed := s.keyspans.RemoveKeySpanRecordByCaptureID(captureID)
			s.logger.Info("capture down, removing keyspans",
				zap.String("capture-id", captureID),
				zap.Any("removed-keyspans", removed))
			s.moveKeySpanManager.OnCaptureRemoved(captureID)
		}
	}
}

func (s *BaseScheduleDispatcher) findDiffKeySpans(
	shouldReplicateKeySpans map[model.KeySpanID]struct{},
) (toAdd, toRemove []model.KeySpanID) {
	// Find keyspans that need to be added.
	for keyspanID := range shouldReplicateKeySpans {
		if _, ok := s.keyspans.GetKeySpanRecord(keyspanID); !ok {
			// keyspan is not found in `s.keyspans`.
			toAdd = append(toAdd, keyspanID)
		}
	}

	// Find keyspans that need to be removed.
	for keyspanID := range s.keyspans.GetAllKeySpans() {
		if _, ok := shouldReplicateKeySpans[keyspanID]; !ok {
			// keyspan is not found in `shouldReplicateKeySpans`.
			toRemove = append(toRemove, keyspanID)
		}
	}
	return
}

func (s *BaseScheduleDispatcher) addKeySpan(
	ctx context.Context,
	keyspanID model.KeySpanID,
) (done bool, err error) {
	// A user triggered move-keyspan will have had the target recorded.
	target, ok := s.moveKeySpanManager.GetTargetByKeySpanID(keyspanID)
	isManualMove := ok
	if !ok {
		target, ok = s.balancer.FindTarget(s.keyspans, s.captures)
		if !ok {
			s.logger.Warn("no active capture")
			return true, nil
		}
	}

	ok, err = s.communicator.DispatchKeySpan(ctx, s.changeFeedID, keyspanID, target, false)
	if err != nil {
		return false, errors.Trace(err)
	}

	if !ok {
		return false, nil
	}
	if isManualMove {
		s.moveKeySpanManager.MarkDone(keyspanID)
	}

	if ok := s.keyspans.AddKeySpanRecord(&util.KeySpanRecord{
		KeySpanID: keyspanID,
		CaptureID: target,
		Status:    util.AddingKeySpan,
	}); !ok {
		s.logger.Panic("duplicate keyspan", zap.Uint64("keyspan-id", keyspanID))
	}
	return true, nil
}

func (s *BaseScheduleDispatcher) removeKeySpan(
	ctx context.Context,
	keyspanID model.KeySpanID,
) (done bool, err error) {
	record, ok := s.keyspans.GetKeySpanRecord(keyspanID)
	if !ok {
		s.logger.Panic("keyspan not found", zap.Uint64("keyspan-id", keyspanID))
	}
	// need to delete keyspan
	captureID := record.CaptureID
	ok, err = s.communicator.DispatchKeySpan(ctx, s.changeFeedID, keyspanID, captureID, true)
	if err != nil {
		return false, errors.Trace(err)
	}
	if !ok {
		return false, nil
	}

	record.Status = util.RemovingKeySpan
	s.keyspans.UpdateKeySpanRecord(record)
	return true, nil
}

// MoveKeySpan implements the interface SchedulerDispatcher.
func (s *BaseScheduleDispatcher) MoveKeySpan(keyspanID model.KeySpanID, target model.CaptureID) {
	if !s.moveKeySpanManager.Add(keyspanID, target) {
		log.Info("Move KeySpan command has been ignored, because the last user triggered"+
			"move has not finished",
			zap.Uint64("keyspan-id", keyspanID),
			zap.String("target-capture", target))
	}
}

func (s *BaseScheduleDispatcher) handleMoveKeySpanJobs(ctx context.Context) (bool, error) {
	removeAllDone, err := s.moveKeySpanManager.DoRemove(ctx,
		func(ctx context.Context, keyspanID model.KeySpanID, target model.CaptureID) (removeKeySpanResult, error) {
			_, ok := s.keyspans.GetKeySpanRecord(keyspanID)
			if !ok {
				s.logger.Warn("keyspan does not exist", zap.Uint64("keyspan-id", keyspanID))
				return removeKeySpanResultGiveUp, nil
			}

			if _, ok := s.captures[target]; !ok {
				s.logger.Warn("move keyspan target does not exist",
					zap.Uint64("keyspan-id", keyspanID),
					zap.String("target-capture", target))
				return removeKeySpanResultGiveUp, nil
			}

			ok, err := s.removeKeySpan(ctx, keyspanID)
			if err != nil {
				return removeKeySpanResultUnavailable, errors.Trace(err)
			}
			if !ok {
				return removeKeySpanResultUnavailable, nil
			}
			return removeKeySpanResultOK, nil
		},
	)
	if err != nil {
		return false, errors.Trace(err)
	}
	return removeAllDone, nil
}

// Rebalance implements the interface ScheduleDispatcher.
func (s *BaseScheduleDispatcher) Rebalance() {
	s.needRebalance = true
}

func (s *BaseScheduleDispatcher) rebalance(ctx context.Context) (done bool, err error) {
	keyspansToRemove := s.balancer.FindVictims(s.keyspans, s.captures)
	for _, record := range keyspansToRemove {
		if record.Status != util.RunningKeySpan {
			s.logger.DPanic("unexpected keyspan status",
				zap.Any("keyspan-record", record))
		}

		// Removes the keyspan from the current capture
		ok, err := s.communicator.DispatchKeySpan(ctx, s.changeFeedID, record.KeySpanID, record.CaptureID, true)
		if err != nil {
			return false, errors.Trace(err)
		}
		if !ok {
			return false, nil
		}

		record.Status = util.RemovingKeySpan
		s.keyspans.UpdateKeySpanRecord(record)
	}
	return true, nil
}

// OnAgentFinishedKeySpanOperation is called when a keyspan operation has been finished by
// the processor.
func (s *BaseScheduleDispatcher) OnAgentFinishedKeySpanOperation(captureID model.CaptureID, keyspanID model.KeySpanID) {
	s.mu.Lock()
	defer s.mu.Unlock()

	logger := s.logger.With(
		zap.String("capture-id", captureID),
		zap.Uint64("keyspan-id", keyspanID),
	)

	if _, ok := s.captures[captureID]; !ok {
		logger.Warn("stale message from dead processor, ignore")
		return
	}

	record, ok := s.keyspans.GetKeySpanRecord(keyspanID)
	if !ok {
		logger.Warn("response about a stale keyspan, ignore")
		return
	}

	if record.CaptureID != captureID {
		logger.Panic("message from unexpected capture",
			zap.String("expected", record.CaptureID))
	}
	logger.Info("owner received dispatch finished")

	switch record.Status {
	case util.AddingKeySpan:
		record.Status = util.RunningKeySpan
		s.keyspans.UpdateKeySpanRecord(record)
	case util.RemovingKeySpan:
		if !s.keyspans.RemoveKeySpanRecord(keyspanID) {
			logger.Panic("failed to remove keyspan")
		}
	case util.RunningKeySpan:
		logger.Panic("response to invalid dispatch message")
	}
}

// OnAgentSyncTaskStatuses is called when the processor sends its complete current state.
func (s *BaseScheduleDispatcher) OnAgentSyncTaskStatuses(captureID model.CaptureID, running, adding, removing []model.KeySpanID) {
	s.mu.Lock()
	defer s.mu.Unlock()

	logger := s.logger.With(zap.String("capture-id", captureID))
	logger.Info("scheduler received sync", zap.String("capture-id", captureID))

	if ce := logger.Check(zap.DebugLevel, "OnAgentSyncTaskStatuses"); ce != nil {
		// Print this information only in debug mode.
		ce.Write(
			zap.Any("running", running),
			zap.Any("adding", adding),
			zap.Any("removing", removing))
	}

	// Clear all keyspans previously run by the sender capture,
	// because `Sync` tells the Owner to reset its state regarding
	// the sender capture.
	s.keyspans.RemoveKeySpanRecordByCaptureID(captureID)

	if _, ok := s.captureStatus[captureID]; !ok {
		logger.Warn("received sync from a capture not previously tracked, ignore",
			zap.Any("capture-status", s.captureStatus))
		return
	}

	for _, keyspanID := range adding {
		if record, ok := s.keyspans.GetKeySpanRecord(keyspanID); ok {
			logger.Panic("duplicate keyspan tasks",
				zap.Uint64("keyspan-id", keyspanID),
				zap.String("actual-capture-id", record.CaptureID))
		}
		s.keyspans.AddKeySpanRecord(&util.KeySpanRecord{KeySpanID: keyspanID, CaptureID: captureID, Status: util.AddingKeySpan})
	}
	for _, keyspanID := range running {
		if record, ok := s.keyspans.GetKeySpanRecord(keyspanID); ok {
			logger.Panic("duplicate keyspan tasks",
				zap.Uint64("keyspan-id", keyspanID),
				zap.String("actual-capture-id", record.CaptureID))
		}
		s.keyspans.AddKeySpanRecord(&util.KeySpanRecord{KeySpanID: keyspanID, CaptureID: captureID, Status: util.RunningKeySpan})
	}
	for _, keyspanID := range removing {
		if record, ok := s.keyspans.GetKeySpanRecord(keyspanID); ok {
			logger.Panic("duplicate keyspan tasks",
				zap.Uint64("keyspan-id", keyspanID),
				zap.String("actual-capture-id", record.CaptureID))
		}
		s.keyspans.AddKeySpanRecord(&util.KeySpanRecord{KeySpanID: keyspanID, CaptureID: captureID, Status: util.RemovingKeySpan})
	}

	s.captureStatus[captureID].SyncStatus = captureSyncFinished
}

// OnAgentCheckpoint is called when the processor sends a checkpoint.
func (s *BaseScheduleDispatcher) OnAgentCheckpoint(captureID model.CaptureID, checkpointTs model.Ts, resolvedTs model.Ts) {
	s.mu.Lock()
	defer s.mu.Unlock()

	logger := s.logger.With(zap.String("capture-id", captureID),
		zap.Uint64("checkpoint-ts", checkpointTs),
		zap.Uint64("resolved-ts", resolvedTs))

	status, ok := s.captureStatus[captureID]
	if !ok || status.SyncStatus != captureSyncFinished {
		logger.Warn("received checkpoint from a capture not synced, ignore")
		return
	}

	status.CheckpointTs = checkpointTs
	status.ResolvedTs = resolvedTs
}
