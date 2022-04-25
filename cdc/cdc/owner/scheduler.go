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
	"fmt"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/tikv/migration/cdc/cdc/model"
	pscheduler "github.com/tikv/migration/cdc/cdc/scheduler"
	"github.com/tikv/migration/cdc/pkg/config"
	"github.com/tikv/migration/cdc/pkg/context"
	cdcContext "github.com/tikv/migration/cdc/pkg/context"
	cerror "github.com/tikv/migration/cdc/pkg/errors"
	"github.com/tikv/migration/cdc/pkg/orchestrator"
	"github.com/tikv/migration/cdc/pkg/p2p"
	"github.com/tikv/migration/cdc/pkg/regionspan"
	"github.com/tikv/migration/cdc/pkg/version"
	"go.uber.org/zap"
)

// scheduler is an interface for scheduling keyspans.
// Since in our design, we do not record checkpoints per keyspan,
// how we calculate the global watermarks (checkpoint-ts and resolved-ts)
// is heavily coupled with how keyspans are scheduled.
// That is why we have a scheduler interface that also reports the global watermarks.
type scheduler interface {
	// Tick is called periodically from the owner, and returns
	// updated global watermarks.
	Tick(
		ctx context.Context,
		state *orchestrator.ChangefeedReactorState,
		captures map[model.CaptureID]*model.CaptureInfo,
	) (newCheckpointTs, newResolvedTs model.Ts, err error)

	// MoveKeySpan is used to trigger manual keyspan moves.
	MoveKeySpan(keyspanID model.KeySpanID, target model.CaptureID)

	// Rebalance is used to trigger manual workload rebalances.
	Rebalance()

	// Close closes the scheduler and releases resources.
	Close(ctx context.Context)
}

type schedulerV2 struct {
	*pscheduler.BaseScheduleDispatcher

	messageServer *p2p.MessageServer
	messageRouter p2p.MessageRouter

	changeFeedID  model.ChangeFeedID
	handlerErrChs []<-chan error

	stats *schedulerStats

	currentKeySpansID     []model.KeySpanID
	currentKeySpans       map[model.KeySpanID]regionspan.Span
	updateCurrentKeySpans func(ctx cdcContext.Context) ([]model.KeySpanID, map[model.KeySpanID]regionspan.Span, error)
}

// NewSchedulerV2 creates a new schedulerV2
func NewSchedulerV2(
	ctx context.Context,
	changeFeedID model.ChangeFeedID,
	checkpointTs model.Ts,
	messageServer *p2p.MessageServer,
	messageRouter p2p.MessageRouter,
) (*schedulerV2, error) {
	ret := &schedulerV2{
		changeFeedID:          changeFeedID,
		messageServer:         messageServer,
		messageRouter:         messageRouter,
		stats:                 &schedulerStats{},
		updateCurrentKeySpans: updateCurrentKeySpansImpl,
	}
	ret.BaseScheduleDispatcher = pscheduler.NewBaseScheduleDispatcher(changeFeedID, ret, checkpointTs)
	if err := ret.registerPeerMessageHandlers(ctx); err != nil {
		return nil, err
	}
	log.Debug("scheduler created", zap.Uint64("checkpoint-ts", checkpointTs))
	return ret, nil
}

// newSchedulerV2FromCtx creates a new schedulerV2 from context.
// This function is factored out to facilitate unit testing.
func newSchedulerV2FromCtx(ctx context.Context, startTs uint64) (scheduler, error) {
	changeFeedID := ctx.ChangefeedVars().ID
	messageServer := ctx.GlobalVars().MessageServer
	messageRouter := ctx.GlobalVars().MessageRouter
	ret, err := NewSchedulerV2(ctx, changeFeedID, startTs, messageServer, messageRouter)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return ret, nil
}

func newScheduler(ctx context.Context, startTs uint64) (scheduler, error) {
	conf := config.GetGlobalServerConfig()
	if conf.Debug.EnableNewScheduler {
		return newSchedulerV2FromCtx(ctx, startTs)
	}
	return newSchedulerV1(), nil
}

func (s *schedulerV2) Tick(
	ctx context.Context,
	state *orchestrator.ChangefeedReactorState,
	captures map[model.CaptureID]*model.CaptureInfo,
) (checkpoint, resolvedTs model.Ts, err error) {
	if err := s.checkForHandlerErrors(ctx); err != nil {
		return pscheduler.CheckpointCannotProceed, pscheduler.CheckpointCannotProceed, errors.Trace(err)
	}
	s.currentKeySpansID, s.currentKeySpans, err = s.updateCurrentKeySpans(ctx)
	if err != nil {
		return pscheduler.CheckpointCannotProceed, pscheduler.CheckpointCannotProceed, errors.Trace(err)
	}
	log.Debug("current key spans ID", zap.Any("currentKeySpansID", s.currentKeySpansID))
	return s.BaseScheduleDispatcher.Tick(ctx, state.Status.CheckpointTs, s.currentKeySpansID, captures)
}

func (s *schedulerV2) DispatchKeySpan(
	ctx context.Context,
	changeFeedID model.ChangeFeedID,
	keyspanID model.KeySpanID,
	captureID model.CaptureID,
	isDelete bool,
) (done bool, err error) {
	topic := model.DispatchKeySpanTopic(changeFeedID)
	message := &model.DispatchKeySpanMessage{
		OwnerRev: ctx.GlobalVars().OwnerRevision,
		ID:       keyspanID,
		IsDelete: isDelete,
	}

	if !isDelete {
		message.Start = s.currentKeySpans[keyspanID].Start
		message.End = s.currentKeySpans[keyspanID].End
		fmt.Println(message.Start, message.End)
	}
	log.Debug("try to send message",
		zap.String("topic", topic),
		zap.Any("message", message))

	ok, err := s.trySendMessage(ctx, captureID, topic, message)
	if err != nil {
		return false, errors.Trace(err)
	}
	if !ok {
		return false, nil
	}

	s.stats.RecordDispatch()
	log.Debug("send message successfully",
		zap.String("topic", topic),
		zap.Any("message", message))

	return true, nil
}

func (s *schedulerV2) Announce(
	ctx context.Context,
	changeFeedID model.ChangeFeedID,
	captureID model.CaptureID,
) (bool, error) {
	topic := model.AnnounceTopic(changeFeedID)
	message := &model.AnnounceMessage{
		OwnerRev:     ctx.GlobalVars().OwnerRevision,
		OwnerVersion: version.ReleaseSemver(),
	}

	ok, err := s.trySendMessage(ctx, captureID, topic, message)
	if err != nil {
		return false, errors.Trace(err)
	}
	if !ok {
		return false, nil
	}

	s.stats.RecordAnnounce()
	log.Debug("send message successfully",
		zap.String("topic", topic),
		zap.Any("message", message))

	return true, nil
}

func (s *schedulerV2) getClient(target model.CaptureID) (*p2p.MessageClient, bool) {
	client := s.messageRouter.GetClient(target)
	if client == nil {
		log.Warn("scheduler: no message client found, retry later",
			zap.String("target", target))
		return nil, false
	}
	return client, true
}

func (s *schedulerV2) trySendMessage(
	ctx context.Context,
	target model.CaptureID,
	topic p2p.Topic,
	value interface{},
) (bool, error) {
	// TODO (zixiong): abstract this function out together with the similar method in cdc/processor/agent.go
	// We probably need more advanced logic to handle and mitigate complex failure situations.

	client, ok := s.getClient(target)
	if !ok {
		return false, nil
	}

	_, err := client.TrySendMessage(ctx, topic, value)
	if err != nil {
		if cerror.ErrPeerMessageSendTryAgain.Equal(err) {
			return false, nil
		}
		if cerror.ErrPeerMessageClientClosed.Equal(err) {
			log.Warn("peer messaging client is closed while trying to send a message through it. "+
				"Report a bug if this warning repeats",
				zap.String("changefeed-id", s.changeFeedID),
				zap.String("target", target))
			return false, nil
		}
		return false, errors.Trace(err)
	}

	return true, nil
}

func (s *schedulerV2) Close(ctx context.Context) {
	log.Debug("scheduler closed", zap.String("changefeed-id", s.changeFeedID))
	s.deregisterPeerMessageHandlers(ctx)
}

func (s *schedulerV2) registerPeerMessageHandlers(ctx context.Context) (ret error) {
	defer func() {
		if ret != nil {
			s.deregisterPeerMessageHandlers(ctx)
		}
	}()

	errCh, err := s.messageServer.SyncAddHandler(
		ctx,
		model.DispatchKeySpanResponseTopic(s.changeFeedID),
		&model.DispatchKeySpanResponseMessage{},
		func(sender string, messageI interface{}) error {
			message := messageI.(*model.DispatchKeySpanResponseMessage)
			s.stats.RecordDispatchResponse()
			s.OnAgentFinishedKeySpanOperation(sender, message.ID)
			return nil
		})
	if err != nil {
		return errors.Trace(err)
	}
	s.handlerErrChs = append(s.handlerErrChs, errCh)

	errCh, err = s.messageServer.SyncAddHandler(
		ctx,
		model.SyncTopic(s.changeFeedID),
		&model.SyncMessage{},
		func(sender string, messageI interface{}) error {
			message := messageI.(*model.SyncMessage)
			s.stats.RecordSync()
			s.OnAgentSyncTaskStatuses(
				sender,
				message.Running,
				message.Adding,
				message.Removing)
			return nil
		})
	if err != nil {
		return errors.Trace(err)
	}
	s.handlerErrChs = append(s.handlerErrChs, errCh)

	errCh, err = s.messageServer.SyncAddHandler(
		ctx,
		model.CheckpointTopic(s.changeFeedID),
		&model.CheckpointMessage{},
		func(sender string, messageI interface{}) error {
			message := messageI.(*model.CheckpointMessage)
			s.stats.RecordCheckpoint()
			s.OnAgentCheckpoint(sender, message.CheckpointTs, message.ResolvedTs)
			return nil
		})
	if err != nil {
		return errors.Trace(err)
	}
	s.handlerErrChs = append(s.handlerErrChs, errCh)

	return nil
}

func (s *schedulerV2) deregisterPeerMessageHandlers(ctx context.Context) {
	err := s.messageServer.SyncRemoveHandler(
		ctx,
		model.DispatchKeySpanResponseTopic(s.changeFeedID))
	if err != nil {
		log.Error("failed to remove peer message handler", zap.Error(err))
	}

	err = s.messageServer.SyncRemoveHandler(
		ctx,
		model.SyncTopic(s.changeFeedID))
	if err != nil {
		log.Error("failed to remove peer message handler", zap.Error(err))
	}

	err = s.messageServer.SyncRemoveHandler(
		ctx,
		model.CheckpointTopic(s.changeFeedID))
	if err != nil {
		log.Error("failed to remove peer message handler", zap.Error(err))
	}
}

func (s *schedulerV2) checkForHandlerErrors(ctx context.Context) error {
	for _, errCh := range s.handlerErrChs {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case err := <-errCh:
			return errors.Trace(err)
		default:
		}
	}
	return nil
}

type schedulerStats struct {
	ChangefeedID model.ChangeFeedID

	AnnounceSentCount            int64
	SyncReceiveCount             int64
	DispatchSentCount            int64
	DispatchResponseReceiveCount int64
	CheckpointReceiveCount       int64

	// TODO add prometheus metrics
}

func (s *schedulerStats) RecordAnnounce() {
	atomic.AddInt64(&s.AnnounceSentCount, 1)
}

func (s *schedulerStats) RecordSync() {
	atomic.AddInt64(&s.SyncReceiveCount, 1)
}

func (s *schedulerStats) RecordDispatch() {
	atomic.AddInt64(&s.DispatchSentCount, 1)
}

func (s *schedulerStats) RecordDispatchResponse() {
	atomic.AddInt64(&s.DispatchResponseReceiveCount, 1)
}

func (s *schedulerStats) RecordCheckpoint() {
	atomic.AddInt64(&s.CheckpointReceiveCount, 1)
}
