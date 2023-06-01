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
	"testing"

	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/tikv/migration/cdc/cdc/model"
	keyspanpipeline "github.com/tikv/migration/cdc/cdc/processor/pipeline"
	"github.com/tikv/migration/cdc/cdc/sink"
	cdcContext "github.com/tikv/migration/cdc/pkg/context"
	cerror "github.com/tikv/migration/cdc/pkg/errors"
	"github.com/tikv/migration/cdc/pkg/orchestrator"
	"github.com/tikv/migration/cdc/pkg/util/testleak"
)

func Test(t *testing.T) { check.TestingT(t) }

type processorSuite struct{}

var _ = check.Suite(&processorSuite{})

func newProcessor4Test(
	ctx cdcContext.Context,
	_ *check.C,
	createKeySpanPipeline func(ctx cdcContext.Context, keyspanID model.KeySpanID, replicaInfo *model.KeySpanReplicaInfo) (keyspanpipeline.KeySpanPipeline, error),
) *processor {
	p := newProcessor(ctx)
	p.lazyInit = func(ctx cdcContext.Context) error {
		p.initialized = true
		return nil
	}
	p.sinkManager = &sink.Manager{}
	p.createKeySpanPipeline = createKeySpanPipeline
	return p
}

func initProcessor4Test(ctx cdcContext.Context, c *check.C) (*processor, *orchestrator.ReactorStateTester) {
	p := newProcessor4Test(ctx, c, func(ctx cdcContext.Context, keyspanID model.KeySpanID, replicaInfo *model.KeySpanReplicaInfo) (keyspanpipeline.KeySpanPipeline, error) {
		return &mockKeySpanPipeline{
			keyspanID:    keyspanID,
			name:         fmt.Sprintf("`test`.`keyspan%d`", keyspanID),
			status:       keyspanpipeline.KeySpanStatusRunning,
			resolvedTs:   replicaInfo.StartTs,
			checkpointTs: replicaInfo.StartTs,
		}, nil
	})
	p.changefeed = orchestrator.NewChangefeedReactorState(ctx.ChangefeedVars().ID)
	return p, orchestrator.NewReactorStateTester(c, p.changefeed, map[string]string{
		"/tikv/cdc/capture/" + ctx.GlobalVars().CaptureInfo.ID:                                     `{"id":"` + ctx.GlobalVars().CaptureInfo.ID + `","address":"127.0.0.1:8600"}`,
		"/tikv/cdc/changefeed/info/" + ctx.ChangefeedVars().ID:                                     `{"sink-uri":"blackhole://","opts":{},"create-time":"2020-02-02T00:00:00.000000+00:00","start-ts":0,"target-ts":0,"admin-job-type":0,"sort-engine":"memory","sort-dir":".","config":{"enable-old-value":false,"check-gc-safe-point":true,"sink":{"dispatchers":null,"protocol":"open-protocol"},"scheduler":{"type":"keyspan-number","polling-time":-1}},"state":"normal","history":null,"error":null,"sync-point-enabled":false,"sync-point-interval":600000000000}`,
		"/tikv/cdc/job/" + ctx.ChangefeedVars().ID:                                                 `{"resolved-ts":0,"checkpoint-ts":0,"admin-job-type":0}`,
		"/tikv/cdc/task/status/" + ctx.GlobalVars().CaptureInfo.ID + "/" + ctx.ChangefeedVars().ID: `{"keyspans":{},"operation":null,"admin-job-type":0}`,
	})
}

type mockKeySpanPipeline struct {
	keyspanID    model.KeySpanID
	name         string
	resolvedTs   model.Ts
	checkpointTs model.Ts
	barrierTs    model.Ts
	stopTs       model.Ts
	status       keyspanpipeline.KeySpanStatus
	canceled     bool
}

func (m *mockKeySpanPipeline) ID() (keyspanID uint64) {
	return m.keyspanID
}

func (m *mockKeySpanPipeline) Name() string {
	return m.name
}

func (m *mockKeySpanPipeline) ResolvedTs() model.Ts {
	return m.resolvedTs
}

func (m *mockKeySpanPipeline) CheckpointTs() model.Ts {
	return m.checkpointTs
}

func (m *mockKeySpanPipeline) UpdateBarrierTs(ts model.Ts) {
	m.barrierTs = ts
}

func (m *mockKeySpanPipeline) AsyncStop(targetTs model.Ts) bool {
	m.stopTs = targetTs
	return true
}

func (m *mockKeySpanPipeline) Workload() model.WorkloadInfo {
	return model.WorkloadInfo{Workload: 1}
}

func (m *mockKeySpanPipeline) Status() keyspanpipeline.KeySpanStatus {
	return m.status
}

func (m *mockKeySpanPipeline) Cancel() {
	if m.canceled {
		log.Panic("cancel a canceled keyspan pipeline")
	}
	m.canceled = true
}

func (m *mockKeySpanPipeline) Wait() {
	// do nothing
}

func (s *processorSuite) TestCheckKeySpansNum(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx := cdcContext.NewBackendContext4Test(true)
	p, tester := initProcessor4Test(ctx, c)
	var err error
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	tester.MustApplyPatches()
	c.Assert(p.changefeed.TaskPositions[p.captureInfo.ID], check.DeepEquals,
		&model.TaskPosition{
			CheckPointTs: 0,
			ResolvedTs:   0,
			Count:        0,
			Error:        nil,
		})

	p, tester = initProcessor4Test(ctx, c)
	p.changefeed.Info.StartTs = 66
	p.changefeed.Status.CheckpointTs = 88
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	tester.MustApplyPatches()
	c.Assert(p.changefeed.TaskPositions[p.captureInfo.ID], check.DeepEquals,
		&model.TaskPosition{
			CheckPointTs: 88,
			ResolvedTs:   88,
			Count:        0,
			Error:        nil,
		})
}

func (s *processorSuite) TestHandleKeySpanOperation4SingleKeySpan(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx := cdcContext.NewBackendContext4Test(true)
	p, tester := initProcessor4Test(ctx, c)
	var err error
	// init tick
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	tester.MustApplyPatches()
	p.changefeed.PatchStatus(func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
		status.CheckpointTs = 90
		status.ResolvedTs = 100
		return status, true, nil
	})
	p.changefeed.PatchTaskPosition(p.captureInfo.ID, func(position *model.TaskPosition) (*model.TaskPosition, bool, error) {
		position.ResolvedTs = 100
		return position, true, nil
	})
	tester.MustApplyPatches()

	// no operation
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	tester.MustApplyPatches()

	// add keyspan, in processing
	// in current implementation of owner, the startTs and BoundaryTs of add keyspan operation should be always equaled.
	p.changefeed.PatchTaskStatus(p.captureInfo.ID, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
		status.AddKeySpan(66, &model.KeySpanReplicaInfo{StartTs: 60}, 60, nil)
		return status, true, nil
	})
	tester.MustApplyPatches()
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	tester.MustApplyPatches()
	c.Assert(p.changefeed.TaskStatuses[p.captureInfo.ID], check.DeepEquals, &model.TaskStatus{
		KeySpans: map[uint64]*model.KeySpanReplicaInfo{
			66: {StartTs: 60},
		},
		Operation: map[uint64]*model.KeySpanOperation{
			66: {Delete: false, BoundaryTs: 60, Status: model.OperProcessed},
		},
	})

	// add keyspan, not finished
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	tester.MustApplyPatches()
	c.Assert(p.changefeed.TaskStatuses[p.captureInfo.ID], check.DeepEquals, &model.TaskStatus{
		KeySpans: map[uint64]*model.KeySpanReplicaInfo{
			66: {StartTs: 60},
		},
		Operation: map[uint64]*model.KeySpanOperation{
			66: {Delete: false, BoundaryTs: 60, Status: model.OperProcessed},
		},
	})

	// add keyspan, push the resolvedTs
	keyspan66 := p.keyspans[66].(*mockKeySpanPipeline)
	keyspan66.resolvedTs = 101
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	tester.MustApplyPatches()
	c.Assert(p.changefeed.TaskStatuses[p.captureInfo.ID], check.DeepEquals, &model.TaskStatus{
		KeySpans: map[uint64]*model.KeySpanReplicaInfo{
			66: {StartTs: 60},
		},
		Operation: map[uint64]*model.KeySpanOperation{
			66: {Delete: false, BoundaryTs: 60, Status: model.OperProcessed},
		},
	})
	c.Assert(p.changefeed.TaskPositions[p.captureInfo.ID].ResolvedTs, check.Equals, uint64(101))

	// finish the operation
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	tester.MustApplyPatches()
	c.Assert(p.changefeed.TaskStatuses[p.captureInfo.ID], check.DeepEquals, &model.TaskStatus{
		KeySpans: map[uint64]*model.KeySpanReplicaInfo{
			66: {StartTs: 60},
		},
		Operation: map[uint64]*model.KeySpanOperation{
			66: {Delete: false, BoundaryTs: 60, Status: model.OperFinished},
		},
	})

	// clear finished operations
	cleanUpFinishedOpOperation(p.changefeed, p.captureInfo.ID, tester)

	// remove keyspan, in processing
	p.changefeed.PatchTaskStatus(p.captureInfo.ID, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
		status.RemoveKeySpan(66, 120, false)
		return status, true, nil
	})
	tester.MustApplyPatches()
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	tester.MustApplyPatches()
	c.Assert(p.changefeed.TaskStatuses[p.captureInfo.ID], check.DeepEquals, &model.TaskStatus{
		KeySpans: map[uint64]*model.KeySpanReplicaInfo{},
		Operation: map[uint64]*model.KeySpanOperation{
			66: {Delete: true, BoundaryTs: 120, Status: model.OperProcessed},
		},
	})
	c.Assert(keyspan66.stopTs, check.Equals, uint64(120))

	// remove keyspan, not finished
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	tester.MustApplyPatches()
	c.Assert(p.changefeed.TaskStatuses[p.captureInfo.ID], check.DeepEquals, &model.TaskStatus{
		KeySpans: map[uint64]*model.KeySpanReplicaInfo{},
		Operation: map[uint64]*model.KeySpanOperation{
			66: {Delete: true, BoundaryTs: 120, Status: model.OperProcessed},
		},
	})

	// remove keyspan, finished
	keyspan66.status = keyspanpipeline.KeySpanStatusStopped
	keyspan66.checkpointTs = 121
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	tester.MustApplyPatches()
	c.Assert(p.changefeed.TaskStatuses[p.captureInfo.ID], check.DeepEquals, &model.TaskStatus{
		KeySpans: map[uint64]*model.KeySpanReplicaInfo{},
		Operation: map[uint64]*model.KeySpanOperation{
			66: {Delete: true, BoundaryTs: 121, Status: model.OperFinished},
		},
	})
	c.Assert(keyspan66.canceled, check.IsTrue)
	c.Assert(p.keyspans[66], check.IsNil)
}

func (s *processorSuite) TestHandleKeySpanOperation4MultiKeySpan(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx := cdcContext.NewBackendContext4Test(true)
	p, tester := initProcessor4Test(ctx, c)
	var err error
	// init tick
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	tester.MustApplyPatches()
	p.changefeed.PatchStatus(func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
		status.CheckpointTs = 20
		status.ResolvedTs = 20
		return status, true, nil
	})
	p.changefeed.PatchTaskPosition(p.captureInfo.ID, func(position *model.TaskPosition) (*model.TaskPosition, bool, error) {
		position.ResolvedTs = 100
		position.CheckPointTs = 90
		return position, true, nil
	})
	tester.MustApplyPatches()

	// no operation
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	tester.MustApplyPatches()

	// add keyspan, in processing
	// in current implementation of owner, the startTs and BoundaryTs of add keyspan operation should be always equaled.
	p.changefeed.PatchTaskStatus(p.captureInfo.ID, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
		status.AddKeySpan(1, &model.KeySpanReplicaInfo{StartTs: 60}, 60, nil)
		status.AddKeySpan(2, &model.KeySpanReplicaInfo{StartTs: 50}, 50, nil)
		status.AddKeySpan(3, &model.KeySpanReplicaInfo{StartTs: 40}, 40, nil)
		status.KeySpans[4] = &model.KeySpanReplicaInfo{StartTs: 30}
		return status, true, nil
	})
	tester.MustApplyPatches()
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	tester.MustApplyPatches()
	c.Assert(p.changefeed.TaskStatuses[p.captureInfo.ID], check.DeepEquals, &model.TaskStatus{
		KeySpans: map[uint64]*model.KeySpanReplicaInfo{
			1: {StartTs: 60},
			2: {StartTs: 50},
			3: {StartTs: 40},
			4: {StartTs: 30},
		},
		Operation: map[uint64]*model.KeySpanOperation{
			1: {Delete: false, BoundaryTs: 60, Status: model.OperProcessed},
			2: {Delete: false, BoundaryTs: 50, Status: model.OperProcessed},
			3: {Delete: false, BoundaryTs: 40, Status: model.OperProcessed},
		},
	})
	c.Assert(p.keyspans, check.HasLen, 4)
	c.Assert(p.changefeed.TaskPositions[p.captureInfo.ID].CheckPointTs, check.Equals, uint64(30))
	c.Assert(p.changefeed.TaskPositions[p.captureInfo.ID].ResolvedTs, check.Equals, uint64(30))

	// add keyspan, push the resolvedTs, finished add keyspan
	keyspan1 := p.keyspans[1].(*mockKeySpanPipeline)
	keyspan2 := p.keyspans[2].(*mockKeySpanPipeline)
	keyspan3 := p.keyspans[3].(*mockKeySpanPipeline)
	keyspan4 := p.keyspans[4].(*mockKeySpanPipeline)
	keyspan1.resolvedTs = 101
	keyspan2.resolvedTs = 101
	keyspan3.resolvedTs = 102
	keyspan4.resolvedTs = 103
	// removed keyspan 3
	p.changefeed.PatchTaskStatus(p.captureInfo.ID, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
		status.RemoveKeySpan(3, 60, false)
		return status, true, nil
	})
	tester.MustApplyPatches()
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	tester.MustApplyPatches()
	c.Assert(p.changefeed.TaskStatuses[p.captureInfo.ID], check.DeepEquals, &model.TaskStatus{
		KeySpans: map[uint64]*model.KeySpanReplicaInfo{
			1: {StartTs: 60},
			2: {StartTs: 50},
			4: {StartTs: 30},
		},
		Operation: map[uint64]*model.KeySpanOperation{
			1: {Delete: false, BoundaryTs: 60, Status: model.OperFinished},
			2: {Delete: false, BoundaryTs: 50, Status: model.OperFinished},
			3: {Delete: true, BoundaryTs: 60, Status: model.OperProcessed},
		},
	})
	c.Assert(p.keyspans, check.HasLen, 4)
	c.Assert(keyspan3.canceled, check.IsFalse)
	c.Assert(keyspan3.stopTs, check.Equals, uint64(60))
	c.Assert(p.changefeed.TaskPositions[p.captureInfo.ID].ResolvedTs, check.Equals, uint64(101))

	// finish remove operations
	keyspan3.status = keyspanpipeline.KeySpanStatusStopped
	keyspan3.checkpointTs = 65
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	tester.MustApplyPatches()
	c.Assert(p.changefeed.TaskStatuses[p.captureInfo.ID], check.DeepEquals, &model.TaskStatus{
		KeySpans: map[uint64]*model.KeySpanReplicaInfo{
			1: {StartTs: 60},
			2: {StartTs: 50},
			4: {StartTs: 30},
		},
		Operation: map[uint64]*model.KeySpanOperation{
			1: {Delete: false, BoundaryTs: 60, Status: model.OperFinished},
			2: {Delete: false, BoundaryTs: 50, Status: model.OperFinished},
			3: {Delete: true, BoundaryTs: 65, Status: model.OperFinished},
		},
	})
	c.Assert(p.keyspans, check.HasLen, 3)
	c.Assert(keyspan3.canceled, check.IsTrue)

	// clear finished operations
	cleanUpFinishedOpOperation(p.changefeed, p.captureInfo.ID, tester)

	// remove keyspan, in processing
	p.changefeed.PatchTaskStatus(p.captureInfo.ID, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
		status.RemoveKeySpan(1, 120, false)
		status.RemoveKeySpan(4, 120, false)
		delete(status.KeySpans, 2)
		return status, true, nil
	})
	tester.MustApplyPatches()
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	tester.MustApplyPatches()
	c.Assert(p.changefeed.TaskStatuses[p.captureInfo.ID], check.DeepEquals, &model.TaskStatus{
		KeySpans: map[uint64]*model.KeySpanReplicaInfo{},
		Operation: map[uint64]*model.KeySpanOperation{
			1: {Delete: true, BoundaryTs: 120, Status: model.OperProcessed},
			4: {Delete: true, BoundaryTs: 120, Status: model.OperProcessed},
		},
	})
	c.Assert(keyspan1.stopTs, check.Equals, uint64(120))
	c.Assert(keyspan4.stopTs, check.Equals, uint64(120))
	c.Assert(keyspan2.canceled, check.IsTrue)
	c.Assert(p.keyspans, check.HasLen, 2)

	// remove keyspan, not finished
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	tester.MustApplyPatches()
	c.Assert(p.changefeed.TaskStatuses[p.captureInfo.ID], check.DeepEquals, &model.TaskStatus{
		KeySpans: map[uint64]*model.KeySpanReplicaInfo{},
		Operation: map[uint64]*model.KeySpanOperation{
			1: {Delete: true, BoundaryTs: 120, Status: model.OperProcessed},
			4: {Delete: true, BoundaryTs: 120, Status: model.OperProcessed},
		},
	})

	// remove keyspan, finished
	keyspan1.status = keyspanpipeline.KeySpanStatusStopped
	keyspan1.checkpointTs = 121
	keyspan4.status = keyspanpipeline.KeySpanStatusStopped
	keyspan4.checkpointTs = 122
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	tester.MustApplyPatches()
	c.Assert(p.changefeed.TaskStatuses[p.captureInfo.ID], check.DeepEquals, &model.TaskStatus{
		KeySpans: map[uint64]*model.KeySpanReplicaInfo{},
		Operation: map[uint64]*model.KeySpanOperation{
			1: {Delete: true, BoundaryTs: 121, Status: model.OperFinished},
			4: {Delete: true, BoundaryTs: 122, Status: model.OperFinished},
		},
	})
	c.Assert(keyspan1.canceled, check.IsTrue)
	c.Assert(keyspan4.canceled, check.IsTrue)
	c.Assert(p.keyspans, check.HasLen, 0)
}

func (s *processorSuite) TestInitKeySpan(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx := cdcContext.NewBackendContext4Test(true)
	p, tester := initProcessor4Test(ctx, c)
	var err error
	// init tick
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	tester.MustApplyPatches()

	p.changefeed.PatchTaskStatus(p.captureInfo.ID, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
		status.KeySpans[1] = &model.KeySpanReplicaInfo{StartTs: 20}
		status.KeySpans[2] = &model.KeySpanReplicaInfo{StartTs: 30}
		return status, true, nil
	})
	tester.MustApplyPatches()
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	tester.MustApplyPatches()
	c.Assert(p.keyspans[1], check.Not(check.IsNil))
	c.Assert(p.keyspans[2], check.Not(check.IsNil))
}

func (s *processorSuite) TestProcessorError(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx := cdcContext.NewBackendContext4Test(true)
	p, tester := initProcessor4Test(ctx, c)
	var err error
	// init tick
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	tester.MustApplyPatches()

	// send a abnormal error
	p.sendError(cerror.ErrSinkURIInvalid)
	_, err = p.Tick(ctx, p.changefeed)
	tester.MustApplyPatches()
	c.Assert(cerror.ErrReactorFinished.Equal(errors.Cause(err)), check.IsTrue)
	c.Assert(p.changefeed.TaskPositions[p.captureInfo.ID], check.DeepEquals, &model.TaskPosition{
		Error: &model.RunningError{
			Addr:    "127.0.0.1:0000",
			Code:    "CDC:ErrSinkURIInvalid",
			Message: "[CDC:ErrSinkURIInvalid]sink uri invalid",
		},
	})

	p, tester = initProcessor4Test(ctx, c)
	// init tick
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	tester.MustApplyPatches()

	// send a normal error
	p.sendError(context.Canceled)
	_, err = p.Tick(ctx, p.changefeed)
	tester.MustApplyPatches()
	c.Assert(cerror.ErrReactorFinished.Equal(errors.Cause(err)), check.IsTrue)
	c.Assert(p.changefeed.TaskPositions[p.captureInfo.ID], check.DeepEquals, &model.TaskPosition{
		Error: nil,
	})
}

func (s *processorSuite) TestProcessorExit(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx := cdcContext.NewBackendContext4Test(true)
	p, tester := initProcessor4Test(ctx, c)
	var err error
	// init tick
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	tester.MustApplyPatches()

	// stop the changefeed
	p.changefeed.PatchStatus(func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
		status.AdminJobType = model.AdminStop
		return status, true, nil
	})
	p.changefeed.PatchTaskStatus(ctx.GlobalVars().CaptureInfo.ID, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
		status.AdminJobType = model.AdminStop
		return status, true, nil
	})
	tester.MustApplyPatches()
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(cerror.ErrReactorFinished.Equal(errors.Cause(err)), check.IsTrue)
	tester.MustApplyPatches()
	c.Assert(p.changefeed.TaskPositions[p.captureInfo.ID], check.DeepEquals, &model.TaskPosition{
		Error: nil,
	})
}

func (s *processorSuite) TestProcessorClose(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx := cdcContext.NewBackendContext4Test(true)
	p, tester := initProcessor4Test(ctx, c)
	var err error
	// init tick
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	tester.MustApplyPatches()

	// add keyspans
	p.changefeed.PatchTaskStatus(p.captureInfo.ID, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
		status.KeySpans[1] = &model.KeySpanReplicaInfo{StartTs: 20}
		status.KeySpans[2] = &model.KeySpanReplicaInfo{StartTs: 30}
		return status, true, nil
	})
	tester.MustApplyPatches()
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	tester.MustApplyPatches()

	// push the resolvedTs and checkpointTs
	p.changefeed.PatchStatus(func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
		status.ResolvedTs = 100
		return status, true, nil
	})
	tester.MustApplyPatches()
	p.keyspans[1].(*mockKeySpanPipeline).resolvedTs = 110
	p.keyspans[2].(*mockKeySpanPipeline).resolvedTs = 90
	p.keyspans[1].(*mockKeySpanPipeline).checkpointTs = 90
	p.keyspans[2].(*mockKeySpanPipeline).checkpointTs = 95
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	tester.MustApplyPatches()
	c.Assert(p.changefeed.TaskPositions[p.captureInfo.ID], check.DeepEquals, &model.TaskPosition{
		CheckPointTs: 90,
		ResolvedTs:   90,
		Error:        nil,
	})
	c.Assert(p.changefeed.TaskStatuses[p.captureInfo.ID], check.DeepEquals, &model.TaskStatus{
		KeySpans: map[uint64]*model.KeySpanReplicaInfo{1: {StartTs: 20}, 2: {StartTs: 30}},
	})
	c.Assert(p.changefeed.Workloads[p.captureInfo.ID], check.DeepEquals, model.TaskWorkload{1: {Workload: 1}, 2: {Workload: 1}})

	c.Assert(p.Close(), check.IsNil)
	tester.MustApplyPatches()
	c.Assert(p.keyspans[1].(*mockKeySpanPipeline).canceled, check.IsTrue)
	c.Assert(p.keyspans[2].(*mockKeySpanPipeline).canceled, check.IsTrue)

	p, tester = initProcessor4Test(ctx, c)
	// init tick
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	tester.MustApplyPatches()

	// add keyspans
	p.changefeed.PatchTaskStatus(p.captureInfo.ID, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
		status.KeySpans[1] = &model.KeySpanReplicaInfo{StartTs: 20}
		status.KeySpans[2] = &model.KeySpanReplicaInfo{StartTs: 30}
		return status, true, nil
	})
	tester.MustApplyPatches()
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	tester.MustApplyPatches()

	// send error
	p.sendError(cerror.ErrSinkURIInvalid)
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(cerror.ErrReactorFinished.Equal(errors.Cause(err)), check.IsTrue)
	tester.MustApplyPatches()

	c.Assert(p.Close(), check.IsNil)
	tester.MustApplyPatches()
	c.Assert(p.changefeed.TaskPositions[p.captureInfo.ID].Error, check.DeepEquals, &model.RunningError{
		Addr:    "127.0.0.1:0000",
		Code:    "CDC:ErrSinkURIInvalid",
		Message: "[CDC:ErrSinkURIInvalid]sink uri invalid",
	})
	c.Assert(p.keyspans[1].(*mockKeySpanPipeline).canceled, check.IsTrue)
	c.Assert(p.keyspans[2].(*mockKeySpanPipeline).canceled, check.IsTrue)
}

func (s *processorSuite) TestPositionDeleted(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx := cdcContext.NewBackendContext4Test(true)
	p, tester := initProcessor4Test(ctx, c)
	p.changefeed.PatchTaskStatus(p.captureInfo.ID, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
		status.KeySpans[1] = &model.KeySpanReplicaInfo{StartTs: 30}
		status.KeySpans[2] = &model.KeySpanReplicaInfo{StartTs: 40}
		return status, true, nil
	})
	var err error
	// init tick
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	tester.MustApplyPatches()

	// cal position
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	tester.MustApplyPatches()
	c.Assert(p.changefeed.TaskPositions[p.captureInfo.ID], check.DeepEquals, &model.TaskPosition{
		CheckPointTs: 30,
		ResolvedTs:   30,
	})

	// some other delete the task position
	p.changefeed.PatchTaskPosition(p.captureInfo.ID, func(position *model.TaskPosition) (*model.TaskPosition, bool, error) {
		return nil, true, nil
	})
	tester.MustApplyPatches()
	// position created again
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	tester.MustApplyPatches()
	c.Assert(p.changefeed.TaskPositions[p.captureInfo.ID], check.DeepEquals, &model.TaskPosition{
		CheckPointTs: 0,
		ResolvedTs:   0,
	})

	// cal position
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	tester.MustApplyPatches()
	c.Assert(p.changefeed.TaskPositions[p.captureInfo.ID], check.DeepEquals, &model.TaskPosition{
		CheckPointTs: 30,
		ResolvedTs:   30,
	})
}

func cleanUpFinishedOpOperation(state *orchestrator.ChangefeedReactorState, captureID model.CaptureID, tester *orchestrator.ReactorStateTester) {
	state.PatchTaskStatus(captureID, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
		if status == nil || status.Operation == nil {
			return status, false, nil
		}
		for keyspanID, opt := range status.Operation {
			if opt.Status == model.OperFinished {
				delete(status.Operation, keyspanID)
			}
		}
		return status, true, nil
	})
	tester.MustApplyPatches()
}

func (s *processorSuite) TestIgnorableError(c *check.C) {
	defer testleak.AfterTest(c)()

	testCases := []struct {
		err       error
		ignorable bool
	}{
		{nil, true},
		{cerror.ErrAdminStopProcessor.GenWithStackByArgs(), true},
		{cerror.ErrReactorFinished.GenWithStackByArgs(), true},
		{cerror.ErrRedoWriterStopped.GenWithStackByArgs(), true},
		{errors.Trace(context.Canceled), true},
		{cerror.ErrProcessorKeySpanNotFound.GenWithStackByArgs(), false},
		{errors.New("test error"), false},
	}
	for _, tc := range testCases {
		c.Assert(isProcessorIgnorableError(tc.err), check.Equals, tc.ignorable)
	}
}

func (s *processorSuite) TestHandleKeySpanOperationWithRelatedKeySpans(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx := cdcContext.NewBackendContext4Test(true)
	p, tester := initProcessor4Test(ctx, c)
	var err error

	// no operation
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	tester.MustApplyPatches()

	// add keyspan1, in processing
	p.changefeed.PatchTaskStatus(p.captureInfo.ID, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
		status.AddKeySpan(1, &model.KeySpanReplicaInfo{StartTs: 60}, 80, nil)
		return status, true, nil
	})
	tester.MustApplyPatches()
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	tester.MustApplyPatches()
	c.Assert(p.changefeed.TaskStatuses[p.captureInfo.ID], check.DeepEquals, &model.TaskStatus{
		KeySpans: map[uint64]*model.KeySpanReplicaInfo{
			1: {StartTs: 60},
		},
		Operation: map[uint64]*model.KeySpanOperation{
			1: {Delete: false, BoundaryTs: 80, Status: model.OperProcessed},
		},
	})
	c.Assert(p.keyspans, check.HasLen, 1)
	c.Assert(p.changefeed.TaskPositions[p.captureInfo.ID].CheckPointTs, check.Equals, uint64(60))
	c.Assert(p.changefeed.TaskPositions[p.captureInfo.ID].ResolvedTs, check.Equals, uint64(60))

	// add keyspan2 & keyspan3, remove keyspan1
	p.changefeed.PatchTaskStatus(p.captureInfo.ID, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
		status.AddKeySpan(2, &model.KeySpanReplicaInfo{StartTs: 60}, 60, []model.KeySpanLocation{{CaptureID: p.captureInfo.ID, KeySpanID: 1}})
		status.AddKeySpan(3, &model.KeySpanReplicaInfo{StartTs: 60}, 60, []model.KeySpanLocation{{CaptureID: p.captureInfo.ID, KeySpanID: 1}})
		status.RemoveKeySpan(1, 60, false)
		return status, true, nil
	})
	tester.MustApplyPatches()
	// try to stop keyspand1
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	tester.MustApplyPatches()
	c.Assert(p.changefeed.TaskStatuses[p.captureInfo.ID].KeySpans, check.DeepEquals, map[uint64]*model.KeySpanReplicaInfo{
		2: {StartTs: 60},
		3: {StartTs: 60},
	})
	c.Assert(p.changefeed.TaskStatuses[p.captureInfo.ID].Operation, check.DeepEquals, map[uint64]*model.KeySpanOperation{
		1: {Delete: true, BoundaryTs: 60, Status: model.OperProcessed},
		2: {Delete: false, BoundaryTs: 60, Status: model.OperDispatched, RelatedKeySpans: []model.KeySpanLocation{{CaptureID: p.captureInfo.ID, KeySpanID: 1}}},
		3: {Delete: false, BoundaryTs: 60, Status: model.OperDispatched, RelatedKeySpans: []model.KeySpanLocation{{CaptureID: p.captureInfo.ID, KeySpanID: 1}}},
	})
	keyspan1 := p.keyspans[1].(*mockKeySpanPipeline)
	keyspan1.status = keyspanpipeline.KeySpanStatusStopped

	// finish stoping keyspand1
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	tester.MustApplyPatches()
	c.Assert(p.changefeed.TaskStatuses[p.captureInfo.ID].Operation, check.DeepEquals, map[uint64]*model.KeySpanOperation{
		1: {Delete: true, BoundaryTs: 60, Status: model.OperFinished},
		2: {Delete: false, BoundaryTs: 60, Status: model.OperDispatched, RelatedKeySpans: []model.KeySpanLocation{{CaptureID: p.captureInfo.ID, KeySpanID: 1}}},
		3: {Delete: false, BoundaryTs: 60, Status: model.OperDispatched, RelatedKeySpans: []model.KeySpanLocation{{CaptureID: p.captureInfo.ID, KeySpanID: 1}}},
	})
	cleanUpFinishedOpOperation(p.changefeed, p.captureInfo.ID, tester)

	// start keyspan2 & keyspan3
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	tester.MustApplyPatches()
	c.Assert(p.changefeed.TaskStatuses[p.captureInfo.ID].Operation, check.DeepEquals, map[uint64]*model.KeySpanOperation{
		2: {Delete: false, BoundaryTs: 60, Status: model.OperProcessed, RelatedKeySpans: []model.KeySpanLocation{{CaptureID: p.captureInfo.ID, KeySpanID: 1}}},
		3: {Delete: false, BoundaryTs: 60, Status: model.OperProcessed, RelatedKeySpans: []model.KeySpanLocation{{CaptureID: p.captureInfo.ID, KeySpanID: 1}}},
	})
}
