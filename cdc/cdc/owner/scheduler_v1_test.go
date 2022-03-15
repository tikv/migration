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

/*

import (
	"fmt"
	"math/rand"

	"github.com/pingcap/check"
	"github.com/tikv/migration/cdc/cdc/model"
	"github.com/tikv/migration/cdc/pkg/etcd"
	"github.com/tikv/migration/cdc/pkg/orchestrator"
	"github.com/tikv/migration/cdc/pkg/util/testleak"
)

var _ = check.Suite(&schedulerSuite{})

type schedulerSuite struct {
	changefeedID model.ChangeFeedID
	state        *orchestrator.ChangefeedReactorState
	tester       *orchestrator.ReactorStateTester
	captures     map[model.CaptureID]*model.CaptureInfo
	scheduler    *oldScheduler
}

func (s *schedulerSuite) reset(c *check.C) {
	s.changefeedID = fmt.Sprintf("test-changefeed-%x", rand.Uint32())
	s.state = orchestrator.NewChangefeedReactorState("test-changefeed")
	s.tester = orchestrator.NewReactorStateTester(c, s.state, nil)
	s.scheduler = newSchedulerV1().(*schedulerV1CompatWrapper).inner
	s.captures = make(map[model.CaptureID]*model.CaptureInfo)
	s.state.PatchStatus(func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
		return &model.ChangeFeedStatus{}, true, nil
	})
	s.tester.MustApplyPatches()
}

func (s *schedulerSuite) addCapture(captureID model.CaptureID) {
	captureInfo := &model.CaptureInfo{
		ID: captureID,
	}
	s.captures[captureID] = captureInfo
	s.state.PatchTaskStatus(captureID, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
		return &model.TaskStatus{}, true, nil
	})
	s.tester.MustApplyPatches()
}

func (s *schedulerSuite) finishKeySpanOperation(captureID model.CaptureID, keyspanIDs ...model.KeySpanID) {
	s.state.PatchTaskStatus(captureID, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
		for _, keyspanID := range keyspanIDs {
			status.Operation[keyspanID].Status = model.OperFinished
		}
		return status, true, nil
	})
	s.state.PatchTaskWorkload(captureID, func(workload model.TaskWorkload) (model.TaskWorkload, bool, error) {
		if workload == nil {
			workload = make(model.TaskWorkload)
		}
		for _, keyspanID := range keyspanIDs {
			if s.state.TaskStatuses[captureID].Operation[keyspanID].Delete {
				delete(workload, keyspanID)
			} else {
				workload[keyspanID] = model.WorkloadInfo{
					Workload: 1,
				}
			}
		}
		return workload, true, nil
	})
	s.tester.MustApplyPatches()
}

func (s *schedulerSuite) TestScheduleOneCapture(c *check.C) {
	defer testleak.AfterTest(c)()

	s.reset(c)
	captureID := "test-capture-0"
	s.addCapture(captureID)

	_, _ = s.scheduler.Tick(s.state, []model.KeySpanID{}, s.captures)

	// Manually simulate the scenario where the corresponding key was deleted in the etcd
	key := &etcd.CDCKey{
		Tp:           etcd.CDCKeyTypeTaskStatus,
		CaptureID:    captureID,
		ChangefeedID: s.state.ID,
	}
	s.tester.MustUpdate(key.String(), nil)
	s.tester.MustApplyPatches()

	s.reset(c)
	captureID = "test-capture-1"
	s.addCapture(captureID)

	// add three keyspans
	shouldUpdateState, err := s.scheduler.Tick(s.state, []model.KeySpanID{1, 2, 3, 4}, s.captures)
	c.Assert(err, check.IsNil)
	c.Assert(shouldUpdateState, check.IsFalse)
	s.tester.MustApplyPatches()
	c.Assert(s.state.TaskStatuses[captureID].KeySpans, check.DeepEquals, map[model.KeySpanID]*model.KeySpanReplicaInfo{
		1: {StartTs: 0}, 2: {StartTs: 0}, 3: {StartTs: 0}, 4: {StartTs: 0},
	})
	c.Assert(s.state.TaskStatuses[captureID].Operation, check.DeepEquals, map[model.KeySpanID]*model.KeySpanOperation{
		1: {Delete: false, BoundaryTs: 0, Status: model.OperDispatched},
		2: {Delete: false, BoundaryTs: 0, Status: model.OperDispatched},
		3: {Delete: false, BoundaryTs: 0, Status: model.OperDispatched},
		4: {Delete: false, BoundaryTs: 0, Status: model.OperDispatched},
	})
	shouldUpdateState, err = s.scheduler.Tick(s.state, []model.KeySpanID{1, 2, 3, 4}, s.captures)
	c.Assert(err, check.IsNil)
	c.Assert(shouldUpdateState, check.IsTrue)
	s.tester.MustApplyPatches()

	// two keyspans finish adding operation
	s.finishKeySpanOperation(captureID, 2, 3)

	// remove keyspan 1,2 and add keyspan 4,5
	shouldUpdateState, err = s.scheduler.Tick(s.state, []model.KeySpanID{3, 4, 5}, s.captures)
	c.Assert(err, check.IsNil)
	c.Assert(shouldUpdateState, check.IsFalse)
	s.tester.MustApplyPatches()
	c.Assert(s.state.TaskStatuses[captureID].KeySpans, check.DeepEquals, map[model.KeySpanID]*model.KeySpanReplicaInfo{
		3: {StartTs: 0}, 4: {StartTs: 0}, 5: {StartTs: 0},
	})
	c.Assert(s.state.TaskStatuses[captureID].Operation, check.DeepEquals, map[model.KeySpanID]*model.KeySpanOperation{
		1: {Delete: true, BoundaryTs: 0, Status: model.OperDispatched},
		2: {Delete: true, BoundaryTs: 0, Status: model.OperDispatched},
		4: {Delete: false, BoundaryTs: 0, Status: model.OperDispatched},
		5: {Delete: false, BoundaryTs: 0, Status: model.OperDispatched},
	})

	// move a non exist keyspan to a non exist capture
	s.scheduler.MoveKeySpan(2, "fake-capture")
	// move keyspans to a non exist capture
	s.scheduler.MoveKeySpan(3, "fake-capture")
	s.scheduler.MoveKeySpan(4, "fake-capture")
	shouldUpdateState, err = s.scheduler.Tick(s.state, []model.KeySpanID{3, 4, 5}, s.captures)
	c.Assert(err, check.IsNil)
	c.Assert(shouldUpdateState, check.IsFalse)
	s.tester.MustApplyPatches()
	c.Assert(s.state.TaskStatuses[captureID].KeySpans, check.DeepEquals, map[model.KeySpanID]*model.KeySpanReplicaInfo{
		4: {StartTs: 0}, 5: {StartTs: 0},
	})
	c.Assert(s.state.TaskStatuses[captureID].Operation, check.DeepEquals, map[model.KeySpanID]*model.KeySpanOperation{
		1: {Delete: true, BoundaryTs: 0, Status: model.OperDispatched},
		2: {Delete: true, BoundaryTs: 0, Status: model.OperDispatched},
		3: {Delete: true, BoundaryTs: 0, Status: model.OperDispatched},
		4: {Delete: false, BoundaryTs: 0, Status: model.OperDispatched},
		5: {Delete: false, BoundaryTs: 0, Status: model.OperDispatched},
	})

	// finish all operations
	s.finishKeySpanOperation(captureID, 1, 2, 3, 4, 5)

	shouldUpdateState, err = s.scheduler.Tick(s.state, []model.KeySpanID{3, 4, 5}, s.captures)
	c.Assert(err, check.IsNil)
	c.Assert(shouldUpdateState, check.IsTrue)
	s.tester.MustApplyPatches()
	c.Assert(s.state.TaskStatuses[captureID].KeySpans, check.DeepEquals, map[model.KeySpanID]*model.KeySpanReplicaInfo{
		4: {StartTs: 0}, 5: {StartTs: 0},
	})
	c.Assert(s.state.TaskStatuses[captureID].Operation, check.DeepEquals, map[model.KeySpanID]*model.KeySpanOperation{})

	// keyspan 3 is missing by expected, because the keyspan was trying to move to a invalid capture
	// and the move will failed, the keyspan 3 will be add in next tick
	shouldUpdateState, err = s.scheduler.Tick(s.state, []model.KeySpanID{3, 4, 5}, s.captures)
	c.Assert(err, check.IsNil)
	c.Assert(shouldUpdateState, check.IsFalse)
	s.tester.MustApplyPatches()
	c.Assert(s.state.TaskStatuses[captureID].KeySpans, check.DeepEquals, map[model.KeySpanID]*model.KeySpanReplicaInfo{
		4: {StartTs: 0}, 5: {StartTs: 0},
	})
	c.Assert(s.state.TaskStatuses[captureID].Operation, check.DeepEquals, map[model.KeySpanID]*model.KeySpanOperation{})

	shouldUpdateState, err = s.scheduler.Tick(s.state, []model.KeySpanID{3, 4, 5}, s.captures)
	c.Assert(err, check.IsNil)
	c.Assert(shouldUpdateState, check.IsFalse)
	s.tester.MustApplyPatches()
	c.Assert(s.state.TaskStatuses[captureID].KeySpans, check.DeepEquals, map[model.KeySpanID]*model.KeySpanReplicaInfo{
		3: {StartTs: 0}, 4: {StartTs: 0}, 5: {StartTs: 0},
	})
	c.Assert(s.state.TaskStatuses[captureID].Operation, check.DeepEquals, map[model.KeySpanID]*model.KeySpanOperation{
		3: {Delete: false, BoundaryTs: 0, Status: model.OperDispatched},
	})
}

func (s *schedulerSuite) TestScheduleMoveKeySpan(c *check.C) {
	defer testleak.AfterTest(c)()
	s.reset(c)
	captureID1 := "test-capture-1"
	captureID2 := "test-capture-2"
	s.addCapture(captureID1)

	// add a keyspan
	shouldUpdateState, err := s.scheduler.Tick(s.state, []model.KeySpanID{1}, s.captures)
	c.Assert(err, check.IsNil)
	c.Assert(shouldUpdateState, check.IsFalse)
	s.tester.MustApplyPatches()
	c.Assert(s.state.TaskStatuses[captureID1].KeySpans, check.DeepEquals, map[model.KeySpanID]*model.KeySpanReplicaInfo{
		1: {StartTs: 0},
	})
	c.Assert(s.state.TaskStatuses[captureID1].Operation, check.DeepEquals, map[model.KeySpanID]*model.KeySpanOperation{
		1: {Delete: false, BoundaryTs: 0, Status: model.OperDispatched},
	})

	s.finishKeySpanOperation(captureID1, 1)
	shouldUpdateState, err = s.scheduler.Tick(s.state, []model.KeySpanID{1}, s.captures)
	c.Assert(err, check.IsNil)
	c.Assert(shouldUpdateState, check.IsTrue)
	s.tester.MustApplyPatches()

	s.addCapture(captureID2)

	// add a keyspan
	shouldUpdateState, err = s.scheduler.Tick(s.state, []model.KeySpanID{1, 2}, s.captures)
	c.Assert(err, check.IsNil)
	c.Assert(shouldUpdateState, check.IsFalse)
	s.tester.MustApplyPatches()
	c.Assert(s.state.TaskStatuses[captureID1].KeySpans, check.DeepEquals, map[model.KeySpanID]*model.KeySpanReplicaInfo{
		1: {StartTs: 0},
	})
	c.Assert(s.state.TaskStatuses[captureID1].Operation, check.DeepEquals, map[model.KeySpanID]*model.KeySpanOperation{})
	c.Assert(s.state.TaskStatuses[captureID2].KeySpans, check.DeepEquals, map[model.KeySpanID]*model.KeySpanReplicaInfo{
		2: {StartTs: 0},
	})
	c.Assert(s.state.TaskStatuses[captureID2].Operation, check.DeepEquals, map[model.KeySpanID]*model.KeySpanOperation{
		2: {Delete: false, BoundaryTs: 0, Status: model.OperDispatched},
	})

	s.finishKeySpanOperation(captureID2, 2)

	s.scheduler.MoveKeySpan(2, captureID1)
	shouldUpdateState, err = s.scheduler.Tick(s.state, []model.KeySpanID{1, 2}, s.captures)
	c.Assert(err, check.IsNil)
	c.Assert(shouldUpdateState, check.IsFalse)
	s.tester.MustApplyPatches()
	c.Assert(s.state.TaskStatuses[captureID1].KeySpans, check.DeepEquals, map[model.KeySpanID]*model.KeySpanReplicaInfo{
		1: {StartTs: 0},
	})
	c.Assert(s.state.TaskStatuses[captureID1].Operation, check.DeepEquals, map[model.KeySpanID]*model.KeySpanOperation{})
	c.Assert(s.state.TaskStatuses[captureID2].KeySpans, check.DeepEquals, map[model.KeySpanID]*model.KeySpanReplicaInfo{})
	c.Assert(s.state.TaskStatuses[captureID2].Operation, check.DeepEquals, map[model.KeySpanID]*model.KeySpanOperation{
		2: {Delete: true, BoundaryTs: 0, Status: model.OperDispatched},
	})

	s.finishKeySpanOperation(captureID2, 2)

	shouldUpdateState, err = s.scheduler.Tick(s.state, []model.KeySpanID{1, 2}, s.captures)
	c.Assert(err, check.IsNil)
	c.Assert(shouldUpdateState, check.IsTrue)
	s.tester.MustApplyPatches()
	c.Assert(s.state.TaskStatuses[captureID1].KeySpans, check.DeepEquals, map[model.KeySpanID]*model.KeySpanReplicaInfo{
		1: {StartTs: 0},
	})
	c.Assert(s.state.TaskStatuses[captureID1].Operation, check.DeepEquals, map[model.KeySpanID]*model.KeySpanOperation{})
	c.Assert(s.state.TaskStatuses[captureID2].KeySpans, check.DeepEquals, map[model.KeySpanID]*model.KeySpanReplicaInfo{})
	c.Assert(s.state.TaskStatuses[captureID2].Operation, check.DeepEquals, map[model.KeySpanID]*model.KeySpanOperation{})

	shouldUpdateState, err = s.scheduler.Tick(s.state, []model.KeySpanID{1, 2}, s.captures)
	c.Assert(err, check.IsNil)
	c.Assert(shouldUpdateState, check.IsFalse)
	s.tester.MustApplyPatches()
	c.Assert(s.state.TaskStatuses[captureID1].KeySpans, check.DeepEquals, map[model.KeySpanID]*model.KeySpanReplicaInfo{
		1: {StartTs: 0}, 2: {StartTs: 0},
	})
	c.Assert(s.state.TaskStatuses[captureID1].Operation, check.DeepEquals, map[model.KeySpanID]*model.KeySpanOperation{
		2: {Delete: false, BoundaryTs: 0, Status: model.OperDispatched},
	})
	c.Assert(s.state.TaskStatuses[captureID2].KeySpans, check.DeepEquals, map[model.KeySpanID]*model.KeySpanReplicaInfo{})
	c.Assert(s.state.TaskStatuses[captureID2].Operation, check.DeepEquals, map[model.KeySpanID]*model.KeySpanOperation{})
}

func (s *schedulerSuite) TestScheduleRebalance(c *check.C) {
	defer testleak.AfterTest(c)()
	s.reset(c)
	captureID1 := "test-capture-1"
	captureID2 := "test-capture-2"
	captureID3 := "test-capture-3"
	s.addCapture(captureID1)
	s.addCapture(captureID2)
	s.addCapture(captureID3)

	s.state.PatchTaskStatus(captureID1, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
		status.KeySpans = make(map[model.KeySpanID]*model.KeySpanReplicaInfo)
		status.KeySpans[1] = &model.KeySpanReplicaInfo{StartTs: 1}
		status.KeySpans[2] = &model.KeySpanReplicaInfo{StartTs: 1}
		status.KeySpans[3] = &model.KeySpanReplicaInfo{StartTs: 1}
		status.KeySpans[4] = &model.KeySpanReplicaInfo{StartTs: 1}
		status.KeySpans[5] = &model.KeySpanReplicaInfo{StartTs: 1}
		status.KeySpans[6] = &model.KeySpanReplicaInfo{StartTs: 1}
		return status, true, nil
	})
	s.tester.MustApplyPatches()

	// rebalance keyspan
	shouldUpdateState, err := s.scheduler.Tick(s.state, []model.KeySpanID{1, 2, 3, 4, 5, 6}, s.captures)
	c.Assert(err, check.IsNil)
	c.Assert(shouldUpdateState, check.IsFalse)
	s.tester.MustApplyPatches()
	// 4 keyspans remove in capture 1, this 4 keyspans will be added to another capture in next tick
	c.Assert(s.state.TaskStatuses[captureID1].KeySpans, check.HasLen, 2)
	c.Assert(s.state.TaskStatuses[captureID2].KeySpans, check.HasLen, 0)
	c.Assert(s.state.TaskStatuses[captureID3].KeySpans, check.HasLen, 0)

	s.state.PatchTaskStatus(captureID1, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
		for _, opt := range status.Operation {
			opt.Status = model.OperFinished
		}
		return status, true, nil
	})
	s.state.PatchTaskWorkload(captureID1, func(workload model.TaskWorkload) (model.TaskWorkload, bool, error) {
		c.Assert(workload, check.IsNil)
		workload = make(model.TaskWorkload)
		for keyspanID := range s.state.TaskStatuses[captureID1].KeySpans {
			workload[keyspanID] = model.WorkloadInfo{Workload: 1}
		}
		return workload, true, nil
	})
	s.tester.MustApplyPatches()

	// clean finished operation
	shouldUpdateState, err = s.scheduler.Tick(s.state, []model.KeySpanID{1, 2, 3, 4, 5, 6}, s.captures)
	c.Assert(err, check.IsNil)
	c.Assert(shouldUpdateState, check.IsTrue)
	s.tester.MustApplyPatches()
	// 4 keyspans add to another capture in this tick
	c.Assert(s.state.TaskStatuses[captureID1].Operation, check.HasLen, 0)

	// rebalance keyspan
	shouldUpdateState, err = s.scheduler.Tick(s.state, []model.KeySpanID{1, 2, 3, 4, 5, 6}, s.captures)
	c.Assert(err, check.IsNil)
	c.Assert(shouldUpdateState, check.IsFalse)
	s.tester.MustApplyPatches()
	// 4 keyspans add to another capture in this tick
	c.Assert(s.state.TaskStatuses[captureID1].KeySpans, check.HasLen, 2)
	c.Assert(s.state.TaskStatuses[captureID2].KeySpans, check.HasLen, 2)
	c.Assert(s.state.TaskStatuses[captureID3].KeySpans, check.HasLen, 2)
	keyspanIDs := make(map[model.KeySpanID]struct{})
	for _, status := range s.state.TaskStatuses {
		for keyspanID := range status.KeySpans {
			keyspanIDs[keyspanID] = struct{}{}
		}
	}
	c.Assert(keyspanIDs, check.DeepEquals, map[model.KeySpanID]struct{}{1: {}, 2: {}, 3: {}, 4: {}, 5: {}, 6: {}})
}
*/
