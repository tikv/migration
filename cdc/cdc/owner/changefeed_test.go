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
	"path/filepath"

	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/tikv/migration/cdc/cdc/model"
	"github.com/tikv/migration/cdc/pkg/config"
	cdcContext "github.com/tikv/migration/cdc/pkg/context"
	"github.com/tikv/migration/cdc/pkg/orchestrator"
	"github.com/tikv/migration/cdc/pkg/txnutil/gc"
	"github.com/tikv/migration/cdc/pkg/util/testleak"
)

var _ = check.Suite(&changefeedSuite{})

type changefeedSuite struct{}

func createChangefeed4Test(ctx cdcContext.Context, c *check.C) (*changefeed, *orchestrator.ChangefeedReactorState,
	map[model.CaptureID]*model.CaptureInfo, *orchestrator.ReactorStateTester) {
	ctx.GlobalVars().PDClient = &gc.MockPDClient{
		UpdateServiceGCSafePointFunc: func(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
			return safePoint, nil
		},
	}
	gcManager := gc.NewManager(ctx.GlobalVars().PDClient)
	cf := newChangefeed4Test(ctx.ChangefeedVars().ID, gcManager)
	state := orchestrator.NewChangefeedReactorState(ctx.ChangefeedVars().ID)
	tester := orchestrator.NewReactorStateTester(c, state, nil)
	state.PatchInfo(func(info *model.ChangeFeedInfo) (*model.ChangeFeedInfo, bool, error) {
		c.Assert(info, check.IsNil)
		info = ctx.ChangefeedVars().Info
		return info, true, nil
	})
	tester.MustUpdate("/tikv/cdc/capture/"+ctx.GlobalVars().CaptureInfo.ID, []byte(`{"id":"`+ctx.GlobalVars().CaptureInfo.ID+`","address":"127.0.0.1:8300"}`))
	tester.MustApplyPatches()
	captures := map[model.CaptureID]*model.CaptureInfo{ctx.GlobalVars().CaptureInfo.ID: ctx.GlobalVars().CaptureInfo}
	return cf, state, captures, tester
}

func (s *changefeedSuite) TestPreCheck(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx := cdcContext.NewBackendContext4Test(true)
	cf, state, captures, tester := createChangefeed4Test(ctx, c)
	cf.Tick(ctx, state, captures)
	tester.MustApplyPatches()
	c.Assert(state.Status, check.NotNil)
	c.Assert(state.TaskStatuses, check.HasKey, ctx.GlobalVars().CaptureInfo.ID)

	// test clean the meta data of offline capture
	offlineCaputreID := "offline-capture"
	state.PatchTaskStatus(offlineCaputreID, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
		return new(model.TaskStatus), true, nil
	})
	state.PatchTaskPosition(offlineCaputreID, func(position *model.TaskPosition) (*model.TaskPosition, bool, error) {
		return new(model.TaskPosition), true, nil
	})
	state.PatchTaskWorkload(offlineCaputreID, func(workload model.TaskWorkload) (model.TaskWorkload, bool, error) {
		return make(model.TaskWorkload), true, nil
	})
	tester.MustApplyPatches()

	cf.Tick(ctx, state, captures)
	tester.MustApplyPatches()
	c.Assert(state.Status, check.NotNil)
	c.Assert(state.TaskStatuses, check.HasKey, ctx.GlobalVars().CaptureInfo.ID)
	c.Assert(state.TaskStatuses, check.Not(check.HasKey), offlineCaputreID)
	c.Assert(state.TaskPositions, check.Not(check.HasKey), offlineCaputreID)
	c.Assert(state.Workloads, check.Not(check.HasKey), offlineCaputreID)
}

func (s *changefeedSuite) TestInitialize(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx := cdcContext.NewBackendContext4Test(true)
	cf, state, captures, tester := createChangefeed4Test(ctx, c)
	defer cf.Close(ctx)
	// pre check
	cf.Tick(ctx, state, captures)
	tester.MustApplyPatches()

	// initialize
	cf.Tick(ctx, state, captures)
	tester.MustApplyPatches()
	c.Assert(state.Status.CheckpointTs, check.Equals, ctx.ChangefeedVars().Info.StartTs)
}

func (s *changefeedSuite) TestHandleError(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx := cdcContext.NewBackendContext4Test(true)
	cf, state, captures, tester := createChangefeed4Test(ctx, c)
	defer cf.Close(ctx)

	// pre check
	cf.Tick(ctx, state, captures)
	tester.MustApplyPatches()

	// initialize
	cf.Tick(ctx, state, captures)
	tester.MustApplyPatches()

	cf.errCh <- errors.New("fake error")
	// handle error
	cf.Tick(ctx, state, captures)
	tester.MustApplyPatches()
	c.Assert(state.Status.CheckpointTs, check.Equals, ctx.ChangefeedVars().Info.StartTs)
	c.Assert(state.Info.Error.Message, check.Equals, "fake error")
}

func (s *changefeedSuite) TestRemoveChangefeed(c *check.C) {
	defer testleak.AfterTest(c)()

	baseCtx, cancel := context.WithCancel(context.Background())
	ctx := cdcContext.NewContext4Test(baseCtx, true)
	info := ctx.ChangefeedVars().Info
	dir := c.MkDir()
	info.Config.Consistent = &config.ConsistentConfig{
		Level:   "eventual",
		Storage: filepath.Join("nfs://", dir),
	}
	ctx = cdcContext.WithChangefeedVars(ctx, &cdcContext.ChangefeedVars{
		ID:   ctx.ChangefeedVars().ID,
		Info: info,
	})
	testChangefeedReleaseResource(c, ctx, cancel, true /*expectedInitialized*/)
}

func (s *changefeedSuite) TestRemovePausedChangefeed(c *check.C) {
	defer testleak.AfterTest(c)()

	baseCtx, cancel := context.WithCancel(context.Background())
	ctx := cdcContext.NewContext4Test(baseCtx, true)
	info := ctx.ChangefeedVars().Info
	info.State = model.StateStopped
	dir := c.MkDir()
	info.Config.Consistent = &config.ConsistentConfig{
		Level:   "eventual",
		Storage: filepath.Join("nfs://", dir),
	}
	ctx = cdcContext.WithChangefeedVars(ctx, &cdcContext.ChangefeedVars{
		ID:   ctx.ChangefeedVars().ID,
		Info: info,
	})
	testChangefeedReleaseResource(c, ctx, cancel, false /*expectedInitialized*/)
}

func testChangefeedReleaseResource(
	c *check.C,
	ctx cdcContext.Context,
	cancel context.CancelFunc,
	expectedInitialized bool,
) {
	cf, state, captures, tester := createChangefeed4Test(ctx, c)

	// pre check
	cf.Tick(ctx, state, captures)
	tester.MustApplyPatches()

	// initialize
	cf.Tick(ctx, state, captures)
	tester.MustApplyPatches()
	c.Assert(cf.initialized, check.Equals, expectedInitialized)

	// remove changefeed from state manager by admin job
	cf.feedStateManager.PushAdminJob(&model.AdminJob{
		CfID: cf.id,
		Type: model.AdminRemove,
	})
	// changefeed tick will release resources
	err := cf.tick(ctx, state, captures)
	c.Assert(err, check.IsNil)
	cancel()
}
