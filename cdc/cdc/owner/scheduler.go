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
	"github.com/tikv/migration/cdc/cdc/model"
	"github.com/tikv/migration/cdc/pkg/context"
	"github.com/tikv/migration/cdc/pkg/orchestrator"
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

func newScheduler(ctx context.Context, startTs uint64) (scheduler, error) {
	return newSchedulerV1(updateCurrentKeySpansImpl), nil
}

func newScheduler4Test(ctx context.Context, startTs uint64) (scheduler, error) {
	return newSchedulerV1(updateCurrentKeySpansImpl4Test), nil
}
