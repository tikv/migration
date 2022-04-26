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
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tikv/migration/cdc/cdc/model"
	"github.com/tikv/migration/cdc/cdc/scheduler/util"
	"go.uber.org/zap"
)

func TestBalancerFindVictims(t *testing.T) {
	balancer := newKeySpanNumberRebalancer(zap.L())
	keyspans := util.NewKeySpanSet()

	keyspans.AddKeySpanRecord(&util.KeySpanRecord{
		KeySpanID: 1,
		CaptureID: "capture-1",
	})
	keyspans.AddKeySpanRecord(&util.KeySpanRecord{
		KeySpanID: 2,
		CaptureID: "capture-1",
	})
	keyspans.AddKeySpanRecord(&util.KeySpanRecord{
		KeySpanID: 3,
		CaptureID: "capture-1",
	})
	keyspans.AddKeySpanRecord(&util.KeySpanRecord{
		KeySpanID: 4,
		CaptureID: "capture-1",
	})
	keyspans.AddKeySpanRecord(&util.KeySpanRecord{
		KeySpanID: 5,
		CaptureID: "capture-2",
	})
	keyspans.AddKeySpanRecord(&util.KeySpanRecord{
		KeySpanID: 6,
		CaptureID: "capture-2",
	})

	mockCaptureInfos := map[model.CaptureID]*model.CaptureInfo{
		"capture-1": {
			ID: "capture-1",
		},
		"capture-2": {
			ID: "capture-2",
		},
		"capture-3": {
			ID: "capture-3",
		},
	}

	victims := balancer.FindVictims(keyspans, mockCaptureInfos)
	require.Len(t, victims, 2)
	require.Contains(t, victims, &util.KeySpanRecord{
		KeySpanID: 1,
		CaptureID: "capture-1",
	})
	require.Contains(t, victims, &util.KeySpanRecord{
		KeySpanID: 2,
		CaptureID: "capture-1",
	})
}

func TestBalancerFindTarget(t *testing.T) {
	balancer := newKeySpanNumberRebalancer(zap.L())
	keyspans := util.NewKeySpanSet()

	keyspans.AddKeySpanRecord(&util.KeySpanRecord{
		KeySpanID: 1,
		CaptureID: "capture-1",
	})
	keyspans.AddKeySpanRecord(&util.KeySpanRecord{
		KeySpanID: 2,
		CaptureID: "capture-1",
	})
	keyspans.AddKeySpanRecord(&util.KeySpanRecord{
		KeySpanID: 3,
		CaptureID: "capture-1",
	})
	keyspans.AddKeySpanRecord(&util.KeySpanRecord{
		KeySpanID: 4,
		CaptureID: "capture-2",
	})
	keyspans.AddKeySpanRecord(&util.KeySpanRecord{
		KeySpanID: 5,
		CaptureID: "capture-2",
	})
	keyspans.AddKeySpanRecord(&util.KeySpanRecord{
		KeySpanID: 6,
		CaptureID: "capture-3",
	})

	mockCaptureInfos := map[model.CaptureID]*model.CaptureInfo{
		"capture-1": {
			ID: "capture-1",
		},
		"capture-2": {
			ID: "capture-2",
		},
		"capture-3": {
			ID: "capture-3",
		},
	}

	target, ok := balancer.FindTarget(keyspans, mockCaptureInfos)
	require.True(t, ok)
	require.Equal(t, "capture-3", target)
}

func TestBalancerNoCaptureAvailable(t *testing.T) {
	balancer := newKeySpanNumberRebalancer(zap.L())
	keyspans := util.NewKeySpanSet()

	_, ok := balancer.FindTarget(keyspans, map[model.CaptureID]*model.CaptureInfo{})
	require.False(t, ok)
}
