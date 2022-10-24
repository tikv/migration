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

package capture

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tikv/migration/cdc/cdc/model"
	"github.com/tikv/migration/cdc/pkg/config"
)

func TestVerifyUpdateChangefeedConfig(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	oldInfo := &model.ChangeFeedInfo{Config: config.GetDefaultReplicaConfig()}
	// test startTs > targetTs
	changefeedConfig := model.ChangefeedConfig{TargetTS: 20}
	oldInfo.StartTs = 40
	newInfo, err := verifyUpdateChangefeedConfig(ctx, changefeedConfig, oldInfo)
	require.NotNil(t, err)
	require.Regexp(t, ".*can not update target-ts.*less than start-ts.*", err)
	require.Nil(t, newInfo)

	// test no change error
	changefeedConfig = model.ChangefeedConfig{SinkURI: "blackhole://"}
	oldInfo.SinkURI = "blackhole://"
	newInfo, err = verifyUpdateChangefeedConfig(ctx, changefeedConfig, oldInfo)
	require.NotNil(t, err)
	require.Regexp(t, ".*changefeed config is the same with the old one.*", err)
	require.Nil(t, newInfo)

	changefeedConfig = model.ChangefeedConfig{SortEngine: "db"}
	newInfo, err = verifyUpdateChangefeedConfig(ctx, changefeedConfig, oldInfo)
	require.NotNil(t, err)
	require.Regexp(t, ".*can not update sort engin.*", err)
	require.Nil(t, newInfo)

	changefeedConfig = model.ChangefeedConfig{StartKey: "r", EndKey: "s", Format: "hex"}
	newInfo, err = verifyUpdateChangefeedConfig(ctx, changefeedConfig, oldInfo)
	require.NotNil(t, err)
	require.Regexp(t, ".*update start-key and end-key is not supported.*", err)
	require.Nil(t, newInfo)

	// test verify success
	changefeedConfig = model.ChangefeedConfig{SortEngine: "memory", SinkConfig: &config.SinkConfig{Protocol: "test"}}
	newInfo, err = verifyUpdateChangefeedConfig(ctx, changefeedConfig, oldInfo)
	require.Nil(t, err)
	require.NotNil(t, newInfo)
}
