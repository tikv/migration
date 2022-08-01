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
	"time"

	"github.com/pingcap/errors"
	"github.com/r3labs/diff"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/migration/cdc/cdc/model"
	"github.com/tikv/migration/cdc/cdc/sink"
	"github.com/tikv/migration/cdc/pkg/config"
	cerror "github.com/tikv/migration/cdc/pkg/errors"

	"github.com/tikv/migration/cdc/pkg/txnutil/gc"
	"github.com/tikv/migration/cdc/pkg/util"
	"github.com/tikv/migration/cdc/pkg/version"
)

// verifyCreateChangefeedConfig verify ChangefeedConfig for create a changefeed
func verifyCreateChangefeedConfig(ctx context.Context, changefeedConfig model.ChangefeedConfig, capture *Capture) (*model.ChangeFeedInfo, error) {
	// verify sinkURI
	if changefeedConfig.SinkURI == "" {
		return nil, cerror.ErrSinkURIInvalid.GenWithStackByArgs("sink-uri is empty, can't not create a changefeed without sink-uri")
	}

	// verify changefeedID
	if err := model.ValidateChangefeedID(changefeedConfig.ID); err != nil {
		return nil, cerror.ErrAPIInvalidParam.GenWithStack("invalid changefeed_id: %s", changefeedConfig.ID)
	}
	// check if the changefeed exists
	cfStatus, err := capture.owner.StatusProvider().GetChangeFeedStatus(ctx, changefeedConfig.ID)
	if err != nil && cerror.ErrChangeFeedNotExists.NotEqual(err) {
		return nil, err
	}
	if cfStatus != nil {
		return nil, cerror.ErrChangeFeedAlreadyExists.GenWithStackByArgs(changefeedConfig.ID)
	}

	// verify start-ts
	if changefeedConfig.StartTS == 0 {
		ts, logical, err := capture.pdClient.GetTS(ctx)
		if err != nil {
			return nil, cerror.ErrPDEtcdAPIError.GenWithStackByArgs("fail to get ts from pd client")
		}
		changefeedConfig.StartTS = oracle.ComposeTS(ts, logical)
	}

	// Ensure the start ts is valid in the next 1 hour.
	const ensureTTL = 60 * 60
	if err := gc.EnsureChangefeedStartTsSafety(
		ctx, capture.pdClient, changefeedConfig.ID, ensureTTL, changefeedConfig.StartTS); err != nil {
		if !cerror.ErrStartTsBeforeGC.Equal(err) {
			return nil, cerror.ErrPDEtcdAPIError.Wrap(err)
		}
		return nil, err
	}

	// verify target-ts
	if changefeedConfig.TargetTS > 0 && changefeedConfig.TargetTS <= changefeedConfig.StartTS {
		return nil, cerror.ErrTargetTsBeforeStartTs.GenWithStackByArgs(changefeedConfig.TargetTS, changefeedConfig.StartTS)
	}

	// init replicaConfig
	replicaConfig := config.GetDefaultReplicaConfig()
	if changefeedConfig.SinkConfig != nil {
		replicaConfig.Sink = changefeedConfig.SinkConfig
	}

	captureInfos, err := capture.owner.StatusProvider().GetCaptures(ctx)
	if err != nil {
		return nil, err
	}
	// set sortEngine and EnableOldValue
	cdcClusterVer, err := version.GetTiKVCDCClusterVersion(model.ListVersionsFromCaptureInfos(captureInfos))
	if err != nil {
		return nil, err
	}
	sortEngine := model.SortUnified
	if !cdcClusterVer.ShouldEnableOldValueByDefault() {
		replicaConfig.EnableOldValue = false
		if !cdcClusterVer.ShouldEnableUnifiedSorterByDefault() {
			sortEngine = model.SortInMemory
		}
	}

	// init ChangefeedInfo
	info := &model.ChangeFeedInfo{
		SinkURI:        changefeedConfig.SinkURI,
		Opts:           make(map[string]string),
		CreateTime:     time.Now(),
		StartTs:        changefeedConfig.StartTS,
		TargetTs:       changefeedConfig.TargetTS,
		Config:         replicaConfig,
		Engine:         sortEngine,
		State:          model.StateNormal,
		CreatorVersion: version.ReleaseVersion,
	}

	tz, err := util.GetTimezone(changefeedConfig.TimeZone)
	if err != nil {
		return nil, cerror.ErrAPIInvalidParam.Wrap(errors.Annotatef(err, "invalid timezone:%s", changefeedConfig.TimeZone))
	}
	ctx = util.PutTimezoneInCtx(ctx, tz)
	if err := sink.Validate(ctx, info.SinkURI, info.Config, info.Opts); err != nil {
		return nil, err
	}

	return info, nil
}

// verifyUpdateChangefeedConfig verify ChangefeedConfig for update a changefeed
func verifyUpdateChangefeedConfig(ctx context.Context, changefeedConfig model.ChangefeedConfig, oldInfo *model.ChangeFeedInfo) (*model.ChangeFeedInfo, error) {
	newInfo, err := oldInfo.Clone()
	if err != nil {
		return nil, cerror.ErrChangefeedUpdateRefused.GenWithStackByArgs(err.Error())
	}
	// verify target_ts
	if changefeedConfig.TargetTS != 0 {
		if changefeedConfig.TargetTS <= newInfo.StartTs {
			return nil, cerror.ErrChangefeedUpdateRefused.GenWithStack("can not update target-ts:%d less than start-ts:%d", changefeedConfig.TargetTS, newInfo.StartTs)
		}
		newInfo.TargetTs = changefeedConfig.TargetTS
	}

	// verify rules
	if changefeedConfig.SinkConfig != nil {
		newInfo.Config.Sink = changefeedConfig.SinkConfig
	}

	// verify sink_uri
	if changefeedConfig.SinkURI != "" {
		newInfo.SinkURI = changefeedConfig.SinkURI
		if err := sink.Validate(ctx, changefeedConfig.SinkURI, newInfo.Config, newInfo.Opts); err != nil {
			return nil, cerror.ErrChangefeedUpdateRefused.GenWithStackByCause(err)
		}
	}

	if !diff.Changed(oldInfo, newInfo) {
		return nil, cerror.ErrChangefeedUpdateRefused.GenWithStackByArgs("changefeed config is the same with the old one, do nothing")
	}

	return newInfo, nil
}
