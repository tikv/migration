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

package model

import (
	"encoding/json"
	"time"

	"github.com/tikv/migration/cdc/pkg/config"
	cerror "github.com/tikv/migration/cdc/pkg/errors"
)

const timeFormat = `"2006-01-02 15:04:05.000"`

// JSONTime used to wrap time into json format
type JSONTime time.Time

// MarshalJSON used to specify the time format
func (t JSONTime) MarshalJSON() ([]byte, error) {
	stamp := time.Time(t).Format(timeFormat)
	return []byte(stamp), nil
}

// UnmarshalJSON is used to parse time.Time from bytes. The builtin json.Unmarshal function cannot unmarshal
// a date string formatted as "2006-01-02 15:04:05.000", so we must implement a customized unmarshal function.
func (t *JSONTime) UnmarshalJSON(data []byte) error {
	tm, err := time.Parse(timeFormat, string(data))
	if err != nil {
		return err
	}

	*t = JSONTime(tm)
	return nil
}

// HTTPError of cdc http api
type HTTPError struct {
	Error string `json:"error_msg"`
	Code  string `json:"error_code"`
}

// NewHTTPError wrap a err into HTTPError
func NewHTTPError(err error) HTTPError {
	errCode, _ := cerror.RFCCode(err)
	return HTTPError{
		Error: err.Error(),
		Code:  string(errCode),
	}
}

// ServerStatus holds some common information of a server
type ServerStatus struct {
	Version string `json:"version"`
	GitHash string `json:"git_hash"`
	ID      string `json:"id"`
	Pid     int    `json:"pid"`
	IsOwner bool   `json:"is_owner"`
}

// ChangefeedCommonInfo holds some common usage information of a changefeed
type ChangefeedCommonInfo struct {
	ID             string        `json:"id"`
	FeedState      FeedState     `json:"state"`
	CheckpointTSO  uint64        `json:"checkpoint_tso"`
	CheckpointTime JSONTime      `json:"checkpoint_time"`
	RunningError   *RunningError `json:"error"`
}

// MarshalJSON use to marshal ChangefeedCommonInfo
func (c ChangefeedCommonInfo) MarshalJSON() ([]byte, error) {
	// alias the original type to prevent recursive call of MarshalJSON
	type Alias ChangefeedCommonInfo
	if c.FeedState == StateNormal {
		c.RunningError = nil
	}
	return json.Marshal(struct {
		Alias
	}{
		Alias: Alias(c),
	})
}

// ChangefeedDetail holds detail info of a changefeed
type ChangefeedDetail struct {
	ID             string              `json:"id"`
	SinkURI        string              `json:"sink_uri"`
	CreateTime     JSONTime            `json:"create_time"`
	StartTs        uint64              `json:"start_ts"`
	ResolvedTs     uint64              `json:"resolved_ts"`
	TargetTs       uint64              `json:"target_ts"`
	CheckpointTSO  uint64              `json:"checkpoint_tso"`
	CheckpointTime JSONTime            `json:"checkpoint_time"`
	Engine         SortEngine          `json:"sort_engine"`
	FeedState      FeedState           `json:"state"`
	RunningError   *RunningError       `json:"error"`
	ErrorHis       []int64             `json:"error_history"`
	CreatorVersion string              `json:"creator_version"`
	TaskStatus     []CaptureTaskStatus `json:"task_status"`
}

// MarshalJSON use to marshal ChangefeedDetail
func (c ChangefeedDetail) MarshalJSON() ([]byte, error) {
	// alias the original type to prevent recursive call of MarshalJSON
	type Alias ChangefeedDetail
	if c.FeedState == StateNormal {
		c.RunningError = nil
	}
	return json.Marshal(struct {
		Alias
	}{
		Alias: Alias(c),
	})
}

// ChangefeedConfig use to create a changefeed
type ChangefeedConfig struct {
	ID         string `json:"changefeed_id"`
	StartTS    uint64 `json:"start_ts"`
	TargetTS   uint64 `json:"target_ts"`
	SinkURI    string `json:"sink_uri"`
	Format     string `json:"format"`
	StartKey   string `json:"start_key"`
	EndKey     string `json:"end_key"`
	SortEngine string `json:"sort_engine"`
	// timezone used when checking sink uri
	TimeZone   string             `json:"timezone" default:"system"`
	SinkConfig *config.SinkConfig `json:"sink_config"`
}

// ProcessorCommonInfo holds the common info of a processor
type ProcessorCommonInfo struct {
	CfID      string `json:"changefeed_id"`
	CaptureID string `json:"capture_id"`
}

// ProcessorDetail holds the detail info of a processor
type ProcessorDetail struct {
	// The maximum event CommitTs that has been synchronized.
	CheckPointTs uint64 `json:"checkpoint_ts"`
	// The event that satisfies CommitTs <= ResolvedTs can be synchronized.
	ResolvedTs uint64 `json:"resolved_ts"`
	// all keyspan that this processor are replicating
	KeySpans map[KeySpanID]*KeySpanReplicaInfo `json:"keyspans"`
	// The count of events that have been replicated.
	Count uint64 `json:"count"`
	// Error code when error happens
	Error *RunningError `json:"error"`
}

// CaptureTaskStatus holds TaskStatus of a capture
type CaptureTaskStatus struct {
	CaptureID string `json:"capture_id"`
	// KeySpan list, containing keyspans that processor should process
	KeySpans  []uint64                        `json:"keyspan_ids"`
	Operation map[KeySpanID]*KeySpanOperation `json:"keyspan_operations"`
}

// Capture holds common information of a capture in cdc
type Capture struct {
	ID            string `json:"id"`
	IsOwner       bool   `json:"is_owner"`
	AdvertiseAddr string `json:"address"`
}
