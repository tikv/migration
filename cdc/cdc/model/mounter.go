// Copyright 2020 PingCAP, Inc.
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
	"context"
)

// PolymorphicEvent describes an event can be in multiple states
type PolymorphicEvent struct {
	StartTs uint64
	// Commit or resolved TS
	CRTs uint64

	RawKV    *RawKVEntry
	finished chan struct{}
}

// NewPolymorphicEvent creates a new PolymorphicEvent with a raw KV
func NewPolymorphicEvent(rawKV *RawKVEntry) *PolymorphicEvent {
	if rawKV.OpType == OpTypeResolved {
		return NewResolvedPolymorphicEvent(rawKV.RegionID, rawKV.CRTs, rawKV.KeySpanID)
	}
	return &PolymorphicEvent{
		StartTs:  rawKV.StartTs,
		CRTs:     rawKV.CRTs,
		RawKV:    rawKV,
		finished: nil,
	}
}

// NewResolvedPolymorphicEvent creates a new PolymorphicEvent with the resolved ts
func NewResolvedPolymorphicEvent(regionID uint64, resolvedTs uint64, keyspanID uint64) *PolymorphicEvent {
	return &PolymorphicEvent{
		CRTs:     resolvedTs,
		RawKV:    &RawKVEntry{CRTs: resolvedTs, OpType: OpTypeResolved, RegionID: regionID, KeySpanID: keyspanID},
		finished: nil,
	}
}

// RegionID returns the region ID where the event comes from.
func (e *PolymorphicEvent) RegionID() uint64 {
	return e.RawKV.RegionID
}

// IsResolved returns true if the event is resolved. Note that this function can
// only be called when `RawKV != nil`.
func (e *PolymorphicEvent) IsResolved() bool {
	return e.RawKV.OpType == OpTypeResolved
}

// ComparePolymorphicEvents compares two events by CRTs, Resolved order.
// It returns true if and only if i should precede j.
func ComparePolymorphicEvents(i, j *PolymorphicEvent) bool {
	if i.CRTs == j.CRTs {
		if i.IsResolved() {
			return false
		} else if j.IsResolved() {
			return true
		}
	}
	return i.CRTs < j.CRTs
}

// SetUpFinishedChan creates an internal channel to support PrepareFinished and WaitPrepare
func (e *PolymorphicEvent) SetUpFinishedChan() {
	if e.finished == nil {
		e.finished = make(chan struct{})
	}
}

// PrepareFinished marks the prepare process is finished
// In prepare process, Mounter will translate raw KV to row data
func (e *PolymorphicEvent) PrepareFinished() {
	if e.finished != nil {
		close(e.finished)
	}
}

// WaitPrepare waits for prepare process finished
func (e *PolymorphicEvent) WaitPrepare(ctx context.Context) error {
	if e.finished != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-e.finished:
		}
	}
	return nil
}
