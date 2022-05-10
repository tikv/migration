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
	"encoding/json"
	"fmt"
	"math"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	cerror "github.com/tikv/migration/cdc/pkg/errors"
	"go.uber.org/zap"
)

// AdminJobType represents for admin job type, both used in owner and processor
type AdminJobType int

// AdminJobOption records addition options of an admin job
type AdminJobOption struct {
	ForceRemove bool
}

// AdminJob holds an admin job
type AdminJob struct {
	CfID  string
	Type  AdminJobType
	Opts  *AdminJobOption
	Error *RunningError
}

// All AdminJob types
const (
	AdminNone AdminJobType = iota
	AdminStop
	AdminResume
	AdminRemove
	AdminFinish
)

// String implements fmt.Stringer interface.
func (t AdminJobType) String() string {
	switch t {
	case AdminNone:
		return "noop"
	case AdminStop:
		return "stop changefeed"
	case AdminResume:
		return "resume changefeed"
	case AdminRemove:
		return "remove changefeed"
	case AdminFinish:
		return "finish changefeed"
	}
	return "unknown"
}

// IsStopState returns whether changefeed is in stop state with give admin job
func (t AdminJobType) IsStopState() bool {
	switch t {
	case AdminStop, AdminRemove, AdminFinish:
		return true
	}
	return false
}

// TaskPosition records the process information of a capture
type TaskPosition struct {
	// The maximum event CommitTs that has been synchronized. This is updated by corresponding processor.
	CheckPointTs uint64 `json:"checkpoint-ts"` // Deprecated
	// The event that satisfies CommitTs <= ResolvedTs can be synchronized. This is updated by corresponding processor.
	ResolvedTs uint64 `json:"resolved-ts"` // Deprecated
	// The count of events were synchronized. This is updated by corresponding processor.
	Count uint64 `json:"count"`
	// Error when error happens
	Error *RunningError `json:"error"`
}

// Marshal returns the json marshal format of a TaskStatus
func (tp *TaskPosition) Marshal() (string, error) {
	data, err := json.Marshal(tp)
	return string(data), cerror.WrapError(cerror.ErrMarshalFailed, err)
}

// Unmarshal unmarshals into *TaskStatus from json marshal byte slice
func (tp *TaskPosition) Unmarshal(data []byte) error {
	err := json.Unmarshal(data, tp)
	return errors.Annotatef(
		cerror.WrapError(cerror.ErrUnmarshalFailed, err), "Unmarshal data: %v", data)
}

// String implements fmt.Stringer interface.
func (tp *TaskPosition) String() string {
	data, _ := tp.Marshal()
	return data
}

// Clone returns a deep clone of TaskPosition
func (tp *TaskPosition) Clone() *TaskPosition {
	ret := &TaskPosition{
		CheckPointTs: tp.CheckPointTs,
		ResolvedTs:   tp.ResolvedTs,
		Count:        tp.Count,
	}
	if tp.Error != nil {
		ret.Error = &RunningError{
			Addr:    tp.Error.Addr,
			Code:    tp.Error.Code,
			Message: tp.Error.Message,
		}
	}
	return ret
}

// MoveKeySpanStatus represents for the status of a MoveKeySpanJob
type MoveKeySpanStatus int

// All MoveKeySpan status
const (
	MoveKeySpanStatusNone MoveKeySpanStatus = iota
	MoveKeySpanStatusDeleted
	MoveKeySpanStatusFinished
)

// MoveKeySpanJob records a move operation of a keyspan
type MoveKeySpanJob struct {
	From               CaptureID
	To                 CaptureID
	KeySpanID          KeySpanID
	KeySpanReplicaInfo *KeySpanReplicaInfo
	Status             MoveKeySpanStatus
}

// All KeySpanOperation flags
const (
	// Move means after the delete operation, the kyespan will be re added.
	// This field is necessary since we must persist enough information to
	// restore complete keyspan operation in case of processor or owner crashes.
	OperFlagMoveKeySpan uint64 = 1 << iota
)

// All KeySpanOperation status
const (
	OperDispatched uint64 = iota
	OperProcessed
	OperFinished
)

// KeySpanOperation records the current information of a keyspan migration
type KeySpanOperation struct {
	Delete bool   `json:"delete"`
	Flag   uint64 `json:"flag,omitempty"`
	// if the operation is a delete operation, BoundaryTs is checkpoint ts
	// if the operation is a add operation, BoundaryTs is start ts
	BoundaryTs uint64 `json:"boundary_ts"`
	Status     uint64 `json:"status,omitempty"`

	RelatedKeySpans []KeySpanLocation `json:"related_key_spans"`
}

// KeySpanProcessed returns whether the keyspan has been processed by processor
func (o *KeySpanOperation) KeySpanProcessed() bool {
	return o.Status == OperProcessed || o.Status == OperFinished
}

// KeySpanApplied returns whether the keyspan has finished the startup procedure.
// Returns true if keyspan has been processed by processor and resolved ts reaches global resolved ts.
func (o *KeySpanOperation) KeySpanApplied() bool {
	return o.Status == OperFinished
}

// Clone returns a deep-clone of the struct
func (o *KeySpanOperation) Clone() *KeySpanOperation {
	if o == nil {
		return nil
	}
	clone := *o
	return &clone
}

// TaskWorkload records the workloads of a task
// the value of the struct is the workload
type TaskWorkload map[KeySpanID]WorkloadInfo

// WorkloadInfo records the workload info of a keyspan
type WorkloadInfo struct {
	Workload int64 `json:"workload"`
}

// Unmarshal unmarshals into *TaskWorkload from json marshal byte slice
func (w *TaskWorkload) Unmarshal(data []byte) error {
	err := json.Unmarshal(data, w)
	return errors.Annotatef(
		cerror.WrapError(cerror.ErrUnmarshalFailed, err), "Unmarshal data: %v", data)
}

// Marshal returns the json marshal format of a TaskWorkload
func (w *TaskWorkload) Marshal() (string, error) {
	if w == nil {
		return "{}", nil
	}
	data, err := json.Marshal(w)
	return string(data), cerror.WrapError(cerror.ErrMarshalFailed, err)
}

// KeySpanReplicaInfo records the keyspan replica info
type KeySpanReplicaInfo struct {
	StartTs Ts `json:"start-ts"`
	Start   []byte
	End     []byte
}

// Clone clones a KeySpanReplicaInfo
func (i *KeySpanReplicaInfo) Clone() *KeySpanReplicaInfo {
	if i == nil {
		return nil
	}
	clone := *i
	return &clone
}

// TaskStatus records the task information of a capture
type TaskStatus struct {
	// KeySpan information list, containing keyspans that processor should process, updated by ownrer, processor is read only.
	KeySpans     map[KeySpanID]*KeySpanReplicaInfo `json:"keyspans"`
	Operation    map[KeySpanID]*KeySpanOperation   `json:"operation"` // Deprecated
	AdminJobType AdminJobType                      `json:"admin-job-type"`
	ModRevision  int64                             `json:"-"`
}

// String implements fmt.Stringer interface.
func (ts *TaskStatus) String() string {
	data, _ := ts.Marshal()
	return data
}

// RemoveKeySpan remove the keyspan in KeySpanInfos and add a remove keyspan operation.
func (ts *TaskStatus) RemoveKeySpan(id KeySpanID, boundaryTs Ts, isMoveKeySpan bool) (*KeySpanReplicaInfo, bool) {
	if ts.KeySpans == nil {
		return nil, false
	}
	keyspan, exist := ts.KeySpans[id]
	if !exist {
		return nil, false
	}
	delete(ts.KeySpans, id)
	log.Info("remove a keyspan", zap.Uint64("keyspanId", id), zap.Uint64("boundaryTs", boundaryTs), zap.Bool("isMoveKeySpan", isMoveKeySpan))
	if ts.Operation == nil {
		ts.Operation = make(map[KeySpanID]*KeySpanOperation)
	}
	op := &KeySpanOperation{
		Delete:     true,
		BoundaryTs: boundaryTs,
	}
	if isMoveKeySpan {
		op.Flag |= OperFlagMoveKeySpan
	}
	ts.Operation[id] = op
	return keyspan, true
}

// AddKeySpan add the keyspan in KeySpanInfos and add a add kyespan operation.
func (ts *TaskStatus) AddKeySpan(id KeySpanID, keyspan *KeySpanReplicaInfo, boundaryTs Ts, relatedKeySpans []KeySpanLocation) {
	if ts.KeySpans == nil {
		ts.KeySpans = make(map[KeySpanID]*KeySpanReplicaInfo)
	}
	_, exist := ts.KeySpans[id]
	if exist {
		return
	}
	ts.KeySpans[id] = keyspan
	log.Info("add a keyspan", zap.Uint64("keyspanId", id), zap.Uint64("boundaryTs", boundaryTs))
	if ts.Operation == nil {
		ts.Operation = make(map[KeySpanID]*KeySpanOperation)
	}
	ts.Operation[id] = &KeySpanOperation{
		Delete:          false,
		BoundaryTs:      boundaryTs,
		Status:          OperDispatched,
		RelatedKeySpans: relatedKeySpans,
	}
}

// SomeOperationsUnapplied returns true if there are some operations not applied
func (ts *TaskStatus) SomeOperationsUnapplied() bool {
	for _, o := range ts.Operation {
		if !o.KeySpanApplied() {
			return true
		}
	}
	return false
}

// AppliedTs returns a Ts which less or equal to the ts boundary of any unapplied operation
func (ts *TaskStatus) AppliedTs() Ts {
	appliedTs := uint64(math.MaxUint64)
	for _, o := range ts.Operation {
		if !o.KeySpanApplied() {
			if appliedTs > o.BoundaryTs {
				appliedTs = o.BoundaryTs
			}
		}
	}
	return appliedTs
}

// Snapshot takes a snapshot of `*TaskStatus` and returns a new `*ProcInfoSnap`
func (ts *TaskStatus) Snapshot(cfID ChangeFeedID, captureID CaptureID, checkpointTs Ts) *ProcInfoSnap {
	snap := &ProcInfoSnap{
		CfID:      cfID,
		CaptureID: captureID,
		KeySpans:  make(map[KeySpanID]*KeySpanReplicaInfo, len(ts.KeySpans)),
	}
	for keyspanID, keyspan := range ts.KeySpans {
		ts := checkpointTs
		if ts < keyspan.StartTs {
			ts = keyspan.StartTs
		}
		snap.KeySpans[keyspanID] = &KeySpanReplicaInfo{
			StartTs: ts,
		}
	}
	return snap
}

// Marshal returns the json marshal format of a TaskStatus
func (ts *TaskStatus) Marshal() (string, error) {
	data, err := json.Marshal(ts)
	return string(data), cerror.WrapError(cerror.ErrMarshalFailed, err)
}

// Unmarshal unmarshals into *TaskStatus from json marshal byte slice
func (ts *TaskStatus) Unmarshal(data []byte) error {
	err := json.Unmarshal(data, ts)
	return errors.Annotatef(
		cerror.WrapError(cerror.ErrUnmarshalFailed, err), "Unmarshal data: %v", data)
}

// Clone returns a deep-clone of the struct
func (ts *TaskStatus) Clone() *TaskStatus {
	clone := *ts
	keyspans := make(map[KeySpanID]*KeySpanReplicaInfo, len(ts.KeySpans))
	for keyspanID, keyspan := range ts.KeySpans {
		keyspans[keyspanID] = keyspan.Clone()
	}
	clone.KeySpans = keyspans
	operation := make(map[KeySpanID]*KeySpanOperation, len(ts.Operation))
	for keyspanID, opt := range ts.Operation {
		operation[keyspanID] = opt.Clone()
	}
	clone.Operation = operation
	return &clone
}

// CaptureID is the type for capture ID
type CaptureID = string

// ChangeFeedID is the type for change feed ID
type ChangeFeedID = string

// KeySpanID is the ID of the KeySpan
type KeySpanID = uint64

// SchemaID is the ID of the schema
type SchemaID = int64

// Ts is the timestamp with a logical count
type Ts = uint64

// ProcessorsInfos maps from capture IDs to TaskStatus
type ProcessorsInfos map[CaptureID]*TaskStatus

// ChangeFeedDDLState is the type for change feed status
type ChangeFeedDDLState int

const (
	// ChangeFeedUnknown stands for all unknown status
	ChangeFeedUnknown ChangeFeedDDLState = iota
	// ChangeFeedSyncDML means DMLs are being processed
	ChangeFeedSyncDML
	// ChangeFeedWaitToExecDDL means we are waiting to execute a DDL
	ChangeFeedWaitToExecDDL
	// ChangeFeedExecDDL means a DDL is being executed
	ChangeFeedExecDDL
	// ChangeFeedDDLExecuteFailed means that an error occurred when executing a DDL
	ChangeFeedDDLExecuteFailed
)

// String implements fmt.Stringer interface.
func (p ProcessorsInfos) String() string {
	s := "{"
	for id, sinfo := range p {
		s += fmt.Sprintf("%s: %+v,", id, *sinfo)
	}

	s += "}"

	return s
}

// String implements fmt.Stringer interface.
func (s ChangeFeedDDLState) String() string {
	switch s {
	case ChangeFeedSyncDML:
		return "SyncDML"
	case ChangeFeedWaitToExecDDL:
		return "WaitToExecDDL"
	case ChangeFeedExecDDL:
		return "ExecDDL"
	case ChangeFeedDDLExecuteFailed:
		return "DDLExecuteFailed"
	}
	return "Unknown"
}

// ChangeFeedStatus stores information about a ChangeFeed
type ChangeFeedStatus struct {
	ResolvedTs   uint64       `json:"resolved-ts"`
	CheckpointTs uint64       `json:"checkpoint-ts"`
	AdminJobType AdminJobType `json:"admin-job-type"`
}

// Marshal returns json encoded string of ChangeFeedStatus, only contains necessary fields stored in storage
func (status *ChangeFeedStatus) Marshal() (string, error) {
	data, err := json.Marshal(status)
	return string(data), cerror.WrapError(cerror.ErrMarshalFailed, err)
}

// Unmarshal unmarshals into *ChangeFeedStatus from json marshal byte slice
func (status *ChangeFeedStatus) Unmarshal(data []byte) error {
	err := json.Unmarshal(data, status)
	return errors.Annotatef(
		cerror.WrapError(cerror.ErrUnmarshalFailed, err), "Unmarshal data: %v", data)
}

// ProcInfoSnap holds most important replication information of a processor
type ProcInfoSnap struct {
	CfID      string                            `json:"changefeed-id"`
	CaptureID string                            `json:"capture-id"`
	KeySpans  map[KeySpanID]*KeySpanReplicaInfo `json:"-"`
}

// KeySpanLocation records which capture a keyspan is in
type KeySpanLocation struct {
	CaptureID string    `json:"capture_id"`
	KeySpanID KeySpanID `json:"keyspan_id"`
}
