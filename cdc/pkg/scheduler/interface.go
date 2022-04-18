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

package scheduler

import (
	"github.com/pingcap/log"
	"github.com/tikv/migration/cdc/cdc/model"
)

// Scheduler is an abstraction for anything that provide the schedule keyspan feature
type Scheduler interface {
	// ResetWorkloads resets the workloads info of the capture
	ResetWorkloads(captureID model.CaptureID, workloads model.TaskWorkload)
	// AlignCapture makes sure that the workloads of the capture is matched with the specified captureIDs
	AlignCapture(captureIDs map[model.CaptureID]struct{})
	// Skewness returns the skewness
	Skewness() float64
	// CalRebalanceOperates calculates the rebalance operates
	// returns  * the skewness after rebalance
	//          * the move jobs need by rebalance
	CalRebalanceOperates(targetSkewness float64) (
		skewness float64, moveKeySpanJobs map[model.KeySpanID]*model.MoveKeySpanJob)
	// DistributeKeySpans distributes the new keyspans to the captures
	// returns the operations of the new keyspans
	DistributeKeySpans(keyspanIDs map[model.KeySpanID]model.Ts) map[model.CaptureID]map[model.KeySpanID]*model.KeySpanOperation
}

// NewScheduler creates a new Scheduler
func NewScheduler(tp string) Scheduler {
	switch tp {
	case "keyspan-number":
		return newKeySpanNumberScheduler()
	default:
		log.Info("invalid scheduler type, using default scheduler")
		return newKeySpanNumberScheduler()
	}
}
