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

import "github.com/tikv/migration/cdc/cdc/model"

// KeySpanNumberScheduler provides a feature that scheduling by the keyspan number
type KeySpanNumberScheduler struct {
	workloads workloads
}

// newKeySpanNumberScheduler creates a new keyspan number scheduler
func newKeySpanNumberScheduler() *KeySpanNumberScheduler {
	return &KeySpanNumberScheduler{
		workloads: make(workloads),
	}
}

// ResetWorkloads implements the Scheduler interface
func (t *KeySpanNumberScheduler) ResetWorkloads(captureID model.CaptureID, workloads model.TaskWorkload) {
	t.workloads.SetCapture(captureID, workloads)
}

// AlignCapture implements the Scheduler interface
func (t *KeySpanNumberScheduler) AlignCapture(captureIDs map[model.CaptureID]struct{}) {
	t.workloads.AlignCapture(captureIDs)
}

// Skewness implements the Scheduler interface
func (t *KeySpanNumberScheduler) Skewness() float64 {
	return t.workloads.Skewness()
}

// CalRebalanceOperates implements the Scheduler interface
func (t *KeySpanNumberScheduler) CalRebalanceOperates(targetSkewness float64) (
	skewness float64, moveKeySpanJobs map[model.KeySpanID]*model.MoveKeySpanJob) {
	var totalKeySpanNumber uint64
	for _, captureWorkloads := range t.workloads {
		totalKeySpanNumber += uint64(len(captureWorkloads))
	}
	limitKeySpanNumber := (float64(totalKeySpanNumber) / float64(len(t.workloads))) + 1
	appendKeySpans := make(map[model.KeySpanID]model.Ts)
	moveKeySpanJobs = make(map[model.KeySpanID]*model.MoveKeySpanJob)

	for captureID, captureWorkloads := range t.workloads {
		for float64(len(captureWorkloads)) >= limitKeySpanNumber {
			for keyspanID := range captureWorkloads {
				// find a keyspan in this capture
				appendKeySpans[keyspanID] = 0
				moveKeySpanJobs[keyspanID] = &model.MoveKeySpanJob{
					From:      captureID,
					KeySpanID: keyspanID,
				}
				t.workloads.RemoveKeySpan(captureID, keyspanID)
				break
			}
		}
	}
	addOperations := t.DistributeKeySpans(appendKeySpans)
	for captureID, keyspanOperations := range addOperations {
		for keyspanID := range keyspanOperations {
			job := moveKeySpanJobs[keyspanID]
			job.To = captureID
			if job.From == job.To {
				delete(moveKeySpanJobs, keyspanID)
			}
		}
	}
	skewness = t.Skewness()
	return
}

// DistributeKeySpans implements the Scheduler interface
func (t *KeySpanNumberScheduler) DistributeKeySpans(keyspanIDs map[model.KeySpanID]model.Ts) map[model.CaptureID]map[model.KeySpanID]*model.KeySpanOperation {
	result := make(map[model.CaptureID]map[model.KeySpanID]*model.KeySpanOperation, len(t.workloads))
	for keyspanID, boundaryTs := range keyspanIDs {
		captureID := t.workloads.SelectIdleCapture()
		operations := result[captureID]
		if operations == nil {
			operations = make(map[model.KeySpanID]*model.KeySpanOperation)
			result[captureID] = operations
		}
		operations[keyspanID] = &model.KeySpanOperation{
			BoundaryTs: boundaryTs,
		}
		t.workloads.SetKeySpan(captureID, keyspanID, model.WorkloadInfo{Workload: 1})
	}
	return result
}
