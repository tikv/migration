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
	"math"

	"github.com/tikv/migration/cdc/cdc/model"
	"github.com/tikv/migration/cdc/cdc/scheduler/util"
	"go.uber.org/zap"
)

// balancer is used to support the rebalance process, in which
// some victims are chosen and de-scheduled. Later, the victims
// will be automatically rescheduled, during which the target captures
// will be chosen so that the workload is the most balanced.
//
// The FindTarget method is also used when we need to schedule any keyspan,
// not only when we need to rebalance.
// TODO: Modify
type balancer interface {
	// FindVictims returns a set of possible victim keyspans.
	// Removing these keyspans will make the workload more balanced.
	FindVictims(
		keyspans *util.KeySpanSet,
		captures map[model.CaptureID]*model.CaptureInfo,
	) (keyspansToRemove []*util.KeySpanRecord)

	// FindTarget returns a target capture to add a keyspan to.
	FindTarget(
		keyspans *util.KeySpanSet,
		captures map[model.CaptureID]*model.CaptureInfo,
	) (minLoadCapture model.CaptureID, ok bool)
}

// keyspanNumberBalancer implements a balance strategy based on the
// current number of keyspans replicated by each capture.
// TODO: Implement finer-grained balance strategy based on the actual
// workload of each keyspan.
type keyspanNumberBalancer struct {
	logger *zap.Logger
}

func newKeySpanNumberRebalancer(logger *zap.Logger) balancer {
	return &keyspanNumberBalancer{
		logger: logger,
	}
}

// FindTarget returns the capture with the smallest workload (in keyspan count).
func (r *keyspanNumberBalancer) FindTarget(
	keyspans *util.KeySpanSet,
	captures map[model.CaptureID]*model.CaptureInfo,
) (minLoadCapture model.CaptureID, ok bool) {
	if len(captures) == 0 {
		return "", false
	}

	captureWorkload := make(map[model.CaptureID]int)
	for captureID := range captures {
		captureWorkload[captureID] = 0
	}

	for captureID, keyspans := range keyspans.GetAllKeySpansGroupedByCaptures() {
		// We use the number of keyspans as workload
		captureWorkload[captureID] = len(keyspans)
	}

	candidate := ""
	minWorkload := math.MaxInt64

	for captureID, workload := range captureWorkload {
		if workload < minWorkload {
			minWorkload = workload
			candidate = captureID
		}
	}

	if minWorkload == math.MaxInt64 {
		r.logger.Panic("unexpected minWorkerload == math.MaxInt64")
	}

	return candidate, true
}

// FindVictims returns some victims to remove.
// Read the comment in the function body on the details of the victim selection.
func (r *keyspanNumberBalancer) FindVictims(
	keyspans *util.KeySpanSet,
	captures map[model.CaptureID]*model.CaptureInfo,
) []*util.KeySpanRecord {
	// Algorithm overview: We try to remove some keyspans as the victims so that
	// no captures are assigned more keyspans than the average workload measured in keyspan number,
	// modulo the necessary margin due to the fraction part of the average.
	//
	// In formula, we try to maintain the invariant:
	//
	// num(keyspans assigned to any capture) < num(keyspans) / num(captures) + 1

	totalKeySpanNum := len(keyspans.GetAllKeySpans())
	captureNum := len(captures)

	if captureNum == 0 {
		return nil
	}

	upperLimitPerCapture := int(math.Ceil(float64(totalKeySpanNum) / float64(captureNum)))

	r.logger.Info("Start rebalancing",
		zap.Int("keyspan-num", totalKeySpanNum),
		zap.Int("capture-num", captureNum),
		zap.Int("target-limit", upperLimitPerCapture))

	var victims []*util.KeySpanRecord
	for _, keyspans := range keyspans.GetAllKeySpansGroupedByCaptures() {
		var keyspanList []model.KeySpanID
		for keyspanID := range keyspans {
			keyspanList = append(keyspanList, keyspanID)
		}
		// We sort the keyspanIDs here so that the result is deterministic,
		// which would aid testing and debugging.
		util.SortKeySpanIDs(keyspanList)

		keyspanNum2Remove := len(keyspans) - upperLimitPerCapture
		if keyspanNum2Remove <= 0 {
			continue
		}

		// here we pick `keyspanNum2Remove` keyspans to delete,
		for _, keyspanID := range keyspanList {
			if keyspanNum2Remove <= 0 {
				break
			}

			record := keyspans[keyspanID]
			if record == nil {
				panic("unreachable")
			}

			r.logger.Info("Rebalance: find victim keyspan",
				zap.Any("keyspan-record", record))
			victims = append(victims, record)
			keyspanNum2Remove--
		}
	}
	return victims
}
