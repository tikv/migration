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
	"sync"

	"github.com/pingcap/errors"
	"github.com/tikv/migration/cdc/cdc/model"
	"github.com/tikv/migration/cdc/pkg/context"
)

// Design Notes:
//
// This file contains the definition and implementation of the move kyespan manager,
// which is responsible for implementing the logic for manual keysapn moves. The logic
// here will ultimately be triggered by the user's call to the move keyspan HTTP API.
//
// POST /api/v1/changefeeds/{changefeed_id}/keyspans/move_keyspan
//
// Abstracting out moveKeySpanManager makes it easier to both test the implementation
// and modify the behavior of this API.
//
// The moveKeySpanManager will help the ScheduleDispatcher to track which keyspans are being
// moved to which capture.

// removeKeySpanFunc is a function used to de-schedule a keyspan from its current processor.
type removeKeySpanFunc = func(
	ctx context.Context,
	keyspanID model.KeySpanID,
	target model.CaptureID) (result removeKeySpanResult, err error)

type removeKeySpanResult int

const (
	// removeKeySpanResultOK indicates that the keyspan has been de-scheduled
	removeKeySpanResultOK removeKeySpanResult = iota + 1

	// removeKeySpanResultUnavailable indicates that the keyspan
	// is temporarily not available for removal. The operation
	// can be tried again later.
	removeKeySpanResultUnavailable

	// removeKeySpanResultGiveUp indicates that the operation is
	// not successful but there is no point in trying again. Such as when
	// 1) the keyspan to be removed is not found,
	// 2) the capture to move the keyspan to is not found.
	removeKeySpanResultGiveUp
)

type moveKeySpanManager interface {
	// Add adds a keyspan to the move keyspan manager.
	// It returns false **if the keyspan is already being moved manually**.
	Add(keyspanID model.KeySpanID, target model.CaptureID) (ok bool)

	// DoRemove tries to de-schedule as many keyspans as possible by using the
	// given function fn. If the function fn returns false, it means the keyspan
	// can not be removed (de-scheduled) for now.
	DoRemove(ctx context.Context, fn removeKeySpanFunc) (ok bool, err error)

	// GetTargetByKeySpanID returns the target capture ID of the given keyspan.
	// It will only return a target if the keyspan is in the process of being manually
	// moved, and the request to de-schedule the given keyspan has already been sent.
	GetTargetByKeySpanID(keyspanID model.KeySpanID) (target model.CaptureID, ok bool)

	// MarkDone informs the moveKeySpanManager that the given keyspan has successfully
	// been moved.
	MarkDone(keyspanID model.KeySpanID)

	// OnCaptureRemoved informs the moveKeySpanManager that a capture has gone offline.
	// Then the moveKeySpanManager will clear all pending jobs to that capture.
	OnCaptureRemoved(captureID model.CaptureID)
}

type moveKeySpanJobStatus int

const (
	moveKeySpanJobStatusReceived = moveKeySpanJobStatus(iota + 1)
	moveKeySpanJobStatusRemoved
)

type moveKeySpanJob struct {
	target model.CaptureID
	status moveKeySpanJobStatus
}

type moveKeySpanManagerImpl struct {
	mu              sync.Mutex
	moveKeySpanJobs map[model.KeySpanID]*moveKeySpanJob
}

func newMoveKeySpanManager() moveKeySpanManager {
	return &moveKeySpanManagerImpl{
		moveKeySpanJobs: make(map[model.KeySpanID]*moveKeySpanJob),
	}
}

func (m *moveKeySpanManagerImpl) Add(keyspanID model.KeySpanID, target model.CaptureID) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.moveKeySpanJobs[keyspanID]; ok {
		// Returns false if the keyspan is already in a move keyspan job.
		return false
	}

	m.moveKeySpanJobs[keyspanID] = &moveKeySpanJob{
		target: target,
		status: moveKeySpanJobStatusReceived,
	}
	return true
}

func (m *moveKeySpanManagerImpl) DoRemove(ctx context.Context, fn removeKeySpanFunc) (ok bool, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// This function tries to remove as many keyspans as possible.
	// But when we cannot proceed (i.e., fn returns false), we return false,
	// so that the caller can retry later.

	for keyspanID, job := range m.moveKeySpanJobs {
		if job.status == moveKeySpanJobStatusRemoved {
			continue
		}

		result, err := fn(ctx, keyspanID, job.target)
		if err != nil {
			return false, errors.Trace(err)
		}

		switch result {
		case removeKeySpanResultOK:
			job.status = moveKeySpanJobStatusRemoved
			continue
		case removeKeySpanResultGiveUp:
			delete(m.moveKeySpanJobs, keyspanID)
			// Giving up means that we can move forward,
			// so there is no need to return false here.
			continue
		case removeKeySpanResultUnavailable:
		}

		// Returning false means that there is a keyspan that cannot be removed for now.
		// This is usually caused by temporary unavailability of underlying resources, such
		// as a congestion in the messaging client.
		//
		// So when we have returned false, the caller should try again later and refrain from
		// other scheduling operations.
		return false, nil
	}
	return true, nil
}

func (m *moveKeySpanManagerImpl) GetTargetByKeySpanID(keyspanID model.KeySpanID) (model.CaptureID, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	job, ok := m.moveKeySpanJobs[keyspanID]
	if !ok {
		return "", false
	}

	// Only after the keyspan has been removed by the moveKeySpanManager,
	// can we provide the target. Otherwise, we risk interfering with
	// other operations.
	if job.status != moveKeySpanJobStatusRemoved {
		return "", false
	}

	return job.target, true
}

func (m *moveKeySpanManagerImpl) MarkDone(keyspanID model.KeySpanID) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.moveKeySpanJobs, keyspanID)
}

func (m *moveKeySpanManagerImpl) OnCaptureRemoved(captureID model.CaptureID) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for keyspanID, job := range m.moveKeySpanJobs {
		if job.target == captureID {
			delete(m.moveKeySpanJobs, keyspanID)
		}
	}
}
