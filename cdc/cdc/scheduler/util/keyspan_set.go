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

package util

import (
	"github.com/pingcap/log"
	"github.com/tikv/migration/cdc/cdc/model"
	"go.uber.org/zap"
)

// KeySpanSet provides a data structure to store the keyspans' states for the
// scheduler.
type KeySpanSet struct {
	// all keyspans' records
	keyspanIDMap map[model.KeySpanID]*KeySpanRecord

	// a non-unique index to facilitate looking up keyspans
	// assigned to a given capture.
	captureIndex map[model.CaptureID]map[model.KeySpanID]*KeySpanRecord
}

// keyspanRecord is a record to be inserted into keyspanSet.
type KeySpanRecord struct {
	KeySpanID model.KeySpanID
	CaptureID model.CaptureID
	Status    KeySpanStatus
}

// Clone returns a copy of the KeySpanSet.
// This method is future-proof in case we add
// something not trivially copyable.
func (r *KeySpanRecord) Clone() *KeySpanRecord {
	return &KeySpanRecord{
		KeySpanID: r.KeySpanID,
		CaptureID: r.CaptureID,
		Status:    r.Status,
	}
}

// KeySpanStatus is a type representing the keyspan's replication status.
type KeySpanStatus int32

const (
	AddingKeySpan = KeySpanStatus(iota) + 1
	RemovingKeySpan
	RunningKeySpan
)

// NewKeySpanSet creates a new KeySpanSet.
func NewKeySpanSet() *KeySpanSet {
	return &KeySpanSet{
		keyspanIDMap: map[model.KeySpanID]*KeySpanRecord{},
		captureIndex: map[model.CaptureID]map[model.KeySpanID]*KeySpanRecord{},
	}
}

// AddKeySpanRecord inserts a new KeySpanRecord.
// It returns true if it succeeds. Returns false if there is a duplicate.
func (s *KeySpanSet) AddKeySpanRecord(record *KeySpanRecord) (successful bool) {
	if _, ok := s.keyspanIDMap[record.KeySpanID]; ok {
		// duplicate KeySpanID
		return false
	}
	recordCloned := record.Clone()
	s.keyspanIDMap[record.KeySpanID] = recordCloned

	captureIndexEntry := s.captureIndex[record.CaptureID]
	if captureIndexEntry == nil {
		captureIndexEntry = make(map[model.KeySpanID]*KeySpanRecord)
		s.captureIndex[record.CaptureID] = captureIndexEntry
	}

	captureIndexEntry[record.KeySpanID] = recordCloned
	return true
}

// UpdateKeySpanRecord updates an existing KeySpanRecord.
// All modifications to a keyspan's status should be done by this method.
func (s *KeySpanSet) UpdateKeySpanRecord(record *KeySpanRecord) (successful bool) {
	oldRecord, ok := s.keyspanIDMap[record.KeySpanID]
	if !ok {
		// keyspan does not exist
		return false
	}

	// If there is no need to modify the CaptureID, we simply
	// update the record.
	if record.CaptureID == oldRecord.CaptureID {
		recordCloned := record.Clone()
		s.keyspanIDMap[record.KeySpanID] = recordCloned
		s.captureIndex[record.CaptureID][record.KeySpanID] = recordCloned
		return true
	}

	// If the CaptureID is changed, we do a proper RemoveKeySpanRecord followed
	// by AddKeySpanRecord.
	if record.CaptureID != oldRecord.CaptureID {
		if ok := s.RemoveKeySpanRecord(record.KeySpanID); !ok {
			log.Panic("unreachable", zap.Any("record", record))
		}
		if ok := s.AddKeySpanRecord(record); !ok {
			log.Panic("unreachable", zap.Any("record", record))
		}
	}
	return true
}

// GetKeySpanRecord tries to obtain a record with the specified keyspanID.
func (s *KeySpanSet) GetKeySpanRecord(keyspanID model.KeySpanID) (*KeySpanRecord, bool) {
	rec, ok := s.keyspanIDMap[keyspanID]
	if ok {
		return rec.Clone(), ok
	}
	return nil, false
}

// RemoveKeySpanRecord removes the record with keyspanID. Returns false
// if none exists.
func (s *KeySpanSet) RemoveKeySpanRecord(keyspanID model.KeySpanID) bool {
	record, ok := s.keyspanIDMap[keyspanID]
	if !ok {
		return false
	}
	delete(s.keyspanIDMap, record.KeySpanID)

	captureIndexEntry, ok := s.captureIndex[record.CaptureID]
	if !ok {
		log.Panic("unreachable", zap.Uint64("keyspan-id", keyspanID))
	}
	delete(captureIndexEntry, record.KeySpanID)
	if len(captureIndexEntry) == 0 {
		delete(s.captureIndex, record.CaptureID)
	}
	return true
}

// RemoveKeySpanRecordByCaptureID removes all keyspan records associated with
// captureID.
func (s *KeySpanSet) RemoveKeySpanRecordByCaptureID(captureID model.CaptureID) []*KeySpanRecord {
	captureIndexEntry, ok := s.captureIndex[captureID]
	if !ok {
		return nil
	}

	var ret []*KeySpanRecord
	for keyspanID, record := range captureIndexEntry {
		delete(s.keyspanIDMap, keyspanID)
		// Since the record has been removed,
		// there is no need to clone it before returning.
		ret = append(ret, record)
	}
	delete(s.captureIndex, captureID)
	return ret
}

// CountKeySpanByCaptureID counts the number of keyspans associated with the captureID.
func (s *KeySpanSet) CountKeySpanByCaptureID(captureID model.CaptureID) int {
	return len(s.captureIndex[captureID])
}

// GetDistinctCaptures counts distinct captures with keyspans.
func (s *KeySpanSet) GetDistinctCaptures() []model.CaptureID {
	var ret []model.CaptureID
	for captureID := range s.captureIndex {
		ret = append(ret, captureID)
	}
	return ret
}

// GetAllKeySpans returns all stored information on all keyspans.
func (s *KeySpanSet) GetAllKeySpans() map[model.KeySpanID]*KeySpanRecord {
	ret := make(map[model.KeySpanID]*KeySpanRecord)
	for keyspanID, record := range s.keyspanIDMap {
		ret[keyspanID] = record.Clone()
	}
	return ret
}

// GetAllKeySpansGroupedByCaptures returns all stored information grouped by associated CaptureID.
func (s *KeySpanSet) GetAllKeySpansGroupedByCaptures() map[model.CaptureID]map[model.KeySpanID]*KeySpanRecord {
	ret := make(map[model.CaptureID]map[model.KeySpanID]*KeySpanRecord)
	for captureID, keyspanIDMap := range s.captureIndex {
		keyspanIDMapCloned := make(map[model.KeySpanID]*KeySpanRecord)
		for keyspanID, record := range keyspanIDMap {
			keyspanIDMapCloned[keyspanID] = record.Clone()
		}
		ret[captureID] = keyspanIDMapCloned
	}
	return ret
}

// CountKeySpanByStatus counts the number of keyspans with the given status.
func (s *KeySpanSet) CountKeySpanByStatus(status KeySpanStatus) (count int) {
	for _, record := range s.keyspanIDMap {
		if record.Status == status {
			count++
		}
	}
	return
}
