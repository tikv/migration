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

package codec

import (
	"context"
	"encoding/binary"
	"time"

	"github.com/pingcap/log"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/migration/cdc/cdc/model"
	"github.com/tikv/migration/cdc/pkg/config"
	"github.com/tikv/migration/cdc/pkg/security"
	"go.uber.org/zap"
)

// EventBatchEncoder is an abstraction for events encoder
type EventBatchEncoder interface {
	// EncodeCheckpointEvent appends a checkpoint event into the batch.
	// This event will be broadcast to all partitions to signal a global checkpoint.
	EncodeCheckpointEvent(ts uint64) (*MQMessage, error)
	// AppendChangedEvent appends a changed event into the batch
	AppendChangedEvent(e *model.RawKVEntry) (EncoderResult, error)
	// AppendResolvedEvent appends a resolved event into the batch.
	// This event is used to tell the encoder that no event prior to ts will be sent.
	AppendResolvedEvent(ts uint64) (EncoderResult, error)
	// Build builds the batch and returns the bytes of key and value.
	Build() []*MQMessage
	// MixedBuild builds the batch and returns the bytes of mixed keys and values.
	// This is used for cdc log, to merge key and value into one byte slice
	// when first create file, we should set withVersion to true, to tell us that
	// the first 8 byte represents the encoder version
	// TODO decouple it out
	MixedBuild(withVersion bool) []byte
	// Size returns the size of the batch(bytes)
	// Deprecated: Size is deprecated
	Size() int
	// Reset reset the kv buffer
	Reset()
	// SetParams provides the encoder with more info on the sink
	SetParams(params map[string]string) error
}

// MQMessage represents an MQ message to the mqSink
type MQMessage struct {
	Key          []byte
	Value        []byte
	Ts           uint64              // reserved for possible output sorting
	Type         model.MqMessageType // type
	Protocol     config.Protocol     // protocol
	entriesCount int                 // entries in one MQ Message
}

// maximumRecordOverhead is used to calculate ProducerMessage's byteSize by sarama kafka client.
// reference: https://github.com/Shopify/sarama/blob/66521126c71c522c15a36663ae9cddc2b024c799/async_producer.go#L233
// for TiKV-CDC, minimum supported kafka version is `0.11.0.2`, which will be treated as `version = 2` by sarama producer.
const maximumRecordOverhead = 5*binary.MaxVarintLen32 + binary.MaxVarintLen64 + 1

// Length returns the expected size of the Kafka message
// We didn't append any `Headers` when send the message, so ignore the calculations related to it.
// If `ProducerMessage` Headers fields used, this method should also adjust.
func (m *MQMessage) Length() int {
	return len(m.Key) + len(m.Value) + maximumRecordOverhead
}

// PhysicalTime returns physical time part of Ts in time.Time
func (m *MQMessage) PhysicalTime() time.Time {
	return oracle.GetTimeFromTS(m.Ts)
}

// GetEntriesCount returns the number of entries batched in one MQMessage
func (m *MQMessage) GetEntriesCount() int {
	return m.entriesCount
}

// SetEntriesCount set the number of entries
func (m *MQMessage) SetEntriesCount(cnt int) {
	m.entriesCount = cnt
}

// IncEntriesCount increase the number of entries
func (m *MQMessage) IncEntriesCount() {
	m.entriesCount++
}

func newResolvedMQMessage(proto config.Protocol, key, value []byte, ts uint64) *MQMessage {
	return NewMQMessage(proto, key, value, ts, model.MqMessageTypeResolved)
}

// NewMQMessage should be used when creating a MQMessage struct.
// It copies the input byte slices to avoid any surprises in asynchronous MQ writes.
func NewMQMessage(proto config.Protocol, key []byte, value []byte, ts uint64, ty model.MqMessageType) *MQMessage {
	ret := &MQMessage{
		Key:          nil,
		Value:        nil,
		Ts:           ts,
		Type:         ty,
		Protocol:     proto,
		entriesCount: 0,
	}

	if key != nil {
		ret.Key = make([]byte, len(key))
		copy(ret.Key, key)
	}

	if value != nil {
		ret.Value = make([]byte, len(value))
		copy(ret.Value, value)
	}

	return ret
}

// EventBatchDecoder is an abstraction for events decoder
// this interface is only for testing now
type EventBatchDecoder interface {
	// HasNext returns
	//     1. the type of the next event
	//     2. a bool if the next event is exist
	//     3. error
	HasNext() (model.MqMessageType, bool, error)
	// NextResolvedEvent returns the next resolved event if exists
	NextResolvedEvent() (uint64, error)
	// NextChangedEvent returns the next row changed event if exists
	NextChangedEvent() (*model.RawKVEntry, error)
}

// EncoderResult indicates an action request by the encoder to the mqSink
type EncoderResult uint8

// Enum types of EncoderResult
const (
	EncoderNoOperation EncoderResult = iota
	EncoderNeedAsyncWrite
	EncoderNeedSyncWrite
)

type EncoderBuilder interface {
	Build(ctx context.Context) (EventBatchEncoder, error)
}

// NewEventBatchEncoderBuilder returns an EncoderBuilder
func NewEventBatchEncoderBuilder(p config.Protocol, credential *security.Credential, opts map[string]string) (EncoderBuilder, error) {
	switch p {
	case config.ProtocolDefault, config.ProtocolOpen:
		return newJSONEventBatchEncoderBuilder(opts), nil
	default:
		log.Warn("unknown codec protocol value of EventBatchEncoder, use open-protocol as the default", zap.Int("protocol_value", int(p)))
		return newJSONEventBatchEncoderBuilder(opts), nil
	}
}
