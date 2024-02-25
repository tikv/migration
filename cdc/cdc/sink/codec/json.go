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
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"strconv"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/tikv/migration/cdc/cdc/model"
	"github.com/tikv/migration/cdc/pkg/config"
	cerror "github.com/tikv/migration/cdc/pkg/errors"
	"go.uber.org/zap"
)

const (
	// BatchVersion1 represents the version of batch format
	BatchVersion1 uint64 = 1
	// DefaultMaxBatchSize sets the default value for max-batch-size
	DefaultMaxBatchSize int = 16
)

type mqMessageKey struct {
	// CRTs in key to keep all versions in MQ against compaction.
	CRTs uint64              `json:"ts"`
	Key  []byte              `json:"k,omitempty"`
	Type model.MqMessageType `json:"t"`
}

func (m *mqMessageKey) Encode() ([]byte, error) {
	data, err := json.Marshal(m)
	return data, cerror.WrapError(cerror.ErrMarshalFailed, err)
}

func (m *mqMessageKey) Decode(data []byte) error {
	return cerror.WrapError(cerror.ErrUnmarshalFailed, json.Unmarshal(data, m))
}

type mqMessageValue struct {
	OpType    model.OpType `json:"op"`
	Value     []byte       `json:"v,omitempty"`
	ExpiredTs *uint64      `json:"ex,omitempty"`
}

func (m *mqMessageValue) Encode() ([]byte, error) {
	data, err := json.Marshal(m)
	return data, cerror.WrapError(cerror.ErrMarshalFailed, err)
}

func (m *mqMessageValue) Decode(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	err := decoder.Decode(m)
	if err != nil {
		return cerror.WrapError(cerror.ErrUnmarshalFailed, err)
	}
	return nil
}

func newResolvedMessage(ts uint64) *mqMessageKey {
	return &mqMessageKey{
		CRTs: ts,
		Type: model.MqMessageTypeResolved,
	}
}

func encodeExpiredTs(e *model.RawKVEntry) *uint64 {
	expiredTs := e.ExpiredTs
	if expiredTs == 0 {
		return nil
	}
	return &expiredTs
}

func decodeExpiredTs(expiredTs *uint64) uint64 {
	if expiredTs == nil {
		return 0
	}
	return *expiredTs
}

func kvEventToMqMessage(e *model.RawKVEntry) (*mqMessageKey, *mqMessageValue) {
	key := &mqMessageKey{
		e.CRTs,
		e.Key,
		model.MqMessageTypeKv,
	}
	value := &mqMessageValue{
		e.OpType,
		e.Value,
		encodeExpiredTs(e),
	}
	return key, value
}

func mqMessageToKvEvent(key *mqMessageKey, value *mqMessageValue) *model.RawKVEntry {
	return &model.RawKVEntry{
		OpType:    value.OpType,
		Key:       key.Key,
		Value:     value.Value,
		CRTs:      key.CRTs,
		ExpiredTs: decodeExpiredTs(value.ExpiredTs),
	}
}

// JSONEventBatchEncoder encodes the events into the byte of a batch into.
type JSONEventBatchEncoder struct {
	// TODO remove deprecated fields
	keyBuf            *bytes.Buffer // Deprecated: only used for MixedBuild for now
	valueBuf          *bytes.Buffer // Deprecated: only used for MixedBuild for now
	supportMixedBuild bool          // TODO decouple this out

	messageBuf      []*MQMessage
	curBatchSize    int
	totalBatchBytes int

	// configs
	maxMessageBytes int
	maxBatchSize    int
}

// GetMaxMessageBytes is only for unit testing.
func (d *JSONEventBatchEncoder) GetMaxMessageBytes() int {
	return d.maxMessageBytes
}

// GetMaxBatchSize is only for unit testing.
func (d *JSONEventBatchEncoder) GetMaxBatchSize() int {
	return d.maxBatchSize
}

// SetMixedBuildSupport is used by CDC Log
func (d *JSONEventBatchEncoder) SetMixedBuildSupport(enabled bool) {
	d.supportMixedBuild = enabled
}

// AppendResolvedEvent is no-op
func (d *JSONEventBatchEncoder) AppendResolvedEvent(ts uint64) (EncoderResult, error) {
	return EncoderNoOperation, nil
}

// EncodeCheckpointEvent implements the EventBatchEncoder interface
func (d *JSONEventBatchEncoder) EncodeCheckpointEvent(ts uint64) (*MQMessage, error) {
	keyMsg := newResolvedMessage(ts)
	key, err := keyMsg.Encode()
	if err != nil {
		return nil, errors.Trace(err)
	}

	var keyLenByte [8]byte
	binary.BigEndian.PutUint64(keyLenByte[:], uint64(len(key)))
	var valueLenByte [8]byte
	binary.BigEndian.PutUint64(valueLenByte[:], 0)

	if d.supportMixedBuild {
		d.keyBuf.Write(keyLenByte[:])
		d.keyBuf.Write(key)
		d.valueBuf.Write(valueLenByte[:])
		return nil, nil
	}

	keyBuf := new(bytes.Buffer)
	var versionByte [8]byte
	binary.BigEndian.PutUint64(versionByte[:], BatchVersion1)
	keyBuf.Write(versionByte[:])
	keyBuf.Write(keyLenByte[:])
	keyBuf.Write(key)

	valueBuf := new(bytes.Buffer)
	valueBuf.Write(valueLenByte[:])

	ret := newResolvedMQMessage(config.ProtocolOpen, keyBuf.Bytes(), valueBuf.Bytes(), ts)
	return ret, nil
}

// AppendRowChangedEvent implements the EventBatchEncoder interface
func (d *JSONEventBatchEncoder) AppendChangedEvent(e *model.RawKVEntry) (EncoderResult, error) {
	keyMsg, valueMsg := kvEventToMqMessage(e)
	key, err := keyMsg.Encode()
	if err != nil {
		return EncoderNoOperation, errors.Trace(err)
	}
	value, err := valueMsg.Encode()
	if err != nil {
		return EncoderNoOperation, errors.Trace(err)
	}

	var keyLenByte [8]byte
	binary.BigEndian.PutUint64(keyLenByte[:], uint64(len(key)))
	var valueLenByte [8]byte
	binary.BigEndian.PutUint64(valueLenByte[:], uint64(len(value)))

	if d.supportMixedBuild {
		d.keyBuf.Write(keyLenByte[:])
		d.keyBuf.Write(key)

		d.valueBuf.Write(valueLenByte[:])
		d.valueBuf.Write(value)
	} else {
		// for single message that longer than max-message-size, do not send it.
		// 16 is the length of `keyLenByte` and `valueLenByte`, 8 is the length of `versionHead`
		length := len(key) + len(value) + maximumRecordOverhead + 16 + 8
		if length > d.maxMessageBytes {
			log.Warn("Single message too large",
				zap.Int("max-message-size", d.maxMessageBytes), zap.Int("length", length))
			return EncoderNoOperation, cerror.ErrJSONCodecKvTooLarge.GenWithStackByArgs()
		}

		if len(d.messageBuf) == 0 ||
			d.curBatchSize >= d.maxBatchSize ||
			d.messageBuf[len(d.messageBuf)-1].Length()+len(key)+len(value)+16 > d.maxMessageBytes {

			versionHead := make([]byte, 8)
			binary.BigEndian.PutUint64(versionHead, BatchVersion1)

			if len(d.messageBuf) > 0 {
				d.totalBatchBytes += d.messageBuf[len(d.messageBuf)-1].Length()
			}
			d.messageBuf = append(d.messageBuf, NewMQMessage(config.ProtocolOpen, versionHead, nil, 0, model.MqMessageTypeKv))
			d.curBatchSize = 0
		}

		message := d.messageBuf[len(d.messageBuf)-1]
		message.Key = append(message.Key, keyLenByte[:]...)
		message.Key = append(message.Key, key...)
		message.Value = append(message.Value, valueLenByte[:]...)
		message.Value = append(message.Value, value...)
		message.Ts = e.CRTs
		message.IncEntriesCount()

		if message.Length() > d.maxMessageBytes {
			// `len(d.messageBuf) == 1` is implied
			log.Debug("Event does not fit into max-message-bytes. Adjust relevant configurations to avoid service interruptions.",
				zap.Int("event-len", message.Length()), zap.Int("max-message-bytes", d.maxMessageBytes))
		}
		d.curBatchSize++
	}
	return EncoderNoOperation, nil
}

// Build implements the EventBatchEncoder interface
func (d *JSONEventBatchEncoder) Build() (mqMessages []*MQMessage) {
	if d.supportMixedBuild {
		if d.valueBuf.Len() == 0 {
			return nil
		}
		/* there could be multiple types of event encoded within a single message which means the type is not sure */
		ret := NewMQMessage(config.ProtocolOpen, d.keyBuf.Bytes(), d.valueBuf.Bytes(), 0, model.MqMessageTypeUnknown)
		return []*MQMessage{ret}
	}

	ret := d.messageBuf
	d.messageBuf = make([]*MQMessage, 0)
	return ret
}

// MixedBuild implements the EventBatchEncoder interface
func (d *JSONEventBatchEncoder) MixedBuild(withVersion bool) []byte {
	if !d.supportMixedBuild {
		log.Panic("mixedBuildSupport not enabled!")
		return nil
	}
	keyBytes := d.keyBuf.Bytes()
	valueBytes := d.valueBuf.Bytes()
	mixedBytes := make([]byte, len(keyBytes)+len(valueBytes))

	index := uint64(0)
	keyIndex := uint64(0)
	valueIndex := uint64(0)

	if withVersion {
		// the first 8 bytes is the version, we should copy directly
		// then skip 8 bytes for next round key value parse
		copy(mixedBytes[:8], keyBytes[:8])
		index = uint64(8)    // skip version
		keyIndex = uint64(8) // skip version
	}

	for {
		if keyIndex >= uint64(len(keyBytes)) {
			break
		}
		keyLen := binary.BigEndian.Uint64(keyBytes[keyIndex : keyIndex+8])
		offset := keyLen + 8
		copy(mixedBytes[index:index+offset], keyBytes[keyIndex:keyIndex+offset])
		keyIndex += offset
		index += offset

		valueLen := binary.BigEndian.Uint64(valueBytes[valueIndex : valueIndex+8])
		offset = valueLen + 8
		copy(mixedBytes[index:index+offset], valueBytes[valueIndex:valueIndex+offset])
		valueIndex += offset
		index += offset
	}
	return mixedBytes
}

// Size implements the EventBatchEncoder interface
func (d *JSONEventBatchEncoder) Size() int {
	if d.supportMixedBuild {
		return d.keyBuf.Len() + d.valueBuf.Len()
	}
	lastMessageLength := 0
	if len(d.messageBuf) > 0 {
		lastMessageLength = d.messageBuf[len(d.messageBuf)-1].Length()
	}
	return d.totalBatchBytes + lastMessageLength
}

// Reset implements the EventBatchEncoder interface
func (d *JSONEventBatchEncoder) Reset() {
	d.keyBuf.Reset()
	d.valueBuf.Reset()

	d.messageBuf = d.messageBuf[:0]
	d.curBatchSize = 0
	d.totalBatchBytes = 0
}

// SetParams reads relevant parameters for Open Protocol
func (d *JSONEventBatchEncoder) SetParams(params map[string]string) error {
	var err error

	maxMessageBytes, ok := params["max-message-bytes"]
	if !ok {
		return cerror.ErrSinkInvalidConfig.Wrap(errors.New("max-message-bytes not found"))
	}

	d.maxMessageBytes, err = strconv.Atoi(maxMessageBytes)
	if err != nil {
		return cerror.ErrSinkInvalidConfig.Wrap(err)
	}
	if d.maxMessageBytes <= 0 {
		return cerror.ErrSinkInvalidConfig.Wrap(errors.Errorf("invalid max-message-bytes %d", d.maxMessageBytes))
	}

	d.maxBatchSize = DefaultMaxBatchSize
	if maxBatchSize, ok := params["max-batch-size"]; ok {
		d.maxBatchSize, err = strconv.Atoi(maxBatchSize)
		if err != nil {
			return cerror.ErrSinkInvalidConfig.Wrap(err)
		}
	}
	if d.maxBatchSize <= 0 {
		return cerror.ErrSinkInvalidConfig.Wrap(errors.Errorf("invalid max-batch-size %d", d.maxBatchSize))
	}

	return nil
}

type jsonEventBatchEncoderBuilder struct {
	opts map[string]string
}

// Build a JSONEventBatchEncoder
func (b *jsonEventBatchEncoderBuilder) Build(ctx context.Context) (EventBatchEncoder, error) {
	encoder := NewJSONEventBatchEncoder()
	if err := encoder.SetParams(b.opts); err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaInvalidConfig, err)
	}

	return encoder, nil
}

func newJSONEventBatchEncoderBuilder(opts map[string]string) EncoderBuilder {
	return &jsonEventBatchEncoderBuilder{opts: opts}
}

// NewJSONEventBatchEncoder creates a new JSONEventBatchEncoder.
func NewJSONEventBatchEncoder() EventBatchEncoder {
	batch := &JSONEventBatchEncoder{
		keyBuf:   &bytes.Buffer{},
		valueBuf: &bytes.Buffer{},
	}
	var versionByte [8]byte
	binary.BigEndian.PutUint64(versionByte[:], BatchVersion1)
	batch.keyBuf.Write(versionByte[:])
	return batch
}

// JSONEventBatchMixedDecoder decodes the byte of a batch into the original messages.
type JSONEventBatchMixedDecoder struct {
	mixedBytes []byte
	nextKey    *mqMessageKey
	nextKeyLen uint64
}

// HasNext implements the EventBatchDecoder interface
func (b *JSONEventBatchMixedDecoder) HasNext() (model.MqMessageType, bool, error) {
	if !b.hasNext() {
		return 0, false, nil
	}
	if err := b.decodeNextKey(); err != nil {
		return 0, false, err
	}
	return b.nextKey.Type, true, nil
}

// NextResolvedEvent implements the EventBatchDecoder interface
func (b *JSONEventBatchMixedDecoder) NextResolvedEvent() (uint64, error) {
	if b.nextKey == nil {
		if err := b.decodeNextKey(); err != nil {
			return 0, err
		}
	}
	b.mixedBytes = b.mixedBytes[b.nextKeyLen+8:]
	if b.nextKey.Type != model.MqMessageTypeResolved {
		return 0, cerror.ErrJSONCodecInvalidData.GenWithStack("not found resolved event message")
	}
	valueLen := binary.BigEndian.Uint64(b.mixedBytes[:8])
	b.mixedBytes = b.mixedBytes[valueLen+8:]
	resolvedTs := b.nextKey.CRTs
	b.nextKey = nil
	return resolvedTs, nil
}

// NextChangedEvent implements the EventBatchDecoder interface
func (b *JSONEventBatchMixedDecoder) NextChangedEvent() (*model.RawKVEntry, error) {
	if b.nextKey == nil {
		if err := b.decodeNextKey(); err != nil {
			return nil, err
		}
	}
	b.mixedBytes = b.mixedBytes[b.nextKeyLen+8:]
	if b.nextKey.Type != model.MqMessageTypeKv {
		return nil, cerror.ErrJSONCodecInvalidData.GenWithStack("not found kv event message")
	}
	valueLen := binary.BigEndian.Uint64(b.mixedBytes[:8])
	value := b.mixedBytes[8 : valueLen+8]
	b.mixedBytes = b.mixedBytes[valueLen+8:]
	kvMsg := new(mqMessageValue)
	if err := kvMsg.Decode(value); err != nil {
		return nil, errors.Trace(err)
	}
	kvEvent := mqMessageToKvEvent(b.nextKey, kvMsg)
	b.nextKey = nil
	return kvEvent, nil
}

func (b *JSONEventBatchMixedDecoder) hasNext() bool {
	return len(b.mixedBytes) > 0
}

func (b *JSONEventBatchMixedDecoder) decodeNextKey() error {
	keyLen := binary.BigEndian.Uint64(b.mixedBytes[:8])
	key := b.mixedBytes[8 : keyLen+8]
	// drop value bytes
	msgKey := new(mqMessageKey)
	err := msgKey.Decode(key)
	if err != nil {
		return errors.Trace(err)
	}
	b.nextKey = msgKey
	b.nextKeyLen = keyLen
	return nil
}

// JSONEventBatchDecoder decodes the byte of a batch into the original messages.
type JSONEventBatchDecoder struct {
	keyBytes   []byte
	valueBytes []byte
	nextKey    *mqMessageKey
	nextKeyLen uint64
}

// HasNext implements the EventBatchDecoder interface
func (b *JSONEventBatchDecoder) HasNext() (model.MqMessageType, bool, error) {
	if !b.hasNext() {
		return 0, false, nil
	}
	if err := b.decodeNextKey(); err != nil {
		return 0, false, err
	}
	return b.nextKey.Type, true, nil
}

// NextResolvedEvent implements the EventBatchDecoder interface
func (b *JSONEventBatchDecoder) NextResolvedEvent() (uint64, error) {
	if b.nextKey == nil {
		if err := b.decodeNextKey(); err != nil {
			return 0, err
		}
	}
	b.keyBytes = b.keyBytes[b.nextKeyLen+8:]
	if b.nextKey.Type != model.MqMessageTypeResolved {
		return 0, cerror.ErrJSONCodecInvalidData.GenWithStack("not found resolved event message")
	}
	valueLen := binary.BigEndian.Uint64(b.valueBytes[:8])
	b.valueBytes = b.valueBytes[valueLen+8:]
	resolvedTs := b.nextKey.CRTs
	b.nextKey = nil
	return resolvedTs, nil
}

// NextChangedEvent implements the EventBatchDecoder interface
func (b *JSONEventBatchDecoder) NextChangedEvent() (*model.RawKVEntry, error) {
	if b.nextKey == nil {
		if err := b.decodeNextKey(); err != nil {
			return nil, err
		}
	}
	b.keyBytes = b.keyBytes[b.nextKeyLen+8:]
	if b.nextKey.Type != model.MqMessageTypeKv {
		return nil, cerror.ErrJSONCodecInvalidData.GenWithStack("not found row event message")
	}
	valueLen := binary.BigEndian.Uint64(b.valueBytes[:8])
	value := b.valueBytes[8 : valueLen+8]
	b.valueBytes = b.valueBytes[valueLen+8:]
	kvMsg := new(mqMessageValue)
	if err := kvMsg.Decode(value); err != nil {
		return nil, errors.Trace(err)
	}
	kvEvent := mqMessageToKvEvent(b.nextKey, kvMsg)
	b.nextKey = nil
	return kvEvent, nil
}

func (b *JSONEventBatchDecoder) hasNext() bool {
	return len(b.keyBytes) > 0 && len(b.valueBytes) > 0
}

func (b *JSONEventBatchDecoder) decodeNextKey() error {
	keyLen := binary.BigEndian.Uint64(b.keyBytes[:8])
	key := b.keyBytes[8 : keyLen+8]
	msgKey := new(mqMessageKey)
	err := msgKey.Decode(key)
	if err != nil {
		return errors.Trace(err)
	}
	b.nextKey = msgKey
	b.nextKeyLen = keyLen
	return nil
}

// NewJSONEventBatchDecoder creates a new JSONEventBatchDecoder.
func NewJSONEventBatchDecoder(key []byte, value []byte) (EventBatchDecoder, error) {
	version := binary.BigEndian.Uint64(key[:8])
	key = key[8:]
	if version != BatchVersion1 {
		return nil, cerror.ErrJSONCodecInvalidData.GenWithStack("unexpected key format version")
	}
	// if only decode one byte slice, we choose MixedDecoder
	if len(key) > 0 && len(value) == 0 {
		return &JSONEventBatchMixedDecoder{
			mixedBytes: key,
		}, nil
	}
	return &JSONEventBatchDecoder{
		keyBytes:   key,
		valueBytes: value,
	}, nil
}
