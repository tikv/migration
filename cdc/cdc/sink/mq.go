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

package sink

import (
	"context"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/tikv/migration/cdc/cdc/model"
	"github.com/tikv/migration/cdc/cdc/sink/codec"
	"github.com/tikv/migration/cdc/cdc/sink/producer"
	"github.com/tikv/migration/cdc/cdc/sink/producer/kafka"
	"github.com/tikv/migration/cdc/pkg/config"
	cerror "github.com/tikv/migration/cdc/pkg/errors"

	"github.com/tikv/migration/cdc/pkg/notify"
	"github.com/tikv/migration/cdc/pkg/security"
	"github.com/twmb/murmur3"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type mqEvent struct {
	entry      *model.RawKVEntry
	resolvedTs uint64
}

const (
	defaultPartitionInputChSize = 12800
)

type mqSink struct {
	mqProducer     producer.Producer
	encoderBuilder codec.EncoderBuilder
	protocol       config.Protocol

	partitionNum        int32
	partitionInput      []chan mqEvent
	partitionResolvedTs []uint64
	checkpointTs        uint64
	resolvedNotifier    *notify.Notifier
	resolvedReceiver    *notify.Receiver

	statistics *Statistics
}

func newMqSink(
	ctx context.Context, credential *security.Credential, mqProducer producer.Producer,
	replicaConfig *config.ReplicaConfig, opts map[string]string, errCh chan error,
) (*mqSink, error) {
	var protocol config.Protocol
	err := protocol.FromString(replicaConfig.Sink.Protocol)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaInvalidConfig, err)
	}
	encoderBuilder, err := codec.NewEventBatchEncoderBuilder(protocol, credential, opts)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaInvalidConfig, err)
	}
	// pre-flight verification of encoder parameters
	if _, err := encoderBuilder.Build(ctx); err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaInvalidConfig, err)
	}

	partitionNum := mqProducer.GetPartitionNum()
	partitionInput := make([]chan mqEvent, partitionNum)
	for i := 0; i < int(partitionNum); i++ {
		partitionInput[i] = make(chan mqEvent, defaultPartitionInputChSize)
	}

	notifier := new(notify.Notifier)
	resolvedReceiver, err := notifier.NewReceiver(50 * time.Millisecond)
	if err != nil {
		return nil, errors.Trace(err)
	}

	s := &mqSink{
		mqProducer:     mqProducer,
		encoderBuilder: encoderBuilder,
		protocol:       protocol,

		partitionNum:        partitionNum,
		partitionInput:      partitionInput,
		partitionResolvedTs: make([]uint64, partitionNum),
		resolvedNotifier:    notifier,
		resolvedReceiver:    resolvedReceiver,

		statistics: NewStatistics(ctx, "MQ", opts),
	}

	go func() {
		if err := s.run(ctx); err != nil && errors.Cause(err) != context.Canceled {
			select {
			case <-ctx.Done():
				return
			case errCh <- err:
			default:
				log.Error("error channel is full", zap.Error(err))
			}
		}
	}()
	return s, nil
}

func (k *mqSink) dispatch(entry *model.RawKVEntry) uint32 {
	hasher := murmur3.New32()
	hasher.Write(entry.Key)
	return hasher.Sum32() % uint32(k.partitionNum)
}

func (k *mqSink) EmitChangedEvents(ctx context.Context, rawKVEntries ...*model.RawKVEntry) error {
	entriesCount := 0

	for _, entry := range rawKVEntries {
		partition := k.dispatch(entry)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case k.partitionInput[partition] <- struct {
			entry      *model.RawKVEntry
			resolvedTs uint64
		}{entry: entry}:
		}
		entriesCount++
	}
	k.statistics.AddEntriesCount(entriesCount)
	return nil
}

func (k *mqSink) FlushChangedEvents(ctx context.Context, keyspanID model.KeySpanID, resolvedTs uint64) (uint64, error) {
	if resolvedTs <= k.checkpointTs {
		return k.checkpointTs, nil
	}

	for i := 0; i < int(k.partitionNum); i++ {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case k.partitionInput[i] <- struct {
			entry      *model.RawKVEntry
			resolvedTs uint64
		}{resolvedTs: resolvedTs}:
		}
	}

	// waiting for all events are sent to mq producer
flushLoop:
	for {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-k.resolvedReceiver.C:
			for i := 0; i < int(k.partitionNum); i++ {
				if resolvedTs > atomic.LoadUint64(&k.partitionResolvedTs[i]) {
					continue flushLoop
				}
			}
			break flushLoop
		}
	}
	err := k.mqProducer.Flush(ctx)
	if err != nil {
		return 0, errors.Trace(err)
	}
	k.checkpointTs = resolvedTs
	k.statistics.PrintStatus(ctx)
	return k.checkpointTs, nil
}

func (k *mqSink) EmitCheckpointTs(ctx context.Context, ts uint64) error {
	encoder, err := k.encoderBuilder.Build(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	msg, err := encoder.EncodeCheckpointEvent(ts)
	if err != nil {
		return errors.Trace(err)
	}
	if msg == nil {
		return nil
	}
	err = k.writeToProducer(ctx, msg, codec.EncoderNeedSyncWrite, -1)
	return errors.Trace(err)
}

func (k *mqSink) Close(ctx context.Context) error {
	err := k.mqProducer.Close()
	return errors.Trace(err)
}

func (k *mqSink) Barrier(cxt context.Context, keyspanID model.KeySpanID) error {
	// Barrier does nothing because FlushChangedEvents in mq sink has flushed
	// all buffered events by force.
	return nil
}

func (k *mqSink) run(ctx context.Context) error {
	defer k.resolvedReceiver.Stop()
	wg, ctx := errgroup.WithContext(ctx)
	for i := int32(0); i < k.partitionNum; i++ {
		partition := i
		wg.Go(func() error {
			return k.runWorker(ctx, partition)
		})
	}
	return wg.Wait()
}

const batchSizeLimit = 4 * 1024 * 1024 // 4MB

func (k *mqSink) runWorker(ctx context.Context, partition int32) error {
	log.Info("mqSink worker start", zap.Int32("partition", partition))

	input := k.partitionInput[partition]
	encoder, err := k.encoderBuilder.Build(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()

	flushToProducer := func(op codec.EncoderResult) error {
		return k.statistics.RecordBatchExecution(func() (int, error) {
			messages := encoder.Build()
			thisBatchSize := 0
			if len(messages) == 0 {
				return 0, nil
			}

			failpoint.Inject("SinkFlushEventPanic", func() {
				time.Sleep(time.Second)
				log.Fatal("kafka sink injected error")
			})

			for _, msg := range messages {
				thisBatchSize += msg.GetEntriesCount()
				err := k.writeToProducer(ctx, msg, codec.EncoderNeedAsyncWrite, partition)
				if err != nil {
					return 0, err
				}
			}

			if op == codec.EncoderNeedSyncWrite {
				err := k.mqProducer.Flush(ctx)
				if err != nil {
					return 0, err
				}
			}
			log.Debug("MQSink flushed", zap.Int("thisBatchSize", thisBatchSize))
			return thisBatchSize, nil
		})
	}
	for {
		var e mqEvent
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
			if err := flushToProducer(codec.EncoderNeedAsyncWrite); err != nil {
				return errors.Trace(err)
			}
			continue
		case e = <-input:
		}
		if e.entry == nil {
			if e.resolvedTs != 0 {
				op, err := encoder.AppendResolvedEvent(e.resolvedTs)
				if err != nil {
					return errors.Trace(err)
				}

				if err := flushToProducer(op); err != nil {
					return errors.Trace(err)
				}

				atomic.StoreUint64(&k.partitionResolvedTs[partition], e.resolvedTs)
				k.resolvedNotifier.Notify()
			}
			continue
		}
		op, err := encoder.AppendChangedEvent(e.entry)
		if err != nil {
			return errors.Trace(err)
		}

		if encoder.Size() >= batchSizeLimit {
			op = codec.EncoderNeedAsyncWrite
		}

		if encoder.Size() >= batchSizeLimit || op != codec.EncoderNoOperation {
			if err := flushToProducer(op); err != nil {
				return errors.Trace(err)
			}
		}
	}
}

func (k *mqSink) writeToProducer(ctx context.Context, message *codec.MQMessage, op codec.EncoderResult, partition int32) error {
	switch op {
	case codec.EncoderNeedAsyncWrite:
		if partition >= 0 {
			return k.mqProducer.AsyncSendMessage(ctx, message, partition)
		}
		return cerror.ErrAsyncBroadcastNotSupport.GenWithStackByArgs()
	case codec.EncoderNeedSyncWrite:
		if partition >= 0 {
			err := k.mqProducer.AsyncSendMessage(ctx, message, partition)
			if err != nil {
				return err
			}
			return k.mqProducer.Flush(ctx)
		}
		return k.mqProducer.SyncBroadcastMessage(ctx, message)
	}

	log.Warn("writeToProducer called with no-op",
		zap.ByteString("key", message.Key),
		zap.ByteString("value", message.Value),
		zap.Int32("partition", partition))
	return nil
}

func newKafkaSaramaSink(ctx context.Context, sinkURI *url.URL, replicaConfig *config.ReplicaConfig, opts map[string]string, errCh chan error) (*mqSink, error) {
	producerConfig, topic, err := parseKafkaSinkConfig(sinkURI, replicaConfig, opts)
	if err != nil {
		return nil, err
	}

	sProducer, err := kafka.NewKafkaSaramaProducer(ctx, topic, producerConfig, opts, errCh)
	if err != nil {
		return nil, errors.Trace(err)
	}
	sink, err := newMqSink(ctx, producerConfig.Credential, sProducer, replicaConfig, opts, errCh)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return sink, nil
}

func parseKafkaSinkConfig(sinkURI *url.URL, replicaConfig *config.ReplicaConfig, opts map[string]string) (producerConfig *kafka.Config, topic string, err error) {
	producerConfig = kafka.NewConfig()
	if err := kafka.CompleteConfigsAndOpts(sinkURI, producerConfig, replicaConfig, opts); err != nil {
		return nil, "", cerror.WrapError(cerror.ErrKafkaInvalidConfig, err)
	}
	// NOTICE: Please check after the completion, as we may get the configuration from the sinkURI.
	err = replicaConfig.Validate()
	if err != nil {
		return nil, "", err
	}

	topic = strings.TrimFunc(sinkURI.Path, func(r rune) bool {
		return r == '/'
	})
	if topic == "" {
		return nil, "", cerror.ErrKafkaInvalidConfig.GenWithStack("no topic is specified in sink-uri")
	}

	return producerConfig, topic, nil
}
