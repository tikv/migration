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

package main

import (
	"context"
	"flag"
	"fmt"
	"math"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/tikv/migration/cdc/cdc/model"
	"github.com/tikv/migration/cdc/cdc/sink"
	"github.com/tikv/migration/cdc/cdc/sink/codec"
	"github.com/tikv/migration/cdc/pkg/config"

	"github.com/tikv/migration/cdc/pkg/logutil"
	"github.com/tikv/migration/cdc/pkg/security"
	"github.com/tikv/migration/cdc/pkg/util"
	"go.uber.org/zap"
)

// Sarama configuration options
var (
	kafkaAddrs           []string
	kafkaTopic           string
	kafkaPartitionNum    int32
	kafkaGroupID         = fmt.Sprintf("tikvcdc_kafka_consumer_%s", uuid.New().String())
	kafkaVersion         = "2.4.0"
	kafkaMaxMessageBytes = math.MaxInt64
	kafkaMaxBatchSize    = math.MaxInt64

	downstreamURIStr string
	waitTopicDur     time.Duration

	logPath       string
	logLevel      string
	timezone      string
	ca, cert, key string
)

func init() {
	var upstreamURIStr string

	flag.StringVar(&upstreamURIStr, "upstream-uri", "", "Kafka uri")
	flag.StringVar(&downstreamURIStr, "downstream-uri", "", "downstream sink uri")
	flag.StringVar(&logPath, "log-file", "cdc_kafka_consumer.log", "log file path")
	flag.StringVar(&logLevel, "log-level", "info", "log file path")
	flag.StringVar(&timezone, "tz", "System", "Specify time zone of Kafka consumer")
	flag.StringVar(&ca, "ca", "", "CA certificate path for Kafka SSL connection")
	flag.StringVar(&cert, "cert", "", "Certificate path for Kafka SSL connection")
	flag.StringVar(&key, "key", "", "Private key path for Kafka SSL connection")
	flag.DurationVar(&waitTopicDur, "wait-topic", time.Minute, "Duration waiting for topic created")
	flag.Parse()

	err := logutil.InitLogger(&logutil.Config{
		Level: logLevel,
		File:  logPath,
	})
	if err != nil {
		log.Fatal("init logger failed", zap.Error(err))
	}

	upstreamURI, err := url.Parse(upstreamURIStr)
	if err != nil {
		log.Fatal("invalid upstream-uri", zap.Error(err))
	}
	scheme := strings.ToLower(upstreamURI.Scheme)
	if scheme != "kafka" {
		log.Fatal("invalid upstream-uri scheme, the scheme of upstream-uri must be `kafka`", zap.String("upstream-uri", upstreamURIStr))
	}
	s := upstreamURI.Query().Get("version")
	if s != "" {
		kafkaVersion = s
	}
	s = upstreamURI.Query().Get("consumer-group-id")
	if s != "" {
		kafkaGroupID = s
	}
	kafkaTopic = strings.TrimFunc(upstreamURI.Path, func(r rune) bool {
		return r == '/'
	})
	kafkaAddrs = strings.Split(upstreamURI.Host, ",")

	config, err := newSaramaConfig()
	if err != nil {
		log.Fatal("Error creating sarama config", zap.Error(err))
	}

	s = upstreamURI.Query().Get("partition-num")
	if s == "" {
		partition, err := getPartitionNum(kafkaAddrs, kafkaTopic, config)
		if err != nil {
			log.Fatal("can not get partition number", zap.String("topic", kafkaTopic), zap.Error(err))
		}
		kafkaPartitionNum = partition
	} else {
		c, err := strconv.ParseInt(s, 10, 32)
		if err != nil {
			log.Fatal("invalid partition-num of upstream-uri")
		}
		kafkaPartitionNum = int32(c)
	}

	s = upstreamURI.Query().Get("max-message-bytes")
	if s != "" {
		c, err := strconv.Atoi(s)
		if err != nil {
			log.Fatal("invalid max-message-bytes of upstream-uri")
		}
		log.Info("Setting max-message-bytes", zap.Int("max-message-bytes", c))
		kafkaMaxMessageBytes = c
	}

	s = upstreamURI.Query().Get("max-batch-size")
	if s != "" {
		c, err := strconv.Atoi(s)
		if err != nil {
			log.Fatal("invalid max-batch-size of upstream-uri")
		}
		log.Info("Setting max-batch-size", zap.Int("max-batch-size", c))
		kafkaMaxBatchSize = c
	}
}

func getPartitionNum(address []string, topic string, cfg *sarama.Config) (int32, error) {
	// get partition number or create topic automatically
	admin, err := sarama.NewClusterAdmin(address, cfg)
	if err != nil {
		return 0, errors.Trace(err)
	}
	topics, err := admin.ListTopics()
	if err != nil {
		return 0, errors.Trace(err)
	}
	err = admin.Close()
	if err != nil {
		return 0, errors.Trace(err)
	}
	topicDetail, exist := topics[topic]
	if !exist {
		return 0, errors.Errorf("can not find topic %s", topic)
	}
	log.Info("get partition number of topic", zap.String("topic", topic), zap.Int32("partition_num", topicDetail.NumPartitions))
	return topicDetail.NumPartitions, nil
}

func waitTopicCreated(address []string, topic string, cfg *sarama.Config) error {
	admin, err := sarama.NewClusterAdmin(address, cfg)
	if err != nil {
		return errors.Trace(err)
	}
	defer admin.Close()
	start := time.Now()
	for time.Since(start) < waitTopicDur {
		topics, err := admin.ListTopics()
		if err != nil {
			return errors.Trace(err)
		}
		if _, ok := topics[topic]; ok {
			return nil
		}
		log.Info("wait the topic created", zap.String("topic", topic))
		time.Sleep(1 * time.Second)
	}
	return errors.Errorf("wait the topic(%s) created timeout", topic)
}

func newSaramaConfig() (*sarama.Config, error) {
	config := sarama.NewConfig()

	version, err := sarama.ParseKafkaVersion(kafkaVersion)
	if err != nil {
		return nil, errors.Trace(err)
	}

	config.ClientID = "tikvcdc_kafka_sarama_consumer"
	config.Version = version

	config.Metadata.Retry.Max = 10000
	config.Metadata.Retry.Backoff = 500 * time.Millisecond
	config.Consumer.Retry.Backoff = 500 * time.Millisecond
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	if len(ca) != 0 {
		config.Net.TLS.Enable = true
		config.Net.TLS.Config, err = (&security.Credential{
			CAPath:   ca,
			CertPath: cert,
			KeyPath:  key,
		}).ToTLSConfig()
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	return config, err
}

func main() {
	log.Info("Starting a new TiKV CDC open protocol consumer")

	/**
	 * Construct a new Sarama configuration.
	 * The Kafka cluster version has to be defined before the consumer/producer is initialized.
	 */
	config, err := newSaramaConfig()
	if err != nil {
		log.Fatal("Error creating sarama config", zap.Error(err))
	}
	err = waitTopicCreated(kafkaAddrs, kafkaTopic, config)
	if err != nil {
		log.Fatal("wait topic created failed", zap.Error(err))
	}
	/**
	 * Setup a new Sarama consumer group
	 */
	consumer, err := NewConsumer(context.TODO())
	if err != nil {
		log.Fatal("Error creating consumer", zap.Error(err))
	}

	ctx, cancel := context.WithCancel(context.Background())
	client, err := sarama.NewConsumerGroup(kafkaAddrs, kafkaGroupID, config)
	if err != nil {
		log.Fatal("Error creating consumer group client", zap.Error(err))
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			// `Consume` should be called inside an infinite loop, when a
			// server-side rebalance happens, the consumer session will need to be
			// recreated to get the new claims
			if err := client.Consume(ctx, strings.Split(kafkaTopic, ","), consumer); err != nil {
				log.Fatal("Error from consumer: %v", zap.Error(err))
			}
			// check if context was cancelled, signaling that the consumer should stop
			if ctx.Err() != nil {
				return
			}
			consumer.ready = make(chan bool)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := consumer.Run(ctx); err != nil {
			if errors.Cause(err) == context.Canceled {
				log.Info("consumer stopped", zap.Error(err))
			} else {
				log.Fatal("Error running consumer: %v", zap.Error(err))
			}
		}
	}()

	<-consumer.ready // Await till the consumer has been set up
	log.Info("TiKV CDC open protocol consumer up and running!...")

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-ctx.Done():
		log.Info("terminating: context cancelled")
	case <-sigterm:
		log.Info("terminating: via signal")
	}
	cancel()
	wg.Wait()
	if err = client.Close(); err != nil {
		log.Fatal("Error closing client", zap.Error(err))
	}
}

type partitionSink struct {
	sink.Sink
	resolvedTs  atomic.Uint64
	partitionNo int
	lastCRTs    atomic.Uint64
}

// Consumer represents a Sarama consumer group consumer
type Consumer struct {
	ready chan bool

	sinks   []*partitionSink
	sinksMu sync.Mutex

	globalResolvedTs atomic.Uint64
}

// NewConsumer creates a new cdc kafka consumer
func NewConsumer(ctx context.Context) (*Consumer, error) {
	tz, err := util.GetTimezone(timezone)
	if err != nil {
		return nil, errors.Annotate(err, "can not load timezone")
	}
	ctx = util.PutTimezoneInCtx(ctx, tz)
	c := new(Consumer)
	c.sinks = make([]*partitionSink, kafkaPartitionNum)
	ctx, cancel := context.WithCancel(ctx)
	errCh := make(chan error, 1)
	opts := map[string]string{}
	for i := 0; i < int(kafkaPartitionNum); i++ {
		s, err := sink.New(ctx, "kafka-consumer", downstreamURIStr, config.GetDefaultReplicaConfig(), opts, errCh)
		if err != nil {
			cancel()
			return nil, errors.Trace(err)
		}
		c.sinks[i] = &partitionSink{Sink: s, partitionNo: i}
	}
	go func() {
		err := <-errCh
		if errors.Cause(err) != context.Canceled {
			log.Error("error on running consumer", zap.Error(err))
		} else {
			log.Info("consumer exited")
		}
		cancel()
	}()
	c.ready = make(chan bool)
	return c, nil
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (c *Consumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark the c as ready
	close(c.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (c *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (c *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	ctx := context.TODO()
	partition := claim.Partition()
	c.sinksMu.Lock()
	sink := c.sinks[partition]
	c.sinksMu.Unlock()
	if sink == nil {
		panic("sink should initialized")
	}
ClaimMessages:
	for message := range claim.Messages() {
		log.Debug("Message claimed", zap.Int32("partition", message.Partition), zap.ByteString("key", message.Key), zap.ByteString("value", message.Value))
		batchDecoder, err := codec.NewJSONEventBatchDecoder(message.Key, message.Value)
		if err != nil {
			return errors.Trace(err)
		}

		counter := 0
		for {
			tp, hasNext, err := batchDecoder.HasNext()
			if err != nil {
				log.Fatal("decode message key failed", zap.Error(err))
			}
			if !hasNext {
				break
			}

			counter++
			// If the message containing only one event exceeds the length limit, CDC will allow it and issue a warning.
			if len(message.Key)+len(message.Value) > kafkaMaxMessageBytes && counter > 1 {
				log.Fatal("kafka max-messages-bytes exceeded", zap.Int("max-message-bytes", kafkaMaxMessageBytes),
					zap.Int("received-bytes", len(message.Key)+len(message.Value)))
			}

			switch tp {
			case model.MqMessageTypeKv:
				kv, err := batchDecoder.NextChangedEvent()
				if err != nil {
					log.Fatal("decode message value failed", zap.ByteString("value", message.Value))
				}
				globalResolvedTs := c.globalResolvedTs.Load()
				if kv.CRTs <= globalResolvedTs || kv.CRTs <= sink.resolvedTs.Load() {
					log.Info("filter fallback kv", zap.ByteString("key", message.Key),
						zap.Uint64("globalResolvedTs", globalResolvedTs),
						zap.Uint64("sinkResolvedTs", sink.resolvedTs.Load()),
						zap.Int32("partition", partition))
					break ClaimMessages
				}
				err = sink.EmitChangedEvents(ctx, kv)
				if err != nil {
					log.Fatal("emit row changed event failed", zap.Error(err))
				}
				log.Debug("Emit ChangedEvent", zap.Any("kv", kv))
				lastCRTs := sink.lastCRTs.Load()
				if lastCRTs < kv.CRTs {
					sink.lastCRTs.Store(kv.CRTs)
				}
			case model.MqMessageTypeResolved:
				ts, err := batchDecoder.NextResolvedEvent()
				if err != nil {
					log.Fatal("decode message value failed", zap.ByteString("value", message.Value))
				}
				resolvedTs := sink.resolvedTs.Load()
				if resolvedTs < ts {
					log.Debug("update sink resolved ts",
						zap.Uint64("ts", ts),
						zap.Int32("partition", partition))
					sink.resolvedTs.Store(ts)
				}
			}
			session.MarkMessage(message, "")
		}

		if counter > kafkaMaxBatchSize {
			log.Fatal("Open Protocol max-batch-size exceeded", zap.Int("max-batch-size", kafkaMaxBatchSize),
				zap.Int("actual-batch-size", counter))
		}
	}

	return nil
}

func (c *Consumer) forEachSink(fn func(sink *partitionSink) error) error {
	c.sinksMu.Lock()
	defer c.sinksMu.Unlock()
	for _, sink := range c.sinks {
		if err := fn(sink); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// Run runs the Consumer
func (c *Consumer) Run(ctx context.Context) error {
	var lastGlobalResolvedTs uint64
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		time.Sleep(100 * time.Millisecond)

		globalResolvedTs := uint64(math.MaxUint64)
		err := c.forEachSink(func(sink *partitionSink) error {
			resolvedTs := sink.resolvedTs.Load()
			if resolvedTs < globalResolvedTs {
				globalResolvedTs = resolvedTs
			}
			return nil
		})
		if err != nil {
			return errors.Trace(err)
		}

		if lastGlobalResolvedTs == globalResolvedTs {
			continue
		}
		lastGlobalResolvedTs = globalResolvedTs
		c.globalResolvedTs.Store(globalResolvedTs)
		log.Info("update globalResolvedTs", zap.Uint64("ts", globalResolvedTs))

		err = c.forEachSink(func(sink *partitionSink) error {
			return syncFlushChangedEvents(ctx, sink, globalResolvedTs)
		})
		if err != nil {
			return errors.Trace(err)
		}
	}
}

func syncFlushChangedEvents(ctx context.Context, sink *partitionSink, resolvedTs uint64) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		keyspanID := model.KeySpanID(0)
		checkpointTs, err := sink.FlushChangedEvents(ctx, keyspanID, resolvedTs)
		if err != nil {
			return errors.Trace(err)
		}
		if checkpointTs < resolvedTs {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		return nil
	}
}
