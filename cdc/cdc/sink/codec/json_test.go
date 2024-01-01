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
	"math"
	"strconv"
	"testing"

	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/tikv/migration/cdc/cdc/model"
	"github.com/tikv/migration/cdc/pkg/config"
	"github.com/tikv/migration/cdc/pkg/util/testleak"
)

func Test(t *testing.T) { check.TestingT(t) }

type batchSuite struct {
	kvCases         [][]*model.RawKVEntry
	resolvedTsCases [][]uint64
}

var _ = check.Suite(&batchSuite{
	kvCases:         codecEntryCases,
	resolvedTsCases: codecResolvedTSCases,
})

func (s *batchSuite) testBatchCodec(c *check.C, newEncoder func() EventBatchEncoder, newDecoder func(key []byte, value []byte) (EventBatchDecoder, error)) {
	checkKvDecoder := func(decoder EventBatchDecoder, cs []*model.RawKVEntry) {
		index := 0
		for {
			tp, hasNext, err := decoder.HasNext()
			c.Assert(err, check.IsNil)
			if !hasNext {
				break
			}
			c.Assert(tp, check.Equals, model.MqMessageTypeKv)
			kv, err := decoder.NextChangedEvent()
			c.Assert(err, check.IsNil)
			c.Assert(kv, check.DeepEquals, cs[index])
			index++
		}
	}
	checkTSDecoder := func(decoder EventBatchDecoder, cs []uint64) {
		index := 0
		for {
			tp, hasNext, err := decoder.HasNext()
			c.Assert(err, check.IsNil)
			if !hasNext {
				break
			}
			c.Assert(tp, check.Equals, model.MqMessageTypeResolved)
			ts, err := decoder.NextResolvedEvent()
			c.Assert(err, check.IsNil)
			c.Assert(ts, check.DeepEquals, cs[index])
			index++
		}
	}

	for _, cs := range s.kvCases {
		encoder := newEncoder()
		err := encoder.SetParams(map[string]string{"max-message-bytes": "8192", "max-batch-size": "64"})
		c.Assert(err, check.IsNil)

		mixedEncoder := newEncoder()
		mixedEncoder.(*JSONEventBatchEncoder).SetMixedBuildSupport(true)
		for _, row := range cs {
			_, err := encoder.AppendChangedEvent(row)
			c.Assert(err, check.IsNil)

			op, err := mixedEncoder.AppendChangedEvent(row)
			c.Assert(op, check.Equals, EncoderNoOperation)
			c.Assert(err, check.IsNil)
		}
		// test mixed decode
		mixed := mixedEncoder.MixedBuild(true)
		c.Assert(len(mixed), check.Equals, mixedEncoder.Size())
		mixedDecoder, err := newDecoder(mixed, nil)
		c.Assert(err, check.IsNil)
		checkKvDecoder(mixedDecoder, cs)
		// test normal decode
		if len(cs) > 0 {
			res := encoder.Build()
			c.Assert(res, check.HasLen, 1)
			decoder, err := newDecoder(res[0].Key, res[0].Value)
			c.Assert(err, check.IsNil)
			checkKvDecoder(decoder, cs)
		}
	}

	for _, cs := range s.resolvedTsCases {
		encoder := newEncoder()
		mixedEncoder := newEncoder()
		err := encoder.SetParams(map[string]string{"max-message-bytes": "8192", "max-batch-size": "64"})
		c.Assert(err, check.IsNil)

		mixedEncoder.(*JSONEventBatchEncoder).SetMixedBuildSupport(true)
		for i, ts := range cs {
			msg, err := encoder.EncodeCheckpointEvent(ts)
			c.Assert(err, check.IsNil)
			c.Assert(msg, check.NotNil)
			decoder, err := newDecoder(msg.Key, msg.Value)
			c.Assert(err, check.IsNil)
			checkTSDecoder(decoder, cs[i:i+1])

			msg, err = mixedEncoder.EncodeCheckpointEvent(ts)
			c.Assert(msg, check.IsNil)
			c.Assert(err, check.IsNil)
		}

		// test mixed encode
		mixed := mixedEncoder.MixedBuild(true)
		c.Assert(len(mixed), check.Equals, mixedEncoder.Size())
		mixedDecoder, err := newDecoder(mixed, nil)
		c.Assert(err, check.IsNil)
		checkTSDecoder(mixedDecoder, cs)
	}
}

func (s *batchSuite) TestParamsEdgeCases(c *check.C) {
	defer testleak.AfterTest(c)()
	encoder := NewJSONEventBatchEncoder().(*JSONEventBatchEncoder)
	err := encoder.SetParams(map[string]string{"max-message-bytes": "10485760"})
	c.Assert(err, check.IsNil)
	c.Assert(encoder.maxBatchSize, check.Equals, DefaultMaxBatchSize)
	c.Assert(encoder.maxMessageBytes, check.Equals, config.DefaultMaxMessageBytes)

	err = encoder.SetParams(map[string]string{"max-message-bytes": "0"})
	c.Assert(err, check.ErrorMatches, ".*invalid.*")

	err = encoder.SetParams(map[string]string{"max-message-bytes": "-1"})
	c.Assert(err, check.ErrorMatches, ".*invalid.*")

	err = encoder.SetParams(map[string]string{"max-message-bytes": strconv.Itoa(math.MaxInt32)})
	c.Assert(err, check.IsNil)
	c.Assert(encoder.maxBatchSize, check.Equals, DefaultMaxBatchSize)
	c.Assert(encoder.maxMessageBytes, check.Equals, math.MaxInt32)

	err = encoder.SetParams(map[string]string{"max-message-bytes": strconv.Itoa(math.MaxUint32)})
	c.Assert(err, check.IsNil)
	c.Assert(encoder.maxBatchSize, check.Equals, DefaultMaxBatchSize)
	c.Assert(encoder.maxMessageBytes, check.Equals, math.MaxUint32)

	err = encoder.SetParams(map[string]string{"max-batch-size": "0"})
	c.Assert(err, check.ErrorMatches, ".*invalid.*")

	err = encoder.SetParams(map[string]string{"max-batch-size": "-1"})
	c.Assert(err, check.ErrorMatches, ".*invalid.*")

	err = encoder.SetParams(map[string]string{"max-message-bytes": "10485760", "max-batch-size": strconv.Itoa(math.MaxInt32)})
	c.Assert(err, check.IsNil)
	c.Assert(encoder.maxBatchSize, check.Equals, math.MaxInt32)
	c.Assert(encoder.maxMessageBytes, check.Equals, config.DefaultMaxMessageBytes)

	err = encoder.SetParams(map[string]string{"max-message-bytes": "10485760", "max-batch-size": strconv.Itoa(math.MaxUint32)})
	c.Assert(err, check.IsNil)
	c.Assert(encoder.maxBatchSize, check.Equals, math.MaxUint32)
	c.Assert(encoder.maxMessageBytes, check.Equals, config.DefaultMaxMessageBytes)
}

func (s *batchSuite) TestSetParams(c *check.C) {
	defer testleak.AfterTest(c)

	opts := make(map[string]string)
	encoderBuilder := newJSONEventBatchEncoderBuilder(opts)
	c.Assert(encoderBuilder, check.NotNil)
	encoder, err := encoderBuilder.Build(context.Background())
	c.Assert(encoder, check.IsNil)
	c.Assert(
		errors.Cause(err),
		check.ErrorMatches,
		".*max-message-bytes not found.*",
	)

	opts["max-message-bytes"] = "1"
	encoderBuilder = newJSONEventBatchEncoderBuilder(opts)
	c.Assert(encoderBuilder, check.NotNil)
	encoder, err = encoderBuilder.Build(context.Background())
	c.Assert(err, check.IsNil)
	c.Assert(encoder, check.NotNil)

	jsonEncoder, ok := encoder.(*JSONEventBatchEncoder)
	c.Assert(ok, check.IsTrue)
	c.Assert(jsonEncoder.GetMaxMessageBytes(), check.Equals, 1)
}

func (s *batchSuite) TestMaxMessageBytes(c *check.C) {
	defer testleak.AfterTest(c)()
	encoder := NewJSONEventBatchEncoder()

	// the size of `testEvent` is 75
	testEvent := &model.RawKVEntry{
		OpType:    model.OpTypePut,
		Key:       []byte("key"),
		Value:     []byte("value"),
		CRTs:      100,
		ExpiredTs: 200,
	}
	eventSize := 75

	// for a single message, the overhead is 36(maximumRecordOverhead) + 8(versionHea) = 44, just can hold it.
	a := strconv.Itoa(eventSize + 44)
	err := encoder.SetParams(map[string]string{"max-message-bytes": a})
	c.Check(err, check.IsNil)
	r, err := encoder.AppendChangedEvent(testEvent)
	c.Check(err, check.IsNil)
	c.Check(r, check.Equals, EncoderNoOperation)

	a = strconv.Itoa(eventSize + 43)
	err = encoder.SetParams(map[string]string{"max-message-bytes": a})
	c.Assert(err, check.IsNil)
	r, err = encoder.AppendChangedEvent(testEvent)
	c.Check(err, check.NotNil)
	c.Check(r, check.Equals, EncoderNoOperation)

	// make sure each batch's `Length` not greater than `max-message-bytes`
	err = encoder.SetParams(map[string]string{"max-message-bytes": "256"})
	c.Check(err, check.IsNil)

	for i := 0; i < 10000; i++ {
		r, err := encoder.AppendChangedEvent(testEvent)
		c.Check(r, check.Equals, EncoderNoOperation)
		c.Check(err, check.IsNil)
	}

	messages := encoder.Build()
	for _, msg := range messages {
		c.Assert(msg.Length(), check.LessEqual, 256)
	}
}

func (s *batchSuite) TestMaxBatchSize(c *check.C) {
	defer testleak.AfterTest(c)()
	encoderBuilder := newJSONEventBatchEncoderBuilder(map[string]string{"max-message-bytes": "1048576", "max-batch-size": "64"})
	c.Assert(encoderBuilder, check.NotNil)
	encoder, err := encoderBuilder.Build(context.Background())
	c.Assert(err, check.IsNil)
	c.Assert(encoder, check.NotNil)

	testEvent := &model.RawKVEntry{
		OpType:    model.OpTypePut,
		Key:       []byte("key"),
		Value:     []byte("value"),
		CRTs:      1,
		ExpiredTs: 20,
	}

	for i := 0; i < 10000; i++ {
		r, err := encoder.AppendChangedEvent(testEvent)
		c.Check(r, check.Equals, EncoderNoOperation)
		c.Check(err, check.IsNil)
	}

	messages := encoder.Build()
	sum := 0
	for _, msg := range messages {
		decoder, err := NewJSONEventBatchDecoder(msg.Key, msg.Value)
		c.Check(err, check.IsNil)
		count := 0
		for {
			t, hasNext, err := decoder.HasNext()
			c.Check(err, check.IsNil)
			if !hasNext {
				break
			}

			c.Check(t, check.Equals, model.MqMessageTypeKv)
			_, err = decoder.NextChangedEvent()
			c.Check(err, check.IsNil)
			count++
		}
		c.Check(count, check.LessEqual, 64)
		sum += count
	}
	c.Check(sum, check.Equals, 10000)
}

func (s *batchSuite) TestDefaultEventBatchCodec(c *check.C) {
	defer testleak.AfterTest(c)()
	s.testBatchCodec(c, func() EventBatchEncoder {
		encoder := NewJSONEventBatchEncoder()
		return encoder
	}, NewJSONEventBatchDecoder)
}
