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

package codec

import (
	"bytes"
	"compress/zlib"
	"testing"

	"github.com/pingcap/check"
	"github.com/tikv/migration/cdc/cdc/model"
	"github.com/tikv/migration/cdc/pkg/util/testleak"
)

func Test(t *testing.T) { check.TestingT(t) }

var (
	codecEntryCases = [][]*model.RawKVEntry{{{
		OpType:    model.OpTypePut,
		Key:       []byte("indexInfo_:_pf01_:_APD0101_:_0000000000000000000"),
		Value:     []byte("0000000000000000000"),
		CRTs:      424316552636792833,
		ExpiredTs: 424316552636792833,
	}}, {{
		OpType:    model.OpTypePut,
		Key:       []byte("indexInfo_:_pf01_:_APD0101_:_0000000000000000001"),
		Value:     []byte("0000000000000000001"),
		CRTs:      424316553934667777,
		ExpiredTs: 424316552636792833,
	}, {
		OpType: model.OpTypeDelete,
		Key:    []byte("indexInfo_:_pf01_:_APD0101_:_0000000000000000002"),
		CRTs:   424316554327097345,
	}, {
		OpType: model.OpTypeDelete,
		Key:    []byte("indexInfo_:_pf01_:_APD0101_:_0000000000000000003"),
		CRTs:   424316554746789889,
	}, {
		OpType: model.OpTypePut,
		Key:    []byte("indexInfo_:_pf01_:_APD0101_:_0000000000000000004"),
		Value:  []byte("0000000000000000004"),
		CRTs:   424316555073945601,
	}}, {}}

	codecResolvedTSCases = [][]uint64{{424316592563683329}, {424316594097225729, 424316594214141953, 424316594345213953}, {}}

	codecBenchmarkKvChanges = codecEntryCases[1]

	codecJSONEncodedKvChanges = []*MQMessage{}
)

var _ = check.Suite(&codecTestSuite{})

type codecTestSuite struct{}

func (s *codecTestSuite) checkCompressedSize(messages []*MQMessage) (int, int) {
	var buff bytes.Buffer
	writer := zlib.NewWriter(&buff)
	originalSize := 0
	for _, message := range messages {
		originalSize += len(message.Key) + len(message.Value)
		if len(message.Key) > 0 {
			_, _ = writer.Write(message.Key)
		}
		_, _ = writer.Write(message.Value)
	}
	writer.Close()
	return originalSize, buff.Len()
}

func (s *codecTestSuite) encodeKvCase(c *check.C, encoder EventBatchEncoder, events []*model.RawKVEntry) []*MQMessage {
	msg, err := codecEncodeKvCase(encoder, events)
	c.Assert(err, check.IsNil)
	return msg
}

func (s *codecTestSuite) TestJsonVsNothing(c *check.C) {
	defer testleak.AfterTest(c)()
	c.Logf("| case | json size | json compressed |")
	c.Logf("| :--- | :-------- | :-------------- |")
	for i, cs := range codecEntryCases {
		if len(cs) == 0 {
			continue
		}
		jsonEncoder := NewJSONEventBatchEncoder()
		jsonMessages := s.encodeKvCase(c, jsonEncoder, cs)
		jsonOriginal, jsonCompressed := s.checkCompressedSize(jsonMessages)
		c.Logf("| case %d | %d | %d (%d%%)- |", i,
			jsonOriginal, jsonCompressed, 100-100*jsonCompressed/jsonOriginal)
	}
}

func codecEncodeKvCase(encoder EventBatchEncoder, events []*model.RawKVEntry) ([]*MQMessage, error) {
	err := encoder.SetParams(map[string]string{"max-message-bytes": "8192", "max-batch-size": "64"})
	if err != nil {
		return nil, err
	}

	for _, event := range events {
		_, err := encoder.AppendChangedEvent(event)
		if err != nil {
			return nil, err
		}
	}

	if len(events) > 0 {
		return encoder.Build(), nil
	}
	return nil, nil
}

func init() {
	var err error
	if codecJSONEncodedKvChanges, err = codecEncodeKvCase(NewJSONEventBatchEncoder(), codecBenchmarkKvChanges); err != nil {
		panic(err)
	}
}

func BenchmarkJsonEncoding(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, _ = codecEncodeKvCase(NewJSONEventBatchEncoder(), codecBenchmarkKvChanges)
	}
}

func BenchmarkJsonDecoding(b *testing.B) {
	for i := 0; i < b.N; i++ {
		for _, message := range codecJSONEncodedKvChanges {
			decoder, err := NewJSONEventBatchDecoder(message.Key, message.Value)
			if err != nil {
				panic(err)
			}
			for {
				if _, hasNext, err := decoder.HasNext(); err != nil {
					panic(err)
				} else if hasNext {
					_, _ = decoder.NextChangedEvent()
				} else {
					break
				}
			}
		}
	}
}
