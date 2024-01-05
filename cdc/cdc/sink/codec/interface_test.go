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
	"github.com/pingcap/check"
	"github.com/tikv/migration/cdc/cdc/model"
	"github.com/tikv/migration/cdc/pkg/config"
	"github.com/tikv/migration/cdc/pkg/util/testleak"
)

type codecInterfaceSuite struct{}

var _ = check.Suite(&codecInterfaceSuite{})

func (s *codecInterfaceSuite) SetUpSuite(c *check.C) {
}

func (s *codecInterfaceSuite) TearDownSuite(c *check.C) {
}

func (s *codecInterfaceSuite) TestCreate(c *check.C) {
	defer testleak.AfterTest(c)()
	event := &model.RawKVEntry{
		StartTs: 1234,
		CRTs:    5678,
	}

	msg := NewMQMessage(config.ProtocolOpen, []byte("key1"), []byte("value1"), event.CRTs, model.MqMessageTypeKv)

	c.Assert(msg.Key, check.BytesEquals, []byte("key1"))
	c.Assert(msg.Value, check.BytesEquals, []byte("value1"))
	c.Assert(msg.Ts, check.Equals, event.CRTs)
	c.Assert(msg.Type, check.Equals, model.MqMessageTypeKv)
	c.Assert(msg.Protocol, check.Equals, config.ProtocolOpen)

	msg = newResolvedMQMessage(config.ProtocolOpen, nil, nil, 1234)
	c.Assert(msg.Key, check.IsNil)
	c.Assert(msg.Value, check.IsNil)
	c.Assert(msg.Ts, check.Equals, uint64(1234))
	c.Assert(msg.Type, check.Equals, model.MqMessageTypeResolved)
	c.Assert(msg.Protocol, check.Equals, config.ProtocolOpen)
}
