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

package model

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tikv/migration/cdc/pkg/regionspan"
	"github.com/tinylib/msgp/msgp"
)

func TestRegionFeedEvent(t *testing.T) {
	t.Parallel()

	raw := &RawKVEntry{
		CRTs:   1,
		OpType: OpTypePut,
	}
	resolved := &ResolvedSpan{
		Span:       regionspan.ComparableSpan{Start: []byte("a"), End: []byte("b")},
		ResolvedTs: 111,
	}

	ev := &RegionFeedEvent{}
	require.Nil(t, ev.GetValue())

	ev = &RegionFeedEvent{Val: raw}
	require.Equal(t, raw, ev.GetValue())

	ev = &RegionFeedEvent{Resolved: resolved}
	require.Equal(t, resolved, ev.GetValue())

	require.Equal(t, "span: [61, 62), resolved-ts: 111", resolved.String())
}

func TestRawKVEntry(t *testing.T) {
	t.Parallel()

	raw := &RawKVEntry{
		StartTs: 100,
		CRTs:    101,
		OpType:  OpTypePut,
		Key:     []byte("123"),
		Value:   []byte("345"),
	}

	require.Equal(t, "OpType: 1, Key: 123, Value: 345, StartTs: 100, CRTs: 101, RegionID: 0, KeySpanID: 0, Sequence: 0", raw.String())
	require.Equal(t, int64(6), raw.ApproximateDataSize())
}

func TestRawKVEntryCodec(t *testing.T) {
	v := RawKVEntry{
		OpType:    OpTypePut,
		Key:       []byte("key"),
		Value:     []byte("value"),
		OldValue:  []byte("old_value"),
		StartTs:   0,
		CRTs:      1,
		ExpiredTs: 2,
		RegionID:  3,
		KeySpanID: 4,
		Sequence:  5,
	}

	{
		bts, err := v.MarshalMsg(nil)
		if err != nil {
			t.Fatal(err)
		}
		vn := RawKVEntry{}
		left, err := vn.UnmarshalMsg(bts)
		if err != nil {
			t.Fatal(err)
		}
		if len(left) > 0 {
			t.Errorf("%d bytes left over after UnmarshalMsg(): %q", len(left), left)
		}
		require.Equal(t, v, vn)
	}

	{
		var buf bytes.Buffer
		msgp.Encode(&buf, &v)

		m := v.Msgsize()
		if buf.Len() > m {
			t.Log("WARNING: TestEncodeDecodeRawKVEntry Msgsize() is inaccurate")
		}

		vn := RawKVEntry{}
		err := msgp.Decode(&buf, &vn)
		if err != nil {
			t.Error(err)
		}

		require.Equal(t, v, vn)

		buf.Reset()
		msgp.Encode(&buf, &v)
		err = msgp.NewReader(&buf).Skip()
		if err != nil {
			t.Error(err)
		}
	}
}
