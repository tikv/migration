// Copyright 2023 PingCAP, Inc.
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
	"testing"

	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/stretchr/testify/require"
)

func TestKvFilterConfig(t *testing.T) {
	t.Parallel()
	conf := KvFilterConfig{}
	require.Nil(t, conf.Validate())

	conf = KvFilterConfig{}
	conf.KeyPrefix = `prefix\x00\x11\\`
	conf.KeyPattern = `key\x00pattern`
	conf.ValuePattern = `value\ffpattern`
	require.Nil(t, conf.Validate())

	conf = KvFilterConfig{}
	conf.KeyPattern = "\xfd\xe2" // invalid utf8
	require.Error(t, conf.Validate())

	conf = KvFilterConfig{}
	conf.KeyPrefix = `\zz` // invalid escaped
	require.Error(t, conf.Validate())
}

func TestKvFilterMatch(t *testing.T) {
	assert := require.New(t)

	entry := cdcpb.Event_Row{
		OpType: cdcpb.Event_Row_PUT,
		Key:    []byte("r\x00\x00\x00key\x00\x11pattern"),
		Value:  []byte("value\xaa\xffpattern"),
	}

	type testCase struct {
		pattern string
		match   bool
	}

	keyPrefixCases := []testCase{
		{`key\x00`, true},
		{`key\x00\x11pattern`, true},
		{`key\x00\x11pattern\x00`, false},
		{`key\x01\x11pattern`, false},
	}
	for _, c := range keyPrefixCases {
		conf := KvFilterConfig{KeyPrefix: c.pattern}
		filter := CreateFilter(&conf)
		matched, err := filter.EventMatch(&entry)
		assert.NoError(err)
		assert.Equalf(c.match, matched, "pattern: %s", c.pattern)
	}

	keyPatternCases := []testCase{
		{`key\x00`, true},
		{`key\x00\x11pattern`, true},
		{`key\x00\x11pattern\x00`, false},
		{`key\x01\x11pattern`, false},
		{`\x00\x11`, true},
		{`\x11`, true},
		{`\x10`, false},
		{`\x00[\x00\x11]`, true},
		{`\x00.?pattern`, true},
	}
	for _, c := range keyPatternCases {
		conf := KvFilterConfig{KeyPattern: c.pattern}
		filter := CreateFilter(&conf)
		matched, err := filter.EventMatch(&entry)
		assert.NoError(err)
		assert.Equalf(c.match, matched, "pattern: %s", c.pattern)
	}

	valuePatternCases := []testCase{
		{`value[\xaa-\xff]+pattern`, true},
		{`value[\xaa-\xbb]+pattern`, false},
	}
	for _, c := range valuePatternCases {
		conf := KvFilterConfig{ValuePattern: c.pattern}
		filter := CreateFilter(&conf)
		matched, err := filter.EventMatch(&entry)
		assert.NoError(err)
		assert.Equalf(c.match, matched, "pattern: %s", c.pattern)
	}

	// delete entry
	{
		entry := cdcpb.Event_Row{
			OpType: cdcpb.Event_Row_DELETE,
			Key:    []byte("r\x00\x00\x00key\x00\x11pattern"),
			Value:  []byte(""),
		}
		conf := KvFilterConfig{
			KeyPrefix:    "key\x00",
			KeyPattern:   `key.*pattern`,
			ValuePattern: `value`,
		}
		filter := CreateFilter(&conf)
		matched, err := filter.EventMatch(&entry)
		assert.NoError(err)
		assert.True(matched)
	}

	// not RawKV V2 key
	{
		entry := cdcpb.Event_Row{
			OpType: cdcpb.Event_Row_DELETE,
			Key:    []byte("k"),
			Value:  []byte(""),
		}
		conf := KvFilterConfig{}
		filter := CreateFilter(&conf)
		_, err := filter.EventMatch(&entry)
		assert.Error(err)
	}
}
