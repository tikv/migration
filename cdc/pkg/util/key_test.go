// Copyright 2022 PingCAP, Inc.
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

	"github.com/stretchr/testify/require"
)

type TestKey struct {
	Format string
	Key    string
	Expect []byte
	Valid  bool
}

func TestParseKey(t *testing.T) {
	testKeys := []TestKey{
		{
			Format: "raw",
			Key:    "abcde",
			Expect: []byte("abcde"),
			Valid:  true,
		},
		{
			Format: "hex",
			Key:    "78bcacef",
			Expect: []byte{0x78, 0xbc, 0xac, 0xef},
			Valid:  true,
		},
		{
			Format: "escaped",
			Key:    "\\a\\x1",
			Expect: []byte("\a\x01"),
			Valid:  true,
		},
		{
			Format: "escaped",
			Key:    "\\b\\f",
			Expect: []byte("\b\f"),
			Valid:  true,
		},
		{
			Format: "hex",
			Key:    "abcde",
			Expect: []byte{},
			Valid:  false,
		},
	}
	for _, testKey := range testKeys {
		key, err := ParseKey(testKey.Format, testKey.Key)
		if testKey.Valid {
			require.Nil(t, err)
			require.Equal(t, testKey.Expect, key)
		} else {
			require.NotNil(t, err)
		}
	}
}

type TestKeyRange struct {
	StartKey []byte
	EndKey   []byte
	Valid    bool
}

func TestEncodeKeySpan(t *testing.T) {
	keySpans := []TestKeyRange{
		{
			StartKey: []byte{},
			EndKey:   []byte{},
			Valid:    true,
		},
		{
			StartKey: []byte{},
			EndKey:   []byte{'z'},
			Valid:    true,
		},
		{
			StartKey: []byte{'a'},
			EndKey:   []byte{},
			Valid:    true,
		},
		{
			StartKey: []byte{'a'},
			EndKey:   []byte{'z'},
			Valid:    true,
		},
		{
			StartKey: []byte{'z'},
			EndKey:   []byte{'y'},
			Valid:    false,
		},
	}
	expect := []TestKeyRange{
		{
			StartKey: APIV2RawKeyPrefix,
			EndKey:   APIV2RawEndKey,
		},
		{
			StartKey: APIV2RawKeyPrefix,
			EndKey:   append(APIV2RawKeyPrefix, 'z'),
		},
		{
			StartKey: append(APIV2RawKeyPrefix, 'a'),
			EndKey:   APIV2RawEndKey,
		},
		{
			StartKey: append(APIV2RawKeyPrefix, 'a'),
			EndKey:   append(APIV2RawKeyPrefix, 'z'),
		},
	}
	for i, testSpan := range keySpans {
		err := ValidKeyFormat("raw", string(testSpan.StartKey), string(testSpan.EndKey))
		if testSpan.Valid {
			require.Nil(t, err)
			start, end, err := EncodeKeySpan("raw", string(testSpan.StartKey), string(testSpan.EndKey))
			require.Nil(t, err)
			require.Equal(t, expect[i].StartKey, start)
			require.Equal(t, expect[i].EndKey, end)
		} else {
			require.NotNil(t, err)
		}
	}
}
