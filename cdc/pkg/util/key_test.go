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
			Key:    "abcde",
			Expect: []byte{0x61, 0x62, 0x63, 0x64, 0x65},
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
}

func TestEncodeKeySpan(t *testing.T) {
	keySpans := []TestKeyRange{
		{
			StartKey: []byte{},
			EndKey:   []byte{},
		},
		{
			StartKey: []byte{},
			EndKey:   []byte{'z'},
		},
		{
			StartKey: []byte{'a'},
			EndKey:   []byte{},
		},
		{
			StartKey: []byte{'a'},
			EndKey:   []byte{'z'},
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
		start, end, err := EncodeKeySpan("raw", string(testSpan.StartKey), string(testSpan.EndKey))
		require.Nil(t, err)
		require.Equal(t, expect[i].StartKey, start)
		require.Equal(t, expect[i].EndKey, end)
	}
}
