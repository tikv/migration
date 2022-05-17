// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package utils

import (
	"encoding/hex"
	"testing"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/stretchr/testify/require"
)

func TestParseKey(t *testing.T) {
	// test rawKey
	testRawKey := []struct {
		rawKey string
		ans    []byte
	}{
		{"1234", []byte("1234")},
		{"abcd", []byte("abcd")},
		{"1a2b", []byte("1a2b")},
		{"AA", []byte("AA")},
		{"\a", []byte("\a")},
		{"\\'", []byte("\\'")},
	}

	for _, tt := range testRawKey {
		parsedKey, err := ParseKey("raw", tt.rawKey)
		require.NoError(t, err)
		require.Equal(t, tt.ans, parsedKey)
	}

	// test EscapedKey
	testEscapedKey := []struct {
		EscapedKey string
		ans        []byte
	}{
		{"\\a\\x1", []byte("\a\x01")},
		{"\\b\\f", []byte("\b\f")},
		{"\\n\\r", []byte("\n\r")},
		{"\\t\\v", []byte("\t\v")},
		{"\\'", []byte("'")},
	}

	for _, tt := range testEscapedKey {
		parsedKey, err := ParseKey("escaped", tt.EscapedKey)
		require.NoError(t, err)
		require.Equal(t, tt.ans, parsedKey)
	}

	// test hexKey
	testHexKey := []struct {
		hexKey string
		ans    []byte
	}{
		{"1234", []byte("1234")},
		{"abcd", []byte("abcd")},
		{"1a2b", []byte("1a2b")},
		{"AA", []byte("AA")},
		{"\a", []byte("\a")},
		{"\\'", []byte("\\'")},
		{"\x01", []byte("\x01")},
		{"\xAA", []byte("\xAA")},
	}

	for _, tt := range testHexKey {
		key := hex.EncodeToString([]byte(tt.hexKey))
		parsedKey, err := ParseKey("hex", key)
		require.NoError(t, err)
		require.Equal(t, tt.ans, parsedKey)
	}

	// test other
	testNotSupportKey := []struct {
		any string
		ans []byte
	}{
		{"1234", []byte("1234")},
		{"abcd", []byte("abcd")},
		{"1a2b", []byte("1a2b")},
		{"AA", []byte("AA")},
		{"\a", []byte("\a")},
		{"\\'", []byte("\\'")},
		{"\x01", []byte("\x01")},
		{"\xAA", []byte("\xAA")},
	}

	for _, tt := range testNotSupportKey {
		_, err := ParseKey("notSupport", tt.any)
		require.Error(t, err)
		require.Regexp(t, "^unknown format", err.Error())
	}
}

func TestCompareEndKey(t *testing.T) {
	// test endKey
	testCase := []struct {
		key1 []byte
		key2 []byte
		ans  int
	}{
		{[]byte("1"), []byte("2"), -1},
		{[]byte("1"), []byte("1"), 0},
		{[]byte("2"), []byte("1"), 1},
		{[]byte("1"), []byte(""), -1},
		{[]byte(""), []byte(""), 0},
		{[]byte(""), []byte("1"), 1},
	}

	for _, tt := range testCase {
		res := CompareEndKey(tt.key1, tt.key2)
		require.Equal(t, tt.ans, res)
	}
}

func TestFormatAPIV2KeyRange(t *testing.T) {
	testCases := []struct {
		apiv1Key KeyRange
		apiv2Key KeyRange
	}{
		{KeyRange{[]byte(""), []byte("")}, KeyRange{[]byte{APIV2KeyPrefix}, []byte{APIV2KeyPrefixEnd}}},
		{KeyRange{[]byte(""), []byte("abc")}, KeyRange{[]byte{APIV2KeyPrefix}, []byte("rabc")}},
		{KeyRange{[]byte("abc"), []byte("")}, KeyRange{[]byte("rabc"), []byte{APIV2KeyPrefixEnd}}},
		{KeyRange{[]byte("abc"), []byte("cde")}, KeyRange{[]byte("rabc"), []byte("rcde")}},
	}
	for _, testCase := range testCases {
		retV2Range := FormatAPIV2KeyRange(testCase.apiv1Key.Start, testCase.apiv1Key.End)
		require.Equal(t, retV2Range, &testCase.apiv2Key)
	}
}

func TestConvertBackupConfigKeyRange(t *testing.T) {
	testCases := []struct {
		input     KeyRange
		srcAPIVer kvrpcpb.APIVersion
		dstAPIVer kvrpcpb.APIVersion
		output    *KeyRange
	}{
		{KeyRange{[]byte(""), []byte("")},
			kvrpcpb.APIVersion_V1,
			kvrpcpb.APIVersion_V1,
			&KeyRange{[]byte(""), []byte("")},
		},
		{KeyRange{[]byte(""), []byte("")},
			kvrpcpb.APIVersion_V1,
			kvrpcpb.APIVersion_V2,
			&KeyRange{[]byte{APIV2KeyPrefix}, []byte{APIV2KeyPrefixEnd}},
		},
		{KeyRange{[]byte(""), []byte("")},
			kvrpcpb.APIVersion_V1TTL,
			kvrpcpb.APIVersion_V2,
			&KeyRange{[]byte{APIV2KeyPrefix}, []byte{APIV2KeyPrefixEnd}},
		},
		{
			KeyRange{[]byte("abc"), []byte("cde")},
			kvrpcpb.APIVersion_V1,
			kvrpcpb.APIVersion_V2,
			&KeyRange{[]byte("rabc"), []byte("rcde")},
		},
		{
			KeyRange{[]byte("abc"), []byte("cde")},
			kvrpcpb.APIVersion_V1TTL,
			kvrpcpb.APIVersion_V2,
			&KeyRange{[]byte("rabc"), []byte("rcde")},
		},
		{
			KeyRange{[]byte("rabc"), []byte("rcde")},
			kvrpcpb.APIVersion_V2,
			kvrpcpb.APIVersion_V1,
			&KeyRange{[]byte("abc"), []byte("cde")},
		},
		{
			KeyRange{[]byte("rabc"), []byte("rcde")},
			kvrpcpb.APIVersion_V2,
			kvrpcpb.APIVersion_V1TTL,
			&KeyRange{[]byte("abc"), []byte("cde")},
		},
		// following are invalid conversion
		{
			KeyRange{[]byte("rabc"), []byte("rcde")},
			kvrpcpb.APIVersion_V1,
			kvrpcpb.APIVersion_V1TTL,
			nil,
		},
		{
			KeyRange{[]byte("rabc"), []byte("rcde")},
			kvrpcpb.APIVersion_V1TTL,
			kvrpcpb.APIVersion_V1,
			nil,
		},
	}

	for _, testCase := range testCases {
		retKeyRange := ConvertBackupConfigKeyRange(
			testCase.input.Start, testCase.input.End, testCase.srcAPIVer, testCase.dstAPIVer)
		require.Equal(t, retKeyRange, testCase.output)
	}
}
