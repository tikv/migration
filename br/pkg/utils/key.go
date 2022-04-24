// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package utils

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	berrors "github.com/tikv/migration/br/pkg/errors"
)

const (
	APIV2KeyPrefix    byte = 'r'
	APIV2KeyPrefixEnd byte = APIV2KeyPrefix + 1
)

// ParseKey parse key by given format.
func ParseKey(format, key string) ([]byte, error) {
	switch format {
	case "raw":
		return []byte(key), nil
	case "escaped":
		return unescapedKey(key)
	case "hex":
		key, err := hex.DecodeString(key)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return key, nil
	}
	return nil, errors.Annotate(berrors.ErrInvalidArgument, "unknown format")
}

// Ref PD: https://github.com/pingcap/pd/blob/master/tools/pd-ctl/pdctl/command/region_command.go#L334
func unescapedKey(text string) ([]byte, error) {
	var buf []byte
	r := bytes.NewBuffer([]byte(text))
	for {
		c, err := r.ReadByte()
		if err != nil {
			if errors.Cause(err) != io.EOF { // nolint:errorlint
				return nil, errors.Trace(err)
			}
			break
		}
		if c != '\\' {
			buf = append(buf, c)
			continue
		}
		n := r.Next(1)
		if len(n) == 0 {
			return nil, io.EOF
		}
		// See: https://golang.org/ref/spec#Rune_literals
		if idx := strings.IndexByte(`abfnrtv\'"`, n[0]); idx != -1 {
			buf = append(buf, []byte("\a\b\f\n\r\t\v\\'\"")[idx])
			continue
		}

		switch n[0] {
		case 'x':
			fmt.Sscanf(string(r.Next(2)), "%02x", &c)
			buf = append(buf, c)
		default:
			n = append(n, r.Next(2)...)
			_, err := fmt.Sscanf(string(n), "%03o", &c)
			if err != nil {
				return nil, errors.Trace(err)
			}
			buf = append(buf, c)
		}
	}
	return buf, nil
}

// CompareEndKey compared two keys that BOTH represent the EXCLUSIVE ending of some range. An empty end key is the very
// end, so an empty key is greater than any other keys.
// Please note that this function is not applicable if any one argument is not an EXCLUSIVE ending of a range.
func CompareEndKey(a, b []byte) int {
	if len(a) == 0 {
		if len(b) == 0 {
			return 0
		}
		return 1
	}

	if len(b) == 0 {
		return -1
	}

	return bytes.Compare(a, b)
}

type KeyRange struct {
	Start []byte
	End   []byte
}

// encode
func formatAPIV2Key(key []byte, isEnd bool) []byte {
	if isEnd && len(key) == 0 {
		return []byte{APIV2KeyPrefixEnd}
	}
	apiv2Key := []byte{APIV2KeyPrefix}
	return append(apiv2Key, key...)
}

// FormatAPIV2KeyRange convert user key to APIV2 format.
func FormatAPIV2KeyRange(startKey, endKey []byte) *KeyRange {
	return &KeyRange{
		Start: formatAPIV2Key(startKey, false),
		End:   formatAPIV2Key(endKey, true),
	}
}

// ConvertBackupConfigKeyRange do conversion between formated APIVersion key and backupmeta key
// for example, backup apiv1 -> apiv2, add `r` prefix and `s` for empty end key.
// apiv2 -> apiv1, remove first byte.
func ConvertBackupConfigKeyRange(startKey, endKey []byte, srcAPIVer, dstAPIVer kvrpcpb.APIVersion) *KeyRange {
	if srcAPIVer == dstAPIVer {
		return &KeyRange{
			Start: startKey,
			End:   endKey,
		}
	}
	if dstAPIVer == kvrpcpb.APIVersion_V2 {
		return FormatAPIV2KeyRange(startKey, endKey)
	}
	if srcAPIVer == kvrpcpb.APIVersion_V2 {
		return &KeyRange{
			Start: startKey[1:],
			End:   endKey[1:],
		}
	}
	// unreachable
	return nil
}
