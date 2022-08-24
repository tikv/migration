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
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"strings"

	"github.com/pingcap/errors"
)

var (
	// APIV2RawKeyPrefix is prefix of raw key in API V2.
	APIV2RawKeyPrefix = []byte{'r', 0, 0, 0}
	// APIV2RawEndKey is max key of raw key in API V2.
	APIV2RawEndKey = []byte{'r', 0, 0, 1}
)

// EncodeV2Key encode a user key into API V2 format.
func EncodeV2Key(key []byte) []byte {
	return append(APIV2RawKeyPrefix, key...)
}

func DecodeV2Key(key []byte) []byte {
	if len(key) < len(APIV2RawKeyPrefix) {
		return key
	}
	return key[len(APIV2RawKeyPrefix):]
}

// EncodeV2Range encode a range into API V2 format.
// TODO: resue code in client-go
func EncodeV2Range(start, end []byte) ([]byte, []byte) {
	var b []byte
	if len(end) > 0 {
		b = EncodeV2Key(end)
	} else {
		b = APIV2RawEndKey
	}
	return EncodeV2Key(start), b
}

// EncodeKeySpan parse user input key and encode them to apiv2 format.
func EncodeKeySpan(format, start, end string) ([]byte, []byte, error) {
	startKey, err := ParseKey(format, start)
	if err != nil {
		return nil, nil, err
	}
	endKey, err := ParseKey(format, end)
	if err != nil {
		return nil, nil, err
	}
	encodedStart, encodedEnd := EncodeV2Range(startKey, endKey)
	return encodedStart, encodedEnd, nil
}

func ValidKeyFormat(format, start, end string) error {
	startKey, endKey, err := EncodeKeySpan(format, start, end)
	if err != nil {
		return errors.Annotatef(err, "Invalid key format, start:%s, end:%s, format:%s. Err", start, end, format)
	}
	if bytes.Compare(startKey, endKey) >= 0 {
		return errors.Errorf("start %s is larger or equal to end %s", start, end)
	}
	return nil
}

// ParseKey parse key by given format.
// TODO: same code with br/pkg/utils/key.go, need make them common.
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
	return nil, errors.Errorf("inavlid key format:%s", format)
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
