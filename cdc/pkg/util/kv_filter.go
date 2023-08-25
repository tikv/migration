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
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"regexp"

	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"golang.org/x/net/html/charset"
)

type KvFilterConfig struct {
	// Binary data is specified in escaped format, e.g. \x00\x01
	KeyPrefix    string `toml:"key-prefix" json:"key-prefix"`
	KeyPattern   string `toml:"key-pattern" json:"key-pattern"`
	ValuePattern string `toml:"value-pattern" json:"value-pattern"`
}

func (c *KvFilterConfig) Validate() error {
	if c.KeyPrefix != "" {
		if _, err := ParseKey("escaped", c.KeyPrefix); err != nil {
			return fmt.Errorf("invalid key-prefix: %s", err.Error())
		}
	}
	if c.KeyPattern != "" {
		if _, err := regexp.Compile(c.KeyPattern); err != nil {
			return fmt.Errorf("invalid key-pattern: %s", err.Error())
		}
	}

	if c.ValuePattern != "" {
		if _, err := regexp.Compile(c.ValuePattern); err != nil {
			return fmt.Errorf("invalid value-pattern: %s", err.Error())
		}
	}

	return nil
}

type KvFilter struct {
	keyPrefix    []byte
	keyPattern   *regexp.Regexp
	valuePattern *regexp.Regexp
}

func CreateFilter(conf *KvFilterConfig) *KvFilter {
	var (
		keyPrefix    []byte
		keyPattern   *regexp.Regexp
		valuePattern *regexp.Regexp
	)

	if conf.KeyPrefix != "" {
		keyPrefix, _ = ParseKey("escaped", conf.KeyPrefix)
	}
	if conf.KeyPattern != "" {
		keyPattern = regexp.MustCompile(conf.KeyPattern)
	}
	if conf.ValuePattern != "" {
		valuePattern = regexp.MustCompile(conf.ValuePattern)
	}

	return &KvFilter{
		keyPrefix:    keyPrefix,
		keyPattern:   keyPattern,
		valuePattern: valuePattern,
	}
}

// Key of entry is expected to be in RawKV APIv2 format.
// Return error if not.
func (f *KvFilter) EventMatch(entry *cdcpb.Event_Row) (bool, error) {
	// Filter on put & delete only.
	if entry.GetOpType() != cdcpb.Event_Row_DELETE && entry.GetOpType() != cdcpb.Event_Row_PUT {
		return true, nil
	}

	userKey, err := DecodeV2Key(entry.Key)
	if err != nil {
		log.Warn("Unexpected key not in RawKV V2 format", zap.String("entry.Key", hex.EncodeToString(entry.Key)), zap.Error(err))
		return false, err
	}

	if len(f.keyPrefix) > 0 && !bytes.HasPrefix(userKey, f.keyPrefix) {
		return false, nil
	}
	if f.keyPattern != nil && !f.keyPattern.MatchString(ConvertToUTF8(userKey, "latin1")) {
		return false, nil
	}
	if entry.GetOpType() == cdcpb.Event_Row_PUT &&
		f.valuePattern != nil &&
		!f.valuePattern.MatchString(ConvertToUTF8(entry.GetValue(), "latin1")) {
		return false, nil
	}

	return true, nil
}

func ConvertToUTF8(strBytes []byte, origEncoding string) string {
	byteReader := bytes.NewReader(strBytes)
	reader, _ := charset.NewReaderLabel(origEncoding, byteReader)
	strBytes, _ = io.ReadAll(reader)
	return string(strBytes)
}
