// Copyright 2022 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package task

import (
	"bytes"
	"strings"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/spf13/pflag"
	berrors "github.com/tikv/migration/br/pkg/errors"
	"github.com/tikv/migration/br/pkg/utils"
)

// RawKvConfig is the common config for rawkv backup and restore.
type RawKvConfig struct {
	Config

	StartKey      []byte `json:"start-key" toml:"start-key"`
	EndKey        []byte `json:"end-key" toml:"end-key"`
	DstAPIVersion string `json:"dst-api-version" toml:"dst-api-version"`
	CompressionConfig
	RemoveSchedulers bool `json:"remove-schedulers" toml:"remove-schedulers"`
}

// ParseBackupConfigFromFlags parses the backup-related flags from the flag set.
func (cfg *RawKvConfig) ParseBackupConfigFromFlags(flags *pflag.FlagSet) error {
	err := cfg.ParseFromFlags(flags, true)
	if err != nil {
		return errors.Trace(err)
	}

	compressionCfg, err := cfg.parseCompressionFlags(flags)
	if err != nil {
		return errors.Trace(err)
	}
	cfg.CompressionConfig = *compressionCfg

	cfg.RemoveSchedulers, err = flags.GetBool(flagRemoveSchedulers)
	if err != nil {
		return errors.Trace(err)
	}
	level, err := flags.GetInt32(flagCompressionLevel)
	if err != nil {
		return errors.Trace(err)
	}
	cfg.CompressionLevel = level

	return nil
}

// ParseFromFlags parses the raw kv backup&restore common flags from the flag set.
func (cfg *RawKvConfig) ParseFromFlags(flags *pflag.FlagSet, parseAPIVer bool) error {
	// parse key format.
	format, err := flags.GetString(flagKeyFormat)
	if err != nil {
		return errors.Trace(err)
	}

	// parse start key.
	start, err := flags.GetString(flagStartKey)
	if err != nil {
		return errors.Trace(err)
	}
	cfg.StartKey, err = utils.ParseKey(format, start)
	if err != nil {
		return errors.Trace(err)
	}

	// parse end key.
	end, err := flags.GetString(flagEndKey)
	if err != nil {
		return errors.Trace(err)
	}
	cfg.EndKey, err = utils.ParseKey(format, end)
	if err != nil {
		return errors.Trace(err)
	}

	// verify whether start key < end key.
	if len(cfg.StartKey) > 0 && len(cfg.EndKey) > 0 && bytes.Compare(cfg.StartKey, cfg.EndKey) >= 0 {
		return errors.Annotate(berrors.ErrBackupInvalidRange, "endKey must be greater than startKey")
	}

	if parseAPIVer {
		// parse and verify destination API version.
		if err = cfg.parseDstAPIVersion(flags); err != nil {
			return err
		}
	}

	// parse other configs.
	if err = cfg.Config.ParseFromFlags(flags); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (cfg *RawKvConfig) parseDstAPIVersion(flags *pflag.FlagSet) error {
	originalValue, err := flags.GetString(flagDstAPIVersion)
	if err != nil {
		return errors.Trace(err)
	}
	cfg.DstAPIVersion = strings.ToUpper(originalValue)
	if _, ok := kvrpcpb.APIVersion_value[cfg.DstAPIVersion]; !ok {
		supportedValues := kvrpcpb.APIVersion_V1.String() +
			", " + kvrpcpb.APIVersion_V1TTL.String() +
			", " + kvrpcpb.APIVersion_V2.String()
		return errors.Errorf("unsupported dst-api-version: %v. supported values are: %v", originalValue, supportedValues)
	}
	return nil
}

// parseCompressionFlags parses the backup-related flags from the flag set.
func (cfg *RawKvConfig) parseCompressionFlags(flags *pflag.FlagSet) (*CompressionConfig, error) {
	compressionStr, err := flags.GetString(flagCompressionType)
	if err != nil {
		return nil, errors.Trace(err)
	}
	compressionType, err := cfg.parseCompressionType(compressionStr)
	if err != nil {
		return nil, errors.Trace(err)
	}
	level, err := flags.GetInt32(flagCompressionLevel)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &CompressionConfig{
		CompressionLevel: level,
		CompressionType:  compressionType,
	}, nil
}

func (cfg *RawKvConfig) parseCompressionType(s string) (backuppb.CompressionType, error) {
	var ct backuppb.CompressionType
	switch s {
	case "lz4":
		ct = backuppb.CompressionType_LZ4
	case "snappy":
		ct = backuppb.CompressionType_SNAPPY
	case "zstd":
		ct = backuppb.CompressionType_ZSTD
	default:
		return backuppb.CompressionType_UNKNOWN, errors.Annotatef(berrors.ErrInvalidArgument, "invalid compression type '%s'", s)
	}
	return ct, nil
}

func (cfg *RawKvConfig) adjustBackupRange(curAPIVersion kvrpcpb.APIVersion) {
	if curAPIVersion == kvrpcpb.APIVersion_V2 {
		cfg.StartKey = utils.FormatAPIV2Key(cfg.StartKey, false)
		cfg.EndKey = utils.FormatAPIV2Key(cfg.EndKey, true)
	}
}
