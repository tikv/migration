// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package task

import (
	"strconv"
	"time"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/spf13/pflag"
	"github.com/tikv/client-go/v2/oracle"
	berrors "github.com/tikv/migration/br/pkg/errors"
	"github.com/tikv/migration/br/pkg/utils"
)

const (
	flagBackupTimeago    = "timeago"
	flagBackupTS         = "backupts"
	flagLastBackupTS     = "lastbackupts"
	flagCompressionType  = "compression"
	flagCompressionLevel = "compression-level"
	flagRemoveSchedulers = "remove-schedulers"
	flagIgnoreStats      = "ignore-stats"
	flagUseBackupMetaV2  = "use-backupmeta-v2"

	flagGCTTL = "gcttl"
)

// CompressionConfig is the configuration for sst file compression.
type CompressionConfig struct {
	CompressionType  backuppb.CompressionType `json:"compression-type" toml:"compression-type"`
	CompressionLevel int32                    `json:"compression-level" toml:"compression-level"`
}

// BackupConfig is the configuration specific for backup tasks.
type BackupConfig struct {
	Config

	TimeAgo          time.Duration `json:"time-ago" toml:"time-ago"`
	BackupTS         uint64        `json:"backup-ts" toml:"backup-ts"`
	LastBackupTS     uint64        `json:"last-backup-ts" toml:"last-backup-ts"`
	GCTTL            int64         `json:"gc-ttl" toml:"gc-ttl"`
	RemoveSchedulers bool          `json:"remove-schedulers" toml:"remove-schedulers"`
	IgnoreStats      bool          `json:"ignore-stats" toml:"ignore-stats"`
	UseBackupMetaV2  bool          `json:"use-backupmeta-v2"`
	CompressionConfig
}

// DefineBackupFlags defines common flags for the backup command.
func DefineBackupFlags(flags *pflag.FlagSet) {
	flags.Duration(
		flagBackupTimeago, 0,
		"The history version of the backup task, e.g. 1m, 1h. Do not exceed GCSafePoint")

	// TODO: remove experimental tag if it's stable
	flags.Uint64(flagLastBackupTS, 0, "(experimental) the last time backup ts,"+
		" use for incremental backup, support TSO only")
	flags.String(flagBackupTS, "", "the backup ts support TSO or datetime,"+
		" e.g. '400036290571534337', '2018-05-11 01:42:23'")
	flags.Int64(flagGCTTL, utils.DefaultBRGCSafePointTTL, "the TTL (in seconds) that PD holds for BR's GC safepoint")
	flags.String(flagCompressionType, "zstd",
		"backup sst file compression algorithm, value can be one of 'lz4|zstd|snappy'")
	flags.Int32(flagCompressionLevel, 0, "compression level used for sst file compression")

	flags.Bool(flagRemoveSchedulers, false,
		"disable the balance, shuffle and region-merge schedulers in PD to speed up backup")
	// This flag can impact the online cluster, so hide it in case of abuse.
	_ = flags.MarkHidden(flagRemoveSchedulers)

	// Disable stats by default. because of
	// 1. DumpStatsToJson is not stable
	// 2. It increases memory usage and might cause BR OOM.
	// TODO: we need a better way to backup/restore stats.
	flags.Bool(flagIgnoreStats, true, "ignore backup stats, used for test")
	// This flag is used for test. we should backup stats all the time.
	_ = flags.MarkHidden(flagIgnoreStats)

	flags.Bool(flagUseBackupMetaV2, false,
		"use backup meta v2 to store meta info")
	// This flag will change the structure of backupmeta.
	// we must make sure the old three version of br can parse the v2 meta to keep compatibility.
	// so this flag should set to false for three version by default.
	// for example:
	// if we put this feature in v4.0.14, then v4.0.14 br can parse v2 meta
	// but will generate v1 meta due to this flag is false. the behaviour is as same as v4.0.15, v4.0.16.
	// finally v4.0.17 will set this flag to true, and generate v2 meta.
	_ = flags.MarkHidden(flagUseBackupMetaV2)
}

// ParseFromFlags parses the backup-related flags from the flag set.
func (cfg *BackupConfig) ParseFromFlags(flags *pflag.FlagSet) error {
	timeAgo, err := flags.GetDuration(flagBackupTimeago)
	if err != nil {
		return errors.Trace(err)
	}
	if timeAgo < 0 {
		return errors.Annotate(berrors.ErrInvalidArgument, "negative timeago is not allowed")
	}
	cfg.TimeAgo = timeAgo
	cfg.LastBackupTS, err = flags.GetUint64(flagLastBackupTS)
	if err != nil {
		return errors.Trace(err)
	}
	backupTS, err := flags.GetString(flagBackupTS)
	if err != nil {
		return errors.Trace(err)
	}
	cfg.BackupTS, err = parseTSString(backupTS)
	if err != nil {
		return errors.Trace(err)
	}
	gcTTL, err := flags.GetInt64(flagGCTTL)
	if err != nil {
		return errors.Trace(err)
	}
	cfg.GCTTL = gcTTL

	compressionCfg, err := parseCompressionFlags(flags)
	if err != nil {
		return errors.Trace(err)
	}
	cfg.CompressionConfig = *compressionCfg

	if err = cfg.Config.ParseFromFlags(flags); err != nil {
		return errors.Trace(err)
	}
	cfg.RemoveSchedulers, err = flags.GetBool(flagRemoveSchedulers)
	if err != nil {
		return errors.Trace(err)
	}
	cfg.IgnoreStats, err = flags.GetBool(flagIgnoreStats)
	if err != nil {
		return errors.Trace(err)
	}
	cfg.UseBackupMetaV2, err = flags.GetBool(flagUseBackupMetaV2)
	return errors.Trace(err)
}

// parseCompressionFlags parses the backup-related flags from the flag set.
func parseCompressionFlags(flags *pflag.FlagSet) (*CompressionConfig, error) {
	compressionStr, err := flags.GetString(flagCompressionType)
	if err != nil {
		return nil, errors.Trace(err)
	}
	compressionType, err := parseCompressionType(compressionStr)
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

// parseTSString port from tidb setSnapshotTS.
func parseTSString(ts string) (uint64, error) {
	if len(ts) == 0 {
		return 0, nil
	}
	if tso, err := strconv.ParseUint(ts, 10, 64); err == nil {
		return tso, nil
	}

	loc := time.Local
	sc := &stmtctx.StatementContext{
		TimeZone: loc,
	}
	t, err := types.ParseTime(sc, ts, mysql.TypeTimestamp, types.MaxFsp)
	if err != nil {
		return 0, errors.Trace(err)
	}
	t1, err := t.GoTime(loc)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return oracle.GoTimeToTS(t1), nil
}

func parseCompressionType(s string) (backuppb.CompressionType, error) {
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
