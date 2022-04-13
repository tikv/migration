// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package task

import (
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/spf13/pflag"
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
