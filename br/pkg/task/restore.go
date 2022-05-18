// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package task

import (
	"context"
	"time"

	"github.com/pingcap/log"
	"github.com/spf13/pflag"
	"github.com/tikv/migration/br/pkg/conn"
	"github.com/tikv/migration/br/pkg/pdutil"
	"github.com/tikv/migration/br/pkg/restore"
	"go.uber.org/zap"
)

const (
	flagOnline   = "online"
	flagNoSchema = "no-schema"

	// FlagMergeRegionSizeBytes is the flag name of merge small regions by size
	FlagMergeRegionSizeBytes = "merge-region-size-bytes"
	// FlagMergeRegionKeyCount is the flag name of merge small regions by key count
	FlagMergeRegionKeyCount = "merge-region-key-count"
	// FlagPDConcurrency controls concurrency pd-relative operations like split & scatter.
	FlagPDConcurrency = "pd-concurrency"
	// FlagBatchFlushInterval controls after how long the restore batch would be auto sended.
	FlagBatchFlushInterval = "batch-flush-interval"

	defaultRestoreConcurrency = 128
	defaultPDConcurrency      = 1
	defaultBatchFlushInterval = 16 * time.Second
)

// DefineRestoreCommonFlags defines common flags for the restore command.
func DefineRestoreCommonFlags(flags *pflag.FlagSet) {
	// TODO remove experimental tag if it's stable
	flags.Bool(flagOnline, false, "(experimental) Whether online when restore")

	flags.Uint64(FlagMergeRegionSizeBytes, restore.DefaultMergeRegionSizeBytes,
		"the threshold of merging small regions (Default 96MB, region split size)")
	flags.Uint64(FlagMergeRegionKeyCount, restore.DefaultMergeRegionKeyCount,
		"the threshold of merging small regions (Default 960_000, region split key count)")
	flags.Uint(FlagPDConcurrency, defaultPDConcurrency,
		"concurrency pd-relative operations like split & scatter.")
	flags.Duration(FlagBatchFlushInterval, defaultBatchFlushInterval,
		"after how long a restore batch would be auto sended.")
	_ = flags.MarkHidden(flagOnline)
	_ = flags.MarkHidden(FlagMergeRegionSizeBytes)
	_ = flags.MarkHidden(FlagMergeRegionKeyCount)
	_ = flags.MarkHidden(FlagPDConcurrency)
	_ = flags.MarkHidden(FlagBatchFlushInterval)
}

// DefineRestoreFlags defines common flags for the restore tidb command.
func DefineRestoreFlags(flags *pflag.FlagSet) {
	flags.Bool(flagNoSchema, false, "skip creating schemas and tables, reuse existing empty ones")
	// Do not expose this flag
	_ = flags.MarkHidden(flagNoSchema)

	DefineRestoreCommonFlags(flags)
}

// restorePreWork executes some prepare work before restore.
// TODO make this function returns a restore post work.
func restorePreWork(ctx context.Context, client *restore.Client, mgr *conn.Mgr) (pdutil.UndoFunc, error) {
	if client.IsOnline() {
		return pdutil.Nop, nil
	}

	// Switch TiKV cluster to import mode (adjust rocksdb configuration).
	client.SwitchToImportMode(ctx)

	return mgr.RemoveSchedulers(ctx)
}

// restorePostWork executes some post work after restore.
// TODO: aggregate all lifetime manage methods into batcher's context manager field.
func restorePostWork(
	ctx context.Context, client *restore.Client, restoreSchedulers pdutil.UndoFunc,
) {
	if ctx.Err() != nil {
		log.Warn("context canceled, try shutdown")
		ctx = context.Background()
	}
	if client.IsOnline() {
		return
	}
	if err := client.SwitchToNormalMode(ctx); err != nil {
		log.Warn("fail to switch to normal mode", zap.Error(err))
	}
	if err := restoreSchedulers(ctx); err != nil {
		log.Warn("failed to restore PD schedulers", zap.Error(err))
	}
}
