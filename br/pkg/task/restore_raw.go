// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package task

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/log"
	"github.com/spf13/cobra"
	"github.com/tikv/migration/br/pkg/checksum"
	berrors "github.com/tikv/migration/br/pkg/errors"
	"github.com/tikv/migration/br/pkg/glue"
	"github.com/tikv/migration/br/pkg/metautil"
	"github.com/tikv/migration/br/pkg/restore"
	"github.com/tikv/migration/br/pkg/summary"
	"github.com/tikv/pd/pkg/codec"
)

// DefineRawRestoreFlags defines common flags for the backup command.
func DefineRawRestoreFlags(command *cobra.Command) {
	command.Flags().StringP(flagKeyFormat, "", "hex", "start/end key format, support raw|escaped|hex")
	command.Flags().StringP(flagStartKey, "", "", "restore raw kv start key, key is inclusive")
	command.Flags().StringP(flagEndKey, "", "", "restore raw kv end key, key is exclusive")
	_ = command.Flags().MarkHidden(flagKeyFormat)
	_ = command.Flags().MarkHidden(flagStartKey)
	_ = command.Flags().MarkHidden(flagEndKey)
	DefineRestoreCommonFlags(command.PersistentFlags())
}

// RunRestoreRaw starts a raw kv restore task inside the current goroutine.
func RunRestoreRaw(c context.Context, g glue.Glue, cmdName string, cfg *RestoreRawConfig) (err error) {
	cfg.adjust()

	defer summary.Summary(cmdName)
	ctx, cancel := context.WithCancel(c)
	defer cancel()

	mgr, err := NewMgr(ctx, g, cfg.PD, cfg.TLS, GetKeepalive(&cfg.Config), cfg.CheckRequirements)
	if err != nil {
		return errors.Trace(err)
	}
	defer mgr.Close()

	keepaliveCfg := GetKeepalive(&cfg.Config)
	// sometimes we have pooled the connections.
	// sending heartbeats in idle times is useful.
	keepaliveCfg.PermitWithoutStream = true
	client, err := restore.NewRestoreClient(g, mgr.GetPDClient(), mgr.GetTLSConfig(), keepaliveCfg, true)
	if err != nil {
		return errors.Trace(err)
	}
	defer client.Close()
	client.SetRateLimit(cfg.RateLimit)
	client.SetCrypter(&cfg.CipherInfo)
	client.SetConcurrency(uint(cfg.Concurrency))
	if cfg.Online {
		client.EnableOnline()
	}
	client.SetSwitchModeInterval(cfg.SwitchModeInterval)

	u, s, backupMeta, err := ReadBackupMeta(ctx, metautil.MetaFile, &cfg.Config)
	if err != nil {
		return errors.Trace(err)
	}
	// for restore, dst and cur are the same.
	cfg.DstAPIVersion = backupMeta.ApiVersion.String()
	cfg.adjustBackupRange(backupMeta.ApiVersion)
	reader := metautil.NewMetaReader(backupMeta, s, &cfg.CipherInfo)
	if err = client.InitBackupMeta(c, backupMeta, u, s, reader); err != nil {
		return errors.Trace(err)
	}

	if !client.IsRawKvMode() {
		return errors.Annotate(berrors.ErrRestoreModeMismatch, "cannot do raw restore from transactional data")
	}

	files, err := client.GetFilesInRawRange(cfg.StartKey, cfg.EndKey, "default")
	if err != nil {
		return errors.Trace(err)
	}
	archiveSize := reader.ArchiveSize(ctx, files)
	g.Record(summary.RestoreDataSize, archiveSize)

	if len(files) == 0 {
		log.Info("all files are filtered out from the backup archive, nothing to restore")
		return nil
	}
	summary.CollectInt("restore files", len(files))

	ranges, _, err := restore.MergeFileRanges(
		files, cfg.MergeSmallRegionKeyCount, cfg.MergeSmallRegionKeyCount)
	if err != nil {
		return errors.Trace(err)
	}

	// Redirect to log if there is no log file to avoid unreadable output.
	updateCh := g.StartProgress(
		ctx,
		"Raw Restore",
		// Split/Scatter + Download/Ingest.
		// Regard split region as one step as it finish quickly compared to ingest.
		int64(1+len(files)),
		!cfg.LogProgress)

	// RawKV restore does not need to rewrite keys.
	rewrite := &restore.RewriteRules{}
	needEncodeKey := (cfg.DstAPIVersion == kvrpcpb.APIVersion_V2.String())
	err = restore.SplitRanges(ctx, client, ranges, rewrite, updateCh, true, needEncodeKey)
	if err != nil {
		return errors.Trace(err)
	}

	restoreSchedulers, err := restorePreWork(ctx, client, mgr)
	if err != nil {
		return errors.Trace(err)
	}
	defer restorePostWork(ctx, client, restoreSchedulers)

	if needEncodeKey {
		for _, file := range files {
			file.StartKey = codec.EncodeBytes(file.StartKey)
			file.EndKey = codec.EncodeBytes(file.EndKey)
		}
	}

	err = client.RestoreRaw(ctx, cfg.StartKey, cfg.EndKey, files, updateCh)
	if err != nil {
		return errors.Trace(err)
	}

	// Restore has finished.
	updateCh.Close()

	finalChecksum := checksum.Checksum{}
	for _, file := range files {
		finalChecksum.Update(file.Crc64Xor, file.TotalKvs, file.TotalBytes)
	}

	if cfg.Checksum {
		executor := checksum.NewExecutor(cfg.StartKey, cfg.EndKey,
			backupMeta.ApiVersion, mgr.GetPDClient(), cfg.ChecksumConcurrency)
		err = checksum.RunChecksumWithRetry(ctx, cmdName, int64(len(files)), executor,
			checksum.StorageChecksumCommand, finalChecksum)
		if err != nil {
			return errors.Trace(err)
		}
	}

	// Set task summary to success status.
	summary.SetSuccessStatus(true)
	return nil
}
