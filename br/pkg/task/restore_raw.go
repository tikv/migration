// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package task

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/spf13/cobra"
	berrors "github.com/tikv/migration/br/pkg/errors"
	"github.com/tikv/migration/br/pkg/glue"
	"github.com/tikv/migration/br/pkg/metautil"
	"github.com/tikv/migration/br/pkg/restore"
	"github.com/tikv/migration/br/pkg/summary"
)

// DefineRawRestoreFlags defines common flags for the backup command.
func DefineRawRestoreFlags(command *cobra.Command) {
	command.Flags().StringP(flagKeyFormat, "", "hex", "start/end key format, support raw|escaped|hex")
	command.Flags().StringP(flagStartKey, "", "", "restore raw kv start key, key is inclusive")
	command.Flags().StringP(flagEndKey, "", "", "restore raw kv end key, key is exclusive")
	command.Flags().StringP(flagDstAPIVersion, "", "",
		"The encoding method of backuped SST files for destination TiKV cluster, default to the source TiKV cluster. Available options: \"v1\", \"v1ttl\", \"v2\".")

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
	// TODO: How to show progress?
	updateCh := g.StartProgress(
		ctx,
		"Raw Restore",
		// Split/Scatter + Download/Ingest
		int64(len(ranges)+len(files)),
		!cfg.LogProgress)

	// RawKV restore does not need to rewrite keys.
	rewrite := &restore.RewriteRules{}
	err = restore.SplitRanges(ctx, client, ranges, rewrite, updateCh, true)
	if err != nil {
		return errors.Trace(err)
	}

	restoreSchedulers, err := restorePreWork(ctx, client, mgr)
	if err != nil {
		return errors.Trace(err)
	}
	defer restorePostWork(ctx, client, restoreSchedulers)

	err = client.RestoreRaw(ctx, cfg.StartKey, cfg.EndKey, files, updateCh)
	if err != nil {
		return errors.Trace(err)
	}

	// Restore has finished.
	updateCh.Close()

	// Set task summary to success status.
	summary.SetSuccessStatus(true)
	return nil
}
