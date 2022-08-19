// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package task

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/log"
	"github.com/spf13/cobra"
	"github.com/tikv/client-go/v2/rawkv"
	"github.com/tikv/migration/br/pkg/checksum"
	berrors "github.com/tikv/migration/br/pkg/errors"
	"github.com/tikv/migration/br/pkg/glue"
	"github.com/tikv/migration/br/pkg/metautil"
	"github.com/tikv/migration/br/pkg/restore"
	"github.com/tikv/migration/br/pkg/summary"
	"github.com/tikv/migration/br/pkg/utils"
)

// DefineRawRestoreFlags defines common flags for the backup command.
func DefineRawRestoreFlags(command *cobra.Command) {
	command.Flags().StringP(flagKeyFormat, "", "hex", "start/end key format, support raw|escaped|hex")
	command.Flags().StringP(flagStartKey, "", "", "restore raw kv start key, key is inclusive")
	command.Flags().StringP(flagEndKey, "", "", "restore raw kv end key, key is exclusive")
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
	if client.GetAPIVersion() != backupMeta.ApiVersion {
		return errors.Errorf("Unsupported backup api version, backup meta: %s, dst:%s",
			backupMeta.ApiVersion.String(), client.GetAPIVersion().String())
	}
	// for restore, dst and cur are the same.
	cfg.DstAPIVersion = client.GetAPIVersion().String()
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
	needEncodeKey := (cfg.DstAPIVersion == kvrpcpb.APIVersion_V2.String())
	err = restore.SplitRanges(ctx, client, ranges, nil, updateCh, true, needEncodeKey)
	if err != nil {
		return errors.Trace(err)
	}

	restoreSchedulers, err := restorePreWork(ctx, client, mgr)
	if err != nil {
		return errors.Trace(err)
	}
	defer restorePostWork(ctx, client, restoreSchedulers)

	// raw key without encoding
	keyRanges := make([]*utils.KeyRange, 0, len(files))
	for _, file := range files {
		keyRanges = append(keyRanges, &utils.KeyRange{
			Start: file.StartKey,
			End:   file.EndKey,
		})
	}
	if needEncodeKey {
		for _, file := range files {
			keyRange := utils.EncodeKeyRange(file.StartKey, file.EndKey)
			file.StartKey = keyRange.Start
			file.EndKey = keyRange.End
		}
	}

	err = client.RestoreRaw(ctx, cfg.StartKey, cfg.EndKey, files, updateCh)
	if err != nil {
		return errors.Trace(err)
	}

	// Restore has finished.
	updateCh.Close()

	if cfg.Checksum {
		finalChecksum := rawkv.RawChecksum{}
		for _, file := range files {
			checksum.UpdateChecksum(&finalChecksum, file.Crc64Xor, file.TotalKvs, file.TotalBytes)
		}
		executor, err := checksum.NewExecutor(ctx, keyRanges, cfg.PD,
			backupMeta.ApiVersion, cfg.ChecksumConcurrency)
		if err != nil {
			return errors.Trace(err)
		}
		defer executor.Close()
		err = checksum.Run(ctx, cmdName, executor,
			checksum.StorageChecksumCommand, finalChecksum)
		if err != nil {
			return errors.Trace(err)
		}
	}

	// Set task summary to success status.
	summary.SetSuccessStatus(true)
	return nil
}
