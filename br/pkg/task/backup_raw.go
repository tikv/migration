// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package task

import (
	"context"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/log"
	"github.com/spf13/cobra"
	"github.com/tikv/migration/br/pkg/backup"
	"github.com/tikv/migration/br/pkg/checksum"
	"github.com/tikv/migration/br/pkg/glue"
	"github.com/tikv/migration/br/pkg/metautil"
	"github.com/tikv/migration/br/pkg/rtree"
	"github.com/tikv/migration/br/pkg/storage"
	"github.com/tikv/migration/br/pkg/summary"
	"github.com/tikv/migration/br/pkg/utils"
	"go.uber.org/zap"
)

const (
	flagKeyFormat     = "format"
	flagStartKey      = "start"
	flagEndKey        = "end"
	flagDstAPIVersion = "dst-api-version"
	flagSafeInterval  = "safe-interval"
)

// DefineRawBackupFlags defines common flags for the backup command.
func DefineRawBackupFlags(command *cobra.Command) {
	command.Flags().StringP(flagStartKey, "", "",
		"The start key of the backup task, key is inclusive.")

	command.Flags().StringP(flagEndKey, "", "",
		"The end key of the backup task, key is exclusive.")

	command.Flags().StringP(flagKeyFormat, "", "hex",
		"The format of start and end key. Available options: \"raw\", \"escaped\", \"hex\".")

	command.Flags().StringP(flagDstAPIVersion, "", "",
		`The encoding method of backuped SST files for destination TiKV cluster. Available options: "v1", "v1ttl", "v2".`)

	command.Flags().String(flagCompressionType, "zstd",
		"The compression algorithm of the backuped SST files. Available options: \"lz4\", \"zstd\", \"snappy\".")

	command.Flags().Bool(flagRemoveSchedulers, false,
		"disable the balance, shuffle and region-merge schedulers in PD to speed up backup.")

	command.Flags().Duration(flagSafeInterval, utils.DefaultBRSafeInterval,
		"The interval between backup-ts and current tso.")

	// This flag can impact the online cluster, so hide it in case of abuse.
	_ = command.Flags().MarkHidden(flagCompressionType)
	_ = command.Flags().MarkHidden(flagRemoveSchedulers)
	_ = command.Flags().MarkHidden(flagStartKey)
	_ = command.Flags().MarkHidden(flagEndKey)
	_ = command.Flags().MarkHidden(flagKeyFormat)
}

// CalcChecksumFromBackupMeta read the backup meta and return Checksum
func CalcChecksumAndRangeFromBackupMeta(ctx context.Context, backupMeta *backuppb.BackupMeta, curAPIVersion kvrpcpb.APIVersion) (checksum.Checksum, []*utils.KeyRange) {
	fileChecksum := checksum.Checksum{}
	keyRanges := make([]*utils.KeyRange, 0, len(backupMeta.Files))
	for _, file := range backupMeta.Files {
		fileChecksum.Update(file.Crc64Xor, file.TotalKvs, file.TotalBytes)
		keyRange := utils.ConvertBackupConfigKeyRange(file.StartKey, file.EndKey, backupMeta.ApiVersion, curAPIVersion)
		keyRanges = append(keyRanges, keyRange)
	}
	return fileChecksum, keyRanges
}

// RunBackupRaw starts a backup task inside the current goroutine.
func RunBackupRaw(c context.Context, g glue.Glue, cmdName string, cfg *RawKvConfig) error {
	cfg.adjust()

	defer summary.Summary(cmdName)
	ctx, cancel := context.WithCancel(c)
	defer cancel()

	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("task.RunBackupRaw", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	u, err := storage.ParseBackend(cfg.Storage, &cfg.BackendOptions)
	if err != nil {
		return errors.Trace(err)
	}
	mgr, err := NewMgr(ctx, g, cfg.PD, cfg.TLS, GetKeepalive(&cfg.Config), cfg.CheckRequirements)
	if err != nil {
		return errors.Trace(err)
	}
	defer mgr.Close()

	client, err := backup.NewBackupClient(ctx, mgr, mgr.GetTLSConfig())
	if err != nil {
		return errors.Trace(err)
	}
	curAPIVersion := client.GetCurAPIVersion()
	cfg.adjustBackupRange(curAPIVersion)
	if len(cfg.DstAPIVersion) == 0 { // if no DstAPIVersion is specified, backup to same api-version.
		cfg.DstAPIVersion = curAPIVersion.String()
	}
	dstAPIVersion := kvrpcpb.APIVersion(kvrpcpb.APIVersion_value[cfg.DstAPIVersion])
	if !CheckBackupAPIVersion(curAPIVersion, dstAPIVersion) {
		return errors.Errorf("Unsupported backup api version, cur:%s, dst:%s",
			curAPIVersion.String(), cfg.DstAPIVersion)
	}
	opts := storage.ExternalStorageOptions{
		NoCredentials:   cfg.NoCreds,
		SendCredentials: cfg.SendCreds,
	}
	if err = client.SetStorage(ctx, u, &opts); err != nil {
		return errors.Trace(err)
	}
	client.SetGCTTL(cfg.GCTTL)
	if curAPIVersion == kvrpcpb.APIVersion_V2 {
		// set safepoint to avoid the logical deletion data to gc.
		backupTs, err := client.UpdateBRGCSafePoint(ctx, cfg.SafeInterval)
		if err != nil {
			return errors.Trace(err)
		}
		g.Record("BackupTS", backupTs)
	}

	backupRange := rtree.Range{StartKey: cfg.StartKey, EndKey: cfg.EndKey}

	if cfg.RemoveSchedulers {
		restore, e := mgr.RemoveSchedulers(ctx)
		defer func() {
			if ctx.Err() != nil {
				log.Warn("context canceled, try shutdown")
				ctx = context.Background()
			}
			if restoreE := restore(ctx); restoreE != nil {
				log.Warn("failed to restore removed schedulers, you may need to restore them manually", zap.Error(restoreE))
			}
		}()
		if e != nil {
			return errors.Trace(err)
		}
	}

	brVersion := g.GetVersion()
	clusterVersion, err := mgr.GetClusterVersion(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	// The number of regions need to backup
	approximateRegions, err := mgr.GetRegionCount(ctx, backupRange.StartKey, backupRange.EndKey)
	if err != nil {
		return errors.Trace(err)
	}

	summary.CollectInt("backup total regions", approximateRegions)

	// Backup
	// Redirect to log if there is no log file to avoid unreadable output.
	updateCh := g.StartProgress(
		ctx, cmdName, int64(approximateRegions), !cfg.LogProgress)

	progressCallBack := func(unit backup.ProgressUnit) {
		if unit == backup.RangeUnit {
			return
		}
		updateCh.Inc()
	}
	req := backuppb.BackupRequest{
		ClusterId:        client.GetClusterID(),
		StartVersion:     0,
		EndVersion:       0,
		RateLimit:        cfg.RateLimit,
		Concurrency:      cfg.Concurrency,
		IsRawKv:          true,
		Cf:               "default",
		DstApiVersion:    dstAPIVersion,
		CompressionType:  cfg.CompressionType,
		CompressionLevel: cfg.CompressionLevel,
		CipherInfo:       &cfg.CipherInfo,
	}
	metaWriter := metautil.NewMetaWriter(client.GetStorage(), metautil.MetaFileSize, false, &cfg.CipherInfo)
	metaWriter.StartWriteMetasAsync(ctx, metautil.AppendDataFile)
	err = client.BackupRange(ctx, backupRange.StartKey, backupRange.EndKey, req, metaWriter, progressCallBack)
	if err != nil {
		return errors.Trace(err)
	}
	// Backup has finished
	updateCh.Close()
	// backup meta range should in DstAPIVersion format
	metaRange := utils.ConvertBackupConfigKeyRange(cfg.StartKey, cfg.EndKey, curAPIVersion, dstAPIVersion)
	if metaRange == nil {
		return errors.Errorf("fail to convert key. curAPIVer:%d, dstAPIVer:%d", curAPIVersion, dstAPIVersion)
	}
	rawRanges := []*backuppb.RawRange{{StartKey: metaRange.Start, EndKey: metaRange.End, Cf: "default"}}
	metaWriter.Update(func(m *backuppb.BackupMeta) {
		m.StartVersion = req.StartVersion
		m.EndVersion = req.EndVersion
		m.IsRawKv = req.IsRawKv
		m.RawRanges = rawRanges
		m.ClusterId = req.ClusterId
		m.ClusterVersion = clusterVersion
		m.BrVersion = brVersion
		m.ApiVersion = dstAPIVersion
	})
	err = metaWriter.FinishWriteMetas(ctx, metautil.AppendDataFile)
	if err != nil {
		return errors.Trace(err)
	}

	err = metaWriter.FlushBackupMeta(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	g.Record(summary.BackupDataSize, metaWriter.ArchiveSize())

	if cfg.Checksum {
		_, _, backupMeta, err := ReadBackupMeta(ctx, metautil.MetaFile, &cfg.Config)
		if err != nil {
			log.Error("fail to read backup meta", zap.Error(err))
			return errors.Trace(err)
		}
		fileChecksum, keyRanges := CalcChecksumAndRangeFromBackupMeta(ctx, backupMeta, curAPIVersion)
		checksumMethod := checksum.StorageChecksumCommand
		if curAPIVersion.String() != cfg.DstAPIVersion {
			checksumMethod = checksum.StorageScanCommand
		}

		executor := checksum.NewExecutor(keyRanges, cfg.PD, mgr.GetPDClient(), curAPIVersion,
			cfg.ChecksumConcurrency)
		err = checksum.Run(ctx, cmdName, executor,
			checksumMethod, fileChecksum)
		if err != nil {
			return errors.Trace(err)
		}
	}

	// Set task summary to success status.
	summary.SetSuccessStatus(true)
	return nil
}
