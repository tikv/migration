// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/hex"
	"time"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/log"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/util/codec"
	"github.com/tikv/migration/br/pkg/conn"
	berrors "github.com/tikv/migration/br/pkg/errors"
	"github.com/tikv/migration/br/pkg/glue"
	"github.com/tikv/migration/br/pkg/logutil"
	"github.com/tikv/migration/br/pkg/metautil"
	"github.com/tikv/migration/br/pkg/redact"
	"github.com/tikv/migration/br/pkg/storage"
	"github.com/tikv/migration/br/pkg/summary"
	"github.com/tikv/migration/br/pkg/utils"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
)

const resetSpeedLimitRetryTimes = 3

// Client sends requests to restore files.
type Client struct {
	pdClient      pd.Client
	toolClient    SplitClient
	fileImporter  FileImporter
	workerPool    *utils.WorkerPool
	tlsConf       *tls.Config
	keepaliveConf keepalive.ClientParameters
	spliterConf   SpliterConfig

	backupMeta    *backuppb.BackupMeta
	dstAPIVersion kvrpcpb.APIVersion

	rateLimit       uint64
	isOnline        bool
	hasSpeedLimited bool // nolint:unused

	cipher             *backuppb.CipherInfo
	storage            storage.ExternalStorage
	backend            *backuppb.StorageBackend
	switchModeInterval time.Duration
	switchCh           chan struct{}
}

// NewRestoreClient returns a new RestoreClient.
func NewRestoreClient(
	pdClient pd.Client,
	tlsConf *tls.Config,
	keepaliveConf keepalive.ClientParameters,
	spliterConf SpliterConfig,
	isRawKv bool,
) (*Client, error) {
	apiVerion, err := conn.GetTiKVApiVersion(context.Background(), pdClient, tlsConf)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &Client{
		pdClient:      pdClient,
		toolClient:    NewSplitClient(pdClient, tlsConf, spliterConf, isRawKv),
		tlsConf:       tlsConf,
		keepaliveConf: keepaliveConf,
		spliterConf:   spliterConf,
		switchCh:      make(chan struct{}),
		dstAPIVersion: apiVerion,
	}, nil
}

// SetRateLimit to set rateLimit.
func (rc *Client) SetRateLimit(rateLimit uint64) {
	rc.rateLimit = rateLimit
}

func (rc *Client) SetCrypter(crypter *backuppb.CipherInfo) {
	rc.cipher = crypter
}

// SetStorage set ExternalStorage for client.
func (rc *Client) SetStorage(ctx context.Context, backend *backuppb.StorageBackend, opts *storage.ExternalStorageOptions) error {
	var err error
	rc.storage, err = storage.New(ctx, backend, opts)
	if err != nil {
		return errors.Trace(err)
	}
	rc.backend = backend
	return nil
}

// GetPDClient returns a pd client.
func (rc *Client) GetPDClient() pd.Client {
	return rc.pdClient
}

// IsOnline tells if it's a online restore.
func (rc *Client) IsOnline() bool {
	return rc.isOnline
}

// SetSwitchModeInterval set switch mode interval for client.
func (rc *Client) SetSwitchModeInterval(interval time.Duration) {
	rc.switchModeInterval = interval
}

// Close a client.
func (rc *Client) Close() {
	log.Info("Restore client closed")
}

// InitBackupMeta loads schemas from BackupMeta to initialize RestoreClient.
func (rc *Client) InitBackupMeta(
	c context.Context,
	backupMeta *backuppb.BackupMeta,
	backend *backuppb.StorageBackend,
	externalStorage storage.ExternalStorage,
	reader *metautil.MetaReader) error {
	if !backupMeta.IsRawKv {
		return errors.Errorf("backup meta for non-rawkv is unsupported")
	}
	rc.backupMeta = backupMeta

	metaClient := NewSplitClient(rc.pdClient, rc.tlsConf, rc.spliterConf, rc.backupMeta.IsRawKv)
	importCli := NewImportClient(metaClient, rc.tlsConf, rc.keepaliveConf)
	rc.fileImporter = NewFileImporter(metaClient, importCli, backend, rc.backupMeta.IsRawKv,
		rc.backupMeta.ApiVersion)
	return rc.fileImporter.CheckMultiIngestSupport(c, rc.pdClient)
}

// IsRawKvMode checks whether the backup data is in raw kv format, in which case transactional recover is forbidden.
func (rc *Client) IsRawKvMode() bool {
	return rc.backupMeta.IsRawKv
}

// GetFilesInRawRange gets all files that are in the given range or intersects with the given range.
func (rc *Client) GetFilesInRawRange(startKey []byte, endKey []byte, cf string) ([]*backuppb.File, error) {
	if !rc.IsRawKvMode() {
		return nil, errors.Annotate(berrors.ErrRestoreModeMismatch, "the backup data is not in raw kv mode")
	}

	for _, rawRange := range rc.backupMeta.RawRanges {
		// First check whether the given range is backup-ed. If not, we cannot perform the restore.
		if rawRange.Cf != cf {
			continue
		}

		if (len(rawRange.EndKey) > 0 && bytes.Compare(startKey, rawRange.EndKey) >= 0) ||
			(len(endKey) > 0 && bytes.Compare(rawRange.StartKey, endKey) >= 0) {
			// The restoring range is totally out of the current range. Skip it.
			continue
		}

		if bytes.Compare(startKey, rawRange.StartKey) < 0 ||
			utils.CompareEndKey(endKey, rawRange.EndKey) > 0 {
			// Only partial of the restoring range is in the current backup-ed range. So the given range can't be fully
			// restored.
			return nil, errors.Annotatef(berrors.ErrRestoreRangeMismatch,
				"the given range to restore [%s, %s) is not fully covered by the range that was backed up [%s, %s)",
				redact.Key(startKey), redact.Key(endKey), redact.Key(rawRange.StartKey), redact.Key(rawRange.EndKey),
			)
		}

		// We have found the range that contains the given range. Find all necessary files.
		files := make([]*backuppb.File, 0)

		for _, file := range rc.backupMeta.Files {
			if file.Cf != cf {
				continue
			}

			if len(file.EndKey) > 0 && bytes.Compare(file.EndKey, startKey) < 0 {
				// The file is before the range to be restored.
				continue
			}
			if len(endKey) > 0 && bytes.Compare(endKey, file.StartKey) <= 0 {
				// The file is after the range to be restored.
				// The specified endKey is exclusive, so when it equals to a file's startKey, the file is still skipped.
				continue
			}

			files = append(files, file)
		}

		// There should be at most one backed up range that covers the restoring range.
		return files, nil
	}

	return nil, errors.Annotate(berrors.ErrRestoreRangeMismatch, "no backup data in the range")
}

// SetConcurrency sets the concurrency of dbs tables files.
func (rc *Client) SetConcurrency(c uint) {
	rc.workerPool = utils.NewWorkerPool(c, "file")
}

// EnableOnline sets the mode of restore to online.
func (rc *Client) EnableOnline() {
	rc.isOnline = true
}

// GetTLSConfig returns the tls config.
func (rc *Client) GetTLSConfig() *tls.Config {
	return rc.tlsConf
}

// GetTS gets a new timestamp from PD.
func (rc *Client) GetTS(ctx context.Context) (uint64, error) {
	p, l, err := rc.pdClient.GetTS(ctx)
	if err != nil {
		return 0, errors.Trace(err)
	}
	restoreTS := oracle.ComposeTS(p, l)
	return restoreTS, nil
}

func (rc *Client) setSpeedLimit(ctx context.Context, rateLimit uint64) error {
	stores, err := conn.GetAllTiKVStores(ctx, rc.pdClient, conn.SkipTiFlash)
	if err != nil {
		return errors.Trace(err)
	}
	for _, store := range stores {
		err = rc.fileImporter.setDownloadSpeedLimit(ctx, store.GetId(), rateLimit)
		if err != nil {
			return errors.Trace(err)
		}
	}
	rc.hasSpeedLimited = true
	return nil
}

func (rc *Client) resetSpeedLimit(ctx context.Context) {
	if rc.hasSpeedLimited {
		var resetErr error
		for retry := 0; retry < resetSpeedLimitRetryTimes; retry++ {
			resetErr = rc.setSpeedLimit(ctx, 0)
			if resetErr != nil {
				log.Warn("failed to reset speed limit, retry it",
					zap.Int("retry time", retry), logutil.ShortError(resetErr))
				time.Sleep(time.Duration(retry+3) * time.Second)
				continue
			}
			break
		}
		if resetErr != nil {
			log.Error("failed to reset speed limit", zap.Error(resetErr))
		}
	}
	rc.hasSpeedLimited = false
}

// RestoreRaw tries to restore raw keys in the specified range.
func (rc *Client) RestoreRaw(
	ctx context.Context, startKey []byte, endKey []byte, files []*backuppb.File, updateCh glue.Progress,
) error {
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		log.Info("Restore Raw",
			logutil.Key("startKey", startKey),
			logutil.Key("endKey", endKey),
			zap.Duration("take", elapsed))
	}()
	errCh := make(chan error, len(files))
	eg, ectx := errgroup.WithContext(ctx)
	defer close(errCh)
	if rc.dstAPIVersion == kvrpcpb.APIVersion_V2 {
		startKey = codec.EncodeBytes(nil, startKey)
		endKey = codec.EncodeBytes(nil, endKey)
		for _, file := range files {
			file.StartKey = codec.EncodeBytes(nil, file.StartKey)
			file.EndKey = codec.EncodeBytes(nil, file.EndKey)
		}
	}

	err := rc.fileImporter.SetRawRange(startKey, endKey)
	if err != nil {
		return errors.Trace(err)
	}
	err = rc.setSpeedLimit(ctx, rc.rateLimit)
	if err != nil {
		return errors.Trace(err)
	}
	// TODO: Need a mechanism to set speed limit in ttl.
	defer rc.resetSpeedLimit(ctx)

	for _, file := range files {
		fileReplica := file
		rc.workerPool.ApplyOnErrorGroup(eg,
			func() error {
				defer updateCh.Inc()
				startTime := time.Now()
				err := rc.fileImporter.Import(ectx, []*backuppb.File{fileReplica}, EmptyRewriteRule(), rc.cipher)
				if err != nil {
					key := "range start:" + hex.EncodeToString(fileReplica.StartKey) +
						" end:" + hex.EncodeToString(fileReplica.EndKey)
					summary.CollectFailureUnit(key, err)
				} else {
					summary.CollectSuccessUnit("Restore file", 1, time.Since(startTime))
				}
				return err
			})
	}
	if err := eg.Wait(); err != nil {
		log.Error(
			"restore raw range failed",
			logutil.Key("startKey", startKey),
			logutil.Key("endKey", endKey),
			zap.Error(err),
		)
		return errors.Trace(err)
	}
	log.Info(
		"finish to restore raw range",
		logutil.Key("startKey", startKey),
		logutil.Key("endKey", endKey),
	)
	return nil
}

// SwitchToImportMode switch tikv cluster to import mode.
func (rc *Client) SwitchToImportMode(ctx context.Context) {
	// tikv automatically switch to normal mode in every 10 minutes
	// so we need ping tikv in less than 10 minute
	go func() {
		tick := time.NewTicker(rc.switchModeInterval)
		defer tick.Stop()

		// [important!] switch tikv mode into import at the beginning
		log.Info("switch to import mode at beginning")
		err := rc.switchTiKVMode(ctx, import_sstpb.SwitchMode_Import)
		if err != nil {
			log.Warn("switch to import mode failed", zap.Error(err))
		}

		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				log.Info("switch to import mode")
				err := rc.switchTiKVMode(ctx, import_sstpb.SwitchMode_Import)
				if err != nil {
					log.Warn("switch to import mode failed", zap.Error(err))
				}
			case <-rc.switchCh:
				log.Info("stop automatic switch to import mode")
				return
			}
		}
	}()
}

// SwitchToNormalMode switch tikv cluster to normal mode.
func (rc *Client) SwitchToNormalMode(ctx context.Context) error {
	close(rc.switchCh)
	return rc.switchTiKVMode(ctx, import_sstpb.SwitchMode_Normal)
}

func (rc *Client) switchTiKVMode(ctx context.Context, mode import_sstpb.SwitchMode) error {
	stores, err := conn.GetAllTiKVStores(ctx, rc.pdClient, conn.SkipTiFlash)
	if err != nil {
		return errors.Trace(err)
	}
	bfConf := backoff.DefaultConfig
	bfConf.MaxDelay = time.Second * 3
	for _, store := range stores {
		opt := grpc.WithInsecure()
		if rc.tlsConf != nil {
			opt = grpc.WithTransportCredentials(credentials.NewTLS(rc.tlsConf))
		}
		gctx, cancel := context.WithTimeout(ctx, time.Second*5)
		connection, err := grpc.DialContext(
			gctx,
			store.GetAddress(),
			opt,
			grpc.WithBlock(),
			grpc.FailOnNonTempDialError(true),
			grpc.WithConnectParams(grpc.ConnectParams{Backoff: bfConf}),
			// we don't need to set keepalive timeout here, because the connection lives
			// at most 5s. (shorter than minimal value for keepalive time!)
		)
		cancel()
		if err != nil {
			return errors.Trace(err)
		}
		client := import_sstpb.NewImportSSTClient(connection)
		_, err = client.SwitchMode(ctx, &import_sstpb.SwitchModeRequest{
			Mode: mode,
		})
		if err != nil {
			return errors.Trace(err)
		}
		err = connection.Close()
		if err != nil {
			log.Error("close grpc connection failed in switch mode", zap.Error(err))
			continue
		}
	}
	return nil
}

func (rc *Client) GetAPIVersion() kvrpcpb.APIVersion {
	return rc.dstAPIVersion
}
