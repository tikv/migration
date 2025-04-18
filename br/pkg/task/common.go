// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package task

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/hex"
	"net/url"
	"os"
	"path"
	"strings"
	"time"

	gcs "cloud.google.com/go/storage"
	"github.com/docker/go-units"
	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/log"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/tikv/migration/br/pkg/conn"
	berrors "github.com/tikv/migration/br/pkg/errors"
	"github.com/tikv/migration/br/pkg/feature"
	"github.com/tikv/migration/br/pkg/glue"
	"github.com/tikv/migration/br/pkg/metautil"
	"github.com/tikv/migration/br/pkg/restore"
	"github.com/tikv/migration/br/pkg/storage"
	"github.com/tikv/migration/br/pkg/utils"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
	"google.golang.org/grpc/keepalive"
)

const (
	// flagSendCreds specify whether to send credentials to tikv
	flagSendCreds = "send-credentials-to-tikv"
	// No credentials specifies that cloud credentials should not be loaded
	flagNoCreds = "no-credentials"
	// flagStorage is the name of storage flag.
	flagStorage = "storage"
	// flagPD is the name of PD url flag.
	flagPD = "pd"

	flagChecksumConcurrency = "checksum-concurrency"
	flagRateLimit           = "ratelimit"
	flagRateLimitUnit       = "ratelimit-unit"
	flagConcurrency         = "concurrency"
	flagChecksum            = "checksum"
	flagCheckRequirement    = "check-requirements"
	flagSwitchModeInterval  = "switch-mode-interval"
	// flagGrpcKeepaliveTime is the interval of pinging the server.
	flagGrpcKeepaliveTime = "grpc-keepalive-time"
	// flagGrpcKeepaliveTimeout is the max time a grpc conn can keep idel before killed.
	flagGrpcKeepaliveTimeout = "grpc-keepalive-timeout"
	// flagGrpcMaxRecvMsgSize is the max allowed size of a message received from tikv.
	flagGrpcMaxRecvMsgSize = "grpc-max-recv-msg-size"
	// flagEnableOpenTracing is whether to enable opentracing
	flagEnableOpenTracing  = "enable-opentracing"
	flagSkipCheckPath      = "skip-check-path"
	flagSplitRegionMaxKeys = "split-region-max-keys"

	defaultSwitchInterval       = 5 * time.Minute
	defaultGRPCKeepaliveTime    = 10 * time.Second
	defaultGRPCKeepaliveTimeout = 3 * time.Second
	defaultGRPCMaxRecvMsgSize   = 32 * 1024 * 1024 // 32MB
	defaultChecksumConcurrency  = 512
	defaultSplitRegionMaxKeys   = 1024

	flagCipherType    = "crypter.method"
	flagCipherKey     = "crypter.key"
	flagCipherKeyFile = "crypter.key-file"

	unlimited           = 0
	crypterAES128KeyLen = 16
	crypterAES192KeyLen = 24
	crypterAES256KeyLen = 32
)

// DefineCommonFlags defines the flags common to all BRIE commands.
func DefineCommonFlags(flags *pflag.FlagSet) {
	flags.BoolP(flagSendCreds, "c", true, "Whether send credentials to tikv")
	flags.StringP(flagStorage, "s", "", `specify the path where backup storage, eg, "local:///home/backup_data"`)
	flags.StringSliceP(flagPD, "u", []string{"127.0.0.1:2379"}, "PD address")
	utils.DefineTLSFlags(flags)
	flags.Uint(flagChecksumConcurrency, defaultChecksumConcurrency, "The concurrency of table checksumming")
	_ = flags.MarkHidden(flagSendCreds)
	_ = flags.MarkHidden(flagChecksumConcurrency)

	flags.Uint64(flagRateLimit, unlimited, "The rate limit of the task, MB/s per node")
	flags.Bool(flagChecksum, false, "Run checksum at end of task")
	// Default concurrency is different for backup and restore.
	// Leave it 0 and let them adjust the value.
	flags.Uint32(flagConcurrency, 0, "The size of thread pool on each node that executes the task")
	// It may confuse users , so just hide it.
	_ = flags.MarkHidden(flagConcurrency)

	flags.Uint64(flagRateLimitUnit, units.MiB, "The unit of rate limit")
	_ = flags.MarkHidden(flagRateLimitUnit)

	flags.Bool(flagCheckRequirement, true,
		"Whether start version check before execute command")
	_ = flags.MarkHidden(flagCheckRequirement)
	flags.Duration(flagSwitchModeInterval, defaultSwitchInterval, "maintain import mode on TiKV during restore")
	_ = flags.MarkHidden(flagSwitchModeInterval)
	flags.Duration(flagGrpcKeepaliveTime, defaultGRPCKeepaliveTime,
		"the interval of pinging gRPC peer, must keep the same value with TiKV and PD")
	flags.Duration(flagGrpcKeepaliveTimeout, defaultGRPCKeepaliveTimeout,
		"the max time a gRPC connection can keep idle before killed, must keep the same value with TiKV and PD")
	flags.Uint(flagGrpcMaxRecvMsgSize, defaultGRPCMaxRecvMsgSize,
		"the max allowed size of a message received from TiKV")
	_ = flags.MarkHidden(flagGrpcKeepaliveTime)
	_ = flags.MarkHidden(flagGrpcKeepaliveTimeout)
	_ = flags.MarkHidden(flagGrpcMaxRecvMsgSize)

	flags.Bool(flagEnableOpenTracing, false,
		"Set whether to enable opentracing during the backup/restore process")
	_ = flags.MarkHidden(flagEnableOpenTracing)
	flags.BoolP(flagNoCreds, "", false, "Don't load credentials")
	_ = flags.MarkHidden(flagNoCreds)
	flags.BoolP(flagSkipCheckPath, "", false, "Skip path verification")
	_ = flags.MarkHidden(flagSkipCheckPath)
	flags.Uint(flagSplitRegionMaxKeys, defaultSplitRegionMaxKeys,
		"The max number of keys in a single split region request")
	_ = flags.MarkHidden(flagSplitRegionMaxKeys)

	flags.String(flagCipherType, "plaintext", "Encrypt/decrypt method, "+
		"be one of plaintext|aes128-ctr|aes192-ctr|aes256-ctr case-insensitively, "+
		"\"plaintext\" represents no encrypt/decrypt")
	flags.String(flagCipherKey, "",
		"aes-crypter key, used to encrypt/decrypt the data "+
			"by the hexadecimal string, eg: \"0123456789abcdef0123456789abcdef\"")
	flags.String(flagCipherKeyFile, "", "FilePath, its content is used as the cipher-key")
	_ = flags.MarkHidden(flagCipherType)
	_ = flags.MarkHidden(flagCipherKey)
	_ = flags.MarkHidden(flagCipherKeyFile)
	storage.DefineFlags(flags)
}

func parseCipherType(t string) (encryptionpb.EncryptionMethod, error) {
	ct := encryptionpb.EncryptionMethod_UNKNOWN
	switch t {
	case "plaintext", "PLAINTEXT":
		ct = encryptionpb.EncryptionMethod_PLAINTEXT
	case "aes128-ctr", "AES128-CTR":
		ct = encryptionpb.EncryptionMethod_AES128_CTR
	case "aes192-ctr", "AES192-CTR":
		ct = encryptionpb.EncryptionMethod_AES192_CTR
	case "aes256-ctr", "AES256-CTR":
		ct = encryptionpb.EncryptionMethod_AES256_CTR
	default:
		return ct, errors.Annotatef(berrors.ErrInvalidArgument, "invalid crypter method '%s'", t)
	}

	return ct, nil
}

func checkCipherKey(cipherKey, cipherKeyFile string) error {
	if (len(cipherKey) == 0) == (len(cipherKeyFile) == 0) {
		return errors.Annotate(berrors.ErrInvalidArgument,
			"exactly one of --crypter.key or --crypter.key-file should be provided")
	}
	return nil
}

func getCipherKeyContent(cipherKey, cipherKeyFile string) ([]byte, error) {
	if err := checkCipherKey(cipherKey, cipherKeyFile); err != nil {
		return nil, errors.Trace(err)
	}

	// if cipher-key is valid, convert the hexadecimal string to bytes
	if len(cipherKey) > 0 {
		return hex.DecodeString(cipherKey)
	}

	// convert the content(as hexadecimal string) from cipher-file to bytes
	content, err := os.ReadFile(cipherKeyFile)
	if err != nil {
		return nil, errors.Annotate(err, "failed to read cipher file")
	}

	content = bytes.TrimSuffix(content, []byte("\n"))
	return hex.DecodeString(string(content))
}

func checkCipherKeyMatch(cipher *backuppb.CipherInfo) bool {
	switch cipher.CipherType {
	case encryptionpb.EncryptionMethod_PLAINTEXT:
		return true
	case encryptionpb.EncryptionMethod_AES128_CTR:
		return len(cipher.CipherKey) == crypterAES128KeyLen
	case encryptionpb.EncryptionMethod_AES192_CTR:
		return len(cipher.CipherKey) == crypterAES192KeyLen
	case encryptionpb.EncryptionMethod_AES256_CTR:
		return len(cipher.CipherKey) == crypterAES256KeyLen
	default:
		return false
	}
}

// NewMgr creates a new mgr at the given PD address.
func NewMgr(ctx context.Context,
	g glue.Glue, pds []string,
	tlsConfig utils.TLSConfig,
	keepalive keepalive.ClientParameters,
	checkRequirements bool,
) (*conn.Mgr, error) {
	var (
		tlsConf *tls.Config
		err     error
	)
	pdAddress := strings.Join(pds, ",")
	if len(pdAddress) == 0 {
		return nil, errors.Annotate(berrors.ErrInvalidArgument, "pd address can not be empty")
	}

	securityOption := pd.SecurityOption{}
	if tlsConfig.IsEnabled() {
		securityOption.CAPath = tlsConfig.CA
		securityOption.CertPath = tlsConfig.Cert
		securityOption.KeyPath = tlsConfig.Key
		tlsConf, err = tlsConfig.ToTLSConfig()
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	// Is it necessary to remove `StoreBehavior`?
	return conn.NewMgr(
		ctx, g, pdAddress, tlsConf, securityOption, keepalive, conn.SkipTiFlash,
		checkRequirements,
	)
}

// GetStorage gets the storage backend from the config.
func GetStorage(
	ctx context.Context,
	cfg *Config,
) (*backuppb.StorageBackend, storage.ExternalStorage, error) {
	u, err := storage.ParseBackend(cfg.Storage, &cfg.BackendOptions)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	s, err := storage.New(ctx, u, storageOpts(cfg))
	if err != nil {
		return nil, nil, errors.Annotate(err, "create storage failed")
	}
	return u, s, nil
}

func storageOpts(cfg *Config) *storage.ExternalStorageOptions {
	return &storage.ExternalStorageOptions{
		NoCredentials:   cfg.NoCreds,
		SendCredentials: cfg.SendCreds,
	}
}

// ReadBackupMeta reads the backupmeta file from the storage.
func ReadBackupMeta(
	ctx context.Context,
	fileName string,
	cfg *Config,
) (*backuppb.StorageBackend, storage.ExternalStorage, *backuppb.BackupMeta, error) {
	u, s, err := GetStorage(ctx, cfg)
	if err != nil {
		return nil, nil, nil, errors.Trace(err)
	}
	metaData, err := s.ReadFile(ctx, fileName)
	if err != nil {
		if gcsObjectNotFound(err) {
			// change gcs://bucket/abc/def to gcs://bucket/abc and read defbackupmeta
			oldPrefix := u.GetGcs().GetPrefix()
			newPrefix, file := path.Split(oldPrefix)
			newFileName := file + fileName
			u.GetGcs().Prefix = newPrefix
			s, err = storage.New(ctx, u, storageOpts(cfg))
			if err != nil {
				return nil, nil, nil, errors.Trace(err)
			}
			log.Info("retry load metadata in gcs", zap.String("newPrefix", newPrefix), zap.String("newFileName", newFileName))
			metaData, err = s.ReadFile(ctx, newFileName)
			if err != nil {
				return nil, nil, nil, errors.Trace(err)
			}
			// reset prefix for tikv download sst file correctly.
			u.GetGcs().Prefix = oldPrefix
		} else {
			return nil, nil, nil, errors.Annotate(err, "load backupmeta failed")
		}
	}

	// the prefix of backupmeta file is iv(16 bytes) if encryption method is valid
	var iv []byte
	if cfg.CipherInfo.CipherType != encryptionpb.EncryptionMethod_PLAINTEXT {
		iv = metaData[:metautil.CrypterIvLen]
	}
	decryptBackupMeta, err := metautil.Decrypt(metaData[len(iv):], &cfg.CipherInfo, iv)
	if err != nil {
		return nil, nil, nil, errors.Annotate(err, "decrypt failed with wrong key")
	}

	backupMeta := &backuppb.BackupMeta{}
	if err = proto.Unmarshal(decryptBackupMeta, backupMeta); err != nil {
		return nil, nil, nil, errors.Annotate(err,
			"parse backupmeta failed because of wrong aes cipher")
	}
	return u, s, backupMeta, nil
}

// flagToZapField checks whether this flag can be logged,
// if need to log, return its zap field. Or return a field with hidden value.
func flagToZapField(f *pflag.Flag) zap.Field {
	if f.Name == flagStorage {
		hiddenQuery, err := url.Parse(f.Value.String())
		if err != nil {
			return zap.String(f.Name, "<invalid URI>")
		}
		// hide all query here.
		hiddenQuery.RawQuery = ""
		return zap.Stringer(f.Name, hiddenQuery)
	}
	return zap.Stringer(f.Name, f.Value)
}

// LogArguments prints origin command arguments.
func LogArguments(cmd *cobra.Command) {
	flags := cmd.Flags()
	fields := make([]zap.Field, 1, flags.NFlag()+1)
	fields[0] = zap.String("__command", cmd.CommandPath())
	flags.Visit(func(f *pflag.Flag) {
		fields = append(fields, flagToZapField(f))
	})
	log.Info("arguments", fields...)
}

// GetKeepalive get the keepalive info from the config.
func GetKeepalive(cfg *Config) keepalive.ClientParameters {
	return keepalive.ClientParameters{
		Time:    cfg.GRPCKeepaliveTime,
		Timeout: cfg.GRPCKeepaliveTimeout,
	}
}

// GetSpliterConfig get the spliter config.
func GetSpliterConfig(cfg *Config) restore.SpliterConfig {
	return restore.SpliterConfig{
		GRPCMaxRecvMsgSize: int(cfg.GRPCMaxRecvMsgSize),
		SplitRegionMaxKeys: int(cfg.SplitRegionMaxKeys),
	}
}

func normalizePDURL(pd string, useTLS bool) (string, error) {
	if strings.HasPrefix(pd, "http://") {
		if useTLS {
			return "", errors.Annotate(berrors.ErrInvalidArgument, "pd url starts with http while TLS enabled")
		}
		return strings.TrimPrefix(pd, "http://"), nil
	}
	if strings.HasPrefix(pd, "https://") {
		if !useTLS {
			return "", errors.Annotate(berrors.ErrInvalidArgument, "pd url starts with https while TLS disabled")
		}
		return strings.TrimPrefix(pd, "https://"), nil
	}
	return pd, nil
}

// check whether it's a bug before #647, to solve case #1
// If the storage is set as gcs://bucket/prefix,
// the SSTs are written correctly to gcs://bucket/prefix/*.sst
// but the backupmeta is written wrongly to gcs://bucket/prefixbackupmeta.
// see details https://github.com/pingcap/br/issues/675#issuecomment-753780742
func gcsObjectNotFound(err error) bool {
	return errors.Cause(err) == gcs.ErrObjectNotExist // nolint:errorlint
}

// CheckBackupAPIVersion return false if backup api version is not supported.
func CheckBackupAPIVersion(gate *feature.Gate, storageAPIVersion, dstAPIVersion kvrpcpb.APIVersion) bool {
	// only support apiv1/v1ttl->apiv2 if apiversions are not the same.
	return storageAPIVersion == dstAPIVersion ||
		(gate.IsEnabled(feature.APIVersionConversion) && dstAPIVersion == kvrpcpb.APIVersion_V2)
}
