package task

import (
	"time"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
	"github.com/pingcap/log"
	filter "github.com/pingcap/tidb-tools/pkg/table-filter"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/spf13/pflag"
	berrors "github.com/tikv/migration/br/pkg/errors"
	"github.com/tikv/migration/br/pkg/storage"
)

// Config is the common configuration for all BRIE tasks.
type Config struct {
	storage.BackendOptions

	Storage             string    `json:"storage" toml:"storage"`
	PD                  []string  `json:"pd" toml:"pd"`
	TLS                 TLSConfig `json:"tls" toml:"tls"`
	RateLimit           uint64    `json:"rate-limit" toml:"rate-limit"`
	ChecksumConcurrency uint      `json:"checksum-concurrency" toml:"checksum-concurrency"`
	Concurrency         uint32    `json:"concurrency" toml:"concurrency"`
	Checksum            bool      `json:"checksum" toml:"checksum"`
	SendCreds           bool      `json:"send-credentials-to-tikv" toml:"send-credentials-to-tikv"`
	// LogProgress is true means the progress bar is printed to the log instead of stdout.
	LogProgress bool `json:"log-progress" toml:"log-progress"`

	// CaseSensitive should not be used.
	//
	// Deprecated: This field is kept only to satisfy the cyclic dependency with TiDB. This field
	// should be removed after TiDB upgrades the BR dependency.
	CaseSensitive bool

	// NoCreds means don't try to load cloud credentials
	NoCreds bool `json:"no-credentials" toml:"no-credentials"`

	CheckRequirements bool `json:"check-requirements" toml:"check-requirements"`
	// EnableOpenTracing is whether to enable opentracing
	EnableOpenTracing bool `json:"enable-opentracing" toml:"enable-opentracing"`
	// SkipCheckPath skips verifying the path
	// deprecated
	SkipCheckPath bool `json:"skip-check-path" toml:"skip-check-path"`
	// Filter should not be used, use TableFilter instead.
	//
	// Deprecated: This field is kept only to satisfy the cyclic dependency with TiDB. This field
	// should be removed after TiDB upgrades the BR dependency.
	Filter filter.MySQLReplicationRules

	SwitchModeInterval time.Duration `json:"switch-mode-interval" toml:"switch-mode-interval"`

	// GrpcKeepaliveTime is the interval of pinging the server.
	GRPCKeepaliveTime time.Duration `json:"grpc-keepalive-time" toml:"grpc-keepalive-time"`
	// GrpcKeepaliveTimeout is the max time a grpc conn can keep idel before killed.
	GRPCKeepaliveTimeout time.Duration `json:"grpc-keepalive-timeout" toml:"grpc-keepalive-timeout"`

	CipherInfo backuppb.CipherInfo `json:"-" toml:"-"`
}

func (cfg *Config) parseCipherInfo(flags *pflag.FlagSet) error {
	crypterStr, err := flags.GetString(flagCipherType)
	if err != nil {
		return errors.Trace(err)
	}

	cfg.CipherInfo.CipherType, err = parseCipherType(crypterStr)
	if err != nil {
		return errors.Trace(err)
	}

	if cfg.CipherInfo.CipherType == encryptionpb.EncryptionMethod_PLAINTEXT {
		return nil
	}

	key, err := flags.GetString(flagCipherKey)
	if err != nil {
		return errors.Trace(err)
	}

	keyFilePath, err := flags.GetString(flagCipherKeyFile)
	if err != nil {
		return errors.Trace(err)
	}

	cfg.CipherInfo.CipherKey, err = getCipherKeyContent(key, keyFilePath)
	if err != nil {
		return errors.Trace(err)
	}

	if !checkCipherKeyMatch(&cfg.CipherInfo) {
		return errors.Annotate(berrors.ErrInvalidArgument, "crypter method and key length not match")
	}

	return nil
}

func (cfg *Config) normalizePDURLs() error {
	for i := range cfg.PD {
		var err error
		cfg.PD[i], err = normalizePDURL(cfg.PD[i], cfg.TLS.IsEnabled())
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// ParseFromFlags parses the config from the flag set.
func (cfg *Config) ParseFromFlags(flags *pflag.FlagSet) error {
	var err error
	if cfg.Storage, err = flags.GetString(flagStorage); err != nil {
		return errors.Trace(err)
	}
	if cfg.SendCreds, err = flags.GetBool(flagSendCreds); err != nil {
		return errors.Trace(err)
	}
	if cfg.NoCreds, err = flags.GetBool(flagNoCreds); err != nil {
		return errors.Trace(err)
	}
	if cfg.Concurrency, err = flags.GetUint32(flagConcurrency); err != nil {
		return errors.Trace(err)
	}
	if cfg.Checksum, err = flags.GetBool(flagChecksum); err != nil {
		return errors.Trace(err)
	}
	if cfg.ChecksumConcurrency, err = flags.GetUint(flagChecksumConcurrency); err != nil {
		return errors.Trace(err)
	}

	var rateLimit, rateLimitUnit uint64
	if rateLimit, err = flags.GetUint64(flagRateLimit); err != nil {
		return errors.Trace(err)
	}
	if rateLimitUnit, err = flags.GetUint64(flagRateLimitUnit); err != nil {
		return errors.Trace(err)
	}
	cfg.RateLimit = rateLimit * rateLimitUnit

	checkRequirements, err := flags.GetBool(flagCheckRequirement)
	if err != nil {
		return errors.Trace(err)
	}
	cfg.CheckRequirements = checkRequirements

	cfg.SwitchModeInterval, err = flags.GetDuration(flagSwitchModeInterval)
	if err != nil {
		return errors.Trace(err)
	}
	cfg.GRPCKeepaliveTime, err = flags.GetDuration(flagGrpcKeepaliveTime)
	if err != nil {
		return errors.Trace(err)
	}
	cfg.GRPCKeepaliveTimeout, err = flags.GetDuration(flagGrpcKeepaliveTimeout)
	if err != nil {
		return errors.Trace(err)
	}
	cfg.EnableOpenTracing, err = flags.GetBool(flagEnableOpenTracing)
	if err != nil {
		return errors.Trace(err)
	}

	if cfg.SwitchModeInterval <= 0 {
		return errors.Annotatef(berrors.ErrInvalidArgument, "--switch-mode-interval must be positive, %s is not allowed", cfg.SwitchModeInterval)
	}

	if err = cfg.BackendOptions.ParseFromFlags(flags); err != nil {
		return errors.Trace(err)
	}
	if err = cfg.TLS.ParseFromFlags(flags); err != nil {
		return errors.Trace(err)
	}
	cfg.PD, err = flags.GetStringSlice(flagPD)
	if err != nil {
		return errors.Trace(err)
	}
	if len(cfg.PD) == 0 {
		return errors.Annotate(berrors.ErrInvalidArgument, "must provide at least one PD server address")
	}
	if cfg.SkipCheckPath, err = flags.GetBool(flagSkipCheckPath); err != nil {
		return errors.Trace(err)
	}
	if cfg.SkipCheckPath {
		log.L().Info("--skip-check-path is deprecated, need explicitly set it anymore")
	}

	if err = cfg.parseCipherInfo(flags); err != nil {
		return errors.Trace(err)
	}

	return cfg.normalizePDURLs()
}

// adjust adjusts the abnormal config value in the current config.
// useful when not starting BR from CLI (e.g. from BRIE in SQL).
func (cfg *Config) adjust() {
	if cfg.GRPCKeepaliveTime == 0 {
		cfg.GRPCKeepaliveTime = defaultGRPCKeepaliveTime
	}
	if cfg.GRPCKeepaliveTimeout == 0 {
		cfg.GRPCKeepaliveTimeout = defaultGRPCKeepaliveTimeout
	}
	if cfg.ChecksumConcurrency == 0 {
		cfg.ChecksumConcurrency = variable.DefChecksumTableConcurrency
	}
}
