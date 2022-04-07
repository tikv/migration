package task

import (
	"github.com/pingcap/errors"
	"github.com/spf13/pflag"
)

// RestoreRawConfig is the configuration specific for raw kv restore tasks.
type RestoreRawConfig struct {
	RawKvConfig
	RestoreCommonConfig
}

// ParseFromFlags parses the backup-related flags from the flag set.
func (cfg *RestoreRawConfig) ParseFromFlags(flags *pflag.FlagSet) error {
	var err error
	cfg.Online, err = flags.GetBool(flagOnline)
	if err != nil {
		return errors.Trace(err)
	}
	err = cfg.RestoreCommonConfig.ParseFromFlags(flags)
	if err != nil {
		return errors.Trace(err)
	}

	return cfg.RawKvConfig.ParseFromFlags(flags)
}

func (cfg *RestoreRawConfig) adjust() {
	cfg.Config.adjust()
	cfg.RestoreCommonConfig.adjust()

	if cfg.Concurrency == 0 {
		cfg.Concurrency = defaultRestoreConcurrency
	}
}
