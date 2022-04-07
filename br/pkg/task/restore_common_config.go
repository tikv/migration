package task

import (
	"github.com/pingcap/errors"
	"github.com/spf13/pflag"
	"github.com/tikv/migration/br/pkg/restore"
)

// RestoreCommonConfig is the common configuration for all BR restore tasks.
type RestoreCommonConfig struct {
	Online bool `json:"online" toml:"online"`

	// MergeSmallRegionSizeBytes is the threshold of merging small regions (Default 96MB, region split size).
	// MergeSmallRegionKeyCount is the threshold of merging smalle regions (Default 960_000, region split key count).
	// See https://github.com/tikv/tikv/blob/v4.0.8/components/raftstore/src/coprocessor/config.rs#L35-L38
	MergeSmallRegionSizeBytes uint64 `json:"merge-region-size-bytes" toml:"merge-region-size-bytes"`
	MergeSmallRegionKeyCount  uint64 `json:"merge-region-key-count" toml:"merge-region-key-count"`
}

// ParseFromFlags parses the config from the flag set.
func (cfg *RestoreCommonConfig) ParseFromFlags(flags *pflag.FlagSet) error {
	var err error
	cfg.Online, err = flags.GetBool(flagOnline)
	if err != nil {
		return errors.Trace(err)
	}
	cfg.MergeSmallRegionKeyCount, err = flags.GetUint64(FlagMergeRegionKeyCount)
	if err != nil {
		return errors.Trace(err)
	}
	cfg.MergeSmallRegionSizeBytes, err = flags.GetUint64(FlagMergeRegionSizeBytes)
	if err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(err)
}

// adjust adjusts the abnormal config value in the current config.
// useful when not starting BR from CLI (e.g. from BRIE in SQL).
func (cfg *RestoreCommonConfig) adjust() {
	if cfg.MergeSmallRegionKeyCount == 0 {
		cfg.MergeSmallRegionKeyCount = restore.DefaultMergeRegionKeyCount
	}
	if cfg.MergeSmallRegionSizeBytes == 0 {
		cfg.MergeSmallRegionSizeBytes = restore.DefaultMergeRegionSizeBytes
	}
}
