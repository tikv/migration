// Copyright 2022 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
