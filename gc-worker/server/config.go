// Copyright 2022 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"flag"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/typeutil"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type TLSConfig struct {
	CA   string `json:"ca" toml:"ca"`
	Cert string `json:"cert" toml:"cert"`
	Key  string `json:"key" toml:"key"`
}

type Config struct {
	flagSet *flag.FlagSet

	Name string `toml:"name" json:"name"`

	Version bool `json:"-"`

	PdAddrs      string `toml:"pd" json:"pd"`
	EtcdEndpoint string `toml:"etcd" json:"etcd"`

	SafePointUpdateInterval typeutil.Duration `toml:"safepoint-update-interval" json:"safepoint-update-interval"`
	EtcdElectionInterval    typeutil.Duration `toml:"etcd-election-interval" json:"etcd-election-interval"`
	GCLifeTime              typeutil.Duration `toml:"gc-life-time" json:"gc-life-time"`
	TlsConfig               TLSConfig         `toml:"security" json:"security"`

	// Log related config.
	Log log.Config `toml:"log" json:"log"`

	configFile string
	logger     *zap.Logger
	logProps   *log.ZapProperties
}

// NewConfig creates a new config.
func NewConfig() *Config {
	cfg := &Config{}
	cfg.flagSet = flag.NewFlagSet("gc-worker", flag.ContinueOnError)
	fs := cfg.flagSet

	fs.StringVar(&cfg.Name, "name", "", "specify cluster name")
	fs.BoolVar(&cfg.Version, "V", false, "print version information and exit")
	fs.BoolVar(&cfg.Version, "version", false, "print version information and exit")

	fs.DurationVar(&cfg.SafePointUpdateInterval.Duration, "safepoint-update-interval",
		defaultUpdateSafePointInterval, "update interval of gc safepoint")
	fs.DurationVar(&cfg.EtcdElectionInterval.Duration, "etcd-election-interval",
		defaultEtcdElectionInterval, "update interval of etcd election")
	fs.DurationVar(&cfg.GCLifeTime.Duration, "gc-life-time",
		defaultGCLifeTime, "gc life time")
	fs.StringVar(&cfg.PdAddrs, "pd", "", "specify pd address (usage: pd '${pd-addrs}'")
	fs.StringVar(&cfg.EtcdEndpoint, "etcd", "", "specify etcd endpoints")
	fs.StringVar(&cfg.Log.Level, "L", "info", "log level: debug, info, warn, error, fatal (default 'info')")
	fs.StringVar(&cfg.Log.File.Filename, "log-file", "gc_worker.log", "log file path")

	fs.StringVar(&cfg.configFile, "config", "", "config file")
	return cfg
}

const (
	defaultName                    = "gc-worker"
	defaultLogFormat               = "text"
	defaultUpdateSafePointInterval = time.Duration(10) * time.Second      // 10 s
	defaultEtcdElectionInterval    = time.Duration(10) * time.Millisecond // 10 ms
	defaultGCLifeTime              = time.Duration(10) * time.Minute      // 10 minutes
)

// configFromFile loads config from file.
func (c *Config) configFromFile(path string) error {
	meta, err := toml.DecodeFile(path, c)
	if err != nil {
		return errors.Trace(err)
	}
	if len(meta.Undecoded()) != 0 {
		undecodeStr := "There is undecoded bytes in config file:"
		for _, undecodedKey := range meta.Undecoded() {
			undecodeStr += undecodedKey.String()
		}
		return errors.New(undecodeStr)
	}
	return errors.WithStack(err)
}

func adjustString(v *string, defValue string) {
	if len(*v) == 0 {
		*v = defValue
	}
}

func adjustDuration(d *typeutil.Duration, defValue time.Duration) {
	if d.Nanoseconds() == 0 {
		*d = typeutil.Duration{Duration: defValue}
	}
}

// Validate is used to validate if some configurations are right.
func (c *Config) Validate() error {
	if c.Version {
		return nil
	}
	if len(c.PdAddrs) == 0 {
		return errors.New("no pd address is parsed.")
	}
	if len(c.EtcdEndpoint) == 0 {
		return errors.New("no etcd enpoint is parsed.")
	}
	return nil
}

// Adjust is used to adjust the configurations.
func (c *Config) Adjust() error {
	adjustString(&c.Name, defaultName)
	// reuse pd's etcd server as server
	adjustString(&c.EtcdEndpoint, c.PdAddrs)

	adjustDuration(&c.SafePointUpdateInterval, defaultUpdateSafePointInterval)
	adjustDuration(&c.EtcdElectionInterval, defaultEtcdElectionInterval)
	adjustDuration(&c.GCLifeTime, defaultGCLifeTime)

	if len(c.Log.Format) == 0 {
		c.Log.Format = defaultLogFormat
	}

	if err := c.Validate(); err != nil {
		return err
	}

	return nil
}

// Parse parses flag definitions from the argument list.
func (c *Config) Parse(arguments []string) error {
	// Parse first to get config file.
	err := c.flagSet.Parse(arguments)
	if err != nil {
		return errors.WithStack(err)
	}

	// Load config file if specified.
	if c.configFile != "" {
		err = c.configFromFile(c.configFile)
		if err != nil {
			return err
		}
	}

	// Parse again to replace with command line options.
	err = c.flagSet.Parse(arguments)
	if err != nil {
		return errors.WithStack(err)
	}

	if len(c.flagSet.Args()) != 0 {
		return errors.Errorf("'%s' is an invalid flag", c.flagSet.Arg(0))
	}

	err = c.Adjust()
	return err
}

// SetupLogger setup the logger.
func (c *Config) SetupLogger() error {
	lg, p, err := log.InitLogger(&c.Log, zap.AddStacktrace(zapcore.FatalLevel))
	if err != nil {
		return errors.Trace(err)
	}
	c.logger = lg
	c.logProps = p
	return nil
}

// GetZapLogger gets the created zap logger.
func (c *Config) GetZapLogger() *zap.Logger {
	return c.logger
}

// GetZapLogProperties gets properties of the zap logger.
func (c *Config) GetZapLogProperties() *log.ZapProperties {
	return c.logProps
}

// GetConfigFile gets the config file.
func (c *Config) GetConfigFile() string {
	return c.configFile
}
