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

package config

import (
	"flag"
	"net/url"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// SecurityConfig indicates the security configuration for pd server
type SecurityConfig struct {
	// grpcutil.TLSConfig  //TODO support security config
	// RedactInfoLog indicates that whether enabling redact log
	RedactInfoLog bool `toml:"redact-info-log" json:"redact-info-log"`
}

// Config is the pd server configuration.
// NOTE: This type is exported by HTTP API. Please pay more attention when modifying it.
type Config struct {
	flagSet *flag.FlagSet

	Name string `toml:"name" json:"name"`

	Version bool `json:"-"`

	PdAddrs      string `toml:"pd" json:"pd"`
	EtcdEndpoint string `toml:"etcd" json:"etcd"`

	Security SecurityConfig `toml:"security" json:"security"`

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

	fs.StringVar(&cfg.configFile, "config", "", "config file")

	fs.StringVar(&cfg.PdAddrs, "pd", "", "specify pd address (usage: pd '${pd-addrs}'")
	fs.StringVar(&cfg.EtcdEndpoint, "etcd", "", "specify etcd endpoints")
	fs.StringVar(&cfg.Log.Level, "L", "info", "log level: debug, info, warn, error, fatal (default 'info')")
	fs.StringVar(&cfg.Log.File.Filename, "log-file", "gc_worker.log", "log file path")

	return cfg
}

const (
	defaultName      = "gc-worker"
	defaultLogFormat = "text"
)

// Special keys for Labels
const (
	// ZoneLabel is the name of the key which indicates DC location of this PD server.
	ZoneLabel = "zone"
)

// configFromFile loads config from file.
func (c *Config) configFromFile(path string) (*toml.MetaData, error) {
	meta, err := toml.DecodeFile(path, c)
	return &meta, errors.WithStack(err)
}

func adjustString(v *string, defValue string) {
	if len(*v) == 0 {
		*v = defValue
	}
}

// Validate is used to validate if some configurations are right.
func (c *Config) Validate() error {
	return nil
}

// Adjust is used to adjust the configurations.
func (c *Config) Adjust(meta *toml.MetaData, reloading bool) error {
	if err := c.Validate(); err != nil {
		return err
	}
	adjustString(&c.Name, defaultName)

	if len(c.Log.Format) == 0 {
		c.Log.Format = defaultLogFormat
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
	var meta *toml.MetaData
	if c.configFile != "" {
		meta, err = c.configFromFile(c.configFile)
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

	err = c.Adjust(meta, false)
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

// parseUrls parse a string into multiple urls.
func parseUrls(s string) ([]url.URL, error) {
	items := strings.Split(s, ",")
	urls := make([]url.URL, 0, len(items))
	for _, item := range items {
		u, err := url.Parse(item)
		if err != nil {
			return nil, errors.Trace(err)
		}

		urls = append(urls, *u)
	}

	return urls, nil
}
