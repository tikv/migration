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

package utils

import (
	"crypto/tls"

	"github.com/pingcap/errors"
	"github.com/spf13/pflag"
	"github.com/tikv/client-go/v2/config"
)

const (
	// flagCA is the name of TLS CA flag.
	flagCA = "ca"
	// flagCert is the name of TLS cert flag.
	flagCert = "cert"
	// flagKey is the name of TLS key flag.
	flagKey = "key"
)

// TLSConfig is the common configuration for TLS connection.
type TLSConfig struct {
	CA   string `json:"ca" toml:"ca"`
	Cert string `json:"cert" toml:"cert"`
	Key  string `json:"key" toml:"key"`
}

// IsEnabled checks if TLS open or not.
func (tls *TLSConfig) IsEnabled() bool {
	return tls.CA != ""
}

// ToTLSConfig generate tls.Config.
func (tls *TLSConfig) ToTLSConfig() (*tls.Config, error) {
	security := config.NewSecurity(tls.CA, tls.Cert, tls.Key, []string{})
	tlsConfig, err := security.ToTLSConfig()
	if err != nil {
		return nil, errors.Trace(err)
	}
	return tlsConfig, nil
}

// DefineTLSFlags define flags for tls config
func DefineTLSFlags(flags *pflag.FlagSet) {
	flags.String(flagCA, "", "CA certificate path for TLS connection")
	flags.String(flagCert, "", "Certificate path for TLS connection")
	flags.String(flagKey, "", "Private key path for TLS connection")
}

// ParseFromFlags parses the TLS config from the flag set.
func (tls *TLSConfig) ParseFromFlags(flags *pflag.FlagSet) (err error) {
	tls.CA, err = flags.GetString(flagCA)
	if err != nil {
		return err
	}
	tls.Cert, err = flags.GetString(flagCert)
	if err != nil {
		return err
	}
	tls.Key, err = flags.GetString(flagKey)
	if err != nil {
		return err
	}
	return nil
}
