package task

import (
	"crypto/tls"

	"github.com/pingcap/errors"
	"github.com/spf13/pflag"
	"go.etcd.io/etcd/pkg/transport"
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
	tlsInfo := transport.TLSInfo{
		CertFile:      tls.Cert,
		KeyFile:       tls.Key,
		TrustedCAFile: tls.CA,
	}
	tlsConfig, err := tlsInfo.ClientConfig()
	if err != nil {
		return nil, errors.Trace(err)
	}
	return tlsConfig, nil
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
