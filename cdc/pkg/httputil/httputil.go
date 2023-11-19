// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package httputil

import (
	"crypto/tls"
	"net/http"
	"time"

	"github.com/tikv/migration/cdc/pkg/security"
)

// Client wraps an HTTP client and support TLS requests.
type Client struct {
	http.Client
}

// NewClient creates an HTTP client with the given Credential.
func NewClient(credential *security.Credential) (*Client, error) {
	transport := http.DefaultTransport
	if credential != nil {
		tlsConf, err := credential.ToTLSConfig()
		if err != nil {
			return nil, err
		}
		if tlsConf != nil {
			httpTrans := http.DefaultTransport.(*http.Transport).Clone()
			httpTrans.TLSClientConfig = tlsConf
			transport = httpTrans
		}
	}
	// TODO: specific timeout in http client
	return &Client{
		Client: http.Client{Transport: transport},
	}, nil
}

// NewClient returns an HTTP(s) client.
// Copied from br/pkg/httputil/http.go
// TODO: eliminate duplicated codes with `NewClient`.
func NewClientByTLSConfig(tlsConf *tls.Config) *http.Client {
	// defaultTimeout for non-context requests.
	const defaultTimeout = 30 * time.Second
	cli := &http.Client{Timeout: defaultTimeout}
	if tlsConf != nil {
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.TLSClientConfig = tlsConf
		cli.Transport = transport
	}
	return cli
}
