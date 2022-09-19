// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package httputil

import (
	"crypto/tls"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestHttpClient test the tls config
func TestHttpClient(t *testing.T) {
	tlsConf := tls.Config{}
	cli := NewClient(&tlsConf)
	require.Equal(t, &tlsConf, cli.Transport.(*http.Transport).TLSClientConfig)
}
