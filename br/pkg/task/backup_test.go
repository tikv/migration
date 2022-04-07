// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package task

import (
	"testing"

	backup "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/stretchr/testify/require"
)

func TestParseCompressionType(t *testing.T) {
	var (
		ct  backup.CompressionType
		err error
	)
	cfg := &RawKvConfig{}
	ct, err = cfg.parseCompressionType("lz4")
	require.NoError(t, err)
	require.Equal(t, 1, int(ct))

	ct, err = cfg.parseCompressionType("snappy")
	require.NoError(t, err)
	require.Equal(t, 2, int(ct))

	ct, err = cfg.parseCompressionType("zstd")
	require.NoError(t, err)
	require.Equal(t, 3, int(ct))

	ct, err = cfg.parseCompressionType("Other Compression (strings)")
	require.Error(t, err)
	require.Regexp(t, "invalid compression.*", err.Error())
	require.Zero(t, ct)
}
