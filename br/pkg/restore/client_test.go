// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tikv/migration/br/pkg/gluetidb"
	"github.com/tikv/migration/br/pkg/mock"
	"github.com/tikv/migration/br/pkg/restore"
	"google.golang.org/grpc/keepalive"
)

var mc *mock.Cluster

var defaultKeepaliveCfg = keepalive.ClientParameters{
	Time:    3 * time.Second,
	Timeout: 10 * time.Second,
}

func TestIsOnline(t *testing.T) {
	m := mc
	client, err := restore.NewRestoreClient(gluetidb.New(), m.PDClient, m.Storage, nil, defaultKeepaliveCfg)
	require.NoError(t, err)

	require.False(t, client.IsOnline())
	client.EnableOnline()
	require.True(t, client.IsOnline())
}
