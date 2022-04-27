// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package server

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/tempurl"
	"go.etcd.io/etcd/embed"
)

func TestServer(t *testing.T) {
	etcdCfg := NewTestSingleConfig()
	etcd, err := embed.StartEtcd(etcdCfg)
	require.Equal(t, err, nil)
	defer etcd.Close()

	cfg := NewConfig()
	for _, url := range etcdCfg.ACUrls {
		cfg.EtcdEndpoint += url.String()
	}

	ctx := context.Background()
	s := &Server{
		cfg:            cfg,
		ctx:            ctx,
		startTimestamp: time.Now().Unix(),
	}
	err = s.createEtcdClient()
	require.Equal(t, err, nil)
	defer s.Close()

	s.StartServer()
	require.Equal(t, s.IsServing(), true)

	time.Sleep(time.Duration(5) * time.Second)
	require.Equal(t, s.IsLead(), true)
	s.Close()
	require.Equal(t, s.IsServing(), false)
}

// NewTestSingleConfig is used to create a etcd config for the unit test purpose.
func NewTestSingleConfig() *embed.Config {
	cfg := embed.NewConfig()
	cfg.Name = "test_etcd"
	cfg.Dir, _ = os.MkdirTemp("/tmp", "test_etcd")
	cfg.WalDir = ""
	cfg.Logger = "zap"
	cfg.LogOutputs = []string{"stdout"}

	pu, _ := url.Parse(tempurl.Alloc())
	cfg.LPUrls = []url.URL{*pu}
	cfg.APUrls = cfg.LPUrls
	cu, _ := url.Parse(tempurl.Alloc())
	cfg.LCUrls = []url.URL{*cu}
	cfg.ACUrls = cfg.LCUrls

	cfg.StrictReconfigCheck = false
	cfg.InitialCluster = fmt.Sprintf("%s=%s", cfg.Name, &cfg.LPUrls[0])
	cfg.ClusterState = embed.ClusterStateFlagNew
	return cfg
}
