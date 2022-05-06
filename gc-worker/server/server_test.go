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

func CreateAndStartTestServer(ctx context.Context, num uint32, cfg *Config) []*Server {
	ret := []*Server{}
	for i := 0; i < int(num); i++ {
		s := &Server{
			cfg:            cfg,
			ctx:            ctx,
			startTimestamp: time.Now().Unix(),
		}
		if err := s.createEtcdClient(); err != nil {
			return nil
		}
		ret = append(ret, s)
	}
	return ret
}

func CloseAllServers(servers []*Server) {
	if len(servers) == 0 {
		return
	}
	for _, server := range servers {
		if server != nil {
			server.Close()
		}
	}
}

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
	servers := CreateAndStartTestServer(ctx, 3, cfg)
	defer CloseAllServers(servers)
	require.NotEqual(t, servers, nil)
	for _, server := range servers {
		require.NotEqual(t, server, nil)
		server.StartServer()
		time.Sleep(time.Second * 1)
		require.Equal(t, server.IsServing(), true)
	}

	time.Sleep(time.Duration(1) * time.Second)
	leaderNum := 0
	for _, server := range servers {
		if server.IsLead() {
			leaderNum += 1
		}
	}
	// only one server can be the leader
	require.Equal(t, leaderNum, 1)
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
