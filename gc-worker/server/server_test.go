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
	"math"
	"net"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
	pd "github.com/tikv/pd/client"
	"go.etcd.io/etcd/server/v3/embed"
)

type MockPDClient struct {
	pd.Client
	// SafePoint set by `UpdateGCSafePoint`. Not to be confused with SafePointKV.
	gcSafePoint uint64
	// Represents the current safePoint of all services including TiDB, representing how much data they want to retain
	// in GC.
	serviceSafePoints map[string]uint64
}

func (c *MockPDClient) GetTS(context.Context) (int64, int64, error) {
	unixTime := time.Now()
	ts := oracle.GoTimeToTS(unixTime)
	return oracle.ExtractPhysical(ts), oracle.ExtractLogical(ts), nil
}

func (c *MockPDClient) UpdateGCSafePoint(ctx context.Context, safePoint uint64) (uint64, error) {
	c.gcSafePoint = safePoint
	return safePoint, nil
}

func (c *MockPDClient) UpdateServiceGCSafePoint(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
	c.serviceSafePoints[serviceID] = safePoint
	minSafePoint := uint64(math.MaxUint64)
	for _, safepoint := range c.serviceSafePoints {
		if safepoint < minSafePoint {
			minSafePoint = safepoint
		}
	}
	return minSafePoint, nil
}

func (c *MockPDClient) Close() {
}

// NewPDClient creates a mock pd.Client that uses local timestamp and meta data
// from a Cluster.
func NewMockPDClient() *MockPDClient {
	mockClient := MockPDClient{
		gcSafePoint:       0,
		serviceSafePoints: make(map[string]uint64),
	}
	return &mockClient
}

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

	pu, _ := url.Parse(TempURL())
	cfg.LPUrls = []url.URL{*pu}
	cfg.APUrls = cfg.LPUrls
	cu, _ := url.Parse(TempURL())
	cfg.LCUrls = []url.URL{*cu}
	cfg.ACUrls = cfg.LCUrls

	cfg.StrictReconfigCheck = false
	cfg.InitialCluster = fmt.Sprintf("%s=%s", cfg.Name, &cfg.LPUrls[0])
	cfg.ClusterState = embed.ClusterStateFlagNew
	return cfg
}

// Alloc allocates a local URL for testing.
func TempURL() string {
	for i := 0; i < 10; i++ {
		if u := tryAllocTestURL(); u != "" {
			return u
		}
		time.Sleep(time.Second)
	}
	return ""
}

func tryAllocTestURL() string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return ""
	}
	defer l.Close()
	addr := fmt.Sprintf("http://%s", l.Addr())
	return addr
}

func TestCalcNewGCSafePoint(t *testing.T) {
	cfg := NewConfig()
	ctx := context.Background()
	s := &Server{
		cfg:            cfg,
		ctx:            ctx,
		startTimestamp: time.Now().Unix(),
	}
	defer s.Close()
	newSp := s.calcNewGCSafePoint(0, 100)
	require.Equal(t, newSp, uint64(100))

	newSp = s.calcNewGCSafePoint(1000, 100)
	require.Equal(t, newSp, uint64(100))

	newSp = s.calcNewGCSafePoint(1000, 10000)
	require.Equal(t, newSp, uint64(1000))

}

func TestCalcGCSafePoint(t *testing.T) {
	mockPdClient := NewMockPDClient()
	cfg := NewConfig()
	cfg.GCLifeTime = defaultGCLifeTime
	ctx := context.Background()
	s := &Server{
		cfg:            cfg,
		ctx:            ctx,
		startTimestamp: time.Now().Unix(),
	}
	defer s.Close()
	s.pdClient = mockPdClient
	curTs := oracle.GoTimeToTS(time.Now())
	expectTs := time.Now().Add(-cfg.GCLifeTime)
	expectGcSafePoint := oracle.GoTimeToTS(expectTs)
	gcSafePoint, err := s.getGCWorkerSafePoint(ctx)
	require.NoError(t, err)
	require.LessOrEqual(t, expectGcSafePoint, gcSafePoint)
	require.LessOrEqual(t, gcSafePoint, curTs)
}

func TestUpdateGCSafePoint(t *testing.T) {
	mockPdClient := NewMockPDClient()
	cfg := NewConfig()
	cfg.GCLifeTime = defaultGCLifeTime
	ctx := context.Background()
	s := &Server{
		cfg:            cfg,
		ctx:            ctx,
		startTimestamp: time.Now().Unix(),
	}
	defer s.Close()
	s.pdClient = mockPdClient
	mockPdClient.UpdateServiceGCSafePoint(ctx, "cdc", math.MaxInt64, 100)
	err := s.updateRawGCSafePoint(ctx)
	require.NoError(t, err)
	require.Equal(t, mockPdClient.gcSafePoint, uint64(100))
}
