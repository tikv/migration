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
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/gcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/pkg/tempurl"
	"github.com/tikv/pd/pkg/tsoutil"
	"github.com/tikv/pd/pkg/typeutil"
	"go.etcd.io/etcd/embed"
	"go.uber.org/atomic"
)

const (
	defaultServiceGroup = "default_rawkv"
)

type MockPDClient struct {
	tsLogical atomic.Uint64
	// SafePoint set by `UpdateGCSafePoint`. Not to be confused with SafePointKV.
	gcSafePoint map[string]uint64
	// Represents the current safePoint of all services including TiDB, representing how much data they want to retain
	// in GC.
	serviceSafePoints map[string]map[string]uint64
}

func (c *MockPDClient) LoadGlobalConfig(ctx context.Context, names []string) ([]pd.GlobalConfigItem, error) {
	return nil, nil
}

func (c *MockPDClient) StoreGlobalConfig(ctx context.Context, items []pd.GlobalConfigItem) error {
	return nil
}

func (c *MockPDClient) WatchGlobalConfig(ctx context.Context) (chan []pd.GlobalConfigItem, error) {
	return nil, nil
}

func (c *MockPDClient) GetClusterID(ctx context.Context) uint64 {
	return 1
}

func (c *MockPDClient) GetLocalTS(ctx context.Context, dcLocation string) (int64, int64, error) {
	return c.GetTS(ctx)
}

func (c *MockPDClient) GetTSAsync(ctx context.Context) pd.TSFuture {
	return &mockTSFuture{c, ctx, false}
}

func (c *MockPDClient) GetLocalTSAsync(ctx context.Context, dcLocation string) pd.TSFuture {
	return c.GetTSAsync(ctx)
}

type mockTSFuture struct {
	pdc  *MockPDClient
	ctx  context.Context
	used bool
}

func (m *mockTSFuture) Wait() (int64, int64, error) {
	if m.used {
		return 0, 0, errors.New("cannot wait tso twice")
	}
	m.used = true
	return m.pdc.GetTS(m.ctx)
}

func (c *MockPDClient) GetRegion(ctx context.Context, key []byte, opts ...pd.GetRegionOption) (*pd.Region, error) {
	return nil, nil
}

func (c *MockPDClient) GetRegionFromMember(ctx context.Context, key []byte, memberURLs []string) (*pd.Region, error) {
	return &pd.Region{}, nil
}

func (c *MockPDClient) GetPrevRegion(ctx context.Context, key []byte, opts ...pd.GetRegionOption) (*pd.Region, error) {
	return nil, nil
}

func (c *MockPDClient) GetRegionByID(ctx context.Context, regionID uint64, opts ...pd.GetRegionOption) (*pd.Region, error) {
	return nil, nil
}

func (c *MockPDClient) ScanRegions(ctx context.Context, startKey []byte, endKey []byte, limit int) ([]*pd.Region, error) {
	return nil, nil
}

func (c *MockPDClient) GetStore(ctx context.Context, storeID uint64) (*metapb.Store, error) {
	return nil, nil
}

func (c *MockPDClient) GetAllStores(ctx context.Context, opts ...pd.GetStoreOption) ([]*metapb.Store, error) {
	return nil, nil
}

func (c *MockPDClient) UpdateGCSafePoint(ctx context.Context, safePoint uint64) (uint64, error) {
	return 0, nil
}

func (c *MockPDClient) UpdateServiceGCSafePoint(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
	return 0, nil
}

func (c *MockPDClient) Close() {
}

func (c *MockPDClient) ScatterRegion(ctx context.Context, regionID uint64) error {
	return nil
}

func (c *MockPDClient) ScatterRegions(ctx context.Context, regionsID []uint64, opts ...pd.RegionsOption) (*pdpb.ScatterRegionResponse, error) {
	return nil, nil
}

func (c *MockPDClient) SplitRegions(ctx context.Context, splitKeys [][]byte, opts ...pd.RegionsOption) (*pdpb.SplitRegionsResponse, error) {
	return nil, nil
}

func (c *MockPDClient) GetOperator(ctx context.Context, regionID uint64) (*pdpb.GetOperatorResponse, error) {
	return nil, nil
}

func (c *MockPDClient) GetAllMembers(ctx context.Context) ([]*pdpb.Member, error) {
	return nil, nil
}

func (c *MockPDClient) GetLeaderAddr() string { return "mockpd" }

func (c *MockPDClient) UpdateOption(option pd.DynamicOption, value interface{}) error {
	return nil
}

func (c *MockPDClient) GetTS(context.Context) (int64, int64, error) {
	unixTime := time.Now()
	ts := tsoutil.GenerateTimestamp(unixTime, c.tsLogical.Add(1)) // set logical as 0
	return ts.Physical, ts.Logical, nil
}

// SplitAndScatterRegions split regions by given split keys and scatter new regions
func (c *MockPDClient) SplitAndScatterRegions(ctx context.Context, splitKeys [][]byte, opts ...pd.RegionsOption) (*pdpb.SplitAndScatterRegionsResponse, error) {
	return nil, nil
}

// GetGCAllServiceGroups returns a list containing all service groups that has safe point in pd
func (c *MockPDClient) GetGCAllServiceGroups(ctx context.Context) ([]string, error) {
	return []string{defaultServiceGroup}, nil
}

// GetGCMinServiceSafePointByServiceGroup return the minimum of all service safe point of the given group
// It also returns the current revision of the pd storage, with in which the min is valid
// If none is found, it will return 0 as min
func (c *MockPDClient) GetGCMinServiceSafePointByServiceGroup(ctx context.Context, serviceGroupID string) (safePoint uint64, revision int64, err error) {
	minSafePoint := uint64(math.MaxUint64)
	for _, safepoint := range c.serviceSafePoints[serviceGroupID] {
		if safepoint < minSafePoint {
			minSafePoint = safepoint
		}
	}
	return minSafePoint, 0, nil
}

// UpdateGCSafePointByServiceGroup update the target safe point, along with revision obtained previously
// If failed, caller should retry from GetGCMinServiceSafePointByServiceGroup
func (c *MockPDClient) UpdateGCSafePointByServiceGroup(ctx context.Context, serviceGroupID string, safePoint uint64, revision int64) (succeeded bool, newSafePoint uint64, err error) {
	c.gcSafePoint[serviceGroupID] = safePoint
	return true, safePoint, nil
}

// UpdateGCServiceSafePointByServiceGroup update the given service's safe point
// Pass in a negative ttl to remove it
// If failed, caller should retry with higher safe point
func (c *MockPDClient) UpdateGCServiceSafePointByServiceGroup(ctx context.Context, serviceGroupID, serviceID string, ttl int64, safePoint uint64) (succeeded bool, gcSafePoint, oldSafePoint, newSafePoint uint64, err error) {
	oldSafePoint = c.serviceSafePoints[serviceGroupID][serviceID]
	c.serviceSafePoints[serviceGroupID][serviceID] = safePoint
	return true, 0, oldSafePoint, safePoint, nil
}

// GetGCAllServiceGroupSafePoints returns GC safe point for all service groups
func (c *MockPDClient) GetGCAllServiceGroupSafePoints(ctx context.Context) ([]*gcpb.ServiceGroupSafePoint, error) {
	ret := make([]*gcpb.ServiceGroupSafePoint, 0)
	for i, s := range c.gcSafePoint {
		ret = append(ret, &gcpb.ServiceGroupSafePoint{
			ServiceGroupId: []byte(i),
			SafePoint:      s,
		})
	}
	return ret, nil
}

// NewPDClient creates a mock pd.Client that uses local timestamp and meta data
// from a Cluster.
func NewMockPDClient() pd.Client {
	mockClient := MockPDClient{
		gcSafePoint:       make(map[string]uint64),
		serviceSafePoints: make(map[string]map[string]uint64),
	}
	mockClient.gcSafePoint[defaultServiceGroup] = 0
	mockClient.serviceSafePoints[defaultServiceGroup] = make(map[string]uint64)
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

func TestCalcGcSafePoint(t *testing.T) {
	mockPdClient := NewMockPDClient()
	cfg := NewConfig()
	cfg.GCLifeTime = typeutil.NewDuration(defaultGCLifeTime)
	ctx := context.Background()
	s := &Server{
		cfg:            cfg,
		ctx:            ctx,
		startTimestamp: time.Now().Unix(),
	}
	defer s.Close()
	s.pdClient = mockPdClient
	curTs := tsoutil.GenerateTS(tsoutil.GenerateTimestamp(time.Now(), 0))
	expectTs := time.Now().Add(-cfg.GCLifeTime.Duration)
	expectGcSafePoint := tsoutil.GenerateTS(tsoutil.GenerateTimestamp(expectTs, 0))
	gcSafePoint, err := s.getGCWorkerSafePoint(ctx)
	require.NoError(t, err)
	require.LessOrEqual(t, expectGcSafePoint, gcSafePoint)
	require.LessOrEqual(t, gcSafePoint, curTs)
}

func TestUpdateGcSafePoint(t *testing.T) {
	mockPdClient := NewMockPDClient()
	cfg := NewConfig()
	cfg.GCLifeTime = typeutil.NewDuration(defaultGCLifeTime)
	ctx := context.Background()
	s := &Server{
		cfg:            cfg,
		ctx:            ctx,
		startTimestamp: time.Now().Unix(),
	}
	defer s.Close()
	s.pdClient = mockPdClient
	// curTs := tsoutil.GenerateTS(tsoutil.GenerateTimestamp(time.Now(), 0))
	// expectTs := time.Now().Add(-cfg.GCLifeTime.Duration)
	// expectGcSafePoint := tsoutil.GenerateTS(tsoutil.GenerateTimestamp(expectTs, 0))
	mockPdClient.UpdateGCServiceSafePointByServiceGroup(ctx, defaultServiceGroup, "cdc", math.MaxInt64, 100)
	err := s.updateRawGCSafePoint(ctx)
	require.NoError(t, err)
	allSafePoints, err := mockPdClient.GetGCAllServiceGroupSafePoints(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, len(allSafePoints))
	require.Equal(t, allSafePoints[0].SafePoint, uint64(100))
}
