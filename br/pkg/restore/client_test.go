// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jarcoal/httpmock"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
	"google.golang.org/grpc/keepalive"
)

var defaultKeepaliveCfg = keepalive.ClientParameters{
	Time:    3 * time.Second,
	Timeout: 10 * time.Second,
}

type fakePDClient struct {
	pd.Client
	stores []*metapb.Store
}

func (fpdc fakePDClient) GetAllStores(context.Context, ...pd.GetStoreOption) ([]*metapb.Store, error) {
	return append([]*metapb.Store{}, fpdc.stores...), nil
}

// Mock ImporterClient interface
type FakeImporterClient struct {
	ImporterClient
}

// Record the stores that have communicated
type RecordStores struct {
	mu     sync.Mutex
	stores map[uint64]uint64
}

func NewRecordStores() RecordStores {
	return RecordStores{stores: make(map[uint64]uint64, 0)}
}

func (r *RecordStores) put(id uint64, rateLimit uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stores[id] = rateLimit
}

func (r *RecordStores) len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.stores)
}

func (r *RecordStores) get(id uint64) uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.stores[id]
}

func (r *RecordStores) toString() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return fmt.Sprintf("%v", r.stores)
}

var recordStores RecordStores

const workingTime = 10

func (fakeImportCli FakeImporterClient) SetDownloadSpeedLimit(
	ctx context.Context,
	storeID uint64,
	req *import_sstpb.SetDownloadSpeedLimitRequest,
) (*import_sstpb.SetDownloadSpeedLimitResponse, error) {
	time.Sleep(workingTime * time.Millisecond) // simulate doing 100 ms work
	recordStores.put(storeID, req.SpeedLimit)
	return nil, nil
}

func TestSetSpeedLimit(t *testing.T) {
	mockStores := []*metapb.Store{
		{Id: 1},
		{Id: 2},
		{Id: 3},
		{Id: 4},
		{Id: 5},
		{Id: 6},
		{Id: 7},
		{Id: 8},
		{Id: 9},
		{Id: 10},
	}
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	// Exact URL match
	httpmock.RegisterResponder("GET", `=~^/config`,
		httpmock.NewStringResponder(200, `{"storage":{"api-version":2, "enable-ttl":true}}`))
	// 1. The cost of concurrent communication is expected to be less than the cost of serial communication.
	client, err := NewRestoreClient(fakePDClient{
		stores: mockStores,
	}, nil, defaultKeepaliveCfg, true)
	require.NoError(t, err)
	client.fileImporter = NewFileImporter(nil, FakeImporterClient{}, nil, true, kvrpcpb.APIVersion_V2)
	ctx := context.Background()

	rateLimit := uint64(10)
	recordStores = NewRecordStores()
	start := time.Now()
	err = client.setSpeedLimit(ctx, rateLimit)
	cost := time.Since(start)
	require.NoError(t, err)

	t.Logf("Total Cost: %v\n", cost)
	t.Logf("Has Communicated: %v\n", recordStores.toString())

	serialCost := time.Duration(len(mockStores)*workingTime) * time.Millisecond
	require.LessOrEqual(t, serialCost, cost)
	require.Equal(t, len(mockStores), recordStores.len())
	for i := 0; i < len(mockStores); i++ {
		require.Equal(t, rateLimit, recordStores.get(mockStores[i].Id))
	}

	recordStores = NewRecordStores()
	start = time.Now()
	client.resetSpeedLimit(ctx)
	cost = time.Since(start)
	require.LessOrEqual(t, serialCost, cost)
	require.Equal(t, len(mockStores), recordStores.len())
	for i := 0; i < len(mockStores); i++ {
		require.Equal(t, uint64(0), recordStores.get(mockStores[i].Id))
	}
}
