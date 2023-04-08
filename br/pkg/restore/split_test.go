// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/tidb/store/pdtypes"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util/codec"
	"github.com/tikv/migration/br/pkg/rtree"
	"github.com/tikv/migration/br/pkg/utils"
	"go.uber.org/multierr"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type TestClient struct {
	mu                  sync.RWMutex
	stores              map[uint64]*metapb.Store
	regions             map[uint64]*RegionInfo
	regionsInfo         *pdtypes.RegionTree // For now it's only used in ScanRegions
	nextRegionID        uint64
	injectInScatter     func(*RegionInfo) error
	supportBatchScatter bool

	scattered map[uint64]bool
}

func NewTestClient(
	stores map[uint64]*metapb.Store,
	regions map[uint64]*RegionInfo,
	nextRegionID uint64,
) *TestClient {
	regionsInfo := &pdtypes.RegionTree{}
	for _, regionInfo := range regions {
		regionsInfo.SetRegion(pdtypes.NewRegionInfo(regionInfo.Region, regionInfo.Leader))
	}
	return &TestClient{
		stores:          stores,
		regions:         regions,
		regionsInfo:     regionsInfo,
		nextRegionID:    nextRegionID,
		scattered:       map[uint64]bool{},
		injectInScatter: func(*RegionInfo) error { return nil },
	}
}

func (c *TestClient) InstallBatchScatterSupport() {
	c.supportBatchScatter = true
}

// ScatterRegions scatters regions in a batch.
func (c *TestClient) ScatterRegions(ctx context.Context, regionInfo []*RegionInfo) error {
	if !c.supportBatchScatter {
		return status.Error(codes.Unimplemented, "Ah, yep")
	}
	regions := map[uint64]*RegionInfo{}
	for _, region := range regionInfo {
		regions[region.Region.Id] = region
	}
	var err error
	for i := 0; i < 3; i++ {
		if len(regions) == 0 {
			return nil
		}
		for id, region := range regions {
			splitErr := c.ScatterRegion(ctx, region)
			if splitErr == nil {
				delete(regions, id)
			}
			err = multierr.Append(err, splitErr)

		}
	}
	return nil
}

func (c *TestClient) GetAllRegions() map[uint64]*RegionInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.regions
}

func (c *TestClient) GetStore(ctx context.Context, storeID uint64) (*metapb.Store, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	store, ok := c.stores[storeID]
	if !ok {
		return nil, errors.Errorf("store not found")
	}
	return store, nil
}

func (c *TestClient) GetRegion(ctx context.Context, key []byte) (*RegionInfo, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, region := range c.regions {
		if bytes.Compare(key, region.Region.StartKey) >= 0 &&
			(len(region.Region.EndKey) == 0 || bytes.Compare(key, region.Region.EndKey) < 0) {
			return region, nil
		}
	}
	return nil, errors.Errorf("region not found: key=%s", string(key))
}

func (c *TestClient) GetRegionByID(ctx context.Context, regionID uint64) (*RegionInfo, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	region, ok := c.regions[regionID]
	if !ok {
		return nil, errors.Errorf("region not found: id=%d", regionID)
	}
	return region, nil
}

func (c *TestClient) SplitRegion(
	ctx context.Context,
	regionInfo *RegionInfo,
	key []byte,
) (*RegionInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var target *RegionInfo
	splitKey := codec.EncodeBytes([]byte{}, key)
	for _, region := range c.regions {
		if bytes.Compare(splitKey, region.Region.StartKey) >= 0 &&
			(len(region.Region.EndKey) == 0 || bytes.Compare(splitKey, region.Region.EndKey) < 0) {
			target = region
		}
	}
	if target == nil {
		return nil, errors.Errorf("region not found: key=%s", string(key))
	}
	newRegion := &RegionInfo{
		Region: &metapb.Region{
			Peers:    target.Region.Peers,
			Id:       c.nextRegionID,
			StartKey: target.Region.StartKey,
			EndKey:   splitKey,
		},
	}
	c.regions[c.nextRegionID] = newRegion
	c.nextRegionID++
	target.Region.StartKey = splitKey
	c.regions[target.Region.Id] = target
	return newRegion, nil
}

func (c *TestClient) BatchSplitRegionsWithOrigin(
	ctx context.Context, regionInfo *RegionInfo, keys [][]byte,
) (*RegionInfo, []*RegionInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	newRegions := make([]*RegionInfo, 0)
	var region *RegionInfo
	for _, key := range keys {
		var target *RegionInfo
		splitKey := codec.EncodeBytes([]byte{}, key)
		for _, region := range c.regions {
			if region.ContainsInterior(splitKey) {
				target = region
			}
		}
		if target == nil {
			continue
		}
		newRegion := &RegionInfo{
			Region: &metapb.Region{
				Peers:    target.Region.Peers,
				Id:       c.nextRegionID,
				StartKey: target.Region.StartKey,
				EndKey:   splitKey,
			},
		}
		c.regions[c.nextRegionID] = newRegion
		c.nextRegionID++
		target.Region.StartKey = splitKey
		c.regions[target.Region.Id] = target
		region = target
		newRegions = append(newRegions, newRegion)
	}
	return region, newRegions, nil
}

func (c *TestClient) BatchSplitRegions(
	ctx context.Context, regionInfo *RegionInfo, keys [][]byte,
) ([]*RegionInfo, error) {
	_, newRegions, err := c.BatchSplitRegionsWithOrigin(ctx, regionInfo, keys)
	return newRegions, err
}

func (c *TestClient) ScatterRegion(ctx context.Context, regionInfo *RegionInfo) error {
	return c.injectInScatter(regionInfo)
}

func (c *TestClient) GetOperator(ctx context.Context, regionID uint64) (*pdpb.GetOperatorResponse, error) {
	return &pdpb.GetOperatorResponse{
		Header: new(pdpb.ResponseHeader),
	}, nil
}

func (c *TestClient) ScanRegions(ctx context.Context, key, endKey []byte, limit int) ([]*RegionInfo, error) {
	infos := c.regionsInfo.ScanRange(key, endKey, limit)
	regions := make([]*RegionInfo, 0, len(infos))
	for _, info := range infos {
		regions = append(regions, &RegionInfo{
			Region: info.Meta,
			Leader: info.Leader,
		})
	}
	return regions, nil
}

func (c *TestClient) GetPlacementRule(ctx context.Context, groupID, ruleID string) (r pdtypes.Rule, err error) {
	return
}

func (c *TestClient) SetPlacementRule(ctx context.Context, rule pdtypes.Rule) error {
	return nil
}

func (c *TestClient) DeletePlacementRule(ctx context.Context, groupID, ruleID string) error {
	return nil
}

func (c *TestClient) SetStoresLabel(ctx context.Context, stores []uint64, labelKey, labelValue string) error {
	return nil
}

type assertRetryLessThanBackoffer struct {
	max     int
	already int
	t       *testing.T
}

func assertRetryLessThan(t *testing.T, times int) utils.Backoffer {
	return &assertRetryLessThanBackoffer{
		max:     times,
		already: 0,
		t:       t,
	}
}

// NextBackoff returns a duration to wait before retrying again
func (b *assertRetryLessThanBackoffer) NextBackoff(err error) time.Duration {
	b.already++
	if b.already >= b.max {
		b.t.Logf("retry more than %d time: test failed", b.max)
		b.t.FailNow()
	}
	return 0
}

// Attempt returns the remain attempt times
func (b *assertRetryLessThanBackoffer) Attempt() int {
	return b.max - b.already
}

func TestScatterFinishInTime(t *testing.T) {
	for _, needEncodeKey := range []bool{false, true} {
		ScatterFinishInTimeImpl(t, needEncodeKey)
	}
}

func ScatterFinishInTimeImpl(t *testing.T, needEncodeKey bool) {
	client := initTestClient()
	ranges := initRanges()
	rewriteRules := initRewriteRules()
	regionSplitter := NewRegionSplitter(client)

	ctx := context.Background()
	err := regionSplitter.Split(ctx, ranges, rewriteRules, needEncodeKey, func(key [][]byte) {}) // TODO: add test case for "isRawKV=true"
	require.NoError(t, err)
	regions := client.GetAllRegions()
	if !validateRegions(regions) {
		for _, region := range regions {
			t.Logf("region: %v\n", region.Region)
		}
		t.Log("get wrong result")
		t.Fail()
	}

	regionInfos := make([]*RegionInfo, 0, len(regions))
	for _, info := range regions {
		regionInfos = append(regionInfos, info)
	}
	failed := map[uint64]int{}
	client.injectInScatter = func(r *RegionInfo) error {
		failed[r.Region.Id]++
		if failed[r.Region.Id] > 7 {
			return nil
		}
		return status.Errorf(codes.Unknown, "region %d is not fully replicated", r.Region.Id)
	}

	// When using a exponential backoffer, if we try to backoff more than 40 times in 10 regions,
	// it would cost time unacceptable.
	regionSplitter.ScatterRegionsWithBackoffer(ctx,
		regionInfos,
		assertRetryLessThan(t, 40))

}

// region: [, aay), [aay, bba), [bba, bbh), [bbh, cca), [cca, )
// range: [aaa, aae), [aae, aaz), [ccd, ccf), [ccf, ccj)
// rewrite rules: aa -> xx,  cc -> bb
// expected regions after split:
//
//	[, aay), [aay, bba), [bba, bbf), [bbf, bbh), [bbh, bbj),
//	[bbj, cca), [cca, xxe), [xxe, xxz), [xxz, )
func TestSplitAndScatter(t *testing.T) {
	for _, needEncodeKey := range []bool{false, true} {
		t.Run("BatchScatter", func(t *testing.T) {
			client := initTestClient()
			client.InstallBatchScatterSupport()
			runTestSplitAndScatterWith(t, client, needEncodeKey)
		})
		t.Run("BackwardCompatibility", func(t *testing.T) {
			client := initTestClient()
			runTestSplitAndScatterWith(t, client, needEncodeKey)
		})
	}

}

func runTestSplitAndScatterWith(t *testing.T, client *TestClient, needEncodeKey bool) {
	ranges := initRanges()
	rewriteRules := initRewriteRules()
	regionSplitter := NewRegionSplitter(client)

	ctx := context.Background()
	err := regionSplitter.Split(ctx, ranges, rewriteRules, needEncodeKey, func(key [][]byte) {}) // TODO: add test case for "isRawKV=true"
	require.NoError(t, err)
	regions := client.GetAllRegions()
	if !validateRegions(regions) {
		for _, region := range regions {
			t.Logf("region: %v\n", region.Region)
		}
		t.Log("get wrong result")
		t.Fail()
	}
	regionInfos := make([]*RegionInfo, 0, len(regions))
	for _, info := range regions {
		regionInfos = append(regionInfos, info)
	}
	scattered := map[uint64]bool{}
	const alwaysFailedRegionID = 1
	client.injectInScatter = func(regionInfo *RegionInfo) error {
		if _, ok := scattered[regionInfo.Region.Id]; !ok || regionInfo.Region.Id == alwaysFailedRegionID {
			scattered[regionInfo.Region.Id] = false
			return status.Errorf(codes.Unknown, "region %d is not fully replicated", regionInfo.Region.Id)
		}
		scattered[regionInfo.Region.Id] = true
		return nil
	}
	regionSplitter.ScatterRegions(ctx, regionInfos)
	for key := range regions {
		if key == alwaysFailedRegionID {
			require.Falsef(t, scattered[key], "always failed region %d was scattered successfully", key)
		} else if !scattered[key] {
			t.Fatalf("region %d has not been scattered: %#v", key, regions[key])
		}
	}
}

// region: [, aay), [aay, bba), [bba, bbh), [bbh, cca), [cca, )
func initTestClient() *TestClient {
	peers := make([]*metapb.Peer, 1)
	peers[0] = &metapb.Peer{
		Id:      1,
		StoreId: 1,
	}
	keys := [6]string{"", "aay", "bba", "bbh", "cca", ""}
	regions := make(map[uint64]*RegionInfo)
	for i := uint64(1); i < 6; i++ {
		startKey := []byte(keys[i-1])
		if len(startKey) != 0 {
			startKey = codec.EncodeBytes([]byte{}, startKey)
		}
		endKey := []byte(keys[i])
		if len(endKey) != 0 {
			endKey = codec.EncodeBytes([]byte{}, endKey)
		}
		regions[i] = &RegionInfo{
			Region: &metapb.Region{
				Id:       i,
				Peers:    peers,
				StartKey: startKey,
				EndKey:   endKey,
			},
		}
	}
	stores := make(map[uint64]*metapb.Store)
	stores[1] = &metapb.Store{
		Id: 1,
	}
	return NewTestClient(stores, regions, 6)
}

// range: [aaa, aae), [aae, aaz), [ccd, ccf), [ccf, ccj)
func initRanges() []rtree.Range {
	var ranges [4]rtree.Range
	ranges[0] = rtree.Range{
		StartKey: []byte("aaa"),
		EndKey:   []byte("aae"),
	}
	ranges[1] = rtree.Range{
		StartKey: []byte("aae"),
		EndKey:   []byte("aaz"),
	}
	ranges[2] = rtree.Range{
		StartKey: []byte("ccd"),
		EndKey:   []byte("ccf"),
	}
	ranges[3] = rtree.Range{
		StartKey: []byte("ccf"),
		EndKey:   []byte("ccj"),
	}
	return ranges[:]
}

func initRewriteRules() *RewriteRules {
	var rules [2]*import_sstpb.RewriteRule
	rules[0] = &import_sstpb.RewriteRule{
		OldKeyPrefix: []byte("aa"),
		NewKeyPrefix: []byte("xx"),
	}
	rules[1] = &import_sstpb.RewriteRule{
		OldKeyPrefix: []byte("cc"),
		NewKeyPrefix: []byte("bb"),
	}
	return &RewriteRules{
		Data: rules[:],
	}
}

// expected regions after split:
//
//	[, aay), [aay, bba), [bba, bbf), [bbf, bbh), [bbh, bbj),
//	[bbj, cca), [cca, xxe), [xxe, xxz), [xxz, )
func validateRegions(regions map[uint64]*RegionInfo) bool {
	keys := [...]string{"", "aay", "bba", "bbf", "bbh", "bbj", "cca", "xxe", "xxz", ""}
	if len(regions) != len(keys)-1 {
		return false
	}
FindRegion:
	for i := 1; i < len(keys); i++ {
		for _, region := range regions {
			startKey := []byte(keys[i-1])
			if len(startKey) != 0 {
				startKey = codec.EncodeBytes([]byte{}, startKey)
			}
			endKey := []byte(keys[i])
			if len(endKey) != 0 {
				endKey = codec.EncodeBytes([]byte{}, endKey)
			}
			if bytes.Equal(region.Region.GetStartKey(), startKey) &&
				bytes.Equal(region.Region.GetEndKey(), endKey) {
				continue FindRegion
			}
		}
		return false
	}
	return true
}

func TestNeedSplit(t *testing.T) {
	for _, needEncodeKey := range []bool{false, true} {
		encode := func(in []byte) []byte {
			if !needEncodeKey {
				return in
			}
			return codec.EncodeBytes([]byte{}, in)
		}

		regions := []*RegionInfo{
			{
				Region: &metapb.Region{
					StartKey: encode([]byte("b")),
					EndKey:   encode([]byte("d")),
				},
			},
		}
		// Out of region
		require.Nil(t, NeedSplit([]byte("a"), regions, needEncodeKey))
		// Region start key
		require.Nil(t, NeedSplit([]byte("b"), regions, needEncodeKey))
		// In region
		region := NeedSplit([]byte("c"), regions, needEncodeKey)
		require.Equal(t, 0, bytes.Compare(region.Region.GetStartKey(), encode([]byte("b"))))
		require.Equal(t, 0, bytes.Compare(region.Region.GetEndKey(), encode([]byte("d"))))
		// Region end key
		require.Nil(t, NeedSplit([]byte("d"), regions, needEncodeKey))
		// Out of region
		require.Nil(t, NeedSplit([]byte("e"), regions, needEncodeKey))
	}
}

func TestRegionConsistency(t *testing.T) {
	cases := []struct {
		startKey []byte
		endKey   []byte
		err      string
		regions  []*RegionInfo
	}{
		{
			codec.EncodeBytes([]byte{}, []byte("a")),
			codec.EncodeBytes([]byte{}, []byte("a")),
			"scan region return empty result, startKey: (.*?), endKey: (.*?)",
			[]*RegionInfo{},
		},
		{
			codec.EncodeBytes([]byte{}, []byte("a")),
			codec.EncodeBytes([]byte{}, []byte("a")),
			"first region's startKey > startKey, startKey: (.*?), regionStartKey: (.*?)",
			[]*RegionInfo{
				{
					Region: &metapb.Region{
						StartKey: codec.EncodeBytes([]byte{}, []byte("b")),
						EndKey:   codec.EncodeBytes([]byte{}, []byte("d")),
					},
				},
			},
		},
		{
			codec.EncodeBytes([]byte{}, []byte("b")),
			codec.EncodeBytes([]byte{}, []byte("e")),
			"last region's endKey < endKey, endKey: (.*?), regionEndKey: (.*?)",
			[]*RegionInfo{
				{
					Region: &metapb.Region{
						StartKey: codec.EncodeBytes([]byte{}, []byte("b")),
						EndKey:   codec.EncodeBytes([]byte{}, []byte("d")),
					},
				},
			},
		},
		{
			codec.EncodeBytes([]byte{}, []byte("c")),
			codec.EncodeBytes([]byte{}, []byte("e")),
			"region endKey not equal to next region startKey(.*?)",
			[]*RegionInfo{
				{
					Region: &metapb.Region{
						StartKey: codec.EncodeBytes([]byte{}, []byte("b")),
						EndKey:   codec.EncodeBytes([]byte{}, []byte("d")),
					},
				},
				{
					Region: &metapb.Region{
						StartKey: codec.EncodeBytes([]byte{}, []byte("e")),
						EndKey:   codec.EncodeBytes([]byte{}, []byte("f")),
					},
				},
			},
		},
	}
	for _, ca := range cases {
		err := CheckRegionConsistency(ca.startKey, ca.endKey, ca.regions)
		require.Error(t, err)
		require.Regexp(t, ca.err, err.Error())
	}
}
