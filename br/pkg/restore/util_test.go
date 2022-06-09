// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore_test

import (
	"context"
	"encoding/binary"
	"testing"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util/codec"
	"github.com/tikv/migration/br/pkg/restore"
)

func TestGetSSTMetaFromFile(t *testing.T) {
	file := &backuppb.File{
		Name:     "file_write.sst",
		StartKey: []byte("t1a"),
		EndKey:   []byte("t1ccc"),
	}
	rule := &import_sstpb.RewriteRule{
		OldKeyPrefix: []byte("t1"),
		NewKeyPrefix: []byte("t2"),
	}
	region := &metapb.Region{
		StartKey: []byte("t2abc"),
		EndKey:   []byte("t3a"),
	}
	sstMeta := restore.GetSSTMetaFromFile([]byte{}, file, region, rule)
	require.Equal(t, "t2abc", string(sstMeta.GetRange().GetStart()))
	require.Equal(t, "t2\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff", string(sstMeta.GetRange().GetEnd()))
}

func TestPaginateScanRegion(t *testing.T) {
	peers := make([]*metapb.Peer, 1)
	peers[0] = &metapb.Peer{
		Id:      1,
		StoreId: 1,
	}
	stores := make(map[uint64]*metapb.Store)
	stores[1] = &metapb.Store{
		Id: 1,
	}

	makeRegions := func(num uint64) (map[uint64]*restore.RegionInfo, []*restore.RegionInfo) {
		regionsMap := make(map[uint64]*restore.RegionInfo, num)
		regions := make([]*restore.RegionInfo, 0, num)
		endKey := make([]byte, 8)
		for i := uint64(0); i < num-1; i++ {
			ri := &restore.RegionInfo{
				Region: &metapb.Region{
					Id:    i + 1,
					Peers: peers,
				},
			}

			if i != 0 {
				startKey := make([]byte, 8)
				binary.BigEndian.PutUint64(startKey, i)
				ri.Region.StartKey = codec.EncodeBytes([]byte{}, startKey)
			}
			endKey = make([]byte, 8)
			binary.BigEndian.PutUint64(endKey, i+1)
			ri.Region.EndKey = codec.EncodeBytes([]byte{}, endKey)

			regionsMap[i] = ri
			regions = append(regions, ri)
		}

		if num == 1 {
			endKey = []byte{}
		} else {
			endKey = codec.EncodeBytes([]byte{}, endKey)
		}
		ri := &restore.RegionInfo{
			Region: &metapb.Region{
				Id:       num,
				Peers:    peers,
				StartKey: endKey,
				EndKey:   []byte{},
			},
		}
		regionsMap[num] = ri
		regions = append(regions, ri)

		return regionsMap, regions
	}

	ctx := context.Background()
	regionMap := make(map[uint64]*restore.RegionInfo)
	var regions []*restore.RegionInfo
	_, err := restore.PaginateScanRegion(ctx, NewTestClient(stores, regionMap, 0), []byte{}, []byte{}, 3)
	require.Error(t, err)
	require.Regexp(t, ".*scan region return empty result.*", err.Error())

	regionMap, regions = makeRegions(1)
	batch, err := restore.PaginateScanRegion(ctx, NewTestClient(stores, regionMap, 0), []byte{}, []byte{}, 3)
	require.NoError(t, err)
	require.Equal(t, regions, batch)

	regionMap, regions = makeRegions(2)
	batch, err = restore.PaginateScanRegion(ctx, NewTestClient(stores, regionMap, 0), []byte{}, []byte{}, 3)
	require.NoError(t, err)
	require.Equal(t, regions, batch)

	regionMap, regions = makeRegions(3)
	batch, err = restore.PaginateScanRegion(ctx, NewTestClient(stores, regionMap, 0), []byte{}, []byte{}, 3)
	require.NoError(t, err)
	require.Equal(t, regions, batch)

	regionMap, regions = makeRegions(8)
	batch, err = restore.PaginateScanRegion(ctx, NewTestClient(stores, regionMap, 0), []byte{}, []byte{}, 3)
	require.NoError(t, err)
	require.Equal(t, regions, batch)

	regionMap, regions = makeRegions(8)
	batch, err = restore.PaginateScanRegion(
		ctx, NewTestClient(stores, regionMap, 0), regions[1].Region.StartKey, []byte{}, 3)
	require.NoError(t, err)
	require.Equal(t, regions[1:], batch)

	batch, err = restore.PaginateScanRegion(
		ctx, NewTestClient(stores, regionMap, 0), []byte{}, regions[6].Region.EndKey, 3)
	require.NoError(t, err)
	require.Equal(t, regions[:7], batch)

	batch, err = restore.PaginateScanRegion(
		ctx, NewTestClient(stores, regionMap, 0), regions[1].Region.StartKey, regions[1].Region.EndKey, 3)
	require.NoError(t, err)
	require.Equal(t, regions[1:2], batch)

	_, err = restore.PaginateScanRegion(ctx, NewTestClient(stores, regionMap, 0), []byte{2}, []byte{1}, 3)
	require.Error(t, err)
	require.Regexp(t, ".*startKey >= endKey.*", err.Error())

	// make the regionMap losing some region, this will cause scan region check fails
	delete(regionMap, uint64(3))
	_, err = restore.PaginateScanRegion(ctx, NewTestClient(stores, regionMap, 0), regions[1].Region.EndKey, regions[5].Region.EndKey, 3)
	require.Error(t, err)
	require.Regexp(t, ".*region endKey not equal to next region startKey.*", err.Error())

}
