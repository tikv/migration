// Copyright 2022 PingCAP, Inc.
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

package checksum

import (
	"context"
	"fmt"
	"hash/crc64"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/tikv/migration/br/pkg/backup"
	berrors "github.com/tikv/migration/br/pkg/errors"
	"github.com/tikv/migration/br/pkg/logutil"
	"github.com/tikv/migration/br/pkg/restore"
	"github.com/tikv/migration/br/pkg/utils"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

type Checksum struct {
	Crc64Xor   uint64
	TotalKvs   uint64
	TotalBytes uint64
}

func (c *Checksum) UpdateChecksum(crc64Xor, totalKvs, totalBytes uint64) {
	c.Crc64Xor ^= crc64Xor
	c.TotalKvs += totalKvs
	c.TotalBytes += totalBytes
}

func (c *Checksum) String() string {
	return fmt.Sprintf("crc64xor:%d, totalKvs:%d, totalBytes:%d",
		c.Crc64Xor, c.TotalKvs, c.TotalBytes)
}

type StorageChecksumMethod int32

const (
	StorageChecksumCommand StorageChecksumMethod = 0
	StorageScanCommand     StorageChecksumMethod = 1
)

// ExecutorBuilder is used to build
type Executor struct {
	startKey    []byte
	endKey      []byte
	apiVersion  kvrpcpb.APIVersion
	pdClient    pd.Client
	concurrency uint
	mutex       sync.Mutex // lock for update checksum
}

// NewExecutorBuilder returns a new executor builder.
func NewExecutor(startKey, endKey []byte, apiVersion kvrpcpb.APIVersion, pdClient pd.Client,
	concurrency uint) *Executor {
	return &Executor{
		startKey:    startKey,
		endKey:      endKey,
		apiVersion:  apiVersion,
		pdClient:    pdClient,
		concurrency: concurrency,
	}
}

func adjustRegionRange(startKey, endKey, regionStart, regionEnd []byte) ([]byte, []byte) {
	retStart := startKey
	if string(retStart) < string(regionStart) {
		retStart = regionStart
	}
	retEnd := endKey
	if len(retEnd) == 0 || (len(regionEnd) != 0 && string(retEnd) > string(regionEnd)) {
		retEnd = regionEnd
	}
	return retStart, retEnd
}

func (exec *Executor) doChecksumOnRegion(
	ctx context.Context,
	regionInfo *restore.RegionInfo,
	splitClient restore.SplitClient,
	finalChecksum *Checksum,
	progressCallBack func(backup.ProgressUnit),
) error {
	var peer *metapb.Peer
	if regionInfo.Leader != nil {
		peer = regionInfo.Leader
	} else {
		if len(regionInfo.Region.Peers) == 0 {
			return errors.Annotate(berrors.ErrRestoreNoPeer, "region does not have peer")
		}
		peer = regionInfo.Region.Peers[0]
	}
	storeID := peer.GetStoreId()
	store, err := splitClient.GetStore(ctx, storeID)
	if err != nil {
		return errors.Trace(err)
	}
	conn, err := grpc.Dial(store.GetAddress(), grpc.WithInsecure())
	if err != nil {
		return errors.Trace(err)
	}
	defer conn.Close()

	client := tikvpb.NewTikvClient(conn)
	rangeStart, rangeEnd := adjustRegionRange(exec.startKey, exec.endKey, regionInfo.Region.StartKey, regionInfo.Region.EndKey)
	apiver := exec.apiVersion
	if apiver == kvrpcpb.APIVersion_V1TTL {
		apiver = kvrpcpb.APIVersion_V1
	}
	resp, err := client.RawChecksum(ctx, &kvrpcpb.RawChecksumRequest{
		Context: &kvrpcpb.Context{
			RegionId:    regionInfo.Region.Id,
			RegionEpoch: regionInfo.Region.RegionEpoch,
			Peer:        peer,
			ApiVersion:  apiver,
		},
		Algorithm: kvrpcpb.ChecksumAlgorithm_Crc64_Xor,
		Ranges: []*kvrpcpb.KeyRange{{
			StartKey: rangeStart,
			EndKey:   rangeEnd,
		}},
	})
	if err != nil {
		return errors.Trace(err)
	}
	if resp.GetRegionError() != nil {
		return errors.New(resp.GetRegionError().String())
	}
	if resp.GetError() != "" {
		return errors.New(resp.GetError())
	}
	progressCallBack(backup.RegionUnit)
	exec.mutex.Lock() // lock for updating checksum
	defer exec.mutex.Unlock()
	finalChecksum.UpdateChecksum(resp.GetChecksum(), resp.GetTotalKvs(), resp.GetTotalBytes())
	logutil.CL(ctx).Info("region checksum finish", zap.Uint64("region id", regionInfo.Region.Id),
		logutil.Key("region start", rangeStart),
		logutil.Key("region end", rangeEnd),
		zap.Reflect("checksum", finalChecksum))
	return nil
}

const (
	MaxScanCntLimit = uint32(1024) // limited by grpc message size
)

func (exec *Executor) doScanChecksum(
	ctx context.Context,
	regionInfo *restore.RegionInfo,
	splitClient restore.SplitClient,
	finalChecksum *Checksum,
	progressCallBack func(backup.ProgressUnit),
) error {
	var peer *metapb.Peer
	if regionInfo.Leader != nil {
		peer = regionInfo.Leader
	} else {
		if len(regionInfo.Region.Peers) == 0 {
			return errors.Annotate(berrors.ErrRestoreNoPeer, "region does not have peer")
		}
		peer = regionInfo.Region.Peers[0]
	}
	storeID := peer.GetStoreId()
	store, err := splitClient.GetStore(ctx, storeID)
	if err != nil {
		return errors.Trace(err)
	}
	conn, err := grpc.Dial(store.GetAddress(), grpc.WithInsecure())
	if err != nil {
		return errors.Trace(err)
	}
	defer conn.Close()

	client := tikvpb.NewTikvClient(conn)
	rangeStart, rangeEnd := adjustRegionRange(exec.startKey, exec.endKey, regionInfo.Region.StartKey, regionInfo.Region.EndKey)
	curStart := rangeStart
	firstLoop := true
	checksum := Checksum{}
	digest := crc64.New(crc64.MakeTable(crc64.ECMA))
	apiver := exec.apiVersion
	if apiver == kvrpcpb.APIVersion_V1TTL {
		apiver = kvrpcpb.APIVersion_V1
	}
	kvrpcpbCtx := &kvrpcpb.Context{
		RegionId:    regionInfo.Region.Id,
		RegionEpoch: regionInfo.Region.RegionEpoch,
		Peer:        peer,
		ApiVersion:  apiver,
	}
	for {
		resp, err := client.RawScan(ctx, &kvrpcpb.RawScanRequest{
			Context:  kvrpcpbCtx,
			StartKey: curStart,
			EndKey:   rangeEnd,
			Cf:       "default",
			Limit:    MaxScanCntLimit,
		})
		if err != nil {
			return errors.Trace(err)
		}
		if resp.GetRegionError() != nil {
			return errors.New(resp.GetRegionError().String())
		}
		iterKvPairs := resp.GetKvs()
		if !firstLoop { // first is duplicated
			iterKvPairs = iterKvPairs[1:]
		}
		for _, kvpair := range iterKvPairs {
			if kvpair.Error != nil {
				return errors.New(kvpair.Error.String())
			}
			newKey := utils.FormatAPIV2Key(kvpair.Key, false)
			// keep the same with tikv-server: https://docs.rs/crc64fast/latest/crc64fast/
			digest.Reset()
			digest.Write(newKey)
			digest.Write(kvpair.Value)
			checksum.Crc64Xor ^= digest.Sum64()
			checksum.TotalKvs += 1
			checksum.TotalBytes += (uint64)(len(newKey) + len(kvpair.Value))
		}
		kvCnt := len(iterKvPairs)
		if kvCnt == 0 {
			break
		}
		curStart = iterKvPairs[kvCnt-1].Key
		firstLoop = false
	}

	progressCallBack(backup.RegionUnit)
	exec.mutex.Lock() // lock for updating checksum
	defer exec.mutex.Unlock()
	finalChecksum.UpdateChecksum(checksum.Crc64Xor, checksum.TotalKvs, checksum.TotalBytes)
	logutil.CL(ctx).Info("region checksum finish", zap.Uint64("region id", regionInfo.Region.Id),
		logutil.Key("region start", regionInfo.Region.StartKey),
		logutil.Key("region end", regionInfo.Region.EndKey),
		zap.Reflect("checksum", finalChecksum))
	return nil
}

func (exec *Executor) execChecksumRequest(
	ctx context.Context,
	method StorageChecksumMethod,
	progressCallBack func(backup.ProgressUnit),
) (Checksum, error) {
	// reuse restore split codes, but do nothing with split.
	splitClient := restore.NewSplitClient(exec.pdClient, nil, true)
	regionInfos, err := restore.PaginateScanRegion(ctx, splitClient, exec.startKey, exec.endKey, restore.ScanRegionPaginationLimit)
	if err != nil {
		return Checksum{}, errors.Trace(err)
	}
	// regionInfos maybe duplicated
	regionMap := make(map[uint64]*restore.RegionInfo)
	for _, region := range regionInfos {
		regionMap[region.Region.GetId()] = region
	}
	finalChecksum := Checksum{}
	workerPool := utils.NewWorkerPool(exec.concurrency, "Ranges")
	eg, ectx := errgroup.WithContext(ctx)
	for _, r := range regionMap {
		regionInfo := r // copy to another variable in case it's overwritten
		workerPool.ApplyOnErrorGroup(eg, func() error {
			elctx := logutil.ContextWithField(ectx, logutil.RedactAny("region-id", regionInfo.Region.Id))
			var err error
			if method == StorageChecksumCommand {
				err = exec.doChecksumOnRegion(elctx, regionInfo, splitClient, &finalChecksum, progressCallBack)
			} else if method == StorageScanCommand {
				err = exec.doScanChecksum(elctx, regionInfo, splitClient, &finalChecksum, progressCallBack)
			} else {
				err = errors.New("unsupported checksum method")
			}
			if err != nil {
				// The error due to context cancel, stack trace is meaningless, the stack shall be suspended (also clear)
				if errors.Cause(err) == context.Canceled {
					return errors.SuspendStack(err)
				}
				return errors.Trace(err)
			}
			return nil
		})
	}
	err = eg.Wait()
	return finalChecksum, err
}

// Execute executes a checksum executor.
func (exec *Executor) Execute(
	ctx context.Context,
	expect Checksum,
	method StorageChecksumMethod,
	progressCallBack func(backup.ProgressUnit),
) error {
	storageChecksum, err := exec.execChecksumRequest(ctx, method, progressCallBack)
	if err != nil {
		return errors.Trace(err)
	}
	if expect != storageChecksum {
		logutil.CL(ctx).Error("checksum fails", zap.Reflect("backup files checksum", expect),
			zap.Reflect("storage checksum", storageChecksum))
		return errors.New("Checksum mismatch")
	}
	return nil
}
