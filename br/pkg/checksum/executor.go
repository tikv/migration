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
type ChecksumExecutor struct {
	startKey       []byte
	endKey         []byte
	expect         *Checksum
	pdClient       pd.Client
	concurrency    uint
	checksumMethod StorageChecksumMethod
	mutex          sync.Mutex // lock for update checksum
}

// NewExecutorBuilder returns a new executor builder.
func NewChecksumExecutor(startKey, endKey []byte, expect *Checksum, pdClient pd.Client,
	concurrency uint, method StorageChecksumMethod) *ChecksumExecutor {
	return &ChecksumExecutor{
		startKey:       startKey,
		endKey:         endKey,
		expect:         expect,
		pdClient:       pdClient,
		concurrency:    concurrency,
		checksumMethod: method,
	}
}

func adjustRegionRange(startKey, endKey []byte) ([]byte, []byte) {
	if len(startKey) == 0 {
		startKey = append(startKey, 'r')
	}
	if len(endKey) == 0 || endKey[0] == 's' {
		endKey = []byte{'s'}
	}
	return startKey, endKey
}

func (exec *ChecksumExecutor) doChecksumOnRegion(
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
	rangeStart, rangeEnd := adjustRegionRange(regionInfo.Region.StartKey, regionInfo.Region.EndKey)
	resp, err := client.RawChecksum(ctx, &kvrpcpb.RawChecksumRequest{
		Context: &kvrpcpb.Context{
			RegionId:    regionInfo.Region.Id,
			RegionEpoch: regionInfo.Region.RegionEpoch,
			Peer:        peer,
			ApiVersion:  kvrpcpb.APIVersion_V2,
		},
		Algorithm: kvrpcpb.ChecksumAlgorithm_Crc64_Xor,
		Ranges: []*kvrpcpb.KeyRange{{
			StartKey: rangeStart,
			EndKey:   rangeEnd,
		}},
	})
	logutil.CL(ctx).Info("do checksum finish", zap.String("start", string(rangeStart)),
		zap.String("end", string(rangeEnd)),
		zap.String("region start", string(regionInfo.Region.StartKey)),
		zap.String("region end", string(regionInfo.Region.EndKey)),
		zap.Error(err))
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
	exec.mutex.Lock()
	defer exec.mutex.Unlock()
	finalChecksum.UpdateChecksum(resp.GetChecksum(), resp.GetTotalKvs(), resp.GetTotalBytes())
	logutil.CL(ctx).Info("region checksum finish", zap.Uint64("region id", regionInfo.Region.Id),
		zap.Reflect("checksum", finalChecksum))
	return nil
}

func (exec *ChecksumExecutor) execChecksumRequest(
	ctx context.Context,
	progressCallBack func(backup.ProgressUnit),
) (*Checksum, error) {
	// reuse restore split codes, but do nothing with split.
	splitClient := restore.NewSplitClient(exec.pdClient, nil, true)
	regionInfos, err := restore.PaginateScanRegion(ctx, splitClient, exec.startKey, exec.endKey, restore.ScanRegionPaginationLimit)
	if err != nil {
		return nil, errors.Trace(err)
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
			err := exec.doChecksumOnRegion(elctx, regionInfo, splitClient, &finalChecksum, progressCallBack)
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
	return &finalChecksum, err
}

// Execute executes a checksum executor.
func (exec *ChecksumExecutor) Execute(
	ctx context.Context,
	progressCallBack func(backup.ProgressUnit),
) error {
	storageChecksum := Checksum{}
	if exec.checksumMethod == StorageChecksumCommand {
		checksum, err := exec.execChecksumRequest(ctx, progressCallBack)
		if err != nil {
			return errors.Trace(err)
		}
		storageChecksum = *checksum
	} else if exec.checksumMethod == StorageScanCommand {
		//TODO: implement checksum with scan
	}
	if *exec.expect != storageChecksum {
		logutil.CL(ctx).Error("checksum fails", zap.Reflect("backup files checksum", *exec.expect),
			zap.Reflect("storage checksum", storageChecksum))
		return errors.New("Checksum mismatch")
	}
	return nil
}
