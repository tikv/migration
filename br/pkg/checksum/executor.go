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
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/rawkv"
	"github.com/tikv/migration/br/pkg/backup"
	berrors "github.com/tikv/migration/br/pkg/errors"
	"github.com/tikv/migration/br/pkg/gluetikv"
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

func (c *Checksum) Update(crc64Xor, totalKvs, totalBytes uint64) {
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
	keyRanges   []*utils.KeyRange
	pdAddrs     []string
	apiVersion  kvrpcpb.APIVersion
	pdClient    pd.Client
	concurrency uint
}

// NewExecutorBuilder returns a new executor builder.
func NewExecutor(keyRanges []*utils.KeyRange, pdAddrs []string, pdClient pd.Client, apiVersion kvrpcpb.APIVersion,
	concurrency uint) *Executor {
	return &Executor{
		keyRanges:   keyRanges,
		pdAddrs:     pdAddrs,
		apiVersion:  apiVersion,
		pdClient:    pdClient,
		concurrency: concurrency,
	}
}

func adjustRegionRange(start, end, regionStart, regionEnd []byte) ([]byte, []byte) {
	retStart := start
	if string(retStart) < string(regionStart) {
		retStart = regionStart
	}
	retEnd := end
	if len(retEnd) == 0 || (len(regionEnd) != 0 && string(retEnd) > string(regionEnd)) {
		retEnd = regionEnd
	}
	return retStart, retEnd
}

const (
	MaxScanCntLimit = 1024 // limited by grpc message size
)

// doScanChecksumOnRange scan all key/value in keyRange and format it to apiv2 format, then calc checksum
// ATTENTION: just support call this func in apiv1/v1ttl store.
func (exec *Executor) doScanChecksumOnRange(
	ctx context.Context,
	keyRange *utils.KeyRange,
) (Checksum, error) {
	if exec.apiVersion != kvrpcpb.APIVersion_V1 && exec.apiVersion != kvrpcpb.APIVersion_V1TTL {
		return Checksum{}, errors.New("not support scan checksum on apiv1/v1ttl")
	}
	rawClient, err := rawkv.NewClient(ctx, exec.pdAddrs, config.Security{})
	if err != nil {
		return Checksum{}, err
	}
	defer rawClient.Close()
	curStart := keyRange.Start
	checksum := Checksum{}
	digest := crc64.New(crc64.MakeTable(crc64.ECMA))
	for {
		keys, values, err := rawClient.Scan(ctx, curStart, keyRange.End, MaxScanCntLimit)
		if err != nil {
			return Checksum{}, err
		}
		for i, key := range keys {
			newKey := utils.FormatAPIV2Key(key, false)
			// keep the same with tikv-server: https://docs.rs/crc64fast/latest/crc64fast/
			digest.Reset()
			digest.Write(newKey)
			digest.Write(values[i])
			checksum.Crc64Xor ^= digest.Sum64()
			checksum.TotalKvs += 1
			checksum.TotalBytes += (uint64)(len(newKey) + len(values[i]))
		}
		if len(keys) < MaxScanCntLimit {
			break // reach the end
		}
		// append '0' to avoid getting the duplicated kv
		curStart = append(keys[len(keys)-1], '0')
	}
	return checksum, nil
}

func checkDuplicateRegion(ctx context.Context, regionInfos []*restore.RegionInfo) error {
	regionMap := make(map[uint64]*restore.RegionInfo)
	for _, region := range regionInfos {
		regionMap[region.Region.GetId()] = region
	}
	if len(regionMap) != len(regionInfos) {
		logutil.CL(ctx).Error("get duplicated regions", zap.Int("region cnt", len(regionInfos)),
			zap.Int("real region cnt", len(regionMap)))
		return errors.New("get duplicated region info")
	}
	return nil
}

func (exec *Executor) doChecksumOnRegion(
	ctx context.Context,
	regionInfo *restore.RegionInfo,
	keyRange *utils.KeyRange,
	splitClient restore.SplitClient,
) (Checksum, error) {
	var peer *metapb.Peer
	if regionInfo.Leader != nil {
		peer = regionInfo.Leader
	} else {
		if len(regionInfo.Region.Peers) == 0 {
			return Checksum{}, errors.Annotate(berrors.ErrRestoreNoPeer, "region does not have peer")
		}
		peer = regionInfo.Region.Peers[0]
	}
	storeID := peer.GetStoreId()
	store, err := splitClient.GetStore(ctx, storeID)
	if err != nil {
		return Checksum{}, errors.Trace(err)
	}
	conn, err := grpc.Dial(store.GetAddress(), grpc.WithInsecure())
	if err != nil {
		return Checksum{}, errors.Trace(err)
	}
	defer conn.Close()

	client := tikvpb.NewTikvClient(conn)
	rangeStart, rangeEnd := adjustRegionRange(keyRange.Start, keyRange.End, regionInfo.Region.StartKey, regionInfo.Region.EndKey)
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
		return Checksum{}, errors.Trace(err)
	}
	if resp.GetRegionError() != nil {
		if resp.GetRegionError().GetEpochNotMatch() != nil {
			return Checksum{}, berrors.ErrKVEpochNotMatch
		} else if resp.GetRegionError().GetNotLeader() != nil {
			return Checksum{}, berrors.ErrKVNotLeader
		} else {
			return Checksum{}, errors.New(resp.GetRegionError().String())
		}
	}
	if resp.GetError() != "" {
		return Checksum{}, errors.New(resp.GetError())
	}
	return Checksum{
		Crc64Xor:   resp.GetChecksum(),
		TotalKvs:   resp.GetTotalKvs(),
		TotalBytes: resp.GetTotalBytes(),
	}, nil
}

func (exec *Executor) doChecksumOnRange(
	ctx context.Context,
	splitClient restore.SplitClient,
	keyRange *utils.KeyRange,
) (Checksum, error) {
	// input key range is rawkey with 'r' prefix, but no encoding, region is encoded.
	if exec.apiVersion == kvrpcpb.APIVersion_V2 {
		keyRange = utils.EncodeKeyRange(keyRange.Start, keyRange.End)
	}
	regionInfos, err := restore.PaginateScanRegion(ctx, splitClient, keyRange.Start, keyRange.End, restore.ScanRegionPaginationLimit)
	if err != nil {
		return Checksum{}, errors.Trace(err)
	}
	err = checkDuplicateRegion(ctx, regionInfos)
	if err != nil {
		return Checksum{}, errors.Trace(err)
	}
	// at most cases, there should be just one region.
	checksum := Checksum{}
	for _, regionInfo := range regionInfos {
		ret, err := exec.doChecksumOnRegion(ctx, regionInfo, keyRange, splitClient)
		if err != nil {
			return Checksum{}, err
		}
		checksum.Update(ret.Crc64Xor, ret.TotalKvs, ret.TotalBytes)
	}
	logutil.CL(ctx).Info("finish checksum on range.",
		logutil.Key("RangeStart", keyRange.Start),
		logutil.Key("RangeEnd", keyRange.End),
		zap.Int("RegionCnt", len(regionInfos)))

	return checksum, nil
}

func (exec *Executor) doChecksumOnRangeWithRetry(
	ctx context.Context,
	keyRange *utils.KeyRange,
) (Checksum, error) {
	// reuse restore split codes, but do nothing with split.
	splitClient := restore.NewSplitClient(exec.pdClient, nil, true)
	checksumRet := Checksum{}
	errRetry := utils.WithRetry(
		ctx,
		func() error {
			ret, err := exec.doChecksumOnRange(ctx, splitClient, keyRange)
			if err != nil {
				logutil.CL(ctx).Error("checksum on range failed, will retry.", logutil.Key("Start", keyRange.Start),
					logutil.Key("End", keyRange.End))
				return err
			}
			checksumRet = ret
			return nil
		},
		utils.NewChecksumBackoffer(),
	)
	return checksumRet, errRetry
}

// Execute executes a checksum executor.
func (exec *Executor) Execute(
	ctx context.Context,
	expect Checksum,
	method StorageChecksumMethod,
	progressCallBack func(backup.ProgressUnit),
) error {
	storageChecksum := Checksum{}
	lock := sync.Mutex{}
	workerPool := utils.NewWorkerPool(exec.concurrency, "Ranges")
	eg, ectx := errgroup.WithContext(ctx)
	for _, r := range exec.keyRanges {
		keyRange := r // copy to another variable in case it's overwritten
		workerPool.ApplyOnErrorGroup(eg, func() error {
			var err error
			var ret Checksum
			if method == StorageChecksumCommand {
				ret, err = exec.doChecksumOnRangeWithRetry(ectx, keyRange)
			} else if method == StorageScanCommand {
				ret, err = exec.doScanChecksumOnRange(ectx, keyRange)
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
			logutil.CL(ctx).Info("range checksum finish",
				logutil.Key("StartKey", keyRange.Start),
				logutil.Key("EndKey", keyRange.End),
				zap.Reflect("checksum", ret))
			lock.Lock()
			storageChecksum.Update(ret.Crc64Xor, ret.TotalKvs, ret.TotalBytes)
			lock.Unlock()
			progressCallBack(backup.RangeUnit)
			return nil
		})
	}
	err := eg.Wait()
	if err != nil {
		return err
	}
	if expect != storageChecksum {
		logutil.CL(ctx).Error("checksum fails", zap.Reflect("backup files checksum", expect),
			zap.Reflect("storage checksum", storageChecksum), zap.Int("range cnt", len(exec.keyRanges)))
		return errors.New("Checksum mismatch")
	}
	return nil
}

func Run(ctx context.Context, cmdName string,
	executor *Executor, method StorageChecksumMethod, expect Checksum) error {
	if executor.apiVersion != kvrpcpb.APIVersion_V1 {
		fmt.Printf("\033[1;37;41m%s\033[0m\n", "TiKV cluster is TTL enabled, checksum may be mismatch if some data expired during backup/restore.")
	}
	glue := new(gluetikv.Glue)
	updateCh := glue.StartProgress(ctx, cmdName+" Checksum", int64(len(executor.keyRanges)), false)
	progressCallBack := func(unit backup.ProgressUnit) {
		updateCh.Inc()
	}
	err := executor.Execute(ctx, expect, method, progressCallBack)
	updateCh.Close()
	if err != nil {
		fmt.Printf("%s succeeded, but checksum failed, err:%v.\n",
			cmdName, errors.Cause((err)))
		return errors.Trace(err)
	}
	return nil
}
