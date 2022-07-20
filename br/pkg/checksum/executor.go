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
	"github.com/tikv/client-go/v2/rawkv"
	"github.com/tikv/migration/br/pkg/backup"
	"github.com/tikv/migration/br/pkg/gluetikv"
	"github.com/tikv/migration/br/pkg/logutil"
	"github.com/tikv/migration/br/pkg/utils"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

func UpdateChecksum(c *rawkv.RawChecksum, crc64Xor, totalKvs, totalBytes uint64) {
	c.Crc64Xor ^= crc64Xor
	c.TotalKvs += totalKvs
	c.TotalBytes += totalBytes
}

type StorageChecksumMethod int32

const (
	StorageChecksumCommand StorageChecksumMethod = 0
	StorageScanCommand     StorageChecksumMethod = 1
)

type ChecksumClient interface {
	Checksum(ctx context.Context, startKey, endKey []byte, options ...rawkv.RawOption) (check rawkv.RawChecksum, err error)
	Scan(ctx context.Context, startKey, endKey []byte, limit int, options ...rawkv.RawOption) (keys [][]byte, values [][]byte, err error)
	Close() error
}

// ExecutorBuilder is used to build
type Executor struct {
	keyRanges      []*utils.KeyRange
	apiVersion     kvrpcpb.APIVersion
	checksumClient ChecksumClient
	concurrency    uint
}

// NewExecutorBuilder returns a new executor builder.
func NewExecutor(ctx context.Context, keyRanges []*utils.KeyRange, pdAddrs []string, apiVersion kvrpcpb.APIVersion,
	concurrency uint) (*Executor, error) {
	rawkvClient, err := rawkv.NewClientWithOpts(ctx, pdAddrs, rawkv.WithAPIVersion(apiVersion))
	if err != nil {
		return nil, nil
	}
	return &Executor{
		keyRanges:      keyRanges,
		apiVersion:     apiVersion,
		checksumClient: rawkvClient,
		concurrency:    concurrency,
	}, nil
}

const (
	MaxScanCntLimit = 1024 // limited by grpc message size
)

// doScanChecksumOnRange scan all key/value in keyRange and format it to apiv2 format, then calc checksum
// ATTENTION: just support call this func in apiv1/v1ttl store.
func (exec *Executor) doScanChecksumOnRange(
	ctx context.Context,
	keyRange *utils.KeyRange,
) (rawkv.RawChecksum, error) {
	if exec.apiVersion != kvrpcpb.APIVersion_V1 && exec.apiVersion != kvrpcpb.APIVersion_V1TTL {
		return rawkv.RawChecksum{}, errors.New("only support scan checksum on apiv1/v1ttl")
	}
	curStart := keyRange.Start
	checksum := rawkv.RawChecksum{}
	digest := crc64.New(crc64.MakeTable(crc64.ECMA))
	for {
		keys, values, err := exec.checksumClient.Scan(ctx, curStart, keyRange.End, MaxScanCntLimit)
		if err != nil {
			return rawkv.RawChecksum{}, err
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

func (exec *Executor) doChecksumOnRange(
	ctx context.Context,
	keyRange *utils.KeyRange,
) (rawkv.RawChecksum, error) {
	// rawkv client accept user key without prefix, convert to v1 format.
	if exec.apiVersion == kvrpcpb.APIVersion_V2 {
		keyRange = utils.ConvertBackupConfigKeyRange(keyRange.Start, keyRange.End, kvrpcpb.APIVersion_V2, kvrpcpb.APIVersion_V1)
	}
	return exec.checksumClient.Checksum(ctx, keyRange.Start, keyRange.End)
}

// Execute executes a checksum executor.
func (exec *Executor) Execute(
	ctx context.Context,
	expect rawkv.RawChecksum,
	method StorageChecksumMethod,
	progressCallBack func(backup.ProgressUnit),
) error {
	storageChecksum := rawkv.RawChecksum{}
	lock := sync.Mutex{}
	workerPool := utils.NewWorkerPool(exec.concurrency, "Ranges")
	eg, ectx := errgroup.WithContext(ctx)
	for _, r := range exec.keyRanges {
		keyRange := r // copy to another variable in case it's overwritten
		workerPool.ApplyOnErrorGroup(eg, func() error {
			var err error
			var ret rawkv.RawChecksum
			if method == StorageChecksumCommand {
				ret, err = exec.doChecksumOnRange(ectx, keyRange)
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
			UpdateChecksum(&storageChecksum, ret.Crc64Xor, ret.TotalKvs, ret.TotalBytes)
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

func (exec *Executor) Close() {
	if exec.checksumClient != nil {
		exec.checksumClient.Close()
	}
}

func Run(ctx context.Context, cmdName string,
	executor *Executor, method StorageChecksumMethod, expect rawkv.RawChecksum) error {
	if executor.apiVersion != kvrpcpb.APIVersion_V1 {
		fmt.Printf("\033[1;37;41m%s\033[0m\n", "Warning: TiKV cluster is TTL enabled, checksum may be mismatch if some data expired during backup/restore.")
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
