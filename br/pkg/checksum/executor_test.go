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
	"bytes"
	"context"
	"fmt"
	"hash/crc64"
	"sort"
	"testing"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/rawkv"
	"github.com/tikv/migration/br/pkg/backup"
	"github.com/tikv/migration/br/pkg/utils"
)

type TestChecksumClient struct {
	store map[string]string
}

func (c *TestChecksumClient) PutBatch(ctx context.Context, keys, values []string) {
	for i, key := range keys {
		c.store[key] = values[i]
	}
}

func (c *TestChecksumClient) Checksum(ctx context.Context, startKey, endKey []byte, options ...rawkv.RawOption) (check rawkv.RawChecksum, err error) {
	digest := crc64.New(crc64.MakeTable(crc64.ECMA))
	checksum := rawkv.RawChecksum{}
	allKeys := []string{}
	for key := range c.store {
		allKeys = append(allKeys, key)
	}
	sort.Strings(allKeys)
	for _, key := range allKeys {
		if bytes.Compare([]byte(key), startKey) < 0 {
			continue
		}
		if len(endKey) > 0 && bytes.Compare([]byte(key), endKey) >= 0 {
			continue
		}
		value := c.store[key]
		digest.Reset()
		digest.Write([]byte(key))
		digest.Write([]byte(value))
		checksum.Crc64Xor ^= digest.Sum64()
		checksum.TotalKvs += 1
		checksum.TotalBytes += (uint64)(len(key) + len(value))
	}
	return checksum, nil
}
func (c *TestChecksumClient) Scan(ctx context.Context, startKey, endKey []byte, limit int, options ...rawkv.RawOption) (keys [][]byte, values [][]byte, err error) {
	retKeys := [][]byte{}
	retValues := [][]byte{}
	allKeys := []string{}
	for key := range c.store {
		allKeys = append(allKeys, key)
	}
	sort.Strings(allKeys)
	for _, key := range allKeys {
		if bytes.Compare([]byte(key), startKey) < 0 {
			continue
		}
		if len(endKey) > 0 && bytes.Compare([]byte(key), endKey) >= 0 {
			continue
		}
		value := c.store[key]
		retKeys = append(retKeys, []byte(key))
		retValues = append(retValues, []byte(value))
	}
	return retKeys, retValues, nil
}

func (c *TestChecksumClient) Close() error {
	return nil
}

func GenerateTestData(keyIdx int64) (key, value string) {
	key = fmt.Sprintf("indexInfo_:%019d", keyIdx)
	value = fmt.Sprintf("v0%020d", keyIdx)
	return
}

func BatchGenerateData(keyCnt int64) (keys, values []string) {
	keys = make([]string, 0, keyCnt)
	values = make([]string, 0, keyCnt)
	for idx := int64(0); idx < keyCnt; idx++ {
		key, value := GenerateTestData(idx)
		keys = append(keys, key)
		values = append(values, value)
	}
	return
}

func TestChecksumAPIV1(t *testing.T) {
	ctx := context.TODO()
	client := TestChecksumClient{
		store: make(map[string]string),
	}
	keyCnt := int64(10240)
	rangeCnt := int64(5)
	keys, values := BatchGenerateData(keyCnt)
	client.PutBatch(ctx, keys, values)

	keyRanges := []*utils.KeyRange{}
	for i := int64(0); i < rangeCnt; i++ {
		start, _ := GenerateTestData(keyCnt / rangeCnt * i)
		end, _ := GenerateTestData(keyCnt / rangeCnt * (i + 1))
		keyRanges = append(keyRanges, &utils.KeyRange{
			Start: []byte(start),
			End:   []byte(end),
		})
	}
	executor := Executor{
		keyRanges:      keyRanges,
		apiVersion:     kvrpcpb.APIVersion_V1,
		checksumClient: &client,
		concurrency:    5,
	}
	rawChecksum, err := client.Checksum(ctx, []byte("a"), []byte("z"))
	require.Nil(t, err)
	callbakcCnt := int64(0)
	callback := func(unit backup.ProgressUnit) {
		callbakcCnt += 1
	}
	err = executor.Execute(ctx, rawChecksum, StorageChecksumCommand, callback)
	require.Nil(t, err)
	require.Equal(t, callbakcCnt, rangeCnt)

	callbakcCnt = int64(0)
	expectConvert := rawkv.RawChecksum{}
	callbakcCnt = 0
	digest := crc64.New(crc64.MakeTable(crc64.ECMA))
	for i, key := range keys {
		digest.Reset()
		newKey := utils.FormatAPIV2Key([]byte(key), false)
		digest.Write(newKey)
		digest.Write([]byte(values[i]))
		expectConvert.Crc64Xor ^= digest.Sum64()
		expectConvert.TotalKvs += 1
		expectConvert.TotalBytes += (uint64)(len(newKey) + len(values[i]))
	}
	err = executor.Execute(ctx, expectConvert, StorageScanCommand, callback)
	require.Nil(t, err)
	require.Equal(t, callbakcCnt, rangeCnt)

	callbakcCnt = int64(0)
	apiv2KeyRanges := []*utils.KeyRange{}
	for i := int64(0); i < rangeCnt; i++ {
		start, _ := GenerateTestData(keyCnt / rangeCnt * i)
		end, _ := GenerateTestData(keyCnt / rangeCnt * (i + 1))
		apiv2KeyRanges = append(apiv2KeyRanges, &utils.KeyRange{
			Start: utils.FormatAPIV2Key([]byte(start), false),
			End:   utils.FormatAPIV2Key([]byte(end), false),
		})
	}
	executor = Executor{
		keyRanges:      apiv2KeyRanges,
		apiVersion:     kvrpcpb.APIVersion_V2,
		checksumClient: &client,
		concurrency:    5,
	}
	err = executor.Execute(ctx, rawChecksum, StorageChecksumCommand, callback)
	require.Nil(t, err)
	require.Equal(t, callbakcCnt, rangeCnt)
}
