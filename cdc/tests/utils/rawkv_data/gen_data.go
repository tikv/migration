// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/log"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/tikv/client-go/v2/rawkv"
	"go.uber.org/zap"
	"golang.org/x/exp/rand"
	"golang.org/x/sync/errgroup"
)

func generateTestData(keyIndex int) (key, value0, value1 []byte) {
	key = []byte(fmt.Sprintf("indexInfo_:_pf01_:_APD0101_:_%019d", keyIndex))
	value0 = []byte{}       // Don't assign nil, which means "NotFound" in CompareAndSwap
	if keyIndex%100 != 42 { // To generate test data with empty value. See https://github.com/tikv/migration/issues/250
		value0 = []byte(fmt.Sprintf("v0_%020d_%v", keyIndex, rand.Uint64()))
	}
	value1 = []byte{}
	if keyIndex%100 != 43 {
		value1 = []byte(fmt.Sprintf("v1_%020d%020d_%v", keyIndex, keyIndex, rand.Uint64()))
	}
	return key, value0, value1
}

func batchGenerateData(keyIndex int, keyCnt int) (keys, values0, values1 [][]byte) {
	keys = make([][]byte, 0, keyCnt)
	values0 = make([][]byte, 0, keyCnt)
	values1 = make([][]byte, 0, keyCnt)
	for idx := keyIndex; idx < keyIndex+keyCnt; idx++ {
		key, value0, value1 := generateTestData(idx)
		keys = append(keys, key)
		values0 = append(values0, value0)
		values1 = append(values1, value1)
	}
	return keys, values0, values1
}

func NewPutCommand() *cobra.Command {
	return &cobra.Command{
		Use:          "put",
		Short:        "Put rawkv data into tikv",
		SilenceUsage: true,
		Args:         cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runPutCmd(cmd)
		},
	}
}

func NewDeleteCommand() *cobra.Command {
	return &cobra.Command{
		Use:          "delete",
		Short:        "Delete rawkv data from tikv",
		SilenceUsage: true,
		Args:         cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runDeleteCmd(cmd)
		},
	}
}

func runDeleteCmd(cmd *cobra.Command) error {
	cfg := &Config{}
	err := cfg.ParseFromFlags(cmd.Flags(), false)
	if err != nil {
		return err
	}

	ctx := context.Background()
	cli, err := rawkv.NewClientWithOpts(ctx, cfg.SrcPD,
		rawkv.WithAPIVersion(kvrpcpb.APIVersion_V2),
		rawkv.WithSecurity(cfg.SrcSec))
	if err != nil {
		return err
	}
	defer cli.Close()

	eg := new(errgroup.Group)
	for i := cfg.StartIndex; i < cfg.Count+cfg.StartIndex; i++ {
		i := i
		eg.Go(func() error {
			key, _, _ := generateTestData(i)
			err := cli.Delete(ctx, key)
			if err != nil {
				return err
			}
			return nil
		})
	}
	err = eg.Wait()
	if err != nil {
		log.Error("failed to delete data", zap.Error(err), zap.Int("delete count", cfg.Count))
	} else {
		log.Info("delete data successfully", zap.Error(err), zap.Int("delete count", cfg.Count))
	}
	return err
}

func runPutCmd(cmd *cobra.Command) error {
	cfg := &Config{}
	err := cfg.ParseFromFlags(cmd.Flags(), false)
	if err != nil {
		return err
	}
	ctx := context.Background()

	cli, err := rawkv.NewClientWithOpts(ctx, cfg.SrcPD,
		rawkv.WithAPIVersion(kvrpcpb.APIVersion_V2),
		rawkv.WithSecurity(cfg.SrcSec))
	if err != nil {
		return err
	}
	defer cli.Close()
	atomicCli, err := rawkv.NewClientWithOpts(ctx, cfg.SrcPD,
		rawkv.WithAPIVersion(kvrpcpb.APIVersion_V2),
		rawkv.WithSecurity(cfg.SrcSec))
	if err != nil {
		return err
	}
	defer atomicCli.Close()
	atomicCli.SetAtomicForCAS(true)

	startIdx := cfg.StartIndex

	rand.Seed(uint64(time.Now().UnixNano()))
	count1 := rand.Intn(cfg.Count)
	count2 := rand.Intn(cfg.Count - count1)

	eg := new(errgroup.Group)
	eg.Go(func() error {
		startIdx1 := startIdx
		endIdx := startIdx + count1
		for i := startIdx1; i < endIdx; i++ {
			key, value0, value1 := generateTestData(i)
			err := cli.Put(ctx, key, value0)
			if err != nil {
				return err
			}
			err = cli.Put(ctx, key, value1)
			if err != nil {
				return err
			}
		}
		return nil
	})

	eg.Go(func() error {
		startIdx1 := startIdx + count1
		endIdx := startIdx + count1 + count2
		kvCntPerBatch := 512
		for startIdx1 < endIdx {
			batchCnt := min(kvCntPerBatch, endIdx-startIdx1)
			keys, values0, values1 := batchGenerateData(startIdx1, batchCnt)
			err := cli.BatchPut(ctx, keys, values0)
			if err != nil {
				return err
			}
			err = cli.BatchPut(ctx, keys, values1)
			if err != nil {
				return err
			}
			startIdx1 += batchCnt
		}
		return nil
	})

	eg.Go(func() error {
		startIdx1 := startIdx + count1 + count2
		endIdx := startIdx + cfg.Count

		for i := startIdx1; i < endIdx; i++ {
			key, value0, value1 := generateTestData(i)
			err := atomicCli.Put(ctx, key, value0)
			if err != nil {
				return err
			}
			preValue, ret, err := atomicCli.CompareAndSwap(ctx, key, value0, value1)
			if err != nil {
				return err
			}
			if !ret && !bytes.Equal(preValue, value1) {
				return errors.Errorf("CAS put data error: preValue: %v, ret: %v, value0: %v, value1: %v", string(preValue), ret, string(value0), string(value1))
			}
		}
		return nil
	})

	err = eg.Wait()
	if err != nil {
		log.Error("failed to put data", zap.Error(err))
	} else {
		log.Info("put data successfully", zap.Int("point put count", count1),
			zap.Int("batch put count", count2), zap.Int("cas put count", cfg.Count-count1-count2))
	}

	return err
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
