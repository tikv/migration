// Copyright 2020 PingCAP, Inc.
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
	"context"
	"fmt"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/log"
	"github.com/spf13/cobra"
	"github.com/tikv/client-go/v2/rawkv"
)

func NewChecksumCommand() *cobra.Command {
	return &cobra.Command{
		Use:          "checksum",
		Short:        "Verify that the upstream and downstream data are consistent",
		SilenceUsage: true,
		Args:         cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runChecksum(cmd)
		},
	}
}

func runChecksum(cmd *cobra.Command) error {
	cfg := &Config{}
	err := cfg.ParseFromFlags(cmd.Flags(), true)
	if err != nil {
		return err
	}
	ctx := context.Background()

	srcCli, err := rawkv.NewClientWithOpts(ctx, cfg.SrcPD,
		rawkv.WithAPIVersion(kvrpcpb.APIVersion_V2),
		rawkv.WithSecurity(cfg.SrcSec))
	if err != nil {
		return err
	}
	defer srcCli.Close()

	dstCli, err := rawkv.NewClientWithOpts(ctx, cfg.DstPD,
		rawkv.WithAPIVersion(kvrpcpb.APIVersion_V2),
		rawkv.WithSecurity(cfg.DstSec))
	if err != nil {
		return err
	}
	defer dstCli.Close()

	if srcCli.ClusterID() == dstCli.ClusterID() {
		return fmt.Errorf("Downstream cluster PD is same with upstream cluster PD")
	}

	srcChecksum, err := srcCli.Checksum(ctx, nil, nil)
	if err != nil {
		return err
	}

	dstChecksum, err := dstCli.Checksum(ctx, nil, nil)
	if err != nil {
		return err
	}

	if srcChecksum != dstChecksum {
		msg := fmt.Sprintf("Upstream checksum %v are not same with downstream %v", srcChecksum, dstChecksum)
		log.Info(msg)
		return fmt.Errorf(msg)
	}
	fmt.Printf("Upstream checksum %v are same with downstream %v\n", srcChecksum, dstChecksum)
	return nil
}

func NewTotalKvsCommand() *cobra.Command {
	return &cobra.Command{
		Use:          "totalkvs",
		Short:        "Verify that the total number of key-values of downstream is equal to --count argument",
		SilenceUsage: true,
		Args:         cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runTotalKvs(cmd)
		},
	}
}

func runTotalKvs(cmd *cobra.Command) error {
	cfg := &Config{}
	err := cfg.ParseFromFlags(cmd.Flags(), true)
	if err != nil {
		return err
	}
	ctx := context.Background()

	dstCli, err := rawkv.NewClientWithOpts(ctx, cfg.DstPD,
		rawkv.WithAPIVersion(kvrpcpb.APIVersion_V2),
		rawkv.WithSecurity(cfg.DstSec))
	if err != nil {
		return err
	}
	defer dstCli.Close()

	dstChecksum, err := dstCli.Checksum(ctx, nil, nil)
	if err != nil {
		return err
	}

	if dstChecksum.TotalKvs != uint64(cfg.Count) {
		msg := fmt.Sprintf("Downstream total kvs %v is not equal to expected %v", dstChecksum, cfg.Count)
		log.Info(msg)
		return fmt.Errorf(msg)
	}
	fmt.Printf("Downstream total kvs %v equals to expected %v", dstChecksum, cfg.Count)
	return nil
}
