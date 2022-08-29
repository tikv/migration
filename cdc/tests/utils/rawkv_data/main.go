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
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

const (
	MAX_KV_CNT int64 = 1e8
)

const (
	flagSrcPD      = "src-pd"
	flagDstPD      = "dst-pd"
	flagCount      = "count"
	flagStartIndex = "start-index"
)

type Config struct {
	SrcPD      string `json:"src-pd"`
	DstPD      string `json:"dst_pd"`
	StartIndex int    `json:"start_index"`
	Count      int    `json:"count"`
}

func AddFlags(cmd *cobra.Command) {
	cmd.PersistentFlags().String(flagSrcPD, "127.0.0.1:2379", "Upstream PD address")
	cmd.PersistentFlags().String(flagDstPD, "", "Downstream PD address")
	cmd.PersistentFlags().Int(flagStartIndex, 0, "The number of key")
	cmd.PersistentFlags().Int(flagCount, 1000, "The number of key")
}

func (cfg *Config) ParseFromFlags(flags *pflag.FlagSet) error {
	var err error
	if cfg.SrcPD, err = flags.GetString(flagSrcPD); err != nil {
		return err
	}
	if cfg.DstPD, err = flags.GetString(flagDstPD); err != nil {
		return err
	}
	if cfg.StartIndex, err = flags.GetInt(flagStartIndex); err != nil {
		return err
	}
	if cfg.Count, err = flags.GetInt(flagCount); err != nil {
		return err
	}
	return nil
}

func main() {
	rootCmd := &cobra.Command{
		Use:              "rawkv_data",
		Short:            "rawkv_data is used to generate rawkv data for test.",
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	AddFlags(rootCmd)

	rootCmd.AddCommand(NewPutCommand())
	rootCmd.AddCommand(NewDeleteCommand())
	rootCmd.AddCommand(NewChecksumCommand())

	rootCmd.SetOut(os.Stdout)
	rootCmd.SetArgs(os.Args[1:])
	if err := rootCmd.Execute(); err != nil {
		fmt.Printf("rawkv_data failed, %v.\n", err)
		os.Exit(1) // nolint:gocritic
	}
}
