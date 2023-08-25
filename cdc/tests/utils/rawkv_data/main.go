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
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/tikv/client-go/v2/config"
)

const (
	MAX_KV_CNT int64 = 1e8
)

const (
	flagSrcPD      = "src-pd"
	flagDstPD      = "dst-pd"
	flagCount      = "count"
	flagStartIndex = "start-index"
	flagCAPath     = "ca-path"
	flagCertPath   = "cert-path"
	flagKeyPath    = "key-path"
)

type Config struct {
	SrcPD      []string        `json:"src-pd"`
	DstPD      []string        `json:"dst-pd"`
	StartIndex int             `json:"start-index"`
	Count      int             `json:"count"`
	CAPath     string          `json:"ca-path"`
	CertPath   string          `json:"cert-path"`
	KeyPath    string          `json:"key-path"`
	SrcSec     config.Security `json:"src-sec"`
	DstSec     config.Security `json:"dst-sec"`
}

func AddFlags(cmd *cobra.Command) {
	cmd.PersistentFlags().String(flagSrcPD, "127.0.0.1:2379", "Upstream PD address")
	cmd.PersistentFlags().String(flagDstPD, "", "Downstream PD address")
	cmd.PersistentFlags().Int(flagStartIndex, 0, "The start index of generated keys")
	cmd.PersistentFlags().Int(flagCount, 1000, "The number of key")
	cmd.PersistentFlags().String(flagCAPath, "", "Path to CA certificate")
	cmd.PersistentFlags().String(flagCertPath, "", "Path to client certificate")
	cmd.PersistentFlags().String(flagKeyPath, "", "Path to client key")
}

func (cfg *Config) ParseFromFlags(flags *pflag.FlagSet, requireDstPD bool) error {
	var err error
	srcPD, err := flags.GetString(flagSrcPD)
	if err != nil {
		return err
	}
	cfg.SrcPD = strings.Split(srcPD, ",")

	dstPD, err := flags.GetString(flagDstPD)
	if err != nil {
		return err
	}
	cfg.DstPD = strings.Split(dstPD, ",")

	if cfg.StartIndex, err = flags.GetInt(flagStartIndex); err != nil {
		return err
	}
	if cfg.Count, err = flags.GetInt(flagCount); err != nil {
		return err
	}
	if cfg.CAPath, err = flags.GetString(flagCAPath); err != nil {
		return err
	}
	if cfg.CertPath, err = flags.GetString(flagCertPath); err != nil {
		return err
	}
	if cfg.KeyPath, err = flags.GetString(flagKeyPath); err != nil {
		return err
	}

	if len(cfg.SrcPD) == 0 {
		return fmt.Errorf("Upstream cluster PD is not set")
	}
	if strings.HasPrefix(cfg.SrcPD[0], "https://") {
		if cfg.CAPath == "" || cfg.CertPath == "" || cfg.KeyPath == "" {
			return fmt.Errorf("CAPath/CertPath/KeyPath is not set")
		}
		cfg.SrcSec.ClusterSSLCA = cfg.CAPath
		cfg.SrcSec.ClusterSSLCert = cfg.CertPath
		cfg.SrcSec.ClusterSSLKey = cfg.KeyPath
	}

	if requireDstPD {
		if len(cfg.DstPD) == 0 {
			return fmt.Errorf("Downstream cluster PD is not set")
		}
		if strings.HasPrefix(cfg.DstPD[0], "https://") {
			if cfg.CAPath == "" || cfg.CertPath == "" || cfg.KeyPath == "" {
				return fmt.Errorf("CAPath/CertPath/KeyPath is not set")
			}
			cfg.DstSec.ClusterSSLCA = cfg.CAPath
			cfg.DstSec.ClusterSSLCert = cfg.CertPath
			cfg.DstSec.ClusterSSLKey = cfg.KeyPath
		}
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
	rootCmd.AddCommand(NewTotalKvsCommand())

	rootCmd.SetOut(os.Stdout)
	rootCmd.SetArgs(os.Args[1:])
	if err := rootCmd.Execute(); err != nil {
		fmt.Printf("rawkv_data failed, %v.\n", err)
		os.Exit(1) // nolint:gocritic
	}
}
