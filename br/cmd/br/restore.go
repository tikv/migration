// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package main

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/spf13/cobra"
	"github.com/tikv/migration/br/pkg/gluetikv"
	"github.com/tikv/migration/br/pkg/summary"
	"github.com/tikv/migration/br/pkg/task"
	"github.com/tikv/migration/br/pkg/trace"
	"github.com/tikv/migration/br/pkg/utils"
	"github.com/tikv/migration/br/pkg/version/build"
	"go.uber.org/zap"
	"sourcegraph.com/sourcegraph/appdash"
)

func runRestoreRawCommand(command *cobra.Command, cmdName string) error {
	cfg := task.RestoreRawConfig{
		RawKvConfig: task.RawKvConfig{Config: task.Config{LogProgress: HasLogFile()}},
	}
	if err := cfg.ParseFromFlags(command.Flags()); err != nil {
		command.SilenceUsage = false
		return errors.Trace(err)
	}

	ctx := GetDefaultContext()
	if cfg.EnableOpenTracing {
		var store *appdash.MemoryStore
		ctx, store = trace.TracerStartSpan(ctx)
		defer trace.TracerFinishSpan(ctx, store)
	}
	if err := task.RunRestoreRaw(GetDefaultContext(), gluetikv.Glue{}, cmdName, &cfg); err != nil {
		log.Error("failed to restore raw kv", zap.Error(err))
		return errors.Trace(err)
	}
	return nil
}

// NewRestoreCommand returns a restore subcommand.
func NewRestoreCommand() *cobra.Command {
	command := &cobra.Command{
		Use:          "restore",
		Short:        "restore a TiDB/TiKV cluster",
		SilenceUsage: true,
		PersistentPreRunE: func(c *cobra.Command, args []string) error {
			if err := Init(c); err != nil {
				return errors.Trace(err)
			}
			build.LogInfo(build.BR)
			utils.LogEnvVariables()
			task.LogArguments(c)
			summary.SetUnit(summary.RestoreUnit)
			return nil
		},
	}
	command.AddCommand(
		newRawRestoreCommand(),
	)
	task.DefineRestoreFlags(command.PersistentFlags())

	return command
}

func newRawRestoreCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "raw",
		Short: "(experimental) restore a raw kv range to TiKV cluster",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runRestoreRawCommand(cmd, "Raw restore")
		},
	}

	task.DefineRawRestoreFlags(command)
	return command
}
