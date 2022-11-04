// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package main

import (
	"context"
	"crypto/tls"
	"path"
	"reflect"
	"strings"

	"github.com/coreos/go-semver/semver"
	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/spf13/cobra"
	"github.com/tikv/migration/br/pkg/checksum"
	"github.com/tikv/migration/br/pkg/conn"
	"github.com/tikv/migration/br/pkg/feature"
	"github.com/tikv/migration/br/pkg/metautil"
	"github.com/tikv/migration/br/pkg/pdutil"
	"github.com/tikv/migration/br/pkg/task"
	"github.com/tikv/migration/br/pkg/utils"
	"github.com/tikv/migration/br/pkg/version/build"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

// NewDebugCommand return a debug subcommand.
func NewDebugCommand() *cobra.Command {
	meta := &cobra.Command{
		Use:          "debug <subcommand>",
		Short:        "commands to check/debug backup data",
		SilenceUsage: false,
		PersistentPreRunE: func(c *cobra.Command, args []string) error {
			if err := Init(c); err != nil {
				return errors.Trace(err)
			}
			build.LogInfo(build.BR)
			utils.LogEnvVariables()
			task.LogArguments(c)
			return nil
		},
		// To be compatible with older BR.
		Aliases: []string{"validate"},
	}
	meta.AddCommand(newCheckSumCommand())
	meta.AddCommand(newBackupMetaCommand())
	meta.AddCommand(decodeBackupMetaCommand())
	meta.AddCommand(encodeBackupMetaCommand())
	meta.AddCommand(setPDConfigCommand())
	meta.Hidden = true

	return meta
}

func newCheckSumCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "checksum",
		Short: "check the backup data",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runRawChecksumCommand(cmd, "RawChecksum")
		},
	}
	return command
}

func newBackupMetaCommand() *cobra.Command {
	command := &cobra.Command{
		Use:          "backupmeta",
		Short:        "utilities of backupmeta",
		SilenceUsage: false,
	}
	command.AddCommand(newBackupMetaValidateCommand())
	return command
}

func newBackupMetaValidateCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "validate",
		Short: "validate key range and rewrite rules of backupmeta",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return errors.Errorf("validate is unsupported")
		},
	}
	command.Flags().Uint64("offset", 0, "the offset of table id alloctor")
	command.Hidden = true
	return command
}

func decodeBackupMetaCommand() *cobra.Command {
	decodeBackupMetaCmd := &cobra.Command{
		Use:   "decode",
		Short: "decode backupmeta to json",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := context.WithCancel(GetDefaultContext())
			defer cancel()

			var cfg task.Config
			if err := cfg.ParseFromFlags(cmd.Flags()); err != nil {
				return errors.Trace(err)
			}
			_, s, backupMeta, err := task.ReadBackupMeta(ctx, metautil.MetaFile, &cfg)
			if err != nil {
				return errors.Trace(err)
			}

			fieldName, _ := cmd.Flags().GetString("field")
			if fieldName == "" {
				// No field flag, write backupmeta to external storage in JSON format.
				backupMetaJSON, err := utils.MarshalBackupMeta(backupMeta)
				if err != nil {
					return errors.Trace(err)
				}
				err = s.WriteFile(ctx, metautil.MetaJSONFile, backupMetaJSON)
				if err != nil {
					return errors.Trace(err)
				}
				cmd.Printf("backupmeta decoded at %s\n", path.Join(cfg.Storage, metautil.MetaJSONFile))
				return nil
			}

			switch fieldName {
			// To be compatible with older BR.
			case "start-version":
				fieldName = "StartVersion"
			case "end-version":
				fieldName = "EndVersion"
			}

			_, found := reflect.TypeOf(*backupMeta).FieldByName(fieldName)
			if !found {
				cmd.Printf("field '%s' not found\n", fieldName)
				return nil
			}
			field := reflect.ValueOf(*backupMeta).FieldByName(fieldName)
			if !field.CanInterface() {
				cmd.Printf("field '%s' can not print\n", fieldName)
			} else {
				cmd.Printf("%v\n", field.Interface())
			}
			return nil
		},
	}

	decodeBackupMetaCmd.Flags().String("field", "", "decode specified field")

	return decodeBackupMetaCmd
}

func encodeBackupMetaCommand() *cobra.Command {
	encodeBackupMetaCmd := &cobra.Command{
		Use:   "encode",
		Short: "encode backupmeta json file to backupmeta",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := context.WithCancel(GetDefaultContext())
			defer cancel()

			var cfg task.Config
			if err := cfg.ParseFromFlags(cmd.Flags()); err != nil {
				return errors.Trace(err)
			}
			_, s, err := task.GetStorage(ctx, &cfg)
			if err != nil {
				return errors.Trace(err)
			}

			metaData, err := s.ReadFile(ctx, metautil.MetaJSONFile)
			if err != nil {
				return errors.Trace(err)
			}

			backupMetaJSON, err := utils.UnmarshalBackupMeta(metaData)
			if err != nil {
				return errors.Trace(err)
			}
			backupMeta, err := proto.Marshal(backupMetaJSON)
			if err != nil {
				return errors.Trace(err)
			}

			fileName := metautil.MetaFile
			if ok, _ := s.FileExists(ctx, fileName); ok {
				// Do not overwrite origin meta file
				fileName += "_from_json"
			}

			encryptedContent, iv, err := metautil.Encrypt(backupMeta, &cfg.CipherInfo)
			if err != nil {
				return errors.Trace(err)
			}

			err = s.WriteFile(ctx, fileName, append(iv, encryptedContent...))
			if err != nil {
				return errors.Trace(err)
			}
			return nil
		},
	}
	return encodeBackupMetaCmd
}

func setPDConfigCommand() *cobra.Command {
	pdConfigCmd := &cobra.Command{
		Use:   "reset-pd-config-as-default",
		Short: "reset pd config adjusted by BR to default value",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := context.WithCancel(GetDefaultContext())
			defer cancel()

			var cfg task.Config
			if err := cfg.ParseFromFlags(cmd.Flags()); err != nil {
				return errors.Trace(err)
			}

			mgr, err := task.NewMgr(ctx, nil, cfg.PD, cfg.TLS, task.GetKeepalive(&cfg), cfg.CheckRequirements)
			if err != nil {
				return errors.Trace(err)
			}
			defer mgr.Close()

			if err := mgr.UpdatePDScheduleConfig(ctx); err != nil {
				return errors.Annotate(err, "fail to update PD merge config")
			}
			log.Info("add pd configs succeed")
			return nil
		},
	}
	return pdConfigCmd
}

func runRawChecksumCommand(command *cobra.Command, cmdName string) error {
	cfg := task.Config{LogProgress: HasLogFile()}
	err := cfg.ParseFromFlags(command.Flags())
	if err != nil {
		command.SilenceUsage = false
		return errors.Trace(err)
	}

	ctx := GetDefaultContext()
	pdAddress := strings.Join(cfg.PD, ",")
	securityOption := pd.SecurityOption{}
	var tlsConf *tls.Config = nil
	if cfg.TLS.IsEnabled() {
		securityOption.CAPath = cfg.TLS.CA
		securityOption.CertPath = cfg.TLS.Cert
		securityOption.KeyPath = cfg.TLS.Key
		tlsConf, err = cfg.TLS.ToTLSConfig()
		if err != nil {
			return errors.Trace(err)
		}
	}
	pdCtrl, err := pdutil.NewPdController(ctx, pdAddress, tlsConf, securityOption)
	if err != nil {
		return errors.Trace(err)
	}
	clusterVersion, err := pdCtrl.GetClusterVersion(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	featureGate := feature.NewFeatureGate(semver.New(clusterVersion))
	if !featureGate.IsEnabled(feature.Checksum) {
		log.Error("TiKV cluster does not support checksum.", zap.String("version", clusterVersion))
		return errors.Errorf("TiKV cluster %s does not support checksum", clusterVersion)
	}
	storageAPIVersion, err := conn.GetTiKVApiVersion(ctx, pdCtrl.GetPDClient(), tlsConf)
	if err != nil {
		return errors.Trace(err)
	}
	_, _, backupMeta, err := task.ReadBackupMeta(ctx, metautil.MetaFile, &cfg)
	if err != nil {
		return errors.Trace(err)
	}
	fileChecksum, keyRanges := task.CalcChecksumAndRangeFromBackupMeta(ctx, backupMeta, storageAPIVersion)
	if !task.CheckBackupAPIVersion(featureGate, storageAPIVersion, backupMeta.ApiVersion) {
		return errors.Errorf("Unsupported api version, storage:%s, backup meta:%s",
			storageAPIVersion.String(), backupMeta.ApiVersion.String())
	}
	checksumMethod := checksum.StorageChecksumCommand
	if storageAPIVersion != backupMeta.ApiVersion {
		checksumMethod = checksum.StorageScanCommand
	}

	executor, err := checksum.NewExecutor(ctx, keyRanges, cfg.PD, storageAPIVersion,
		cfg.ChecksumConcurrency, cfg.TLS)
	if err != nil {
		return errors.Trace(err)
	}
	defer executor.Close()
	err = checksum.Run(ctx, cmdName, executor, checksumMethod, fileChecksum)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}
