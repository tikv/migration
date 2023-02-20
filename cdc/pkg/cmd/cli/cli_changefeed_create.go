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

package cli

import (
	"context"
	"net/url"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/spf13/cobra"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/migration/cdc/cdc/model"
	"github.com/tikv/migration/cdc/cdc/sink"
	cmdcontext "github.com/tikv/migration/cdc/pkg/cmd/context"
	"github.com/tikv/migration/cdc/pkg/cmd/factory"
	"github.com/tikv/migration/cdc/pkg/cmd/util"
	"github.com/tikv/migration/cdc/pkg/config"
	cerror "github.com/tikv/migration/cdc/pkg/errors"
	"github.com/tikv/migration/cdc/pkg/etcd"
	"github.com/tikv/migration/cdc/pkg/security"
	"github.com/tikv/migration/cdc/pkg/txnutil/gc"
	ticdcutil "github.com/tikv/migration/cdc/pkg/util"
	"github.com/tikv/migration/cdc/pkg/version"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

// TODO: define other flags in Command.
const (
	flagStartKey  = "start-key"
	flagEndKey    = "end-key"
	flagKeyFormat = "format"
)

// changefeedCommonOptions defines common changefeed flags.
type changefeedCommonOptions struct {
	noConfirm  bool
	targetTs   uint64
	sinkURI    string
	configFile string
	opts       []string
	sortEngine string
	sortDir    string
	format     string
	startKey   string
	endKey     string
}

// newChangefeedCommonOptions creates new changefeed common options.
func newChangefeedCommonOptions() *changefeedCommonOptions {
	return &changefeedCommonOptions{}
}

// addFlags receives a *cobra.Command reference and binds
// flags related to template printing to it.
func (o *changefeedCommonOptions) addFlags(cmd *cobra.Command) {
	if o == nil {
		return
	}

	cmd.PersistentFlags().BoolVar(&o.noConfirm, "no-confirm", false, "Don't ask user whether to confirm warnings")
	cmd.PersistentFlags().Uint64Var(&o.targetTs, "target-ts", 0, "Target ts of changefeed")
	cmd.PersistentFlags().StringVar(&o.sinkURI, "sink-uri", "", "sink uri")
	cmd.PersistentFlags().StringVar(&o.configFile, "config", "", "Path of the configuration file")
	cmd.PersistentFlags().StringSliceVar(&o.opts, "opts", nil, "Extra options, in the `key=value` format")
	cmd.PersistentFlags().StringVar(&o.sortEngine, "sort-engine", model.SortUnified, "sort engine used for data sort")
	cmd.PersistentFlags().StringVar(&o.sortDir, "sort-dir", "", "directory used for data sort")
	_ = cmd.PersistentFlags().MarkHidden("sort-dir")
	cmd.PersistentFlags().StringVar(&o.format, flagKeyFormat, "hex", "The format of start and end key. Available options: \"raw\", \"escaped\", \"hex\".")
	cmd.PersistentFlags().StringVar(&o.startKey, flagStartKey, "", "The start key of the changefeed, key is inclusive.")
	cmd.PersistentFlags().StringVar(&o.endKey, flagEndKey, "", "The end key of the changefeed, key is exclusive.")
}

// strictDecodeConfig do strictDecodeFile check and only verify the rules for now.
func (o *changefeedCommonOptions) strictDecodeConfig(component string, cfg *config.ReplicaConfig) error {
	err := util.StrictDecodeFile(o.configFile, component, cfg)
	if err != nil {
		return err
	}

	return err
}

func (o *changefeedCommonOptions) validKeyFormat() error {
	return ticdcutil.ValidKeyFormat(o.format, o.startKey, o.endKey)
}

// createChangefeedOptions defines common flags for the `cli changefeed crate` command.
type createChangefeedOptions struct {
	commonChangefeedOptions *changefeedCommonOptions

	etcdClient *etcd.CDCEtcdClient
	pdClient   pd.Client

	pdAddr     string
	credential *security.Credential

	changefeedID            string
	disableGCSafePointCheck bool
	startTs                 uint64
	timezone                string

	cfg *config.ReplicaConfig
}

// newCreateChangefeedOptions creates new options for the `cli changefeed create` command.
func newCreateChangefeedOptions(commonChangefeedOptions *changefeedCommonOptions) *createChangefeedOptions {
	return &createChangefeedOptions{
		commonChangefeedOptions: commonChangefeedOptions,
	}
}

// addFlags receives a *cobra.Command reference and binds
// flags related to template printing to it.
func (o *createChangefeedOptions) addFlags(cmd *cobra.Command) {
	if o == nil {
		return
	}

	o.commonChangefeedOptions.addFlags(cmd)
	cmd.PersistentFlags().StringVarP(&o.changefeedID, "changefeed-id", "c", "", "Replication task (changefeed) ID")
	cmd.PersistentFlags().BoolVarP(&o.disableGCSafePointCheck, "disable-gc-check", "", false, "Disable GC safe point check")
	cmd.PersistentFlags().Uint64Var(&o.startTs, "start-ts", 0, "Start ts of changefeed")
	cmd.PersistentFlags().StringVar(&o.timezone, "tz", "SYSTEM", "timezone used when checking sink uri (changefeed timezone is determined by cdc server)")
}

// complete adapts from the command line args to the data and client required.
func (o *createChangefeedOptions) complete(ctx context.Context, f factory.Factory, cmd *cobra.Command) error {
	etcdClient, err := f.EtcdClient()
	if err != nil {
		return err
	}

	o.etcdClient = etcdClient

	pdClient, err := f.PdClient()
	if err != nil {
		return err
	}

	o.pdClient = pdClient

	o.pdAddr = f.GetPdAddr()
	o.credential = f.GetCredential()

	if o.startTs == 0 {
		ts, logical, err := o.pdClient.GetTS(ctx)
		if err != nil {
			return err
		}
		o.startTs = oracle.ComposeTS(ts, logical)
	}

	return o.completeCfg(ctx, cmd)
}

// completeCfg complete the replica config from file and cmd flags.
func (o *createChangefeedOptions) completeCfg(_ context.Context, cmd *cobra.Command) error {
	cfg := config.GetDefaultReplicaConfig()
	if len(o.commonChangefeedOptions.configFile) > 0 {
		if err := o.commonChangefeedOptions.strictDecodeConfig("TiKV-CDC changefeed", cfg); err != nil {
			return err
		}
	}

	if !cfg.EnableOldValue {
		sinkURIParsed, err := url.Parse(o.commonChangefeedOptions.sinkURI)
		if err != nil {
			return cerror.WrapError(cerror.ErrSinkURIInvalid, err)
		}

		protocol := sinkURIParsed.Query().Get(config.ProtocolKey)
		if protocol != "" {
			cfg.Sink.Protocol = protocol
		}
		for _, fp := range config.ForceEnableOldValueProtocols {
			if cfg.Sink.Protocol == fp {
				log.Warn("Attempting to replicate without old value enabled. CDC will enable old value and continue.", zap.String("protocol", cfg.Sink.Protocol))
				cfg.EnableOldValue = true
				break
			}
		}
	}

	for _, rules := range cfg.Sink.DispatchRules {
		switch strings.ToLower(rules.Dispatcher) {
		case "rowid", "index-value":
			if cfg.EnableOldValue {
				cmd.Printf("[WARN] This index-value distribution mode "+
					"does not guarantee row-level orderliness when "+
					"switching on the old value, so please use caution! dispatch-rules: %#v", rules)
			}
		}
	}

	if o.disableGCSafePointCheck {
		cfg.CheckGCSafePoint = false
	}
	// Complete cfg.
	o.cfg = cfg

	return nil
}

// validate checks that the provided attach options are specified.
func (o *createChangefeedOptions) validate(ctx context.Context, cmd *cobra.Command) error {
	if o.commonChangefeedOptions.sinkURI == "" {
		return errors.New("Creating changefeed without a sink-uri")
	}

	err := o.cfg.Validate()
	if err != nil {
		return err
	}

	if err := o.validateStartTs(ctx); err != nil {
		return err
	}

	if err := o.validateTargetTs(); err != nil {
		return err
	}

	// user is not allowed to set sort-dir at changefeed level
	if o.commonChangefeedOptions.sortDir != "" {
		cmd.Printf(color.HiYellowString("[WARN] --sort-dir is deprecated in changefeed settings. " +
			"Please use `cdc server --data-dir` to start the cdc server if possible, sort-dir will be set automatically. " +
			"The --sort-dir here will be no-op\n"))
		return errors.New("Creating changefeed with `--sort-dir`, it's invalid")
	}

	switch o.commonChangefeedOptions.sortEngine {
	case model.SortUnified, model.SortInMemory:
	case model.SortInFile:
		// obsolete. But we keep silent here. We create a Unified Sorter when the owner/processor sees this option
		// for backward-compatibility.
	default:
		return errors.Errorf("Creating changefeed with an invalid sort engine(%s), "+
			"`%s` and `%s` are the only valid options.", o.commonChangefeedOptions.sortEngine, model.SortUnified, model.SortInMemory)
	}

	return o.commonChangefeedOptions.validKeyFormat()
}

// getInfo constructs the information for the changefeed.
func (o *createChangefeedOptions) getInfo(cmd *cobra.Command) *model.ChangeFeedInfo {
	info := &model.ChangeFeedInfo{
		SinkURI:        o.commonChangefeedOptions.sinkURI,
		Opts:           make(map[string]string),
		CreateTime:     time.Now(),
		StartTs:        o.startTs,
		TargetTs:       o.commonChangefeedOptions.targetTs,
		StartKey:       o.commonChangefeedOptions.startKey,
		EndKey:         o.commonChangefeedOptions.endKey,
		Format:         o.commonChangefeedOptions.format,
		Config:         o.cfg,
		Engine:         o.commonChangefeedOptions.sortEngine,
		State:          model.StateNormal,
		CreatorVersion: version.ReleaseVersion,
	}

	if info.Engine == model.SortInFile {
		cmd.Printf("[WARN] file sorter is deprecated. " +
			"make sure that you DO NOT use it in production. " +
			"Adjust \"sort-engine\" to make use of the right sorter.\n")
	}

	for _, opt := range o.commonChangefeedOptions.opts {
		s := strings.SplitN(opt, "=", 2)
		if len(s) <= 0 {
			cmd.Printf("omit opt: %s", opt)
			continue
		}

		var key string
		var value string

		key = s[0]
		if len(s) > 1 {
			value = s[1]
		}
		info.Opts[key] = value
	}

	return info
}

// validateStartTs checks if startTs is a valid value.
func (o *createChangefeedOptions) validateStartTs(ctx context.Context) error {
	if o.disableGCSafePointCheck {
		return nil
	}
	// Ensure the start ts is validate in the next 1 hour.
	const ensureTTL = 60 * 60.
	return gc.EnsureChangefeedStartTsSafety(
		ctx, o.pdClient, o.changefeedID, ensureTTL, o.startTs)
}

// validateTargetTs checks if targetTs is a valid value.
func (o *createChangefeedOptions) validateTargetTs() error {
	if o.commonChangefeedOptions.targetTs > 0 && o.commonChangefeedOptions.targetTs <= o.startTs {
		return errors.Errorf("target-ts %d must be larger than start-ts: %d", o.commonChangefeedOptions.targetTs, o.startTs)
	}
	return nil
}

// validateSink will create a sink and verify that the configuration is correct.
func (o *createChangefeedOptions) validateSink(
	ctx context.Context, cfg *config.ReplicaConfig, opts map[string]string,
) error {
	return sink.Validate(ctx, o.commonChangefeedOptions.sinkURI, cfg, opts)
}

// run the `cli changefeed create` command.
func (o *createChangefeedOptions) run(ctx context.Context, cmd *cobra.Command) error {
	id := o.changefeedID
	if id == "" {
		id = uuid.New().String()
	}
	if err := model.ValidateChangefeedID(id); err != nil {
		return err
	}

	if !o.commonChangefeedOptions.noConfirm {
		currentPhysical, _, err := o.pdClient.GetTS(ctx)
		if err != nil {
			return err
		}
		if err := confirmLargeDataGap(cmd, currentPhysical, o.startTs); err != nil {
			return err
		}
	}

	info := o.getInfo(cmd)

	tz, err := ticdcutil.GetTimezone(o.timezone)
	if err != nil {
		return errors.Annotate(err, "can not load timezone, Please specify the time zone through environment variable `TZ` or command line parameters `--tz`")
	}

	ctx = ticdcutil.PutTimezoneInCtx(ctx, tz)
	err = o.validateSink(ctx, info.Config, info.Opts)
	if err != nil {
		return err
	}

	infoStr, err := info.Marshal()
	if err != nil {
		return err
	}

	err = o.etcdClient.CreateChangefeedInfo(ctx, info, id)
	if err != nil {
		return err
	}

	cmd.Printf("Create changefeed successfully!\nID: %s\nInfo: %s\n", id, infoStr)

	return nil
}

// newCmdCreateChangefeed creates the `cli changefeed create` command.
func newCmdCreateChangefeed(f factory.Factory) *cobra.Command {
	commonChangefeedOptions := newChangefeedCommonOptions()

	o := newCreateChangefeedOptions(commonChangefeedOptions)

	command := &cobra.Command{
		Use:   "create",
		Short: "Create a new replication task (changefeed)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmdcontext.GetDefaultContext()

			err := o.complete(ctx, f, cmd)
			if err != nil {
				return err
			}

			err = o.validate(ctx, cmd)
			if err != nil {
				return err
			}

			return o.run(ctx, cmd)
		},
	}

	o.addFlags(command)

	return command
}
