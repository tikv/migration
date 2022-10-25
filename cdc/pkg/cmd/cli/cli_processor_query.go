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

	"github.com/spf13/cobra"
	"github.com/tikv/migration/cdc/cdc/model"
	apiv1client "github.com/tikv/migration/cdc/pkg/api/v1"
	cmdcontext "github.com/tikv/migration/cdc/pkg/cmd/context"
	"github.com/tikv/migration/cdc/pkg/cmd/factory"
	"github.com/tikv/migration/cdc/pkg/cmd/util"
	"github.com/tikv/migration/cdc/pkg/etcd"
)

type processorMeta struct {
	Status   *model.TaskStatus   `json:"status"`
	Position *model.TaskPosition `json:"position"`
}

// queryProcessorOptions defines flags for the `cli processor query` command.
type queryProcessorOptions struct {
	etcdClient *etcd.CDCEtcdClient
	apiClient  apiv1client.APIV1Interface

	changefeedID string
	captureID    string
}

// newQueryProcessorOptions creates new options for the `cli changefeed query` command.
func newQueryProcessorOptions() *queryProcessorOptions {
	return &queryProcessorOptions{}
}

// complete adapts from the command line args to the data and client required.
func (o *queryProcessorOptions) complete(f factory.Factory) error {
	etcdClient, err := f.EtcdClient()
	if err != nil {
		return err
	}

	o.etcdClient = etcdClient
	ctx := cmdcontext.GetDefaultContext()
	owner, err := getOwnerCapture(ctx, o.etcdClient)
	if err != nil {
		return err
	}

	o.apiClient, err = apiv1client.NewAPIClient(owner.AdvertiseAddr, f.GetCredential())
	if err != nil {
		return err
	}

	return nil
}

// addFlags receives a *cobra.Command reference and binds
// flags related to template printing to it.
func (o *queryProcessorOptions) addFlags(cmd *cobra.Command) {
	cmd.PersistentFlags().StringVarP(&o.changefeedID, "changefeed-id", "c", "", "Replication task (changefeed) ID")
	cmd.PersistentFlags().StringVarP(&o.captureID, "capture-id", "p", "", "capture ID")
	_ = cmd.MarkPersistentFlagRequired("changefeed-id")
	_ = cmd.MarkPersistentFlagRequired("capture-id")
}

// run cli cmd with api client
func (o *queryProcessorOptions) runCliWithAPIClient(ctx context.Context, cmd *cobra.Command) error {
	processor, err := o.apiClient.Processors().Get(ctx, o.changefeedID, o.captureID)
	if err != nil {
		return err
	}

	meta := &processorMeta{
		Status: &model.TaskStatus{
			KeySpans: processor.KeySpans,
			// Operations, AdminJobType and ModRevision are vacant
		},
		Position: &model.TaskPosition{
			CheckPointTs: processor.CheckPointTs,
			ResolvedTs:   processor.ResolvedTs,
			Count:        processor.Count,
			Error:        processor.Error,
		},
	}

	return util.JSONPrint(cmd, meta)
}

// run runs the `cli processor query` command.
func (o *queryProcessorOptions) run(cmd *cobra.Command) error {
	ctx := cmdcontext.GetDefaultContext()
	return o.runCliWithAPIClient(ctx, cmd)
}

// newCmdQueryProcessor creates the `cli processor query` command.
func newCmdQueryProcessor(f factory.Factory) *cobra.Command {
	o := newQueryProcessorOptions()

	command := &cobra.Command{
		Use:   "query",
		Short: "Query information and status of a sub replication task (processor)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			err := o.complete(f)
			if err != nil {
				return err
			}

			return o.run(cmd)
		},
	}

	o.addFlags(command)

	return command
}
