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

package cdc

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/migration/cdc/cdc/kv"
	"github.com/tikv/migration/cdc/cdc/owner"
	"github.com/tikv/migration/cdc/cdc/processor"
	keyspanpipeline "github.com/tikv/migration/cdc/cdc/processor/pipeline"
	"github.com/tikv/migration/cdc/cdc/puller"
	"github.com/tikv/migration/cdc/cdc/sink"
	"github.com/tikv/migration/cdc/cdc/sorter"
	"github.com/tikv/migration/cdc/cdc/sorter/unified"
	"github.com/tikv/migration/cdc/pkg/db"
	"github.com/tikv/migration/cdc/pkg/etcd"
	"github.com/tikv/migration/cdc/pkg/orchestrator"
)

var registry = prometheus.NewRegistry()

func init() {
	registry.MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	registry.MustRegister(prometheus.NewGoCollector())

	kv.InitMetrics(registry)
	puller.InitMetrics(registry)
	sink.InitMetrics(registry)
	processor.InitMetrics(registry)
	keyspanpipeline.InitMetrics(registry)
	owner.InitMetrics(registry)
	etcd.InitMetrics(registry)
	initServerMetrics(registry)
	orchestrator.InitMetrics(registry)
	// Sorter metrics
	sorter.InitMetrics(registry)
	unified.InitMetrics(registry)
	db.InitMetrics(registry)
}
