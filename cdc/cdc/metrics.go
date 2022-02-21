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
	"github.com/tikv/migration/cdc/cdc/entry"
	"github.com/tikv/migration/cdc/cdc/kv"
	"github.com/tikv/migration/cdc/cdc/owner"
	"github.com/tikv/migration/cdc/cdc/processor"
	tablepipeline "github.com/tikv/migration/cdc/cdc/processor/pipeline"
	"github.com/tikv/migration/cdc/cdc/puller"
	redowriter "github.com/tikv/migration/cdc/cdc/redo/writer"
	"github.com/tikv/migration/cdc/cdc/sink"
	"github.com/tikv/migration/cdc/cdc/sorter"
	"github.com/tikv/migration/cdc/cdc/sorter/leveldb"
	"github.com/tikv/migration/cdc/cdc/sorter/memory"
	"github.com/tikv/migration/cdc/cdc/sorter/unified"
	"github.com/tikv/migration/cdc/pkg/actor"
	"github.com/tikv/migration/cdc/pkg/db"
	"github.com/tikv/migration/cdc/pkg/etcd"
	"github.com/tikv/migration/cdc/pkg/orchestrator"
	"github.com/tikv/migration/cdc/pkg/p2p"
)

var registry = prometheus.NewRegistry()

func init() {
	registry.MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	registry.MustRegister(prometheus.NewGoCollector())

	kv.InitMetrics(registry)
	puller.InitMetrics(registry)
	sink.InitMetrics(registry)
	entry.InitMetrics(registry)
	processor.InitMetrics(registry)
	tablepipeline.InitMetrics(registry)
	owner.InitMetrics(registry)
	etcd.InitMetrics(registry)
	initServerMetrics(registry)
	actor.InitMetrics(registry)
	orchestrator.InitMetrics(registry)
	p2p.InitMetrics(registry)
	// Sorter metrics
	sorter.InitMetrics(registry)
	memory.InitMetrics(registry)
	unified.InitMetrics(registry)
	leveldb.InitMetrics(registry)
	redowriter.InitMetrics(registry)
	db.InitMetrics(registry)
}
