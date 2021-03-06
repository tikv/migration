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

package processor

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	resolvedTsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "tikv_cdc",
			Subsystem: "processor",
			Name:      "resolved_ts",
			Help:      "local resolved ts of processor",
		}, []string{"changefeed", "capture"})
	resolvedTsLagGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "tikv_cdc",
			Subsystem: "processor",
			Name:      "resolved_ts_lag",
			Help:      "local resolved ts lag of processor",
		}, []string{"changefeed", "capture"})
	checkpointTsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "tikv_cdc",
			Subsystem: "processor",
			Name:      "checkpoint_ts",
			Help:      "global checkpoint ts of processor",
		}, []string{"changefeed", "capture"})
	checkpointTsLagGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "tikv_cdc",
			Subsystem: "processor",
			Name:      "checkpoint_ts_lag",
			Help:      "global checkpoint ts lag of processor",
		}, []string{"changefeed", "capture"})
	syncKeySpanNumGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "tikv_cdc",
			Subsystem: "processor",
			Name:      "num_of_keyspans",
			Help:      "number of synchronized keyspan of processor",
		}, []string{"changefeed", "capture"})
	processorErrorCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tikv_cdc",
			Subsystem: "processor",
			Name:      "exit_with_error_count",
			Help:      "counter for processor exits with error",
		}, []string{"changefeed", "capture"})
	processorSchemaStorageGcTsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "tikv_cdc",
			Subsystem: "processor",
			Name:      "schema_storage_gc_ts",
			Help:      "the TS of the currently maintained oldest snapshot in SchemaStorage",
		}, []string{"changefeed", "capture"})
)

// InitMetrics registers all metrics used in processor
func InitMetrics(registry *prometheus.Registry) {
	registry.MustRegister(resolvedTsGauge)
	registry.MustRegister(resolvedTsLagGauge)
	registry.MustRegister(checkpointTsGauge)
	registry.MustRegister(checkpointTsLagGauge)
	registry.MustRegister(syncKeySpanNumGauge)
	registry.MustRegister(processorErrorCounter)
	registry.MustRegister(processorSchemaStorageGcTsGauge)
}
