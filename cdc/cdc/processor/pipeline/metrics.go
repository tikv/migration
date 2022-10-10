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

package pipeline

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	keyspanResolvedTsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "tikv_cdc",
			Subsystem: "processor",
			Name:      "keyspan_resolved_ts",
			Help:      "local resolved ts of processor",
		}, []string{"changefeed", "capture", "keyspan"})
	txnCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tikv_cdc",
			Subsystem: "processor",
			Name:      "txn_count",
			Help:      "txn count received/executed by this processor",
		}, []string{"type", "changefeed", "capture"})
	changefeedMemoryHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tikv_cdc",
			Subsystem: "processor",
			Name:      "changefeed_memory_consumption",
			Help:      "estimated memory consumption for a keyspan after the sorter",
			Buckets:   prometheus.ExponentialBuckets(1*1024*1024 /* mb */, 2, 10),
		}, []string{"changefeed", "capture"})
	flowControllerConsumeHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tikv_cdc",
			Subsystem: "processor",
			Name:      "flow_controller_consume_duration",
			Help:      "bucketed histogram of processing time (s) of flowController consume",
			Buckets:   prometheus.ExponentialBuckets(0.002 /* 2 ms */, 2, 18),
		}, []string{"changefeed", "capture"})
)

// InitMetrics registers all metrics used in processor
func InitMetrics(registry *prometheus.Registry) {
	registry.MustRegister(keyspanResolvedTsGauge)
	registry.MustRegister(txnCounter)
	registry.MustRegister(changefeedMemoryHistogram)
	registry.MustRegister(flowControllerConsumeHistogram)
}
