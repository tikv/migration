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

package sink

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	execBatchHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tikv_cdc",
			Subsystem: "sink",
			Name:      "txn_batch_size",
			Help:      "Bucketed histogram of batch size of a txn.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 18),
		}, []string{"capture", "changefeed", "type"})

	execTxnHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tikv_cdc",
			Subsystem: "sink",
			Name:      "txn_exec_duration",
			Help:      "Bucketed histogram of processing time (s) of a txn.",
			Buckets:   prometheus.ExponentialBuckets(0.002 /* 2 ms */, 2, 18),
		}, []string{"capture", "changefeed", "type"})

	executionErrorCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tikv_cdc",
			Subsystem: "sink",
			Name:      "execution_error",
			Help:      "Total count of execution errors",
		}, []string{"capture", "changefeed", "type"})

	conflictDetectDurationHis = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tikv_cdc",
			Subsystem: "sink",
			Name:      "conflict_detect_duration",
			Help:      "Bucketed histogram of conflict detect time (s) for single DML statement",
			Buckets:   prometheus.ExponentialBuckets(0.001 /* 1 ms */, 2, 20),
		}, []string{"capture", "changefeed"})

	bucketSizeCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tikv_cdc",
			Subsystem: "sink",
			Name:      "bucket_size",
			Help:      "Size of the DML bucket",
		}, []string{"capture", "changefeed", "bucket"})

	totalRowsCountGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "tikv_cdc",
			Subsystem: "sink",
			Name:      "total_rows_count",
			Help:      "The total count of rows that are processed by sink",
		}, []string{"capture", "changefeed"})

	totalFlushedRowsCountGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "tikv_cdc",
			Subsystem: "sink",
			Name:      "total_flushed_rows_count",
			Help:      "The total count of rows that are flushed by sink",
		}, []string{"capture", "changefeed"})

	flushRowChangedDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tikv_cdc",
			Subsystem: "sink",
			Name:      "flush_event_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of flushing events in processor",
			Buckets:   prometheus.ExponentialBuckets(0.002 /* 2ms */, 2, 20),
		}, []string{"capture", "changefeed", "type"})

	keyspanSinkTotalEventsCountCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tikv_cdc",
			Subsystem: "sink",
			Name:      "keyspan_sink_total_event_count",
			Help:      "The total count of rows that are processed by keyspan sink",
		}, []string{"capture", "changefeed"})

	bufferSinkTotalRowsCountCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tikv_cdc",
			Subsystem: "sink",
			Name:      "buffer_sink_total_rows_count",
			Help:      "The total count of rows that are processed by buffer sink",
		}, []string{"capture", "changefeed"})
)

// InitMetrics registers all metrics in this file
func InitMetrics(registry *prometheus.Registry) {
	registry.MustRegister(execBatchHistogram)
	registry.MustRegister(execTxnHistogram)
	registry.MustRegister(executionErrorCounter)
	registry.MustRegister(conflictDetectDurationHis)
	registry.MustRegister(bucketSizeCounter)
	registry.MustRegister(totalRowsCountGauge)
	registry.MustRegister(totalFlushedRowsCountGauge)
	registry.MustRegister(flushRowChangedDuration)
	registry.MustRegister(keyspanSinkTotalEventsCountCounter)
	registry.MustRegister(bufferSinkTotalRowsCountCounter)
}
