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

package owner

import "github.com/prometheus/client_golang/prometheus"

var (
	changefeedCheckpointTsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "tikv-cdc",
			Subsystem: "owner",
			Name:      "checkpoint_ts",
			Help:      "checkpoint ts of changefeeds",
		}, []string{"changefeed"})
	changefeedCheckpointTsLagGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "tikv-cdc",
			Subsystem: "owner",
			Name:      "checkpoint_ts_lag",
			Help:      "checkpoint ts lag of changefeeds in seconds",
		}, []string{"changefeed"})
	changefeedResolvedTsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "tikv-cdc",
			Subsystem: "owner",
			Name:      "resolved_ts",
			Help:      "resolved ts of changefeeds",
		}, []string{"changefeed"})
	changefeedResolvedTsLagGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "tikv-cdc",
			Subsystem: "owner",
			Name:      "resolved_ts_lag",
			Help:      "resolved ts lag of changefeeds in seconds",
		}, []string{"changefeed"})
	ownershipCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tikv-cdc",
			Subsystem: "owner",
			Name:      "ownership_counter",
			Help:      "The counter of ownership increases every 5 seconds on a owner capture",
		})
	ownerMaintainKeySpanNumGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "tikv-cdc",
			Subsystem: "owner",
			Name:      "maintain_keyspan_num",
			Help:      "number of replicated keyspans maintained in owner",
		}, []string{"changefeed", "capture", "type"})
	changefeedStatusGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "tikv-cdc",
			Subsystem: "owner",
			Name:      "status",
			Help:      "The status of changefeeds",
		}, []string{"changefeed"})
)

const (
	// total keyspans that have been dispatched to a single processor
	maintainKeySpanTypeTotal string = "total"
	// keyspans that are dispatched to a processor and have not been finished yet
	maintainKeySpanTypeWip string = "wip"
)

// InitMetrics registers all metrics used in owner
func InitMetrics(registry *prometheus.Registry) {
	registry.MustRegister(changefeedCheckpointTsGauge)
	registry.MustRegister(changefeedResolvedTsGauge)
	registry.MustRegister(changefeedCheckpointTsLagGauge)
	registry.MustRegister(changefeedResolvedTsLagGauge)
	registry.MustRegister(ownershipCounter)
	registry.MustRegister(ownerMaintainKeySpanNumGauge)
	registry.MustRegister(changefeedStatusGauge)
}
