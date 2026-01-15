// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package dns

import (
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/pkg/meter"
)

type metrics struct {
	// DNS-related metrics
	dnsQueryCount           meter.Counter
	dnsQueryFailedCount     meter.Counter
	dnsQueryTotalDuration   meter.Counter
	dnsQueryDuration        meter.Histogram
	dnsSRVLookupCount       meter.Counter
	dnsSRVLookupFailedCount meter.Counter

	// gRPC-related metrics
	grpcQueryCount         meter.Counter
	grpcQueryFailedCount   meter.Counter
	grpcQueryTotalDuration meter.Counter
	grpcQueryDuration      meter.Histogram

	// Summary metrics
	discoveryCount         meter.Counter
	discoveryFailedCount   meter.Counter
	discoveryTotalDuration meter.Counter
	discoveryDuration      meter.Histogram

	totalNodesCount meter.Gauge

	// Retry-related metrics
	nodeRetryCount        meter.Counter
	nodeRetryFailedCount  meter.Counter
	nodeRetrySuccessCount meter.Counter
	retryQueueSize        meter.Gauge
}

// newMetrics creates a new metrics instance.
func newMetrics(factory observability.Factory) *metrics {
	return &metrics{
		// DNS-related metrics
		dnsQueryCount:           factory.NewCounter("dns_query_count"),
		dnsQueryFailedCount:     factory.NewCounter("dns_query_failed_count"),
		dnsQueryTotalDuration:   factory.NewCounter("dns_query_total_duration"),
		dnsQueryDuration:        factory.NewHistogram("dns_query_duration", meter.DefBuckets),
		dnsSRVLookupCount:       factory.NewCounter("dns_srv_lookup_count"),
		dnsSRVLookupFailedCount: factory.NewCounter("dns_srv_lookup_failed_count"),

		// gRPC-related metrics
		grpcQueryCount:         factory.NewCounter("grpc_query_count"),
		grpcQueryFailedCount:   factory.NewCounter("grpc_query_failed_count"),
		grpcQueryTotalDuration: factory.NewCounter("grpc_query_total_duration"),
		grpcQueryDuration:      factory.NewHistogram("grpc_query_duration", meter.DefBuckets),

		// Summary metrics
		discoveryCount:         factory.NewCounter("discovery_count"),
		discoveryFailedCount:   factory.NewCounter("discovery_failed_count"),
		discoveryTotalDuration: factory.NewCounter("discovery_total_duration"),
		discoveryDuration:      factory.NewHistogram("discovery_duration", meter.DefBuckets),

		totalNodesCount: factory.NewGauge("total_nodes_count"),

		// Retry-related metrics
		nodeRetryCount:        factory.NewCounter("node_retry_count"),
		nodeRetryFailedCount:  factory.NewCounter("node_retry_failed_count"),
		nodeRetrySuccessCount: factory.NewCounter("node_retry_success_count"),
		retryQueueSize:        factory.NewGauge("retry_queue_size"),
	}
}

// IncRetryCount implements RetryMetrics interface.
func (m *metrics) IncRetryCount() {
	m.nodeRetryCount.Inc(1)
}

// IncRetrySuccess implements RetryMetrics interface.
func (m *metrics) IncRetrySuccess() {
	m.nodeRetrySuccessCount.Inc(1)
}

// IncRetryFailed implements RetryMetrics interface.
func (m *metrics) IncRetryFailed() {
	m.nodeRetryFailedCount.Inc(1)
}

// SetQueueSize implements RetryMetrics interface.
func (m *metrics) SetQueueSize(size float64) {
	m.retryQueueSize.Set(size)
}
