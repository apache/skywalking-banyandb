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

package grpc

import (
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/pkg/meter"
)

type metrics struct {
	totalStarted  meter.Counter
	totalFinished meter.Counter
	totalErr      meter.Counter
	totalPanic    meter.Counter
	totalLatency  meter.Counter

	totalStreamStarted  meter.Counter
	totalStreamFinished meter.Counter
	totalStreamErr      meter.Counter
	totalStreamLatency  meter.Counter

	totalStreamMsgReceived    meter.Counter
	totalStreamMsgReceivedErr meter.Counter
	totalStreamMsgSent        meter.Counter
	totalStreamMsgSentErr     meter.Counter

	totalRegistryStarted  meter.Counter
	totalRegistryFinished meter.Counter
	totalRegistryErr      meter.Counter
	totalRegistryLatency  meter.Counter

	memoryLoadSheddingRejections meter.Counter
	grpcBufferSize               meter.Gauge // Shared gauge for both conn and stream buffer sizes
	memoryState                  meter.Gauge

	// Step 2.7 — schema-barrier observability. The three Await* RPCs each
	// emit one histogram observation per call labeled by `result`
	// ("applied", "timeout", "invalid_argument"). Laggards observed during
	// any barrier are counted by `barrier`, `role`, `node` so dashboards
	// can break out which node fell behind on which call. The two status
	// counters cover the gate's STATUS_SCHEMA_NOT_APPLIED / EXPIRED_SCHEMA
	// outcomes by `rpc` and `group`; in v0.11.0 the only `reason` value
	// emitted is "wait_timeout" — the label is retained for forward-compat
	// with optional fast-sync paths that may land in a later release.
	schemaAwaitRevisionAppliedDuration meter.Histogram
	schemaAwaitSchemaAppliedDuration   meter.Histogram
	schemaAwaitSchemaDeletedDuration   meter.Histogram
	schemaBarrierLaggards              meter.Counter
	schemaStatusNotApplied             meter.Counter
	schemaStatusExpired                meter.Counter

	// BydbQL prepared-statement cache. bydbqlPreparedCacheTotal counts lookups by
	// `result` ("hit"/"miss"); the gauges track the live hit ratio and the cache's
	// current entry count and approximate byte size.
	bydbqlPreparedCacheTotal    meter.Counter
	bydbqlPreparedCacheHitRatio meter.Gauge
	bydbqlPreparedCacheCount    meter.Gauge
	bydbqlPreparedCacheBytes    meter.Gauge
	bydbqlSlowQueryTotal        meter.Counter
}

func newMetrics(factory observability.Factory) *metrics {
	return &metrics{
		totalStarted:                       factory.NewCounter("total_started", "group", "service", "method"),
		totalFinished:                      factory.NewCounter("total_finished", "group", "service", "method"),
		totalErr:                           factory.NewCounter("total_err", "group", "service", "method"),
		totalPanic:                         factory.NewCounter("total_panic"),
		totalLatency:                       factory.NewCounter("total_latency", "group", "service", "method"),
		totalStreamStarted:                 factory.NewCounter("total_stream_started", "service", "method"),
		totalStreamFinished:                factory.NewCounter("total_stream_finished", "service", "method"),
		totalStreamErr:                     factory.NewCounter("total_stream_err", "service", "method"),
		totalStreamLatency:                 factory.NewCounter("total_stream_latency", "service", "method"),
		totalStreamMsgReceived:             factory.NewCounter("total_stream_msg_received", "group", "service", "method"),
		totalStreamMsgReceivedErr:          factory.NewCounter("total_stream_msg_received_err", "group", "service", "method"),
		totalStreamMsgSent:                 factory.NewCounter("total_stream_msg_sent", "group", "service", "method"),
		totalStreamMsgSentErr:              factory.NewCounter("total_stream_msg_sent_err", "group", "service", "method"),
		totalRegistryStarted:               factory.NewCounter("total_registry_started", "group", "service", "method"),
		totalRegistryFinished:              factory.NewCounter("total_registry_finished", "group", "service", "method"),
		totalRegistryErr:                   factory.NewCounter("total_registry_err", "group", "service", "method"),
		totalRegistryLatency:               factory.NewCounter("total_registry_latency", "group", "service", "method"),
		memoryLoadSheddingRejections:       factory.NewCounter("memory_load_shedding_rejections_total", "service"),
		grpcBufferSize:                     factory.NewGauge("grpc_buffer_size_bytes", "type"),
		memoryState:                        factory.NewGauge("memory_state"),
		schemaAwaitRevisionAppliedDuration: factory.NewHistogram("schema_await_revision_applied_duration_seconds", meter.DefBuckets, "result"),
		schemaAwaitSchemaAppliedDuration:   factory.NewHistogram("schema_await_schema_applied_duration_seconds", meter.DefBuckets, "result"),
		schemaAwaitSchemaDeletedDuration:   factory.NewHistogram("schema_await_schema_deleted_duration_seconds", meter.DefBuckets, "result"),
		schemaBarrierLaggards:              factory.NewCounter("schema_barrier_laggard_nodes_total", "barrier", "role", "node"),
		schemaStatusNotApplied:             factory.NewCounter("schema_status_schema_not_applied_total", "rpc", "group", "reason"),
		schemaStatusExpired:                factory.NewCounter("schema_status_expired_schema_total", "rpc", "group"),
		bydbqlPreparedCacheTotal:           factory.NewCounter("bydbql_prepared_cache_total", "result"),
		bydbqlPreparedCacheHitRatio:        factory.NewGauge("bydbql_prepared_cache_hit_ratio"),
		bydbqlPreparedCacheCount:           factory.NewGauge("bydbql_prepared_cache_count"),
		bydbqlPreparedCacheBytes:           factory.NewGauge("bydbql_prepared_cache_bytes"),
		bydbqlSlowQueryTotal:               factory.NewCounter("bydbql_slow_query_total"),
	}
}

// updateBufferSizeMetrics updates the buffer size metrics.
func (m *metrics) updateBufferSizeMetrics(connSize, streamSize int32) {
	if connSize > 0 {
		m.grpcBufferSize.Set(float64(connSize), "conn")
	}
	if streamSize > 0 {
		m.grpcBufferSize.Set(float64(streamSize), "stream")
	}
}

// updateMemoryState updates the memory state metric.
func (m *metrics) updateMemoryState(state int) {
	m.memoryState.Set(float64(state))
}
