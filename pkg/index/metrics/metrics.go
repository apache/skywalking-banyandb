// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package metrics holds the index metrics shared by the native index owners.
package metrics

import (
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/pkg/meter"
)

// Metrics is the metrics surface a native.Owner-backed index (the series
// index, the Stream element index, and the Property store) reports through.
// It was formerly pkg/index/inverted.Metrics; every field it exposed for the
// legacy index writer's status either has a native-owner equivalent
// (ObserveNative, ObserveNativeActivity) or has no native counterpart and is
// kept zero.
type Metrics struct {
	totalUpdates meter.Gauge
	totalDeletes meter.Gauge
	totalBatches meter.Gauge
	totalErrors  meter.Gauge

	totalAnalysisTime meter.Gauge
	totalIndexTime    meter.Gauge

	totalTermSearchersStarted  meter.Gauge
	totalTermSearchersFinished meter.Gauge

	totalMergeStarted  meter.Gauge
	totalMergeFinished meter.Gauge
	totalMergeLatency  meter.Gauge
	totalMergeErrors   meter.Gauge

	totalMemSegments    meter.Gauge
	totalFileSegments   meter.Gauge
	curOnDiskBytes      meter.Gauge
	curOnDiskFiles      meter.Gauge
	nativeSnapshotBytes meter.Gauge

	totalDocCount meter.Gauge

	cacheGetCalls     meter.Gauge
	cacheSetCalls     meter.Gauge
	cacheMisses       meter.Gauge
	cacheEntriesCount meter.Gauge
	cacheBytesSize    meter.Gauge
	cacheMaxBytesSize meter.Gauge
}

// NewMetrics creates a new Metrics for a native-owner-backed index.
func NewMetrics(factory observability.Factory, labelNames ...string) *Metrics {
	return &Metrics{
		totalUpdates: factory.NewGauge("inverted_index_total_updates", labelNames...),
		totalDeletes: factory.NewGauge("inverted_index_total_deletes", labelNames...),
		totalBatches: factory.NewGauge("inverted_index_total_batches", labelNames...),
		totalErrors:  factory.NewGauge("inverted_index_total_errors", labelNames...),

		totalAnalysisTime: factory.NewGauge("inverted_index_total_analysis_time", labelNames...),
		totalIndexTime:    factory.NewGauge("inverted_index_total_index_time", labelNames...),

		totalTermSearchersStarted:  factory.NewGauge("inverted_index_total_term_searchers_started", labelNames...),
		totalTermSearchersFinished: factory.NewGauge("inverted_index_total_term_searchers_finished", labelNames...),

		totalMergeStarted:  factory.NewGauge("inverted_index_total_merge_started", append(labelNames, "type")...),
		totalMergeFinished: factory.NewGauge("inverted_index_total_merge_finished", append(labelNames, "type")...),
		totalMergeLatency:  factory.NewGauge("inverted_index_total_merge_latency", append(labelNames, "type")...),
		totalMergeErrors:   factory.NewGauge("inverted_index_total_merge_errors", append(labelNames, "type")...),

		totalMemSegments:    factory.NewGauge("inverted_index_total_mem_segments", labelNames...),
		totalFileSegments:   factory.NewGauge("inverted_index_total_file_segments", labelNames...),
		curOnDiskBytes:      factory.NewGauge("inverted_index_cur_on_disk_bytes", labelNames...),
		curOnDiskFiles:      factory.NewGauge("inverted_index_cur_on_disk_files", labelNames...),
		nativeSnapshotBytes: factory.NewGauge("inverted_index_native_snapshot_bytes", labelNames...),

		totalDocCount: factory.NewGauge("inverted_index_total_doc_count", labelNames...),

		cacheGetCalls:     factory.NewGauge("inverted_index_cache_get_calls", labelNames...),
		cacheSetCalls:     factory.NewGauge("inverted_index_cache_set_calls", labelNames...),
		cacheMisses:       factory.NewGauge("inverted_index_cache_misses", labelNames...),
		cacheEntriesCount: factory.NewGauge("inverted_index_cache_entries_count", labelNames...),
		cacheBytesSize:    factory.NewGauge("inverted_index_cache_bytes_size", labelNames...),
		cacheMaxBytesSize: factory.NewGauge("inverted_index_cache_max_bytes_size", labelNames...),
	}
}

// DeleteAll deletes all metrics with the given label values.
func (m *Metrics) DeleteAll(labelValues ...string) {
	if m == nil {
		return
	}
	m.totalUpdates.Delete(labelValues...)
	m.totalDeletes.Delete(labelValues...)
	m.totalBatches.Delete(labelValues...)
	m.totalErrors.Delete(labelValues...)

	m.totalAnalysisTime.Delete(labelValues...)
	m.totalIndexTime.Delete(labelValues...)

	m.totalTermSearchersStarted.Delete(labelValues...)
	m.totalTermSearchersFinished.Delete(labelValues...)

	m.totalMergeStarted.Delete(append(labelValues, "mem")...)
	m.totalMergeFinished.Delete(append(labelValues, "mem")...)
	m.totalMergeLatency.Delete(append(labelValues, "mem")...)
	m.totalMergeErrors.Delete(append(labelValues, "mem")...)

	m.totalMergeStarted.Delete(append(labelValues, "file")...)
	m.totalMergeFinished.Delete(append(labelValues, "file")...)
	m.totalMergeLatency.Delete(append(labelValues, "file")...)
	m.totalMergeErrors.Delete(append(labelValues, "file")...)

	m.totalMemSegments.Delete(labelValues...)
	m.totalFileSegments.Delete(labelValues...)
	m.curOnDiskBytes.Delete(labelValues...)
	m.curOnDiskFiles.Delete(labelValues...)
	m.nativeSnapshotBytes.Delete(labelValues...)

	m.cacheGetCalls.Delete(labelValues...)
	m.cacheSetCalls.Delete(labelValues...)
	m.cacheMisses.Delete(labelValues...)
	m.cacheEntriesCount.Delete(labelValues...)
	m.cacheBytesSize.Delete(labelValues...)
	m.cacheMaxBytesSize.Delete(labelValues...)
}

// ObserveNative records metrics available from the native owner: the live
// document count and the current immutable-root payload size.
func (m *Metrics) ObserveNative(dataCount, dataSizeBytes int64, labelValues ...string) {
	if m == nil {
		return
	}
	m.totalDocCount.Set(float64(dataCount), labelValues...)
	m.nativeSnapshotBytes.Set(float64(dataSizeBytes), labelValues...)
}

// ObserveNativeActivity records the native owner's monotonic activity
// counters under the legacy names: admitted documents as total updates, and
// acquired read views as term searchers started.
func (m *Metrics) ObserveNativeActivity(admittedDocuments, acquiredViews uint64, labelValues ...string) {
	if m == nil {
		return
	}
	m.totalUpdates.Set(float64(admittedDocuments), labelValues...)
	m.totalTermSearchersStarted.Set(float64(acquiredViews), labelValues...)
}
