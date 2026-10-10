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
// (ObserveNative) or has no native counterpart and is kept zero.
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

	// nativeTimeSegments and nativeTimeCandidatesPruned back NativeTimeMetrics,
	// the native time-range pruning observability surface (native-index-time-
	// pruning design, §6).
	nativeTimeSegments         meter.Counter
	nativeTimeCandidatesPruned meter.Counter
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

		nativeTimeSegments:         factory.NewCounter("native_time_segments_total", append(labelNames, "class")...),
		nativeTimeCandidatesPruned: factory.NewCounter("native_time_candidates_pruned_total", labelNames...),
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

	for _, class := range nativeTimeClasses {
		m.nativeTimeSegments.Delete(append(append([]string{}, labelValues...), class)...)
	}
	m.nativeTimeCandidatesPruned.Delete(labelValues...)
}

// nativeTimeClasses are every value the "class" label on
// native_time_segments_total takes, mirrored here so DeleteAll can clear each
// one. pkg/index/native classifies a segment into exactly one of these per
// query (native-index-time-pruning design, §4.2, §4.5).
var nativeTimeClasses = [...]string{"disjoint", "contained", "overlap_trie", "overlap_fallback"}

// NativeTimeMetrics returns a recorder bound to labelValues -- the same
// per-owner identity every other metric on m uses -- implementing the
// pkg/index/native.TimeMetrics capability. This package does not import
// native: the two methods below satisfy that interface structurally.
func (m *Metrics) NativeTimeMetrics(labelValues ...string) *NativeTimeMetrics {
	if m == nil {
		return nil
	}
	return &NativeTimeMetrics{metrics: m, labelValues: append([]string(nil), labelValues...)}
}

// NativeTimeMetrics records native time-range pruning counts for one bound
// label set. A nil *NativeTimeMetrics is valid everywhere its methods are
// called and drops every count, so native.OwnerOptions.TimeMetrics may be set
// unconditionally from NativeTimeMetrics's result even when the underlying
// *Metrics is nil.
type NativeTimeMetrics struct {
	metrics     *Metrics
	labelValues []string
}

// IncTimeSegments records one segment's time-range classification: one of
// nativeTimeClasses. It implements pkg/index/native.TimeMetrics.
func (r *NativeTimeMetrics) IncTimeSegments(class string) {
	if r == nil || r.metrics == nil {
		return
	}
	r.metrics.nativeTimeSegments.Inc(1, append(append([]string(nil), r.labelValues...), class)...)
}

// AddTimeCandidatesPruned adds delta candidates removed by intersecting a
// segment's candidates with the _timestamp trie's coverage. It implements
// pkg/index/native.TimeMetrics.
func (r *NativeTimeMetrics) AddTimeCandidatesPruned(delta uint64) {
	if r == nil || r.metrics == nil || delta == 0 {
		return
	}
	r.metrics.nativeTimeCandidatesPruned.Inc(float64(delta), r.labelValues...)
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
