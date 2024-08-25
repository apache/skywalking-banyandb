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

package inverted

import (
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/pkg/meter"
)

// Metrics is the metrics for the inverted index.
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

	totalMemSegments  meter.Gauge
	totalFileSegments meter.Gauge
	curOnDiskBytes    meter.Gauge
	curOnDiskFiles    meter.Gauge

	totalDocCount meter.Gauge
}

// NewMetrics creates a new metrics for the inverted index.
func NewMetrics(factory *observability.Factory, labelNames ...string) *Metrics {
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

		totalMemSegments:  factory.NewGauge("inverted_index_total_mem_segments", labelNames...),
		totalFileSegments: factory.NewGauge("inverted_index_total_file_segments", labelNames...),
		curOnDiskBytes:    factory.NewGauge("inverted_index_cur_on_disk_bytes", labelNames...),
		curOnDiskFiles:    factory.NewGauge("inverted_index_cur_on_disk_files", labelNames...),

		totalDocCount: factory.NewGauge("inverted_index_total_doc_count", labelNames...),
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
}

func (s *store) CollectMetrics(labelValues ...string) {
	if s.metrics == nil {
		return
	}
	status := s.writer.Status()
	s.metrics.totalUpdates.Set(float64(status.TotUpdates), labelValues...)
	s.metrics.totalDeletes.Set(float64(status.TotDeletes), labelValues...)
	s.metrics.totalBatches.Set(float64(status.TotBatches), labelValues...)
	s.metrics.totalErrors.Set(float64(status.TotOnErrors), labelValues...)

	s.metrics.totalAnalysisTime.Set(float64(status.TotAnalysisTime), labelValues...)
	s.metrics.totalIndexTime.Set(float64(status.TotIndexTime), labelValues...)

	s.metrics.totalTermSearchersStarted.Set(float64(status.TotTermSearchersStarted), labelValues...)
	s.metrics.totalTermSearchersFinished.Set(float64(status.TotTermSearchersFinished), labelValues...)

	s.metrics.totalMergeStarted.Set(float64(status.TotMemMergeZapBeg), append(labelValues, "mem")...)
	s.metrics.totalMergeFinished.Set(float64(status.TotMemMergeZapEnd), append(labelValues, "mem")...)
	s.metrics.totalMergeLatency.Set(float64(status.TotMemMergeZapTime), append(labelValues, "mem")...)
	s.metrics.totalMergeErrors.Set(float64(status.TotMemMergeErr), append(labelValues, "mem")...)

	s.metrics.totalMergeStarted.Set(float64(status.TotFileMergeZapBeg), append(labelValues, "file")...)
	s.metrics.totalMergeFinished.Set(float64(status.TotFileMergeZapEnd), append(labelValues, "file")...)
	s.metrics.totalMergeLatency.Set(float64(status.TotFileMergeZapTime), append(labelValues, "file")...)
	s.metrics.totalMergeErrors.Set(float64(status.TotFileMergeLoopErr+status.TotFileMergePlanErr+status.TotFileMergePlanTasksErr), append(labelValues, "file")...)

	s.metrics.totalMemSegments.Set(float64(status.TotMemorySegmentsAtRoot), labelValues...)
	s.metrics.totalFileSegments.Set(float64(status.TotFileSegmentsAtRoot), labelValues...)
	s.metrics.curOnDiskBytes.Set(float64(status.CurOnDiskBytes), labelValues...)
	s.metrics.curOnDiskFiles.Set(float64(status.CurOnDiskFiles), labelValues...)

	r, err := s.writer.Reader()
	if err != nil {
		return
	}
	defer r.Close()
	n, err := r.Count()
	if err != nil {
		return
	}
	s.metrics.totalDocCount.Set(float64(n), labelValues...)
}
