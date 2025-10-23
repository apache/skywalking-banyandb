// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package trace

import (
	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/pkg/index/inverted"
	"github.com/apache/skywalking-banyandb/pkg/meter"
)

var (
	streamScope = observability.RootScope.SubScope("trace")
	tbScope     = streamScope.SubScope("tst")
)

type metrics struct {
	tbMetrics
	indexMetrics           *inverted.Metrics
	totalWritten           meter.Counter
	totalBatch             meter.Counter
	totalBatchIntroLatency meter.Counter

	totalIntroduceLoopStarted  meter.Counter
	totalIntroduceLoopFinished meter.Counter

	totalFlushLoopStarted  meter.Counter
	totalFlushLoopFinished meter.Counter
	totalFlushLoopErr      meter.Counter

	totalMergeLoopStarted  meter.Counter
	totalMergeLoopFinished meter.Counter
	totalMergeLoopErr      meter.Counter

	totalSyncLoopStarted  meter.Counter
	totalSyncLoopFinished meter.Counter
	totalSyncLoopErr      meter.Counter
	totalSyncLoopLatency  meter.Counter
	totalSyncLoopBytes    meter.Counter

	totalFlushLoopProgress   meter.Counter
	totalFlushed             meter.Counter
	totalFlushedMemParts     meter.Counter
	totalFlushPauseCompleted meter.Counter
	totalFlushPauseBreak     meter.Counter
	totalFlushIntroLatency   meter.Counter
	totalFlushLatency        meter.Counter

	totalMergedParts  meter.Counter
	totalMergeLatency meter.Counter
	totalMerged       meter.Counter
}

func (tst *tsTable) incTotalWritten(delta int) {
	if tst == nil || tst.metrics == nil {
		return
	}
	tst.metrics.totalWritten.Inc(float64(delta))
}

func (tst *tsTable) incTotalBatch(delta int) {
	if tst == nil || tst.metrics == nil {
		return
	}
	tst.metrics.totalBatch.Inc(float64(delta))
}

func (tst *tsTable) incTotalBatchIntroLatency(delta float64) {
	if tst == nil || tst.metrics == nil {
		return
	}
	tst.metrics.totalBatchIntroLatency.Inc(delta)
}

func (tst *tsTable) incTotalIntroduceLoopStarted(phase string) {
	if tst == nil || tst.metrics == nil {
		return
	}
	tst.metrics.totalIntroduceLoopStarted.Inc(1, phase)
}

func (tst *tsTable) incTotalIntroduceLoopFinished(phase string) {
	if tst == nil || tst.metrics == nil {
		return
	}
	tst.metrics.totalIntroduceLoopFinished.Inc(1, phase)
}

func (tst *tsTable) incTotalFlushLoopStarted(delta int) {
	if tst == nil || tst.metrics == nil {
		return
	}
	tst.metrics.totalFlushLoopStarted.Inc(float64(delta))
}

func (tst *tsTable) incTotalFlushLoopFinished(delta int) {
	if tst == nil || tst.metrics == nil {
		return
	}
	tst.metrics.totalFlushLoopFinished.Inc(float64(delta))
}

func (tst *tsTable) incTotalFlushLoopErr(delta int) {
	if tst == nil || tst.metrics == nil {
		return
	}
	tst.metrics.totalFlushLoopErr.Inc(float64(delta))
}

func (tst *tsTable) incTotalMergeLoopStarted(delta int) {
	if tst == nil || tst.metrics == nil {
		return
	}
	tst.metrics.totalMergeLoopStarted.Inc(float64(delta))
}

func (tst *tsTable) incTotalMergeLoopFinished(delta int) {
	if tst == nil || tst.metrics == nil {
		return
	}
	tst.metrics.totalMergeLoopFinished.Inc(float64(delta))
}

func (tst *tsTable) incTotalMergeLoopErr(delta int) {
	if tst == nil || tst.metrics == nil {
		return
	}
	tst.metrics.totalMergeLoopErr.Inc(float64(delta))
}

func (tst *tsTable) incTotalSyncLoopStarted(delta int) {
	if tst == nil || tst.metrics == nil {
		return
	}
	tst.metrics.totalSyncLoopStarted.Inc(float64(delta))
}

func (tst *tsTable) incTotalSyncLoopFinished(delta int) {
	if tst == nil || tst.metrics == nil {
		return
	}
	tst.metrics.totalSyncLoopFinished.Inc(float64(delta))
}

func (tst *tsTable) incTotalSyncLoopErr(delta int) {
	if tst == nil || tst.metrics == nil {
		return
	}
	tst.metrics.totalSyncLoopErr.Inc(float64(delta))
}

func (tst *tsTable) incTotalSyncLoopLatency(delta float64) {
	if tst == nil || tst.metrics == nil {
		return
	}
	tst.metrics.totalSyncLoopLatency.Inc(delta)
}

func (tst *tsTable) incTotalSyncLoopBytes(delta uint64) {
	if tst == nil || tst.metrics == nil {
		return
	}
	tst.metrics.totalSyncLoopBytes.Inc(float64(delta))
}

func (tst *tsTable) incTotalFlushLoopProgress(delta int) {
	if tst == nil || tst.metrics == nil {
		return
	}
	tst.metrics.totalFlushLoopProgress.Inc(float64(delta))
}

func (tst *tsTable) incTotalFlushed(delta int) {
	if tst == nil || tst.metrics == nil {
		return
	}
	tst.metrics.totalFlushed.Inc(float64(delta))
}

func (tst *tsTable) incTotalFlushedMemParts(delta int) {
	if tst == nil || tst.metrics == nil {
		return
	}
	tst.metrics.totalFlushedMemParts.Inc(float64(delta))
}

func (tst *tsTable) incTotalFlushPauseCompleted(delta int) {
	if tst == nil || tst.metrics == nil {
		return
	}
	tst.metrics.totalFlushPauseCompleted.Inc(float64(delta))
}

func (tst *tsTable) incTotalFlushPauseBreak(delta int) {
	if tst == nil || tst.metrics == nil {
		return
	}
	tst.metrics.totalFlushPauseBreak.Inc(float64(delta))
}

func (tst *tsTable) incTotalFlushIntroLatency(delta float64) {
	if tst == nil || tst.metrics == nil {
		return
	}
	tst.metrics.totalFlushIntroLatency.Inc(delta)
}

func (tst *tsTable) incTotalFlushLatency(delta float64) {
	if tst == nil || tst.metrics == nil {
		return
	}
	tst.metrics.totalFlushLatency.Inc(delta)
}

func (tst *tsTable) incTotalMergedParts(delta int, typ string) {
	if tst == nil || tst.metrics == nil {
		return
	}
	tst.metrics.totalMergedParts.Inc(float64(delta), typ)
}

func (tst *tsTable) incTotalMergeLatency(delta float64, typ string) {
	if tst == nil || tst.metrics == nil {
		return
	}
	tst.metrics.totalMergeLatency.Inc(delta, typ)
}

func (tst *tsTable) incTotalMerged(delta int, typ string) {
	if tst == nil || tst.metrics == nil {
		return
	}
	tst.metrics.totalMerged.Inc(float64(delta), typ)
}

func (m *metrics) DeleteAll() {
	if m == nil {
		return
	}
	m.totalWritten.Delete()
	m.totalBatch.Delete()
	m.totalBatchIntroLatency.Delete()

	m.totalIntroduceLoopStarted.Delete("mem")
	m.totalIntroduceLoopStarted.Delete("flush")
	m.totalIntroduceLoopStarted.Delete("merge")
	m.totalIntroduceLoopFinished.Delete("mem")
	m.totalIntroduceLoopFinished.Delete("flush")
	m.totalIntroduceLoopFinished.Delete("merge")

	m.totalFlushLoopStarted.Delete()
	m.totalFlushLoopFinished.Delete()
	m.totalFlushLoopErr.Delete()

	m.totalMergeLoopStarted.Delete()
	m.totalMergeLoopFinished.Delete()
	m.totalMergeLoopErr.Delete()

	m.totalSyncLoopStarted.Delete()
	m.totalSyncLoopFinished.Delete()
	m.totalSyncLoopErr.Delete()
	m.totalSyncLoopLatency.Delete()
	m.totalSyncLoopBytes.Delete()

	m.totalFlushLoopProgress.Delete()
	m.totalFlushed.Delete()
	m.totalFlushedMemParts.Delete()
	m.totalFlushPauseCompleted.Delete()
	m.totalFlushPauseBreak.Delete()
	m.totalFlushLatency.Delete()

	m.totalMergedParts.Delete("mem")
	m.totalMergeLatency.Delete("mem")
	m.totalMerged.Delete("mem")
	m.totalMergedParts.Delete("file")
	m.totalMergeLatency.Delete("file")
	m.totalMerged.Delete("file")
}

func (s *supplier) newMetrics(p common.Position) storage.Metrics {
	factory := s.omr.With(tbScope.ConstLabels(meter.ToLabelPairs(common.DBLabelNames(), p.DBLabelValues())))
	return &metrics{
		totalWritten:               factory.NewCounter("total_written"),
		totalBatch:                 factory.NewCounter("total_batch"),
		totalBatchIntroLatency:     factory.NewCounter("total_batch_intro_time"),
		totalIntroduceLoopStarted:  factory.NewCounter("total_introduce_loop_started", "phase"),
		totalIntroduceLoopFinished: factory.NewCounter("total_introduce_loop_finished", "phase"),
		totalFlushLoopStarted:      factory.NewCounter("total_flush_loop_started"),
		totalFlushLoopFinished:     factory.NewCounter("total_flush_loop_finished"),
		totalFlushLoopErr:          factory.NewCounter("total_flush_loop_err"),
		totalMergeLoopStarted:      factory.NewCounter("total_merge_loop_started"),
		totalMergeLoopFinished:     factory.NewCounter("total_merge_loop_finished"),
		totalMergeLoopErr:          factory.NewCounter("total_merge_loop_err"),
		totalSyncLoopStarted:       factory.NewCounter("total_sync_loop_started"),
		totalSyncLoopFinished:      factory.NewCounter("total_sync_loop_finished"),
		totalSyncLoopErr:           factory.NewCounter("total_sync_loop_err"),
		totalSyncLoopLatency:       factory.NewCounter("total_sync_loop_latency"),
		totalSyncLoopBytes:         factory.NewCounter("total_sync_loop_bytes"),
		totalFlushLoopProgress:     factory.NewCounter("total_flush_loop_progress"),
		totalFlushed:               factory.NewCounter("total_flushed"),
		totalFlushedMemParts:       factory.NewCounter("total_flushed_mem_parts"),
		totalFlushPauseCompleted:   factory.NewCounter("total_flush_pause_completed"),
		totalFlushPauseBreak:       factory.NewCounter("total_flush_pause_break"),
		totalFlushIntroLatency:     factory.NewCounter("total_flush_intro_latency"),
		totalFlushLatency:          factory.NewCounter("total_flush_latency"),
		totalMergedParts:           factory.NewCounter("total_merged_parts", "type"),
		totalMergeLatency:          factory.NewCounter("total_merge_latency", "type"),
		totalMerged:                factory.NewCounter("total_merged", "type"),
		tbMetrics: tbMetrics{
			totalMemParts:                  factory.NewGauge("total_mem_part", common.ShardLabelNames()...),
			totalMemElements:               factory.NewGauge("total_mem_elements", common.ShardLabelNames()...),
			totalMemBlocks:                 factory.NewGauge("total_mem_blocks", common.ShardLabelNames()...),
			totalMemPartBytes:              factory.NewGauge("total_mem_part_bytes", common.ShardLabelNames()...),
			totalMemPartUncompressedBytes:  factory.NewGauge("total_mem_part_uncompressed_bytes", common.ShardLabelNames()...),
			totalFileParts:                 factory.NewGauge("total_file_parts", common.ShardLabelNames()...),
			totalFileElements:              factory.NewGauge("total_file_elements", common.ShardLabelNames()...),
			totalFileBlocks:                factory.NewGauge("total_file_blocks", common.ShardLabelNames()...),
			totalFilePartBytes:             factory.NewGauge("total_file_part_bytes", common.ShardLabelNames()...),
			totalFilePartUncompressedBytes: factory.NewGauge("total_file_part_uncompressed_bytes", common.ShardLabelNames()...),
		},
		indexMetrics: inverted.NewMetrics(factory, common.SegLabelNames()...),
	}
}

func (qs *queueSupplier) newMetrics(p common.Position) storage.Metrics {
	factory := qs.omr.With(tbScope.ConstLabels(meter.ToLabelPairs(common.DBLabelNames(), p.DBLabelValues())))
	return &metrics{
		totalWritten:               factory.NewCounter("total_written"),
		totalBatch:                 factory.NewCounter("total_batch"),
		totalBatchIntroLatency:     factory.NewCounter("total_batch_intro_time"),
		totalIntroduceLoopStarted:  factory.NewCounter("total_introduce_loop_started", "phase"),
		totalIntroduceLoopFinished: factory.NewCounter("total_introduce_loop_finished", "phase"),
		totalFlushLoopStarted:      factory.NewCounter("total_flush_loop_started"),
		totalFlushLoopFinished:     factory.NewCounter("total_flush_loop_finished"),
		totalFlushLoopErr:          factory.NewCounter("total_flush_loop_err"),
		totalMergeLoopStarted:      factory.NewCounter("total_merge_loop_started"),
		totalMergeLoopFinished:     factory.NewCounter("total_merge_loop_finished"),
		totalMergeLoopErr:          factory.NewCounter("total_merge_loop_err"),
		totalSyncLoopStarted:       factory.NewCounter("total_sync_loop_started"),
		totalSyncLoopFinished:      factory.NewCounter("total_sync_loop_finished"),
		totalSyncLoopErr:           factory.NewCounter("total_sync_loop_err"),
		totalSyncLoopLatency:       factory.NewCounter("total_sync_loop_latency"),
		totalSyncLoopBytes:         factory.NewCounter("total_sync_loop_bytes"),
		totalFlushLoopProgress:     factory.NewCounter("total_flush_loop_progress"),
		totalFlushed:               factory.NewCounter("total_flushed"),
		totalFlushedMemParts:       factory.NewCounter("total_flushed_mem_parts"),
		totalFlushPauseCompleted:   factory.NewCounter("total_flush_pause_completed"),
		totalFlushPauseBreak:       factory.NewCounter("total_flush_pause_break"),
		totalFlushIntroLatency:     factory.NewCounter("total_flush_intro_latency"),
		totalFlushLatency:          factory.NewCounter("total_flush_latency"),
		totalMergedParts:           factory.NewCounter("total_merged_parts", "type"),
		totalMergeLatency:          factory.NewCounter("total_merge_latency", "type"),
		totalMerged:                factory.NewCounter("total_merged", "type"),
		tbMetrics: tbMetrics{
			totalMemParts:                  factory.NewGauge("total_mem_part", common.ShardLabelNames()...),
			totalMemElements:               factory.NewGauge("total_mem_elements", common.ShardLabelNames()...),
			totalMemBlocks:                 factory.NewGauge("total_mem_blocks", common.ShardLabelNames()...),
			totalMemPartBytes:              factory.NewGauge("total_mem_part_bytes", common.ShardLabelNames()...),
			totalMemPartUncompressedBytes:  factory.NewGauge("total_mem_part_uncompressed_bytes", common.ShardLabelNames()...),
			totalFileParts:                 factory.NewGauge("total_file_parts", common.ShardLabelNames()...),
			totalFileElements:              factory.NewGauge("total_file_elements", common.ShardLabelNames()...),
			totalFileBlocks:                factory.NewGauge("total_file_blocks", common.ShardLabelNames()...),
			totalFilePartBytes:             factory.NewGauge("total_file_part_bytes", common.ShardLabelNames()...),
			totalFilePartUncompressedBytes: factory.NewGauge("total_file_part_uncompressed_bytes", common.ShardLabelNames()...),
		},
		indexMetrics: inverted.NewMetrics(factory, common.SegLabelNames()...),
	}
}

func (tst *tsTable) Collect(m storage.Metrics) {
	if m == nil {
		return
	}
	metrics := m.(*metrics)
	snp := tst.currentSnapshot()
	if snp == nil {
		return
	}

	defer snp.decRef()

	var totalMemPart, totalMemElements, totalMemBlocks, totalMemPartBytes, totalMemPartUncompressedBytes uint64
	var totalFileParts, totalFileElements, totalFileBlocks, totalFilePartBytes, totalFilePartUncompressedBytes uint64
	for _, p := range snp.parts {
		if p.mp == nil {
			totalFileParts++
			totalFileElements += p.p.partMetadata.TotalCount
			totalFileBlocks += p.p.partMetadata.BlocksCount
			totalFilePartBytes += p.p.partMetadata.CompressedSizeBytes
			totalFilePartUncompressedBytes += p.p.partMetadata.UncompressedSpanSizeBytes
			continue
		}
		totalMemPart++
		totalMemElements += p.mp.partMetadata.TotalCount
		totalMemBlocks += p.mp.partMetadata.BlocksCount
		totalMemPartBytes += p.mp.partMetadata.CompressedSizeBytes
		totalMemPartUncompressedBytes += p.mp.partMetadata.UncompressedSpanSizeBytes
	}
	metrics.totalMemParts.Set(float64(totalMemPart), tst.p.ShardLabelValues()...)
	metrics.totalMemElements.Set(float64(totalMemElements), tst.p.ShardLabelValues()...)
	metrics.totalMemBlocks.Set(float64(totalMemBlocks), tst.p.ShardLabelValues()...)
	metrics.totalMemPartBytes.Set(float64(totalMemPartBytes), tst.p.ShardLabelValues()...)
	metrics.totalMemPartUncompressedBytes.Set(float64(totalMemPartUncompressedBytes), tst.p.ShardLabelValues()...)
	metrics.totalFileParts.Set(float64(totalFileParts), tst.p.ShardLabelValues()...)
	metrics.totalFileElements.Set(float64(totalFileElements), tst.p.ShardLabelValues()...)
	metrics.totalFileBlocks.Set(float64(totalFileBlocks), tst.p.ShardLabelValues()...)
	metrics.totalFilePartBytes.Set(float64(totalFilePartBytes), tst.p.ShardLabelValues()...)
	metrics.totalFilePartUncompressedBytes.Set(float64(totalFilePartUncompressedBytes), tst.p.ShardLabelValues()...)
}

func (tst *tsTable) deleteMetrics() {
	if tst.metrics == nil {
		return
	}
	tst.metrics.tbMetrics.totalMemParts.Delete(tst.p.ShardLabelValues()...)
	tst.metrics.tbMetrics.totalMemElements.Delete(tst.p.ShardLabelValues()...)
	tst.metrics.tbMetrics.totalMemBlocks.Delete(tst.p.ShardLabelValues()...)
	tst.metrics.tbMetrics.totalMemPartBytes.Delete(tst.p.ShardLabelValues()...)
	tst.metrics.tbMetrics.totalMemPartUncompressedBytes.Delete(tst.p.ShardLabelValues()...)
	tst.metrics.tbMetrics.totalFileParts.Delete(tst.p.ShardLabelValues()...)
	tst.metrics.tbMetrics.totalFileElements.Delete(tst.p.ShardLabelValues()...)
	tst.metrics.tbMetrics.totalFileBlocks.Delete(tst.p.ShardLabelValues()...)
	tst.metrics.tbMetrics.totalFilePartBytes.Delete(tst.p.ShardLabelValues()...)
	tst.metrics.tbMetrics.totalFilePartUncompressedBytes.Delete(tst.p.ShardLabelValues()...)
	tst.metrics.indexMetrics.DeleteAll(tst.p.SegLabelValues()...)
}

type tbMetrics struct {
	totalMemParts                 meter.Gauge
	totalMemElements              meter.Gauge
	totalMemBlocks                meter.Gauge
	totalMemPartBytes             meter.Gauge
	totalMemPartUncompressedBytes meter.Gauge

	totalFileParts                 meter.Gauge
	totalFileElements              meter.Gauge
	totalFileBlocks                meter.Gauge
	totalFilePartBytes             meter.Gauge
	totalFilePartUncompressedBytes meter.Gauge
}
