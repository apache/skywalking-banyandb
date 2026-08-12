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
	"sync"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/pkg/index/inverted"
	"github.com/apache/skywalking-banyandb/pkg/meter"
	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk"
)

var (
	tbScope       = traceScope.SubScope("tst")
	storageScope  = traceScope.SubScope("storage")
	pipelineScope = traceScope.SubScope("pipeline")
)

type metrics struct {
	tbMetrics
	totalMergeBackoffSeconds meter.Counter
	// Plugin overhead is emitted once per decision batch and once per executed
	// plugin link, never per trace. Names come from the bounded configured chain;
	// result and reason values are closed constants.
	pipelinePluginDurationSeconds    meter.Histogram
	totalIntroduceLoopStarted        meter.Counter
	totalIntroduceLoopFinished       meter.Counter
	totalFlushLoopStarted            meter.Counter
	totalFlushLoopFinished           meter.Counter
	totalFlushLoopErr                meter.Counter
	totalMergeLoopStarted            meter.Counter
	totalMergeLoopFinished           meter.Counter
	totalMergeLoopErr                meter.Counter
	totalSyncLoopStarted             meter.Counter
	totalSyncLoopFinished            meter.Counter
	totalSyncLoopErr                 meter.Counter
	totalSyncLoopLatency             meter.Counter
	totalSyncLoopBytes               meter.Counter
	totalFlushLoopProgress           meter.Counter
	totalFlushed                     meter.Counter
	totalFlushedMemParts             meter.Counter
	totalFlushPauseCompleted         meter.Counter
	totalFlushPauseBreak             meter.Counter
	totalFlushIntroLatency           meter.Counter
	totalFlushLatency                meter.Counter
	totalMergedParts                 meter.Counter
	totalMergeLatency                meter.Counter
	totalMerged                      meter.Counter
	totalMergeQueueLatency           meter.Counter
	totalMergePartQuarantined        meter.Counter
	totalWritten                     meter.Counter
	totalBatchIntroLatency           meter.Counter
	pipelineTracesDropped            meter.Counter
	totalMergePanicRecovered         meter.Counter
	pipelineTracesRetained           meter.Counter
	pipelineTracesImmature           meter.Counter
	pipelineOversizedTracesBypassed  meter.Counter
	pipelinePluginErrors             meter.Counter
	pipelinePluginBatches            meter.Counter
	pipelinePluginExecutions         meter.Counter
	pipelineTracesEvaluated          meter.Counter
	pipelinePluginBatchTraces        meter.Histogram
	pipelinePluginLinkBypasses       meter.Counter
	pipelineAmbiguous                meter.Counter
	pipelineSidxPruned               meter.Counter
	pipelineGuardBloomProbes         meter.Counter
	pipelineGuardDeferred            meter.Counter
	pipelineGuardBudgetExhausted     meter.Counter
	pipelineGuardPublicationRejected meter.Counter
	pipelineGuardLosslessRetry       meter.Counter
	pipelineGuardBypassed            meter.Counter
	// Ceiling metrics are group-scoped. The lane is retained only where ordinary
	// and finalize merge behavior must be distinguished.
	pipelineTracesRetainedByCeiling meter.Counter
	pipelineMergesCeilingReached    meter.Counter
	// The budget gauge and entry histogram expose drop-set headroom before the
	// ceiling starts retaining traces.
	pipelineDropSetBudgetBytes meter.Gauge
	pipelineDropSetEntries     meter.Histogram
	totalBatch                 meter.Counter
	indexMetrics               *inverted.Metrics
	pipelinePluginNames        sync.Map
	pipelinePluginLifecycleMu  sync.RWMutex
	pipelinePluginClosed       bool
}

// dropSetEntryBuckets spans a single dropped trace ID to well past the ceiling a
// 16 GiB node derives (~671k entries, spec section 3.4), so a dashboard can watch
// the distribution climb toward whatever ceiling resolveDropSetBudget produced.
var dropSetEntryBuckets = meter.Buckets{
	1, 100, 1_000, 10_000, 50_000, 100_000, 250_000, 500_000, 1_000_000, 2_000_000,
}

var (
	pipelinePluginDurationBuckets = meter.Buckets{
		0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10,
	}
	pipelinePluginBatchTraceBuckets = meter.Buckets{
		1, 8, 32, 128, 512, 2_048, 8_192, 32_768, 131_072, 524_288, 1_048_576,
	}
)

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

func (tst *tsTable) incTotalMergedParts(delta int, typ, lane string) {
	if tst == nil || tst.metrics == nil {
		return
	}
	tst.metrics.totalMergedParts.Inc(float64(delta), typ, lane)
}

func (tst *tsTable) incTotalMergeLatency(delta float64, typ, lane string) {
	if tst == nil || tst.metrics == nil {
		return
	}
	tst.metrics.totalMergeLatency.Inc(delta, typ, lane)
}

func (tst *tsTable) incTotalMerged(delta int, typ, lane string) {
	if tst == nil || tst.metrics == nil {
		return
	}
	tst.metrics.totalMerged.Inc(float64(delta), typ, lane)
}

func (tst *tsTable) incTotalMergeQueueLatency(delta float64, typ, lane string) {
	if tst == nil || tst.metrics == nil {
		return
	}
	tst.metrics.totalMergeQueueLatency.Inc(delta, typ, lane)
}

// incTotalMergePartQuarantined counts a part newly crossing the quarantine threshold.
func (tst *tsTable) incTotalMergePartQuarantined(delta int) {
	if tst == nil || tst.metrics == nil {
		return
	}
	tst.metrics.totalMergePartQuarantined.Inc(float64(delta))
}

// incTotalMergeBackoffSeconds accumulates the time the dispatcher spent sleeping due to
// repeated merge failures (Fix C).
func (tst *tsTable) incTotalMergeBackoffSeconds(delta float64) {
	if tst == nil || tst.metrics == nil {
		return
	}
	tst.metrics.totalMergeBackoffSeconds.Inc(delta)
}

// incTotalMergePanicRecovered counts a panic caught and converted into an ordinary merge
// failure by the Fix D backstop (worker or dispatcher).
func (tst *tsTable) incTotalMergePanicRecovered(delta int) {
	if tst == nil || tst.metrics == nil {
		return
	}
	tst.metrics.totalMergePanicRecovered.Inc(float64(delta))
}

// The pipeline metric increment helpers below are wired into the merge filter by
// the config-driven activation story.

//nolint:unused
func (tst *tsTable) incPipelineTracesEvaluated(delta int) {
	if tst == nil || tst.metrics == nil {
		return
	}
	tst.metrics.pipelineTracesEvaluated.Inc(float64(delta))
}

//nolint:unused
func (tst *tsTable) incPipelineTracesDropped(delta int) {
	if tst == nil || tst.metrics == nil {
		return
	}
	tst.metrics.pipelineTracesDropped.Inc(float64(delta))
}

//nolint:unused
func (tst *tsTable) incPipelineTracesRetained(delta int) {
	if tst == nil || tst.metrics == nil {
		return
	}
	tst.metrics.pipelineTracesRetained.Inc(float64(delta))
}

// incPipelineTracesRetainedByCeiling counts a trace retained because the
// merge's drop-set ceiling was reached, distinct from retained-by-verdict.
// Every such trace also increments incPipelineTracesRetained.
func (tst *tsTable) incPipelineTracesRetainedByCeiling(delta int) {
	if tst == nil || tst.metrics == nil {
		return
	}
	tst.metrics.pipelineTracesRetainedByCeiling.Inc(float64(delta))
}

// incPipelineMergesCeilingReached counts one merge whose drop-set ceiling was
// reached, split by lane.
func (tst *tsTable) incPipelineMergesCeilingReached(lane string) {
	if tst == nil || tst.metrics == nil {
		return
	}
	tst.metrics.pipelineMergesCeilingReached.Inc(1, lane)
}

// observeDropSetUsage publishes what one merge's drop set cost against the
// ceiling it was charged: the resolved budget as a gauge, and the number of IDs
// the merge finished holding as a distribution. Called once per merge, for capped
// and uncapped merges alike — an uncapped observation is what makes headroom
// visible before the ceiling bites.
func (tst *tsTable) observeDropSetUsage(budget uint64, entries int, lane string) {
	if tst == nil || tst.metrics == nil || budget == 0 {
		return
	}
	tst.metrics.pipelineDropSetBudgetBytes.Set(float64(budget))
	tst.metrics.pipelineDropSetEntries.Observe(float64(entries), lane)
}

//nolint:unused
func (tst *tsTable) incPipelineTracesImmature(delta int) {
	if tst == nil || tst.metrics == nil {
		return
	}
	tst.metrics.pipelineTracesImmature.Inc(float64(delta))
}

//nolint:unused
func (tst *tsTable) incPipelineOversizedTracesBypassed(delta int) {
	if tst == nil || tst.metrics == nil {
		return
	}
	tst.metrics.pipelineOversizedTracesBypassed.Inc(float64(delta))
}

//nolint:unused
func (tst *tsTable) incPipelinePluginErrors(delta int, reason string) {
	if tst == nil || tst.metrics == nil {
		return
	}
	tst.metrics.pipelinePluginErrors.Inc(float64(delta), reason)
}

func (tst *tsTable) observePipelinePluginExecution(observation pluginExecutionObservation) {
	if tst == nil || tst.metrics == nil {
		return
	}
	tst.metrics.pipelinePluginLifecycleMu.RLock()
	defer tst.metrics.pipelinePluginLifecycleMu.RUnlock()
	if tst.metrics.pipelinePluginClosed {
		return
	}
	tst.metrics.pipelinePluginBatches.Inc(1, observation.result)
	tst.metrics.pipelinePluginBatchTraces.Observe(float64(observation.batchTraces), observation.result)
}

func (tst *tsTable) observePipelinePluginLinkExecution(observation pluginLinkExecutionObservation) {
	if tst == nil || tst.metrics == nil {
		return
	}
	tst.metrics.pipelinePluginLifecycleMu.RLock()
	defer tst.metrics.pipelinePluginLifecycleMu.RUnlock()
	if tst.metrics.pipelinePluginClosed {
		return
	}
	tst.metrics.pipelinePluginNames.Store(observation.pluginName, struct{}{})
	tst.metrics.pipelinePluginExecutions.Inc(1, observation.pluginName, observation.result)
	tst.metrics.pipelinePluginDurationSeconds.Observe(observation.elapsed.Seconds(), observation.pluginName, observation.result)
	if observation.bypassReason != "" {
		tst.metrics.pipelinePluginLinkBypasses.Inc(1, observation.pluginName, observation.bypassReason)
	}
}

//nolint:unused
func (tst *tsTable) incPipelineAmbiguous(delta int) {
	if tst == nil || tst.metrics == nil {
		return
	}
	tst.metrics.pipelineAmbiguous.Inc(float64(delta))
}

//nolint:unused
func (tst *tsTable) incPipelineSidxPruned(delta int) {
	if tst == nil || tst.metrics == nil {
		return
	}
	tst.metrics.pipelineSidxPruned.Inc(float64(delta))
}

func (tst *tsTable) incPipelineGuardBloomProbes(delta int) {
	if tst == nil || tst.metrics == nil || delta <= 0 {
		return
	}
	tst.metrics.pipelineGuardBloomProbes.Inc(float64(delta))
}

func (tst *tsTable) incPipelineGuardDeferred(delta int) {
	if tst == nil || tst.metrics == nil || delta <= 0 {
		return
	}
	tst.metrics.pipelineGuardDeferred.Inc(float64(delta))
}

func (tst *tsTable) incPipelineGuardBudgetExhausted(delta int) {
	if tst == nil || tst.metrics == nil || delta <= 0 {
		return
	}
	tst.metrics.pipelineGuardBudgetExhausted.Inc(float64(delta))
}

func (tst *tsTable) incPipelineGuardPublicationRejected(delta int) {
	if tst == nil || tst.metrics == nil || delta <= 0 {
		return
	}
	tst.metrics.pipelineGuardPublicationRejected.Inc(float64(delta))
}

func (tst *tsTable) incPipelineGuardLosslessRetry(delta int) {
	if tst == nil || tst.metrics == nil || delta <= 0 {
		return
	}
	tst.metrics.pipelineGuardLosslessRetry.Inc(float64(delta))
}

func (tst *tsTable) incPipelineGuardBypassed() {
	if tst == nil || tst.metrics == nil {
		return
	}
	tst.metrics.pipelineGuardBypassed.Inc(1)
}

func (tst *tsTable) addPendingDataCount(delta int64) {
	tst.pendingDataCount.Add(delta)
	if tst.metrics == nil {
		return
	}
	tst.metrics.tbMetrics.pendingDataCount.Add(float64(delta), tst.p.ShardLabelValues()...)
}

func (tst *tsTable) getPendingDataCount() int64 {
	return tst.pendingDataCount.Load()
}

func (m *metrics) DeleteAll() {
	if m == nil {
		return
	}
	m.pipelinePluginLifecycleMu.Lock()
	defer m.pipelinePluginLifecycleMu.Unlock()
	m.pipelinePluginClosed = true
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

	m.totalMergedParts.Delete("mem", "")
	m.totalMergeLatency.Delete("mem", "")
	m.totalMerged.Delete("mem", "")
	m.totalMergedParts.Delete("file", "fast")
	m.totalMergeLatency.Delete("file", "fast")
	m.totalMerged.Delete("file", "fast")
	m.totalMergedParts.Delete("file", "slow")
	m.totalMergeLatency.Delete("file", "slow")
	m.totalMerged.Delete("file", "slow")
	m.totalMergeQueueLatency.Delete("file", "fast")
	m.totalMergeQueueLatency.Delete("file", "slow")
	m.totalMergePartQuarantined.Delete()
	m.totalMergeBackoffSeconds.Delete()
	m.totalMergePanicRecovered.Delete()

	m.pipelineTracesEvaluated.Delete()
	m.pipelineTracesDropped.Delete()
	m.pipelineTracesRetained.Delete()
	m.pipelineTracesImmature.Delete()
	m.pipelineOversizedTracesBypassed.Delete()
	for _, result := range []string{pluginExecutionResultSuccess, pluginExecutionResultTimeout, pluginExecutionResultCircuitOpen} {
		m.pipelinePluginBatches.Delete(result)
		m.pipelinePluginBatchTraces.Delete(result)
	}
	m.pipelinePluginNames.Range(func(name, _ any) bool {
		pluginName := name.(string)
		for _, result := range []string{
			pluginExecutionResultSuccess, pluginExecutionResultDecideError, pluginExecutionResultMismatch,
			pluginExecutionResultPanic, pluginExecutionResultLate,
		} {
			m.pipelinePluginExecutions.Delete(pluginName, result)
			m.pipelinePluginDurationSeconds.Delete(pluginName, result)
		}
		for _, reason := range []string{sdk.BypassReasonDecideError, sdk.BypassReasonLengthMismatch, sdk.BypassReasonPanic} {
			m.pipelinePluginLinkBypasses.Delete(pluginName, reason)
		}
		m.pipelinePluginNames.Delete(pluginName)
		return true
	})
	m.pipelineAmbiguous.Delete()
	m.pipelineSidxPruned.Delete()
	m.pipelineGuardBloomProbes.Delete()
	m.pipelineGuardDeferred.Delete()
	m.pipelineGuardBudgetExhausted.Delete()
	m.pipelineGuardPublicationRejected.Delete()
	m.pipelineGuardLosslessRetry.Delete()
	m.pipelineGuardBypassed.Delete()
	m.pipelineTracesRetainedByCeiling.Delete()
	for _, lane := range []string{"", mergeLaneFast, mergeLaneSlow, mergeLaneFinalize} {
		m.pipelineMergesCeilingReached.Delete(lane)
		m.pipelineDropSetEntries.Delete(lane)
	}
	m.pipelineDropSetBudgetBytes.Delete()
}

func (s *supplier) newMetrics(p common.Position) storage.Metrics {
	factory := s.omr.With(tbScope.ConstLabels(meter.ToLabelPairs(common.DBLabelNames(), p.DBLabelValues())))
	return &metrics{
		totalWritten:                     factory.NewCounter("total_written"),
		totalBatch:                       factory.NewCounter("total_batch"),
		totalBatchIntroLatency:           factory.NewCounter("total_batch_intro_time"),
		totalIntroduceLoopStarted:        factory.NewCounter("total_introduce_loop_started", "phase"),
		totalIntroduceLoopFinished:       factory.NewCounter("total_introduce_loop_finished", "phase"),
		totalFlushLoopStarted:            factory.NewCounter("total_flush_loop_started"),
		totalFlushLoopFinished:           factory.NewCounter("total_flush_loop_finished"),
		totalFlushLoopErr:                factory.NewCounter("total_flush_loop_err"),
		totalMergeLoopStarted:            factory.NewCounter("total_merge_loop_started"),
		totalMergeLoopFinished:           factory.NewCounter("total_merge_loop_finished"),
		totalMergeLoopErr:                factory.NewCounter("total_merge_loop_err"),
		totalSyncLoopStarted:             factory.NewCounter("total_sync_loop_started"),
		totalSyncLoopFinished:            factory.NewCounter("total_sync_loop_finished"),
		totalSyncLoopErr:                 factory.NewCounter("total_sync_loop_err"),
		totalSyncLoopLatency:             factory.NewCounter("total_sync_loop_latency"),
		totalSyncLoopBytes:               factory.NewCounter("total_sync_loop_bytes"),
		totalFlushLoopProgress:           factory.NewCounter("total_flush_loop_progress"),
		totalFlushed:                     factory.NewCounter("total_flushed"),
		totalFlushedMemParts:             factory.NewCounter("total_flushed_mem_parts"),
		totalFlushPauseCompleted:         factory.NewCounter("total_flush_pause_completed"),
		totalFlushPauseBreak:             factory.NewCounter("total_flush_pause_break"),
		totalFlushIntroLatency:           factory.NewCounter("total_flush_intro_latency"),
		totalFlushLatency:                factory.NewCounter("total_flush_latency"),
		totalMergedParts:                 factory.NewCounter("total_merged_parts", "type", "lane"),
		totalMergeLatency:                factory.NewCounter("total_merge_latency", "type", "lane"),
		totalMerged:                      factory.NewCounter("total_merged", "type", "lane"),
		totalMergeQueueLatency:           factory.NewCounter("total_merge_queue_latency", "type", "lane"),
		totalMergePartQuarantined:        factory.NewCounter("total_merge_part_quarantined"),
		totalMergeBackoffSeconds:         factory.NewCounter("total_merge_backoff_seconds"),
		totalMergePanicRecovered:         factory.NewCounter("total_merge_panic_recovered"),
		pipelineTracesEvaluated:          factory.NewCounter("pipeline_traces_evaluated"),
		pipelineTracesDropped:            factory.NewCounter("pipeline_traces_dropped"),
		pipelineTracesRetained:           factory.NewCounter("pipeline_traces_retained"),
		pipelineTracesImmature:           factory.NewCounter("pipeline_traces_immature"),
		pipelineOversizedTracesBypassed:  factory.NewCounter("pipeline_oversized_traces_bypassed"),
		pipelinePluginErrors:             factory.NewCounter("pipeline_plugin_errors", "reason"),
		pipelinePluginBatches:            factory.NewCounter("pipeline_plugin_batches", "result"),
		pipelinePluginExecutions:         factory.NewCounter("pipeline_plugin_executions", "plugin_name", "result"),
		pipelinePluginDurationSeconds:    factory.NewHistogram("pipeline_plugin_execution_duration_seconds", pipelinePluginDurationBuckets, "plugin_name", "result"),
		pipelinePluginBatchTraces:        factory.NewHistogram("pipeline_plugin_batch_traces", pipelinePluginBatchTraceBuckets, "result"),
		pipelinePluginLinkBypasses:       factory.NewCounter("pipeline_plugin_link_bypasses", "plugin_name", "reason"),
		pipelineAmbiguous:                factory.NewCounter("pipeline_ambiguous"),
		pipelineSidxPruned:               factory.NewCounter("pipeline_sidx_pruned"),
		pipelineGuardBloomProbes:         factory.NewCounter("pipeline_guard_bloom_probes"),
		pipelineGuardDeferred:            factory.NewCounter("pipeline_guard_deferred"),
		pipelineGuardBudgetExhausted:     factory.NewCounter("pipeline_guard_budget_exhausted"),
		pipelineGuardPublicationRejected: factory.NewCounter("pipeline_guard_publication_rejected"),
		pipelineGuardLosslessRetry:       factory.NewCounter("pipeline_guard_lossless_retry"),
		pipelineGuardBypassed:            factory.NewCounter("pipeline_guard_bypassed"),
		pipelineTracesRetainedByCeiling:  factory.NewCounter("pipeline_traces_retained_by_ceiling"),
		pipelineMergesCeilingReached:     factory.NewCounter("pipeline_merges_ceiling_reached", "lane"),
		pipelineDropSetBudgetBytes:       factory.NewGauge("pipeline_drop_set_budget_bytes"),
		pipelineDropSetEntries:           factory.NewHistogram("pipeline_drop_set_entries", dropSetEntryBuckets, "lane"),
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
			pendingDataCount:               factory.NewGauge("pending_data_count", common.ShardLabelNames()...),
			mergeQuarantinedParts:          factory.NewGauge("merge_quarantined_parts", common.ShardLabelNames()...),
		},
		indexMetrics: inverted.NewMetrics(factory, common.SegLabelNames()...),
	}
}

func (qs *queueSupplier) newMetrics(p common.Position) (storage.Metrics, observability.Factory) {
	factory := qs.omr.With(tbScope.ConstLabels(meter.ToLabelPairs(common.DBLabelNames(), p.DBLabelValues())))
	return &metrics{
		totalWritten:                     factory.NewCounter("total_written"),
		totalBatch:                       factory.NewCounter("total_batch"),
		totalBatchIntroLatency:           factory.NewCounter("total_batch_intro_time"),
		totalIntroduceLoopStarted:        factory.NewCounter("total_introduce_loop_started", "phase"),
		totalIntroduceLoopFinished:       factory.NewCounter("total_introduce_loop_finished", "phase"),
		totalFlushLoopStarted:            factory.NewCounter("total_flush_loop_started"),
		totalFlushLoopFinished:           factory.NewCounter("total_flush_loop_finished"),
		totalFlushLoopErr:                factory.NewCounter("total_flush_loop_err"),
		totalMergeLoopStarted:            factory.NewCounter("total_merge_loop_started"),
		totalMergeLoopFinished:           factory.NewCounter("total_merge_loop_finished"),
		totalMergeLoopErr:                factory.NewCounter("total_merge_loop_err"),
		totalSyncLoopStarted:             factory.NewCounter("total_sync_loop_started"),
		totalSyncLoopFinished:            factory.NewCounter("total_sync_loop_finished"),
		totalSyncLoopErr:                 factory.NewCounter("total_sync_loop_err"),
		totalSyncLoopLatency:             factory.NewCounter("total_sync_loop_latency"),
		totalSyncLoopBytes:               factory.NewCounter("total_sync_loop_bytes"),
		totalFlushLoopProgress:           factory.NewCounter("total_flush_loop_progress"),
		totalFlushed:                     factory.NewCounter("total_flushed"),
		totalFlushedMemParts:             factory.NewCounter("total_flushed_mem_parts"),
		totalFlushPauseCompleted:         factory.NewCounter("total_flush_pause_completed"),
		totalFlushPauseBreak:             factory.NewCounter("total_flush_pause_break"),
		totalFlushIntroLatency:           factory.NewCounter("total_flush_intro_latency"),
		totalFlushLatency:                factory.NewCounter("total_flush_latency"),
		totalMergedParts:                 factory.NewCounter("total_merged_parts", "type", "lane"),
		totalMergeLatency:                factory.NewCounter("total_merge_latency", "type", "lane"),
		totalMerged:                      factory.NewCounter("total_merged", "type", "lane"),
		totalMergeQueueLatency:           factory.NewCounter("total_merge_queue_latency", "type", "lane"),
		totalMergePartQuarantined:        factory.NewCounter("total_merge_part_quarantined"),
		totalMergeBackoffSeconds:         factory.NewCounter("total_merge_backoff_seconds"),
		totalMergePanicRecovered:         factory.NewCounter("total_merge_panic_recovered"),
		pipelineTracesEvaluated:          factory.NewCounter("pipeline_traces_evaluated"),
		pipelineTracesDropped:            factory.NewCounter("pipeline_traces_dropped"),
		pipelineTracesRetained:           factory.NewCounter("pipeline_traces_retained"),
		pipelineTracesImmature:           factory.NewCounter("pipeline_traces_immature"),
		pipelineOversizedTracesBypassed:  factory.NewCounter("pipeline_oversized_traces_bypassed"),
		pipelinePluginErrors:             factory.NewCounter("pipeline_plugin_errors", "reason"),
		pipelinePluginBatches:            factory.NewCounter("pipeline_plugin_batches", "result"),
		pipelinePluginExecutions:         factory.NewCounter("pipeline_plugin_executions", "plugin_name", "result"),
		pipelinePluginDurationSeconds:    factory.NewHistogram("pipeline_plugin_execution_duration_seconds", pipelinePluginDurationBuckets, "plugin_name", "result"),
		pipelinePluginBatchTraces:        factory.NewHistogram("pipeline_plugin_batch_traces", pipelinePluginBatchTraceBuckets, "result"),
		pipelinePluginLinkBypasses:       factory.NewCounter("pipeline_plugin_link_bypasses", "plugin_name", "reason"),
		pipelineAmbiguous:                factory.NewCounter("pipeline_ambiguous"),
		pipelineSidxPruned:               factory.NewCounter("pipeline_sidx_pruned"),
		pipelineGuardBloomProbes:         factory.NewCounter("pipeline_guard_bloom_probes"),
		pipelineGuardDeferred:            factory.NewCounter("pipeline_guard_deferred"),
		pipelineGuardBudgetExhausted:     factory.NewCounter("pipeline_guard_budget_exhausted"),
		pipelineGuardPublicationRejected: factory.NewCounter("pipeline_guard_publication_rejected"),
		pipelineGuardLosslessRetry:       factory.NewCounter("pipeline_guard_lossless_retry"),
		pipelineGuardBypassed:            factory.NewCounter("pipeline_guard_bypassed"),
		pipelineTracesRetainedByCeiling:  factory.NewCounter("pipeline_traces_retained_by_ceiling"),
		pipelineMergesCeilingReached:     factory.NewCounter("pipeline_merges_ceiling_reached", "lane"),
		pipelineDropSetBudgetBytes:       factory.NewGauge("pipeline_drop_set_budget_bytes"),
		pipelineDropSetEntries:           factory.NewHistogram("pipeline_drop_set_entries", dropSetEntryBuckets, "lane"),
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
			pendingDataCount:               factory.NewGauge("pending_data_count", common.ShardLabelNames()...),
			mergeQuarantinedParts:          factory.NewGauge("merge_quarantined_parts", common.ShardLabelNames()...),
		},
		indexMetrics: inverted.NewMetrics(factory, common.SegLabelNames()...),
	}, factory
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

	tst.quarantineMu.Lock()
	var quarantinedParts int
	for _, fails := range tst.quarantineFails {
		if fails >= quarantineThreshold {
			quarantinedParts++
		}
	}
	tst.quarantineMu.Unlock()
	metrics.mergeQuarantinedParts.Set(float64(quarantinedParts), tst.p.ShardLabelValues()...)
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
	tst.metrics.tbMetrics.pendingDataCount.Delete(tst.p.ShardLabelValues()...)
	tst.metrics.tbMetrics.mergeQuarantinedParts.Delete(tst.p.ShardLabelValues()...)
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

	pendingDataCount meter.Gauge

	mergeQuarantinedParts meter.Gauge
}
