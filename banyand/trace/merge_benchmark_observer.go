// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses this
// file to you under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package trace

import (
	"bufio"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/apache/skywalking-banyandb/banyand/internal/sidx"
	"github.com/apache/skywalking-banyandb/pkg/fs"
)

type mergeSamplingClassification string

const (
	mergeSamplingExecuted            mergeSamplingClassification = "executed"
	mergeSamplingEnabledNoEvaluation mergeSamplingClassification = "enabled_no_evaluation"
	mergeSamplingNotExecuted         mergeSamplingClassification = "not_executed"
)

type mergeSamplingReason string

type mergeStagingFlushReason string

const (
	mergeReasonPipelineDisabled mergeSamplingReason = "pipeline_disabled"
	mergeReasonNoSampler        mergeSamplingReason = "no_sampler"
	mergeReasonEventDisabled    mergeSamplingReason = "event_disabled"
	mergeReasonGrace            mergeSamplingReason = "merge_grace"
	mergeReasonGuardUnavailable mergeSamplingReason = "guard_unavailable"
	mergeReasonAllOversized     mergeSamplingReason = "all_traces_oversized"
	mergeReasonLosslessRetry    mergeSamplingReason = "lossless_retry"
	mergeReasonEmptyInput       mergeSamplingReason = "empty_input"
	mergeReasonOther            mergeSamplingReason = "other"
)

const (
	mergeStagingFlushEndOfMerge        mergeStagingFlushReason = "end_of_merge"
	mergeStagingFlushByteLimit         mergeStagingFlushReason = "byte_limit"
	mergeStagingFlushTraceLimit        mergeStagingFlushReason = "trace_limit"
	mergeStagingFlushByteAndTraceLimit mergeStagingFlushReason = "byte_and_trace_limit"
	mergeStagingFlushOversizedTrace    mergeStagingFlushReason = "oversized_trace"
)

type mergeBenchmarkPhase string

const (
	mergePhasePrimary  mergeBenchmarkPhase = "primary"
	mergePhaseDrain    mergeBenchmarkPhase = "drain"
	mergePhaseCooldown mergeBenchmarkPhase = "cooldown"
)

type (
	// BenchmarkMergeSampling classifies whether sampling was executed for a merge.
	BenchmarkMergeSampling = mergeSamplingClassification
	// BenchmarkMergeReason identifies a bounded reason sampling did not execute.
	BenchmarkMergeReason = mergeSamplingReason
	// BenchmarkMergePhase identifies the benchmark phase in which a merge began.
	BenchmarkMergePhase = mergeBenchmarkPhase
	// BenchmarkMergeStagingFlushReason identifies why a complete-trace staging batch was flushed.
	BenchmarkMergeStagingFlushReason = mergeStagingFlushReason
	// BenchmarkMergeChild records a secondary-index merge linked to its core merge.
	BenchmarkMergeChild = mergeBenchmarkChild
	// BenchmarkMergeResources records process resource deltas attributed to a merge.
	BenchmarkMergeResources = mergeBenchmarkResources
	// BenchmarkMergeStagingBatch records one complete-trace staging flush.
	BenchmarkMergeStagingBatch = mergeBenchmarkStagingBatch
	// BenchmarkMergePluginBatch aggregates one chain decision result.
	BenchmarkMergePluginBatch = mergeBenchmarkPluginBatch
	// BenchmarkMergePluginExecution aggregates one plugin/result pair.
	BenchmarkMergePluginExecution = mergeBenchmarkPluginExecution
	// BenchmarkMergeEvent records one core merge and its secondary-index children.
	BenchmarkMergeEvent = mergeBenchmarkEvent
	// BenchmarkMergeAggregate groups merge measurements by phase, sampling state, reason, and lane.
	BenchmarkMergeAggregate = mergeBenchmarkAggregate
	// BenchmarkMergeReport contains completed merge events and grouped aggregates.
	BenchmarkMergeReport = mergeBenchmarkSnapshot
)

const (
	// BenchmarkMergePhasePrimary is the benchmark's steady input phase.
	BenchmarkMergePhasePrimary BenchmarkMergePhase = mergePhasePrimary
	// BenchmarkMergePhaseDrain is the phase that drains outstanding merge work.
	BenchmarkMergePhaseDrain BenchmarkMergePhase = mergePhaseDrain
	// BenchmarkMergePhaseCooldown is the post-input observation phase.
	BenchmarkMergePhaseCooldown BenchmarkMergePhase = mergePhaseCooldown
	// BenchmarkMergeSamplingExecuted means at least one sampler evaluated traces.
	BenchmarkMergeSamplingExecuted BenchmarkMergeSampling = mergeSamplingExecuted
	// BenchmarkMergeSamplingEnabledNoEvaluation means sampling was enabled but no trace reached a sampler.
	BenchmarkMergeSamplingEnabledNoEvaluation BenchmarkMergeSampling = mergeSamplingEnabledNoEvaluation
	// BenchmarkMergeSamplingNotExecuted means sampling was ineligible for the merge.
	BenchmarkMergeSamplingNotExecuted BenchmarkMergeSampling = mergeSamplingNotExecuted
	// BenchmarkMergeStagingFlushEndOfMerge means the final complete-trace batch was flushed when input ended.
	BenchmarkMergeStagingFlushEndOfMerge BenchmarkMergeStagingFlushReason = mergeStagingFlushEndOfMerge
	// BenchmarkMergeStagingFlushByteLimit means the charged staging-byte limit triggered the flush.
	BenchmarkMergeStagingFlushByteLimit BenchmarkMergeStagingFlushReason = mergeStagingFlushByteLimit
	// BenchmarkMergeStagingFlushTraceLimit means the logical trace-count limit triggered the flush.
	BenchmarkMergeStagingFlushTraceLimit BenchmarkMergeStagingFlushReason = mergeStagingFlushTraceLimit
	// BenchmarkMergeStagingFlushByteAndTraceLimit means both staging limits were reached at the flush boundary.
	BenchmarkMergeStagingFlushByteAndTraceLimit BenchmarkMergeStagingFlushReason = mergeStagingFlushByteAndTraceLimit
	// BenchmarkMergeStagingFlushOversizedTrace means an oversized trace forced the preceding complete batch to flush.
	BenchmarkMergeStagingFlushOversizedTrace BenchmarkMergeStagingFlushReason = mergeStagingFlushOversizedTrace
	// BenchmarkMergeReasonPipelineDisabled means the native pipeline was disabled.
	BenchmarkMergeReasonPipelineDisabled BenchmarkMergeReason = mergeReasonPipelineDisabled
	// BenchmarkMergeReasonNoSampler means the group had no sampler.
	BenchmarkMergeReasonNoSampler BenchmarkMergeReason = mergeReasonNoSampler
	// BenchmarkMergeReasonEventDisabled means merge sampling was disabled by configuration.
	BenchmarkMergeReasonEventDisabled BenchmarkMergeReason = mergeReasonEventDisabled
	// BenchmarkMergeReasonGrace means the selected parts were inside merge grace.
	BenchmarkMergeReasonGrace BenchmarkMergeReason = mergeReasonGrace
	// BenchmarkMergeReasonGuardUnavailable means the fragment guard could not be created.
	BenchmarkMergeReasonGuardUnavailable BenchmarkMergeReason = mergeReasonGuardUnavailable
	// BenchmarkMergeReasonAllOversized means every trace bypassed evaluation due to its size.
	BenchmarkMergeReasonAllOversized BenchmarkMergeReason = mergeReasonAllOversized
	// BenchmarkMergeReasonLosslessRetry means guard revalidation forced a lossless retry.
	BenchmarkMergeReasonLosslessRetry BenchmarkMergeReason = mergeReasonLosslessRetry
	// BenchmarkMergeReasonEmptyInput means no input parts were supplied.
	BenchmarkMergeReasonEmptyInput BenchmarkMergeReason = mergeReasonEmptyInput
	// BenchmarkMergeReasonOther is the bounded fallback for an unclassified bypass.
	BenchmarkMergeReasonOther BenchmarkMergeReason = mergeReasonOther
)

// BenchmarkMergeRecordingOptions controls benchmark-only merge recording.
type BenchmarkMergeRecordingOptions struct {
	Writer      io.Writer
	Phase       BenchmarkMergePhase
	Attribution bool
}

type mergeBenchmarkObserverOptions struct {
	Phase       mergeBenchmarkPhase
	Attribution bool
}

type mergeBenchmarkChild struct {
	Sampling       mergeSamplingClassification `json:"sampling"`
	Reason         mergeSamplingReason         `json:"reason,omitempty"`
	Name           string                      `json:"name"`
	ParentSequence uint64                      `json:"parentSequence"`
	OutputPartID   uint64                      `json:"outputPartID"`
	InputBytes     uint64                      `json:"inputBytes"`
	OutputBytes    uint64                      `json:"outputBytes"`
	InputRows      uint64                      `json:"inputRows"`
	OutputRows     uint64                      `json:"outputRows"`
	ElapsedNanos   int64                       `json:"elapsedNanos"`
	Attempt        uint32                      `json:"attempt"`
	Published      bool                        `json:"published"`
}

type mergeBenchmarkResources struct {
	Error            string `json:"error,omitempty"`
	ReadBytes        uint64 `json:"readBytes"`
	CPUNanos         int64  `json:"cpuNanos"`
	AllocatedBytes   uint64 `json:"allocatedBytes"`
	Allocations      uint64 `json:"allocations"`
	WriteBytes       uint64 `json:"writeBytes"`
	PeakHeapBytes    uint64 `json:"peakHeapBytes"`
	EndHeapBytes     uint64 `json:"endHeapBytes"`
	PeakRSSBytes     uint64 `json:"peakRSSBytes"`
	EndRSSBytes      uint64 `json:"endRSSBytes"`
	ElapsedNanos     int64  `json:"elapsedNanos"`
	CrossedPhase     bool   `json:"crossedPhase"`
	AttributionValid bool   `json:"attributionValid"`
	Overlapped       bool   `json:"overlapped"`
}

type mergeBenchmarkStagingBatch struct {
	Reason mergeStagingFlushReason `json:"reason"`
	Bytes  uint64                  `json:"bytes"`
	Traces uint64                  `json:"traces"`
}

type mergeBenchmarkDurationBucket struct {
	UpperBoundNanos int64  `json:"upperBoundNanos"`
	Calls           uint64 `json:"calls"`
}

type mergeBenchmarkPluginExecution struct {
	Phase            mergeBenchmarkPhase            `json:"phase"`
	PluginName       string                         `json:"pluginName"`
	Result           string                         `json:"result"`
	BypassReason     string                         `json:"bypassReason,omitempty"`
	DurationBuckets  []mergeBenchmarkDurationBucket `json:"durationBuckets,omitempty"`
	Calls            uint64                         `json:"calls"`
	DurationOverflow uint64                         `json:"durationOverflow,omitempty"`
	ElapsedNanos     int64                          `json:"elapsedNanos"`
	MaxElapsedNanos  int64                          `json:"maxElapsedNanos"`
}

type mergeBenchmarkPluginBatch struct {
	Phase   mergeBenchmarkPhase `json:"phase"`
	Result  string              `json:"result"`
	Batches uint64              `json:"batches"`
	Traces  uint64              `json:"traces"`
}

type mergeBenchmarkPluginExecutionKey struct {
	pluginName   string
	result       string
	bypassReason string
}

type mergeBenchmarkEvent struct {
	Error                 string                          `json:"error,omitempty"`
	Reason                mergeSamplingReason             `json:"reason,omitempty"`
	InitialReason         mergeSamplingReason             `json:"initialReason,omitempty"`
	Phase                 mergeBenchmarkPhase             `json:"phase"`
	Type                  string                          `json:"type"`
	Lane                  string                          `json:"lane"`
	Sampling              mergeSamplingClassification     `json:"sampling"`
	RecordingError        string                          `json:"recordingError,omitempty"`
	SelectionSHA256       string                          `json:"selectionSHA256"`
	InputPartIDs          []uint64                        `json:"inputPartIDs"`
	Children              []mergeBenchmarkChild           `json:"children,omitempty"`
	StagingBatches        []mergeBenchmarkStagingBatch    `json:"stagingBatches,omitempty"`
	PluginBatches         []mergeBenchmarkPluginBatch     `json:"pluginBatches,omitempty"`
	PluginExecutions      []mergeBenchmarkPluginExecution `json:"pluginExecutions,omitempty"`
	GuardDeferred         map[string]uint64               `json:"guardDeferred,omitempty"`
	Resources             mergeBenchmarkResources         `json:"resources"`
	CoreElapsedNanos      int64                           `json:"coreElapsedNanos"`
	OutputRows            uint64                          `json:"outputRows"`
	TracesEvaluated       uint64                          `json:"tracesEvaluated"`
	InputBytes            uint64                          `json:"inputBytes"`
	OutputBytes           uint64                          `json:"outputBytes"`
	InputRows             uint64                          `json:"inputRows"`
	Sequence              uint64                          `json:"sequence"`
	HotInputRows          uint64                          `json:"hotInputRows"`
	MatureInputRows       uint64                          `json:"matureInputRows"`
	MinTimestamp          int64                           `json:"minTimestamp"`
	MaxTimestamp          int64                           `json:"maxTimestamp"`
	LogicalNow            int64                           `json:"logicalNow"`
	MaturityFrontier      int64                           `json:"maturityFrontier"`
	PluginCalls           uint64                          `json:"pluginCalls"`
	OutputPartID          uint64                          `json:"outputPartID,omitempty"`
	TracesRetained        uint64                          `json:"tracesRetained"`
	TracesDropped         uint64                          `json:"tracesDropped"`
	OversizedTraces       uint64                          `json:"oversizedTraces"`
	EstimatedStagingBytes uint64                          `json:"estimatedStagingBytes"`
	StagingHardLimit      uint64                          `json:"stagingHardLimit"`
	DecisionBatchLimit    uint64                          `json:"decisionBatchLimit"`
	PlannedStagingBatches uint64                          `json:"plannedStagingBatches"`
	ChargedStagingBytes   uint64                          `json:"chargedStagingBytes"`
	PeakStagedBytes       uint64                          `json:"peakStagedBytes"`
	// DropSetBudget is the resolved drop-set ceiling this merge's filter carried,
	// from resolveDropSetBudget. Zero means unlimited, which a production merge no
	// longer reports: a zero here indicates a filter built without a budget.
	DropSetBudget uint64 `json:"dropSetBudget"`
	// TracesRetainedByCeiling is the number of traces this merge retained because
	// the drop-set ceiling was reached. These traces are a subset of
	// TracesRetained, not a separate category: a ceiling retention increments both,
	// which is what keeps retained+dropped == evaluated true for consumers of that
	// invariant. Subtract this field from TracesRetained for the verdict-only count.
	TracesRetainedByCeiling uint64 `json:"tracesRetainedByCeiling"`
	QueueNanos              int64  `json:"queueNanos"`
	HotInputParts           uint32 `json:"hotInputParts"`
	MatureInputParts        uint32 `json:"matureInputParts"`
	InputMinDepth           uint32 `json:"inputMinDepth"`
	InputMaxDepth           uint32 `json:"inputMaxDepth"`
	OutputDepth             uint32 `json:"outputDepth"`
	Version                 uint32 `json:"version"`
	DecisionMaxTraceCount   int    `json:"decisionMaxTraceCount"`
	LosslessRetry           bool   `json:"losslessRetry"`
	// DropSetCapped is true when this merge's drop-set ceiling was reached at
	// least once (equivalent to TracesRetainedByCeiling > 0).
	DropSetCapped bool `json:"dropSetCapped"`
}

type mergeBenchmarkAggregate struct {
	Sampling          mergeSamplingClassification `json:"sampling"`
	Reason            mergeSamplingReason         `json:"reason,omitempty"`
	Phase             mergeBenchmarkPhase         `json:"phase"`
	Lane              string                      `json:"lane"`
	Merges            uint64                      `json:"merges"`
	InputBytes        uint64                      `json:"inputBytes"`
	OutputBytes       uint64                      `json:"outputBytes"`
	InputRows         uint64                      `json:"inputRows"`
	OutputRows        uint64                      `json:"outputRows"`
	ChildMerges       uint64                      `json:"childMerges"`
	ChildInputBytes   uint64                      `json:"childInputBytes"`
	ChildOutputBytes  uint64                      `json:"childOutputBytes"`
	ChildInputRows    uint64                      `json:"childInputRows"`
	ChildOutputRows   uint64                      `json:"childOutputRows"`
	ChildElapsedNanos int64                       `json:"childElapsedNanos"`
	PluginCalls       uint64                      `json:"pluginCalls"`
	TracesEvaluated   uint64                      `json:"tracesEvaluated"`
	TracesRetained    uint64                      `json:"tracesRetained"`
	TracesDropped     uint64                      `json:"tracesDropped"`
	OversizedTraces   uint64                      `json:"oversizedTraces"`
	ElapsedNanos      int64                       `json:"elapsedNanos"`
}

type mergeBenchmarkSnapshot struct {
	Error                     string                          `json:"error,omitempty"`
	Events                    []mergeBenchmarkEvent           `json:"events"`
	Aggregates                []mergeBenchmarkAggregate       `json:"aggregates"`
	PluginExecutions          []mergeBenchmarkPluginExecution `json:"pluginExecutions,omitempty"`
	PluginBatches             []mergeBenchmarkPluginBatch     `json:"pluginBatches,omitempty"`
	PeakConcurrentStagedBytes uint64                          `json:"peakConcurrentStagedBytes"`
}

type mergeBenchmarkAggregateKey struct {
	sampling mergeSamplingClassification
	reason   mergeSamplingReason
	phase    mergeBenchmarkPhase
	lane     string
}

type mergeBenchmarkObserver struct {
	writer                    io.Writer
	recordErr                 error
	aggregates                map[mergeBenchmarkAggregateKey]*mergeBenchmarkAggregate
	phase                     mergeBenchmarkPhase
	events                    []mergeBenchmarkEvent
	sequence                  atomic.Uint64
	active                    atomic.Int64
	currentStagedBytes        atomic.Int64
	peakConcurrentStagedBytes atomic.Uint64
	overlapGen                atomic.Uint64
	mu                        sync.Mutex
	attribution               bool
}

func newMergeBenchmarkObserver(writer io.Writer, options mergeBenchmarkObserverOptions) *mergeBenchmarkObserver {
	phase := options.Phase
	if phase == "" {
		phase = mergePhasePrimary
	}
	return &mergeBenchmarkObserver{
		writer: writer, phase: phase, attribution: options.Attribution,
		aggregates: make(map[mergeBenchmarkAggregateKey]*mergeBenchmarkAggregate),
	}
}

func validMergeBenchmarkPhase(phase mergeBenchmarkPhase) bool {
	return phase == mergePhasePrimary || phase == mergePhaseDrain || phase == mergePhaseCooldown
}

func (tst *tsTable) setMergeBenchmarkObserver(observer *mergeBenchmarkObserver) bool {
	tst.benchmarkMu.Lock()
	defer tst.benchmarkMu.Unlock()
	if tst.mergeBenchmark.Load() != nil {
		return false
	}
	tst.mergeAttribution.Store(observer != nil && observer.attribution)
	tst.mergeBenchmark.Store(observer)
	return true
}

// EnableMergeRecording installs benchmark-only per-merge recording on this receiver.
func (bpr *BenchmarkPartReceiver) EnableMergeRecording(options BenchmarkMergeRecordingOptions) error {
	if bpr == nil || bpr.table == nil {
		return fmt.Errorf("benchmark receiver is not open")
	}
	if options.Phase != "" && !validMergeBenchmarkPhase(options.Phase) {
		return fmt.Errorf("invalid merge recording phase %q", options.Phase)
	}
	installed := bpr.table.setMergeBenchmarkObserver(newMergeBenchmarkObserver(options.Writer, mergeBenchmarkObserverOptions{
		Phase: options.Phase, Attribution: options.Attribution,
	}))
	if !installed {
		return fmt.Errorf("merge recording is already enabled")
	}
	return nil
}

// SetMergeRecordingPhase changes the bounded phase assigned to subsequently started merges.
func (bpr *BenchmarkPartReceiver) SetMergeRecordingPhase(phase BenchmarkMergePhase) error {
	if bpr == nil || bpr.table == nil {
		return fmt.Errorf("benchmark receiver is not open")
	}
	observer := bpr.table.mergeBenchmark.Load()
	if observer == nil {
		return fmt.Errorf("merge recording is not enabled")
	}
	if !validMergeBenchmarkPhase(phase) {
		return fmt.Errorf("invalid merge recording phase %q", phase)
	}
	observer.setPhase(phase)
	return nil
}

// MergeRecordingReport returns a consistent completed-event and aggregate snapshot.
func (bpr *BenchmarkPartReceiver) MergeRecordingReport() (BenchmarkMergeReport, error) {
	if bpr == nil || bpr.table == nil {
		return BenchmarkMergeReport{}, fmt.Errorf("benchmark receiver is not open")
	}
	observer := bpr.table.mergeBenchmark.Load()
	if observer == nil {
		return BenchmarkMergeReport{}, fmt.Errorf("merge recording is not enabled")
	}
	report := observer.snapshot()
	if report.Error != "" {
		return report, fmt.Errorf("merge benchmark recorder failed: %s", report.Error)
	}
	return report, nil
}

func (mbo *mergeBenchmarkObserver) setPhase(phase mergeBenchmarkPhase) {
	if mbo == nil || phase == "" {
		return
	}
	mbo.mu.Lock()
	mbo.phase = phase
	mbo.mu.Unlock()
}

func (mbo *mergeBenchmarkObserver) snapshot() mergeBenchmarkSnapshot {
	if mbo == nil {
		return mergeBenchmarkSnapshot{}
	}
	mbo.mu.Lock()
	defer mbo.mu.Unlock()
	snapshot := mergeBenchmarkSnapshot{
		Events:                    append([]mergeBenchmarkEvent(nil), mbo.events...),
		PeakConcurrentStagedBytes: mbo.peakConcurrentStagedBytes.Load(),
	}
	if mbo.recordErr != nil {
		snapshot.Error = mbo.recordErr.Error()
	}
	snapshot.Aggregates = make([]mergeBenchmarkAggregate, 0, len(mbo.aggregates))
	for _, aggregate := range mbo.aggregates {
		snapshot.Aggregates = append(snapshot.Aggregates, *aggregate)
	}
	sort.Slice(snapshot.Aggregates, func(leftIdx, rightIdx int) bool {
		left := snapshot.Aggregates[leftIdx]
		right := snapshot.Aggregates[rightIdx]
		leftKey := string(left.Phase) + "\x00" + string(left.Sampling) + "\x00" + string(left.Reason) + "\x00" + left.Lane
		rightKey := string(right.Phase) + "\x00" + string(right.Sampling) + "\x00" + string(right.Reason) + "\x00" + right.Lane
		return leftKey < rightKey
	})
	snapshot.PluginExecutions = aggregatePluginExecutions(snapshot.Events)
	snapshot.PluginBatches = aggregatePluginBatches(snapshot.Events)
	return snapshot
}

func aggregatePluginBatches(events []mergeBenchmarkEvent) []mergeBenchmarkPluginBatch {
	aggregated := make(map[string]*mergeBenchmarkPluginBatch)
	for eventIdx := range events {
		for batchIdx := range events[eventIdx].PluginBatches {
			batch := events[eventIdx].PluginBatches[batchIdx]
			key := string(batch.Phase) + "\x00" + batch.Result
			current := aggregated[key]
			if current == nil {
				copied := batch
				aggregated[key] = &copied
				continue
			}
			current.Batches += batch.Batches
			current.Traces += batch.Traces
		}
	}
	result := make([]mergeBenchmarkPluginBatch, 0, len(aggregated))
	for _, batch := range aggregated {
		result = append(result, *batch)
	}
	sort.Slice(result, func(leftIdx, rightIdx int) bool {
		left := result[leftIdx]
		right := result[rightIdx]
		return string(left.Phase)+"\x00"+left.Result < string(right.Phase)+"\x00"+right.Result
	})
	return result
}

func sortPluginExecutions(executions []mergeBenchmarkPluginExecution) {
	sort.Slice(executions, func(leftIdx, rightIdx int) bool {
		left := executions[leftIdx]
		right := executions[rightIdx]
		leftKey := string(left.Phase) + "\x00" + left.PluginName + "\x00" + left.Result + "\x00" + left.BypassReason
		rightKey := string(right.Phase) + "\x00" + right.PluginName + "\x00" + right.Result + "\x00" + right.BypassReason
		return leftKey < rightKey
	})
}

func aggregatePluginExecutions(events []mergeBenchmarkEvent) []mergeBenchmarkPluginExecution {
	aggregated := make(map[string]*mergeBenchmarkPluginExecution)
	for eventIdx := range events {
		for executionIdx := range events[eventIdx].PluginExecutions {
			execution := events[eventIdx].PluginExecutions[executionIdx]
			key := string(execution.Phase) + "\x00" + execution.PluginName + "\x00" + execution.Result + "\x00" + execution.BypassReason
			current := aggregated[key]
			if current == nil {
				copied := execution
				copied.DurationBuckets = append([]mergeBenchmarkDurationBucket(nil), execution.DurationBuckets...)
				aggregated[key] = &copied
				continue
			}
			current.Calls += execution.Calls
			current.DurationOverflow += execution.DurationOverflow
			current.ElapsedNanos += execution.ElapsedNanos
			current.MaxElapsedNanos = max(current.MaxElapsedNanos, execution.MaxElapsedNanos)
			mergePluginDurationBuckets(current, execution.DurationBuckets)
		}
	}
	result := make([]mergeBenchmarkPluginExecution, 0, len(aggregated))
	for _, execution := range aggregated {
		result = append(result, *execution)
	}
	sortPluginExecutions(result)
	return result
}

func newPluginDurationBuckets() []mergeBenchmarkDurationBucket {
	buckets := make([]mergeBenchmarkDurationBucket, len(pipelinePluginDurationBuckets))
	for bucketIdx, upperBoundSeconds := range pipelinePluginDurationBuckets {
		buckets[bucketIdx].UpperBoundNanos = int64(upperBoundSeconds * float64(time.Second))
	}
	return buckets
}

func recordPluginDuration(execution *mergeBenchmarkPluginExecution, elapsedNanos int64) {
	if len(execution.DurationBuckets) == 0 {
		execution.DurationBuckets = newPluginDurationBuckets()
	}
	for bucketIdx := range execution.DurationBuckets {
		if elapsedNanos <= execution.DurationBuckets[bucketIdx].UpperBoundNanos {
			execution.DurationBuckets[bucketIdx].Calls++
			return
		}
	}
	execution.DurationOverflow++
}

func mergePluginDurationBuckets(execution *mergeBenchmarkPluginExecution, incoming []mergeBenchmarkDurationBucket) {
	if len(incoming) == 0 {
		return
	}
	if len(execution.DurationBuckets) == 0 {
		execution.DurationBuckets = append([]mergeBenchmarkDurationBucket(nil), incoming...)
		return
	}
	for incomingIdx := range incoming {
		for currentIdx := range execution.DurationBuckets {
			if execution.DurationBuckets[currentIdx].UpperBoundNanos == incoming[incomingIdx].UpperBoundNanos {
				execution.DurationBuckets[currentIdx].Calls += incoming[incomingIdx].Calls
				break
			}
		}
	}
}

type mergeEvaluationObservation struct {
	observer           *mergeBenchmarkObserver
	guardDeferred      map[string]uint64
	pluginExecutions   map[mergeBenchmarkPluginExecutionKey]*mergeBenchmarkPluginExecution
	pluginBatches      map[string]*mergeBenchmarkPluginBatch
	stagingBatches     []mergeBenchmarkStagingBatch
	evaluated          atomic.Uint64
	pluginCalls        atomic.Uint64
	retained           atomic.Uint64
	dropped            atomic.Uint64
	oversized          atomic.Uint64
	currentStagedBytes atomic.Uint64
	peakStagedBytes    atomic.Uint64
	mu                 sync.Mutex
}

func (meo *mergeEvaluationObservation) recordPluginBatch(observation pluginExecutionObservation) {
	if meo == nil {
		return
	}
	meo.mu.Lock()
	if meo.pluginBatches == nil {
		meo.pluginBatches = make(map[string]*mergeBenchmarkPluginBatch)
	}
	batch := meo.pluginBatches[observation.result]
	if batch == nil {
		batch = &mergeBenchmarkPluginBatch{Result: observation.result}
		meo.pluginBatches[observation.result] = batch
	}
	batch.Batches++
	batch.Traces += uint64(observation.batchTraces)
	meo.mu.Unlock()
}

func (meo *mergeEvaluationObservation) pluginBatchSnapshot(phase mergeBenchmarkPhase) []mergeBenchmarkPluginBatch {
	if meo == nil {
		return nil
	}
	meo.mu.Lock()
	defer meo.mu.Unlock()
	batches := make([]mergeBenchmarkPluginBatch, 0, len(meo.pluginBatches))
	for _, batch := range meo.pluginBatches {
		copied := *batch
		copied.Phase = phase
		batches = append(batches, copied)
	}
	sort.Slice(batches, func(leftIdx, rightIdx int) bool { return batches[leftIdx].Result < batches[rightIdx].Result })
	return batches
}

func (meo *mergeEvaluationObservation) recordPluginExecution(observation pluginLinkExecutionObservation) {
	if meo == nil {
		return
	}
	key := mergeBenchmarkPluginExecutionKey{
		pluginName: observation.pluginName, result: observation.result, bypassReason: observation.bypassReason,
	}
	meo.mu.Lock()
	if meo.pluginExecutions == nil {
		meo.pluginExecutions = make(map[mergeBenchmarkPluginExecutionKey]*mergeBenchmarkPluginExecution)
	}
	execution := meo.pluginExecutions[key]
	if execution == nil {
		execution = &mergeBenchmarkPluginExecution{
			PluginName: observation.pluginName, Result: observation.result, BypassReason: observation.bypassReason,
		}
		meo.pluginExecutions[key] = execution
	}
	elapsedNanos := observation.elapsed.Nanoseconds()
	execution.Calls++
	execution.ElapsedNanos += elapsedNanos
	execution.MaxElapsedNanos = max(execution.MaxElapsedNanos, elapsedNanos)
	recordPluginDuration(execution, elapsedNanos)
	meo.mu.Unlock()
}

func (meo *mergeEvaluationObservation) pluginExecutionSnapshot(phase mergeBenchmarkPhase) []mergeBenchmarkPluginExecution {
	if meo == nil {
		return nil
	}
	meo.mu.Lock()
	defer meo.mu.Unlock()
	executions := make([]mergeBenchmarkPluginExecution, 0, len(meo.pluginExecutions))
	for _, execution := range meo.pluginExecutions {
		copied := *execution
		copied.Phase = phase
		copied.DurationBuckets = append([]mergeBenchmarkDurationBucket(nil), execution.DurationBuckets...)
		executions = append(executions, copied)
	}
	sortPluginExecutions(executions)
	return executions
}

func (meo *mergeEvaluationObservation) observeStagedBytes(value uint64) {
	if meo == nil {
		return
	}
	previous := meo.currentStagedBytes.Swap(value)
	updateAtomicMaximum(&meo.peakStagedBytes, value)
	if meo.observer == nil || previous == value {
		return
	}
	var concurrent int64
	if value > previous {
		concurrent = meo.observer.currentStagedBytes.Add(int64(value - previous))
	} else {
		concurrent = meo.observer.currentStagedBytes.Add(-int64(previous - value))
	}
	if concurrent > 0 {
		updateAtomicMaximum(&meo.observer.peakConcurrentStagedBytes, uint64(concurrent))
	}
}

func (meo *mergeEvaluationObservation) recordStagingBatch(reason mergeStagingFlushReason, bytes, traces uint64) {
	if meo == nil || traces == 0 {
		return
	}
	meo.mu.Lock()
	meo.stagingBatches = append(meo.stagingBatches, mergeBenchmarkStagingBatch{Reason: reason, Bytes: bytes, Traces: traces})
	meo.mu.Unlock()
}

func (meo *mergeEvaluationObservation) stagingSnapshot() []mergeBenchmarkStagingBatch {
	if meo == nil {
		return nil
	}
	meo.mu.Lock()
	defer meo.mu.Unlock()
	return append([]mergeBenchmarkStagingBatch(nil), meo.stagingBatches...)
}

func (meo *mergeEvaluationObservation) recordGuardDeferred(reason traceFragmentGuardReason) {
	if meo == nil {
		return
	}
	meo.mu.Lock()
	if meo.guardDeferred == nil {
		meo.guardDeferred = make(map[string]uint64)
	}
	meo.guardDeferred[string(reason)]++
	meo.mu.Unlock()
}

func (meo *mergeEvaluationObservation) guardDeferredSnapshot() map[string]uint64 {
	if meo == nil {
		return nil
	}
	meo.mu.Lock()
	defer meo.mu.Unlock()
	return maps.Clone(meo.guardDeferred)
}

type mergeBenchmarkOperation struct {
	startedAt      time.Time
	recordErr      error
	observer       *mergeBenchmarkObserver
	evaluation     *mergeEvaluationObservation
	monitor        *mergeResourceMonitor
	initialReason  mergeSamplingReason
	children       []mergeBenchmarkChild
	event          mergeBenchmarkEvent
	overlapGen     uint64
	startedOverlap bool
}

type mergeBenchmarkSeed struct {
	dispatched time.Time
	observer   *mergeBenchmarkObserver
	event      mergeBenchmarkEvent
}

func (mbo *mergeBenchmarkOperation) recordChild(child mergeBenchmarkChild) {
	if mbo == nil {
		return
	}
	child.ParentSequence = mbo.event.Sequence
	mbo.children = append(mbo.children, child)
}

func (mbo *mergeBenchmarkOperation) setInitialReason(reason mergeSamplingReason) {
	if mbo == nil {
		return
	}
	mbo.initialReason = reason
	mbo.event.InitialReason = reason
}

func (mbo *mergeBenchmarkOperation) publishAttempt(attempt uint32) {
	if mbo == nil {
		return
	}
	for childIdx := range mbo.children {
		if mbo.children[childIdx].Attempt == attempt {
			mbo.children[childIdx].Published = true
		}
	}
}

func (mbo *mergeBenchmarkOperation) recordFailure(recordErr error) {
	if mbo != nil && mbo.recordErr == nil && recordErr != nil {
		mbo.recordErr = recordErr
	}
}

func benchmarkSidxPartTotals(fileSystem fs.FileSystem, paths map[uint64]string) (uint64, uint64, error) {
	var bytes, rows uint64
	for _, partPath := range paths {
		metadata, metadataErr := sidx.ParsePartMetadata(fileSystem, partPath)
		if metadataErr != nil {
			return 0, 0, fmt.Errorf("cannot read secondary-index part metadata %s: %w", partPath, metadataErr)
		}
		bytes += metadata.CompressedSizeBytes
		rows += metadata.TotalCount
	}
	return bytes, rows, nil
}

func (mbo *mergeBenchmarkObserver) seed(tst *tsTable, parts []*partWrapper, typ, lane string) *mergeBenchmarkSeed {
	if mbo == nil {
		return nil
	}
	mbo.mu.Lock()
	phase := mbo.phase
	mbo.mu.Unlock()
	event := buildMergeBenchmarkEvent(tst, parts, typ, lane, phase)
	event.Sequence = mbo.sequence.Add(1)
	seed := &mergeBenchmarkSeed{
		observer: mbo, dispatched: time.Now(), event: event,
	}
	return seed
}

func buildMergeBenchmarkEvent(tst *tsTable, parts []*partWrapper, typ, lane string, phase mergeBenchmarkPhase) mergeBenchmarkEvent {
	event := mergeBenchmarkEvent{
		Version: 1, Phase: phase, Type: typ, Lane: lane, InputPartIDs: make([]uint64, 0, len(parts)),
	}
	logicalNow := tst.mergeNow().UnixNano()
	event.LogicalNow = logicalNow
	event.MaturityFrontier = traceFragmentSaturatingSub(logicalNow, tst.effectiveMergeGraceNs())
	selectionDigest := sha256.New()
	for partIdx, partData := range parts {
		metadata := &partData.p.partMetadata
		event.InputPartIDs = append(event.InputPartIDs, partData.ID())
		event.InputBytes += metadata.CompressedSizeBytes
		event.InputRows += metadata.TotalCount
		if metadata.MaxTimestamp <= event.MaturityFrontier {
			event.MatureInputParts++
			event.MatureInputRows += metadata.TotalCount
		} else {
			event.HotInputParts++
			event.HotInputRows += metadata.TotalCount
		}
		if partIdx == 0 || metadata.MinTimestamp < event.MinTimestamp {
			event.MinTimestamp = metadata.MinTimestamp
		}
		if partIdx == 0 || metadata.MaxTimestamp > event.MaxTimestamp {
			event.MaxTimestamp = metadata.MaxTimestamp
		}
		if partIdx == 0 || partData.mergeDepth < event.InputMinDepth {
			event.InputMinDepth = partData.mergeDepth
		}
		if partData.mergeDepth > event.InputMaxDepth {
			event.InputMaxDepth = partData.mergeDepth
		}
		var encodedID [8]byte
		binary.BigEndian.PutUint64(encodedID[:], partData.ID())
		_, _ = selectionDigest.Write(encodedID[:])
	}
	event.OutputDepth = event.InputMaxDepth + 1
	event.SelectionSHA256 = fmt.Sprintf("%x", selectionDigest.Sum(nil))
	return event
}

func (mbo *mergeBenchmarkObserver) beginSeed(seed *mergeBenchmarkSeed, reason mergeSamplingReason) *mergeBenchmarkOperation {
	if mbo == nil || seed == nil {
		return nil
	}
	startedAt := time.Now()
	operation := &mergeBenchmarkOperation{
		observer: mbo, evaluation: &mergeEvaluationObservation{observer: mbo}, initialReason: reason, startedAt: startedAt, event: seed.event,
	}
	operation.event.InitialReason = reason
	operation.event.QueueNanos = startedAt.Sub(seed.dispatched).Nanoseconds()
	active := mbo.active.Add(1)
	operation.overlapGen = mbo.overlapGen.Load()
	if active > 1 {
		operation.startedOverlap = true
		operation.overlapGen = mbo.overlapGen.Add(1)
	}
	operation.monitor = startMergeResourceMonitor(mbo.attribution)
	return operation
}

func (mbo *mergeBenchmarkObserver) finish(operation *mergeBenchmarkOperation, output *partWrapper, resultErr error, losslessRetry bool) {
	if mbo == nil || operation == nil {
		return
	}
	event := operation.event
	event.Children = append([]mergeBenchmarkChild(nil), operation.children...)
	event.PluginCalls = operation.evaluation.pluginCalls.Load()
	event.TracesEvaluated = operation.evaluation.evaluated.Load()
	event.TracesRetained = operation.evaluation.retained.Load()
	event.TracesDropped = operation.evaluation.dropped.Load()
	event.OversizedTraces = operation.evaluation.oversized.Load()
	event.PeakStagedBytes = operation.evaluation.peakStagedBytes.Load()
	event.StagingBatches = operation.evaluation.stagingSnapshot()
	event.PluginBatches = operation.evaluation.pluginBatchSnapshot(event.Phase)
	event.PluginExecutions = operation.evaluation.pluginExecutionSnapshot(event.Phase)
	event.GuardDeferred = operation.evaluation.guardDeferredSnapshot()
	for batchIdx := range event.StagingBatches {
		event.ChargedStagingBytes = saturatingAddUint64(event.ChargedStagingBytes, event.StagingBatches[batchIdx].Bytes)
	}
	operation.evaluation.observeStagedBytes(0)
	event.LosslessRetry = losslessRetry
	overlapped := operation.startedOverlap || mbo.active.Load() > 1 || mbo.overlapGen.Load() != operation.overlapGen
	mbo.active.Add(-1)
	mbo.mu.Lock()
	crossedPhase := mbo.phase != event.Phase
	mbo.mu.Unlock()
	event.Resources = operation.monitor.stop(time.Since(operation.startedAt))
	event.Resources.Overlapped = overlapped
	event.Resources.CrossedPhase = crossedPhase
	event.Resources.AttributionValid = mbo.attribution && !overlapped && !crossedPhase && event.Resources.Error == ""
	if output != nil {
		event.OutputPartID = output.ID()
		event.OutputBytes = output.p.partMetadata.CompressedSizeBytes
		event.OutputRows = output.p.partMetadata.TotalCount
	}
	if operation.recordErr != nil {
		event.RecordingError = operation.recordErr.Error()
	}
	if resultErr != nil {
		event.Error = resultErr.Error()
	}
	event.Sampling, event.Reason = classifyMergeObservation(operation.initialReason, event.PluginCalls, event.TracesEvaluated,
		event.OversizedTraces, losslessRetry)
	for childIdx := range event.Children {
		event.Children[childIdx].Sampling = event.Sampling
		event.Children[childIdx].Reason = event.Reason
		event.Children[childIdx].OutputPartID = event.OutputPartID
	}
	mbo.record(event)
}

func classifyMergeObservation(initialReason mergeSamplingReason, pluginCalls, evaluated, oversized uint64,
	losslessRetry bool,
) (mergeSamplingClassification, mergeSamplingReason) {
	switch {
	case losslessRetry:
		return mergeSamplingNotExecuted, mergeReasonLosslessRetry
	case pluginCalls > 0 && evaluated > 0:
		return mergeSamplingExecuted, ""
	case initialReason != "":
		return mergeSamplingNotExecuted, initialReason
	case oversized > 0:
		return mergeSamplingEnabledNoEvaluation, mergeReasonAllOversized
	default:
		return mergeSamplingEnabledNoEvaluation, mergeReasonOther
	}
}

func (mbo *mergeBenchmarkObserver) record(event mergeBenchmarkEvent) {
	mbo.mu.Lock()
	defer mbo.mu.Unlock()
	mbo.events = append(mbo.events, event)
	if event.RecordingError != "" && mbo.recordErr == nil {
		mbo.recordErr = fmt.Errorf("merge benchmark event %d: %s", event.Sequence, event.RecordingError)
	}
	key := mergeBenchmarkAggregateKey{sampling: event.Sampling, reason: event.Reason, phase: event.Phase, lane: event.Lane}
	aggregate := mbo.aggregates[key]
	if aggregate == nil {
		aggregate = &mergeBenchmarkAggregate{Sampling: event.Sampling, Reason: event.Reason, Phase: event.Phase, Lane: event.Lane}
		mbo.aggregates[key] = aggregate
	}
	aggregate.Merges++
	aggregate.InputBytes += event.InputBytes
	aggregate.OutputBytes += event.OutputBytes
	aggregate.InputRows += event.InputRows
	aggregate.OutputRows += event.OutputRows
	for childIdx := range event.Children {
		child := &event.Children[childIdx]
		aggregate.ChildMerges++
		aggregate.ChildInputBytes += child.InputBytes
		aggregate.ChildOutputBytes += child.OutputBytes
		aggregate.ChildInputRows += child.InputRows
		aggregate.ChildOutputRows += child.OutputRows
		aggregate.ChildElapsedNanos += child.ElapsedNanos
	}
	aggregate.PluginCalls += event.PluginCalls
	aggregate.TracesEvaluated += event.TracesEvaluated
	aggregate.TracesRetained += event.TracesRetained
	aggregate.TracesDropped += event.TracesDropped
	aggregate.OversizedTraces += event.OversizedTraces
	aggregate.ElapsedNanos += event.Resources.ElapsedNanos
	if mbo.writer != nil {
		encoded, marshalErr := json.Marshal(event)
		if marshalErr != nil {
			mbo.recordErr = fmt.Errorf("cannot encode merge benchmark event %d: %w", event.Sequence, marshalErr)
			return
		}
		encoded = append(encoded, '\n')
		written, writeErr := mbo.writer.Write(encoded)
		if writeErr != nil {
			mbo.recordErr = fmt.Errorf("cannot write merge benchmark event %d: %w", event.Sequence, writeErr)
			return
		}
		if written != len(encoded) {
			mbo.recordErr = fmt.Errorf("short write for merge benchmark event %d: got %d, want %d", event.Sequence, written, len(encoded))
		}
	}
}

type mergeProcessResources struct {
	cpuNanos    int64
	allocBytes  uint64
	allocations uint64
	heapBytes   uint64
	rssBytes    uint64
	readBytes   uint64
	writeBytes  uint64
}

type mergeResourceMonitor struct {
	startErr    error
	stopCh      chan struct{}
	doneCh      chan struct{}
	start       mergeProcessResources
	peakHeap    atomic.Uint64
	peakRSS     atomic.Uint64
	attribution bool
}

func startMergeResourceMonitor(attribution bool) *mergeResourceMonitor {
	monitor := &mergeResourceMonitor{attribution: attribution}
	monitor.start, monitor.startErr = readMergeProcessResources()
	monitor.peakHeap.Store(monitor.start.heapBytes)
	monitor.peakRSS.Store(monitor.start.rssBytes)
	if !attribution {
		return monitor
	}
	monitor.stopCh = make(chan struct{})
	monitor.doneCh = make(chan struct{})
	go monitor.sample()
	return monitor
}

func (mrm *mergeResourceMonitor) sample() {
	defer close(mrm.doneCh)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-mrm.stopCh:
			return
		case <-ticker.C:
			resources, readErr := readMergeProcessResources()
			if readErr != nil {
				continue
			}
			updateAtomicMaximum(&mrm.peakHeap, resources.heapBytes)
			updateAtomicMaximum(&mrm.peakRSS, resources.rssBytes)
		}
	}
}

func (mrm *mergeResourceMonitor) stop(elapsed time.Duration) mergeBenchmarkResources {
	if mrm.attribution {
		close(mrm.stopCh)
		<-mrm.doneCh
	}
	end, endErr := readMergeProcessResources()
	updateAtomicMaximum(&mrm.peakHeap, end.heapBytes)
	updateAtomicMaximum(&mrm.peakRSS, end.rssBytes)
	resources := mergeBenchmarkResources{
		CPUNanos: max(int64(0), end.cpuNanos-mrm.start.cpuNanos), AllocatedBytes: saturatingCounterDelta(end.allocBytes, mrm.start.allocBytes),
		Allocations: saturatingCounterDelta(end.allocations, mrm.start.allocations), ReadBytes: saturatingCounterDelta(end.readBytes, mrm.start.readBytes),
		WriteBytes: saturatingCounterDelta(end.writeBytes, mrm.start.writeBytes), PeakHeapBytes: mrm.peakHeap.Load(), EndHeapBytes: end.heapBytes,
		PeakRSSBytes: mrm.peakRSS.Load(), EndRSSBytes: end.rssBytes, ElapsedNanos: elapsed.Nanoseconds(),
	}
	if resourceErr := errorsJoinText(mrm.startErr, endErr); resourceErr != "" {
		resources.Error = resourceErr
	}
	return resources
}

func updateAtomicMaximum(target *atomic.Uint64, value uint64) {
	for current := target.Load(); value > current; current = target.Load() {
		if target.CompareAndSwap(current, value) {
			return
		}
	}
}

func saturatingCounterDelta(end, start uint64) uint64 {
	if end < start {
		return 0
	}
	return end - start
}

func readMergeProcessResources() (mergeProcessResources, error) {
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	resources := mergeProcessResources{allocBytes: memory.TotalAlloc, allocations: memory.Mallocs, heapBytes: memory.HeapAlloc}
	var usage syscall.Rusage
	if usageErr := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); usageErr != nil {
		return resources, fmt.Errorf("cannot read process CPU: %w", usageErr)
	}
	resources.cpuNanos = usage.Utime.Nano() + usage.Stime.Nano()
	statmData, statmErr := os.ReadFile("/proc/self/statm")
	if statmErr != nil {
		return resources, fmt.Errorf("cannot read process RSS: %w", statmErr)
	}
	statmFields := strings.Fields(string(statmData))
	if len(statmFields) < 2 {
		return resources, fmt.Errorf("process RSS has %d fields", len(statmFields))
	}
	rssPages, parseErr := strconv.ParseUint(statmFields[1], 10, 64)
	if parseErr != nil {
		return resources, fmt.Errorf("cannot parse process RSS: %w", parseErr)
	}
	resources.rssBytes = rssPages * uint64(os.Getpagesize())
	ioData, ioErr := os.Open("/proc/self/io")
	if ioErr != nil {
		return resources, fmt.Errorf("cannot read process I/O: %w", ioErr)
	}
	scanner := bufio.NewScanner(ioData)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) != 2 {
			continue
		}
		value, valueErr := strconv.ParseUint(fields[1], 10, 64)
		if valueErr != nil {
			continue
		}
		switch strings.TrimSuffix(fields[0], ":") {
		case "read_bytes":
			resources.readBytes = value
		case "write_bytes":
			resources.writeBytes = value
		}
	}
	closeErr := ioData.Close()
	if scanErr := scanner.Err(); scanErr != nil {
		return resources, fmt.Errorf("cannot scan process I/O: %w", scanErr)
	}
	if closeErr != nil {
		return resources, fmt.Errorf("cannot close process I/O: %w", closeErr)
	}
	return resources, nil
}

func errorsJoinText(errors ...error) string {
	var messages []string
	for _, currentErr := range errors {
		if currentErr != nil {
			messages = append(messages, currentErr.Error())
		}
	}
	return strings.Join(messages, "; ")
}
