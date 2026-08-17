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

package trace

import (
	"fmt"
	"sync"
	"time"

	internalencoding "github.com/apache/skywalking-banyandb/banyand/internal/encoding"
	pkgbytes "github.com/apache/skywalking-banyandb/pkg/bytes"
	"github.com/apache/skywalking-banyandb/pkg/cgroups"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk"
)

var (
	chainLog = logger.GetLogger("trace").Named("pipeline-chain")
	// samplerExecutionSlots bounds batches retained by sampler calls that ignore
	// the execution timeout. A slot is released only after Decide returns.
	samplerExecutionSlots = make(chan struct{}, cgroups.CPUs())
)

const (
	pluginExecutionResultSuccess     = "success"
	pluginExecutionResultTimeout     = "timeout"
	pluginExecutionResultCircuitOpen = "circuit_open"
	pluginExecutionResultDecideError = sdk.BypassReasonDecideError
	pluginExecutionResultMismatch    = sdk.BypassReasonLengthMismatch
	pluginExecutionResultPanic       = sdk.BypassReasonPanic
	pluginExecutionResultLate        = "late"
)

type pluginExecutionObservation struct {
	result      string
	batchTraces int
}

type pluginLinkExecutionObservation struct {
	pluginName   string
	result       string
	bypassReason string
	elapsed      time.Duration
}

// mergeChain runs an ordered sampler chain over a vectorized TraceBatch and
// returns the conjunction keep-mask. The whole chain runs in a worker goroutine
// under a hard timeout so a slow plugin cannot stall compaction; every failure
// path (panic, error, length mismatch, timeout) fails open (retain). A
// consecutive-timeout circuit breaker disables the chain after circuitBreakN
// timeouts.
type mergeChain struct {
	observeExecution     func(pluginExecutionObservation)
	timer                *time.Timer
	worker               *mergeDecisionWorker
	executionSlots       chan struct{}
	observeLinkExecution func(pluginLinkExecutionObservation)
	group                string
	schema               string
	samplers             []sdk.Sampler
	samplerNames         []string
	projection           sdk.Projection
	consecutiveTOs       int
	circuitBreakN        int
	executionMu          sync.Mutex
	mu                   sync.Mutex
	circuitOpen          bool
}

type mergeDecisionRequest struct {
	batch        *sdk.TraceBatch
	observation  *mergeEvaluationObservation
	decisionMask []bool
}

type mergeDecisionWorker struct {
	observation *mergeEvaluationObservation
	requests    chan mergeDecisionRequest
	results     chan sdk.Verdict
	stopCh      chan struct{}
	samplers    []sdk.Sampler
	stopOnce    sync.Once
}

type observedSampler struct {
	sdk.Sampler
	chain      *mergeChain
	worker     *mergeDecisionWorker
	pluginName string
}

func (os *observedSampler) Decide(batch *sdk.TraceBatch) (verdict sdk.Verdict, decideErr error) {
	started := time.Now()
	result := pluginExecutionResultSuccess
	bypassReason := ""
	defer func() {
		if recovered := recover(); recovered != nil {
			os.observe(started, pluginExecutionResultPanic, sdk.BypassReasonPanic)
			panic(recovered)
		}
		if decideErr != nil {
			result = pluginExecutionResultDecideError
			bypassReason = sdk.BypassReasonDecideError
		} else if len(verdict.Keep) != len(batch.Traces) {
			result = pluginExecutionResultMismatch
			bypassReason = sdk.BypassReasonLengthMismatch
		}
		os.observe(started, result, bypassReason)
	}()
	return os.Sampler.Decide(batch)
}

func (os *observedSampler) observe(started time.Time, result, bypassReason string) {
	select {
	case <-os.worker.stopCh:
		result = pluginExecutionResultLate
	default:
	}
	observation := pluginLinkExecutionObservation{
		pluginName: os.pluginName, result: result, bypassReason: bypassReason,
		elapsed: time.Since(started),
	}
	if os.chain.observeLinkExecution != nil {
		os.chain.observeLinkExecution(observation)
	}
	if os.worker.observation != nil {
		os.worker.observation.recordPluginExecution(observation)
	}
}

// newMergeChain builds a chain from the ordered samplers and computes the union
// projection: Tags is the union of all plugins' projected tag names; SpanIDs and
// Spans are true if any plugin requests them.
//
//nolint:unparam
func newMergeChain(group, schema string, samplers []sdk.Sampler, circuitBreakN int) *mergeChain {
	named := make([]namedSampler, len(samplers))
	for idx, sampler := range samplers {
		named[idx] = namedSampler{sampler: sampler}
	}
	return newNamedMergeChain(group, schema, named, circuitBreakN)
}

func defaultSamplerMetricName(idx int) string {
	return fmt.Sprintf("plugin_%d", idx+1)
}

func newNamedMergeChain(group, schema string, samplers []namedSampler, circuitBreakN int) *mergeChain {
	var union sdk.Projection
	seen := make(map[string]struct{})
	activeSamplers := make([]sdk.Sampler, 0, len(samplers))
	activeNames := make([]string, 0, len(samplers))
	for _, named := range samplers {
		if named.sampler == nil {
			continue
		}
		sampler := named.sampler
		activeSamplers = append(activeSamplers, sampler)
		name := named.name
		if name == "" {
			name = defaultSamplerMetricName(len(activeNames))
		}
		activeNames = append(activeNames, name)
		proj := sampler.Project()
		for _, name := range proj.Tags {
			if _, ok := seen[name]; ok {
				continue
			}
			seen[name] = struct{}{}
			union.Tags = append(union.Tags, name)
		}
		if proj.SpanIDs {
			union.SpanIDs = true
		}
		if proj.Spans {
			union.Spans = true
		}
	}
	return &mergeChain{
		projection:     union,
		samplers:       activeSamplers,
		samplerNames:   activeNames,
		executionSlots: samplerExecutionSlots,
		group:          group,
		schema:         schema,
		circuitBreakN:  circuitBreakN,
	}
}

// Execute runs the chain over batch under the given timeout. It returns the
// per-trace keep verdict (aligned to batch.Traces) and, on a fail-open path, a
// non-nil error describing the reason (the verdict is then retain-all). The
// result channel is buffered so an abandoned (timed-out) goroutine never blocks.
func (mc *mergeChain) Execute(batch *sdk.TraceBatch, timeout time.Duration) (sdk.Verdict, error) {
	return mc.executeObserved(batch, timeout, nil)
}

func (mc *mergeChain) executeObserved(batch *sdk.TraceBatch, timeout time.Duration,
	observation *mergeEvaluationObservation,
) (sdk.Verdict, error) {
	verdict, _, executeErr := mc.executeObservedInto(batch, timeout, observation, nil)
	return verdict, executeErr
}

func (mc *mergeChain) executeObservedInto(batch *sdk.TraceBatch, timeout time.Duration,
	observation *mergeEvaluationObservation, decisionMask []bool,
) (sdk.Verdict, bool, error) {
	mc.executionMu.Lock()
	defer mc.executionMu.Unlock()

	mc.mu.Lock()
	if mc.circuitOpen {
		mc.mu.Unlock()
		mc.observe(pluginExecutionObservation{
			result: pluginExecutionResultCircuitOpen, batchTraces: len(batch.Traces),
		}, observation)
		return retainAllVerdict(len(batch.Traces), decisionMask), true, nil
	}
	mc.mu.Unlock()

	if !mc.acquireExecutionSlot(timeout) {
		return mc.handleTimeout(batch, observation, decisionMask, nil, true)
	}

	worker := mc.acquireWorker()
	worker.requests <- mergeDecisionRequest{batch: batch, observation: observation, decisionMask: decisionMask}
	timeoutCh := mc.resetTimer(timeout)

	select {
	case verdict := <-worker.results:
		mc.stopTimer()
		mc.mu.Lock()
		mc.consecutiveTOs = 0
		mc.mu.Unlock()
		mc.observe(pluginExecutionObservation{
			result: pluginExecutionResultSuccess, batchTraces: len(batch.Traces),
		}, observation)
		return verdict, true, nil
	case <-timeoutCh:
		return mc.handleTimeout(batch, observation, nil, worker, false)
	}
}

func (mc *mergeChain) acquireExecutionSlot(timeout time.Duration) bool {
	select {
	case mc.executionSlots <- struct{}{}:
		return true
	default:
	}
	timeoutCh := mc.resetTimer(timeout)
	select {
	case mc.executionSlots <- struct{}{}:
		mc.stopTimer()
		return true
	case <-timeoutCh:
		return false
	}
}

func (mc *mergeChain) handleTimeout(batch *sdk.TraceBatch, observation *mergeEvaluationObservation, decisionMask []bool,
	worker *mergeDecisionWorker, reusable bool,
) (sdk.Verdict, bool, error) {
	if worker != nil {
		mc.abandonWorker(worker)
	}
	mc.mu.Lock()
	mc.consecutiveTOs++
	opened := false
	if mc.circuitBreakN > 0 && mc.consecutiveTOs >= mc.circuitBreakN {
		mc.circuitOpen = true
		opened = true
	}
	mc.mu.Unlock()
	mc.observe(pluginExecutionObservation{
		result: pluginExecutionResultTimeout, batchTraces: len(batch.Traces),
	}, observation)
	if opened {
		return retainAllVerdict(len(batch.Traces), decisionMask), reusable, fmt.Errorf("circuit_open")
	}
	return retainAllVerdict(len(batch.Traces), decisionMask), reusable, fmt.Errorf("timeout")
}

func (mc *mergeChain) observe(observation pluginExecutionObservation, evaluation *mergeEvaluationObservation) {
	if len(mc.samplers) == 0 {
		return
	}
	if mc.observeExecution != nil {
		mc.observeExecution(observation)
	}
	evaluation.recordPluginBatch(observation)
}

func retainAllVerdict(traceCount int, mask []bool) sdk.Verdict {
	if cap(mask) < traceCount {
		mask = make([]bool, traceCount)
	} else {
		mask = mask[:traceCount]
	}
	for traceIdx := range mask {
		mask[traceIdx] = true
	}
	return sdk.Verdict{Keep: mask}
}

func (mc *mergeChain) acquireWorker() *mergeDecisionWorker {
	if mc.worker != nil {
		return mc.worker
	}
	worker := &mergeDecisionWorker{
		requests: make(chan mergeDecisionRequest),
		results:  make(chan sdk.Verdict),
		stopCh:   make(chan struct{}),
	}
	worker.samplers = make([]sdk.Sampler, len(mc.samplers))
	for idx, sampler := range mc.samplers {
		worker.samplers[idx] = &observedSampler{
			Sampler: sampler, chain: mc, worker: worker, pluginName: mc.samplerNames[idx],
		}
	}
	mc.worker = worker
	go worker.run(mc)
	return worker
}

func (mdw *mergeDecisionWorker) run(chain *mergeChain) {
	for {
		select {
		case request := <-mdw.requests:
			if !mdw.execute(chain, request) {
				return
			}
		case <-mdw.stopCh:
			return
		}
	}
}

func (mdw *mergeDecisionWorker) execute(chain *mergeChain, request mergeDecisionRequest) (keepRunning bool) {
	defer func() { <-chain.executionSlots }()
	mdw.observation = request.observation
	result := chain.runChainWithSamplers(mdw.samplers, request.batch, request.observation, request.decisionMask)
	mdw.observation = nil
	select {
	case mdw.results <- result:
		return true
	case <-mdw.stopCh:
		return false
	}
}

func (mc *mergeChain) abandonWorker(worker *mergeDecisionWorker) {
	worker.stopOnce.Do(func() { close(worker.stopCh) })
	if mc.worker == worker {
		mc.worker = nil
	}
}

func (mc *mergeChain) resetTimer(timeout time.Duration) <-chan time.Time {
	if mc.timer == nil {
		mc.timer = time.NewTimer(timeout)
		return mc.timer.C
	}
	mc.stopTimer()
	mc.timer.Reset(timeout)
	return mc.timer.C
}

func (mc *mergeChain) stopTimer() {
	if mc.timer == nil || mc.timer.Stop() {
		return
	}
	select {
	case <-mc.timer.C:
	default:
	}
}

func (mc *mergeChain) close() {
	mc.executionMu.Lock()
	defer mc.executionMu.Unlock()
	mc.stopTimer()
	if mc.worker != nil {
		mc.abandonWorker(mc.worker)
	}
}

// runChain evaluates the chain via the shared sdk.EvaluateChain — the same
// AND-aggregation + per-link panic/error/length-mismatch fail-open logic the
// offline sdktest.RunChain harness uses — passing an onBypass observer that
// reproduces the pre-refactor WARN logs (same fields, same messages) so this
// change is behavior-preserving.
func (mc *mergeChain) runChain(batch *sdk.TraceBatch, observation *mergeEvaluationObservation, decisionMask []bool) sdk.Verdict {
	return mc.runChainWithSamplers(mc.samplers, batch, observation, decisionMask)
}

func (mc *mergeChain) runChainWithSamplers(samplers []sdk.Sampler, batch *sdk.TraceBatch, observation *mergeEvaluationObservation,
	decisionMask []bool,
) sdk.Verdict {
	onBypass := func(_ int, info sdk.BypassInfo) {
		if info.Reason == sdk.BypassReasonLengthMismatch {
			chainLog.Warn().Int("got", info.Got).Int("want", info.Want).
				Str("group", mc.group).Str("schema", mc.schema).Msg("sampler verdict length mismatch; bypassing (retain)")
			return
		}
		chainLog.Warn().Err(info.Err).Str("group", mc.group).Str("schema", mc.schema).Msg("sampler link failed; bypassing (retain)")
	}
	if observation != nil {
		observation.pluginCalls.Add(uint64(len(mc.samplers)))
		if len(mc.samplers) > 0 {
			observation.evaluated.Add(uint64(len(batch.Traces)))
		}
	}
	return sdk.EvaluateChainInto(samplers, batch, decisionMask, onBypass)
}

func projectionRequiresSlowPath(projection sdk.Projection) bool {
	return len(projection.Tags) > 0 || projection.SpanIDs || projection.Spans
}

func projectedRawTagName(metadata *blockMetadata, projectedName string) (string, bool) {
	if _, exists := metadata.tags[projectedName]; exists {
		return projectedName, true
	}
	selectedName := ""
	for encodedName := range metadata.tags {
		if decodeTypedTag(encodedName) == projectedName && (selectedName == "" || encodedName < selectedName) {
			selectedName = encodedName
		}
	}
	return selectedName, selectedName != ""
}

func appendProjectedTraceBlock(vectors *stagedEvaluationVectors, traceID string, minTS, maxTS int64, source *block,
	projection sdk.Projection,
) sdk.TraceBlock {
	arenaSize := 0
	for _, projectedName := range projection.Tags {
		for tagIdx := range source.tags {
			sourceTag := &source.tags[tagIdx]
			if decodeTypedTag(sourceTag.name) != projectedName {
				continue
			}
			for _, value := range sourceTag.values {
				arenaSize += len(value)
			}
			break
		}
	}
	if projection.Spans {
		for _, span := range source.spans {
			arenaSize += len(span)
		}
	}
	if projection.SpanIDs {
		for _, spanID := range source.spanIDs {
			arenaSize += len(spanID)
		}
	}
	var arena *pkgbytes.Buffer
	if arenaSize > 0 {
		arena = acquireStagedByteArena(arenaSize)
		vectors.projectionArenas = append(vectors.projectionArenas, arena)
	}
	offset := 0
	copyBytes := func(sourceBytes []byte) []byte {
		if sourceBytes == nil {
			return nil
		}
		if len(sourceBytes) == 0 {
			return []byte{}
		}
		target := arena.Buf[offset : offset+len(sourceBytes) : offset+len(sourceBytes)]
		copy(target, sourceBytes)
		offset += len(sourceBytes)
		return target
	}
	traceBlock := sdk.TraceBlock{TraceID: traceID, MinTS: minTS, MaxTS: maxTS}
	columnStart := len(vectors.tagColumns)
	for _, projectedName := range projection.Tags {
		for tagIdx := range source.tags {
			sourceTag := &source.tags[tagIdx]
			if decodeTypedTag(sourceTag.name) != projectedName {
				continue
			}
			valueStart := len(vectors.tagValues)
			for _, value := range sourceTag.values {
				vectors.tagValues = append(vectors.tagValues, copyBytes(value))
			}
			values := vectors.tagValues[valueStart:len(vectors.tagValues):len(vectors.tagValues)]
			vectors.tagColumns = append(vectors.tagColumns, sdk.TagColumn{
				Name: projectedName, ValueType: sourceTag.valueType, Values: values,
			})
			break
		}
	}
	traceBlock.Tags = vectors.tagColumns[columnStart:len(vectors.tagColumns):len(vectors.tagColumns)]
	if projection.SpanIDs {
		spanIDStart := len(vectors.spanIDs)
		for _, spanID := range source.spanIDs {
			vectors.spanIDs = append(vectors.spanIDs, convert.BytesToString(copyBytes(convert.StringToBytes(spanID))))
		}
		traceBlock.SpanIDs = vectors.spanIDs[spanIDStart:len(vectors.spanIDs):len(vectors.spanIDs)]
	}
	if projection.Spans {
		spanStart := len(vectors.spans)
		for _, span := range source.spans {
			vectors.spans = append(vectors.spans, copyBytes(span))
		}
		traceBlock.Spans = vectors.spans[spanStart:len(vectors.spans):len(vectors.spans)]
	}
	return traceBlock
}

func assembleRawTraceBlockInto(vectors *stagedEvaluationVectors, stagedBlock *stagedTrace, projection sdk.Projection) (sdk.TraceBlock, bool) {
	metadata := stagedBlock.rawBM
	if metadata == nil || metadata.traceID != stagedBlock.traceID || !metadata.timestamps.known || metadata.timestamps.min > metadata.timestamps.max {
		return sdk.TraceBlock{}, false
	}
	decoder := generateColumnValuesDecoder()
	defer releaseColumnValuesDecoder(decoder)
	projectedBlock := block{minTS: metadata.timestamps.min, maxTS: metadata.timestamps.max}
	if len(projection.Tags) > 0 {
		for _, projectedName := range projection.Tags {
			encodedName, exists := projectedRawTagName(metadata, projectedName)
			if !exists {
				continue
			}
			valueType := metadata.tagType[encodedName]
			encodedValues, exists := stagedBlock.rawTags[encodedName]
			if !exists {
				return sdk.TraceBlock{}, false
			}
			valueBuffer := pkgbytes.Buffer{Buf: encodedValues}
			values, decodeErr := internalencoding.DecodeTagValues(nil, decoder, &valueBuffer, valueType, int(metadata.count))
			if decodeErr != nil {
				return sdk.TraceBlock{}, false
			}
			projectedBlock.tags = append(projectedBlock.tags, tag{name: projectedName, valueType: valueType, values: values})
		}
	}
	if projection.SpanIDs || projection.Spans {
		spanIDBytes, spanTail, decodeErr := decoder.DecodeWithTail(nil, stagedBlock.rawSpans, metadata.count)
		if decodeErr != nil {
			return sdk.TraceBlock{}, false
		}
		if projection.SpanIDs {
			projectedBlock.spanIDs = make([]string, len(spanIDBytes))
			for spanIdx, spanID := range spanIDBytes {
				projectedBlock.spanIDs[spanIdx] = convert.BytesToString(spanID)
			}
		}
		if projection.Spans {
			spans, spansErr := decoder.Decode(nil, spanTail, metadata.count)
			if spansErr != nil {
				return sdk.TraceBlock{}, false
			}
			projectedBlock.spans = spans
		}
	}
	return appendProjectedTraceBlock(vectors, stagedBlock.traceID, metadata.timestamps.min, metadata.timestamps.max, &projectedBlock, projection), true
}

func assembleStagedTraceBlockInto(vectors *stagedEvaluationVectors, group stagedTraceGroup, staged []stagedTrace,
	projection sdk.Projection,
) (sdk.TraceBlock, bool) {
	if group.traceID == "" || len(staged) == 0 || !group.validMetadata || group.minTS > group.maxTS {
		return sdk.TraceBlock{}, false
	}
	if !projectionRequiresSlowPath(projection) {
		return sdk.TraceBlock{TraceID: group.traceID, MinTS: group.minTS, MaxTS: group.maxTS}, true
	}
	if len(staged) == 1 && staged[0].isRaw {
		return assembleRawTraceBlockInto(vectors, &staged[0], projection)
	}
	aggregate := generateBlockPointer()
	defer releaseBlockPointer(aggregate)
	aggregate.bm.traceID = group.traceID
	for stagedIdx := range staged {
		stagedBlock := &staged[stagedIdx]
		if stagedBlock.traceID != group.traceID || stagedBlock.isRaw || stagedBlock.slowBlock == nil {
			return sdk.TraceBlock{}, false
		}
		metadata := &stagedBlock.slowBlock.bm
		if metadata.traceID != group.traceID || !metadata.timestamps.known || metadata.timestamps.min > metadata.timestamps.max {
			return sdk.TraceBlock{}, false
		}
		aggregate.appendAll(stagedBlock.slowBlock)
	}
	if aggregate.Len() == 0 || aggregate.timestampBoundsUnknown {
		return sdk.TraceBlock{}, false
	}
	return appendProjectedTraceBlock(vectors, group.traceID, group.minTS, group.maxTS, &aggregate.block, projection), true
}
