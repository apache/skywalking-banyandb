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

	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk"
)

var chainLog = logger.GetLogger("trace").Named("pipeline-chain")

// mergeChain runs an ordered sampler chain over a vectorized TraceBatch and
// returns the conjunction keep-mask. The whole chain runs in a worker goroutine
// under a hard timeout so a slow plugin cannot stall compaction; every failure
// path (panic, error, length mismatch, timeout) fails open (retain). A
// consecutive-timeout circuit breaker disables the chain after circuitBreakN
// timeouts.
type mergeChain struct {
	worker         *mergeDecisionWorker
	timer          *time.Timer
	samplers       []sdk.Sampler
	group          string
	schema         string
	projection     sdk.Projection
	circuitOpen    bool
	circuitBreakN  int
	consecutiveTOs int
	executionMu    sync.Mutex
	mu             sync.Mutex
}

type mergeDecisionRequest struct {
	batch        *sdk.TraceBatch
	observation  *mergeEvaluationObservation
	decisionMask []bool
}

type mergeDecisionWorker struct {
	requests chan mergeDecisionRequest
	results  chan sdk.Verdict
	stopCh   chan struct{}
	stopOnce sync.Once
}

// newMergeChain builds a chain from the ordered samplers and computes the union
// projection: Tags is the union of all plugins' projected tag names; SpanIDs and
// Spans are true if any plugin requests them.
//
//nolint:unparam
func newMergeChain(group, schema string, samplers []sdk.Sampler, circuitBreakN int) *mergeChain {
	var union sdk.Projection
	seen := make(map[string]struct{})
	activeSamplers := make([]sdk.Sampler, 0, len(samplers))
	for _, sampler := range samplers {
		if sampler == nil {
			continue
		}
		activeSamplers = append(activeSamplers, sampler)
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
		projection:    union,
		samplers:      activeSamplers,
		group:         group,
		schema:        schema,
		circuitBreakN: circuitBreakN,
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
		return retainAllVerdict(len(batch.Traces), decisionMask), true, nil
	}
	mc.mu.Unlock()

	worker := mc.acquireWorker()
	worker.requests <- mergeDecisionRequest{batch: batch, observation: observation, decisionMask: decisionMask}
	timeoutCh := mc.resetTimer(timeout)

	select {
	case verdict := <-worker.results:
		mc.stopTimer()
		mc.mu.Lock()
		mc.consecutiveTOs = 0
		mc.mu.Unlock()
		return verdict, true, nil
	case <-timeoutCh:
		mc.abandonWorker(worker)
		mc.mu.Lock()
		mc.consecutiveTOs++
		opened := false
		if mc.circuitBreakN > 0 && mc.consecutiveTOs >= mc.circuitBreakN {
			mc.circuitOpen = true
			opened = true
		}
		mc.mu.Unlock()
		if opened {
			return retainAllVerdict(len(batch.Traces), nil), false, fmt.Errorf("circuit_open")
		}
		return retainAllVerdict(len(batch.Traces), nil), false, fmt.Errorf("timeout")
	}
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
	mc.worker = worker
	go worker.run(mc)
	return worker
}

func (mdw *mergeDecisionWorker) run(chain *mergeChain) {
	for {
		select {
		case request := <-mdw.requests:
			verdict := chain.runChain(request.batch, request.observation, request.decisionMask)
			select {
			case mdw.results <- verdict:
			case <-mdw.stopCh:
				return
			}
		case <-mdw.stopCh:
			return
		}
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
	return sdk.EvaluateChainInto(mc.samplers, batch, decisionMask, onBypass)
}

// assembleTraceBlock builds a COPY-backed sdk.TraceBlock from a loaded merge
// block, name-selecting only the columns named in proj. Every returned slice is
// owned by the block (deep-copied from the engine's pooled buffers) so the merge
// loop may recycle/overwrite those buffers while an abandoned Decide goroutine
// still reads the block.
func assembleTraceBlock(traceID string, bp *blockPointer, proj sdk.Projection) sdk.TraceBlock {
	tb := sdk.TraceBlock{
		TraceID: traceID,
		MinTS:   bp.bm.timestamps.min,
		MaxTS:   bp.bm.timestamps.max,
	}
	if len(proj.Tags) > 0 {
		for _, name := range proj.Tags {
			for i := range bp.block.tags {
				if decodeTypedTag(bp.block.tags[i].name) != name {
					continue
				}
				src := bp.block.tags[i].values
				values := make([][]byte, len(src))
				for j, v := range src {
					if v != nil {
						values[j] = append([]byte(nil), v...)
					}
				}
				tb.Tags = append(tb.Tags, sdk.TagColumn{
					Name:      name,
					Values:    values,
					ValueType: bp.block.tags[i].valueType,
				})
				break
			}
		}
	}
	if proj.SpanIDs {
		spanIDs := make([]string, len(bp.block.spanIDs))
		copy(spanIDs, bp.block.spanIDs)
		tb.SpanIDs = spanIDs
	}
	if proj.Spans {
		spans := make([][]byte, len(bp.block.spans))
		for i, s := range bp.block.spans {
			if s != nil {
				spans[i] = append([]byte(nil), s...)
			}
		}
		tb.Spans = spans
	}
	return tb
}

func projectionRequiresSlowPath(projection sdk.Projection) bool {
	return len(projection.Tags) > 0 || projection.SpanIDs || projection.Spans
}

func assembleStagedTraceBlock(group stagedTraceGroup, staged []stagedTrace, projection sdk.Projection) (sdk.TraceBlock, bool) {
	if group.traceID == "" || len(staged) == 0 || !group.validMetadata || group.minTS > group.maxTS {
		return sdk.TraceBlock{}, false
	}
	requiresSlowPath := projectionRequiresSlowPath(projection)
	if !requiresSlowPath {
		return sdk.TraceBlock{TraceID: group.traceID, MinTS: group.minTS, MaxTS: group.maxTS}, true
	}
	aggregate := generateBlockPointer()
	defer releaseBlockPointer(aggregate)
	aggregate.bm.traceID = group.traceID
	for stagedIdx := range staged {
		stagedBlock := &staged[stagedIdx]
		if stagedBlock.traceID != group.traceID {
			return sdk.TraceBlock{}, false
		}
		var metadata *blockMetadata
		switch {
		case stagedBlock.isRaw:
			return sdk.TraceBlock{}, false
		case stagedBlock.slowBlock != nil:
			metadata = &stagedBlock.slowBlock.bm
			aggregate.appendAll(stagedBlock.slowBlock)
		default:
			return sdk.TraceBlock{}, false
		}
		if metadata.traceID != group.traceID || !metadata.timestamps.known ||
			metadata.timestamps.min > metadata.timestamps.max {
			return sdk.TraceBlock{}, false
		}
	}
	if aggregate.Len() == 0 || aggregate.timestampBoundsUnknown {
		return sdk.TraceBlock{}, false
	}
	aggregate.bm.timestamps = timestampsMetadata{
		min:   group.minTS,
		max:   group.maxTS,
		known: true,
	}
	return assembleTraceBlock(group.traceID, aggregate, projection), true
}
