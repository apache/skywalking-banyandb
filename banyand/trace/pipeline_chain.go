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
	samplers       []sdk.Sampler
	group          string
	schema         string
	projection     sdk.Projection
	circuitOpen    bool
	circuitBreakN  int
	consecutiveTOs int
	mu             sync.Mutex
}

// newMergeChain builds a chain from the ordered samplers and computes the union
// projection: Tags is the union of all plugins' projected tag names; SpanIDs and
// Spans are true if any plugin requests them.
//
//nolint:unparam
func newMergeChain(group, schema string, samplers []sdk.Sampler, circuitBreakN int) *mergeChain {
	var union sdk.Projection
	seen := make(map[string]struct{})
	for _, sampler := range samplers {
		if sampler == nil {
			continue
		}
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
		samplers:      samplers,
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
	retainAll := func() sdk.Verdict {
		keep := make([]bool, len(batch.Traces))
		for i := range keep {
			keep[i] = true
		}
		return sdk.Verdict{Keep: keep}
	}

	mc.mu.Lock()
	if mc.circuitOpen {
		mc.mu.Unlock()
		return retainAll(), true, nil
	}
	mc.mu.Unlock()

	resultCh := make(chan sdk.Verdict, 1)
	go func() {
		resultCh <- mc.runChain(batch, observation, decisionMask)
	}()

	select {
	case verdict := <-resultCh:
		mc.mu.Lock()
		mc.consecutiveTOs = 0
		mc.mu.Unlock()
		return verdict, true, nil
	case <-time.After(timeout):
		mc.mu.Lock()
		mc.consecutiveTOs++
		opened := false
		if mc.circuitBreakN > 0 && mc.consecutiveTOs >= mc.circuitBreakN {
			mc.circuitOpen = true
			opened = true
		}
		mc.mu.Unlock()
		if opened {
			return retainAll(), false, fmt.Errorf("circuit_open")
		}
		return retainAll(), false, fmt.Errorf("timeout")
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
	if observation == nil {
		return sdk.EvaluateChainInto(mc.samplers, batch, decisionMask, onBypass)
	}
	observedSamplers := make([]sdk.Sampler, len(mc.samplers))
	evaluatedOnce := &sync.Once{}
	for samplerIdx, sampler := range mc.samplers {
		if sampler != nil {
			observedSamplers[samplerIdx] = observedMergeSampler{Sampler: sampler, observation: observation, evaluatedOnce: evaluatedOnce}
		}
	}
	return sdk.EvaluateChainInto(observedSamplers, batch, decisionMask, onBypass)
}

type observedMergeSampler struct {
	sdk.Sampler
	observation   *mergeEvaluationObservation
	evaluatedOnce *sync.Once
}

func (oms observedMergeSampler) Decide(batch *sdk.TraceBatch) (sdk.Verdict, error) {
	oms.observation.pluginCalls.Add(1)
	oms.evaluatedOnce.Do(func() {
		oms.observation.evaluated.Add(uint64(len(batch.Traces)))
	})
	return oms.Sampler.Decide(batch)
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
