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
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/dustin/go-humanize"

	"github.com/apache/skywalking-banyandb/banyand/internal/sidx"
	pkgbytes "github.com/apache/skywalking-banyandb/pkg/bytes"
	"github.com/apache/skywalking-banyandb/pkg/cgroups"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk"
	pkgpool "github.com/apache/skywalking-banyandb/pkg/pool"
	"github.com/apache/skywalking-banyandb/pkg/run"
	"github.com/apache/skywalking-banyandb/pkg/watcher"
)

var mergeMaxConcurrencyCh = make(chan struct{}, cgroups.CPUs())

const (
	mergeTypeMem      = "mem"
	mergeTypeFile     = "file"
	mergeTypeFinalize = "finalize"
	mergeLaneFast     = "fast"
	mergeLaneSlow     = "slow"
	mergeLaneFinalize = "finalize"
)

const defaultSmallMergeThreshold = 32 << 20 // 32MB fallback

func computeSmallMergeThreshold() uint64 {
	memLimit, err := cgroups.MemoryLimit()
	if err != nil || memLimit <= 0 {
		return defaultSmallMergeThreshold
	}
	threshold := uint64(memLimit) / 16
	if threshold < defaultSmallMergeThreshold {
		return defaultSmallMergeThreshold
	}
	return threshold
}

const (
	// defaultStageBudgetFloor is the lower bound on the staged-set budget so the
	// retention filter still makes useful progress on small or unknown limits.
	defaultStageBudgetFloor = 16 << 20 // 16MB
	// stageBudgetAggregateDivisor bounds the aggregate bytes staged across all
	// concurrent merges to ~memLimit/divisor: each of the up-to-CPUs() concurrent
	// merges gets memLimit/(divisor*CPUs).
	stageBudgetAggregateDivisor = 4
	// defaultMaxStagedTraceCount independently bounds the number of logical
	// traces exposed to one sampler call even when their byte footprint is tiny.
	defaultMaxStagedTraceCount = 64 * 1024
	// adaptiveBatchHardLimitDivisor keeps the preferred decision working set
	// below the resource-derived safety ceiling while leaving room for a whole
	// trace to cross the preferred boundary.
	adaptiveBatchHardLimitDivisor = 8
	// adaptiveBatchArenaMultiplier lets a completed batch reuse the bounded raw
	// arena cache during the following batch without making the cache itself the
	// staging safety limit.
	adaptiveBatchArenaMultiplier = 2
)

type adaptiveDecisionBatchPlan struct {
	EstimatedBytes uint64
	HardLimit      uint64
	BatchLimit     uint64
	PlannedBatches uint64
}

func planAdaptiveDecisionBatch(parts []*partWrapper, hardLimit uint64) adaptiveDecisionBatchPlan {
	var compressedBytes, uncompressedBytes, blocksCount uint64
	for _, partData := range parts {
		if partData == nil || partData.p == nil {
			continue
		}
		metadata := &partData.p.partMetadata
		compressedBytes = saturatingAddUint64(compressedBytes, metadata.CompressedSizeBytes)
		uncompressedBytes = saturatingAddUint64(uncompressedBytes, metadata.UncompressedSpanSizeBytes)
		blocksCount = saturatingAddUint64(blocksCount, metadata.BlocksCount)
	}
	payloadBytes := max(compressedBytes, uncompressedBytes)
	structuralBytes := saturatingMultiplyUint64(blocksCount, estimatedStagedBlockStructuralBytes())
	estimatedBytes := saturatingAddUint64(payloadBytes, structuralBytes)
	nominalLimit := nominalDecisionBatchLimit(hardLimit)
	if estimatedBytes == 0 {
		return adaptiveDecisionBatchPlan{HardLimit: hardLimit, BatchLimit: nominalLimit, PlannedBatches: 1}
	}
	plannedBatches := roundedQuotientUint64(estimatedBytes, nominalLimit)
	batchLimit := ceilDivUint64(estimatedBytes, plannedBatches)
	minimumLimit := min(uint64(defaultStageBudgetFloor), hardLimit)
	batchLimit = min(max(batchLimit, minimumLimit), hardLimit)
	return adaptiveDecisionBatchPlan{
		EstimatedBytes: estimatedBytes,
		HardLimit:      hardLimit,
		BatchLimit:     batchLimit,
		PlannedBatches: plannedBatches,
	}
}

func estimatedStagedBlockStructuralBytes() uint64 {
	return uint64(unsafe.Sizeof(stagedTrace{})) + uint64(unsafe.Sizeof(blockMetadata{})) +
		uint64(unsafe.Sizeof(stagedDataBlockBuffer{})) + uint64(unsafe.Sizeof(stagedTraceGroup{})) +
		stagedTraceDecisionBytes() + stagedEvaluationSlotBytes(true)
}

func nominalDecisionBatchLimit(hardLimit uint64) uint64 {
	if hardLimit == 0 {
		return defaultSmallMergeThreshold
	}
	poolWindow := uint64(maxPooledStagedArenaBytes * adaptiveBatchArenaMultiplier)
	resourceWindow := hardLimit / adaptiveBatchHardLimitDivisor
	nominalLimit := max(uint64(defaultStageBudgetFloor), poolWindow, resourceWindow)
	return min(nominalLimit, hardLimit)
}

func roundedQuotientUint64(numerator, denominator uint64) uint64 {
	if denominator == 0 {
		return 1
	}
	quotient := numerator / denominator
	remainder := numerator % denominator
	if remainder >= ceilDivUint64(denominator, 2) {
		quotient++
	}
	return max(uint64(1), quotient)
}

func ceilDivUint64(numerator, denominator uint64) uint64 {
	if denominator == 0 {
		return 0
	}
	quotient := numerator / denominator
	if numerator%denominator != 0 {
		quotient++
	}
	return quotient
}

func saturatingAddUint64(left, right uint64) uint64 {
	if ^uint64(0)-left < right {
		return ^uint64(0)
	}
	return left + right
}

func saturatingMultiplyUint64(left, right uint64) uint64 {
	if left == 0 || right == 0 {
		return 0
	}
	if left > ^uint64(0)/right {
		return ^uint64(0)
	}
	return left * right
}

// testStageBudgetOverride forces the staging budget when non-zero. Test-only
// seam (mirrors forceSlowMerge); production always derives it from the memory
// limit via stageBudgetFromLimit.
var testStageBudgetOverride uint64

// resolveStageBudget returns the resource-derived hard staging ceiling. A
// selected merge derives its smaller preferred decision-batch limit separately
// from part metadata. There is no operator flag.
func resolveStageBudget(opt option) uint64 {
	if testStageBudgetOverride > 0 {
		return testStageBudgetOverride
	}
	var limit uint64
	if opt.protector != nil {
		limit = opt.protector.GetLimit()
	}
	return stageBudgetFromLimit(limit)
}

// resolveTraceBudget returns the hard limit on bytes retained for one logical
// trace evaluation. A trace that exceeds it is retained without reaching the
// sampler, because a partial trace must never be evaluated. It deliberately
// uses the same memory-derived value as the aggregate staging budget: the two
// limits protect different scopes even when their numeric values match.
func resolveTraceBudget(opt option) uint64 {
	var limit uint64
	if opt.protector != nil {
		limit = opt.protector.GetLimit()
	}
	return stageBudgetFromLimit(limit)
}

// stageBudgetFromLimit derives the per-merge staged-byte budget from the memory
// limit. With up to cgroups.CPUs() merges staging at once, each merge is allowed
// memLimit/(stageBudgetAggregateDivisor*CPUs) so the aggregate stays ~memLimit/
// stageBudgetAggregateDivisor, clamped to [defaultStageBudgetFloor,
// computeSmallMergeThreshold()] (never more than the fast/slow lane boundary).
// A zero limit (protector disabled) falls back to the lane-split floor.
func stageBudgetFromLimit(limit uint64) uint64 {
	if limit == 0 {
		return defaultSmallMergeThreshold
	}
	cpus := uint64(max(1, cgroups.CPUs()))
	budget := limit / (stageBudgetAggregateDivisor * cpus)
	budget = max(budget, uint64(defaultStageBudgetFloor))
	budget = min(budget, computeSmallMergeThreshold())
	return budget
}

func maxStagedTraceCountFromBudget(stageBudget uint64) int {
	minimumTraceBytes := uint64(unsafe.Sizeof(stagedTrace{})) + uint64(unsafe.Sizeof(stagedTraceGroup{})) +
		stagedTraceDecisionBytes() + stagedEvaluationSlotBytes(true)
	if stageBudget == 0 || minimumTraceBytes == 0 {
		return defaultMaxStagedTraceCount
	}
	byteBound := max(uint64(1), stageBudget/minimumTraceBytes)
	return int(min(byteBound, uint64(defaultMaxStagedTraceCount)))
}

type mergeDispatchRequest struct {
	enqueuedAt time.Time
	benchmark  *mergeBenchmarkSeed
	toBeMerged map[uint64]struct{}
	typ        string
	lane       string
	parts      []*partWrapper
}

func (tst *tsTable) mergeLoop(merges chan *mergerIntroduction, flusherNotifier watcher.Channel) {
	defer tst.loopCloser.Done()
	defer tst.mergeControl.stop()

	var lastProcessedEpoch uint64

	ew := flusherNotifier.Add(0, tst.loopCloser.CloseNotify())
	if ew == nil {
		return
	}

	threshold := computeSmallMergeThreshold()
	fastWorkers := max(1, cgroups.CPUs()/2)
	fastCh := make(chan *mergeDispatchRequest, fastWorkers)
	slowCh := make(chan *mergeDispatchRequest, 1)

	var workersWg, dispatcherWg sync.WaitGroup

	for i := 0; i < fastWorkers; i++ {
		workersWg.Add(1)
		run.Go(context.Background(), "trace.merger.fast-lane", tst.l, func(_ context.Context) {
			defer workersWg.Done()
			tst.mergeLaneWorker(fastCh, merges) //nolint:contextcheck
		})
	}
	workersWg.Add(1)
	run.Go(context.Background(), "trace.merger.slow-lane", tst.l, func(_ context.Context) {
		defer workersWg.Done()
		tst.mergeLaneWorker(slowCh, merges) //nolint:contextcheck
	})

	dispatcherWg.Add(1)
	run.Go(context.Background(), "trace.merger.dispatcher", tst.l, func(_ context.Context) {
		defer dispatcherWg.Done()
		tst.dispatcherLoop(threshold, fastCh, slowCh)
	})

	// Shutdown order: stop dispatcher first so no new work enters the lane
	// channels, then close the lane channels so idle workers exit their range
	// loops, then wait for workers to drain any in-flight merges.
	defer func() {
		dispatcherWg.Wait()
		close(fastCh)
		close(slowCh)
		workersWg.Wait()
	}()

	for {
		select {
		case <-tst.loopCloser.CloseNotify():
			return
		case <-ew.Watch():
			if curSnapshot := tst.currentSnapshot(); curSnapshot != nil {
				if curSnapshot.epoch > lastProcessedEpoch {
					if triggerErr := tst.triggerMerge(); triggerErr != nil {
						curSnapshot.decRef()
						return
					}
					lastProcessedEpoch = curSnapshot.epoch
				}
				curSnapshot.decRef()
			}
			ew = flusherNotifier.Add(lastProcessedEpoch, tst.loopCloser.CloseNotify())
			if ew == nil {
				return
			}
		}
	}
}

func (tst *tsTable) dispatcherLoop(threshold uint64, fastCh, slowCh chan *mergeDispatchRequest) {
	for {
		select {
		case <-tst.loopCloser.CloseNotify():
			return
		case <-tst.mergeControl.trigger:
			for {
				delay := tst.mergeControl.backoffRemaining()
				if delay <= 0 {
					break
				}
				state := tst.mergeControl.state()
				sleepStart := time.Now()
				select {
				case <-time.After(delay):
				case <-state.changed:
				case <-tst.loopCloser.CloseNotify():
					tst.incTotalMergeBackoffSeconds(time.Since(sleepStart).Seconds())
					return
				}
				tst.incTotalMergeBackoffSeconds(time.Since(sleepStart).Seconds())
			}
			dispatch, maxPartID := tst.mergeControl.beginDispatch()
			if !dispatch {
				continue
			}
			if tst.dispatchAllMergesUpTo(threshold, fastCh, slowCh, maxPartID) {
				tst.mergeControl.endDispatch()
				return
			}
			tst.mergeControl.endDispatch()
		}
	}
}

func (tst *tsTable) dispatchAllMerges(threshold uint64, fastCh, slowCh chan *mergeDispatchRequest) bool {
	return tst.dispatchAllMergesUpTo(threshold, fastCh, slowCh, 0)
}

func (tst *tsTable) dispatchAllMergesUpTo(threshold uint64, fastCh, slowCh chan *mergeDispatchRequest, maxPartID uint64) bool {
	for {
		curSnapshot := tst.currentSnapshot()
		if curSnapshot == nil {
			if tst.mergeControl != nil {
				tst.mergeControl.observeEmpty(0)
			}
			return false
		}
		freeDiskSize := tst.freeDiskSpace(tst.root)
		var dst []*partWrapper
		dst, toBeMerged := tst.getPartsToMergeUpTo(curSnapshot, freeDiskSize, dst, maxPartID)
		if len(dst) < 2 {
			epoch := curSnapshot.epoch
			curSnapshot.decRef()
			if tst.mergeControl != nil {
				tst.mergeControl.observeEmpty(epoch)
			}
			return false
		}
		for _, pw := range dst {
			pw.incRef()
		}
		curSnapshot.decRef()

		tst.inFlightMu.Lock()
		if tst.inFlight == nil {
			tst.inFlight = make(map[uint64]struct{})
		}
		// Re-check membership under the lock before pinning. getPartsToMerge already
		// skips in-flight parts, but the finalize scanner is a second, concurrent part
		// selector: it may have pinned one of these parts in the window between
		// getPartsToMerge's RLock and this Lock. Pinning it here anyway would let both
		// actors merge the same part, and since snapshot.remove tolerates an absent id
		// both merge outputs would survive — duplicating traces. On conflict, abandon
		// this dispatch cycle (the next flush trigger re-dispatches).
		conflict := false
		for _, pw := range dst {
			if _, inFlight := tst.inFlight[pw.ID()]; inFlight {
				conflict = true
				break
			}
		}
		if conflict {
			tst.inFlightMu.Unlock()
			for _, pw := range dst {
				pw.decRef()
			}
			return false
		}
		for _, pw := range dst {
			tst.inFlight[pw.ID()] = struct{}{}
		}
		tst.inFlightMu.Unlock()

		var totalSize uint64
		for _, pw := range dst {
			totalSize += pw.p.partMetadata.CompressedSizeBytes
		}

		lane := mergeLaneSlow
		targetCh := slowCh
		if totalSize < threshold {
			lane = mergeLaneFast
			targetCh = fastCh
		}

		req := &mergeDispatchRequest{
			parts:      dst,
			toBeMerged: toBeMerged,
			typ:        mergeTypeFile,
			lane:       lane,
			enqueuedAt: time.Now(),
		}
		req.benchmark = tst.mergeBenchmark.Load().seed(tst, dst, req.typ, req.lane)

		tst.l.Info().
			Str("lane", lane).
			Uint64("totalSize", totalSize).
			Uint64("threshold", threshold).
			Int("partCount", len(dst)).
			Msg("dispatching merge")

		if tst.mergeControl != nil {
			tst.mergeControl.addQueued()
		}
		select {
		case targetCh <- req:
		case <-tst.loopCloser.CloseNotify():
			if tst.mergeControl != nil {
				tst.mergeControl.cancelQueued()
			}
			tst.releaseDispatchRequest(req)
			return true
		}
		if tst.mergeControl != nil && tst.mergeControl.finishWaveDispatch() {
			return false
		}
	}
}

func (tst *tsTable) mergeLaneWorker(ch chan *mergeDispatchRequest, merges chan *mergerIntroduction) {
	for req := range ch {
		if tst.mergeControl != nil {
			tst.mergeControl.startQueued()
		}
		if !req.enqueuedAt.IsZero() {
			tst.incTotalMergeQueueLatency(time.Since(req.enqueuedAt).Seconds(), req.typ, req.lane)
		}
		select {
		case mergeMaxConcurrencyCh <- struct{}{}:
		case <-tst.loopCloser.CloseNotify():
			tst.stopMergeLaneWorker(req, ch)
			return
		}
		attributionAcquired := false
		if tst.mergeAttribution.Load() {
			select {
			case tst.attributionCh <- struct{}{}:
				attributionAcquired = true
			case <-tst.loopCloser.CloseNotify():
				<-mergeMaxConcurrencyCh
				tst.stopMergeLaneWorker(req, ch)
				return
			}
		}

		tst.incTotalMergeLoopStarted(1)
		_, mergeErr := tst.mergePartsThenSendIntroductionObserved(
			snapshotCreatorMerger, req.parts, req.toBeMerged, merges,
			tst.loopCloser.CloseNotify(), req.typ, req.lane, nil, req.benchmark,
		)
		tst.incTotalMergeLoopFinished(1)
		if attributionAcquired {
			<-tst.attributionCh
		}
		<-mergeMaxConcurrencyCh

		tst.releaseDispatchRequest(req)
		if tst.mergeControl != nil {
			tst.mergeControl.finishRunning()
		}

		if mergeErr != nil {
			if !errors.Is(mergeErr, errClosed) {
				tst.l.Logger.Warn().Err(mergeErr).Str("typ", req.typ).Str("lane", req.lane).Msg("merge lane worker error")
				tst.incTotalMergeLoopErr(1)
				tst.recordUnreadablePart(mergeErr)
				if tst.mergeControl != nil {
					tst.mergeControl.recordOutcome(false)
				}
			}
		} else if tst.mergeControl != nil {
			tst.mergeControl.recordOutcome(true)
		}
	}
}

func (tst *tsTable) stopMergeLaneWorker(req *mergeDispatchRequest, ch chan *mergeDispatchRequest) {
	tst.releaseDispatchRequest(req)
	if tst.mergeControl != nil {
		tst.mergeControl.finishRunning()
	}
	// Drain remaining buffered requests so their inFlight entries and part
	// references are released. The lane channel is closed by mergeLoop after the
	// dispatcher exits, which lets this range terminate.
	for pending := range ch {
		tst.releaseDispatchRequest(pending)
		if tst.mergeControl != nil {
			tst.mergeControl.cancelQueued()
		}
	}
}

func (tst *tsTable) releaseDispatchRequest(req *mergeDispatchRequest) {
	tst.inFlightMu.Lock()
	for _, pw := range req.parts {
		delete(tst.inFlight, pw.ID())
	}
	tst.inFlightMu.Unlock()
	for _, pw := range req.parts {
		pw.decRef()
	}
	if tst.mergeControl != nil {
		tst.mergeControl.notify()
	}
}

// mergeOverrides lets the finalize round drive mergePartsThenSendIntroduction with a
// pre-built sampler filter (bypassing the hot-path isMergeHot/merge_grace build) and a
// finalize-generation stamp for the output part. A nil *mergeOverrides means the hot /
// flusher path: build the filter from merge-time rules and min-propagate finalizeGen.
type mergeOverrides struct {
	filter      *mergeFilter
	finalizeGen *uint64
}

// buildHotMergeFilter builds the in-merge retention filter for the hot / flusher merge
// path from the merge-time rules (registered samplers + merge_grace maturity). It
// returns nil when the native pipeline is off, no samplers are registered, or the merge
// is still hot (parts younger than merge_grace), in which case the merge runs unfiltered.
func (tst *tsTable) buildHotMergeFilter(parts []*partWrapper) *mergeFilter {
	filter, _ := tst.buildHotMergeFilterDecision(parts)
	return filter
}

func (tst *tsTable) buildHotMergeFilterDecision(parts []*partWrapper) (*mergeFilter, mergeSamplingReason) {
	return tst.buildHotMergeFilterDecisionAt(parts, tst.mergeNow().UnixNano())
}

func (tst *tsTable) buildHotMergeFilterDecisionAt(parts []*partWrapper, logicalNow int64) (*mergeFilter, mergeSamplingReason) {
	if len(parts) == 0 {
		return nil, mergeReasonEmptyInput
	}
	if !tst.option.nativePipelineEnabled {
		return nil, mergeReasonPipelineDisabled
	}
	samplers := lookupSamplers(tst.group)
	hasSampler := false
	for _, sampler := range samplers {
		if sampler != nil {
			hasSampler = true
			break
		}
	}
	if !hasSampler {
		return nil, mergeReasonNoSampler
	}
	// The sampler set is shared with FINALIZE (DD11); only filter hot merges when the
	// MERGE event is actually enabled for this group, so a FINALIZE-only config does
	// not silently filter hot merges.
	if !mergeEventEnabledForGroup(tst.group) {
		return nil, mergeReasonEventDisabled
	}
	graceNs := tst.effectiveMergeGraceNs()
	if tst.option.maxTraceFragmentGap <= 0 || time.Duration(graceNs) < tst.option.maxTraceFragmentGap {
		tst.incPipelineGuardBypassed()
		return nil, mergeReasonFragmentGap
	}
	if isMergeHot(parts, graceNs, logicalNow) {
		tst.incPipelineGuardBypassed()
		return nil, mergeReasonGrace
	}
	stagingHardLimit := resolveStageBudget(tst.option)
	stagingPlan := planAdaptiveDecisionBatch(parts, stagingHardLimit)
	guard := tst.newTraceFragmentGuardSession(parts, tst.option.maxTraceFragmentGap, stagingHardLimit)
	if guard == nil {
		tst.incPipelineGuardBypassed()
		return nil, mergeReasonGuardUnavailable
	}
	chain := newMergeChain(tst.group, "", samplers, tst.option.decideTimeoutCircuitBreak)
	return &mergeFilter{
		chain:                 chain,
		guard:                 guard,
		ctx:                   tst.loopCloser.Ctx(),
		owner:                 tst,
		timeout:               tst.option.decideTimeout,
		stagingHardLimit:      stagingHardLimit,
		decisionBatchLimit:    stagingPlan.BatchLimit,
		estimatedStagingBytes: stagingPlan.EstimatedBytes,
		plannedStagingBatches: stagingPlan.PlannedBatches,
		traceBudget:           resolveTraceBudget(tst.option),
		maxTraceCount:         maxStagedTraceCountFromBudget(stagingPlan.BatchLimit),
		forceSlow:             projectionRequiresSlowPath(chain.projection),
	}, ""
}

func (tst *tsTable) mergeNow() time.Time {
	if logicalNow := tst.mergeNowOverride.Load(); logicalNow != 0 {
		return time.Unix(0, logicalNow)
	}
	return time.Now()
}

func (tst *tsTable) setMergeNow(now time.Time) {
	tst.mergeNowOverride.Store(now.UnixNano())
}

func (tst *tsTable) effectiveMergeGraceNs() int64 {
	graceNs := lookupMergeGrace(tst.group)
	if graceNs <= 0 {
		graceNs = int64(tst.option.mergeGraceDefault)
	}
	return graceNs
}

func (tst *tsTable) mergePartsThenSendIntroduction(creator snapshotCreator, parts []*partWrapper, merged map[uint64]struct{}, merges chan *mergerIntroduction,
	closeCh <-chan struct{}, typ string, lane string, ov *mergeOverrides,
) (*partWrapper, error) {
	return tst.mergePartsThenSendIntroductionObserved(creator, parts, merged, merges, closeCh, typ, lane, ov, nil)
}

func (tst *tsTable) mergePartsThenSendIntroductionObserved(creator snapshotCreator, parts []*partWrapper, merged map[uint64]struct{},
	merges chan *mergerIntroduction, closeCh <-chan struct{}, typ string, lane string, ov *mergeOverrides, seed *mergeBenchmarkSeed,
) (mergedPart *partWrapper, resultErr error) {
	reservedSpace := tst.reserveSpace(parts)
	defer releaseDiskSpace(reservedSpace)
	observer := tst.mergeBenchmark.Load()
	if seed != nil {
		observer = seed.observer
	} else {
		seed = observer.seed(tst, parts, typ, lane)
	}
	operation := observer.beginSeed(seed, "")
	var filter *mergeFilter
	var finalizeGenOverride *uint64
	var initialReason mergeSamplingReason
	if ov != nil {
		filter = ov.filter
		finalizeGenOverride = ov.finalizeGen
		if filter == nil {
			initialReason = mergeReasonOther
		}
	} else {
		logicalNow := tst.mergeNow().UnixNano()
		if operation != nil {
			logicalNow = operation.event.LogicalNow
		}
		filter, initialReason = tst.buildHotMergeFilterDecisionAt(parts, logicalNow)
	}
	operation.setInitialReason(initialReason)
	if filter != nil && operation != nil {
		filter.observation = operation.evaluation
		operation.event.EstimatedStagingBytes = filter.estimatedStagingBytes
		operation.event.StagingHardLimit = filter.stagingHardLimit
		operation.event.DecisionBatchLimit = filter.decisionBatchLimit
		operation.event.PlannedStagingBatches = filter.plannedStagingBatches
		operation.event.DecisionMaxTraceCount = filter.maxTraceCount
	}
	losslessRetry := false
	defer func() {
		observer.finish(operation, mergedPart, resultErr, losslessRetry)
	}()
	var guardSession *traceFragmentGuardSession
	if filter != nil && filter.guard != nil {
		guardSession = filter.guard
		defer guardSession.Close()
	}

	result, mergeErr := tst.mergePartsThenIntroduceAttempt(
		creator, parts, merged, merges, closeCh, typ, lane, filter, finalizeGenOverride, operation, 1,
	)
	if mergeErr != nil {
		return nil, mergeErr
	}
	if !result.rejected {
		return result.part, nil
	}

	tst.incPipelineGuardPublicationRejected(1)
	tst.l.Warn().
		Str("group", tst.group).
		Uint32("shard", uint32(tst.shardID)).
		Str("mergeType", typ).
		Str("reason", string(result.revalidation.Reason)).
		Int("recheckedTraces", result.revalidation.RecheckedTraces).
		Int("bloomProbes", result.revalidation.BloomProbes).
		Msg("trace fragment guard rejected filtered output; retrying merge losslessly")
	if guardSession != nil {
		guardSession.Close()
	}
	tst.incPipelineGuardLosslessRetry(1)
	losslessRetry = true
	losslessResult, losslessErr := tst.mergePartsThenIntroduceAttempt(
		creator, parts, merged, merges, closeCh, typ, lane, nil, finalizeGenOverride, operation, 2,
	)
	if losslessErr != nil {
		return nil, losslessErr
	}
	if losslessResult.rejected {
		return nil, fmt.Errorf("lossless trace merge was unexpectedly rejected: %s", losslessResult.revalidation.Reason)
	}
	return losslessResult.part, nil
}

type mergeAttemptResult struct {
	part         *partWrapper
	revalidation traceFragmentGuardRevalidation
	rejected     bool
}

func (tst *tsTable) observeCoreMerge(parts []*partWrapper, newPart *partWrapper, elapsed time.Duration,
	creator snapshotCreator, typ, lane string,
) {
	tst.incTotalMergeLatency(elapsed.Seconds(), typ, lane)
	tst.incTotalMerged(1, typ, lane)
	tst.incTotalMergedParts(len(parts), typ, lane)
	if elapsed > 30*time.Second {
		var totalCount uint64
		for _, partData := range parts {
			totalCount += partData.p.partMetadata.TotalCount
		}
		tst.l.Warn().
			Uint64("beforeTotalCount", totalCount).
			Uint64("afterTotalCount", newPart.p.partMetadata.TotalCount).
			Int("beforePartCount", len(parts)).
			Dur("elapsed", elapsed).
			Msg("background merger takes too long")
		return
	}
	if snapshotCreatorMerger != creator || !tst.l.Info().Enabled() || len(parts) <= 2 {
		return
	}
	var minSize, maxSize, totalSize, totalCount uint64
	for _, partData := range parts {
		metadata := &partData.p.partMetadata
		totalCount += metadata.TotalCount
		totalSize += metadata.CompressedSizeBytes
		if minSize == 0 || minSize > metadata.CompressedSizeBytes {
			minSize = metadata.CompressedSizeBytes
		}
		maxSize = max(maxSize, metadata.CompressedSizeBytes)
	}
	if totalSize <= 10<<20 || minSize*uint64(len(parts)) >= maxSize {
		return
	}
	// An unbalanced merge is acceptable while its total size remains small.
	tst.l.Info().
		Str("beforeTotalCount", humanize.Comma(int64(totalCount))).
		Str("afterTotalCount", humanize.Comma(int64(newPart.p.partMetadata.TotalCount))).
		Int("beforePartCount", len(parts)).
		Str("minSize", humanize.IBytes(minSize)).
		Str("maxSize", humanize.IBytes(maxSize)).
		Dur("elapsedMS", elapsed).
		Msg("background merger merges unbalanced parts")
}

func (tst *tsTable) mergePartsThenIntroduceAttempt(creator snapshotCreator, parts []*partWrapper, merged map[uint64]struct{},
	merges chan *mergerIntroduction, closeCh <-chan struct{}, typ, lane string, filter *mergeFilter, finalizeGenOverride *uint64,
	operation *mergeBenchmarkOperation, attempt uint32,
) (mergeAttemptResult, error) {
	start := time.Now()
	newPartID := atomic.AddUint64(&tst.curPartID, 1)
	newPart, dropped, err := tst.mergeParts(tst.fileSystem, closeCh, parts, newPartID, tst.root, filter, finalizeGenOverride)
	if err != nil {
		return mergeAttemptResult{}, err
	}
	if operation != nil {
		newPart.mergeDepth = operation.event.OutputDepth
	}
	elapsed := time.Since(start)
	tst.observeCoreMerge(parts, newPart, elapsed, creator, typ, lane)
	partIDMap := make(map[uint64]struct{})
	for _, pw := range parts {
		partIDMap[pw.ID()] = struct{}{}
	}
	// When the core merge dropped any trace, prune the same trace ids from every
	// sibling sidx part via an opaque per-element predicate. The trace layer owns
	// the encoding (decodeTraceID); sidx stays encoding-agnostic. Undecodable
	// elements fail open (retain).
	var keepFn func([]byte) bool
	if len(dropped) > 0 {
		keepFn = func(data []byte) bool {
			id, decErr := decodeTraceID(data)
			if decErr != nil {
				return true
			}
			_, isDropped := dropped[id]
			return !isDropped
		}
	}
	mergerIntroductionMap := make(map[string]*sidx.MergerIntroduction)
	sidxInstances := tst.getAllSidx()
	mergeSidx := func(sidxName string, sidxInstance sidx.SIDX) error {
		var childInputBytes, childInputRows uint64
		if operation != nil {
			var totalsErr error
			childInputBytes, childInputRows, totalsErr = benchmarkSidxPartTotals(tst.fileSystem, sidxInstance.PartPaths(partIDMap))
			operation.recordFailure(totalsErr)
		}
		start = time.Now()
		mergerIntroduction, mergeErr := sidxInstance.Merge(closeCh, partIDMap, newPartID, keepFn)
		if mergeErr != nil {
			tst.l.Warn().Err(mergeErr).Msg("sidx merge mem parts failed")
			tst.removeSidxPartOnFailure(sidxName, newPartID)
			tst.removeTracePartOnFailure(newPart)
			for doneSidxName, intro := range mergerIntroductionMap {
				intro.ReleaseNewPart()
				tst.removeSidxPartOnFailure(doneSidxName, newPartID)
				intro.Release()
			}
			return mergeErr
		}
		if mergerIntroduction == nil {
			return nil
		}
		mergerIntroductionMap[sidxName] = mergerIntroduction
		elapsed = time.Since(start)
		if operation != nil {
			childOutputBytes, childOutputRows, totalsErr := benchmarkSidxPartTotals(tst.fileSystem,
				map[uint64]string{newPartID: sidxPartPath(tst.root, sidxName, newPartID)})
			operation.recordFailure(totalsErr)
			operation.recordChild(mergeBenchmarkChild{
				Name: sidxName, InputBytes: childInputBytes, OutputBytes: childOutputBytes, InputRows: childInputRows,
				OutputRows: childOutputRows, ElapsedNanos: elapsed.Nanoseconds(), Attempt: attempt,
			})
		}
		sidxTyp := fmt.Sprintf("%s_%s", typ, sidxName)
		tst.incTotalMergeLatency(elapsed.Seconds(), sidxTyp, lane)
		tst.incTotalMerged(1, sidxTyp, lane)
		tst.incTotalMergedParts(len(parts), sidxTyp, lane)
		if elapsed > 30*time.Second {
			tst.l.Warn().Int("mergedPartsCount", len(parts)).Str("sidxName", sidxName).Dur("elapsed", elapsed).Msg("sidx merge parts took too long")
		}
		return nil
	}
	if operation == nil {
		for sidxName, sidxInstance := range sidxInstances {
			if mergeErr := mergeSidx(sidxName, sidxInstance); mergeErr != nil {
				return mergeAttemptResult{}, mergeErr
			}
		}
	} else {
		sidxNames := make([]string, 0, len(sidxInstances))
		for sidxName := range sidxInstances {
			sidxNames = append(sidxNames, sidxName)
		}
		sort.Strings(sidxNames)
		for _, sidxName := range sidxNames {
			if mergeErr := mergeSidx(sidxName, sidxInstances[sidxName]); mergeErr != nil {
				return mergeAttemptResult{}, mergeErr
			}
		}
	}
	if len(mergerIntroductionMap) > 0 {
		defer func() {
			for _, mergerIntroduction := range mergerIntroductionMap {
				mergerIntroduction.Release()
			}
		}()
	}
	cleanupOutput := func() {
		for sidxName, mergerIntroduction := range mergerIntroductionMap {
			mergerIntroduction.ReleaseNewPart()
			tst.removeSidxPartOnFailure(sidxName, newPartID)
		}
		tst.removeTracePartOnFailure(newPart)
	}

	var revalidation traceFragmentGuardRevalidation
	hasRevalidation := false
	if len(dropped) > 0 && filter != nil && filter.guard != nil {
		revalidation = filter.guard.revalidate(tst)
		hasRevalidation = true
		tst.incPipelineGuardBloomProbes(revalidation.BloomProbes)
		if !revalidation.Publish {
			cleanupOutput()
			return mergeAttemptResult{revalidation: revalidation, rejected: true}, nil
		}
	}

	mi := generateMergerIntroduction()
	defer releaseMergerIntroduction(mi)
	mi.creator = creator
	mi.newPart = newPart
	mi.merged = merged
	mi.sidxMergerIntroduced = mergerIntroductionMap
	if hasRevalidation {
		mi.guard = filter.guard
		mi.guardRevalidation = revalidation
		mi.guardRevalidated = true
	}
	mi.applied = make(chan struct{})
	select {
	case merges <- mi:
	case <-tst.loopCloser.CloseNotify():
		cleanupOutput()
		return mergeAttemptResult{}, errClosed
	}
	<-mi.applied
	if mi.resultErr != nil {
		cleanupOutput()
		if mi.guardRejected {
			return mergeAttemptResult{
				revalidation: mi.guardRevalidation,
				rejected:     true,
			}, nil
		}
		return mergeAttemptResult{}, mi.resultErr
	}
	operation.publishAttempt(attempt)
	return mergeAttemptResult{part: newPart}, nil
}

func (tst *tsTable) freeDiskSpace(path string) uint64 {
	free := tst.fileSystem.MustGetFreeSpace(path)
	reserved := atomic.LoadUint64(&reservedDiskSpace)
	if free < reserved {
		return 0
	}
	return free - reserved
}

func (tst *tsTable) tryReserveDiskSpace(n uint64) bool {
	available := tst.fileSystem.MustGetFreeSpace(tst.root)
	reserved := reserveDiskSpace(n)
	if available > reserved {
		return true
	}
	releaseDiskSpace(n)
	return false
}

func reserveDiskSpace(n uint64) uint64 {
	return atomic.AddUint64(&reservedDiskSpace, n)
}

func releaseDiskSpace(n uint64) {
	atomic.AddUint64(&reservedDiskSpace, ^(n - 1))
}

var reservedDiskSpace uint64

func (tst *tsTable) getPartsToMerge(snapshot *snapshot, freeDiskSize uint64) ([]*partWrapper, map[uint64]struct{}) {
	return tst.getPartsToMergeUpTo(snapshot, freeDiskSize, nil, 0)
}

func (tst *tsTable) getPartsToMergeUpTo(snapshot *snapshot, freeDiskSize uint64, dst []*partWrapper,
	maxPartID uint64,
) ([]*partWrapper, map[uint64]struct{}) {
	var parts []*partWrapper
	// Avoid allocating liveIDs on the hot path when nothing is quarantined.
	sweep := tst.hasQuarantinedParts()
	var liveIDs map[uint64]struct{}
	if sweep {
		liveIDs = make(map[uint64]struct{}, len(snapshot.parts))
	}

	tst.inFlightMu.RLock()
	for _, pw := range snapshot.parts {
		if pw.mp != nil || pw.p.partMetadata.TotalCount < 1 {
			continue
		}
		if sweep {
			liveIDs[pw.ID()] = struct{}{}
		}
		if maxPartID > 0 && pw.ID() > maxPartID {
			continue
		}
		if _, inFlight := tst.inFlight[pw.ID()]; inFlight {
			continue
		}
		if tst.isPartQuarantined(pw.ID()) {
			continue
		}
		parts = append(parts, pw)
	}
	tst.inFlightMu.RUnlock()

	if sweep {
		tst.sweepQuarantine(liveIDs)
	}

	dst = tst.option.mergePolicy.getPartsToMerge(dst, parts, freeDiskSize)
	if len(dst) == 0 {
		return nil, nil
	}

	toBeMerged := make(map[uint64]struct{})
	for _, pw := range dst {
		toBeMerged[pw.ID()] = struct{}{}
	}
	return dst, toBeMerged
}

func (tst *tsTable) reserveSpace(parts []*partWrapper) uint64 {
	var needSize uint64
	for i := range parts {
		needSize += parts[i].p.partMetadata.CompressedSizeBytes
	}
	if tst.tryReserveDiskSpace(needSize) {
		return needSize
	}
	return 0
}

var errNoPartToMerge = fmt.Errorf("no part to merge")

// removeTracePartOnFailure closes the part and removes its directory from disk.
// Used when a merge fails after the trace part was created so the directory is not left as trash.
func (tst *tsTable) removeTracePartOnFailure(pw *partWrapper) {
	if pw == nil {
		return
	}
	pathToRemove := pw.p.path
	pw.decRef()
	tst.fileSystem.MustRMAll(pathToRemove)
}

// sidxPartPath returns the on-disk path for a sidx part (same layout as sidx package).
func sidxPartPath(traceRoot, sidxName string, partID uint64) string {
	return filepath.Join(traceRoot, sidxDirName, sidxName, fmt.Sprintf("%016x", partID))
}

// removeSidxPartOnFailure removes a sidx part directory from disk.
// Used when a merge fails after one or more sidx parts were created.
func (tst *tsTable) removeSidxPartOnFailure(sidxName string, partID uint64) {
	pathToRemove := sidxPartPath(tst.root, sidxName, partID)
	tst.fileSystem.MustRMAll(pathToRemove)
}

func (tst *tsTable) mergeParts(fileSystem fs.FileSystem, closeCh <-chan struct{}, parts []*partWrapper, partID uint64, root string,
	filter *mergeFilter, finalizeGenOverride *uint64,
) (*partWrapper, map[string]struct{}, error) {
	if len(parts) == 0 {
		return nil, nil, errNoPartToMerge
	}
	dstPath := partPath(root, partID)
	var totalSize int64
	var traceSize uint64
	pii := make([]*partMergeIter, 0, len(parts))
	for i := range parts {
		pmi := generatePartMergeIter()
		pmi.mustInitFromPart(parts[i].p)
		pii = append(pii, pmi)
		totalSize += int64(parts[i].p.partMetadata.CompressedSizeBytes)
		traceSize += parts[i].p.partMetadata.BlocksCount
	}
	shouldCache := tst.pm.ShouldCache(totalSize)
	br := generateBlockReader()
	br.init(pii)
	bw := generateBlockWriter()
	outputPublished := false
	bw.mustInitForFilePart(fileSystem, dstPath, shouldCache, int(traceSize))
	// Remove the partially-written output on any failure or panic so failed merges never leak part directories.
	defer func() {
		if outputPublished {
			return
		}
		if panicVal := recover(); panicVal != nil {
			fileSystem.MustRMAll(dstPath)
			panic(panicVal)
		}
		fileSystem.MustRMAll(dstPath)
	}()
	conflictTags := collectConflictTags(parts)

	var minTimestamp, maxTimestamp int64
	for i, pw := range parts {
		pm := pw.p.partMetadata
		if i == 0 {
			minTimestamp = pm.MinTimestamp
			maxTimestamp = pm.MaxTimestamp
			continue
		}
		if pm.MinTimestamp < minTimestamp {
			minTimestamp = pm.MinTimestamp
		}
		if pm.MaxTimestamp > maxTimestamp {
			maxTimestamp = pm.MaxTimestamp
		}
	}

	pm, tf, tt, dropped, err := mergeBlocks(closeCh, bw, br, conflictTags, filter)
	releaseBlockWriter(bw)
	releaseBlockReader(br)
	for i := range pii {
		releasePartMergeIter(pii[i])
	}
	if err != nil {
		return nil, nil, err
	}
	tf.mustWriteTraceIDFilter(fileSystem, dstPath)
	tf.reset()
	tt.mustWriteTagType(fileSystem, dstPath)
	pm.MinTimestamp = minTimestamp
	pm.MaxTimestamp = maxTimestamp
	// Finalization-sampling generation stamp, applied BEFORE the on-disk metadata write
	// (and the subsequent re-open below) so it survives restart. finalizeGenOverride set
	// => this is a finalize round: stamp the output at the round's generation. nil => any
	// other merge (hot/flusher): min-propagate from inputs so a merge is "finalized" only
	// as much as its least-finalized input — merging two G-stamped parts stays G (never
	// un-finalizes), merging a G part with an unstamped late part yields 0 (selectable).
	if finalizeGenOverride != nil {
		pm.FinalizeGen = *finalizeGenOverride
	} else {
		minGen := parts[0].p.partMetadata.FinalizeGen
		for _, pw := range parts[1:] {
			if g := pw.p.partMetadata.FinalizeGen; g < minGen {
				minGen = g
			}
		}
		pm.FinalizeGen = minGen
	}
	pm.mustWriteMetadata(fileSystem, dstPath)
	// No SyncPath here: each mustWrite* helper goes through fileSystem.WriteAtomic
	// which already fsyncs the parent directory after rename. The last atomic
	// metadata write covers all prior dirent changes (data file creations).
	p := mustOpenFilePart(partID, root, fileSystem)
	outputPublished = true
	return newPartWrapper(nil, p), dropped, nil
}

var errClosed = fmt.Errorf("the merger is closed")

// forceSlowMerge is used for testing to disable the fast raw merge path.
var forceSlowMerge = false

func collectConflictTags(parts []*partWrapper) map[string]struct{} {
	tagTypes := make(map[string]map[pbv1.ValueType]struct{})
	for _, pw := range parts {
		for tag, vt := range pw.p.tagType {
			t := decodeTypedTag(tag)
			if tagTypes[t] == nil {
				tagTypes[t] = make(map[pbv1.ValueType]struct{})
			}
			tagTypes[t][vt] = struct{}{}
		}
	}
	var result map[string]struct{}
	for tag, types := range tagTypes {
		if len(types) > 1 {
			if result == nil {
				result = make(map[string]struct{})
			}
			result[tag] = struct{}{}
		}
	}
	return result
}

// stagedTrace holds a deep copy of a trace's block(s) deferred for an ordered
// post-Decide write. When the hook is active EVERY trace is staged (in ascending
// traceID stream order) so the final writes stay ordered. A raw-fast-path
// trace carries its rawBlock pieces; a slow trace carries an allocated
// blockPointer (released after the write decision).
type stagedTrace struct {
	rawTags           map[string][]byte
	rawTagMetadata    map[string][]byte
	slowBlock         *blockPointer
	rawBM             *blockMetadata
	rawArena          *pkgbytes.Buffer
	rawMetadataBlocks *stagedDataBlockBuffer
	traceID           string
	rawSpans          []byte
	isRaw             bool
}

const (
	maxPooledStagedArenaBytes   = 16 << 20
	maxPooledMetadataTags       = 256
	maxPooledRawMetadataObjects = 1024
	maxPooledStagedBlocks       = 64 * 1024
	maxPooledTraceGroups        = defaultMaxStagedTraceCount
	maxPooledEvaluationTraces   = defaultMaxStagedTraceCount
)

type stagedByteArenaPool struct {
	pool *pkgpool.Bounded[*pkgbytes.Buffer]
}

func newStagedByteArenaPool(name string, maxBytes int64) *stagedByteArenaPool {
	return &stagedByteArenaPool{
		pool: pkgpool.RegisterBounded(name, maxBytes, func() *pkgbytes.Buffer {
			return &pkgbytes.Buffer{}
		}, func(arena *pkgbytes.Buffer) int64 {
			return int64(cap(arena.Buf))
		}),
	}
}

func (arenaPool *stagedByteArenaPool) get(size int) *pkgbytes.Buffer {
	arena := arenaPool.pool.Get()
	arena.Buf = pkgbytes.ResizeExact(arena.Buf[:0], size)
	return arena
}

func (arenaPool *stagedByteArenaPool) put(arena *pkgbytes.Buffer) bool {
	if arena == nil {
		return false
	}
	clear(arena.Buf)
	arena.Reset()
	return arenaPool.pool.Put(arena)
}

type stagedDataBlockBuffer struct {
	values []dataBlock
}

type stagedTraceBuffer struct {
	values []stagedTrace
}

func (buffer *stagedTraceBuffer) reset() {
	clear(buffer.values)
	buffer.values = buffer.values[:0]
}

type stagedTraceGroupBuffer struct {
	values []stagedTraceGroup
}

func (buffer *stagedTraceGroupBuffer) reset() {
	clear(buffer.values)
	buffer.values = buffer.values[:0]
}

type stagedEvaluationVectors struct {
	traceBlocks  []sdk.TraceBlock
	traceIDs     []string
	guardRanges  []stagedTraceRange
	decisionMask []bool
}

func (vectors *stagedEvaluationVectors) reset() {
	for traceIdx := range vectors.traceBlocks {
		vectors.traceBlocks[traceIdx] = sdk.TraceBlock{}
	}
	clear(vectors.traceIDs)
	clear(vectors.guardRanges)
	clear(vectors.decisionMask)
	vectors.traceBlocks = vectors.traceBlocks[:0]
	vectors.traceIDs = vectors.traceIDs[:0]
	vectors.guardRanges = vectors.guardRanges[:0]
	vectors.decisionMask = vectors.decisionMask[:0]
}

var (
	stagedByteArenas        = newStagedByteArenaPool("trace-staged-byte-arena", maxPooledStagedArenaBytes)
	stagedBlockMetadataPool = pkgpool.RegisterBounded("trace-staged-block-metadata", maxPooledRawMetadataObjects, func() *blockMetadata {
		return &blockMetadata{}
	}, func(*blockMetadata) int64 {
		return 1
	})
	stagedDataBlockPool = pkgpool.RegisterBounded("trace-staged-data-blocks", maxPooledRawMetadataObjects, func() *stagedDataBlockBuffer {
		return &stagedDataBlockBuffer{}
	}, func(*stagedDataBlockBuffer) int64 {
		return 1
	})
	stagedTraceBufferPool = pkgpool.Register[*stagedTraceBuffer]("trace-staged-traces")
	stagedTraceGroupPool  = pkgpool.Register[*stagedTraceGroupBuffer]("trace-staged-trace-groups")
	stagedEvaluationPool  = pkgpool.Register[*stagedEvaluationVectors]("trace-staged-evaluation")
)

func acquireStagedDataBlockBuffer(size int) *stagedDataBlockBuffer {
	buffer := stagedDataBlockPool.Get()
	if cap(buffer.values) < size {
		buffer.values = make([]dataBlock, size)
	} else {
		buffer.values = buffer.values[:size]
		clear(buffer.values)
	}
	return buffer
}

func releaseStagedDataBlockBuffer(buffer *stagedDataBlockBuffer) {
	if buffer == nil {
		return
	}
	clear(buffer.values)
	buffer.values = buffer.values[:0]
	if cap(buffer.values) > maxPooledMetadataTags {
		stagedDataBlockPool.Discard(buffer)
		return
	}
	stagedDataBlockPool.Put(buffer)
}

func acquireStagedTraceBuffer() *stagedTraceBuffer {
	buffer := stagedTraceBufferPool.Get()
	if buffer == nil {
		return &stagedTraceBuffer{}
	}
	return buffer
}

func releaseStagedTraceBuffer(buffer *stagedTraceBuffer) {
	if buffer == nil {
		return
	}
	buffer.reset()
	if cap(buffer.values) <= maxPooledStagedBlocks {
		stagedTraceBufferPool.Put(buffer)
		return
	}
	stagedTraceBufferPool.Discard(buffer)
}

func acquireStagedTraceGroupBuffer() *stagedTraceGroupBuffer {
	buffer := stagedTraceGroupPool.Get()
	if buffer == nil {
		return &stagedTraceGroupBuffer{}
	}
	return buffer
}

func releaseStagedTraceGroupBuffer(buffer *stagedTraceGroupBuffer) {
	if buffer == nil {
		return
	}
	buffer.reset()
	if cap(buffer.values) <= maxPooledTraceGroups {
		stagedTraceGroupPool.Put(buffer)
		return
	}
	stagedTraceGroupPool.Discard(buffer)
}

func acquireStagedEvaluationVectors(traceCount int, withGuard bool) *stagedEvaluationVectors {
	vectors := stagedEvaluationPool.Get()
	if vectors == nil {
		vectors = &stagedEvaluationVectors{}
	}
	if cap(vectors.traceBlocks) < traceCount {
		vectors.traceBlocks = make([]sdk.TraceBlock, 0, traceCount)
	}
	if cap(vectors.traceIDs) < traceCount {
		vectors.traceIDs = make([]string, 0, traceCount)
	}
	if withGuard && cap(vectors.guardRanges) < traceCount {
		vectors.guardRanges = make([]stagedTraceRange, 0, traceCount)
	}
	if cap(vectors.decisionMask) < traceCount {
		vectors.decisionMask = make([]bool, traceCount)
	} else {
		vectors.decisionMask = vectors.decisionMask[:traceCount]
		clear(vectors.decisionMask)
	}
	return vectors
}

func releaseStagedEvaluationVectors(vectors *stagedEvaluationVectors, reusable bool) bool {
	if vectors == nil {
		return false
	}
	if !reusable {
		stagedEvaluationPool.Discard(vectors)
		return false
	}
	vectors.reset()
	if cap(vectors.traceBlocks) > maxPooledEvaluationTraces || cap(vectors.traceIDs) > maxPooledEvaluationTraces ||
		cap(vectors.guardRanges) > maxPooledEvaluationTraces || cap(vectors.decisionMask) > maxPooledEvaluationTraces {
		stagedEvaluationPool.Discard(vectors)
		return false
	}
	stagedEvaluationPool.Put(vectors)
	return true
}

func acquireStagedByteArena(size int) *pkgbytes.Buffer {
	return stagedByteArenas.get(size)
}

func releaseStagedByteArena(arena *pkgbytes.Buffer) {
	stagedByteArenas.put(arena)
}

func acquireStagedBlockMetadata() *blockMetadata {
	return stagedBlockMetadataPool.Get()
}

func releaseStagedBlockMetadata(metadata *blockMetadata) {
	if metadata == nil {
		return
	}
	oversized := len(metadata.tags) > maxPooledMetadataTags || len(metadata.tagType) > maxPooledMetadataTags
	metadata.reset()
	if oversized {
		stagedBlockMetadataPool.Discard(metadata)
		return
	}
	stagedBlockMetadataPool.Put(metadata)
}

type stagedTraceRange struct {
	start int
	end   int
}

type stagedTraceGroup struct {
	traceID        string
	start          int
	end            int
	minTS          int64
	maxTS          int64
	accountedBytes uint64
	validMetadata  bool
}

// mergeFilter carries the resolved in-merge retention hook state into
// mergeBlocks. When nil, mergeBlocks behaves exactly as before (no staging, no
// decode changes).
type mergeFilter struct {
	chain                 *mergeChain
	guard                 *traceFragmentGuardSession
	observation           *mergeEvaluationObservation
	ctx                   context.Context
	owner                 *tsTable
	timeout               time.Duration
	stagingHardLimit      uint64 // resource-derived safety ceiling for one merge's staged bytes
	decisionBatchLimit    uint64 // metadata-derived preferred bytes per complete-trace Decide batch
	estimatedStagingBytes uint64 // selected-part metadata estimate used to plan decision batches
	plannedStagingBatches uint64 // estimated number of balanced decision batches
	traceBudget           uint64 // hard cap on one trace's staged bytes; exceeding it retains the trace without evaluation (0 disables bypass)
	maxTraceCount         int    // hard cap on logical traces sent to one Decide call (0 disables the count cap)
	forceSlow             bool   // forces the slow assembly path when the chain projects row data
}

func (st *stagedTrace) metadata() *blockMetadata {
	if st.isRaw {
		return st.rawBM
	}
	if st.slowBlock == nil {
		return nil
	}
	return &st.slowBlock.bm
}

func (f *mergeFilter) guardContext() context.Context {
	if f == nil || f.ctx == nil {
		return context.Background()
	}
	return f.ctx
}

// approxBytes estimates the deep-copied heap a staged trace holds so mergeBlocks
// can bound the total staged set rather than holding the whole merge in memory.
func (st *stagedTrace) approxBytes() uint64 {
	n := uint64(len(st.traceID))
	if st.isRaw {
		if st.rawArena != nil {
			n += uint64(cap(st.rawArena.Buf))
		}
		n += approximateStringMapBytes(len(st.rawTags), unsafe.Sizeof([]byte{}))
		for key := range st.rawTags {
			n += uint64(len(key))
		}
		n += approximateStringMapBytes(len(st.rawTagMetadata), unsafe.Sizeof([]byte{}))
		for key := range st.rawTagMetadata {
			n += uint64(len(key))
		}
		if st.rawBM != nil {
			n += st.rawBM.approxBytes()
		}
		if st.rawMetadataBlocks != nil {
			n += uint64(unsafe.Sizeof(stagedDataBlockBuffer{}))
			retainedDescriptors := cap(st.rawMetadataBlocks.values)
			if st.rawBM != nil {
				retainedDescriptors -= len(st.rawBM.tags)
			}
			if retainedDescriptors > 0 {
				n += uint64(retainedDescriptors) * uint64(unsafe.Sizeof(dataBlock{}))
			}
		}
		return n
	}
	if st.slowBlock != nil {
		n += uint64(unsafe.Sizeof(blockPointer{}))
		n += uint64(cap(st.slowBlock.block.spans)) * uint64(unsafe.Sizeof([]byte{}))
		for _, span := range st.slowBlock.block.spans {
			n += uint64(cap(span))
		}
		n += uint64(cap(st.slowBlock.block.spanIDs)) * uint64(unsafe.Sizeof(""))
		for _, spanID := range st.slowBlock.block.spanIDs {
			n += uint64(len(spanID))
		}
		n += uint64(cap(st.slowBlock.block.tags)) * uint64(unsafe.Sizeof(tag{}))
		for tagIdx := range st.slowBlock.block.tags {
			stagedTag := &st.slowBlock.block.tags[tagIdx]
			n += uint64(len(stagedTag.name))
			n += uint64(cap(stagedTag.values)) * uint64(unsafe.Sizeof([]byte{}))
			for _, value := range stagedTag.values {
				n += uint64(cap(value))
			}
		}
		n += st.slowBlock.bm.approxBytes()
	}
	return n
}

func (st *stagedTrace) approxEvaluationBytes(projection sdk.Projection) uint64 {
	if st.slowBlock == nil || !projectionRequiresSlowPath(projection) {
		return 0
	}
	block := &st.slowBlock.block
	n := uint64(unsafe.Sizeof(blockPointer{}))
	n += uint64(len(block.spans)) * uint64(unsafe.Sizeof([]byte{}))
	n += uint64(len(block.spanIDs)) * uint64(unsafe.Sizeof(""))
	n += uint64(len(block.tags)) * uint64(unsafe.Sizeof(tag{}))
	for tagIdx := range block.tags {
		n += uint64(len(block.tags[tagIdx].values)) * uint64(unsafe.Sizeof([]byte{}))
	}
	if projection.Spans {
		n += uint64(len(block.spans)) * uint64(unsafe.Sizeof([]byte{}))
		for _, span := range block.spans {
			n += uint64(len(span))
		}
	}
	if projection.SpanIDs {
		n += uint64(len(block.spanIDs)) * uint64(unsafe.Sizeof(""))
	}
	for _, projectedTag := range projection.Tags {
		for tagIdx := range block.tags {
			stagedTag := &block.tags[tagIdx]
			if decodeTypedTag(stagedTag.name) != projectedTag {
				continue
			}
			n += uint64(unsafe.Sizeof(sdk.TagColumn{}))
			n += uint64(len(stagedTag.values)) * uint64(unsafe.Sizeof([]byte{}))
			for _, value := range stagedTag.values {
				n += uint64(len(value))
			}
			break
		}
	}
	return n
}

func (bm *blockMetadata) approxBytes() uint64 {
	n := uint64(unsafe.Sizeof(dataBlock{}))
	n += approximateStringMapBytes(len(bm.tags), unsafe.Sizeof((*dataBlock)(nil)))
	n += uint64(len(bm.tags)) * uint64(unsafe.Sizeof(dataBlock{}))
	for tagName := range bm.tags {
		n += uint64(len(tagName))
	}
	n += approximateStringMapBytes(len(bm.tagType), unsafe.Sizeof(pbv1.ValueType(0)))
	for tagName := range bm.tagType {
		n += uint64(len(tagName))
	}
	return n
}

func approximateStringMapBytes(length int, valueBytes uintptr) uint64 {
	if length == 0 {
		return 0
	}
	const (
		mapHeaderBytes = 48
		mapBucketSlots = 8
	)
	bucketCount := 1
	for bucketCount*mapBucketSlots < length {
		bucketCount *= 2
	}
	bucketBytes := uintptr(16) + mapBucketSlots*(unsafe.Sizeof("")+valueBytes)
	return mapHeaderBytes + uint64(bucketCount)*uint64(bucketBytes)
}

func stagedBatchFixedBytes() uint64 {
	return uint64(unsafe.Sizeof(stagedEvaluationBatch{})) + approximateStringMapBytes(1, unsafe.Sizeof(struct{}{}))
}

func stagedTraceDecisionBytes() uint64 {
	decisionBytes := uint64(unsafe.Sizeof(false))
	// Reserve the exact-drop lookup entry that may be created after Decide.
	decisionBytes += uint64(unsafe.Sizeof("")) + uint64(unsafe.Sizeof(struct{}{}))
	return decisionBytes
}

func stagedEvaluationSlotBytes(hasGuard bool) uint64 {
	// Evaluation vectors allocate exactly one slot per logical trace group.
	slotBytes := uint64(unsafe.Sizeof("")) + uint64(unsafe.Sizeof(sdk.TraceBlock{}))
	if hasGuard {
		slotBytes += uint64(unsafe.Sizeof(stagedTraceRange{}))
	}
	return slotBytes
}

// isMergeHot reports true when any part being merged contains data written
// within graceNs of now. A hot merge means some traces may still have in-flight
// spans arriving in newer parts, so the caller should skip filter evaluation.
func isMergeHot(parts []*partWrapper, graceNs int64, now int64) bool {
	if graceNs < 0 {
		return true
	}
	maturityFrontier := traceFragmentSaturatingSub(now, graceNs)
	for _, pw := range parts {
		if pw.p.partMetadata.MaxTimestamp > maturityFrontier {
			return true
		}
	}
	return false
}

// stageRawTrace deep-copies the shared rawBlk into a stagedTrace so the next
// mustReadRaw may overwrite rawBlk without corrupting the staged copy.
func stageRawTrace(rawBlk *rawBlock) stagedTrace {
	st := stagedTrace{
		isRaw:             true,
		traceID:           rawBlk.bm.traceID,
		rawBM:             acquireStagedBlockMetadata(),
		rawMetadataBlocks: acquireStagedDataBlockBuffer(len(rawBlk.bm.tags)),
	}
	copyStagedBlockMetadata(st.rawBM, rawBlk.bm, st.rawMetadataBlocks.values)
	arenaSize := len(rawBlk.spans)
	for _, value := range rawBlk.tags {
		arenaSize += len(value)
	}
	for _, value := range rawBlk.tagMetadata {
		arenaSize += len(value)
	}
	st.rawArena = acquireStagedByteArena(arenaSize)
	offset := 0
	copyIntoArena := func(source []byte) []byte {
		if len(source) == 0 {
			return nil
		}
		target := st.rawArena.Buf[offset : offset+len(source)]
		copy(target, source)
		offset += len(source)
		return target
	}
	st.rawSpans = copyIntoArena(rawBlk.spans)
	if len(rawBlk.tags) > 0 {
		st.rawTags = make(map[string][]byte, len(rawBlk.tags))
		for key, value := range rawBlk.tags {
			st.rawTags[key] = copyIntoArena(value)
		}
	}
	if len(rawBlk.tagMetadata) > 0 {
		st.rawTagMetadata = make(map[string][]byte, len(rawBlk.tagMetadata))
		for key, value := range rawBlk.tagMetadata {
			st.rawTagMetadata[key] = copyIntoArena(value)
		}
	}
	return st
}

func copyStagedBlockMetadata(dst, src *blockMetadata, tagBlocks []dataBlock) {
	dst.traceID = src.traceID
	dst.uncompressedSpanSizeBytes = src.uncompressedSpanSizeBytes
	dst.count = src.count
	if dst.spans == nil {
		dst.spans = &dataBlock{}
	}
	dst.spans.copyFrom(src.spans)
	dst.timestamps.copyFrom(&src.timestamps)
	if len(src.tags) > 0 && dst.tags == nil {
		dst.tags = make(map[string]*dataBlock, len(src.tags))
	}
	tagIdx := 0
	for tagName, sourceBlock := range src.tags {
		targetBlock := &tagBlocks[tagIdx]
		targetBlock.copyFrom(sourceBlock)
		dst.tags[tagName] = targetBlock
		tagIdx++
	}
	if len(src.tagType) > 0 && dst.tagType == nil {
		dst.tagType = make(map[string]pbv1.ValueType, len(src.tagType))
	}
	for tagName, valueType := range src.tagType {
		dst.tagType[tagName] = valueType
	}
}

// writeStagedKeep persists a kept staged trace from its own deep-copied bytes.
func writeStagedKeep(bw *blockWriter, st *stagedTrace) {
	if st.isRaw {
		rawBlk := rawBlock{
			bm:          st.rawBM,
			tags:        st.rawTags,
			tagMetadata: st.rawTagMetadata,
			spans:       st.rawSpans,
		}
		bw.mustWriteRawBlock(&rawBlk)
		return
	}
	bw.mustWriteBlock(st.traceID, &st.slowBlock.block)
}

func releaseStagedTrace(st *stagedTrace) {
	if st.isRaw {
		releaseStagedBlockMetadata(st.rawBM)
		releaseStagedDataBlockBuffer(st.rawMetadataBlocks)
		releaseStagedByteArena(st.rawArena)
		st.rawBM = nil
		st.rawMetadataBlocks = nil
		st.rawArena = nil
		st.rawSpans = nil
		st.rawTags = nil
		st.rawTagMetadata = nil
		return
	}
	if st.slowBlock == nil {
		return
	}
	releaseBlockPointer(st.slowBlock)
	st.slowBlock = nil
}

type stagedEvaluationBatch struct {
	vectors *stagedEvaluationVectors
}

func assembleStagedEvaluationBatch(filter *mergeFilter, staged []stagedTrace, groups []stagedTraceGroup) (stagedEvaluationBatch, bool) {
	vectors := acquireStagedEvaluationVectors(len(groups), filter.guard != nil)
	assembledBatch := stagedEvaluationBatch{vectors: vectors}
	expectedStart := 0
	for groupIdx := range groups {
		group := &groups[groupIdx]
		if group.traceID == "" || group.start != expectedStart || group.end <= group.start || group.end > len(staged) ||
			group.minTS > group.maxTS || groupIdx > 0 && group.traceID <= groups[groupIdx-1].traceID {
			releaseStagedEvaluationVectors(vectors, true)
			return stagedEvaluationBatch{}, false
		}
		stagedTraceBlock, assembled := assembleStagedTraceBlock(*group, staged[group.start:group.end], filter.chain.projection)
		if assembled {
			vectors.traceIDs = append(vectors.traceIDs, group.traceID)
			vectors.traceBlocks = append(vectors.traceBlocks, stagedTraceBlock)
			if filter.guard != nil {
				vectors.guardRanges = append(vectors.guardRanges, stagedTraceRange{start: group.start, end: group.end})
			}
		}
		expectedStart = group.end
	}
	if expectedStart != len(staged) {
		releaseStagedEvaluationVectors(vectors, true)
		return stagedEvaluationBatch{}, false
	}
	return assembledBatch, true
}

func resolveStagedDrops(filter *mergeFilter, staged []stagedTrace, assembledBatch stagedEvaluationBatch) (map[string]struct{}, bool) {
	if len(assembledBatch.vectors.traceBlocks) == 0 {
		return nil, true
	}
	traceBatch := sdk.TraceBatch{Traces: assembledBatch.vectors.traceBlocks}
	verdict, reusable, execErr := filter.chain.executeObservedInto(
		&traceBatch, filter.timeout, filter.observation, assembledBatch.vectors.decisionMask,
	)
	keepMask := verdict.Keep
	if execErr != nil || len(keepMask) != len(assembledBatch.vectors.traceIDs) {
		if filter.observation != nil {
			filter.observation.retained.Add(uint64(len(assembledBatch.vectors.traceIDs)))
		}
		if filter.owner != nil {
			filter.owner.incPipelinePluginErrors(1, "decide_failed_open")
		}
		return nil, reusable
	}
	if filter.owner != nil {
		filter.owner.incPipelineTracesEvaluated(len(assembledBatch.vectors.traceIDs))
	}
	var dropMature map[string]struct{}
	for traceIdx, traceID := range assembledBatch.vectors.traceIDs {
		if keepMask[traceIdx] {
			if filter.observation != nil {
				filter.observation.retained.Add(1)
			}
			if filter.owner != nil {
				filter.owner.incPipelineTracesRetained(1)
			}
			continue
		}
		if filter.guard == nil {
			if dropMature == nil {
				dropMature = make(map[string]struct{})
			}
			dropMature[traceID] = struct{}{}
			if filter.observation != nil {
				filter.observation.dropped.Add(1)
			}
			if filter.owner != nil {
				filter.owner.incPipelineTracesDropped(1)
			}
			continue
		}
		guardRange := assembledBatch.vectors.guardRanges[traceIdx]
		guardTrace := assembleTraceFragmentGuardTrace(traceID, staged[guardRange.start:guardRange.end])
		decision := filter.guard.guard.Resolve(filter.guardContext(), guardTrace, traceFragmentSamplerActionDrop)
		if filter.owner != nil {
			filter.owner.incPipelineGuardBloomProbes(decision.BloomProbes)
		}
		if decision.Action == traceFragmentGuardActionDrop && decision.ConfirmedDrop != nil {
			if dropMature == nil {
				dropMature = make(map[string]struct{})
			}
			dropMature[traceID] = struct{}{}
			if filter.observation != nil {
				filter.observation.dropped.Add(1)
			}
			if filter.owner != nil {
				filter.owner.incPipelineTracesDropped(1)
			}
			continue
		}
		if filter.owner != nil {
			filter.owner.incPipelineTracesRetained(1)
			filter.owner.incPipelineGuardDeferred(1)
			if decision.Reason == traceFragmentGuardReasonBudgetExhausted {
				filter.owner.incPipelineGuardBudgetExhausted(1)
			}
		}
		if filter.observation != nil {
			filter.observation.retained.Add(1)
		}
	}
	return dropMature, reusable
}

// flushStaged evaluates staged traces and writes them in ascending trace-ID order.
// Chain failures retain the whole batch, and allocated slow blocks are released.
func flushStaged(bw *blockWriter, filter *mergeFilter, staged []stagedTrace, groups []stagedTraceGroup, validBatch bool,
	droppedSet map[string]struct{},
) {
	if len(staged) == 0 {
		return
	}
	// Build one Decide entry per trace ID. A trace split across physical blocks is
	// assembled completely, decided once, and dropped as a unit.
	assembledBatch, validOrder := assembleStagedEvaluationBatch(filter, staged, groups)
	var dropMature map[string]struct{}
	reusable := true
	if validBatch && validOrder {
		dropMature, reusable = resolveStagedDrops(filter, staged, assembledBatch)
	}
	for i := range staged {
		if _, isDropped := dropMature[staged[i].traceID]; isDropped {
			droppedSet[staged[i].traceID] = struct{}{}
		} else {
			writeStagedKeep(bw, &staged[i])
		}
		releaseStagedTrace(&staged[i])
	}
	if validOrder {
		releaseStagedEvaluationVectors(assembledBatch.vectors, reusable)
	}
}

// rawFastPathEligible reports whether the current block can be copied raw
// (without unmarshaling): raw merge is not force-disabled, the active retention
// filter does not require the slow tag-projecting path, and this is the only
// block for its traceID (the next block, if any, has a different traceID).
func rawFastPathEligible(filter *mergeFilter, nextB, b *blockPointer) bool {
	if forceSlowMerge {
		return false
	}
	if filter != nil && filter.forceSlow {
		return false
	}
	return nextB == nil || nextB.bm.traceID != b.bm.traceID
}

type traceEvaluationStager struct {
	bw                *blockWriter
	filter            *mergeFilter
	droppedSet        map[string]struct{}
	traceBuffer       *stagedTraceBuffer
	groupBuffer       *stagedTraceGroupBuffer
	lastStagedTraceID string
	bypassedTraceID   string
	staged            []stagedTrace
	groups            []stagedTraceGroup
	stagedBytes       uint64
	currentTraceBytes uint64
	currentTraceStart int
	stagedTraceCount  int
	invalidOrder      bool
	invalidMetadata   bool
}

func (tes *traceEvaluationStager) ensureBuffers() {
	if tes.traceBuffer == nil {
		tes.traceBuffer = acquireStagedTraceBuffer()
		tes.staged = tes.traceBuffer.values
	}
	if tes.groupBuffer == nil {
		tes.groupBuffer = acquireStagedTraceGroupBuffer()
		tes.groups = tes.groupBuffer.values
	}
}

func (tes *traceEvaluationStager) releaseBuffers() {
	if tes.filter != nil && tes.filter.observation != nil {
		tes.filter.observation.observeStagedBytes(0)
	}
	for stagedIdx := range tes.staged {
		releaseStagedTrace(&tes.staged[stagedIdx])
	}
	if tes.traceBuffer != nil {
		tes.traceBuffer.values = tes.staged
		releaseStagedTraceBuffer(tes.traceBuffer)
	}
	if tes.groupBuffer != nil {
		tes.groupBuffer.values = tes.groups
		releaseStagedTraceGroupBuffer(tes.groupBuffer)
	}
	tes.traceBuffer = nil
	tes.groupBuffer = nil
	tes.staged = nil
	tes.groups = nil
}

func (tes *traceEvaluationStager) flush(reason mergeStagingFlushReason) {
	if len(tes.staged) == 0 {
		return
	}
	if tes.filter.observation != nil {
		tes.filter.observation.recordStagingBatch(reason, tes.stagedBytes, uint64(len(tes.groups)))
	}
	flushStaged(tes.bw, tes.filter, tes.staged, tes.groups, !tes.invalidOrder && !tes.invalidMetadata, tes.droppedSet)
	clear(tes.staged)
	tes.staged = tes.staged[:0]
	clear(tes.groups)
	tes.groups = tes.groups[:0]
	tes.traceBuffer.values = tes.staged
	tes.groupBuffer.values = tes.groups
	tes.stagedBytes = 0
	tes.currentTraceBytes = 0
	tes.currentTraceStart = 0
	tes.stagedTraceCount = 0
	tes.invalidOrder = false
	tes.invalidMetadata = false
	if tes.filter.observation != nil {
		tes.filter.observation.observeStagedBytes(0)
	}
}

func (tes *traceEvaluationStager) writeBypassed(bypassed []stagedTrace) {
	for stagedIdx := range bypassed {
		writeStagedKeep(tes.bw, &bypassed[stagedIdx])
		releaseStagedTrace(&bypassed[stagedIdx])
	}
}

func (tes *traceEvaluationStager) stage(st stagedTrace) {
	tes.ensureBuffers()
	if tes.bypassedTraceID != "" {
		if st.traceID == tes.bypassedTraceID {
			writeStagedKeep(tes.bw, &st)
			releaseStagedTrace(&st)
			tes.lastStagedTraceID = ""
			return
		}
		tes.bypassedTraceID = ""
	}
	startsTrace := len(tes.staged) == 0 || st.traceID != tes.lastStagedTraceID
	var groupSliceGrowthBytes uint64
	if startsTrace {
		tes.currentTraceStart = len(tes.staged)
		tes.currentTraceBytes = 0
		tes.stagedTraceCount++
		if len(tes.groups) > 0 && st.traceID <= tes.groups[len(tes.groups)-1].traceID {
			tes.invalidOrder = true
		}
		previousGroupCapacity := cap(tes.groups)
		tes.groups = append(tes.groups, stagedTraceGroup{
			traceID: st.traceID, start: len(tes.staged), end: len(tes.staged), validMetadata: true,
		})
		tes.groupBuffer.values = tes.groups
		groupSliceGrowthBytes = uint64(cap(tes.groups)-previousGroupCapacity) * uint64(unsafe.Sizeof(stagedTraceGroup{}))
	}
	stagedTraceBytes := st.approxBytes()
	if tes.filter.chain != nil {
		stagedTraceBytes += st.approxEvaluationBytes(tes.filter.chain.projection)
	}
	previousCapacity := cap(tes.staged)
	tes.staged = append(tes.staged, st)
	tes.traceBuffer.values = tes.staged
	sliceGrowthBytes := uint64(cap(tes.staged)-previousCapacity) * uint64(unsafe.Sizeof(stagedTrace{}))
	batchBytes := stagedTraceBytes + sliceGrowthBytes
	traceBytes := stagedTraceBytes + uint64(unsafe.Sizeof(stagedTrace{}))
	if startsTrace {
		traceDecisionBytes := stagedTraceDecisionBytes() + stagedEvaluationSlotBytes(tes.filter.guard != nil)
		batchBytes += groupSliceGrowthBytes + traceDecisionBytes
		traceBytes += uint64(unsafe.Sizeof(stagedTraceGroup{})) + traceDecisionBytes
		if len(tes.staged) == 1 {
			batchBytes += stagedBatchFixedBytes()
		}
	}
	tes.stagedBytes += batchBytes
	tes.currentTraceBytes += traceBytes
	if tes.filter.observation != nil {
		tes.filter.observation.observeStagedBytes(tes.stagedBytes)
	}
	group := &tes.groups[len(tes.groups)-1]
	group.end = len(tes.staged)
	group.accountedBytes = tes.currentTraceBytes
	metadata := st.metadata()
	switch {
	case metadata == nil || metadata.traceID != st.traceID || !metadata.timestamps.known || metadata.timestamps.min > metadata.timestamps.max:
		tes.invalidMetadata = true
		group.validMetadata = false
	case group.end-group.start == 1:
		group.minTS = metadata.timestamps.min
		group.maxTS = metadata.timestamps.max
	default:
		group.minTS = min(group.minTS, metadata.timestamps.min)
		group.maxTS = max(group.maxTS, metadata.timestamps.max)
	}
	tes.lastStagedTraceID = st.traceID
	if tes.filter.traceBudget == 0 || tes.currentTraceBytes <= tes.filter.traceBudget {
		return
	}
	if tes.currentTraceStart > 0 {
		if tes.filter.observation != nil {
			prefixBytes := stagedBatchFixedBytes()
			for groupIdx := range tes.groups[:len(tes.groups)-1] {
				prefixBytes += tes.groups[groupIdx].accountedBytes
			}
			tes.filter.observation.recordStagingBatch(mergeStagingFlushOversizedTrace, prefixBytes, uint64(len(tes.groups)-1))
		}
		flushStaged(tes.bw, tes.filter, tes.staged[:tes.currentTraceStart], tes.groups[:len(tes.groups)-1],
			!tes.invalidOrder && !tes.invalidMetadata, tes.droppedSet)
	}
	tes.writeBypassed(tes.staged[tes.currentTraceStart:])
	tes.bypassedTraceID = st.traceID
	tes.lastStagedTraceID = ""
	clear(tes.staged)
	tes.staged = tes.staged[:0]
	clear(tes.groups)
	tes.groups = tes.groups[:0]
	tes.traceBuffer.values = tes.staged
	tes.groupBuffer.values = tes.groups
	tes.stagedBytes = 0
	tes.currentTraceBytes = 0
	tes.currentTraceStart = 0
	tes.stagedTraceCount = 0
	tes.invalidOrder = false
	tes.invalidMetadata = false
	if tes.filter.observation != nil {
		tes.filter.observation.observeStagedBytes(0)
	}
	if tes.filter.owner != nil {
		tes.filter.owner.incPipelineOversizedTracesBypassed(1)
	}
	if tes.filter.observation != nil {
		tes.filter.observation.oversized.Add(1)
	}
}

func (tes *traceEvaluationStager) flushBefore(nextTraceID string, pendingBlock *blockPointer, pendingBlockIsEmpty bool) {
	if !tes.batchBudgetReached() || len(tes.staged) == 0 || nextTraceID == tes.lastStagedTraceID {
		return
	}
	pendingCompletesStagedTrace := !pendingBlockIsEmpty && pendingBlock.bm.traceID == tes.lastStagedTraceID
	if !pendingCompletesStagedTrace {
		tes.flush(tes.budgetFlushReason())
	}
}

func (tes *traceEvaluationStager) flushAfter(completedTraceID, nextTraceID string) {
	if tes.batchBudgetReached() && completedTraceID != nextTraceID {
		tes.flush(tes.budgetFlushReason())
	}
}

func (tes *traceEvaluationStager) budgetFlushReason() mergeStagingFlushReason {
	bytesReached := tes.filter.decisionBatchLimit > 0 && tes.stagedBytes >= tes.filter.decisionBatchLimit
	tracesReached := tes.filter.maxTraceCount > 0 && tes.stagedTraceCount >= tes.filter.maxTraceCount
	switch {
	case bytesReached && tracesReached:
		return mergeStagingFlushByteAndTraceLimit
	case bytesReached:
		return mergeStagingFlushByteLimit
	default:
		return mergeStagingFlushTraceLimit
	}
}

func (tes *traceEvaluationStager) batchBudgetReached() bool {
	bytesReached := tes.filter.decisionBatchLimit > 0 && tes.stagedBytes >= tes.filter.decisionBatchLimit
	tracesReached := tes.filter.maxTraceCount > 0 && tes.stagedTraceCount >= tes.filter.maxTraceCount
	return bytesReached || tracesReached
}

func mergeBlocks(closeCh <-chan struct{}, bw *blockWriter, br *blockReader, conflictTags map[string]struct{},
	filter *mergeFilter,
) (*partMetadata, *traceIDFilter, *tagType, map[string]struct{}, error) {
	pendingBlockIsEmpty := true
	pendingBlock := generateBlockPointer()
	defer releaseBlockPointer(pendingBlock)
	var tmpBlock *blockPointer
	var decoder *encoding.BytesBlockDecoder
	var rawBlk rawBlock
	getDecoder := func() *encoding.BytesBlockDecoder {
		if decoder == nil {
			decoder = generateColumnValuesDecoder()
		}
		return decoder
	}
	releaseDecoder := func() {
		if decoder != nil {
			releaseColumnValuesDecoder(decoder)
			decoder = nil
		}
	}
	loadAndRename := func() {
		br.loadBlockData(getDecoder())
		renameConflictTags(&br.block.block, conflictTags)
	}
	readAndRename := func(bm *blockMetadata) {
		br.mustReadRaw(&rawBlk, bm)
		renameRawConflictTags(&rawBlk, conflictTags)
	}
	var droppedSet map[string]struct{}
	var evaluationStager *traceEvaluationStager
	if filter != nil {
		droppedSet = make(map[string]struct{})
		evaluationStager = &traceEvaluationStager{
			bw:         bw,
			filter:     filter,
			droppedSet: droppedSet,
		}
		defer evaluationStager.releaseBuffers()
	}
	// writeRawBlock writes the just-read rawBlk. When the hook is inactive it
	// writes immediately (byte-identical to the legacy path). When active the
	// trace is staged (in ascending traceID stream order) for an ordered,
	// post-Decide flush. Staging keeps the final block writes in ascending
	// traceID order (mustWriteBlock requires it); the chunked flush bounds heap.
	writeRawBlock := func() {
		if filter == nil {
			bw.mustWriteRawBlock(&rawBlk)
			return
		}
		evaluationStager.stage(stageRawTrace(&rawBlk))
	}
	// writeSlowBlock writes (or, on the active hook path, stages) an accumulated
	// slow-path block.
	writeSlowBlock := func(bp *blockPointer) {
		if filter == nil {
			bw.mustWriteBlock(bp.bm.traceID, &bp.block)
			return
		}
		newBP := generateBlockPointer()
		newBP.copyFrom(bp)
		// copyFrom copies slice headers only; the bytes alias the decoder's
		// internal buffer which is reset when the next trace is loaded.
		newBP.block.deepCopyValues()
		evaluationStager.stage(stagedTrace{
			traceID:   bp.bm.traceID,
			slowBlock: newBP,
		})
	}
	for br.nextBlockMetadata() {
		select {
		case <-closeCh:
			return nil, nil, nil, nil, errClosed
		default:
		}
		b := br.block
		// Bounded staging: at a trace boundary (the current block belongs to a
		// different traceID than the last staged one), once staged bytes exceed
		// the budget, decide+write the accumulated chunk before reading further.
		// A pending output block may still complete the last staged trace even
		// after the source reader advances to a new trace ID. Do not flush until
		// that pending block is staged. The separate per-trace budget fails open
		// and streams an oversized trace without sending a partial trace to Decide.
		if evaluationStager != nil {
			evaluationStager.flushBefore(b.bm.traceID, pendingBlock, pendingBlockIsEmpty)
		}
		// Fast path: if this is the only block for this traceID AND we have no pending block,
		// copy it raw without unmarshaling
		nextB := br.peek()
		if pendingBlockIsEmpty && rawFastPathEligible(filter, nextB, b) {
			// fast path: only a single block for the trace id and no pending data
			readAndRename(&b.bm)
			writeRawBlock()
			continue
		}

		if pendingBlockIsEmpty {
			loadAndRename()
			pendingBlock.copyFrom(b)
			pendingBlockIsEmpty = false
			continue
		}

		if pendingBlock.bm.traceID != b.bm.traceID || pendingBlock.block.spanSize() >= maxUncompressedSpanSize {
			pendingTraceID := pendingBlock.bm.traceID
			writeSlowBlock(pendingBlock)
			releaseDecoder()
			pendingBlock.reset()
			pendingBlockIsEmpty = true
			if evaluationStager != nil {
				evaluationStager.flushAfter(pendingTraceID, b.bm.traceID)
			}
			// After writing the pending block, check if the new block can be copied raw
			// This is the same fast path check as at the beginning of the loop
			nextB = br.peek()
			if rawFastPathEligible(filter, nextB, b) {
				// fast path: only a single block for this new trace id
				readAndRename(&b.bm)
				writeRawBlock()
				continue
			}
			// Slow path: start accumulating the new block
			loadAndRename()
			pendingBlock.copyFrom(b)
			pendingBlockIsEmpty = false
			continue
		}

		if tmpBlock == nil {
			tmpBlock = generateBlockPointer()
			defer releaseBlockPointer(tmpBlock)
		}
		tmpBlock.reset()
		tmpBlock.bm.traceID = b.bm.traceID
		loadAndRename()
		mergeTwoBlocks(tmpBlock, pendingBlock, b)
		if tmpBlock.block.spanSize() <= maxUncompressedSpanSize {
			if len(tmpBlock.spans) == 0 {
				pendingBlockIsEmpty = true
			}
			pendingBlock, tmpBlock = tmpBlock, pendingBlock
			continue
		}
		writeSlowBlock(tmpBlock)
		releaseDecoder()
		pendingBlock.reset()
		tmpBlock.reset()
		pendingBlockIsEmpty = true
	}
	if err := br.error(); err != nil {
		return nil, nil, nil, nil, fmt.Errorf("cannot read block to merge: %w", err)
	}
	if !pendingBlockIsEmpty {
		writeSlowBlock(pendingBlock)
	}
	releaseDecoder()
	if evaluationStager != nil {
		evaluationStager.flush(mergeStagingFlushEndOfMerge)
	}
	var pm partMetadata
	var tf traceIDFilter
	tt := make(tagType)
	bw.Flush(&pm, &tf, &tt)
	if len(droppedSet) == 0 {
		droppedSet = nil
	}
	return &pm, &tf, &tt, droppedSet, nil
}

func mergeTwoBlocks(target, left, right *blockPointer) {
	leftBoundsKnown := len(left.spans) == 0 || left.bm.timestamps.known && !left.block.timestampBoundsUnknown
	rightBoundsKnown := len(right.spans) == 0 || right.bm.timestamps.known && !right.block.timestampBoundsUnknown
	target.appendAll(left)
	target.appendAll(right)
	target.block.timestampBoundsUnknown = len(target.spans) > 0 && (!leftBoundsKnown || !rightBoundsKnown)
	target.bm.timestamps.known = len(target.spans) > 0 && !target.block.timestampBoundsUnknown
	if target.bm.timestamps.known {
		target.bm.timestamps.min = target.block.minTS
		target.bm.timestamps.max = target.block.maxTS
	}
}

func renameConflictTags(b *block, conflictTags map[string]struct{}) {
	if len(conflictTags) == 0 {
		return
	}
	for i := range b.tags {
		if _, ok := conflictTags[b.tags[i].name]; ok {
			b.tags[i].name = encodeTypedTag(b.tags[i].name, b.tags[i].valueType)
		}
	}
}

func renameRawConflictTags(r *rawBlock, conflictTags map[string]struct{}) {
	if len(conflictTags) == 0 {
		return
	}
	bm := r.bm
	for tag := range conflictTags {
		if _, ok := bm.tags[tag]; !ok {
			continue
		}
		valueType := bm.tagType[tag]
		typedTag := encodeTypedTag(tag, valueType)
		bm.tags[typedTag] = bm.tags[tag]
		delete(bm.tags, tag)
		bm.tagType[typedTag] = valueType
		delete(bm.tagType, tag)
		if rawData, ok := r.tags[tag]; ok {
			r.tags[typedTag] = rawData
			delete(r.tags, tag)
		}
		if rawMeta, ok := r.tagMetadata[tag]; ok {
			r.tagMetadata[typedTag] = rawMeta
			delete(r.tagMetadata, tag)
		}
	}
}
