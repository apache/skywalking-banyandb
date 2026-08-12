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
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/internal/sidx"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk"
	"github.com/apache/skywalking-banyandb/pkg/run"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

// ceilingTestBudget is a drop-set budget chosen so the ceiling this file's merges
// derive is exactly 2: two entries' price, plus one byte so the division cannot
// reach three. ceilingTestIDBodyBytes is the allocation class the short "trace-x"
// IDs below land in — allocClassBytes rounds any length up to 16 to 16 — and the
// expression mirrors dropSetBytesPerEntry, which cannot be called in a const.
// Lengthening those IDs past 16 bytes would move them into the next class and
// change the derived ceiling.
const (
	ceilingTestIDBodyBytes = 16
	ceilingTestBudget      = 2*(dropSetEntryHeaderBytes+ceilingTestIDBodyBytes+dropSetEntrySlotBytes) + 1
)

// idSet builds a drop-everything set for fakeSampler from a slice of trace IDs.
func idSet(ids []string) map[string]struct{} {
	set := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		set[id] = struct{}{}
	}
	return set
}

// buildCeilingTestParts writes one single-trace file part per id and returns
// their wrappers, ready to merge.
func buildCeilingTestParts(t *testing.T, fileSystem fs.FileSystem, tmpPath string, ids []string) []*partWrapper {
	t.Helper()
	trs := singleTraceParts(ids)
	parts := make([]*partWrapper, 0, len(trs))
	// Start part IDs well above curPartID's zero value so the merge output's
	// newPartID (AddUint64(&tst.curPartID, 1), i.e. 1 for a fresh table) never
	// collides with an input part's on-disk directory.
	const partIDBase = uint64(100)
	for i, tr := range trs {
		partID := partIDBase + uint64(i)
		mp := generateMemPart()
		mp.mustInitFromTraces(tr)
		mp.mustFlush(fileSystem, partPath(tmpPath, partID))
		p := mustOpenFilePart(partID, tmpPath, fileSystem)
		parts = append(parts, newPartWrapper(nil, p))
		releaseMemPart(mp)
	}
	return parts
}

// newCeilingTestTable builds a minimal *tsTable suitable for driving a real
// mergePartsThenSendIntroduction call, with an optional sidx map, and no
// running background loops.
func newCeilingTestTable(t *testing.T, fileSystem fs.FileSystem, tmpPath string, sidxMap map[string]sidx.SIDX) *tsTable {
	t.Helper()
	closer := run.NewCloser(1)
	t.Cleanup(closer.Done)
	return &tsTable{
		pm:         protector.Nop{},
		fileSystem: fileSystem,
		root:       tmpPath,
		loopCloser: closer,
		l:          logger.GetLogger("drop-set-ceiling-test"),
		sidxMap:    sidxMap,
	}
}

// runCeilingMerge drives one real merge through mergePartsThenSendIntroduction
// with a mock introducer that applies every introduction immediately, and
// returns the published output part (ref=1, caller must decRef).
func runCeilingMerge(t *testing.T, tst *tsTable, parts []*partWrapper, filter *mergeFilter) *partWrapper {
	t.Helper()
	merged := make(map[uint64]struct{}, len(parts))
	for _, pw := range parts {
		merged[pw.ID()] = struct{}{}
	}
	merges := make(chan *mergerIntroduction, 1)
	introducerDone := make(chan struct{})
	go func() {
		defer close(introducerDone)
		for mi := range merges {
			close(mi.applied)
		}
	}()
	closeCh := make(chan struct{})
	newPart, err := tst.mergePartsThenSendIntroduction(
		snapshotCreatorMerger, parts, merged, merges, closeCh,
		mergeTypeFile, mergeLaneFast, &mergeOverrides{filter: filter},
	)
	require.NoError(t, err)
	close(merges)
	<-introducerDone
	return newPart
}

// readPartTraceIDs reads back the ascending trace-ID sequence a merged part
// was written with.
func readPartTraceIDs(t *testing.T, pw *partWrapper) []string {
	t.Helper()
	iter := generatePartMergeIter()
	iter.mustInitFromPart(pw.p)
	reader := generateBlockReader()
	reader.init([]*partMergeIter{iter})
	var ids []string
	for reader.nextBlockMetadata() {
		ids = append(ids, reader.block.bm.traceID)
	}
	require.NoError(t, reader.error())
	releaseBlockReader(reader)
	releasePartMergeIter(iter)
	return ids
}

// recordingFakeSIDX is a sidx.SIDX whose Merge applies the caller-supplied
// keepFn to a fixed candidate element set and records the survivors, so a
// test can assert the drop set pruned SIDX-side elements exactly.
type recordingFakeSIDX struct {
	fakeSIDX
	elements  []string
	survivors []string
}

func (f *recordingFakeSIDX) Merge(_ <-chan struct{}, _ map[uint64]struct{}, _ uint64, keepFn func([]byte) bool) (*sidx.MergerIntroduction, error) {
	f.survivors = f.survivors[:0]
	for _, elem := range f.elements {
		encoded := append([]byte{byte(idFormatV1)}, elem...)
		if keepFn == nil || keepFn(encoded) {
			f.survivors = append(f.survivors, elem)
		}
	}
	return nil, nil
}

// recordingTraceFragmentGuard is a traceFragmentGuard test double that
// confirms every Resolve call as a drop and records call counts, so a test
// can assert the ceiling skips it for ceiling-retained traces while still
// exercising the real guard-revalidation call site.
type recordingTraceFragmentGuard struct {
	resolveCalls    int
	revalidateCalls int
	bloomProbes     int
}

func (g *recordingTraceFragmentGuard) Resolve(_ context.Context, trace traceFragmentGuardTrace, _ traceFragmentSamplerAction) traceFragmentGuardDecision {
	g.resolveCalls++
	g.bloomProbes++
	return traceFragmentGuardDecision{
		Action:        traceFragmentGuardActionDrop,
		ConfirmedDrop: &traceFragmentGuardConfirmedDrop{TraceID: trace.TraceID},
		BloomProbes:   1,
	}
}

func (g *recordingTraceFragmentGuard) RevalidateDrops(context.Context, traceFragmentGuardRevalidationRequest) traceFragmentGuardRevalidation {
	g.revalidateCalls++
	return traceFragmentGuardRevalidation{Publish: true}
}

func (*recordingTraceFragmentGuard) Close() {}

// rejectingTraceFragmentGuard confirms every proposed drop but refuses to publish
// the filtered output, which is what forces the merge onto its lossless retry.
type rejectingTraceFragmentGuard struct {
	revalidateCalls int
}

func (*rejectingTraceFragmentGuard) Resolve(_ context.Context, trace traceFragmentGuardTrace, _ traceFragmentSamplerAction) traceFragmentGuardDecision {
	return traceFragmentGuardDecision{
		Action:        traceFragmentGuardActionDrop,
		ConfirmedDrop: &traceFragmentGuardConfirmedDrop{TraceID: trace.TraceID},
		BloomProbes:   1,
	}
}

func (g *rejectingTraceFragmentGuard) RevalidateDrops(context.Context, traceFragmentGuardRevalidationRequest) traceFragmentGuardRevalidation {
	g.revalidateCalls++
	return traceFragmentGuardRevalidation{Publish: false, Reason: traceFragmentGuardReasonBudgetExhausted}
}

func (*rejectingTraceFragmentGuard) Close() {}

// TestMergeCeilingPrunesSidxExactly forces a two-entry ceiling on a merge
// whose sampler proposes five drops, and asserts strict set equality — in
// both directions — between each sidx instance's surviving element set and
// the output part's trace-ID set: an orphan or a missing entry either way
// fails the test.
func TestMergeCeilingPrunesSidxExactly(t *testing.T) {
	tmpPath, defFn := test.Space(require.New(t))
	defer defFn()
	fileSystem := fs.NewLocalFileSystem()

	ids := []string{"trace-a", "trace-b", "trace-c", "trace-d", "trace-e"}
	parts := buildCeilingTestParts(t, fileSystem, tmpPath, ids)
	defer func() {
		for _, pw := range parts {
			pw.decRef()
		}
	}()

	latency := &recordingFakeSIDX{elements: append([]string(nil), ids...)}
	startTime := &recordingFakeSIDX{elements: append([]string(nil), ids...)}
	tst := newCeilingTestTable(t, fileSystem, tmpPath, map[string]sidx.SIDX{
		"latency": latency, "start_time": startTime,
	})

	chain := newMergeChain("g", "s", []sdk.Sampler{&fakeSampler{dropIDs: idSet(ids)}}, 0)
	filter := &mergeFilter{chain: chain, timeout: time.Second, owner: tst, budget: ceilingTestBudget}
	defer filter.chain.close()

	newPart := runCeilingMerge(t, tst, parts, filter)
	defer newPart.decRef()

	outputIDs := readPartTraceIDs(t, newPart)
	require.ElementsMatch(t, []string{"trace-c", "trace-d", "trace-e"}, outputIDs)

	require.ElementsMatch(t, outputIDs, latency.survivors, "latency sidx must retain exactly the output part's trace IDs")
	require.ElementsMatch(t, outputIDs, startTime.survivors, "start_time sidx must retain exactly the output part's trace IDs")
}

// TestMergeCeilingRetainsTheAscendingTail pins the ordering bias of spec
// section 4 as observed behavior: a capped merge spares the
// lexicographically largest of the proposed drops, because core merge
// proposes drops in ascending trace-ID order.
func TestMergeCeilingRetainsTheAscendingTail(t *testing.T) {
	tmpPath, defFn := test.Space(require.New(t))
	defer defFn()
	fileSystem := fs.NewLocalFileSystem()

	ids := []string{"trace-a", "trace-b", "trace-c", "trace-d", "trace-e"}
	parts := buildCeilingTestParts(t, fileSystem, tmpPath, ids)
	defer func() {
		for _, pw := range parts {
			pw.decRef()
		}
	}()
	tst := newCeilingTestTable(t, fileSystem, tmpPath, nil)

	chain := newMergeChain("g", "s", []sdk.Sampler{&fakeSampler{dropIDs: idSet(ids)}}, 0)
	filter := &mergeFilter{chain: chain, timeout: time.Second, owner: tst, budget: ceilingTestBudget}
	defer filter.chain.close()

	newPart := runCeilingMerge(t, tst, parts, filter)
	defer newPart.decRef()

	// The block reader yields blocks in the same ascending order they were
	// written, so an exact (ordered) comparison also confirms writeStagedKeep
	// preserved ascending order for the spared tail.
	require.Equal(t, []string{"trace-c", "trace-d", "trace-e"}, readPartTraceIDs(t, newPart))
}

// TestMergeCeilingCounterAccounting asserts retained-by-ceiling equals the
// proposed drops beyond the ceiling, and that total retained (visible via the
// output part) equals verdict-retained plus ceiling-retained.
func TestMergeCeilingCounterAccounting(t *testing.T) {
	tmpPath, defFn := test.Space(require.New(t))
	defer defFn()
	fileSystem := fs.NewLocalFileSystem()

	// The sampler drops a,b,c,d (four proposed drops) but verdict-retains e.
	// With a two-entry ceiling: a,b are dropped for real; c,d are
	// ceiling-retained; e is verdict-retained. Total retained (3) = 1
	// verdict-retained + 2 ceiling-retained.
	ids := []string{"trace-a", "trace-b", "trace-c", "trace-d", "trace-e"}
	proposedDrops := []string{"trace-a", "trace-b", "trace-c", "trace-d"}
	parts := buildCeilingTestParts(t, fileSystem, tmpPath, ids)
	defer func() {
		for _, pw := range parts {
			pw.decRef()
		}
	}()
	tst := newCeilingTestTable(t, fileSystem, tmpPath, nil)

	chain := newMergeChain("g", "s", []sdk.Sampler{&fakeSampler{dropIDs: idSet(proposedDrops)}}, 0)
	filter := &mergeFilter{chain: chain, timeout: time.Second, owner: tst, budget: ceilingTestBudget}
	defer filter.chain.close()

	newPart := runCeilingMerge(t, tst, parts, filter)
	defer newPart.decRef()

	outputIDs := readPartTraceIDs(t, newPart)
	require.ElementsMatch(t, []string{"trace-c", "trace-d", "trace-e"}, outputIDs)
	require.Equal(t, uint64(2), filter.retainedByCeiling.Load(), "retained-by-ceiling must equal the proposed drops beyond the ceiling")
	require.Len(t, outputIDs, 3, "total retained must equal verdict-retained (1) plus ceiling-retained (2)")
}

// TestMergeCeilingSkipsGuard asserts the guard's Resolve call count equals the
// number of pre-ceiling proposed drops: the guard is never consulted for a
// trace that will be retained by the ceiling.
func TestMergeCeilingSkipsGuard(t *testing.T) {
	tmpPath, defFn := test.Space(require.New(t))
	defer defFn()
	fileSystem := fs.NewLocalFileSystem()

	ids := []string{"trace-a", "trace-b", "trace-c", "trace-d", "trace-e"}
	parts := buildCeilingTestParts(t, fileSystem, tmpPath, ids)
	defer func() {
		for _, pw := range parts {
			pw.decRef()
		}
	}()
	tst := newCeilingTestTable(t, fileSystem, tmpPath, nil)

	guard := &recordingTraceFragmentGuard{}
	chain := newMergeChain("g", "s", []sdk.Sampler{&fakeSampler{dropIDs: idSet(ids)}}, 0)
	filter := &mergeFilter{
		chain: chain, timeout: time.Second, owner: tst, budget: ceilingTestBudget,
		guard: &traceFragmentGuardSession{guard: guard},
	}
	defer filter.chain.close()

	newPart := runCeilingMerge(t, tst, parts, filter)
	defer newPart.decRef()

	require.Equal(t, 2, guard.resolveCalls, "the guard must only be consulted for the pre-ceiling proposed drops")
	require.Equal(t, 2, guard.bloomProbes)
}

// TestMergeCeilingStillRevalidatesGuard asserts a capped merge that dropped at
// least one trace still runs the guard's revalidation — the regression the
// rejected bloom-release design (spec section 8) would have introduced.
func TestMergeCeilingStillRevalidatesGuard(t *testing.T) {
	tmpPath, defFn := test.Space(require.New(t))
	defer defFn()
	fileSystem := fs.NewLocalFileSystem()

	ids := []string{"trace-a", "trace-b", "trace-c", "trace-d", "trace-e"}
	parts := buildCeilingTestParts(t, fileSystem, tmpPath, ids)
	defer func() {
		for _, pw := range parts {
			pw.decRef()
		}
	}()
	tst := newCeilingTestTable(t, fileSystem, tmpPath, nil)

	guard := &recordingTraceFragmentGuard{}
	chain := newMergeChain("g", "s", []sdk.Sampler{&fakeSampler{dropIDs: idSet(ids)}}, 0)
	filter := &mergeFilter{
		chain: chain, timeout: time.Second, owner: tst, budget: ceilingTestBudget,
		guard: &traceFragmentGuardSession{guard: guard},
	}
	defer filter.chain.close()

	newPart := runCeilingMerge(t, tst, parts, filter)
	defer newPart.decRef()

	require.Equal(t, 1, guard.revalidateCalls, "a capped merge that dropped at least one trace must still revalidate")
}

// TestProductionBudgetMergeReachesNoCeiling asserts an ordinary-sized merge
// carrying the real resolved production budget records every proposed drop and
// retains nothing by ceiling. Before DS-5 this test asserted the budget==0
// (unlimited) sentinel directly; now that both construction sites resolve a real
// budget it pins two things instead: that the resolved value is the one under
// test, and that the ceiling it derives leaves substantial headroom. Asserting
// the derived ceiling — not just "this merge did not cap" — is what keeps the
// test from passing for any budget above a few kilobytes, so shrinking
// defaultDropSetBudget to a value that would cap real merges fails here.
func TestProductionBudgetMergeReachesNoCeiling(t *testing.T) {
	const (
		dropCount = 150
		// The production default must admit far more than a single merge's worth of
		// drops; well below the ~246k the 16MiB fallback derives for these IDs, but
		// high enough that any meaningful shrink of the constant trips it.
		minProductionCeiling = 100_000
	)
	ids := make([]string, dropCount)
	for i := range ids {
		ids[i] = fmtTraceID(i)
	}

	budget := resolveDropSetBudget(option{})
	require.NotZero(t, budget, "a production merge must resolve a real ceiling, not the unlimited sentinel")
	derivedCeiling := maxIDsForBudget(budget, len(ids[0]))
	require.GreaterOrEqual(t, derivedCeiling, minProductionCeiling,
		"the production default budget must leave headroom for an ordinary merge")
	require.Greater(t, derivedCeiling, dropCount, "this merge must sit below the derived ceiling by construction")

	filter := &mergeFilter{chain: newTestChain(idSet(ids)), timeout: time.Second, budget: budget}
	defer filter.chain.close()

	_, dropped := mergeWithFilter(t, singleTraceParts(ids), filter)

	require.Len(t, dropped, dropCount, "every proposed drop must be recorded below the ceiling")
	require.Zero(t, filter.retainedByCeiling.Load())
}

// TestLosslessRetryClearsCeilingReporting asserts a capped attempt that the guard
// rejects does not leave ceiling telemetry behind: the retry publishes an
// unfiltered output that drops nothing, so reporting a ceiling for it would tell
// an operator to raise memory over a merge that never hit one. Observability is
// the only signal this design has for under-deletion (spec section 6), so it has
// to describe the output that was actually published.
func TestLosslessRetryClearsCeilingReporting(t *testing.T) {
	tmpPath, defFn := test.Space(require.New(t))
	defer defFn()
	fileSystem := fs.NewLocalFileSystem()

	ids := []string{"trace-a", "trace-b", "trace-c", "trace-d", "trace-e"}
	tst := newCeilingTestTable(t, fileSystem, tmpPath, nil)
	parts := buildCeilingTestParts(t, fileSystem, tmpPath, ids)

	// A guard that confirms drops but refuses publication forces the lossless
	// retry, which re-runs the merge with no filter at all.
	guard := &rejectingTraceFragmentGuard{}
	chain := newTestChain(idSet(ids))
	filter := &mergeFilter{
		chain: chain, timeout: time.Second, owner: tst, budget: ceilingTestBudget,
		guard: &traceFragmentGuardSession{guard: guard},
	}
	defer chain.close()

	newPart := runCeilingMerge(t, tst, parts, filter)
	if newPart != nil {
		defer newPart.decRef()
	}

	require.Positive(t, guard.revalidateCalls, "the rejecting guard must have forced the lossless retry")
	require.Zero(t, filter.retainedByCeiling.Load(),
		"a discarded capped attempt must not leave ceiling retentions behind for the published output")
	require.ElementsMatch(t, ids, readPartTraceIDs(t, newPart),
		"the lossless retry publishes every trace, so nothing was dropped or ceiling-retained")
}

// TestLargeForcedCeilingMergeBoundsDropSetResidency is an in-process check —
// NOT the containerized, resource-limited controlled run DS-5's exit gate
// calls for. It drives one real merge over a data set two orders of
// magnitude larger than this file's other ceiling tests (300 proposed drops
// versus 5) through a forced ceiling, and asserts the number of traces
// actually recorded as dropped stops exactly at maxIDsForBudget's derived
// cap: never more, confirming residency never exceeds the resolved budget,
// and never less, confirming the ceiling does not retain early.
func TestLargeForcedCeilingMergeBoundsDropSetResidency(t *testing.T) {
	const (
		dropCount   = 300
		wantCeiling = 60
	)
	ids := make([]string, dropCount)
	for i := range ids {
		ids[i] = fmtTraceID(i)
	}
	price := dropSetBytesPerEntry(len(ids[0]))
	budget := uint64(wantCeiling)*uint64(price) + 1
	require.Equal(t, wantCeiling, maxIDsForBudget(budget, len(ids[0])), "the chosen budget must derive exactly the target ceiling")

	filter := &mergeFilter{chain: newTestChain(idSet(ids)), timeout: time.Second, budget: budget}
	defer filter.chain.close()

	retained, dropped := mergeWithFilter(t, singleTraceParts(ids), filter)

	require.Len(t, dropped, wantCeiling, "drop-set residency must stop exactly at the derived ceiling, not beyond it")
	require.Len(t, retained, dropCount-wantCeiling, "everything past the ceiling must be retained instead of dropped")
	require.Equal(t, uint64(dropCount-wantCeiling), filter.retainedByCeiling.Load())
}

// fmtTraceID builds an ascending, zero-padded trace ID so a large synthetic
// drop set stays in the ascending order add() requires.
func fmtTraceID(i int) string {
	const digits = "0123456789abcdef"
	buf := make([]byte, 0, 16)
	buf = append(buf, "trace-"...)
	for shift := 28; shift >= 0; shift -= 4 {
		buf = append(buf, digits[(i>>shift)&0xf])
	}
	return string(buf)
}

// TestCappedFinalizeRoundLeavesStateUnchanged asserts a capped finalize round
// writes the same finalize.json fields a normal round does: generation
// advanced, counter reset, no new fields, FinalizeRounds incremented once.
// Guards spec section 11 invariant 7 that finalize scheduling is untouched.
func TestCappedFinalizeRoundLeavesStateUnchanged(t *testing.T) {
	tmpPath, defFn := test.Space(require.New(t))
	defer defFn()
	fileSystem := fs.NewLocalFileSystem()

	tst, err := newTSTable(
		fileSystem, tmpPath,
		common.Position{Database: "capped-finalize"},
		logger.GetLogger("capped-finalize-test"),
		timestamp.NewInclusiveTimeRange(time.Unix(-1, 0), time.Unix(1, 0)),
		option{
			flushTimeout:        0,
			mergePolicy:         newDefaultMergePolicyForTesting(),
			protector:           protector.Nop{},
			decideTimeout:       time.Second,
			mergeGraceDefault:   time.Millisecond,
			maxTraceFragmentGap: time.Nanosecond,
		},
		nil,
	)
	require.NoError(t, err)
	defer tst.Close()

	ids := []string{"trace-a", "trace-b", "trace-c", "trace-d", "trace-e"}
	tst.mustAddTraces(tracesWithIDs(ids...), nil)
	require.Eventually(t, func() bool {
		s := tst.currentSnapshot()
		if s == nil {
			return false
		}
		defer s.decRef()
		for _, pw := range s.parts {
			if pw.mp == nil && pw.p.partMetadata.TotalCount > 0 {
				return true
			}
		}
		return false
	}, 10*time.Second, 20*time.Millisecond, "flushed file part must appear")
	require.Equal(t, uint64(len(ids)), snapshotTotalCount(tst))

	testDropSetBudgetOverride = ceilingTestBudget
	t.Cleanup(func() { testDropSetBudgetOverride = 0 })

	// dropPrefixSampler{prefix: ""} proposes every trace ID as a drop.
	finalized, roundErr := tst.runFinalizeRound([]sdk.Sampler{dropPrefixSampler{prefix: ""}}, int64(time.Millisecond))
	require.NoError(t, roundErr)
	require.True(t, finalized, "a round must have committed even though it was capped")

	// Two of five dropped for real, three ceiling-retained: the round did
	// less deletion than an uncapped round would have, but it still ran.
	require.Equal(t, uint64(3), snapshotTotalCount(tst), "ceiling-retained traces must survive the round")
	require.Equal(t, uint64(1), tst.finalizeGenCached.Load(), "generation must still advance to 1")
	require.Zero(t, tst.unsampledBytes.Load(), "counter must still reset after the round")

	st := readFinalizeState(fileSystem, tmpPath)
	require.Equal(t, uint64(1), st.FinalizeGeneration, "persisted generation must be 1, same shape as an uncapped round")
	require.Equal(t, 1, st.FinalizeRounds, "persisted round count must be incremented exactly once")
	require.NotEmpty(t, st.LastFinalizedAt)
}

// TestCappedFinalizeRoundWarnsOnce asserts exactly one warning is logged for a
// capped finalize round, and none for an uncapped one.
func TestCappedFinalizeRoundWarnsOnce(t *testing.T) {
	newTable := func(t *testing.T, name string, buf *bytes.Buffer) *tsTable {
		t.Helper()
		tmpPath, defFn := test.Space(require.New(t))
		t.Cleanup(defFn)
		fileSystem := fs.NewLocalFileSystem()
		zl := zerolog.New(buf)
		tst, err := newTSTable(
			fileSystem, tmpPath,
			common.Position{Database: name},
			&logger.Logger{Logger: &zl},
			timestamp.NewInclusiveTimeRange(time.Unix(-1, 0), time.Unix(1, 0)),
			option{
				flushTimeout:        0,
				mergePolicy:         newDefaultMergePolicyForTesting(),
				protector:           protector.Nop{},
				decideTimeout:       time.Second,
				mergeGraceDefault:   time.Millisecond,
				maxTraceFragmentGap: time.Nanosecond,
			},
			nil,
		)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, tst.Close()) })
		return tst
	}
	waitForFlush := func(t *testing.T, tst *tsTable) {
		t.Helper()
		require.Eventually(t, func() bool {
			s := tst.currentSnapshot()
			if s == nil {
				return false
			}
			defer s.decRef()
			for _, pw := range s.parts {
				if pw.mp == nil && pw.p.partMetadata.TotalCount > 0 {
					return true
				}
			}
			return false
		}, 10*time.Second, 20*time.Millisecond, "flushed file part must appear")
	}
	ids := []string{"trace-a", "trace-b", "trace-c", "trace-d", "trace-e"}

	t.Run("capped", func(t *testing.T) {
		var buf bytes.Buffer
		tst := newTable(t, "capped-warn", &buf)
		tst.mustAddTraces(tracesWithIDs(ids...), nil)
		waitForFlush(t, tst)

		testDropSetBudgetOverride = ceilingTestBudget
		t.Cleanup(func() { testDropSetBudgetOverride = 0 })

		finalized, roundErr := tst.runFinalizeRound([]sdk.Sampler{dropPrefixSampler{prefix: ""}}, int64(time.Millisecond))
		require.NoError(t, roundErr)
		require.True(t, finalized)

		warnCount := bytes.Count(buf.Bytes(), []byte("drop-set ceiling"))
		require.Equal(t, 1, warnCount, "exactly one warning must be logged for a capped round")
	})

	t.Run("uncapped", func(t *testing.T) {
		var buf bytes.Buffer
		tst := newTable(t, "uncapped-warn", &buf)
		tst.mustAddTraces(tracesWithIDs(ids...), nil)
		waitForFlush(t, tst)

		finalized, roundErr := tst.runFinalizeRound([]sdk.Sampler{dropPrefixSampler{prefix: ""}}, int64(time.Millisecond))
		require.NoError(t, roundErr)
		require.True(t, finalized)

		warnCount := bytes.Count(buf.Bytes(), []byte("drop-set ceiling"))
		require.Zero(t, warnCount, "an uncapped round must not warn")
	})
}

// TestBenchmarkEventCarriesDropSetCeiling forces a ceiling and asserts the
// benchmark event's resolved budget, capped flag, and retained-by-ceiling
// count are populated and consistent with each other.
func TestBenchmarkEventCarriesDropSetCeiling(t *testing.T) {
	tmpPath, defFn := test.Space(require.New(t))
	defer defFn()
	fileSystem := fs.NewLocalFileSystem()

	ids := []string{"trace-a", "trace-b", "trace-c", "trace-d", "trace-e"}
	parts := buildCeilingTestParts(t, fileSystem, tmpPath, ids)
	defer func() {
		for _, pw := range parts {
			pw.decRef()
		}
	}()
	tst := newCeilingTestTable(t, fileSystem, tmpPath, nil)
	observer := newMergeBenchmarkObserver(nil, mergeBenchmarkObserverOptions{})
	require.True(t, tst.setMergeBenchmarkObserver(observer))

	chain := newMergeChain("g", "s", []sdk.Sampler{&fakeSampler{dropIDs: idSet(ids)}}, 0)
	filter := &mergeFilter{chain: chain, timeout: time.Second, owner: tst, budget: ceilingTestBudget}
	defer filter.chain.close()

	newPart := runCeilingMerge(t, tst, parts, filter)
	defer newPart.decRef()

	snapshot := observer.snapshot()
	require.Len(t, snapshot.Events, 1)
	event := snapshot.Events[0]
	require.Equal(t, uint64(ceilingTestBudget), event.DropSetBudget)
	require.True(t, event.DropSetCapped)
	require.Equal(t, filter.retainedByCeiling.Load(), event.TracesRetainedByCeiling)
	require.Positive(t, event.TracesRetainedByCeiling)
}

// TestFiltersCarryResolvedDropSetBudget is DS-5's activation test: both
// mergeFilter construction sites — the hot filter (buildHotMergeFilterDecisionAt)
// and the finalize filter (runFinalizeRound) — must carry resolveDropSetBudget's
// value, and that value must be identical at both sites. The equality assertion
// is deliberate: it is what fails if a later change reintroduces a lane-specific
// budget without updating spec section 3.4.
func TestFiltersCarryResolvedDropSetBudget(t *testing.T) {
	const group = "ds5-filters-carry-budget"
	resetRegistries()
	t.Cleanup(resetRegistries)

	// A fixed, non-default, non-override protector limit makes it evident that
	// the value observed at both sites is really resolveDropSetBudget's output,
	// not a coincidental match with some other constant.
	opt := option{
		flushTimeout:          0,
		mergePolicy:           newDefaultMergePolicyForTesting(),
		protector:             fixedLimitProtector{limit: 4 << 30},
		decideTimeout:         time.Second,
		mergeGraceDefault:     time.Millisecond,
		maxTraceFragmentGap:   time.Microsecond,
		nativePipelineEnabled: true,
	}
	tmpPath, cleanupFn := test.Space(require.New(t))
	t.Cleanup(cleanupFn)
	fileSystem := fs.NewLocalFileSystem()
	tst, tableErr := newTSTable(
		fileSystem, tmpPath,
		common.Position{Database: group},
		logger.GetLogger(group),
		timestamp.NewInclusiveTimeRange(time.Unix(-1, 0), time.Unix(1, 0)),
		opt, nil,
	)
	require.NoError(t, tableErr)
	t.Cleanup(func() { require.NoError(t, tst.Close()) })

	deregister := registerSampler(group, &durationEnvelopeSampler{})
	t.Cleanup(deregister)
	setMergeEventForGroup(group, true)
	t.Cleanup(func() { setMergeEventForGroup(group, false) })

	tst.mustAddTraces(tracesWithIDs("trace-a", "trace-b"), nil)
	parts := waitForImplementationFileParts(t, tst, 1)
	defer releaseImplementationParts(parts)

	want := resolveDropSetBudget(tst.option)
	require.NotZero(t, want, "the resolved budget must be a real ceiling once DS-5 activates it")

	// Hot construction site.
	hotFilter, reason := tst.buildHotMergeFilterDecision(parts)
	require.NotNil(t, hotFilter, "hot filter must build; reason: %v", reason)
	defer hotFilter.chain.close()
	require.Equal(t, want, hotFilter.budget, "hot filter must carry resolveDropSetBudget's value")

	// Finalize construction site: the filter is local to runFinalizeRound, so
	// capture its budget the same way production observability does — through
	// the benchmark event, which mergePartsThenSendIntroductionObserved
	// populates from filter.budget before any decision is made.
	observer := newMergeBenchmarkObserver(nil, mergeBenchmarkObserverOptions{})
	require.True(t, tst.setMergeBenchmarkObserver(observer))
	finalized, roundErr := tst.runFinalizeRound([]sdk.Sampler{dropPrefixSampler{prefix: "nonexistent-"}}, int64(time.Millisecond))
	require.NoError(t, roundErr)
	require.True(t, finalized, "the finalize round must commit")

	snapshot := observer.snapshot()
	require.Len(t, snapshot.Events, 1)
	require.Equal(t, want, snapshot.Events[0].DropSetBudget, "finalize filter must carry resolveDropSetBudget's value")

	require.Equal(t, hotFilter.budget, snapshot.Events[0].DropSetBudget,
		"hot and finalize construction sites must carry the identical resolved budget (spec section 3.4)")
}
