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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk"
	"github.com/apache/skywalking-banyandb/pkg/run"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

// dropOneSampler is a minimal deterministic sampler that drops exactly one trace id.
type dropOneSampler struct{ dropID string }

func (dropOneSampler) Kind() sdk.Kind          { return sdk.KindSampler }
func (dropOneSampler) Project() sdk.Projection { return sdk.Projection{} }
func (dropOneSampler) Close() error            { return nil }
func (s dropOneSampler) Decide(b *sdk.TraceBatch) (sdk.Verdict, error) {
	keep := make([]bool, len(b.Traces))
	for i := range b.Traces {
		keep[i] = b.Traces[i].TraceID != s.dropID
	}
	return sdk.Verdict{Keep: keep}, nil
}

// dropPrefixSampler drops every trace whose id starts with the prefix. Used to prove
// MERGE and FINALIZE compose: the same registered sampler runs in both events.
type dropPrefixSampler struct{ prefix string }

func (dropPrefixSampler) Kind() sdk.Kind          { return sdk.KindSampler }
func (dropPrefixSampler) Project() sdk.Projection { return sdk.Projection{} }
func (dropPrefixSampler) Close() error            { return nil }
func (s dropPrefixSampler) Decide(b *sdk.TraceBatch) (sdk.Verdict, error) {
	keep := make([]bool, len(b.Traces))
	for i := range b.Traces {
		keep[i] = !strings.HasPrefix(b.Traces[i].TraceID, s.prefix)
	}
	return sdk.Verdict{Keep: keep}, nil
}

// tracesWithIDs builds a minimal traces fixture (one ancient-timestamp span per id).
func tracesWithIDs(ids ...string) *traces {
	ts := &traces{}
	for _, id := range ids {
		ts.traceIDs = append(ts.traceIDs, id)
		ts.timestamps = append(ts.timestamps, int64(1))
		ts.tags = append(ts.tags, []*tagValue{{tag: "t", valueType: pbv1.ValueTypeStr, value: []byte("v")}})
		ts.spans = append(ts.spans, []byte("span-"+id))
		ts.spanIDs = append(ts.spanIDs, "span-"+id)
	}
	return ts
}

func snapshotTotalCount(tst *tsTable) uint64 {
	s := tst.currentSnapshot()
	if s == nil {
		return 0
	}
	defer s.decRef()
	var total uint64
	for _, pw := range s.parts {
		if pw.mp == nil {
			total += pw.p.partMetadata.TotalCount
		}
	}
	return total
}

func oneTraceFixture(id string, ts int64) *traces {
	return &traces{
		traceIDs:   []string{id},
		timestamps: []int64{ts},
		tags: [][]*tagValue{
			{{tag: "tag1", valueType: pbv1.ValueTypeStr, value: []byte("v"), valueArr: nil}},
		},
		spans:   [][]byte{[]byte("span-" + id)},
		spanIDs: []string{"span-" + id},
	}
}

// TestMergeParts_FinalizeGenPropagation covers the four DD1.C1 cases for the finalize
// generation stamp threaded through mergeParts. Each output is re-read from disk
// (mustOpenFilePart -> mustReadMetadata) to assert the ON-DISK value survives, i.e.
// the stamp is written before pm.mustWriteMetadata (crash-safe across a restart).
func TestMergeParts_FinalizeGenPropagation(t *testing.T) {
	tmpPath, defFn := test.Space(require.New(t))
	defer defFn()
	fileSystem := fs.NewLocalFileSystem()
	tst := &tsTable{pm: protector.Nop{}, fileSystem: fileSystem, root: tmpPath}

	var nextID uint64
	// flushPart writes a single-trace part and returns its wrapper with FinalizeGen set
	// in-memory to gen (mergeParts min-propagates from the in-memory input value).
	flushPart := func(id string, gen uint64) *partWrapper {
		nextID++
		pid := nextID
		mp := generateMemPart()
		mp.mustInitFromTraces(oneTraceFixture(id, int64(pid)))
		mp.mustFlush(fileSystem, partPath(tmpPath, pid))
		releaseMemPart(mp)
		p := mustOpenFilePart(pid, tmpPath, fileSystem)
		p.partMetadata.ID = pid
		p.partMetadata.FinalizeGen = gen
		return newPartWrapper(nil, p)
	}
	// mergeAndReadGen merges parts with the given override, then re-opens the output
	// part from disk and returns its persisted FinalizeGen.
	mergeAndReadGen := func(parts []*partWrapper, override *uint64) uint64 {
		nextID++
		outID := nextID
		closeCh := make(chan struct{})
		out, _, err := tst.mergeParts(fileSystem, closeCh, parts, outID, tmpPath, nil, override)
		close(closeCh)
		require.NoError(t, err)
		require.NotNil(t, out)
		gotID := out.ID()
		out.decRef()
		for _, pw := range parts {
			pw.decRef()
		}
		reopened := mustOpenFilePart(gotID, tmpPath, fileSystem)
		return reopened.partMetadata.FinalizeGen
	}

	// (a) hot merge (nil override) of two gen-G parts => output gen G (min), so a hot
	// merge inside a finalized segment does NOT un-finalize.
	assert.Equal(t, uint64(3), mergeAndReadGen([]*partWrapper{flushPart("a1", 3), flushPart("a2", 3)}, nil),
		"two gen-3 parts must merge to gen 3 (min)")

	// (b) hot merge of a gen-G part + an unstamped (0) late part => output 0 (min), so
	// the merged part is selectable by the next finalize round.
	assert.Equal(t, uint64(0), mergeAndReadGen([]*partWrapper{flushPart("b1", 5), flushPart("b2", 0)}, nil),
		"gen-5 + unstamped must merge to gen 0 (min)")

	// (c) a finalize round (override=&Gnew) stamps the output at Gnew on disk; the
	// re-open proves the value is persisted (crash-safe), and Gnew < FinalizeGeneration
	// would be false on replay so it is not re-selected.
	gnew := uint64(7)
	assert.Equal(t, uint64(7), mergeAndReadGen([]*partWrapper{flushPart("c1", 0), flushPart("c2", 0)}, &gnew),
		"finalize override must stamp Gnew on disk")

	// (d) a lone unstamped part stays 0 under nil override.
	assert.Equal(t, uint64(0), mergeAndReadGen([]*partWrapper{flushPart("d1", 0)}, nil),
		"a lone unstamped part must stay gen 0")
}

// TestRunFinalizeRound_DropsAndStamps drives a full finalize round on a running
// tsTable: writes traces, waits for the flush, then finalizes through a drop-one
// sampler and asserts the dropped trace is gone, the generation advanced, and the
// per-shard finalize state is persisted.
func TestRunFinalizeRound_DropsAndStamps(t *testing.T) {
	tmpPath, defFn := test.Space(require.New(t))
	defer defFn()
	fileSystem := fs.NewLocalFileSystem()

	tst, err := newTSTable(
		fileSystem, tmpPath,
		common.Position{Database: "fg"},
		logger.GetLogger("finalize-round-test"),
		timestamp.TimeRange{},
		option{
			flushTimeout:  0,
			mergePolicy:   newDefaultMergePolicyForTesting(),
			protector:     protector.Nop{},
			decideTimeout: time.Second,
		},
		nil,
	)
	require.NoError(t, err)
	defer tst.Close()

	tst.mustAddTraces(tsTS1, nil)
	// Wait for the mem part to flush to a file part (mp == nil).
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

	require.Equal(t, uint64(3), snapshotTotalCount(tst), "3 traces before finalize")

	// finalize_grace tiny so the cooled parts (timestamp 1) are non-hot.
	finalized, roundErr := tst.runFinalizeRound([]sdk.Sampler{dropOneSampler{dropID: "trace2"}}, int64(time.Millisecond))
	require.NoError(t, roundErr)
	require.True(t, finalized, "a round must have committed")

	assert.Equal(t, uint64(2), snapshotTotalCount(tst), "trace2 must be dropped, leaving 2")
	assert.Equal(t, uint64(1), tst.finalizeGenCached.Load(), "generation must advance to 1")
	assert.Zero(t, tst.unsampledBytes.Load(), "counter must reset after a round")

	st := readFinalizeState(fileSystem, tmpPath)
	assert.Equal(t, uint64(1), st.FinalizeGeneration, "persisted generation must be 1")
	assert.Equal(t, 1, st.FinalizeRounds, "persisted round count must be 1")
	assert.NotEmpty(t, st.LastFinalizedAt, "lastFinalizedAt must be set")
}

// TestRunFinalizeRound_ReplayExcludesStampedParts reproduces the DD6 crash window: a
// part is stamped at the round generation on disk, but the per-shard state (and cache)
// still reads the OLD generation (as if the process died after introduce, before the
// state persist). On the next round the predicate finalizeGen >= gNext must EXCLUDE the
// stamped part, so no round runs and nothing is re-sampled.
func TestRunFinalizeRound_ReplayExcludesStampedParts(t *testing.T) {
	tmpPath, defFn := test.Space(require.New(t))
	defer defFn()
	fileSystem := fs.NewLocalFileSystem()

	tst, err := newTSTable(
		fileSystem, tmpPath, common.Position{Database: "rp"},
		logger.GetLogger("finalize-replay-test"), timestamp.TimeRange{},
		option{flushTimeout: 0, mergePolicy: newDefaultMergePolicyForTesting(), protector: protector.Nop{}, decideTimeout: time.Second},
		nil,
	)
	require.NoError(t, err)
	defer tst.Close()

	tst.mustAddTraces(tsTS1, nil)
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
	}, 10*time.Second, 20*time.Millisecond)

	// Round 1 stamps the output at gen 1 (on disk) and persists state gen 1.
	finalized, roundErr := tst.runFinalizeRound([]sdk.Sampler{dropOneSampler{dropID: "trace2"}}, int64(time.Millisecond))
	require.NoError(t, roundErr)
	require.True(t, finalized)
	countAfterFirst := snapshotTotalCount(tst)

	// Simulate the crash window: the parts stay stamped gen 1 on disk, but the state +
	// cache revert to gen 0 (the persist that would have recorded gen 1 never happened).
	tst.finalizeGenCached.Store(0)
	require.NoError(t, writeFinalizeState(fileSystem, tmpPath, finalizeState{}))

	// Replay: the gen-1 output part must be excluded (finalizeGen 1 >= gNext 1), so no
	// round runs — no double-sample of already-committed data.
	finalized2, roundErr2 := tst.runFinalizeRound([]sdk.Sampler{dropOneSampler{dropID: "trace2"}}, int64(time.Millisecond))
	require.NoError(t, roundErr2)
	assert.False(t, finalized2, "a part already stamped at the round generation must be excluded on replay")
	assert.Equal(t, uint64(0), tst.finalizeGenCached.Load(), "excluded replay must not advance the generation")
	assert.Equal(t, countAfterFirst, snapshotTotalCount(tst), "replay must not re-sample committed data")
}

// TestFinalizeAndMerge_Compose proves the in-merge filter (PIPELINE_EVENT_MERGE) and
// finalization sampling (PIPELINE_EVENT_FINALIZE) compose on ONE running shard, using
// the same registered sampler (DD11): a real hot merge drops its targets during the
// merge, a never-merged part is left for finalize, and a finalize round drops that
// part's target while re-keeping the merge survivors (no double-drop). The auto-merge
// loop is disabled (maxFanOutSize=0) so the merge below is the only one and the test
// is deterministic; the manual merge bypasses the policy, exercising the exact hot
// merge path (mergePartsThenSendIntroduction with nil override).
func TestFinalizeAndMerge_Compose(t *testing.T) {
	resetRegistries()
	defer resetRegistries()

	const group = "cg"
	deregister := registerSampler(group, dropPrefixSampler{prefix: "drop"})
	defer deregister()

	tmpPath, defFn := test.Space(require.New(t))
	defer defFn()
	fileSystem := fs.NewLocalFileSystem()

	tst, err := newTSTable(
		fileSystem, tmpPath,
		common.Position{Database: group},
		logger.GetLogger("finalize-merge-compose"),
		timestamp.TimeRange{},
		option{
			flushTimeout:              0,
			mergePolicy:               newMergePolicy(3, 1, run.Bytes(0)), // maxFanOutSize=0 disables auto-merge
			protector:                 protector.Nop{},
			decideTimeout:             time.Second,
			decideTimeoutCircuitBreak: 3,
			mergeGraceDefault:         time.Millisecond, // ancient parts are never hot
			nativePipelineEnabled:     true,             // activates the in-merge MERGE filter
		},
		nil,
	)
	require.NoError(t, err)
	defer tst.Close()

	// waitForFileParts blocks until the snapshot holds exactly n file parts.
	waitForFileParts := func(n int) {
		require.Eventually(t, func() bool {
			s := tst.currentSnapshot()
			if s == nil {
				return false
			}
			defer s.decRef()
			cnt := 0
			for _, pw := range s.parts {
				if pw.mp == nil && pw.p.partMetadata.TotalCount > 0 {
					cnt++
				}
			}
			return cnt == n
		}, 10*time.Second, 20*time.Millisecond)
	}
	collectFileParts := func() []*partWrapper {
		s := tst.currentSnapshot()
		require.NotNil(t, s)
		defer s.decRef()
		var pws []*partWrapper
		for _, pw := range s.parts {
			if pw.mp == nil && pw.p.partMetadata.TotalCount > 0 {
				pw.incRef()
				pws = append(pws, pw)
			}
		}
		return pws
	}

	// Two separately-flushed parts, each with one drop-target and one keeper.
	tst.mustAddTraces(tracesWithIDs("dropA", "keepB"), nil)
	waitForFileParts(1)
	tst.mustAddTraces(tracesWithIDs("dropC", "keepD"), nil)
	waitForFileParts(2)
	require.Equal(t, uint64(4), snapshotTotalCount(tst), "4 traces across 2 parts before any sampling")

	// A real hot merge (MERGE event): the in-merge sampler drops dropA + dropC.
	parts := collectFileParts()
	require.Len(t, parts, 2)
	merged := make(map[uint64]struct{}, len(parts))
	for _, pw := range parts {
		merged[pw.ID()] = struct{}{}
	}
	closeCh := make(chan struct{})
	_, mErr := tst.mergePartsThenSendIntroduction(
		snapshotCreatorMerger, parts, merged, tst.mergeCh, closeCh, mergeTypeFile, mergeLaneFast, nil,
	)
	close(closeCh)
	require.NoError(t, mErr)
	for _, pw := range parts {
		pw.decRef()
	}
	assert.Equal(t, uint64(2), snapshotTotalCount(tst), "MERGE must drop dropA + dropC, leaving keepB + keepD")
	assert.Zero(t, tst.finalizeGenCached.Load(), "a MERGE must not advance the finalize generation")

	// A third part that never merges — the case finalize exists to backstop.
	tst.mustAddTraces(tracesWithIDs("dropE", "keepF"), nil)
	waitForFileParts(2) // merged part + part3
	require.Equal(t, uint64(4), snapshotTotalCount(tst), "keepB + keepD + dropE + keepF")

	// FINALIZE event: drops dropE from the never-merged part and re-keeps the MERGE
	// survivors (deterministic sampler => no double-drop). Advances the generation.
	finalized, fErr := tst.runFinalizeRound([]sdk.Sampler{dropPrefixSampler{prefix: "drop"}}, int64(time.Millisecond))
	require.NoError(t, fErr)
	require.True(t, finalized, "finalize must have run")
	assert.Equal(t, uint64(3), snapshotTotalCount(tst), "keepB + keepD + keepF survive; dropE dropped, no keeper double-dropped")
	assert.Equal(t, uint64(1), tst.finalizeGenCached.Load(), "finalize must advance the generation to 1")
}
