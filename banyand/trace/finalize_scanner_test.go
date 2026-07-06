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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

// TestWarrantsFinalize exercises the threshold decision: never-finalized always
// warrants; a terminal or max-rounds shard never does (and max marks terminal); an
// un-elapsed cooldown blocks; and once finalized, the unsampled-bytes counter must
// cross the floor.
func TestWarrantsFinalize(t *testing.T) {
	fileSystem := fs.NewLocalFileSystem()
	newTst := func(dir string) *tsTable {
		return &tsTable{fileSystem: fileSystem, root: dir, l: logger.GetLogger("warrants-test")}
	}
	// ratio 0 so the threshold is just the absolute floor (no snapshot needed).
	cfg := finalizeConfig{floorBytes: 100, ratio: 0, cooldownNs: int64(time.Minute), maxRounds: 3}

	// Never finalized (no state file) => always warrants.
	assert.True(t, warrantsFinalize(newTst(t.TempDir()), cfg), "gen 0 must always warrant")

	// Terminal => never.
	dTerm := t.TempDir()
	require.NoError(t, writeFinalizeState(fileSystem, dTerm, finalizeState{Terminal: true}))
	assert.False(t, warrantsFinalize(newTst(dTerm), cfg))

	// Rounds at the cap => false AND the shard is marked terminal.
	dMax := t.TempDir()
	require.NoError(t, writeFinalizeState(fileSystem, dMax, finalizeState{FinalizeGeneration: 1, FinalizeRounds: 3}))
	assert.False(t, warrantsFinalize(newTst(dMax), cfg))
	assert.True(t, readFinalizeState(fileSystem, dMax).Terminal, "reaching max rounds must mark terminal")

	// Cooldown not elapsed => false.
	dCool := t.TempDir()
	require.NoError(t, writeFinalizeState(fileSystem, dCool, finalizeState{
		FinalizeGeneration: 1, FinalizeRounds: 1,
		LastFinalizedAt: time.Now().UTC().Format(time.RFC3339Nano),
	}))
	assert.False(t, warrantsFinalize(newTst(dCool), cfg))

	// Finalized, cooldown elapsed: warrants iff unsampled bytes reach the floor.
	dThresh := t.TempDir()
	require.NoError(t, writeFinalizeState(fileSystem, dThresh, finalizeState{
		FinalizeGeneration: 1, FinalizeRounds: 1,
		LastFinalizedAt: time.Now().Add(-time.Hour).UTC().Format(time.RFC3339Nano),
	}))
	tst := newTst(dThresh)
	tst.unsampledBytes.Store(200)
	assert.True(t, warrantsFinalize(tst, cfg), "unsampled above floor must warrant")
	tst.unsampledBytes.Store(50)
	assert.False(t, warrantsFinalize(tst, cfg), "unsampled below floor must not warrant")
}

// TestWarrantsFinalize_RatioBranch exercises the RATIO*total threshold (the branch
// TestWarrantsFinalize's ratio=0 never reaches): for a large finalized shard, a re-round
// is warranted only when new unsampled bytes reach RATIO of the segment total.
func TestWarrantsFinalize_RatioBranch(t *testing.T) {
	fileSystem := fs.NewLocalFileSystem()
	dir := t.TempDir()
	// Finalized (gen>0) with an elapsed cooldown, so warrants gates purely on the bytes.
	require.NoError(t, writeFinalizeState(fileSystem, dir, finalizeState{
		FinalizeGeneration: 1, FinalizeRounds: 1,
		LastFinalizedAt: time.Now().Add(-time.Hour).UTC().Format(time.RFC3339Nano),
	}))
	tst := &tsTable{fileSystem: fileSystem, root: dir, l: logger.GetLogger("warrants-ratio-test")}
	// Snapshot totals 1000 uncompressed bytes so RATIO dominates the absolute floor.
	tst.snapshot = &snapshot{ref: 1, parts: []*partWrapper{
		{p: &part{partMetadata: partMetadata{UncompressedSpanSizeBytes: 1000, TotalCount: 1}}},
	}}
	cfg := finalizeConfig{floorBytes: 100, ratio: 0.5, cooldownNs: int64(time.Minute), maxRounds: 8}

	tst.unsampledBytes.Store(600)
	assert.True(t, warrantsFinalize(tst, cfg), "600 >= max(floor 100, 0.5*1000=500) must warrant")
	tst.unsampledBytes.Store(400)
	assert.False(t, warrantsFinalize(tst, cfg), "400 < 500 (ratio-dominated threshold) must not warrant")
}

// TestShardMayWarrant verifies the reopen pre-filter: it skips (without reopening) only
// the authoritative-from-disk cases — terminal, max-rounds, and un-elapsed cooldown —
// and conservatively reopens everything else (never-finalized, past-cooldown).
func TestShardMayWarrant(t *testing.T) {
	cfg := finalizeConfig{floorBytes: 100, ratio: 0.1, cooldownNs: int64(time.Minute), maxRounds: 8}

	assert.True(t, shardMayWarrant(finalizeState{}, cfg), "never-finalized must be reopened")
	assert.False(t, shardMayWarrant(finalizeState{Terminal: true}, cfg), "terminal must be skipped")
	assert.False(t, shardMayWarrant(finalizeState{FinalizeGeneration: 1, FinalizeRounds: 8}, cfg), "max rounds must be skipped")
	assert.False(t, shardMayWarrant(finalizeState{
		FinalizeGeneration: 1, FinalizeRounds: 1,
		LastFinalizedAt: time.Now().UTC().Format(time.RFC3339Nano),
	}, cfg), "un-elapsed cooldown must be skipped without reopening")
	assert.True(t, shardMayWarrant(finalizeState{
		FinalizeGeneration: 1, FinalizeRounds: 1,
		LastFinalizedAt: time.Now().Add(-time.Hour).UTC().Format(time.RFC3339Nano),
	}, cfg), "past-cooldown must be reopened for the precise threshold")
	assert.True(t, shardMayWarrant(finalizeState{FinalizeGeneration: 1, FinalizeRounds: 1}, cfg),
		"finalized with no cooldown clock recorded is reopened conservatively")
}

// TestRunFinalizeScan_Gating verifies the scan pass is a safe no-op when there are no
// finalize-enabled groups, and skips a finalize group that has no samplers (never
// reaching loadTSDB).
func TestRunFinalizeScan_Gating(t *testing.T) {
	resetRegistries()
	defer resetRegistries()

	sr := makeDataSchemaRepo("/some/dir")

	// No finalize groups: no-op, no panic.
	require.NotPanics(t, func() { sr.runFinalizeScan(nil) })

	// A finalize-configured group with NO samplers is skipped before any TSDB load.
	setFinalizeConfigForGroup("g-no-samplers", &finalizeConfig{})
	setFinalizeGraceForGroup("g-no-samplers", int64(5*time.Minute))
	require.NotPanics(t, func() { sr.runFinalizeScan(nil) })
}

// TestFinalizeScan_SelectsCooledSegmentAndFinalizes closes the scanner→round seam: it
// builds a real TSDB, writes into a cooled (past) segment, then drives the exact
// storage calls scanGroup makes — SelectSegments(coolRange, reopenClosed=true) ->
// GetTimeRange cool re-check -> Tables() -> runFinalizeRound — and asserts the dropped
// trace is gone, the shard generation advanced, and segments are DecRef'd back to
// dormant (no open-segment leak).
func TestFinalizeScan_SelectsCooledSegmentAndFinalizes(t *testing.T) {
	tmpPath, defFn := test.Space(require.New(t))
	defer defFn()

	// Hourly segments + long TTL so a 2h-old segment is cooled but NOT retention-expired
	// (SelectSegments excludes expired segments).
	hourly := storage.IntervalRule{Unit: storage.HOUR, Num: 1}
	opts := storage.TSDBOpts[*tsTable, option]{
		ShardNum:        1,
		Location:        filepath.Join(tmpPath, "tab"),
		TSTableCreator:  newTSTable,
		SegmentInterval: hourly,
		TTL:             storage.IntervalRule{Unit: storage.DAY, Num: 30},
		Option:          option{flushTimeout: 0, protector: protector.Nop{}, mergePolicy: newDefaultMergePolicyForTesting(), decideTimeout: time.Second},
	}
	require.NoError(t, os.MkdirAll(opts.Location, storage.DirPerm))
	ctx := common.SetPosition(
		context.WithValue(context.Background(), logger.ContextKey, logger.GetLogger("finalize-scan-test")),
		func(p common.Position) common.Position { p.Database = "test"; return p },
	)
	db, err := storage.OpenTSDB[*tsTable, option](ctx, opts, nil, "test-group")
	require.NoError(t, err)
	defer db.Close()

	// Create a cooled segment ~2h in the past and write traces dated within it.
	past := time.Now().Add(-2 * time.Hour)
	seg, err := db.CreateSegmentIfNotExist(past)
	require.NoError(t, err)
	// A brand-new segment has no shard tables until one is created (they are lazy).
	tst, err := seg.CreateTSTableIfNotExist(common.ShardID(0))
	require.NoError(t, err)

	pastTraces := &traces{
		traceIDs:   []string{"keep1", "drop2", "keep3"},
		timestamps: []int64{past.UnixNano(), past.UnixNano(), past.UnixNano()},
		tags: [][]*tagValue{
			{{tag: "t", valueType: pbv1.ValueTypeStr, value: []byte("a")}},
			{{tag: "t", valueType: pbv1.ValueTypeStr, value: []byte("b")}},
			{{tag: "t", valueType: pbv1.ValueTypeStr, value: []byte("c")}},
		},
		spans:   [][]byte{[]byte("s1"), []byte("s2"), []byte("s3")},
		spanIDs: []string{"s1", "s2", "s3"},
	}
	tst.mustAddTraces(pastTraces, nil)
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
	seg.DecRef()

	// Drive scanGroup's storage calls directly (loadTSDB is a trivial registry lookup).
	graceNs := int64(time.Millisecond)
	now := time.Now().UnixNano()
	coolRange := timestamp.TimeRange{Start: time.Unix(0, 0), End: time.Unix(0, now-graceNs)}

	// PeekSegments must surface the cooled segment's shard path WITHOUT reopening, and
	// the gen-0 shard must pass the reopen pre-filter.
	peeks := db.PeekSegments(coolRange)
	require.NotEmpty(t, peeks, "PeekSegments must return the cooled segment")
	sawShard := false
	for _, pk := range peeks {
		if pk.End.UnixNano() <= now-graceNs && len(pk.ShardPaths) > 0 {
			sawShard = true
			assert.True(t, segmentMayWarrant(fs.NewLocalFileSystem(), pk.ShardPaths, finalizeConfig{floorBytes: 1, ratio: 0.1, maxRounds: 8}),
				"a never-finalized cooled segment must pass the reopen pre-filter")
		}
	}
	require.True(t, sawShard, "PeekSegments must expose the cooled segment's shard path")

	segs, err := db.SelectSegments(coolRange, true)
	require.NoError(t, err)
	require.NotEmpty(t, segs, "the cooled segment must be selected")

	var ranRound bool
	for _, s := range segs {
		if s.GetTimeRange().End.UnixNano() > now-graceNs {
			continue
		}
		tt, _ := s.Tables()
		for _, table := range tt {
			finalized, ferr := table.runFinalizeRound([]sdk.Sampler{dropOneSampler{dropID: "drop2"}}, graceNs)
			require.NoError(t, ferr)
			if finalized {
				ranRound = true
				assert.Equal(t, uint64(2), snapshotTotalCount(table), "drop2 must be removed, leaving 2")
				assert.Equal(t, uint64(1), table.finalizeGenCached.Load())
			}
		}
	}
	for _, s := range segs {
		s.DecRef()
	}
	assert.True(t, ranRound, "a finalize round must have run on the cooled segment")
}
