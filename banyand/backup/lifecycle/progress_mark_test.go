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

package lifecycle

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// TestMarkPerTargetDoesNotMoveCounters pins the contract that per-target
// Mark*Completed / Mark*Error only maintain the per-target dedup / error
// maps and do NOT advance Counts or Progress. Counts is set by pre-walk
// (Set*Count) and Progress is advanced exclusively by MarkSource*Completed.
func TestMarkPerTargetDoesNotMoveCounters(t *testing.T) {
	p := NewProgress("", logger.GetLogger("test"))

	// Stream part: per-target mark several times — counters must stay 0.
	p.MarkStreamPartCompleted("g", "tgt-seg-1", 0, 1)
	p.MarkStreamPartCompleted("g", "tgt-seg-2", 0, 1)
	p.MarkStreamPartError("g", "tgt-seg-3", 0, 1, "err")
	assert.Equal(t, 0, p.StreamPartCounts["g"])
	assert.Equal(t, 0, p.StreamPartProgress["g"])

	// Per-target dedup map populated.
	assert.True(t, p.IsStreamPartCompleted("g", "tgt-seg-1", 0, 1))
	assert.True(t, p.IsStreamPartCompleted("g", "tgt-seg-2", 0, 1))

	// Same for series / element_index / measure / trace.
	p.MarkStreamSeriesCompleted("g", "tgt-seg-1", 0)
	p.MarkStreamElementIndexCompleted("g", "tgt-seg-1", 0)
	p.MarkMeasurePartCompleted("g", "tgt-seg-1", 0, 1)
	p.MarkMeasureSeriesCompleted("g", "tgt-seg-1", 0)
	p.MarkTraceShardCompleted("g", "tgt-seg-1", 0)
	p.MarkTraceSeriesCompleted("g", "tgt-seg-1", 0)
	for _, counter := range []int{
		p.StreamSeriesCounts["g"], p.StreamSeriesProgress["g"],
		p.StreamElementIndexCounts["g"], p.StreamElementIndexProgress["g"],
		p.MeasurePartCounts["g"], p.MeasurePartProgress["g"],
		p.MeasureSeriesCounts["g"], p.MeasureSeriesProgress["g"],
		p.TraceShardCounts["g"], p.TraceShardProgress["g"],
		p.TraceSeriesCounts["g"], p.TraceSeriesProgress["g"],
	} {
		assert.Equal(t, 0, counter, "per-target marks must not move counters")
	}
}

// TestSetCountIsThePlannedDenominator pins that Set*Count is the only writer
// of the per-resource Counts denominator (set by pre-walk).
func TestSetCountIsThePlannedDenominator(t *testing.T) {
	p := NewProgress("", logger.GetLogger("test"))

	p.SetStreamPartCount("g", 100)
	p.SetStreamSeriesCount("g", 50)
	p.SetStreamElementIndexCount("g", 10)
	p.SetMeasurePartCount("g", 70)
	p.SetMeasureSeriesCount("g", 35)
	p.SetTraceShardCount("g", 5)
	p.SetTraceSeriesCount("g", 8)

	assert.Equal(t, 100, p.StreamPartCounts["g"])
	assert.Equal(t, 50, p.StreamSeriesCounts["g"])
	assert.Equal(t, 10, p.StreamElementIndexCounts["g"])
	assert.Equal(t, 70, p.MeasurePartCounts["g"])
	assert.Equal(t, 35, p.MeasureSeriesCounts["g"])
	assert.Equal(t, 5, p.TraceShardCounts["g"])
	assert.Equal(t, 8, p.TraceSeriesCounts["g"])

	// Set is overwrite semantics — last call wins.
	p.SetStreamPartCount("g", 200)
	assert.Equal(t, 200, p.StreamPartCounts["g"])
}

// TestMarkSourceCompletedIdempotent pins that MarkSource*Completed advances
// Progress exactly once per unique source key across all 7 resources.
func TestMarkSourceCompletedIdempotent(t *testing.T) {
	p := NewProgress("", logger.GetLogger("test"))

	// Stream part: same key 3 times → Progress=1.
	p.MarkSourceStreamPartCompleted("g", "src-seg", 0, 1)
	p.MarkSourceStreamPartCompleted("g", "src-seg", 0, 1)
	p.MarkSourceStreamPartCompleted("g", "src-seg", 0, 1)
	assert.Equal(t, 1, p.StreamPartProgress["g"])
	assert.True(t, p.SourceCompletedStreamParts["g"]["src-seg"][0][1])
	// Different partID = different source = +1.
	p.MarkSourceStreamPartCompleted("g", "src-seg", 0, 2)
	assert.Equal(t, 2, p.StreamPartProgress["g"])

	// Stream series, element_index, measure, trace: same pattern, +1 once.
	p.MarkSourceStreamSeriesCompleted("g", "src-seg", 0)
	p.MarkSourceStreamSeriesCompleted("g", "src-seg", 0)
	assert.Equal(t, 1, p.StreamSeriesProgress["g"])

	p.MarkSourceStreamElementIndexCompleted("g", "src-seg", 0)
	p.MarkSourceStreamElementIndexCompleted("g", "src-seg", 0)
	assert.Equal(t, 1, p.StreamElementIndexProgress["g"])

	p.MarkSourceMeasurePartCompleted("g", "src-seg", 0, 1)
	p.MarkSourceMeasurePartCompleted("g", "src-seg", 0, 1)
	assert.Equal(t, 1, p.MeasurePartProgress["g"])

	p.MarkSourceMeasureSeriesCompleted("g", "src-seg", 0)
	p.MarkSourceMeasureSeriesCompleted("g", "src-seg", 0)
	assert.Equal(t, 1, p.MeasureSeriesProgress["g"])

	p.MarkSourceTraceShardCompleted("g", "src-seg", 0)
	p.MarkSourceTraceShardCompleted("g", "src-seg", 0)
	assert.Equal(t, 1, p.TraceShardProgress["g"])

	p.MarkSourceTraceSeriesCompleted("g", "src-seg", 0)
	p.MarkSourceTraceSeriesCompleted("g", "src-seg", 0)
	assert.Equal(t, 1, p.TraceSeriesProgress["g"])
}

// TestClearErrors_AllSevenBuckets pins that ClearErrors resets every error
// map (including trace) and does NOT touch Counts/Progress (which now
// derive from pre-walk + MarkSource and are immutable to error replay).
func TestClearErrors_AllSevenBuckets(t *testing.T) {
	p := NewProgress("", logger.GetLogger("test"))

	// Set planned counts (pre-walk).
	p.SetStreamPartCount("g", 10)
	p.SetStreamSeriesCount("g", 10)
	p.SetStreamElementIndexCount("g", 10)
	p.SetMeasurePartCount("g", 10)
	p.SetMeasureSeriesCount("g", 10)
	p.SetTraceShardCount("g", 10)
	p.SetTraceSeriesCount("g", 10)

	// Drop one error per bucket.
	p.MarkStreamPartError("g", "s", 0, 1, "x")
	p.MarkStreamSeriesError("g", "s", 0, "x")
	p.MarkStreamElementIndexError("g", "s", 0, "x")
	p.MarkMeasurePartError("g", "s", 0, 1, "x")
	p.MarkMeasureSeriesError("g", "s", 0, "x")
	p.MarkTraceShardError("g", "s", 0, "x")
	p.MarkTraceSeriesError("g", "s", 0, "x")

	p.ClearErrors()

	assert.Empty(t, p.StreamPartErrors["g"])
	assert.Empty(t, p.StreamSeriesErrors["g"])
	assert.Empty(t, p.StreamElementIndexErrors["g"])
	assert.Empty(t, p.MeasurePartErrors["g"])
	assert.Empty(t, p.MeasureSeriesErrors["g"])
	assert.Empty(t, p.TraceShardErrors["g"])
	assert.Empty(t, p.TraceSeriesErrors["g"])

	// Counts are pre-walk planned — must survive ClearErrors.
	for _, c := range []int{
		p.StreamPartCounts["g"], p.StreamSeriesCounts["g"], p.StreamElementIndexCounts["g"],
		p.MeasurePartCounts["g"], p.MeasureSeriesCounts["g"],
		p.TraceShardCounts["g"], p.TraceSeriesCounts["g"],
	} {
		assert.Equal(t, 10, c, "Counts must not be touched by ClearErrors")
	}
}

// TestPerTargetAndPerSourceAreIndependent pins that per-target dedup and
// per-source progress are tracked independently: per-target Mark does not
// move source progress, and MarkSource does not populate the per-target map.
func TestPerTargetAndPerSourceAreIndependent(t *testing.T) {
	p := NewProgress("", logger.GetLogger("test"))

	p.MarkStreamPartCompleted("g", "tgt-1", 0, 1)
	assert.True(t, p.IsStreamPartCompleted("g", "tgt-1", 0, 1))
	assert.False(t, p.SourceCompletedStreamParts["g"]["src-1"][0][1])
	assert.Equal(t, 0, p.StreamPartProgress["g"])

	p.MarkSourceStreamPartCompleted("g", "src-1", 0, 1)
	assert.True(t, p.SourceCompletedStreamParts["g"]["src-1"][0][1])
	// Per-target map untouched by source mark.
	assert.False(t, p.IsStreamPartCompleted("g", "src-1", 0, 1))
	assert.Equal(t, 1, p.StreamPartProgress["g"])
}

// TestPartCompletionKeyedBySourceSegment pins that measure/stream part
// completion dedup is keyed by the SOURCE segment, not the target segment.
// Part IDs reset per source segment, so when several source segments collapse
// into one larger target segment (warm interval > hot interval), two source
// segments can each carry part_id=1 on the same shard. If completion were keyed
// by the target segment, marking source-A's part done would make source-B's
// part look done and the visitor (VisitPart) would skip it, dropping source-B's
// data. Keying by the source segment — what measure/stream VisitPart now pass,
// matching trace's source-keyed VisitShard — keeps the two parts independent.
func TestPartCompletionKeyedBySourceSegment(t *testing.T) {
	p := NewProgress("", logger.GetLogger("test"))
	const (
		group   = "g"
		srcSegA = "[2026-05-19 00:00:00, 2026-05-20 00:00:00)"
		srcSegB = "[2026-05-20 00:00:00, 2026-05-21 00:00:00)"
	)
	var shard common.ShardID // 0 on both source segments
	const partID uint64 = 1  // resets per source segment, so it collides

	// Source segment A's part 1 migrated into the shared target segment.
	p.MarkMeasurePartCompleted(group, srcSegA, shard, partID)
	p.MarkStreamPartCompleted(group, srcSegA, shard, partID)

	// Source segment B's part 1 (same shard + part ID, different source segment)
	// must NOT be seen as already done — otherwise VisitPart skips it and loses
	// B's data.
	assert.False(t, p.IsMeasurePartCompleted(group, srcSegB, shard, partID),
		"measure: source segment B part must be independent of source segment A")
	assert.False(t, p.IsStreamPartCompleted(group, srcSegB, shard, partID),
		"stream: source segment B part must be independent of source segment A")

	// Source segment A stays marked.
	assert.True(t, p.IsMeasurePartCompleted(group, srcSegA, shard, partID))
	assert.True(t, p.IsStreamPartCompleted(group, srcSegA, shard, partID))
}

// TestSyncBreakdownCountersPersistAndAccumulate pins that the chunk-sync /
// row-replay counters and the per-node replay errors are written to the
// progress file and reloaded on resume, and that a subsequent Add accumulates
// onto the reloaded value rather than resetting it.
func TestSyncBreakdownCountersPersistAndAccumulate(t *testing.T) {
	pf := filepath.Join(t.TempDir(), "progress.json")
	l := logger.GetLogger("test")

	p := NewProgress(pf, l)
	p.AddMeasureChunkSyncPart("g")  // measure: 1 chunk-sync part
	p.AddMeasureChunkSyncPart("g")  // measure: 2 chunk-sync parts
	p.AddMeasureRowReplay("g", 5)   // measure: 1 row-replay part, 5 rows
	p.AddStreamChunkSyncPart("g2")  // stream: 1 chunk-sync part
	p.AddStreamRowReplay("g2", 3)   // stream: 1 row-replay part, 3 rows
	p.AddTraceChunkSyncShard("g3")  // trace: 1 chunk-sync shard
	p.AddTraceRowReplay("g3", 2, 6) // trace: 2 row-replay parts, 6 rows
	p.RecordRowReplayNodeErrors("g2", map[string]*common.Error{"node-1": common.NewError("boom")})

	// Reload from disk (resume).
	reloaded := LoadProgress(pf, l)
	assert.Equal(t, uint64(2), reloaded.MeasureChunkSyncParts["g"])
	assert.Equal(t, uint64(1), reloaded.MeasureRowReplayParts["g"])
	assert.Equal(t, uint64(5), reloaded.MeasureRowReplayRows["g"])
	assert.Equal(t, uint64(1), reloaded.StreamChunkSyncParts["g2"])
	assert.Equal(t, uint64(1), reloaded.StreamRowReplayParts["g2"])
	assert.Equal(t, uint64(3), reloaded.StreamRowReplayRows["g2"])
	assert.Equal(t, uint64(1), reloaded.TraceChunkSyncShards["g3"])
	assert.Equal(t, uint64(2), reloaded.TraceRowReplayParts["g3"])
	assert.Equal(t, uint64(6), reloaded.TraceRowReplayRows["g3"])
	require.Contains(t, reloaded.RowReplayNodeErrors, "g2")
	assert.Contains(t, reloaded.RowReplayNodeErrors["g2"]["node-1"], "boom")

	// Resume accumulation: a further Add builds on the reloaded value.
	reloaded.AddMeasureRowReplay("g", 10)
	assert.Equal(t, uint64(2), reloaded.MeasureRowReplayParts["g"])
	assert.Equal(t, uint64(15), reloaded.MeasureRowReplayRows["g"])
}
