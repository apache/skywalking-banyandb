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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// TestBuildMigrationReport_PartialFailure pins down the JSON shape and
// numeric values that buildMigrationReport must produce under a partial
// migration: one stream group has a failed part write while every other
// group and every other resource within the failing group completes. The
// shapes and counts are source-level — Counts comes from the pre-walk
// (Set*Count), Progress is bumped per source by MarkSource*Completed, and
// completion_rate = Progress / Counts is always in [0, 100] with 100 %
// meaning every source item migrated.
func TestBuildMigrationReport_PartialFailure(t *testing.T) {
	p := NewProgress("", logger.GetLogger("test"))
	p.SetGroupsToProcess([]string{"sw_a", "sw_b", "sw_metrics", "sw_trace"})

	p.SnapshotStreamDir = "/tmp/stream/snapshots/example"
	p.SnapshotMeasureDir = "/tmp/measure/snapshots/example"
	p.SnapshotTraceDir = "/tmp/trace/snapshots/example"

	// === sw_a (stream): full success on 2 source parts, 2 source series files, 1 source element_index visit. ===
	p.SetStreamPartCount("sw_a", 2)
	p.MarkSourceStreamPartCompleted("sw_a", "20260513", 0, 1)
	p.MarkSourceStreamPartCompleted("sw_a", "20260513", 0, 2)
	p.SetStreamSeriesCount("sw_a", 2)
	p.MarkSourceStreamSeriesCompleted("sw_a", "20260513", 100)
	p.MarkSourceStreamSeriesCompleted("sw_a", "20260513", 101)
	p.SetStreamElementIndexCount("sw_a", 1)
	p.MarkSourceStreamElementIndexCompleted("sw_a", "20260513", 0)
	p.MarkGroupCompleted("sw_a")
	p.MarkStreamGroupDeleted("sw_a")

	// === sw_b (stream): 4 source parts planned, 3 complete + 1 fails (partID 7 on shard 1). ===
	// series + element_index all complete.
	p.SetStreamPartCount("sw_b", 4)
	p.MarkSourceStreamPartCompleted("sw_b", "20260512", 0, 1)
	p.MarkSourceStreamPartCompleted("sw_b", "20260512", 0, 2)
	p.MarkSourceStreamPartCompleted("sw_b", "20260512", 1, 1)
	// partID 7 fails on shard 1 — record the per-target error, source not advanced.
	p.MarkStreamPartError("sw_b", "20260512", 1, 7, "failed to stream part to target node-cold-0: connection refused")
	p.SetStreamSeriesCount("sw_b", 2)
	p.MarkSourceStreamSeriesCompleted("sw_b", "20260512", 0)
	p.MarkSourceStreamSeriesCompleted("sw_b", "20260512", 1)
	p.SetStreamElementIndexCount("sw_b", 2)
	p.MarkSourceStreamElementIndexCompleted("sw_b", "20260512", 0)
	p.MarkSourceStreamElementIndexCompleted("sw_b", "20260512", 1)
	// sw_b is NOT marked completed (processStreamGroup early-returns on the failure).

	// === sw_metrics (measure): full success. ===
	p.SetMeasurePartCount("sw_metrics", 3)
	p.MarkSourceMeasurePartCompleted("sw_metrics", "20260513", 0, 1)
	p.MarkSourceMeasurePartCompleted("sw_metrics", "20260513", 0, 2)
	p.MarkSourceMeasurePartCompleted("sw_metrics", "20260513", 0, 3)
	p.SetMeasureSeriesCount("sw_metrics", 1)
	p.MarkSourceMeasureSeriesCompleted("sw_metrics", "20260513", 100)
	p.MarkGroupCompleted("sw_metrics")
	p.MarkMeasureGroupDeleted("sw_metrics")

	// === sw_trace: full success. ===
	p.SetTraceShardCount("sw_trace", 1)
	p.MarkSourceTraceShardCompleted("sw_trace", "20260513", 0)
	p.SetTraceSeriesCount("sw_trace", 1)
	p.MarkSourceTraceSeriesCompleted("sw_trace", "20260513", 100)
	p.MarkGroupCompleted("sw_trace")
	p.MarkTraceGroupDeleted("sw_trace")

	// --- Build report ---
	svc := &lifecycleService{l: logger.GetLogger("test")}
	report := svc.buildMigrationReport(p)

	require.Equal(t, "2.1", report["report_version"])
	summary, ok := report["summary"].(map[string]interface{})
	require.True(t, ok)
	errs, ok := report["errors"].(map[string]interface{})
	require.True(t, ok)

	// migration_status: 4 scheduled, 3 completed (sw_a, sw_metrics, sw_trace), 75 %.
	ms, ok := summary["migration_status"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, 4, ms["total_groups"])
	assert.Equal(t, 3, ms["completed_groups"])
	assert.InDelta(t, 75.0, ms["completion_rate"], 1e-9)

	// stream parts: 2+4=6 planned, 2+3=5 source-completed.
	assertResource(t, summary, "stream_migration", "parts", 6, 5, 83.33333333333334, 1)
	assertResource(t, summary, "stream_migration", "series", 4, 4, 100.0, 0)
	assertResource(t, summary, "stream_migration", "element_index", 3, 3, 100.0, 0)

	// measure
	assertResource(t, summary, "measure_migration", "parts", 3, 3, 100.0, 0)
	assertResource(t, summary, "measure_migration", "series", 1, 1, 100.0, 0)

	// trace
	assertResource(t, summary, "trace_migration", "parts", 1, 1, 100.0, 0)
	assertResource(t, summary, "trace_migration", "series", 1, 1, 100.0, 0)

	// errors: stream_parts has one entry for sw_b; everything else empty.
	for _, key := range []string{
		"stream_parts", "stream_series", "stream_element_index",
		"measure_parts", "measure_series",
		"trace_parts", "trace_series",
	} {
		v, found := errs[key]
		require.Truef(t, found, "errors.%s must be present", key)
		_, isMap := v.(map[string]interface{})
		require.Truef(t, isMap, "errors.%s must be map[string]interface{}", key)
	}
	streamPartErrs := errs["stream_parts"].(map[string]interface{})
	require.Lenf(t, streamPartErrs, 1, "errors.stream_parts must hold one group entry, got %v", streamPartErrs)
	require.Contains(t, streamPartErrs, "sw_b")

	for _, key := range []string{
		"stream_series", "stream_element_index",
		"measure_parts", "measure_series",
		"trace_parts", "trace_series",
	} {
		v := errs[key].(map[string]interface{})
		assert.Emptyf(t, v, "errors.%s must be empty for this scenario", key)
	}
}

// TestBuildMigrationReport_CompletedScopedToScheduledSet pins the
// resume-safety invariant: CompletedGroups carrying entries from a prior
// cycle that are no longer in the current GroupsToProcess set must NOT
// inflate completed_groups beyond total_groups.
func TestBuildMigrationReport_CompletedScopedToScheduledSet(t *testing.T) {
	p := NewProgress("", logger.GetLogger("test"))
	p.MarkGroupCompleted("sw_a")
	p.MarkGroupCompleted("sw_b")
	p.MarkGroupCompleted("sw_c")
	p.MarkGroupCompleted("sw_d")
	p.MarkGroupCompleted("sw_e")
	p.SetGroupsToProcess([]string{"sw_f", "sw_g", "sw_h"})
	p.MarkGroupCompleted("sw_f")
	p.MarkGroupCompleted("sw_g")
	p.MarkGroupCompleted("sw_h")

	svc := &lifecycleService{l: logger.GetLogger("test")}
	report := svc.buildMigrationReport(p)
	summary := report["summary"].(map[string]interface{})
	ms := summary["migration_status"].(map[string]interface{})

	assert.Equal(t, 3, ms["total_groups"])
	assert.Equal(t, 3, ms["completed_groups"])
	assert.InDelta(t, 100.0, ms["completion_rate"], 1e-9)
}

// TestBuildMigrationReport_EmptyCycleHonestTotalGroups pins that an
// empty-snapshot cycle reports total_groups=0 / rate=0 instead of inheriting
// a stale prior-cycle GroupsToProcess.
func TestBuildMigrationReport_EmptyCycleHonestTotalGroups(t *testing.T) {
	p := NewProgress("", logger.GetLogger("test"))
	p.MarkGroupCompleted("sw_a")
	p.MarkGroupCompleted("sw_b")
	p.SetGroupsToProcess(nil)

	svc := &lifecycleService{l: logger.GetLogger("test")}
	report := svc.buildMigrationReport(p)
	summary := report["summary"].(map[string]interface{})
	ms := summary["migration_status"].(map[string]interface{})

	assert.Equal(t, 0, ms["total_groups"])
	assert.Equal(t, 0, ms["completed_groups"])
	assert.InDelta(t, 0.0, ms["completion_rate"], 1e-9)
}

// TestBuildMigrationReport_SyncBreakdown pins the sync_breakdown blocks and the
// row_replay_node_errors block introduced in report_version 2.1. It seeds both
// chunk-sync and row-replay activity across measure/stream/trace groups and
// asserts the per-group and _total accounting plus the per-node replay error.
func TestBuildMigrationReport_SyncBreakdown(t *testing.T) {
	p := NewProgress("", logger.GetLogger("test"))

	// measure: 2 chunk-sync parts; 1 row-replay part carrying 5 rows.
	p.AddMeasureChunkSyncPart("sw_metrics")
	p.AddMeasureChunkSyncPart("sw_metrics")
	p.AddMeasureRowReplay("sw_metrics", 5)
	// stream: 1 chunk-sync part; 2 row-replay parts carrying 3 + 4 rows.
	p.AddStreamChunkSyncPart("sw_stream")
	p.AddStreamRowReplay("sw_stream", 3)
	p.AddStreamRowReplay("sw_stream", 4)
	// trace: 1 chunk-sync shard; 1 row-replay shard with 2 parts / 6 rows.
	p.AddTraceChunkSyncShard("sw_trace")
	p.AddTraceRowReplay("sw_trace", 2, 6)
	// per-node replay error on the stream group.
	p.RecordRowReplayNodeErrors("sw_stream", map[string]*common.Error{
		"data-node-1": common.NewError("flush timeout"),
	})

	svc := &lifecycleService{l: logger.GetLogger("test")}
	report := svc.buildMigrationReport(p)
	summary := report["summary"].(map[string]interface{})

	mb := summary["measure_migration"].(map[string]interface{})["sync_breakdown"].(map[string]interface{})
	assertBreakdown(t, mb, "sw_metrics", "chunk_sync_parts", 2, 1, 5)
	assertBreakdown(t, mb, "_total", "chunk_sync_parts", 2, 1, 5)

	sb := summary["stream_migration"].(map[string]interface{})["sync_breakdown"].(map[string]interface{})
	assertBreakdown(t, sb, "sw_stream", "chunk_sync_parts", 1, 2, 7)
	assertBreakdown(t, sb, "_total", "chunk_sync_parts", 1, 2, 7)

	tb := summary["trace_migration"].(map[string]interface{})["sync_breakdown"].(map[string]interface{})
	assertBreakdown(t, tb, "sw_trace", "chunk_sync_shards", 1, 2, 6)
	assertBreakdown(t, tb, "_total", "chunk_sync_shards", 1, 2, 6)

	errs := report["errors"].(map[string]interface{})
	nodeErrs, ok := errs["row_replay_node_errors"].(map[string]interface{})
	require.True(t, ok, "errors.row_replay_node_errors must be a map")
	groupErrs, ok := nodeErrs["sw_stream"].(map[string]interface{})
	require.True(t, ok, "row_replay_node_errors must contain sw_stream")
	assert.Contains(t, groupErrs["data-node-1"], "flush timeout")
}

// assertBreakdown checks one sync_breakdown entry's chunk count (keyed by
// chunkKey), row-replay part count and row-replay row count.
func assertBreakdown(t *testing.T, breakdown map[string]interface{}, key, chunkKey string, chunk, replayParts, replayRows uint64) {
	t.Helper()
	entry, ok := breakdown[key].(map[string]interface{})
	require.Truef(t, ok, "sync_breakdown[%s] must be a map", key)
	assert.Equalf(t, chunk, entry[chunkKey], "sync_breakdown[%s].%s", key, chunkKey)
	assert.Equalf(t, replayParts, entry["row_replay_parts"], "sync_breakdown[%s].row_replay_parts", key)
	assert.Equalf(t, replayRows, entry["row_replay_rows"], "sync_breakdown[%s].row_replay_rows", key)
}

// assertResource pins down a single resource sub-block under a catalog
// (stream | measure | trace)_migration. total = pre-walk source count;
// completed = source items fully migrated; errors = per-target error count;
// rate = completed / total ∈ [0, 100].
func assertResource(t *testing.T, summary map[string]interface{}, catalog, resource string, total, completed int, rate float64, errors int) {
	t.Helper()
	cat, ok := summary[catalog].(map[string]interface{})
	require.Truef(t, ok, "summary.%s must be a map", catalog)
	res, ok := cat[resource].(map[string]interface{})
	require.Truef(t, ok, "summary.%s.%s must be a map", catalog, resource)
	assert.Equalf(t, total, res["total"], "summary.%s.%s.total", catalog, resource)
	assert.Equalf(t, completed, res["completed"], "summary.%s.%s.completed", catalog, resource)
	assert.Equalf(t, errors, res["errors"], "summary.%s.%s.errors", catalog, resource)
	assert.InDeltaf(t, rate, res["completion_rate"], 1e-9, "summary.%s.%s.completion_rate", catalog, resource)
}

// TestCompletionRateNeverExceeds100UnderFanOut replays the original 266 %
// regression scenario: per-target Mark*Completed is invoked once per
// (source × target_segment × target_shard) tuple while MarkSource*Completed
// is invoked once per source. Under Option 3 the per-target Marks must NOT
// move counters and MarkSource bumps Progress exactly once per source, so
// completion_rate = Progress / pre-walk-Counts stays in [0, 100] no matter
// how large the fan-out is. This is the regression test that pins the 266 %
// bug as fixed.
func TestCompletionRateNeverExceeds100UnderFanOut(t *testing.T) {
	p := NewProgress("", logger.GetLogger("test"))
	p.SetGroupsToProcess([]string{"sw_fanout"})

	// 4 source series files × 2 target segments × 3 target shards = 24
	// per-target Mark calls. Pre-walk says 4 source items.
	const sourceCount = 4
	p.SetStreamSeriesCount("sw_fanout", sourceCount)
	targetSegs := []string{"tgt-seg-0", "tgt-seg-1"}

	for src := 0; src < sourceCount; src++ {
		sourceShard := common.ShardID(src)
		// Fan-out: emit per-target Marks for every (target_segment, target_shard).
		for _, tgtSeg := range targetSegs {
			for tgtShard := common.ShardID(0); tgtShard < 3; tgtShard++ {
				p.MarkStreamSeriesCompleted("sw_fanout", tgtSeg, tgtShard)
			}
		}
		// Per-source: advance Progress exactly once per source file.
		p.MarkSourceStreamSeriesCompleted("sw_fanout", "src-segment", sourceShard)
	}
	p.MarkGroupCompleted("sw_fanout")

	svc := &lifecycleService{l: logger.GetLogger("test")}
	report := svc.buildMigrationReport(p)
	summary := report["summary"].(map[string]interface{})

	// Counts comes from pre-walk Set*Count, not from per-target Marks.
	// Progress is one bump per source MarkSource, not one per fan-out Mark.
	// rate = 4/4 = 100 % (NOT 24/4 = 600 %).
	streamMigration := summary["stream_migration"].(map[string]interface{})
	series := streamMigration["series"].(map[string]interface{})
	assert.Equal(t, sourceCount, series["total"], "Counts must come from Set*Count, not from per-target Marks")
	assert.Equal(t, sourceCount, series["completed"], "Progress must be one bump per source, not per fan-out target")

	rate := series["completion_rate"].(float64)
	assert.GreaterOrEqualf(t, rate, 0.0, "completion_rate must be >= 0, got %v", rate)
	assert.LessOrEqualf(t, rate, 100.0, "completion_rate must stay <= 100%% under fan-out (266%% regression), got %v", rate)
	assert.InDelta(t, 100.0, rate, 1e-9)
}
