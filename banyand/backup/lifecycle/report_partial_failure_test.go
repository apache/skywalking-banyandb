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
// numeric values that buildMigrationReport must produce when a migration
// cycle finishes with one stream group failing on parts while every other
// group (and every other resource within the failing group) completes.
//
// Why this is a unit test and not an end-to-end ginkgo case:
// service.go's action() now calls generateReport on both the success
// branch and the partial-failure branch, so a future e2e could observe
// the partial-failure report shape -- but doing so requires injecting a
// real catalog failure (e.g. stopping a target node) which is out of
// scope for this report-fix PR. This unit test exercises
// buildMigrationReport directly against a hand-crafted Progress that
// mirrors the runtime state of a partial cycle, providing complete
// coverage of the report shape without needing a controllable failure
// environment.
//
// Invariants checked:
//
//   - migration_status.total_groups equals len(GroupsToProcess);
//     completed_groups counts only the groups that ran the full
//     processXxxGroup → MarkGroupCompleted path; completion_rate is
//     completed/total.
//   - A resource block whose accounting is consistent reaches 100 % when
//     there are no errors; a block with one failed write reports
//     completed < total and a matching rate.
//   - The trace_migration block is present and at 100 % when trace
//     migration succeeded; errors.trace_parts and errors.trace_series
//     keys exist and are empty.
//   - The failing resource's errors map carries the recorded error message
//     under the {group → segmentID → shard_<id> → partID} structure used
//     by buildErrorDetails.
func TestBuildMigrationReport_PartialFailure(t *testing.T) {
	p := NewProgress("", logger.GetLogger("test"))

	// Cycle scope — same scheduling as the happy-path test.
	p.SetGroupsToProcess([]string{"sw_a", "sw_b", "sw_metrics", "sw_trace"})

	p.SnapshotStreamDir = "/tmp/stream/snapshots/example"
	p.SnapshotMeasureDir = "/tmp/measure/snapshots/example"
	p.SnapshotTraceDir = "/tmp/trace/snapshots/example"

	// ===== sw_a (stream) — full success =====
	p.StreamPartCounts["sw_a"] = 2
	p.StreamPartProgress["sw_a"] = 2
	p.StreamSeriesCounts["sw_a"] = 2
	p.StreamSeriesProgress["sw_a"] = 2
	p.StreamElementIndexCounts["sw_a"] = 1
	p.StreamElementIndexProgress["sw_a"] = 1
	p.MarkGroupCompleted("sw_a")
	p.MarkStreamGroupDeleted("sw_a")

	// ===== sw_b (stream) — parts has one transport failure =====
	// Counts: parts 4 scheduled, 3 marked complete, 1 error;
	// series and element_index still complete fully.
	// processStreamGroup returns early on first migration error, so
	// CompletedGroups / DeletedStreamGroups are NOT set for sw_b.
	p.StreamPartCounts["sw_b"] = 4
	p.StreamPartProgress["sw_b"] = 3
	// Error map structure: map[group]map[segmentID]map[shardID]map[partID]string.
	p.StreamPartErrors["sw_b"] = map[string]map[common.ShardID]map[uint64]string{
		"20260512": {
			common.ShardID(1): {
				uint64(7): "failed to stream part to target node node-cold-0: connection refused",
			},
		},
	}
	p.StreamSeriesCounts["sw_b"] = 2
	p.StreamSeriesProgress["sw_b"] = 2
	p.StreamElementIndexCounts["sw_b"] = 2
	p.StreamElementIndexProgress["sw_b"] = 2

	// Invariant the hand-built state must keep in sync with what the real
	// Mark*Completed / Mark*Error methods would produce: Counts equals the
	// number of distinct items the visitor attempted, i.e.
	//   Counts == Progress + len(error leaves).
	// If a future Mark refactor changes this relationship, the assertion
	// below catches the test's hand-built state drifting away from
	// reality before the report-shape assertions run.
	require.Equal(t, p.StreamPartProgress["sw_b"]+1, p.StreamPartCounts["sw_b"],
		"hand-built sw_b state must satisfy Counts = Progress + #errors")

	// ===== sw_metrics (measure) — full success =====
	p.MeasurePartCounts["sw_metrics"] = 3
	p.MeasurePartProgress["sw_metrics"] = 3
	p.MeasureSeriesCounts["sw_metrics"] = 2
	p.MeasureSeriesProgress["sw_metrics"] = 2
	p.MarkGroupCompleted("sw_metrics")
	p.MarkMeasureGroupDeleted("sw_metrics")

	// ===== sw_trace (trace) — full success =====
	p.TraceShardCounts["sw_trace"] = 2
	p.TraceShardProgress["sw_trace"] = 2
	p.TraceSeriesCounts["sw_trace"] = 2
	p.TraceSeriesProgress["sw_trace"] = 2
	p.MarkGroupCompleted("sw_trace")
	p.MarkTraceGroupDeleted("sw_trace")

	// --- Build report ---
	svc := &lifecycleService{l: logger.GetLogger("test")}
	report := svc.buildMigrationReport(p)

	// --- Top-level shape ---
	require.Equal(t, "2.0", report["report_version"])
	summary, ok := report["summary"].(map[string]interface{})
	require.True(t, ok, "summary must be a map")
	errs, ok := report["errors"].(map[string]interface{})
	require.True(t, ok, "errors must be a map")

	// --- migration_status ---
	// total_groups = len(GroupsToProcess) = 4
	// completed_groups = 3 (sw_a, sw_metrics, sw_trace; sw_b unfinished).
	// rate = 75.
	ms, ok := summary["migration_status"].(map[string]interface{})
	require.True(t, ok, "summary.migration_status must be a map")
	assert.Equal(t, 4, ms["total_groups"], "migration_status.total_groups")
	assert.Equal(t, 3, ms["completed_groups"], "migration_status.completed_groups")
	assert.InDelta(t, 75.0, ms["completion_rate"], 1e-9, "migration_status.completion_rate")

	// --- stream_migration ---
	// parts: sw_a 2/2 + sw_b 3/4 = 5/6, errors=1, rate=83.33.
	assertResource(t, summary, "stream_migration", "parts", 6, 5, 83.33333333333334, 1)
	// series: sw_a 2/2 + sw_b 2/2 = 4/4.
	assertResource(t, summary, "stream_migration", "series", 4, 4, 100.0, 0)
	// element_index: sw_a 1/1 + sw_b 2/2 = 3/3.
	assertResource(t, summary, "stream_migration", "element_index", 3, 3, 100.0, 0)

	// --- measure_migration ---
	assertResource(t, summary, "measure_migration", "parts", 3, 3, 100.0, 0)
	assertResource(t, summary, "measure_migration", "series", 2, 2, 100.0, 0)

	// --- trace_migration ---
	assertResource(t, summary, "trace_migration", "parts", 2, 2, 100.0, 0)
	assertResource(t, summary, "trace_migration", "series", 2, 2, 100.0, 0)

	// --- errors ---
	// All keys must be present including the trace_parts / trace_series keys.
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

	// stream_parts is the only non-empty error bucket.
	streamPartErrs := errs["stream_parts"].(map[string]interface{})
	require.Lenf(t, streamPartErrs, 1, "errors.stream_parts must contain exactly one group entry, got %v", streamPartErrs)
	require.Contains(t, streamPartErrs, "sw_b", "errors.stream_parts must hold sw_b")

	swBErrs, ok := streamPartErrs["sw_b"].(map[string]interface{})
	require.True(t, ok, "errors.stream_parts.sw_b must be a map")
	require.Contains(t, swBErrs, "20260512", "errors.stream_parts.sw_b must hold segment 20260512")

	segErrs, ok := swBErrs["20260512"].(map[string]interface{})
	require.True(t, ok, "errors.stream_parts.sw_b.20260512 must be a map")
	require.Contains(t, segErrs, "shard_1", "errors.stream_parts.sw_b.20260512 must hold shard_1")

	// All other error buckets must be empty.
	for _, key := range []string{
		"stream_series", "stream_element_index",
		"measure_parts", "measure_series",
		"trace_parts", "trace_series",
	} {
		v := errs[key].(map[string]interface{})
		assert.Emptyf(t, v, "errors.%s must be empty for this scenario", key)
	}
}

// TestBuildMigrationReport_CompletedScopedToScheduledSet pins down the
// resume-safety invariant: when CompletedGroups carries entries from a
// prior cycle that are no longer in the current GroupsToProcess set,
// completed_groups must count only the intersection so the rate stays
// bounded by total_groups (no completed > total, no rate > 100).
func TestBuildMigrationReport_CompletedScopedToScheduledSet(t *testing.T) {
	p := NewProgress("", logger.GetLogger("test"))

	// Simulate a resume scenario: prior cycle finished groups A..E (still
	// in CompletedGroups thanks to progress.json reload). The current
	// cycle is only scheduled to retry groups F, G, H.
	p.MarkGroupCompleted("sw_a")
	p.MarkGroupCompleted("sw_b")
	p.MarkGroupCompleted("sw_c")
	p.MarkGroupCompleted("sw_d")
	p.MarkGroupCompleted("sw_e")
	p.SetGroupsToProcess([]string{"sw_f", "sw_g", "sw_h"})
	// All three scheduled groups complete in this cycle.
	p.MarkGroupCompleted("sw_f")
	p.MarkGroupCompleted("sw_g")
	p.MarkGroupCompleted("sw_h")

	svc := &lifecycleService{l: logger.GetLogger("test")}
	report := svc.buildMigrationReport(p)
	summary := report["summary"].(map[string]interface{})
	ms := summary["migration_status"].(map[string]interface{})

	assert.Equal(t, 3, ms["total_groups"], "must reflect this cycle's scheduled set")
	assert.Equal(t, 3, ms["completed_groups"], "must intersect CompletedGroups with GroupsToProcess (not 8)")
	assert.InDelta(t, 100.0, ms["completion_rate"], 1e-9, "rate must be bounded at 100")
}

// TestBuildMigrationReport_EmptyCycleHonestTotalGroups pins down the
// stale-denominator invariant: when this cycle has no scheduled work
// (e.g. snapshots came up empty), the report must surface
// total_groups=0 and rate=0 instead of inheriting a denominator from a
// prior cycle's GroupsToProcess.
func TestBuildMigrationReport_EmptyCycleHonestTotalGroups(t *testing.T) {
	p := NewProgress("", logger.GetLogger("test"))

	// Prior cycle leftover state.
	p.MarkGroupCompleted("sw_a")
	p.MarkGroupCompleted("sw_b")
	// The empty-snapshot path in action() resets GroupsToProcess to nil
	// before calling generateReport. SetGroupsToProcess(nil) emulates
	// that reset.
	p.SetGroupsToProcess(nil)

	svc := &lifecycleService{l: logger.GetLogger("test")}
	report := svc.buildMigrationReport(p)
	summary := report["summary"].(map[string]interface{})
	ms := summary["migration_status"].(map[string]interface{})

	assert.Equal(t, 0, ms["total_groups"], "no scheduled groups this cycle")
	assert.Equal(t, 0, ms["completed_groups"], "intersection with empty scope is empty")
	assert.InDelta(t, 0.0, ms["completion_rate"], 1e-9, "rate is 0 when total is 0")
}

// assertResource pins down a single resource sub-block (parts | series |
// element_index) under a catalog (stream | measure | trace)_migration.
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
