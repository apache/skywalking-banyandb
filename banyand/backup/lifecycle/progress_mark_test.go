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

	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// TestMarkCompleted_Idempotent verifies that repeated Mark*Completed calls
// for the same (group, segment, shard, partID) bump Counts and Progress
// exactly once across all 7 resource types. This is the invariant that lets
// resume re-walk already-completed items without inflating the denominator.
func TestMarkCompleted_Idempotent(t *testing.T) {
	p := NewProgress("", logger.GetLogger("test"))

	// Stream parts: 3 calls on the same key, then a 4th on a different partID.
	p.MarkStreamPartCompleted("g", "seg", 0, 1)
	p.MarkStreamPartCompleted("g", "seg", 0, 1)
	p.MarkStreamPartCompleted("g", "seg", 0, 1)
	assert.Equal(t, 1, p.StreamPartCounts["g"], "duplicate stream part marks must collapse to 1")
	assert.Equal(t, 1, p.StreamPartProgress["g"])
	assert.True(t, p.IsStreamPartCompleted("g", "seg", 0, 1))
	p.MarkStreamPartCompleted("g", "seg", 0, 2)
	assert.Equal(t, 2, p.StreamPartCounts["g"], "distinct partIDs must each contribute 1")
	assert.Equal(t, 2, p.StreamPartProgress["g"])
	assert.True(t, p.IsStreamPartCompleted("g", "seg", 0, 2))

	// Stream series.
	p.MarkStreamSeriesCompleted("g", "seg", 0)
	p.MarkStreamSeriesCompleted("g", "seg", 0)
	assert.Equal(t, 1, p.StreamSeriesCounts["g"])
	assert.Equal(t, 1, p.StreamSeriesProgress["g"])
	assert.True(t, p.IsStreamSeriesCompleted("g", "seg", 0))

	// Stream element_index.
	p.MarkStreamElementIndexCompleted("g", "seg", 0)
	p.MarkStreamElementIndexCompleted("g", "seg", 0)
	assert.Equal(t, 1, p.StreamElementIndexCounts["g"])
	assert.Equal(t, 1, p.StreamElementIndexProgress["g"])
	assert.True(t, p.IsStreamElementIndexCompleted("g", "seg", 0))

	// Measure parts.
	p.MarkMeasurePartCompleted("g", "seg", 0, 1)
	p.MarkMeasurePartCompleted("g", "seg", 0, 1)
	assert.Equal(t, 1, p.MeasurePartCounts["g"])
	assert.Equal(t, 1, p.MeasurePartProgress["g"])
	assert.True(t, p.IsMeasurePartCompleted("g", "seg", 0, 1))

	// Measure series.
	p.MarkMeasureSeriesCompleted("g", "seg", 0)
	p.MarkMeasureSeriesCompleted("g", "seg", 0)
	assert.Equal(t, 1, p.MeasureSeriesCounts["g"])
	assert.Equal(t, 1, p.MeasureSeriesProgress["g"])
	assert.True(t, p.IsMeasureSeriesCompleted("g", "seg", 0))

	// Trace shard.
	p.MarkTraceShardCompleted("g", "seg", 0)
	p.MarkTraceShardCompleted("g", "seg", 0)
	assert.Equal(t, 1, p.TraceShardCounts["g"])
	assert.Equal(t, 1, p.TraceShardProgress["g"])
	assert.True(t, p.IsTraceShardCompleted("g", "seg", 0))

	// Trace series.
	p.MarkTraceSeriesCompleted("g", "seg", 0)
	p.MarkTraceSeriesCompleted("g", "seg", 0)
	assert.Equal(t, 1, p.TraceSeriesCounts["g"])
	assert.Equal(t, 1, p.TraceSeriesProgress["g"])
	assert.True(t, p.IsTraceSeriesCompleted("g", "seg", 0))
}

// TestMarkError_Idempotent verifies that repeated Mark*Error calls for the
// same key bump Counts exactly once across all 7 error buckets. Counts is
// bumped (not Progress) so the denominator includes failed attempts and
// the report can show completion_rate < 100 honestly.
func TestMarkError_Idempotent(t *testing.T) {
	p := NewProgress("", logger.GetLogger("test"))

	p.MarkStreamPartError("g", "seg", 0, 1, "first")
	p.MarkStreamPartError("g", "seg", 0, 1, "duplicate")
	assert.Equal(t, 1, p.StreamPartCounts["g"])
	assert.Equal(t, 0, p.StreamPartProgress["g"], "errors must not bump Progress")

	p.MarkStreamSeriesError("g", "seg", 0, "first")
	p.MarkStreamSeriesError("g", "seg", 0, "duplicate")
	assert.Equal(t, 1, p.StreamSeriesCounts["g"])
	assert.Equal(t, 0, p.StreamSeriesProgress["g"], "errors must not bump Progress")

	p.MarkStreamElementIndexError("g", "seg", 0, "first")
	p.MarkStreamElementIndexError("g", "seg", 0, "duplicate")
	assert.Equal(t, 1, p.StreamElementIndexCounts["g"])
	assert.Equal(t, 0, p.StreamElementIndexProgress["g"], "errors must not bump Progress")

	p.MarkMeasurePartError("g", "seg", 0, 1, "first")
	p.MarkMeasurePartError("g", "seg", 0, 1, "duplicate")
	assert.Equal(t, 1, p.MeasurePartCounts["g"])
	assert.Equal(t, 0, p.MeasurePartProgress["g"], "errors must not bump Progress")

	p.MarkMeasureSeriesError("g", "seg", 0, "first")
	p.MarkMeasureSeriesError("g", "seg", 0, "duplicate")
	assert.Equal(t, 1, p.MeasureSeriesCounts["g"])
	assert.Equal(t, 0, p.MeasureSeriesProgress["g"], "errors must not bump Progress")

	p.MarkTraceShardError("g", "seg", 0, "first")
	p.MarkTraceShardError("g", "seg", 0, "duplicate")
	assert.Equal(t, 1, p.TraceShardCounts["g"])
	assert.Equal(t, 0, p.TraceShardProgress["g"], "errors must not bump Progress")

	p.MarkTraceSeriesError("g", "seg", 0, "first")
	p.MarkTraceSeriesError("g", "seg", 0, "duplicate")
	assert.Equal(t, 1, p.TraceSeriesCounts["g"])
	assert.Equal(t, 0, p.TraceSeriesProgress["g"], "errors must not bump Progress")
}

// TestClearErrors_AdjustsCountsForRetry pins down the retry invariant:
// after ClearErrors, the count contribution from cleared errors must be
// subtracted so a successful retry's Mark*Completed can re-bump Counts
// without producing a denominator that grows beyond the actual workload.
// This guards against the >100 % completion rates the original report had.
// Trace is exercised alongside stream because the original ClearErrors
// completely missed the trace error maps -- the most regression-prone
// path post-fix.
func TestClearErrors_AdjustsCountsForRetry(t *testing.T) {
	p := NewProgress("", logger.GetLogger("test"))

	// --- Stream part path ---
	// Cycle 1: one stream part succeeds, one fails. counts=2, progress=1.
	p.MarkStreamPartCompleted("g", "seg", 0, 1)
	p.MarkStreamPartError("g", "seg", 0, 2, "transport error")
	assert.Equal(t, 2, p.StreamPartCounts["g"])
	assert.Equal(t, 1, p.StreamPartProgress["g"])

	// --- Trace shard path (the regression target ClearErrors must keep correct) ---
	p.MarkTraceShardCompleted("trg", "tseg", 0)
	p.MarkTraceShardError("trg", "tseg", 1, "trace transport error")
	assert.Equal(t, 2, p.TraceShardCounts["trg"])
	assert.Equal(t, 1, p.TraceShardProgress["trg"])

	// action() calls ClearErrors at the top of each cycle.
	p.ClearErrors()

	// Stream: cleared error subtracted; success retained.
	assert.Equal(t, 1, p.StreamPartCounts["g"], "ClearErrors must subtract cleared stream error contribution")
	assert.Equal(t, 1, p.StreamPartProgress["g"])
	assert.Empty(t, p.StreamPartErrors["g"])

	// Trace: cleared error subtracted; success retained.
	assert.Equal(t, 1, p.TraceShardCounts["trg"], "ClearErrors must subtract cleared trace error contribution")
	assert.Equal(t, 1, p.TraceShardProgress["trg"])
	assert.Empty(t, p.TraceShardErrors["trg"])

	// Cycle 2 retry: the previously-failed items now succeed.
	p.MarkStreamPartCompleted("g", "seg", 0, 2)
	assert.Equal(t, 2, p.StreamPartCounts["g"], "after stream retry counts back to 2 (parts 1+2)")
	assert.Equal(t, 2, p.StreamPartProgress["g"])

	p.MarkTraceShardCompleted("trg", "tseg", 1)
	assert.Equal(t, 2, p.TraceShardCounts["trg"], "after trace retry counts back to 2 (shards 0+1)")
	assert.Equal(t, 2, p.TraceShardProgress["trg"])
}

// TestClearErrors_HandlesAllSevenBuckets is the contract for the trace
// error fix: a previous version of ClearErrors only reset 5 stream/measure
// error maps, leaving TraceShardErrors and TraceSeriesErrors untouched.
// Stale trace errors then survived across cycles and (because the new
// Mark*Error semantic bumps Counts) inflated the trace_migration
// denominator on every retry. This test fails on the pre-fix code.
func TestClearErrors_HandlesAllSevenBuckets(t *testing.T) {
	p := NewProgress("", logger.GetLogger("test"))

	p.MarkStreamPartError("g", "seg", 0, 1, "x")
	p.MarkStreamSeriesError("g", "seg", 0, "x")
	p.MarkStreamElementIndexError("g", "seg", 0, "x")
	p.MarkMeasurePartError("g", "seg", 0, 1, "x")
	p.MarkMeasureSeriesError("g", "seg", 0, "x")
	p.MarkTraceShardError("g", "seg", 0, "x")
	p.MarkTraceSeriesError("g", "seg", 0, "x")

	for _, c := range []int{
		p.StreamPartCounts["g"], p.StreamSeriesCounts["g"], p.StreamElementIndexCounts["g"],
		p.MeasurePartCounts["g"], p.MeasureSeriesCounts["g"],
		p.TraceShardCounts["g"], p.TraceSeriesCounts["g"],
	} {
		assert.Equal(t, 1, c, "each error bucket must bump its Counts by 1")
	}

	p.ClearErrors()

	assert.Empty(t, p.StreamPartErrors["g"], "stream_parts errors must be cleared")
	assert.Empty(t, p.StreamSeriesErrors["g"], "stream_series errors must be cleared")
	assert.Empty(t, p.StreamElementIndexErrors["g"], "stream_element_index errors must be cleared")
	assert.Empty(t, p.MeasurePartErrors["g"], "measure_parts errors must be cleared")
	assert.Empty(t, p.MeasureSeriesErrors["g"], "measure_series errors must be cleared")
	assert.Empty(t, p.TraceShardErrors["g"], "trace_parts errors must be cleared")
	assert.Empty(t, p.TraceSeriesErrors["g"], "trace_series errors must be cleared")

	for _, c := range []int{
		p.StreamPartCounts["g"], p.StreamSeriesCounts["g"], p.StreamElementIndexCounts["g"],
		p.MeasurePartCounts["g"], p.MeasureSeriesCounts["g"],
		p.TraceShardCounts["g"], p.TraceSeriesCounts["g"],
	} {
		assert.Equal(t, 0, c, "each Counts must be subtracted back to 0 after ClearErrors")
	}
}
