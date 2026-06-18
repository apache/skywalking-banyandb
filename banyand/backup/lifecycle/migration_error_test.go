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
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

// TestFormatIntervalRule pins the compact interval rendering for the report's
// interval field across day and hour granularities.
func TestFormatIntervalRule(t *testing.T) {
	assert.Equal(t, "5d", formatIntervalRule(storage.IntervalRule{Unit: storage.DAY, Num: 5}))
	assert.Equal(t, "1d", formatIntervalRule(storage.IntervalRule{Unit: storage.DAY, Num: 1}))
	assert.Equal(t, "1h", formatIntervalRule(storage.IntervalRule{Unit: storage.HOUR, Num: 1}))
	assert.Equal(t, "12h", formatIntervalRule(storage.IntervalRule{Unit: storage.HOUR, Num: 12}))
}

// TestSegmentErrorLocation pins the source segment directory name + interval
// derivation, the inverse of storage.ParseSegmentTime, for day and hour units.
func TestSegmentErrorLocation(t *testing.T) {
	start := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	dayTR := &timestamp.TimeRange{Start: start, End: start.Add(5 * 24 * time.Hour)}
	seg, interval := segmentErrorLocation(dayTR, storage.IntervalRule{Unit: storage.DAY, Num: 5})
	assert.Equal(t, "seg-20260601", seg)
	assert.Equal(t, "5d", interval)

	hourTR := &timestamp.TimeRange{Start: start, End: start.Add(time.Hour)}
	seg, interval = segmentErrorLocation(hourTR, storage.IntervalRule{Unit: storage.HOUR, Num: 1})
	assert.Equal(t, "seg-2026060100", seg)
	assert.Equal(t, "1h", interval)

	// Nil time range (e.g. a node-scoped error) yields empty segment/interval.
	seg, interval = segmentErrorLocation(nil, storage.IntervalRule{Unit: storage.DAY, Num: 5})
	assert.Empty(t, seg)
	assert.Empty(t, interval)
}

// TestErrorBucketKey pins the (catalog, scope) -> report bucket mapping for
// every supported combination, including the cross-catalog node bucket.
func TestErrorBucketKey(t *testing.T) {
	cases := []struct {
		catalog, scope, want string
	}{
		{catalogMeasure, scopePart, "measure_parts"},
		{catalogMeasure, scopeSeries, "measure_series"},
		{catalogStream, scopePart, "stream_parts"},
		{catalogStream, scopeSeries, "stream_series"},
		{catalogStream, scopeElementIndex, "stream_element_index"},
		{catalogTrace, scopeShard, "trace_parts"},
		{catalogTrace, scopeSeries, "trace_series"},
		{catalogMeasure, scopeNode, "row_replay_node_errors"},
		{catalogStream, scopeNode, "row_replay_node_errors"},
		{catalogTrace, scopeNode, "row_replay_node_errors"},
		{"unknown", "unknown", ""},
	}
	for _, c := range cases {
		assert.Equalf(t, c.want, errorBucketKey(c.catalog, c.scope), "errorBucketKey(%q,%q)", c.catalog, c.scope)
	}
}

// TestVisitorRecordError verifies each visitor's recordError builds a complete
// MigrationError: stage/group/catalog/scope from the visitor, segment dir name +
// interval from the source segment, and a shard pointer (so shard 0 emits) while
// the part pointer is honored or nil per scope.
func TestVisitorRecordError(t *testing.T) {
	require.NoError(t, logger.Init(logger.Logging{Env: "dev", Level: "error"}))
	start := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	segTR := &timestamp.TimeRange{Start: start, End: start.Add(5 * 24 * time.Hour)}
	grp := &commonv1.Group{Metadata: &commonv1.Metadata{Name: "sw_metricsHour"}}
	dayInterval := storage.IntervalRule{Unit: storage.DAY, Num: 5}

	t.Run("measure_part_with_shard_zero", func(t *testing.T) {
		p := NewProgress("", logger.GetLogger("test"))
		mv := newMeasureMigrationVisitor(grp, 1, 0, nil, nil, logger.GetLogger("test"), p, 0, dayInterval, nil,
			"hot", "warm", dayInterval, orphanConfig{})
		part := uint64(1)
		mv.recordError(scopePart, segTR, 0, &part, "boom")
		require.Len(t, p.MigrationErrors, 1)
		e := p.MigrationErrors[0]
		assert.Equal(t, "hot", e.SourceStage)
		assert.Equal(t, "warm", e.TargetStage)
		assert.Equal(t, "sw_metricsHour", e.Group)
		assert.Equal(t, catalogMeasure, e.Catalog)
		assert.Equal(t, scopePart, e.Scope)
		assert.Equal(t, "seg-20260601", e.Segment)
		assert.Equal(t, "5d", e.Interval)
		require.NotNil(t, e.Shard)
		assert.Equal(t, uint32(0), *e.Shard, "shard 0 must be emitted, not omitted")
		require.NotNil(t, e.Part)
		assert.Equal(t, uint64(1), *e.Part)
		assert.Equal(t, "boom", e.Error)
	})

	t.Run("measure_series_has_no_part", func(t *testing.T) {
		p := NewProgress("", logger.GetLogger("test"))
		mv := newMeasureMigrationVisitor(grp, 1, 0, nil, nil, logger.GetLogger("test"), p, 0, dayInterval, nil,
			"hot", "warm", dayInterval, orphanConfig{})
		mv.recordError(scopeSeries, segTR, 3, nil, "series boom")
		require.Len(t, p.MigrationErrors, 1)
		e := p.MigrationErrors[0]
		assert.Equal(t, scopeSeries, e.Scope)
		require.NotNil(t, e.Shard)
		assert.Equal(t, uint32(3), *e.Shard)
		assert.Nil(t, e.Part, "series-scoped error must omit part")
	})

	t.Run("trace_shard_catalog", func(t *testing.T) {
		p := NewProgress("", logger.GetLogger("test"))
		tv := newTraceMigrationVisitor(grp, 1, 0, nil, nil, logger.GetLogger("test"), p, 0, dayInterval, nil,
			"warm", "cold", dayInterval)
		tv.recordError(scopeShard, segTR, 2, "trace boom")
		require.Len(t, p.MigrationErrors, 1)
		e := p.MigrationErrors[0]
		assert.Equal(t, catalogTrace, e.Catalog)
		assert.Equal(t, scopeShard, e.Scope)
		assert.Equal(t, "warm", e.SourceStage)
		assert.Equal(t, "cold", e.TargetStage)
	})
}

// TestAddMigrationError_DedupByLocation pins the resume-safety invariant: a
// re-errored artifact (same catalog/scope/group/segment/shard/part) overwrites
// the prior entry with the latest message instead of accumulating a duplicate,
// while a different node (or part) is kept as a distinct entry.
func TestAddMigrationError_DedupByLocation(t *testing.T) {
	p := NewProgress("", logger.GetLogger("test"))
	shard0, part1, part2 := uint32(0), uint64(1), uint64(2)
	base := MigrationError{
		SourceStage: "hot", TargetStage: "warm", Group: "g",
		Catalog: catalogMeasure, Scope: scopePart, Segment: "seg-20260601", Interval: "5d",
		Shard: &shard0, Part: &part1, Error: "attempt 1",
	}
	p.AddMigrationError(base)
	again := base
	again.Error = "attempt 2 after resume"
	p.AddMigrationError(again)
	require.Len(t, p.MigrationErrors, 1, "same location must overwrite, not duplicate")
	assert.Equal(t, "attempt 2 after resume", p.MigrationErrors[0].Error)

	// A different part on the same shard is a distinct location.
	other := base
	other.Part = &part2
	other.Error = "other part"
	p.AddMigrationError(other)
	require.Len(t, p.MigrationErrors, 2)

	// Node-scoped entries dedupe by node id.
	n := MigrationError{Group: "g", Catalog: catalogStream, Scope: scopeNode, Node: "data-1", Error: "x"}
	p.AddMigrationError(n)
	n.Error = "y"
	p.AddMigrationError(n)
	require.Len(t, p.MigrationErrors, 3, "same node must overwrite")
	assert.Equal(t, "y", p.MigrationErrors[2].Error)
}

// TestAddMigrationError_Concurrent defensively validates that AddMigrationError's
// p.mu guard keeps the dedup-and-append race-free. The migration loop calls it
// sequentially today, so this guards against a future concurrent caller rather
// than an existing one. Each goroutine owns a disjoint key space (keyed by worker
// id) and records each location twice to drive the overwrite branch concurrently
// too. An empty progress path makes saveProgress a no-op, keeping this a pure
// in-memory check with bounded work that runs fine on a single, busy core.
func TestAddMigrationError_Concurrent(t *testing.T) {
	p := NewProgress("", logger.GetLogger("test"))
	const workers, perWorker = 8, 50
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				shard := uint32(i)
				e := MigrationError{
					Group: "g", Catalog: catalogMeasure, Scope: scopePart,
					Segment: fmt.Sprintf("seg-w%d-i%d", w, i), Shard: &shard, Error: "boom",
				}
				p.AddMigrationError(e)
				e.Error = "boom-again" // same location -> overwrite branch
				p.AddMigrationError(e)
			}
		}(w)
	}
	wg.Wait()
	assert.Len(t, p.MigrationErrors, workers*perWorker, "each distinct location recorded exactly once")
}

// TestBuildMigrationReport_ErrorFormat is the end-to-end format check: it seeds
// one MigrationError of every (catalog, scope) the migration can produce, builds
// the report, JSON round-trips it, and asserts the final wire shape — the 8
// stable buckets, each an array; populated entries carry every field with the
// right values; empty buckets serialize as []; and a node-scoped entry omits
// segment/interval/shard/part while carrying node.
func TestBuildMigrationReport_ErrorFormat(t *testing.T) {
	p := NewProgress("", logger.GetLogger("test"))
	shard0, part1 := uint32(0), uint64(1)
	p.MigrationErrors = []MigrationError{
		{
			SourceStage: "hot", TargetStage: "warm", Group: "sw_metricsHour",
			Catalog: catalogMeasure, Scope: scopePart, Segment: "seg-20260601", Interval: "5d",
			Shard: &shard0, Part: &part1, Error: "row-replay skipped 377 unresolved rows",
		},
		{
			SourceStage: "hot", TargetStage: "warm", Group: "sw_metricsHour",
			Catalog: catalogMeasure, Scope: scopeSeries, Segment: "seg-20260601", Interval: "5d",
			Shard: &shard0, Error: "open series file failed",
		},
		{
			SourceStage: "warm", TargetStage: "cold", Group: "sw_stream",
			Catalog: catalogStream, Scope: scopeNode, Node: "data-node-1", Error: "flush timeout",
		},
	}

	svc := &lifecycleService{l: logger.GetLogger("test")}
	report := svc.buildMigrationReport(p)

	// JSON round-trip to assert the final wire shape, not just the Go map.
	raw, err := json.Marshal(report)
	require.NoError(t, err)
	var decoded map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(raw, &decoded))
	var errs map[string][]map[string]interface{}
	require.NoError(t, json.Unmarshal(decoded["errors"], &errs))

	// All 8 buckets present; empty ones are [] (non-nil), not null/object.
	for _, key := range []string{
		"measure_parts", "measure_series", "stream_parts", "stream_series",
		"stream_element_index", "trace_parts", "trace_series", "row_replay_node_errors",
	} {
		v, ok := errs[key]
		require.Truef(t, ok, "errors.%s must be present as an array", key)
		require.NotNilf(t, v, "errors.%s must serialize as [] not null", key)
	}
	assert.Len(t, errs["measure_parts"], 1)
	assert.Len(t, errs["measure_series"], 1)
	assert.Len(t, errs["row_replay_node_errors"], 1)
	assert.Empty(t, errs["stream_parts"])
	assert.Empty(t, errs["trace_series"])

	// Populated measure-part entry carries every field with the right values.
	mp := errs["measure_parts"][0]
	assert.Equal(t, "hot", mp["source_stage"])
	assert.Equal(t, "warm", mp["target_stage"])
	assert.Equal(t, "sw_metricsHour", mp["group"])
	assert.Equal(t, "measure", mp["catalog"])
	assert.Equal(t, "part", mp["scope"])
	assert.Equal(t, "seg-20260601", mp["segment"])
	assert.Equal(t, "5d", mp["interval"])
	assert.Equal(t, float64(0), mp["shard"], "shard 0 must be present in JSON")
	assert.Equal(t, float64(1), mp["part"])
	assert.Equal(t, "row-replay skipped 377 unresolved rows", mp["error"])
	_, hasNode := mp["node"]
	assert.False(t, hasNode, "part entry must omit node")

	// Series entry omits part but keeps shard.
	ms := errs["measure_series"][0]
	assert.Equal(t, "series", ms["scope"])
	_, hasPart := ms["part"]
	assert.False(t, hasPart, "series entry must omit part")
	assert.Equal(t, float64(0), ms["shard"])

	// Node entry omits segment/interval/shard/part but carries node.
	ne := errs["row_replay_node_errors"][0]
	assert.Equal(t, "node", ne["scope"])
	assert.Equal(t, "data-node-1", ne["node"])
	for _, omitted := range []string{"segment", "interval", "shard", "part"} {
		_, has := ne[omitted]
		assert.Falsef(t, has, "node entry must omit %q", omitted)
	}
}

// TestBuildMigrationReport_OrphansSection pins the report's orphans section: it
// carries the configured policy plus a per catalog -> group -> deleted subject
// row-count breakdown, and round-trips through JSON in that shape.
func TestBuildMigrationReport_OrphansSection(t *testing.T) {
	p := NewProgress("", logger.GetLogger("test"))
	p.AddOrphanRows("sw_metricsHour", catalogMeasure, map[string]uint64{"meter_deleted_hour": 5})
	p.AddOrphanRows("sw_stream", catalogStream, map[string]uint64{"deleted_stream": 2})

	svc := &lifecycleService{l: logger.GetLogger("test"), orphanPolicyStr: "archive"}
	report := svc.buildMigrationReport(p)

	raw, err := json.Marshal(report)
	require.NoError(t, err)
	var decoded map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(raw, &decoded))
	var orphans map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(decoded["orphans"], &orphans))

	var policy string
	require.NoError(t, json.Unmarshal(orphans["policy"], &policy))
	assert.Equal(t, "archive", policy)

	var measure map[string]map[string]uint64
	require.NoError(t, json.Unmarshal(orphans["measure"], &measure))
	assert.Equal(t, uint64(5), measure["sw_metricsHour"]["meter_deleted_hour"])

	var stream map[string]map[string]uint64
	require.NoError(t, json.Unmarshal(orphans["stream"], &stream))
	assert.Equal(t, uint64(2), stream["sw_stream"]["deleted_stream"])
}
