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

package main

import (
	"fmt"
	"math"
	"math/rand"
	"path/filepath"
	"testing"
	"time"
)

// timeZero is a fixed stamp so summarize output is deterministic in tests.
var timeZero = time.Unix(0, 0).UTC()

// collectStreamIdxTags renders the per-series tags the fixture would emit, so
// determinism can be asserted without a live BanyanDB.
func collectStreamIdxTags(series, perSeries int) []string {
	out := make([]string, 0, series)
	for idx := 0; idx < series; idx++ {
		out = append(out, fmt.Sprintf("%s|%s", streamIdxServiceID(idx), streamIdxPodForSeries(idx)))
	}
	_ = perSeries
	return out
}

func collectStreamIdxLevels(n int, counts map[string]int) {
	//nolint:gosec // G404: the fixture's determinism is the point.
	rnd := rand.New(rand.NewSource(streamIdxFixtureSeed))
	for i := 0; i < n; i++ {
		counts[streamIdxPickLevel(rnd)]++
	}
}

func collectStreamIdxDurations(n int) []int64 {
	//nolint:gosec // G404: the fixture's determinism is the point.
	rnd := rand.New(rand.NewSource(streamIdxFixtureSeed))
	out := make([]int64, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, streamIdxDuration(rnd))
	}
	return out
}

// The fixture spreads elements across a fixed window; a query far outside that
// window must be classifiable as disjoint, which is what makes the
// class="disjoint" branch of the native time pruner observable. Assert the
// spread actually spans the configured window.
func TestStreamIdxTimestampsSpanTheFixtureWindow(t *testing.T) {
	total := 1000
	base := time.Unix(1_700_000_000, 0).UTC()
	first := streamIdxElementTimestamp(base, 0, total)
	last := streamIdxElementTimestamp(base, total-1, total)
	// The last element sits at (total-1)/total of the window, so the observed
	// span is a hair under the configured spread. The assertion only needs to
	// separate "spread across the window" from "clustered at one instant".
	span := last.Sub(first)
	if span < streamIdxFixtureSpread-time.Hour {
		t.Errorf("fixture spans %s, want most of %s", span, streamIdxFixtureSpread)
	}
	if !first.Equal(base) {
		t.Errorf("first element timestamp = %s, want the base %s", first, base)
	}
}

// Two elements must never share a timestamp, or the numeric trie has no
// granularity to prune on within the fixture.
func TestStreamIdxTimestampsAreMonotonic(t *testing.T) {
	total := 500
	base := time.Unix(1_700_000_000, 0).UTC()
	prev := streamIdxElementTimestamp(base, 0, total)
	for i := 1; i < total; i++ {
		next := streamIdxElementTimestamp(base, i, total)
		if !next.After(prev) {
			t.Fatalf("element %d timestamp %s is not after %s", i, next, prev)
		}
		prev = next
	}
}

// The catalog's point-lookup anchor relies on the trace id being unique per
// element; a collision would make filter_message/trace lookups return more than
// one row and parity would stop being meaningful.
func TestStreamIdxTraceIDsAreUnique(t *testing.T) {
	seen := make(map[string]struct{}, 2000)
	//nolint:gosec // G404: the fixture's determinism is the point.
	rnd := rand.New(rand.NewSource(streamIdxFixtureSeed))
	for version := 1; version <= 2000; version++ {
		seriesIdx := rnd.Intn(1000)
		traceID := fmt.Sprintf("trace-%08d-%06d", version, seriesIdx%1000)
		if _, dup := seen[traceID]; dup {
			t.Fatalf("trace id %q generated twice", traceID)
		}
		seen[traceID] = struct{}{}
	}
}

// Index rules must carry a non-zero Id and distinct names, because the Id is
// the element-index field key: a zero or duplicated id makes the field
// unfindable or makes two rules collide on one key.
func TestStreamIdxIndexRulesAreWellFormed(t *testing.T) {
	ids := make(map[uint32]struct{}, len(streamIdxIndexRules))
	names := make(map[string]struct{}, len(streamIdxIndexRules))
	sortable := 0
	for _, def := range streamIdxIndexRules {
		if def.id == 0 {
			t.Errorf("index rule %q has id 0, which cannot key the element index", def.name)
		}
		if _, dup := ids[def.id]; dup {
			t.Errorf("index rule id %d is used twice", def.id)
		}
		ids[def.id] = struct{}{}
		if _, dup := names[def.name]; dup {
			t.Errorf("index rule name %q is used twice", def.name)
		}
		names[def.name] = struct{}{}
		if len(def.tags) == 0 {
			t.Errorf("index rule %q has no tags", def.name)
		}
		if !def.noSort {
			sortable++
		}
	}
	// Exactly one sortable rule: duration, the field the SORT_* catalog queries
	// pin. More would mean every rule carries a doc-value column.
	if sortable != 1 {
		t.Errorf("found %d sortable index rules, want exactly 1 (%s)", sortable, streamIdxSortRule)
	}
}

// The sort rule the catalog pins must actually exist in the rule set.
func TestStreamIdxSortRuleIsDefined(t *testing.T) {
	for _, def := range streamIdxIndexRules {
		if def.name == streamIdxSortRule {
			if def.noSort {
				t.Errorf("sort rule %q is marked no_sort; the ordered query would find no field", streamIdxSortRule)
			}
			return
		}
	}
	t.Errorf("sort rule %q is not defined in streamIdxIndexRules", streamIdxSortRule)
}

// An unchecked float -> time.Duration conversion silently queries the wrong
// range: 1e-20 truncates to zero, and >=2^31 hours overflows to MinInt64,
// after which `until.Add(-window)` wraps into a ~292-year lookback. Both must
// be rejected at LOAD time, not discovered by a query that returns everything.
func TestStreamIdxWindowDurationRejectsUnusableValues(t *testing.T) {
	bad := []struct {
		name  string
		hours float64
	}{
		{"negative", -1},
		{"below duration resolution", 1e-20},
		{"overflows int64 hours", 2562048},
		{"absurdly large", 1e100},
		{"NaN", math.NaN()},
	}
	for _, tc := range bad {
		if d, err := streamIdxWindowDuration(tc.hours); err == nil {
			t.Errorf("streamIdxWindowDuration(%v) [%s] = %v, want an error", tc.hours, tc.name, d)
		}
	}
	for _, hours := range []float64{0, 0.25, 1, 24, 168} {
		d, err := streamIdxWindowDuration(hours)
		if err != nil {
			t.Errorf("streamIdxWindowDuration(%v) returned error: %v", hours, err)
		}
		if hours == 0 && d != 0 {
			t.Errorf("streamIdxWindowDuration(0) = %v, want 0 (inherit the caller's window)", d)
		}
		if hours > 0 && d != time.Duration(hours*float64(time.Hour)) {
			t.Errorf("streamIdxWindowDuration(%v) = %v", hours, d)
		}
	}
}

// The catalog on disk must survive load-time validation, or every run fails at
// startup for a reason unrelated to the index.
func TestStreamIndexCatalogWindowsAreValid(t *testing.T) {
	entries, err := loadStreamCatalog(filepath.Join("catalog", "stream_index.json"))
	if err != nil {
		t.Fatalf("load catalog: %v", err)
	}
	narrow := 0
	for _, entry := range entries {
		if entry.WindowHours > 0 {
			narrow++
			if entry.window <= 0 {
				t.Errorf("entry %q has windowHours=%v but a zero window", entry.ID, entry.WindowHours)
			}
		} else if entry.window != 0 {
			t.Errorf("entry %q has no windowHours but window=%v", entry.ID, entry.window)
		}
	}
	if narrow == 0 {
		t.Error("no catalog entry uses a narrow window; the time pruner can never be exercised")
	}
}

// The chunking exists so the fixture becomes several time-disjoint segments
// instead of one 24h segment. Two properties make that work, and both must hold:
// the slices must COVER every element exactly once (no gaps, no double-writes),
// and each slice must be contiguous in the timestamp ordering (a slice that
// wrapped the timeline would produce an overlapping, not disjoint, segment).
func TestStreamIdxChunkSizeCoversEveryElementOnce(t *testing.T) {
	for _, total := range []int{1, 2, 7, 96, 100, 14_400, 500_000} {
		for _, chunks := range []int{1, 2, 96, 128} {
			size := streamIdxChunkSize(total, chunks)
			if size < 1 {
				t.Fatalf("total=%d chunks=%d: size %d < 1", total, chunks, size)
			}
			covered, seen := 0, make([]int, total)
			for start := 0; start < total; start += size {
				end := start + size
				if end > total {
					end = total
				}
				for i := start; i < end; i++ {
					seen[i]++
				}
				covered += end - start
			}
			if covered != total {
				t.Errorf("total=%d chunks=%d size=%d: covered %d, want %d", total, chunks, size, covered, total)
			}
			// No break after Fatalf: it calls runtime.Goexit, so anything
			// after it is unreachable.
			for i, n := range seen {
				if n != 1 {
					t.Fatalf("total=%d chunks=%d size=%d: element %d written %d times", total, chunks, size, i, n)
				}
			}
		}
	}
}

// Each chunk must map to a contiguous, non-overlapping span of the fixture
// window. That is precisely what makes a segment disjoint from a query window,
// so assert the slices partition the timeline rather than merely the element
// list.
func TestStreamIdxChunksPartitionTheTimeline(t *testing.T) {
	total, chunks := 14_400, 96
	size := streamIdxChunkSize(total, chunks)
	base := time.Unix(1_700_000_000, 0).UTC()
	prevEnd := base
	first := true
	for start := 0; start < total; start += size {
		end := start + size
		if end > total {
			end = total
		}
		sliceStart := streamIdxElementTimestamp(base, start, total)
		sliceEnd := streamIdxElementTimestamp(base, end-1, total)
		if !sliceStart.After(prevEnd) && !first {
			t.Fatalf("chunk [%d,%d) starts at %s, not after the previous slice end %s", start, end, sliceStart, prevEnd)
		}
		first = false
		prevEnd = sliceEnd
	}
}

// More chunks must mean smaller slices, never the same segment.
func TestStreamIdxMoreChunksMeansShorterSlices(t *testing.T) {
	prev := streamIdxChunkSize(1_000_000, 1)
	for _, chunks := range []int{2, 8, 32, 96, 256} {
		size := streamIdxChunkSize(1_000_000, chunks)
		if size >= prev {
			t.Errorf("chunks=%d gave size %d, not smaller than %d", chunks, size, prev)
		}
		prev = size
	}
}
