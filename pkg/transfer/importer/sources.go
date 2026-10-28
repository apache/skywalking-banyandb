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

package importer

import (
	"cmp"
	"fmt"
	"math"
	"slices"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/manifest"
)

// The reasons of a source decision.
const (
	// ReasonGap: the source covers time no earlier choice covers.
	ReasonGap = "gap"
	// ReasonSubset: one chosen source covers the source's time and the counts agree.
	ReasonSubset = "subset"
	// ReasonCountSpread: covered by a chosen source whose row count differs too much; pushed too.
	ReasonCountSpread = "count-spread"
	// ReasonNoSingleCovering: covered only by several chosen sources together; pushed too.
	ReasonNoSingleCovering = "no-single-covering"
	// ReasonEmpty: the source has no rows or no parts.
	ReasonEmpty = "empty"
	// ReasonUnusableRange: some part has no usable time range, so every source is pushed.
	ReasonUnusableRange = "unusable-range"
	// ReasonAllSources: --all-sources pushes every source.
	ReasonAllSources = "all-sources"
)

// Count-spread thresholds of stream and trace: a covered source is pushed too when its row
// count differs from its covering source's by more than both.
const (
	countSpreadRelative = 0.05
	countSpreadAbsolute = 64
)

// SourceInput is one source node's copy of a shard.
type SourceInput struct {
	Node       string
	Parts      []manifest.Part
	TotalCount uint64
}

// SourceDecision is the verdict on one source.
type SourceDecision struct {
	Node string `json:"node"`
	// Reason is one of the Reason* constants.
	Reason string `json:"reason"`
	// Ref is the chosen source a subset or count-spread source was compared with.
	Ref    string `json:"ref,omitempty"`
	Chosen bool   `json:"chosen"`
}

// Selection is the result of SelectSources.
type Selection struct {
	// Chosen are the nodes to push, in node order.
	Chosen []string
	// Decisions has one entry per input source, in node order.
	Decisions []SourceDecision
	// Warnings explain the sources pushed although another source covers them.
	Warnings []string
}

type interval struct{ lo, hi int64 }

type candidate struct {
	node     string
	coverage []interval
	length   uint64
	rows     uint64
}

// SelectSources picks the sources of a shard to push. Empty sources are
// dropped; the rest are sorted by (coverage length, rows, node name) descending and chosen
// greedily while they cover time the chosen ones do not; a skipped source is pushed too when
// no single chosen source covers it, or, for stream and trace, when its row count differs
// from the smallest single covering source by more than 5% and 64 rows. A stream or trace gap
// source that also overlaps the chosen sources is warned about. A part without a
// usable time range makes every non-empty source chosen.
func SelectSources(catalog commonv1.Catalog, sources []SourceInput) Selection {
	sel := Selection{}
	decisions := map[string]SourceDecision{}
	var cands []candidate
	unusable := false
	for _, s := range sources {
		if s.TotalCount == 0 || len(s.Parts) == 0 {
			decisions[s.Node] = SourceDecision{Node: s.Node, Reason: ReasonEmpty}
			continue
		}
		cov, ok := coverage(s.Parts)
		if !ok {
			unusable = true
		}
		cands = append(cands, candidate{node: s.Node, coverage: cov, length: coverageLength(cov), rows: s.TotalCount})
	}
	if unusable {
		for _, c := range cands {
			decisions[c.node] = SourceDecision{Node: c.node, Reason: ReasonUnusableRange, Chosen: true}
		}
		if len(cands) > 1 {
			sel.Warnings = append(sel.Warnings, "some part has no usable time range, so every source is pushed")
		}
		return finish(sel, decisions)
	}
	slices.SortFunc(cands, func(a, b candidate) int {
		return cmp.Or(cmp.Compare(b.length, a.length), cmp.Compare(b.rows, a.rows), cmp.Compare(b.node, a.node))
	})
	var covered []interval
	var chosen, skipped []candidate
	for _, c := range cands {
		fresh := subtract(c.coverage, covered)
		if len(chosen) == 0 || len(fresh) > 0 {
			// Measure dedups by version on the target, so only stream and trace duplicate rows
			// in the window a gap source shares with the chosen ones.
			if len(chosen) > 0 && catalog != commonv1.Catalog_CATALOG_MEASURE && coverageLength(fresh) < c.length {
				sel.Warnings = append(sel.Warnings, fmt.Sprintf(
					"source %s covers time no chosen source covers but also overlaps chosen sources; rows in the overlap are pushed more than once", c.node))
			}
			chosen = append(chosen, c)
			covered = union(covered, c.coverage)
			decisions[c.node] = SourceDecision{Node: c.node, Reason: ReasonGap, Chosen: true}
			continue
		}
		skipped = append(skipped, c)
	}
	// The rescue pass only revisits skipped sources: it confirms what the greedy pass dropped.
	for _, s := range skipped {
		ref, found := smallestCovering(s, chosen)
		switch {
		case !found:
			decisions[s.node] = SourceDecision{Node: s.node, Reason: ReasonNoSingleCovering, Chosen: true}
			sel.Warnings = append(sel.Warnings, fmt.Sprintf("source %s is covered only by several chosen sources together, so it is pushed too", s.node))
		case catalog != commonv1.Catalog_CATALOG_MEASURE && countSpread(s.rows, ref.rows):
			decisions[s.node] = SourceDecision{Node: s.node, Reason: ReasonCountSpread, Ref: ref.node, Chosen: true}
			sel.Warnings = append(sel.Warnings, fmt.Sprintf("source %s (%d rows) is covered by %s (%d rows) but the counts differ, so it is pushed too",
				s.node, s.rows, ref.node, ref.rows))
		default:
			decisions[s.node] = SourceDecision{Node: s.node, Reason: ReasonSubset, Ref: ref.node}
		}
	}
	return finish(sel, decisions)
}

// AllSources chooses every non-empty source, for --all-sources.
func AllSources(sources []SourceInput) Selection {
	decisions := map[string]SourceDecision{}
	for _, s := range sources {
		if s.TotalCount == 0 {
			decisions[s.Node] = SourceDecision{Node: s.Node, Reason: ReasonEmpty}
			continue
		}
		decisions[s.Node] = SourceDecision{Node: s.Node, Reason: ReasonAllSources, Chosen: true}
	}
	return finish(Selection{}, decisions)
}

func finish(sel Selection, decisions map[string]SourceDecision) Selection {
	for _, d := range decisions {
		sel.Decisions = append(sel.Decisions, d)
	}
	slices.SortFunc(sel.Decisions, func(a, b SourceDecision) int { return cmp.Compare(a.Node, b.Node) })
	for _, d := range sel.Decisions {
		if d.Chosen {
			sel.Chosen = append(sel.Chosen, d.Node)
		}
	}
	slices.Sort(sel.Warnings)
	return sel
}

func countSpread(a, b uint64) bool {
	lo, hi := min(a, b), max(a, b)
	diff := hi - lo
	return diff > countSpreadAbsolute && float64(diff) > countSpreadRelative*float64(hi)
}

// smallestCovering returns the chosen source with the shortest coverage that contains all of
// s's coverage; ties go to the node name.
func smallestCovering(s candidate, chosen []candidate) (candidate, bool) {
	var best candidate
	found := false
	for _, c := range chosen {
		if len(subtract(s.coverage, c.coverage)) > 0 {
			continue
		}
		if !found || c.length < best.length || (c.length == best.length && c.node < best.node) {
			best, found = c, true
		}
	}
	return best, found
}

// coverage merges the closed time intervals of parts, joining adjacent ones (next.lo <=
// cur.hi+1) as well as overlapping ones. It reports false when a part has no usable range:
// both bounds zero or min above max.
func coverage(parts []manifest.Part) ([]interval, bool) {
	ivs := make([]interval, 0, len(parts))
	for _, p := range parts {
		if (p.MinTimestamp == 0 && p.MaxTimestamp == 0) || p.MinTimestamp > p.MaxTimestamp {
			return nil, false
		}
		ivs = append(ivs, interval{lo: p.MinTimestamp, hi: p.MaxTimestamp})
	}
	return normalize(ivs), true
}

// normalize sorts intervals and merges overlapping or adjacent ones.
func normalize(ivs []interval) []interval {
	slices.SortFunc(ivs, func(a, b interval) int { return cmp.Or(cmp.Compare(a.lo, b.lo), cmp.Compare(a.hi, b.hi)) })
	var out []interval
	for _, iv := range ivs {
		last := len(out) - 1
		// cur.hi+1 would overflow at MaxInt64; such an interval already reaches every later one.
		if last >= 0 && (out[last].hi == math.MaxInt64 || iv.lo <= out[last].hi+1) {
			out[last].hi = max(out[last].hi, iv.hi)
			continue
		}
		out = append(out, iv)
	}
	return out
}

func union(a, b []interval) []interval {
	return normalize(slices.Concat(a, b))
}

// subtract returns the parts of a that b does not cover; both are normalized.
func subtract(a, b []interval) []interval {
	var out []interval
	for _, x := range a {
		rest := []interval{x}
		for _, y := range b {
			var next []interval
			for _, r := range rest {
				if y.hi < r.lo || y.lo > r.hi {
					next = append(next, r)
					continue
				}
				if y.lo > r.lo {
					next = append(next, interval{lo: r.lo, hi: y.lo - 1})
				}
				if y.hi < r.hi {
					next = append(next, interval{lo: y.hi + 1, hi: r.hi})
				}
			}
			rest = next
		}
		out = append(out, rest...)
	}
	return out
}

// coverageLength is the number of nanoseconds the closed intervals cover, saturating at MaxUint64.
func coverageLength(ivs []interval) uint64 {
	var total uint64
	for _, iv := range ivs {
		n := uint64(iv.hi-iv.lo) + 1 // two's complement: hi-lo wraps correctly into uint64
		if n == 0 || total+n < total {
			return math.MaxUint64
		}
		total += n
	}
	return total
}
