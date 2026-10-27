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

// Package rowdiff compares two CSV artifacts as sets of logical rows: A is a source export
// and B the re-export of the cluster A was imported into. Rows are keyed per catalog, replicas
// are deduplicated, measure versions are reduced to the newest one, and cells are compared as
// their canonical CSV encoding, with columns aligned by header name. A property is keyed by
// its entity (group, _name, _id) and only its latest revision is compared (the largest
// _mod_revision; at an equal revision a tombstone wins over a live copy), with _delete_time
// compared as live or tombstone only; older revisions are ignored on both sides.
//
// Memory: every deduplicated row of the compared groups of both artifacts is held in memory
// with its encoded cells. It is meant for test-scale artifacts (integration and e2e), not for
// production-size exports.
package rowdiff

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
)

// Diff kinds.
const (
	KindMissingInB         = "missing_in_b"
	KindExtraInB           = "extra_in_b"
	KindValueMismatch      = "value_mismatch"
	KindReplicaConflict    = "replica_conflict"
	KindStageCountMismatch = "stage_count_mismatch"
	KindLayoutMismatch     = "layout_mismatch"
)

// Options configures Compare.
type Options struct {
	// Groups restricts the comparison to these groups; empty means every group present in A.
	Groups []string
}

// Diff is one difference between the artifacts. Key is the readable logical key of the row,
// the stage of a stage_count_mismatch, or empty for a group-level layout_mismatch.
type Diff struct {
	Catalog  string
	Group    string
	Resource string
	Key      string
	Kind     string
	Detail   string
}

// String renders the diff on one line.
func (d Diff) String() string {
	return fmt.Sprintf("%s %s/%s/%s [%s] %s", d.Kind, d.Catalog, d.Group, d.Resource, d.Key, d.Detail)
}

// CatalogStat counts the deduplicated rows of one catalog in each artifact; for property it
// counts entities.
type CatalogStat struct {
	Catalog string
	RowsA   int
	RowsB   int
}

// Report is the result of Compare.
type Report struct {
	// StageCounts maps "catalog/group/stage" to the deduplicated row counts {A, B}; for
	// property, the entity counts.
	StageCounts map[string][2]int
	Diffs       []Diff
	Stats       []CatalogStat
}

// OK reports whether the artifacts hold the same logical rows.
func (r *Report) OK() bool {
	return len(r.Diffs) == 0
}

// Summary renders the first maxDiffs diffs, one per line, followed by a "... N more" line
// when there are more; a negative maxDiffs renders every diff.
func (r *Report) Summary(maxDiffs int) []string {
	n := len(r.Diffs)
	if maxDiffs >= 0 && maxDiffs < n {
		n = maxDiffs
	}
	out := make([]string, 0, n+1)
	for _, d := range r.Diffs[:n] {
		out = append(out, d.String())
	}
	if n < len(r.Diffs) {
		out = append(out, fmt.Sprintf("... %d more", len(r.Diffs)-n))
	}
	return out
}

// Compare loads the artifacts at dirA and dirB and compares their logical rows.
func Compare(dirA, dirB string, opts Options) (*Report, error) {
	var scope map[string]bool
	if len(opts.Groups) > 0 {
		scope = map[string]bool{}
		for _, g := range opts.Groups {
			scope[g] = true
		}
	}
	ctx := context.Background()
	a, err := loadArtifact(ctx, dirA, scope)
	if err != nil {
		return nil, fmt.Errorf("artifact A: %w", err)
	}
	if scope == nil {
		scope = map[string]bool{}
		for k := range a.groups {
			scope[k.group] = true
		}
	}
	b, err := loadArtifact(ctx, dirB, scope)
	if err != nil {
		return nil, fmt.Errorf("artifact B: %w", err)
	}
	rep := &Report{StageCounts: map[string][2]int{}}
	keys := maps.Clone(a.groups)
	maps.Copy(keys, b.groups)
	sorted := slices.SortedFunc(maps.Keys(keys), func(x, y groupKey) int {
		if c := strings.Compare(x.catalog, y.catalog); c != 0 {
			return c
		}
		return strings.Compare(x.group, y.group)
	})
	for _, k := range sorted {
		ga, gb := a.groups[k], b.groups[k]
		if n := len(rep.Stats); n == 0 || rep.Stats[n-1].Catalog != k.catalog {
			rep.Stats = append(rep.Stats, CatalogStat{Catalog: k.catalog})
		}
		st := &rep.Stats[len(rep.Stats)-1]
		st.RowsA += ga.rowCount()
		st.RowsB += gb.rowCount()
		rep.Diffs = append(rep.Diffs, ga.conflicts("A")...)
		rep.Diffs = append(rep.Diffs, gb.conflicts("B")...)
		rep.Diffs = append(rep.Diffs, compareGroup(k, ga, gb)...)
		rep.Diffs = append(rep.Diffs, compareStages(k, ga, gb, rep.StageCounts)...)
	}
	return rep, nil
}

func compareGroup(k groupKey, ga, gb *groupData) []Diff {
	var diffs []Diff
	diff := func(resource, key, kind, detail string) {
		diffs = append(diffs, Diff{Catalog: k.catalog, Group: k.group, Resource: resource, Key: key, Kind: kind, Detail: detail})
	}
	switch {
	case ga == nil:
		diff("", "", KindLayoutMismatch, "the group is missing in A")
		ga = &groupData{}
	case gb == nil:
		diff("", "", KindLayoutMismatch, "the group is missing in B")
		gb = &groupData{}
	default:
		for _, name := range unionKeys(ga.resources, gb.resources) {
			ra, rb := ga.resources[name], gb.resources[name]
			switch {
			case ra == nil:
				diff(name, "", KindLayoutMismatch, "the resource is missing in the schema of A")
			case rb == nil:
				diff(name, "", KindLayoutMismatch, "the resource is missing in the schema of B")
			default:
				onlyA, onlyB := headerDelta(ra.headers, rb.headers)
				if len(onlyA) > 0 || len(onlyB) > 0 {
					diff(name, "", KindLayoutMismatch, fmt.Sprintf("columns only in A: %v; only in B: %v", onlyA, onlyB))
				}
			}
		}
	}
	for _, key := range slices.Sorted(maps.Keys(ga.rows)) {
		ea := ga.rows[key]
		eb, ok := gb.rows[key]
		if !ok {
			diff(ea.resource, ea.key, KindMissingInB, "")
			continue
		}
		if detail := compareCells(ga.resources[ea.resource], ea.cells, gb.resources[eb.resource], eb.cells); detail != "" {
			diff(ea.resource, ea.key, KindValueMismatch, detail)
		}
	}
	for _, key := range slices.Sorted(maps.Keys(gb.rows)) {
		if _, ok := ga.rows[key]; !ok {
			eb := gb.rows[key]
			diff(eb.resource, eb.key, KindExtraInB, "")
		}
	}
	return diffs
}

func compareStages(k groupKey, ga, gb *groupData, out map[string][2]int) []Diff {
	ca, cb := ga.stageCounts(), gb.stageCounts()
	stages := unionKeys(ca, cb)
	var diffs []Diff
	for _, stage := range stages {
		counts := [2]int{ca[stage], cb[stage]}
		out[k.catalog+"/"+k.group+"/"+stage] = counts
		if counts[0] != counts[1] {
			diffs = append(diffs, Diff{
				Catalog: k.catalog, Group: k.group, Key: stage, Kind: KindStageCountMismatch,
				Detail: fmt.Sprintf("A has %d rows, B has %d", counts[0], counts[1]),
			})
		}
	}
	return diffs
}

// compareCells compares the cells of the columns both resources have, by header name, and
// describes every differing column; it returns "" when they agree.
func compareCells(ra *resourceCols, a []string, rb *resourceCols, b []string) string {
	var parts []string
	for i, h := range ra.headers {
		j, ok := rb.pos[h]
		if !ok || a[i] == b[j] {
			continue
		}
		parts = append(parts, fmt.Sprintf("%s: A=%s B=%s", h, truncate(a[i]), truncate(b[j])))
	}
	return strings.Join(parts, "; ")
}

const maxCellDisplay = 80

func truncate(s string) string {
	if len(s) <= maxCellDisplay {
		return fmt.Sprintf("%q", s)
	}
	return fmt.Sprintf("%q...(%d bytes)", s[:maxCellDisplay], len(s))
}

func headerDelta(a, b []string) (onlyA, onlyB []string) {
	for _, h := range a {
		if !slices.Contains(b, h) {
			onlyA = append(onlyA, h)
		}
	}
	for _, h := range b {
		if !slices.Contains(a, h) {
			onlyB = append(onlyB, h)
		}
	}
	return onlyA, onlyB
}

// unionKeys returns the sorted union of the keys of a and b.
func unionKeys[V any](a, b map[string]V) []string {
	keys := slices.AppendSeq(slices.Collect(maps.Keys(a)), maps.Keys(b))
	slices.Sort(keys)
	return slices.Compact(keys)
}
