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
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/manifest"
)

const (
	catStream  = commonv1.Catalog_CATALOG_STREAM
	catMeasure = commonv1.Catalog_CATALOG_MEASURE
	catTrace   = commonv1.Catalog_CATALOG_TRACE
)

// src builds a source whose parts cover the given closed ranges, rows split evenly.
func src(node string, rows uint64, ranges ...[2]int64) SourceInput {
	s := SourceInput{Node: node, TotalCount: rows}
	for i, r := range ranges {
		s.Parts = append(s.Parts, manifest.Part{ID: uint64(i + 1), MinTimestamp: r[0], MaxTimestamp: r[1], TotalCount: rows / uint64(len(ranges))})
	}
	return s
}

func reasons(sel Selection) map[string]string {
	out := map[string]string{}
	for _, d := range sel.Decisions {
		out[d.Node] = d.Reason
	}
	return out
}

func TestSelectSourcesScenarios(t *testing.T) {
	tests := []struct {
		reasons map[string]string
		name    string
		chosen  []string
		sources []SourceInput
		catalog commonv1.Catalog
		warns   int
	}{
		{
			name: "single source", catalog: catStream,
			sources: []SourceInput{src("a", 100, [2]int64{1, 100})},
			chosen:  []string{"a"}, reasons: map[string]string{"a": ReasonGap},
		},
		{
			name: "true replicas with equal counts", catalog: catStream,
			sources: []SourceInput{src("a", 1000, [2]int64{1, 100}), src("b", 1000, [2]int64{1, 100})},
			chosen:  []string{"b"}, reasons: map[string]string{"a": ReasonSubset, "b": ReasonGap},
		},
		{
			name: "replicas differing by 3%", catalog: catStream,
			sources: []SourceInput{src("a", 1000, [2]int64{1, 100}), src("b", 970, [2]int64{1, 100})},
			chosen:  []string{"a"}, reasons: map[string]string{"a": ReasonGap, "b": ReasonSubset},
		},
		{
			name: "measure replicas differing by 45%", catalog: catMeasure,
			sources: []SourceInput{src("a", 1000, [2]int64{1, 100}), src("b", 550, [2]int64{1, 100})},
			chosen:  []string{"a"}, reasons: map[string]string{"a": ReasonGap, "b": ReasonSubset},
		},
		{
			name: "complementary halves sharing a boundary timestamp", catalog: catTrace,
			sources: []SourceInput{src("a", 500, [2]int64{0, 50}), src("b", 500, [2]int64{50, 100})},
			chosen:  []string{"a", "b"}, reasons: map[string]string{"a": ReasonGap, "b": ReasonGap}, warns: 1,
		},
		{
			name: "gap source partly overlapping a chosen source", catalog: catTrace,
			sources: []SourceInput{src("a", 1000, [2]int64{0, 100}), src("b", 1000, [2]int64{50, 150})},
			chosen:  []string{"a", "b"}, reasons: map[string]string{"a": ReasonGap, "b": ReasonGap}, warns: 1,
		},
		{
			name: "measure gap source partly overlapping a chosen source", catalog: catMeasure,
			sources: []SourceInput{src("a", 1000, [2]int64{0, 100}), src("b", 1000, [2]int64{50, 150})},
			chosen:  []string{"a", "b"}, reasons: map[string]string{"a": ReasonGap, "b": ReasonGap},
		},
		{
			name: "complementary halves with a gap", catalog: catStream,
			sources: []SourceInput{src("a", 500, [2]int64{0, 40}), src("b", 500, [2]int64{60, 100})},
			chosen:  []string{"a", "b"}, reasons: map[string]string{"a": ReasonGap, "b": ReasonGap},
		},
		{
			name: "contained source with a large count difference", catalog: catStream,
			sources: []SourceInput{src("a", 1000, [2]int64{0, 100}), src("b", 800, [2]int64{20, 80})},
			chosen:  []string{"a", "b"}, reasons: map[string]string{"a": ReasonGap, "b": ReasonCountSpread}, warns: 1,
		},
		{
			name: "contained source with a small count difference", catalog: catStream,
			sources: []SourceInput{src("a", 1000, [2]int64{0, 100}), src("b", 990, [2]int64{20, 80})},
			chosen:  []string{"a"}, reasons: map[string]string{"a": ReasonGap, "b": ReasonSubset},
		},
		{
			name: "same window diverged", catalog: catTrace,
			sources: []SourceInput{src("a", 1000, [2]int64{0, 100}), src("b", 600, [2]int64{0, 100})},
			chosen:  []string{"a", "b"}, reasons: map[string]string{"a": ReasonGap, "b": ReasonCountSpread}, warns: 1,
		},
		{
			name: "coverage with a hole filled by another source", catalog: catStream,
			sources: []SourceInput{src("a", 800, [2]int64{0, 30}, [2]int64{70, 100}), src("b", 500, [2]int64{20, 80})},
			chosen:  []string{"a", "b"}, reasons: map[string]string{"a": ReasonGap, "b": ReasonGap}, warns: 1,
		},
		{
			name: "three replicas", catalog: catStream,
			sources: []SourceInput{src("a", 1000, [2]int64{0, 100}), src("b", 1000, [2]int64{0, 100}), src("c", 1000, [2]int64{0, 100})},
			chosen:  []string{"c"}, reasons: map[string]string{"a": ReasonSubset, "b": ReasonSubset, "c": ReasonGap},
		},
		{
			name: "five sources in two clusters", catalog: catStream,
			sources: []SourceInput{
				src("a1", 1000, [2]int64{0, 99}), src("a2", 1000, [2]int64{0, 99}), src("a3", 1000, [2]int64{0, 99}),
				src("b1", 900, [2]int64{200, 299}), src("b2", 900, [2]int64{200, 299}),
			},
			chosen: []string{"a3", "b2"},
			reasons: map[string]string{
				"a1": ReasonSubset, "a2": ReasonSubset, "a3": ReasonGap, "b1": ReasonSubset, "b2": ReasonGap,
			},
		},
		{
			name: "empty shard mixed in", catalog: catStream,
			sources: []SourceInput{src("a", 1000, [2]int64{0, 100}), {Node: "b"}, {Node: "c", TotalCount: 5}},
			chosen:  []string{"a"}, reasons: map[string]string{"a": ReasonGap, "b": ReasonEmpty, "c": ReasonEmpty},
		},
		{
			name: "unusable range pushes every source", catalog: catStream,
			sources: []SourceInput{src("a", 1000, [2]int64{0, 0}), src("b", 1000, [2]int64{0, 100})},
			chosen:  []string{"a", "b"}, reasons: map[string]string{"a": ReasonUnusableRange, "b": ReasonUnusableRange}, warns: 1,
		},
		{
			name: "covered only by two chosen sources together", catalog: catMeasure,
			sources: []SourceInput{src("a", 700, [2]int64{0, 6}), src("b", 600, [2]int64{5, 10}), src("c", 500, [2]int64{3, 8})},
			chosen:  []string{"a", "b", "c"}, reasons: map[string]string{"a": ReasonGap, "b": ReasonGap, "c": ReasonNoSingleCovering}, warns: 1,
		},
		{
			name: "contained source with a small absolute difference", catalog: catStream,
			sources: []SourceInput{src("a", 100, [2]int64{0, 100}), src("b", 50, [2]int64{0, 100})},
			chosen:  []string{"a"}, reasons: map[string]string{"a": ReasonGap, "b": ReasonSubset},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sel := SelectSources(tt.catalog, tt.sources)
			assert.Equal(t, tt.chosen, sel.Chosen)
			assert.Equal(t, tt.reasons, reasons(sel))
			assert.Len(t, sel.Warnings, tt.warns)
		})
	}
}

func TestSelectSourcesDeterministicUnderShuffle(t *testing.T) {
	sources := []SourceInput{
		src("a1", 1000, [2]int64{0, 99}), src("a2", 1000, [2]int64{0, 99}), src("a3", 1000, [2]int64{0, 99}),
		src("b1", 900, [2]int64{200, 299}), src("b2", 900, [2]int64{200, 299}), src("c", 0),
		src("d", 400, [2]int64{50, 60}),
	}
	want := SelectSources(catStream, sources)
	rng := rand.New(rand.NewSource(1)) //nolint:gosec // a seeded shuffle keeps the test reproducible
	for i := 0; i < 10; i++ {
		shuffled := append([]SourceInput(nil), sources...)
		rng.Shuffle(len(shuffled), func(a, b int) { shuffled[a], shuffled[b] = shuffled[b], shuffled[a] })
		require.Equal(t, want, SelectSources(catStream, shuffled), "shuffle %d", i)
	}
}

func TestCoverageMergesAdjacentAndGuardsOverflow(t *testing.T) {
	cov, ok := coverage([]manifest.Part{{MinTimestamp: 6, MaxTimestamp: 9}, {MinTimestamp: 1, MaxTimestamp: 5}})
	require.True(t, ok)
	assert.Equal(t, []interval{{1, 9}}, cov)
	assert.Equal(t, uint64(9), coverageLength(cov))

	cov, ok = coverage([]manifest.Part{{MinTimestamp: 10, MaxTimestamp: math.MaxInt64}, {MinTimestamp: 20, MaxTimestamp: 30}})
	require.True(t, ok)
	assert.Equal(t, []interval{{10, math.MaxInt64}}, cov)
	assert.Equal(t, uint64(math.MaxUint64), coverageLength([]interval{{math.MinInt64, math.MaxInt64}}))

	_, ok = coverage([]manifest.Part{{MinTimestamp: 5, MaxTimestamp: 1}})
	assert.False(t, ok)
	assert.Equal(t, []interval{{0, 4}, {9, 10}}, subtract([]interval{{0, 10}}, []interval{{5, 8}}))
}

func TestAllSources(t *testing.T) {
	sel := AllSources([]SourceInput{src("a", 10, [2]int64{1, 2}), src("b", 10, [2]int64{1, 2}), {Node: "c"}})
	assert.Equal(t, []string{"a", "b"}, sel.Chosen)
	assert.Equal(t, map[string]string{"a": ReasonAllSources, "b": ReasonAllSources, "c": ReasonEmpty}, reasons(sel))
}
