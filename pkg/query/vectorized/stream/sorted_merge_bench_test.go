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

package stream

import (
	"math/rand"
	"testing"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

const (
	benchRows      = 100_000
	benchBatchSize = 1024
	benchTopN      = 20
	// benchKeepNone leaves the criteria tag empty, which is what the merge-only
	// benchmarks carried before the filtered cases were added.
	benchKeepNone  = 0
	benchKeepTag   = "keep"
	benchRejectTag = "drop"
)

// benchCorpus builds a fixed benchRows-row corpus split into benchBatchSize
// batches. A cardinality of 0 gives every row its own ElementID; a positive one
// caps the number of distinct ElementIDs. Shuffling denies the merge an
// already-ordered input. The corpus is built once per benchmark and replayed on
// every iteration, so the reported allocations are the merge pipeline's rather
// than the fixture's.
//
// keepPercent stamps benchKeepTag on that percentage of the rows and
// benchRejectTag on the rest, so a criteria of `tag == keep` has exactly that
// selectivity. Tagging happens BEFORE the shuffle, so the survivors scatter across
// every batch rather than clustering. Pass benchKeepNone for an unfiltered corpus.
func benchCorpus(schema *vectorized.BatchSchema, cardinality int, shuffle bool, keepPercent int) []*vectorized.RecordBatch {
	rows := make([]testRow, benchRows)
	for rowIdx := range rows {
		elemID := uint64(rowIdx)
		if cardinality > 0 {
			elemID = uint64(rowIdx % cardinality)
		}
		rows[rowIdx] = testRow{ts: int64(rowIdx), elemID: elemID}
		if keepPercent > benchKeepNone {
			rows[rowIdx].tag = benchRejectTag
			if rowIdx%100 < keepPercent {
				rows[rowIdx].tag = benchKeepTag
			}
		}
	}
	if shuffle {
		rng := rand.New(rand.NewSource(1)) //nolint:gosec // fixed seed: the corpus must be identical across runs to compare benchmarks
		rng.Shuffle(len(rows), func(leftIdx, rightIdx int) {
			rows[leftIdx], rows[rightIdx] = rows[rightIdx], rows[leftIdx]
		})
	}
	var batches []*vectorized.RecordBatch
	for start := 0; start < len(rows); start += benchBatchSize {
		batches = append(batches, buildBatch(schema, rows[start:min(start+benchBatchSize, len(rows))]))
	}
	return batches
}

// runMergeBenchmark drives the merge pipeline over a prebuilt corpus at the given
// merge cap, failing if the drained result is not wantRows rows. Sharing it keeps
// the capped, duplicate-heavy and uncapped cases from drifting apart.
func runMergeBenchmark(b *testing.B, schema *vectorized.BatchSchema, batches []*vectorized.RecordBatch, mergeCap, wantRows int,
	preMerge ...vectorized.FusibleOperator,
) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		// A pre-merge fusible narrows the SOURCE batches in place, and the corpus is
		// replayed on every iteration, so a leftover selection would make the second
		// iteration scan only the first one's survivors. Reset before each run.
		if len(preMerge) > 0 {
			for _, batch := range batches {
				batch.Selection = nil
			}
		}
		pipe, err := BuildStreamMergePipeline(
			newStaticBatchSource(schema, batches...), schema, true, 0, benchTopN, benchBatchSize, mergeCap, preMerge...)
		if err != nil {
			b.Fatalf("build pipeline: %v", err)
		}
		tss, _, _ := drainRows(b, pipe)
		if len(tss) != wantRows {
			b.Fatalf("expected %d rows, got %d", wantRows, len(tss))
		}
		if closeErr := pipe.Close(); closeErr != nil {
			b.Fatalf("close pipeline: %v", closeErr)
		}
	}
}

// BenchmarkSortedMergeTopN is the LIMIT-20-over-100k-rows case from the
// bounded-merge issue: it reports the auxiliary memory the merge spends to
// return benchTopN elements out of benchRows scanned rows.
func BenchmarkSortedMergeTopN(b *testing.B) {
	schema := tsSchema()
	runMergeBenchmark(b, schema, benchCorpus(schema, 0, true, benchKeepNone), benchTopN, benchTopN)
}

// BenchmarkSortedMergeTopNDuplicateHeavy is the adversarial corpus for the
// incremental prune: only benchTopN/2 distinct ElementIDs across all benchRows
// rows, so the distinct-ID cap can never truncate on distinct count alone.
func BenchmarkSortedMergeTopNDuplicateHeavy(b *testing.B) {
	schema := tsSchema()
	runMergeBenchmark(b, schema, benchCorpus(schema, benchTopN/2, false, benchKeepNone), benchTopN, benchTopN/2)
}

// BenchmarkSortedMergeUncapped is the uncapped control: the same corpus with
// mergeCap 0, which must keep buffering every row. It bounds how much of the
// capped case's cost is inherent to the scan.
func BenchmarkSortedMergeUncapped(b *testing.B) {
	schema := tsSchema()
	runMergeBenchmark(b, schema, benchCorpus(schema, 0, true, benchKeepNone), 0, benchTopN)
}

// benchTagFilter builds the pre-merge `service == keep` criteria over the
// tsSchema projection, wired exactly as localIndexScan.ExecuteVectorized wires it.
func benchTagFilter(b *testing.B, schema *vectorized.BatchSchema) *TagFilter {
	b.Helper()
	filter, err := logical.BuildSimpleTagFilter(&modelv1.Criteria{Exp: &modelv1.Criteria_Condition{Condition: &modelv1.Condition{
		Name:  testTagName,
		Op:    modelv1.Condition_BINARY_OP_EQ,
		Value: strTagValue(benchKeepTag),
	}}})
	if err != nil {
		b.Fatalf("build tag filter: %v", err)
	}
	registry := logical.TagSpecMap{}
	registry.RegisterTagFamilies([]*databasev1.TagFamilySpec{{
		Name: testTagFamily,
		Tags: []*databasev1.TagSpec{{Name: testTagName, Type: databasev1.TagType_TAG_TYPE_STRING}},
	}})
	return NewTagFilter(schema, []model.TagProjection{{Family: testTagFamily, Names: []string{testTagName}}}, filter, registry)
}

// BenchmarkSortedMergeFilteredTopN measures the criteria pipeline at the two
// selectivities the cost model splits on. The pushdown moves the tag filter off
// the egress, where it saw at most limit+offset materialized elements, and onto
// every scanned row — so it buys a bounded merge at the price of per-row filter
// work whose size depends on how little the criteria rejects.
//
// Each regime reports two arms over an IDENTICAL corpus:
//   - uncapped-no-premerge-filter: the shape this change replaced. The merge
//     buffers every one of benchRows rows before anything is discarded.
//   - capped-with-premerge-filter: the shape it ships. The filter runs first and
//     the merge caps at benchTopN.
//
// Neither arm materializes Elements, so the egress filter's own cost sits outside
// both numbers. What the pair isolates is the merge's buffering against the
// pre-merge filter's per-row work.
func BenchmarkSortedMergeFilteredTopN(b *testing.B) {
	for _, tc := range []struct {
		name        string
		keepPercent int
	}{
		{name: "high-selectivity-keep1pct", keepPercent: 1},
		{name: "low-selectivity-keep95pct", keepPercent: 95},
	} {
		schema := tsSchema()
		batches := benchCorpus(schema, 0, true, tc.keepPercent)
		b.Run(tc.name+"/uncapped-no-premerge-filter", func(sub *testing.B) {
			runMergeBenchmark(sub, schema, batches, 0, benchTopN)
		})
		b.Run(tc.name+"/capped-with-premerge-filter", func(sub *testing.B) {
			runMergeBenchmark(sub, schema, batches, benchTopN, benchTopN, benchTagFilter(sub, schema))
		})
	}
}
