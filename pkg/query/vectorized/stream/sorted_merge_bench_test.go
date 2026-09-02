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

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

const (
	benchRows      = 100_000
	benchBatchSize = 1024
	benchTopN      = 20
)

// benchCorpus builds a fixed benchRows-row corpus split into benchBatchSize
// batches. A cardinality of 0 gives every row its own ElementID; a positive one
// caps the number of distinct ElementIDs. Shuffling denies the merge an
// already-ordered input. The corpus is built once per benchmark and replayed on
// every iteration, so the reported allocations are the merge pipeline's rather
// than the fixture's.
func benchCorpus(schema *vectorized.BatchSchema, cardinality int, shuffle bool) []*vectorized.RecordBatch {
	rows := make([]testRow, benchRows)
	for rowIdx := range rows {
		elemID := uint64(rowIdx)
		if cardinality > 0 {
			elemID = uint64(rowIdx % cardinality)
		}
		rows[rowIdx] = testRow{ts: int64(rowIdx), elemID: elemID}
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
func runMergeBenchmark(b *testing.B, schema *vectorized.BatchSchema, batches []*vectorized.RecordBatch, mergeCap, wantRows int) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		pipe, err := BuildStreamMergePipeline(
			newStaticBatchSource(schema, batches...), schema, true, 0, benchTopN, benchBatchSize, mergeCap)
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
	runMergeBenchmark(b, schema, benchCorpus(schema, 0, true), benchTopN, benchTopN)
}

// BenchmarkSortedMergeTopNDuplicateHeavy is the adversarial corpus for the
// incremental prune: only benchTopN/2 distinct ElementIDs across all benchRows
// rows, so the distinct-ID cap can never truncate on distinct count alone.
func BenchmarkSortedMergeTopNDuplicateHeavy(b *testing.B) {
	schema := tsSchema()
	runMergeBenchmark(b, schema, benchCorpus(schema, benchTopN/2, false), benchTopN, benchTopN/2)
}

// BenchmarkSortedMergeUncapped is the uncapped control: the same corpus with
// mergeCap 0 (the filtered index-order path), which must keep buffering every
// row. It bounds how much of the capped case's cost is inherent to the scan.
func BenchmarkSortedMergeUncapped(b *testing.B) {
	schema := tsSchema()
	runMergeBenchmark(b, schema, benchCorpus(schema, 0, true), 0, benchTopN)
}
