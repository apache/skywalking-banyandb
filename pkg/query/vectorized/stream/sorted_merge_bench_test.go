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

// benchBatches builds a fixed corpus of benchRows rows, shuffled so the merge
// cannot exploit an already-ordered input, split into benchBatchSize batches.
// The corpus is built once per benchmark and replayed every iteration so the
// reported allocations are the merge pipeline's, not the fixture's.
func benchBatches(schema *vectorized.BatchSchema) []*vectorized.RecordBatch {
	rows := make([]testRow, benchRows)
	for i := range rows {
		rows[i] = testRow{ts: int64(i), elemID: uint64(i)}
	}
	rng := rand.New(rand.NewSource(1)) //nolint:gosec // fixed seed: the corpus must be identical across runs to compare benchmarks
	rng.Shuffle(len(rows), func(i, j int) { rows[i], rows[j] = rows[j], rows[i] })
	var batches []*vectorized.RecordBatch
	for start := 0; start < len(rows); start += benchBatchSize {
		end := start + benchBatchSize
		if end > len(rows) {
			end = len(rows)
		}
		batches = append(batches, buildBatch(schema, rows[start:end]))
	}
	return batches
}

// BenchmarkSortedMergeTopN is the LIMIT-20-over-100k-rows case from the
// bounded-merge issue: it reports the auxiliary memory the merge spends to
// return benchTopN elements out of benchRows scanned rows.
func BenchmarkSortedMergeTopN(b *testing.B) {
	schema := tsSchema()
	batches := benchBatches(schema)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		src := newStaticBatchSource(schema, batches...)
		pipe, err := BuildStreamMergePipeline(src, schema, true, 0, benchTopN, benchBatchSize, benchTopN)
		if err != nil {
			b.Fatalf("build pipeline: %v", err)
		}
		tss, _, _ := drainRows(b, pipe)
		if len(tss) != benchTopN {
			b.Fatalf("expected %d rows, got %d", benchTopN, len(tss))
		}
		if closeErr := pipe.Close(); closeErr != nil {
			b.Fatalf("close pipeline: %v", closeErr)
		}
	}
}

// BenchmarkSortedMergeTopNDuplicateHeavy is the adversarial corpus for the
// incremental prune: only benchTopN/2 distinct ElementIDs across all benchRows
// rows, so the distinct-ID cap can never truncate and every prune re-sorts a
// buffer that never shrinks. It must not degrade past the uncapped baseline.
func BenchmarkSortedMergeTopNDuplicateHeavy(b *testing.B) {
	schema := tsSchema()
	rows := make([]testRow, benchRows)
	for i := range rows {
		rows[i] = testRow{ts: int64(i), elemID: uint64(i % (benchTopN / 2))}
	}
	var batches []*vectorized.RecordBatch
	for start := 0; start < len(rows); start += benchBatchSize {
		batches = append(batches, buildBatch(schema, rows[start:min(start+benchBatchSize, len(rows))]))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		src := newStaticBatchSource(schema, batches...)
		pipe, err := BuildStreamMergePipeline(src, schema, true, 0, benchTopN, benchBatchSize, benchTopN)
		if err != nil {
			b.Fatalf("build pipeline: %v", err)
		}
		tss, _, _ := drainRows(b, pipe)
		if len(tss) != benchTopN/2 {
			b.Fatalf("expected %d rows, got %d", benchTopN/2, len(tss))
		}
		if closeErr := pipe.Close(); closeErr != nil {
			b.Fatalf("close pipeline: %v", closeErr)
		}
	}
}

// BenchmarkSortedMergeUncapped is the uncapped control: the same corpus with
// mergeCap 0 (the filtered index-order path), which must keep buffering every
// row. It bounds how much of the capped case's cost is inherent to the scan.
func BenchmarkSortedMergeUncapped(b *testing.B) {
	schema := tsSchema()
	batches := benchBatches(schema)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		src := newStaticBatchSource(schema, batches...)
		pipe, err := BuildStreamMergePipeline(src, schema, true, 0, benchTopN, benchBatchSize, 0)
		if err != nil {
			b.Fatalf("build pipeline: %v", err)
		}
		tss, _, _ := drainRows(b, pipe)
		if len(tss) != benchTopN {
			b.Fatalf("expected %d rows, got %d", benchTopN, len(tss))
		}
		if closeErr := pipe.Close(); closeErr != nil {
			b.Fatalf("close pipeline: %v", closeErr)
		}
	}
}
