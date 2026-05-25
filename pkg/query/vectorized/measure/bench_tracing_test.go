// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

// §11.6 Trace-on/trace-off overhead microbenchmarks for BatchTop.
//
// TODO(follow-up): The baseline anchor file (testdata/bench_baseline.json) and
// the CI tolerance gate (2% ns/op regression, 10% allocs/op regression, 5%
// B/op regression) described in plan §11.6 are deferred to a follow-up commit.
// These benchmarks provide the measurement harness; the gate thresholds and
// anchoring script should be added once a stable baseline is captured on the
// reference CI machine.
//
// Run both benchmarks with:
//
//	go test -bench=BenchmarkBatchTopConsume -benchtime=10x -benchmem \
//	  ./pkg/query/vectorized/measure/

package measure

import (
	"context"
	"testing"

	"github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// benchTopN is the heap bound for the BatchTop microbenchmarks.
const benchTopN = 50

// buildTopBenchInput constructs a deterministic 10k-row single-field int64
// BatchSchema and a slice of RecordBatch values ready to be consumed by
// BatchTop. Each row carries a distinct int64 value so the heap sees a
// non-degenerate ordering (no early-exit on tied values). The batches are
// built once per benchmark iteration group (outside the timed section).
func buildTopBenchInput(batchSize int) (*vectorized.BatchSchema, []*vectorized.RecordBatch) {
	const totalRows = 10_000
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleField, Name: "v", Type: vectorized.ColumnTypeInt64},
	})
	pool := vectorized.NewBatchPool(schema, batchSize)

	nBatches := (totalRows + batchSize - 1) / batchSize
	batches := make([]*vectorized.RecordBatch, 0, nBatches)
	rowIdx := 0
	for rowIdx < totalRows {
		b := pool.Get()
		col := b.Columns[0].(*vectorized.TypedColumn[int64])
		rowsThisBatch := min(batchSize, totalRows-rowIdx)
		for k := range rowsThisBatch {
			col.Append(int64(rowIdx + k))
		}
		b.Len = rowsThisBatch
		batches = append(batches, b)
		rowIdx += rowsThisBatch
	}
	return schema, batches
}

// runTopConsume drives one full BatchTop cycle (Init → Consume all batches →
// Finalize → drain NextBatch) on a fresh operator constructed from the given
// schema using ctx (which may or may not carry a tracer).
func runTopConsume(ctx context.Context, schema *vectorized.BatchSchema, batches []*vectorized.RecordBatch) {
	top := NewBatchTop(schema, 0, benchTopN, false, defaultBatch)
	if initErr := top.Init(ctx); initErr != nil {
		panic(initErr)
	}
	for _, b := range batches {
		if consumeErr := top.Consume(ctx, b); consumeErr != nil {
			panic(consumeErr)
		}
	}
	if finalErr := top.Finalize(ctx); finalErr != nil {
		panic(finalErr)
	}
	for {
		nb, nextErr := top.NextBatch(ctx)
		if nextErr != nil {
			panic(nextErr)
		}
		if nb == nil {
			break
		}
		benchSink += nb.Len
	}
	if closeErr := top.Close(); closeErr != nil {
		panic(closeErr)
	}
}

// BenchmarkBatchTopConsume_TraceOff measures BatchTop over 10k rows with no
// tracer attached to the context. This is the no-overhead baseline.
func BenchmarkBatchTopConsume_TraceOff(b *testing.B) {
	schema, batches := buildTopBenchInput(defaultBatch)
	ctx := context.Background() // no tracer
	b.ReportAllocs()
	for b.Loop() {
		runTopConsume(ctx, schema, batches)
	}
}

// BenchmarkBatchTopConsume_TraceOn measures BatchTop over 10k rows with a
// tracer attached, so the operator opens its "top" span and emits tags on
// Close. The difference vs TraceOff isolates the span lifecycle overhead.
func BenchmarkBatchTopConsume_TraceOn(b *testing.B) {
	schema, batches := buildTopBenchInput(defaultBatch)
	b.ReportAllocs()
	for b.Loop() {
		_, ctx := query.NewTracer(context.Background(), "bench-trace")
		runTopConsume(ctx, schema, batches)
	}
}
