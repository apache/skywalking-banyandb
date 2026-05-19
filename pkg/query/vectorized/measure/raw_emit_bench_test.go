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

package measure

import (
	"context"
	"fmt"
	"testing"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure/frame"
)

// BenchmarkG9f_DistributedFanout is the in-process proxy for G9f.6's
// distributed soak/bench. It simulates a fan-out of N data nodes each
// running AggModeMap → frame.Encode, the cluster wire (just the byte
// slice handoff), and the liaison's frame.Decode → AggModeReduce +
// (shard, group) dedup → serialise to InternalDataPoint.
//
// Compared against BenchmarkG9f_SingleNodeAggModeAll on the same row
// set, this measures the *full path overhead* of the throughout-vec
// distributed loop: per-shard Map fold + columnar serialise + bytes +
// columnar deserialise + Reduce + final proto materialise. The ratio
// of the two numbers is the load-bearing efficiency claim of G9f.
//
// Real-cluster soak (gRPC + retry + network latency) is a follow-up
// gate; this benchmark isolates the codec + operator overhead so
// regressions in those layers surface here as soon as they land.
func BenchmarkG9f_DistributedFanout(b *testing.B) {
	rows := generateBenchRows(1000)
	schema := topologyRawSchema()
	for _, shardCount := range []int{1, 3, 6} {
		b.Run(fmt.Sprintf("Shards%d", shardCount), func(b *testing.B) {
			// Pre-encode the per-shard frames once — production
			// re-emits them per query but this benchmark is measuring
			// liaison-side throughput so we hold the data-node cost
			// out of the inner loop. The Drain+Encode cost is covered
			// by BenchmarkG9f_DataNodeEmit.
			frames := buildPartialFramesB(b, schema, rows, shardCount)
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				idps, err := ReduceFramesToInternalDataPoints(frames,
					[]string{"g"},
					[]AggReduceSpec{{OutputName: "out", Func: AggSum}},
					nil, 1024, vectorized.NewMemoryTracker(1<<30))
				if err != nil {
					b.Fatalf("ReduceFramesToInternalDataPoints: %v", err)
				}
				if len(idps) == 0 {
					b.Fatal("reduce returned 0 datapoints")
				}
			}
		})
	}
}

// BenchmarkG9f_SingleNodeAggModeAll is the AggModeAll oracle: full
// dataset processed in one BatchAggregation pass with no wire crossing.
// The number this produces is the lower bound — any distributed path
// adding wire / serialise / decode steps must stay close to it.
func BenchmarkG9f_SingleNodeAggModeAll(b *testing.B) {
	rows := generateBenchRows(1000)
	schema := topologyRawSchema()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		op := NewBatchAggregation(schema, []int{1},
			[]AggSpec{{Func: AggSum, InputCol: 2, Output: "out"}},
			AggModeAll, 1024, vectorized.NewMemoryTracker(1<<30), 0)
		if err := op.Init(context.Background()); err != nil {
			b.Fatalf("Init: %v", err)
		}
		batch := buildBenchInputBatch(schema, rows)
		if err := op.Consume(context.Background(), batch); err != nil {
			b.Fatalf("Consume: %v", err)
		}
		if err := op.Finalize(context.Background()); err != nil {
			b.Fatalf("Finalize: %v", err)
		}
		for {
			nb, nextErr := op.NextBatch(context.Background())
			if nextErr != nil {
				b.Fatalf("NextBatch: %v", nextErr)
			}
			if nb == nil {
				break
			}
		}
		_ = op.Close()
	}
}

// BenchmarkG9f_DataNodeEmit measures the per-data-node Map + Encode
// step alone — the upstream cost of the distributed path. Useful when
// regressions appear in BenchmarkG9f_DistributedFanout to localise
// whether they're on the encode side or the decode/reduce side.
func BenchmarkG9f_DataNodeEmit(b *testing.B) {
	rows := generateBenchRows(1000)
	schema := topologyRawSchema()
	for _, shardCount := range []int{1, 3, 6} {
		b.Run(fmt.Sprintf("Shards%d", shardCount), func(b *testing.B) {
			perShard := splitRowsAcrossShards(rows, shardCount)
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				for shardID, srows := range perShard {
					if len(srows) == 0 {
						continue
					}
					body := encodePartialBenchHelper(b, schema, int64(shardID+1), srows)
					if len(body) == 0 {
						b.Fatal("empty body")
					}
				}
			}
		})
	}
}

// generateBenchRows builds a deterministic synthetic dataset with the
// declared row count and a small group cardinality.
func generateBenchRows(n int) []topologyRow {
	groups := []string{"a", "b", "c", "d", "e", "f", "g", "h"}
	out := make([]topologyRow, n)
	for i := 0; i < n; i++ {
		out[i] = topologyRow{group: groups[i%len(groups)], value: int64(i)}
	}
	return out
}

func buildBenchInputBatch(schema *vectorized.BatchSchema, rows []topologyRow) *vectorized.RecordBatch {
	b := vectorized.NewRecordBatch(schema, len(rows))
	for _, r := range rows {
		b.Columns[0].(*vectorized.TypedColumn[int64]).Append(0)
		b.Columns[1].(*vectorized.TypedColumn[string]).Append(r.group)
		b.Columns[2].(*vectorized.TypedColumn[int64]).Append(r.value)
	}
	b.Len = len(rows)
	return b
}

func splitRowsAcrossShards(rows []topologyRow, shardCount int) [][]topologyRow {
	out := make([][]topologyRow, shardCount)
	for i, r := range rows {
		idx := i % shardCount
		out[idx] = append(out[idx], r)
	}
	return out
}

// buildPartialFramesB is a benchmark-friendly variant of
// buildPartialFrames — *testing.B instead of *testing.T, no replica
// duplication (the dedup is exercised by the topology matrix test).
func buildPartialFramesB(b *testing.B, schema *vectorized.BatchSchema, rows []topologyRow, shardCount int) [][]byte {
	b.Helper()
	perShard := splitRowsAcrossShards(rows, shardCount)
	var out [][]byte
	for shardID, srows := range perShard {
		if len(srows) == 0 {
			continue
		}
		out = append(out, encodePartialBenchHelper(b, schema, int64(shardID+1), srows))
	}
	return out
}

func encodePartialBenchHelper(b *testing.B, schema *vectorized.BatchSchema, shardID int64, rows []topologyRow) []byte {
	b.Helper()
	op := NewBatchAggregation(schema, []int{1},
		[]AggSpec{{Func: AggSum, InputCol: 2, Output: "out"}},
		AggModeMap, 1024, vectorized.NewMemoryTracker(1<<30), 0)
	defer op.Close()
	if err := op.Init(context.Background()); err != nil {
		b.Fatalf("Init: %v", err)
	}
	batch := vectorized.NewRecordBatch(schema, len(rows))
	for _, r := range rows {
		batch.Columns[0].(*vectorized.TypedColumn[int64]).Append(shardID)
		batch.Columns[1].(*vectorized.TypedColumn[string]).Append(r.group)
		batch.Columns[2].(*vectorized.TypedColumn[int64]).Append(r.value)
	}
	batch.Len = len(rows)
	if err := op.Consume(context.Background(), batch); err != nil {
		b.Fatalf("Consume: %v", err)
	}
	if err := op.Finalize(context.Background()); err != nil {
		b.Fatalf("Finalize: %v", err)
	}
	out, err := op.NextBatch(context.Background())
	if err != nil {
		b.Fatalf("NextBatch: %v", err)
	}
	body, encErr := frame.Encode(out)
	if encErr != nil {
		b.Fatalf("Encode: %v", encErr)
	}
	return body
}
