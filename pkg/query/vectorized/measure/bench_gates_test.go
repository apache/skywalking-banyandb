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
	"os"
	"testing"
)

// G9e acceptance gates. Ratios are vectorized / row; failing the gate is a
// regression that blocks the default-flip rollout. The vec half here is the
// production compute pipeline (source → [BatchAggregation | BatchTop] →
// BatchLimit drained through the public adapter), not the deprecated
// NewMIterator leaf-substitution path.
//
// Tolerance rationale:
//
//   - Scan shapes (W1, W2, W7): ns ≤ 1.05, allocs ≤ 1.005, bytes ≤ 1.20.
//     The alloc gate is 1.005 (0.5%) rather than a literal 1.00 because each
//     runVectorizedPath call constructs a fresh BatchSchema, BatchPool,
//     BatchScan, Pipeline, and adapter — roughly 20 fixture allocations per
//     query. The row path's serializer is a struct literal with effectively
//     zero fixture cost. Spread over W1's 10K rows that is a 0.05%
//     per-iteration delta; over 100K-row workloads, 0.014%. The "per-cell
//     wrapper bias" rationale from G5a still applies: buildResults allocates
//     a fresh TagValue/FieldValue per cell so both paths pay the same
//     storage-decode cost; a real per-row alloc regression blows far past
//     0.5% while fixture noise stays under it. bytes ≤ 1.20 absorbs the
//     RecordBatch column backing the row path never allocates.
//
//   - Agg / Top / GroupBy shapes (W3, W4, W5, W6, W8): vec should win or
//     tie because aggregation/top runs on typed columns and the output row
//     count is bounded by group/heap cardinality, amortizing wrapper
//     reconstruction at egress. ns ≤ 1.05, allocs ≤ 1.05, bytes ≤ 1.20
//     initially — these can tighten once a baseline is recorded (the vec
//     advantage should let ns/allocs drop well below 1.0 for high-input,
//     low-output shapes).
type benchGate struct {
	id            string
	maxNsRatio    float64 // ns/op   ≤ row × maxNsRatio
	maxAllocRatio float64 // allocs  ≤ row × maxAllocRatio
	maxBytesRatio float64 // B/op    ≤ row × maxBytesRatio
}

var benchGates = map[string]benchGate{
	// Scan-shaped (regression continuity + hidden-tags egress strip).
	"W1": {id: "W1", maxNsRatio: 1.05, maxAllocRatio: 1.005, maxBytesRatio: 1.20},
	"W2": {id: "W2", maxNsRatio: 1.05, maxAllocRatio: 1.005, maxBytesRatio: 1.20},
	"W7": {id: "W7", maxNsRatio: 1.05, maxAllocRatio: 1.005, maxBytesRatio: 1.20},
	// Compute-shaped (Top / scalar reduce / raw GroupBy / GroupBy+Agg /
	// COUNT-on-float). vec should win or tie; tighten once baselined.
	"W3": {id: "W3", maxNsRatio: 1.05, maxAllocRatio: 1.05, maxBytesRatio: 1.20},
	"W4": {id: "W4", maxNsRatio: 1.05, maxAllocRatio: 1.05, maxBytesRatio: 1.20},
	"W5": {id: "W5", maxNsRatio: 1.05, maxAllocRatio: 1.05, maxBytesRatio: 1.20},
	"W6": {id: "W6", maxNsRatio: 1.05, maxAllocRatio: 1.05, maxBytesRatio: 1.20},
	"W8": {id: "W8", maxNsRatio: 1.05, maxAllocRatio: 1.05, maxBytesRatio: 1.20},
}

// TestBenchGates_PerWorkload runs both paths inside testing.B harnesses and
// asserts the vec/row ratios. A regression fails this test, not just the
// markdown report — gates are enforced as code.
//
// Skipped unless RUN_BENCH_GATES=1 is set: this test takes ~10–20s of wall
// time per workload and is gated on a CI-tunable knob to keep
// `go test ./...` fast.
func TestBenchGates_PerWorkload(t *testing.T) {
	if os.Getenv("RUN_BENCH_GATES") != "1" {
		t.Skip("set RUN_BENCH_GATES=1 to run G9e bench gates")
	}
	if testing.Short() {
		t.Skip("skipping bench gates in -short mode")
	}
	for _, spec := range allWorkloads {
		t.Run(spec.id, func(t *testing.T) {
			gate := benchGates[spec.id]
			row := timeWorkload(spec, false)
			vec := timeWorkload(spec, true)

			t.Logf("%s row:  %d ns/op, %d B/op, %d allocs/op",
				spec.id, row.NsPerOp(), row.AllocedBytesPerOp(), row.AllocsPerOp())
			t.Logf("%s vec:  %d ns/op, %d B/op, %d allocs/op",
				spec.id, vec.NsPerOp(), vec.AllocedBytesPerOp(), vec.AllocsPerOp())

			if !ratioLE(vec.NsPerOp(), row.NsPerOp(), gate.maxNsRatio) {
				t.Fatalf("%s ns/op gate: vec %d > row %d × %.2f (= %d)",
					spec.id, vec.NsPerOp(), row.NsPerOp(), gate.maxNsRatio,
					int64(float64(row.NsPerOp())*gate.maxNsRatio))
			}
			if !ratioLE(vec.AllocsPerOp(), row.AllocsPerOp(), gate.maxAllocRatio) {
				t.Fatalf("%s allocs/op gate: vec %d > row %d × %.2f (= %d)",
					spec.id, vec.AllocsPerOp(), row.AllocsPerOp(), gate.maxAllocRatio,
					int64(float64(row.AllocsPerOp())*gate.maxAllocRatio))
			}
			if !ratioLE(vec.AllocedBytesPerOp(), row.AllocedBytesPerOp(), gate.maxBytesRatio) {
				t.Fatalf("%s B/op gate: vec %d > row %d × %.2f (= %d)",
					spec.id, vec.AllocedBytesPerOp(), row.AllocedBytesPerOp(), gate.maxBytesRatio,
					int64(float64(row.AllocedBytesPerOp())*gate.maxBytesRatio))
			}
		})
	}
}

// ratioLE reports whether got ≤ baseline × ratio. baseline=0 means we cannot
// form a meaningful ratio; treat as pass.
func ratioLE(got, baseline int64, ratio float64) bool {
	if baseline <= 0 {
		return true
	}
	return float64(got) <= float64(baseline)*ratio
}

// timeWorkload runs the appropriate path inside a testing.B for ~2 seconds
// and returns the result. The caller treats vectorized and row identically
// at the comparison layer.
func timeWorkload(spec workloadSpec, vectorized bool) testing.BenchmarkResult {
	results := buildResults(spec)
	schema := buildSchema(spec)
	opts := buildOpts(spec)
	cfg := cfgFor()
	return testing.Benchmark(func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if vectorized {
				runVectorizedPath(spec, results, schema, opts, cfg)
			} else {
				runRowPath(spec, results, opts)
			}
		}
	})
}
