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

// G5a acceptance gates per spec §"Performance Evaluation Plan". Ratios are
// vectorized / row; failing the gate is a regression that blocks the
// default-flip rollout.
type benchGate struct {
	id            string
	maxNsRatio    float64 // ns/op   ≤ row × maxNsRatio
	maxAllocRatio float64 // allocs  ≤ row × maxAllocRatio
	maxBytesRatio float64 // B/op    ≤ row × maxBytesRatio
}

var benchGates = map[string]benchGate{
	"W1": {id: "W1", maxNsRatio: 1.05, maxAllocRatio: 1.00, maxBytesRatio: 1.20},
	"W2": {id: "W2", maxNsRatio: 1.05, maxAllocRatio: 1.00, maxBytesRatio: 1.20},
	"W3": {id: "W3", maxNsRatio: 1.00, maxAllocRatio: 1.00, maxBytesRatio: 1.20},
	"W4": {id: "W4", maxNsRatio: 1.05, maxAllocRatio: 1.00, maxBytesRatio: 1.20},
	"W5": {id: "W5", maxNsRatio: 1.05, maxAllocRatio: 1.00, maxBytesRatio: 1.20},
}

// TestBenchGates_PerWorkload runs both serialization paths inside testing.B
// harnesses and asserts the spec's vec/row ratios. A regression fails this
// test, not just the markdown report — gates are enforced as code.
//
// Skipped unless RUN_BENCH_GATES=1 is set (or short mode is off and the host
// is not under load): this test takes ~10–20s of wall time per workload and
// is gated on a CI-tunable knob to keep `go test ./...` fast.
func TestBenchGates_PerWorkload(t *testing.T) {
	if os.Getenv("RUN_BENCH_GATES") != "1" {
		t.Skip("set RUN_BENCH_GATES=1 to run G5a bench gates")
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
	cfg := VectorizedConfig{Enabled: true, BatchSize: 1024, QueryMemoryMiB: 64}
	return testing.Benchmark(func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if vectorized {
				runVectorizedPath(results, schema, opts, cfg)
			} else {
				runRowPath(results, opts)
			}
		}
	})
}
