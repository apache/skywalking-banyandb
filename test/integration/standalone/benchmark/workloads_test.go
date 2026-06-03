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

// Package benchmark_test holds the macro benchmark workload catalog and
// helpers. Workloads mirror the catalog in
// .omc/specs/deep-interview-vectorized-query-critique.md §"Performance
// Evaluation Plan". Each entry is the single source of truth shared between
// the microbench gate (pkg/query/vectorized/measure/bench_test.go) and this
// macro suite.
package benchmark_test

// Workload describes one macro shape — series count, points per series, and
// the kind of work that dominates (scan, group-by, top-N).
type Workload struct {
	ID          string
	Description string
	Series      int
	PointsEach  int
	Kind        WorkloadKind
}

// WorkloadKind tags the benchmark intent so the runner can pick the right
// query template per shape.
type WorkloadKind int

// Recognized workload kinds.
const (
	WorkloadScan WorkloadKind = iota
	WorkloadMixedProjection
	WorkloadGroupByAggregation
	WorkloadTopN
	WorkloadLargeFanout
)

// Workloads returns the W1..W5 catalog at the spec's nominal scale. The
// macro suite scales these to fit available host capacity; the microbench
// uses smaller variants in pkg/query/vectorized/measure/bench_test.go.
func Workloads() []Workload {
	return []Workload{
		{ID: "W1", Description: "Single-series scan; 1×10 000", Series: 1, PointsEach: 10000, Kind: WorkloadScan},
		{ID: "W2", Description: "Mixed projection; 100×1 000", Series: 100, PointsEach: 1000, Kind: WorkloadMixedProjection},
		{ID: "W3", Description: "GroupBy + SUM/COUNT; 1 000×100", Series: 1000, PointsEach: 100, Kind: WorkloadGroupByAggregation},
		{ID: "W4", Description: "Top-N=100 over 100×10 000", Series: 100, PointsEach: 10000, Kind: WorkloadTopN},
		{ID: "W5", Description: "Wide fanout; 10 000×1 000 full projection", Series: 10000, PointsEach: 1000, Kind: WorkloadLargeFanout},
	}
}
