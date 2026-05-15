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

// Package benchmark_test contains the macro benchmark suite for the
// vectorized query path. Each workload boots a real standalone Measure
// module twice (flag-off and flag-on), drives identical queries against
// each, and asserts the spec's gate ratios. Skipped unless
// RUN_VECTORIZED_BENCH=1 is set so it does not run on every PR.
package benchmark_test

import (
	"os"

	"github.com/onsi/ginkgo/v2"
)

// G5a macro suite — currently a structural placeholder. The microbench
// gate at pkg/query/vectorized/measure/bench_gates_test.go is the
// authoritative G5a check; this suite captures end-to-end query latency
// against a live standalone for the rollout decision and is deferred until
// the vectorized adapter has wired operators (BatchLimit / BatchGroupBy /
// BatchAggregation / BatchTop) into the live Pipeline. Without that
// wiring, end-to-end W3/W4 measurements still flow through the row path's
// post-aggregation stage and don't reflect operator-level cost.
//
// To enable the suite (e.g. once operator wiring lands):
//
//	RUN_VECTORIZED_BENCH=1 go test \
//	  ./test/integration/standalone/benchmark/... \
//	  -timeout=60m -ginkgo.label-filter '!slow'
var _ = ginkgo.Describe("vectorized macro benchmarks", ginkgo.Ordered, func() {
	ginkgo.BeforeAll(func() {
		if os.Getenv("RUN_VECTORIZED_BENCH") != "1" {
			ginkgo.Skip("set RUN_VECTORIZED_BENCH=1 to run the macro vectorized benchmarks")
		}
	})

	for _, w := range Workloads() {
		w := w
		ginkgo.It("workload "+w.ID, ginkgo.Label("slow"), func() {
			// TODO(G5a-followup): boot two standalones (flag-off, flag-on)
			//   reusing setup.ClosableStandalone, run RunWorkload(w) against
			//   each, write the markdown report via Report(), then assert
			//   gate ratios. Tracked in the deferred items list at the
			//   bottom of .omc/plans/autopilot-impl.md.
			ginkgo.Skip("macro implementation deferred; see microbench gate for G5a signal")
		})
	}
})
