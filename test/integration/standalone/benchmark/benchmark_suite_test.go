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

package benchmark_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	integration_standalone "github.com/apache/skywalking-banyandb/test/integration/standalone"
)

// TestVectorizedBenchmark is the Ginkgo entry point for the macro suite. It
// runs the W1..W5 workloads against two real Measure modules (flag-off and
// flag-on) and asserts the spec's gate ratios on wall-clock latency, B/op,
// and allocs/op. Default-skipped because each workload boots a full
// standalone — set RUN_VECTORIZED_BENCH=1 to enable.
//
// Use:
//
//	RUN_VECTORIZED_BENCH=1 go test ./test/integration/standalone/benchmark/...
//	  --label-filter '!slow' --vv -timeout=60m
func TestVectorizedBenchmark(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Vectorized Macro Benchmark Suite", Label(integration_standalone.Labels...))
}
