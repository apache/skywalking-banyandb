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

//go:build trace_pipeline

package pipeline_test

import (
	tracepipeline "github.com/apache/skywalking-banyandb/test/cases/tracepipeline"
)

// RegisterSoak wires the opt-in basic soak into the standalone pipeline suite.
//
// The soak reuses the SharedContext (and therefore the same external standalone server)
// set up by common.go's SynchronizedBeforeSuite.  The instant suite configures the server
// with --trace-pipeline-merge-grace-default=0; the soak exercises the non-zero grace path
// by writing all soak timestamps soakGracePad (2 min) in the past, which guarantees
// isMergeHot returns false for any grace value ≤ 2 min (including the server default of 30 s).
//
// Enable with: TRACE_PIPELINE_SOAK=1
// Override duration: TRACE_PIPELINE_SOAK_DURATION=20s  (default: 3 m)
//
// Example short smoke:
//
//	TRACE_PIPELINE_SOAK=1 TRACE_PIPELINE_SOAK_DURATION=20s \
//	  BANYAND_TRACE_PLUGIN=<path>/latencystatussampler.so \
//	  go test ./test/integration/standalone/pipeline/... -run Soak -timeout 300s
var _ = tracepipeline.RegisterSoak("Standalone (.so plugin): In-Merge Filter Soak")

// RegisterSoakDynamic wires the US-010 dynamic soak into the standalone pipeline suite.
//
// The dynamic soak extends the basic write→merge→verify loop with:
//   - Periodic Register/Update/Remove sampler churn (every 15 iterations), rotating
//     through base (thresholdMs=500) → variant (thresholdMs=200) → remove → base.
//   - Mid-run panicking-sampler injection (iteration 5): registers the latencystatussampler
//     with "panic":true in its config so Decide panics unconditionally.  The engine's
//     fail-open recover wrapper absorbs the panic; the test asserts the node is still
//     reachable (GroupRegistryService.List), traces are retained (no sampler active →
//     retain all), and subsequent merges are not stalled.
//   - Bounded test-driver heap growth gate (3× initial HeapInuse).
//   - No close-balance / ref-count gate (R4): plugins are immortal; Close() is never
//     called on registry mutation.
//
// The spec is Serial because it mutates the group's pipeline config and must not
// race with the basic soak or the instant-suite specs.
//
// Enable with: TRACE_PIPELINE_SOAK=1  (same gate as the basic soak).
// Override duration: TRACE_PIPELINE_SOAK_DURATION=20s  (default: 3 m).
var _ = tracepipeline.RegisterSoakDynamic(
	"Standalone (.so plugin): Dynamic Sampler Registration Soak (US-010)",
	func() string { return svrSoPath },
)
