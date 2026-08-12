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

package trace

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk"
	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk/sdktest"
)

// TestConformance_EngineMergePathMatchesSdktest is the DD-T6 golden test: it
// feeds the SAME logical fixture — three traces, one sampler configured to
// drop the middle one — through BOTH the real engine merge path
// (mergeWithFilter, exercising mergeChain.Execute/runChain over actual
// on-disk parts) and the offline sdktest.RunChain harness, and asserts the
// two report identical keep/drop verdicts.
//
// Today this is guaranteed to hold structurally, because runChain and
// RunChain both delegate to the single shared sdk.EvaluateChain (D0) — but
// that is exactly the point: this test is the belt-and-suspenders guard that
// keeps it that way. If a future change (e.g. #1126-style column-assembly
// semantics) makes the engine's fixture-building or chain wiring diverge from
// sdktest's, this test fails loudly instead of the divergence going
// unnoticed until a field report.
func TestConformance_EngineMergePathMatchesSdktest(t *testing.T) {
	ids := []string{"traceA", "traceB", "traceC"}
	sampler := &fakeSampler{dropIDs: map[string]struct{}{"traceB": {}}}

	// --- Real engine merge path ---
	filter := &mergeFilter{
		chain:   newMergeChain("g", "s", []sdk.Sampler{sampler}, 0),
		timeout: time.Second,
	}
	engineGot, engineDropped := mergeWithFilter(t, singleTraceParts(ids), filter)
	require.Equal(t, []string{"traceA", "traceC"}, engineGot)
	require.Contains(t, engineDropped, "traceB")
	require.Len(t, engineDropped, 1)

	// --- Offline sdktest chain harness, same sampler, same trace IDs ---
	blocks := make([]sdk.TraceBlock, 0, len(ids))
	for _, id := range ids {
		block, buildErr := sdktest.NewTrace(id).Build()
		require.NoError(t, buildErr)
		blocks = append(blocks, block)
	}
	batch := sdktest.Batch(blocks...)
	verdict, report := sdktest.RunChain([]sdk.Sampler{sampler}, batch)
	require.Empty(t, report.Bypassed, "the shared sampler must not be bypassed on either path")

	sdktestDropped := make(map[string]struct{})
	for i, keep := range verdict.Keep {
		if !keep {
			sdktestDropped[batch.Traces[i].TraceID] = struct{}{}
		}
	}

	require.Equal(t, len(engineDropped), len(sdktestDropped),
		"the real engine merge path and sdktest.RunChain must drop the same NUMBER of traces")
	for id := range engineDropped {
		_, ok := sdktestDropped[id]
		require.True(t, ok, "trace %q dropped by the engine merge path must also be dropped by sdktest.RunChain", id)
	}
}
