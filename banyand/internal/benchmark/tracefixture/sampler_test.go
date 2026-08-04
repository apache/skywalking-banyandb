// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses this
// file to you under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package tracefixture

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/convert"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk"
)

type recordingSampler struct {
	project sdk.Projection
	blocks  []sdk.TraceBlock
}

func TestClassifyDefaultSamplerRuleUsesPluginPriority(t *testing.T) {
	row := func(start, latency, isError int64) Row {
		return Row{Tags: map[string][]byte{
			"start_time": convert.Int64ToBytes(start), "latency": convert.Int64ToBytes(latency), "is_error": convert.Int64ToBytes(isError),
		}}
	}
	require.Equal(t, "duration", classifyDefaultSamplerRule(LoadedTrace{Fragments: []LoadedFragment{{Rows: []Row{row(0, 600, 1)}}}}))
	require.Equal(t, "error", classifyDefaultSamplerRule(LoadedTrace{Fragments: []LoadedFragment{{Rows: []Row{row(0, 10, 1)}}}}))
	require.Equal(t, "healthy", classifyDefaultSamplerRule(LoadedTrace{Fragments: []LoadedFragment{{Rows: []Row{row(0, 10, 0)}}}}))
	require.Contains(t, string(DefaultSkyWalkingSamplerConfig), `"healthySampleRate":"0.1"`)
}

func (rs *recordingSampler) Kind() sdk.Kind          { return sdk.KindSampler }
func (rs *recordingSampler) Project() sdk.Projection { return rs.project }
func (rs *recordingSampler) Close() error            { return nil }
func (rs *recordingSampler) Decide(batch *sdk.TraceBatch) (sdk.Verdict, error) {
	rs.blocks = append(rs.blocks, batch.Traces...)
	keep := make([]bool, len(batch.Traces))
	for traceIdx := range keep {
		keep[traceIdx] = traceIdx%3 != 0
	}
	return sdk.Verdict{Keep: keep}, nil
}

func TestBuildSamplerBlockUsesCompleteTrace(t *testing.T) {
	source := Source{Mature: []LoadedTrace{
		{
			SourceID: "source",
			Fragments: []LoadedFragment{
				{Fragment: Fragment{MinTimestamp: 10, MaxTimestamp: 20}, Rows: []Row{{
					SpanID: "one", Tags: map[string][]byte{"latency": {1}}, TagTypes: map[string]pbv1.ValueType{"latency": pbv1.ValueTypeInt64},
				}}},
				{Fragment: Fragment{MinTimestamp: 30, MaxTimestamp: 40}, Rows: []Row{{
					SpanID: "two", Tags: map[string][]byte{"latency": {2}}, TagTypes: map[string]pbv1.ValueType{"latency": pbv1.ValueTypeInt64},
				}}},
			},
		},
	}}
	lookup := buildSourceLookup(source)
	block, blockErr := buildSamplerBlock(Instance{SourceID: "source", GeneratedID: "generated"}, lookup,
		sdk.Projection{Tags: []string{"latency"}, SpanIDs: true})
	require.NoError(t, blockErr)
	require.Equal(t, "generated", block.TraceID)
	require.Equal(t, int64(10), block.MinTS)
	require.Equal(t, int64(40), block.MaxTS)
	require.Equal(t, []string{"one", "two"}, block.SpanIDs)
	require.Equal(t, [][]byte{{1}, {2}}, block.Tags[0].Values)
}

func TestValidateDefaultSamplerVerdict(t *testing.T) {
	require.NoError(t, validateDefaultSamplerVerdict(samplerVerdict{traceID: "slow", reason: "duration", keep: true}))
	require.ErrorContains(t, validateDefaultSamplerVerdict(samplerVerdict{traceID: "slow", reason: "duration"}), "dropped sure-keep")
	require.NoError(t, validateDefaultSamplerVerdict(samplerVerdict{traceID: "error", reason: "error", keep: true}))
	require.ErrorContains(t, validateDefaultSamplerVerdict(samplerVerdict{traceID: "error", reason: "error"}), "dropped sure-keep")

	healthyKept, healthyDropped := healthyTraceIDs(t)
	require.NoError(t, validateDefaultSamplerVerdict(samplerVerdict{traceID: healthyKept, reason: "healthy", keep: true}))
	require.ErrorContains(t, validateDefaultSamplerVerdict(samplerVerdict{traceID: healthyKept, reason: "healthy"}), "want true")
	require.NoError(t, validateDefaultSamplerVerdict(samplerVerdict{traceID: healthyDropped, reason: "healthy"}))
	require.ErrorContains(t, validateDefaultSamplerVerdict(samplerVerdict{traceID: healthyDropped, reason: "healthy", keep: true}), "want false")
	require.ErrorContains(t, validateDefaultSamplerVerdict(samplerVerdict{traceID: "unknown", reason: "other", keep: true}), "unknown sampler rule")
}

func healthyTraceIDs(t *testing.T) (string, string) {
	t.Helper()
	var kept, dropped string
	for traceIdx := 0; kept == "" || dropped == ""; traceIdx++ {
		traceID := fmt.Sprintf("trace-%d", traceIdx)
		if defaultHealthySamplerKeep(traceID) {
			kept = traceID
		} else {
			dropped = traceID
		}
	}
	return kept, dropped
}
