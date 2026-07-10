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

package sdk_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk"
)

// chainFakeSampler is a minimal sdk.Sampler used to drive EvaluateChain
// without a real .so. It optionally panics, errors, or returns a
// wrong-length verdict to exercise the three bypass reasons.
type chainFakeSampler struct {
	dropIDs   map[string]struct{}
	panicNow  bool
	errNow    bool
	wrongSize bool
}

func (f *chainFakeSampler) Kind() sdk.Kind          { return sdk.KindSampler }
func (f *chainFakeSampler) Project() sdk.Projection { return sdk.Projection{} }
func (f *chainFakeSampler) Close() error            { return nil }

func (f *chainFakeSampler) Decide(batch *sdk.TraceBatch) (sdk.Verdict, error) {
	if f.panicNow {
		panic("boom")
	}
	if f.errNow {
		return sdk.Verdict{}, fmt.Errorf("sampler error")
	}
	if f.wrongSize {
		return sdk.Verdict{Keep: []bool{true}}, nil
	}
	keep := make([]bool, len(batch.Traces))
	for i := range batch.Traces {
		_, drop := f.dropIDs[batch.Traces[i].TraceID]
		keep[i] = !drop
	}
	return sdk.Verdict{Keep: keep}, nil
}

func batchOf(ids ...string) *sdk.TraceBatch {
	traces := make([]sdk.TraceBlock, len(ids))
	for i, id := range ids {
		traces[i] = sdk.TraceBlock{TraceID: id}
	}
	return &sdk.TraceBatch{Traces: traces}
}

func TestEvaluateChain_Conjunction(t *testing.T) {
	batch := batchOf("a", "b", "c")
	s1 := &chainFakeSampler{dropIDs: map[string]struct{}{"b": {}}}
	s2 := &chainFakeSampler{dropIDs: map[string]struct{}{"c": {}}}
	verdict := sdk.EvaluateChain([]sdk.Sampler{s1, s2}, batch, nil)
	require.Equal(t, []bool{true, false, false}, verdict.Keep)
}

func TestEvaluateChain_NilSamplerSkipped(t *testing.T) {
	batch := batchOf("a", "b")
	verdict := sdk.EvaluateChain([]sdk.Sampler{nil, &chainFakeSampler{}}, batch, nil)
	require.Equal(t, []bool{true, true}, verdict.Keep)
}

func TestEvaluateChain_DecideErrorBypasses(t *testing.T) {
	batch := batchOf("a", "b")
	var got []sdk.BypassInfo
	verdict := sdk.EvaluateChain([]sdk.Sampler{&chainFakeSampler{errNow: true}}, batch,
		func(_ int, info sdk.BypassInfo) { got = append(got, info) })
	require.Equal(t, []bool{true, true}, verdict.Keep, "error link is bypassed => retain")
	require.Len(t, got, 1)
	assert.Equal(t, sdk.BypassReasonDecideError, got[0].Reason)
	assert.Error(t, got[0].Err)
}

func TestEvaluateChain_PanicBypasses(t *testing.T) {
	batch := batchOf("a", "b")
	var got []sdk.BypassInfo
	verdict := sdk.EvaluateChain([]sdk.Sampler{&chainFakeSampler{panicNow: true}}, batch,
		func(_ int, info sdk.BypassInfo) { got = append(got, info) })
	require.Equal(t, []bool{true, true}, verdict.Keep, "panicking link is bypassed => retain")
	require.Len(t, got, 1)
	assert.Equal(t, sdk.BypassReasonPanic, got[0].Reason)
	assert.Error(t, got[0].Err)
}

func TestEvaluateChain_LengthMismatchBypasses(t *testing.T) {
	batch := batchOf("a", "b", "c")
	var got []sdk.BypassInfo
	verdict := sdk.EvaluateChain([]sdk.Sampler{&chainFakeSampler{wrongSize: true}}, batch,
		func(_ int, info sdk.BypassInfo) { got = append(got, info) })
	require.Equal(t, []bool{true, true, true}, verdict.Keep, "length-mismatched link is bypassed => retain")
	require.Len(t, got, 1)
	assert.Equal(t, sdk.BypassReasonLengthMismatch, got[0].Reason)
	assert.Equal(t, 1, got[0].Got)
	assert.Equal(t, 3, got[0].Want)
}

func TestEvaluateChain_OnBypassNilIsSafe(t *testing.T) {
	batch := batchOf("a")
	require.NotPanics(t, func() {
		sdk.EvaluateChain([]sdk.Sampler{&chainFakeSampler{panicNow: true}}, batch, nil)
	})
}
