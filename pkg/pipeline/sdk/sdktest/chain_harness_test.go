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

package sdktest_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk"
	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk/sdktest"
)

// chainHarnessSampler is a minimal, configurable sdk.Sampler for exercising
// RunChain's bypass-recording behavior.
type chainHarnessSampler struct {
	dropIDs   map[string]struct{}
	panicNow  bool
	errNow    bool
	wrongSize bool
}

func (f *chainHarnessSampler) Kind() sdk.Kind          { return sdk.KindSampler }
func (f *chainHarnessSampler) Project() sdk.Projection { return sdk.Projection{} }
func (f *chainHarnessSampler) Close() error            { return nil }
func (f *chainHarnessSampler) Decide(batch *sdk.TraceBatch) (sdk.Verdict, error) {
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

func TestRunChain_Conjunction(t *testing.T) {
	a, err := sdktest.NewTrace("a").Build()
	require.NoError(t, err)
	b, err := sdktest.NewTrace("b").Build()
	require.NoError(t, err)
	batch := sdktest.Batch(a, b)

	s1 := &chainHarnessSampler{dropIDs: map[string]struct{}{"a": {}}}
	verdict, report := sdktest.RunChain([]sdk.Sampler{s1}, batch)
	assert.Equal(t, []bool{false, true}, verdict.Keep)
	assert.Empty(t, report.Bypassed)
}

func TestRunChain_RecordsBypassReasons(t *testing.T) {
	trace, err := sdktest.NewTrace("a").Build()
	require.NoError(t, err)
	batch := sdktest.Batch(trace)

	samplers := []sdk.Sampler{
		&chainHarnessSampler{errNow: true},
		&chainHarnessSampler{panicNow: true},
		&chainHarnessSampler{},
	}
	verdict, report := sdktest.RunChain(samplers, batch)
	assert.Equal(t, []bool{true}, verdict.Keep, "both failing links bypass; the healthy link keeps everything")
	require.Len(t, report.Bypassed, 2)
	assert.Equal(t, 0, report.Bypassed[0].Idx)
	assert.Equal(t, sdk.BypassReasonDecideError, report.Bypassed[0].Reason)
	assert.Equal(t, 1, report.Bypassed[1].Idx)
	assert.Equal(t, sdk.BypassReasonPanic, report.Bypassed[1].Reason)
}

func TestRunChain_LengthMismatchRecorded(t *testing.T) {
	a, err := sdktest.NewTrace("a").Build()
	require.NoError(t, err)
	b, err := sdktest.NewTrace("b").Build()
	require.NoError(t, err)
	batch := sdktest.Batch(a, b)

	_, report := sdktest.RunChain([]sdk.Sampler{&chainHarnessSampler{wrongSize: true}}, batch)
	require.Len(t, report.Bypassed, 1)
	assert.Equal(t, sdk.BypassReasonLengthMismatch, report.Bypassed[0].Reason)
	assert.Equal(t, 1, report.Bypassed[0].Got)
	assert.Equal(t, 2, report.Bypassed[0].Want)
}
