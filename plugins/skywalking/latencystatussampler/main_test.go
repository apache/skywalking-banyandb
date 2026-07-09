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

// This test file verifies Decide directly against the sdktest fixture kit —
// no .so build, no cluster — exactly the offline verify-before-you-build-a-.so
// workflow plugins/README.md recommends for a new plugin. Being package main
// (a same-directory white-box test), it can construct latencyStatusSampler
// directly rather than going through NewSampler+plugin.Open.
package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk/sdktest"
)

func TestLatencyStatusSampler_Decide(t *testing.T) {
	sampler, err := NewSampler([]byte(`{"thresholdMs":300,"successValue":"success"}`))
	require.NoError(t, err)

	dropped, buildErr := sdktest.NewTrace("dropped").
		Tag("duration", int64(100)).
		Tag("status", "success").
		Build()
	require.NoError(t, buildErr)

	keptWrongStatus, buildErr := sdktest.NewTrace("kept-wrong-status").
		Tag("duration", int64(100)).
		Tag("status", "failure").
		Build()
	require.NoError(t, buildErr)

	keptTooSlow, buildErr := sdktest.NewTrace("kept-too-slow").
		Tag("duration", int64(500)).
		Tag("status", "success").
		Build()
	require.NoError(t, buildErr)

	keptNoColumns, buildErr := sdktest.NewTrace("kept-no-columns").Build()
	require.NoError(t, buildErr)

	batch := sdktest.Batch(dropped, keptWrongStatus, keptTooSlow, keptNoColumns)
	verdict, report := sdktest.Run(sampler, batch)
	require.NoError(t, report.Err)
	require.NoError(t, report.ProjectionErr)
	assert.Empty(t, report.ProjectionDivergedIDs,
		"latencystatussampler must only read the columns it projects (duration, status)")
	require.Equal(t, []bool{false, true, true, true}, verdict.Keep)
}

func TestLatencyStatusSampler_DefaultsAndInvalidConfig(t *testing.T) {
	// Empty config uses the documented defaults (thresholdMs=500, successValue="success").
	sampler, err := NewSampler(nil)
	require.NoError(t, err)
	s, ok := sampler.(*latencyStatusSampler)
	require.True(t, ok)
	assert.Equal(t, int64(defaultThresholdMs), s.thresholdMs)
	assert.Equal(t, defaultSuccessValue, s.successValue)

	// An empty JSON object still yields the defaults (no field present).
	sampler, err = NewSampler([]byte(`{}`))
	require.NoError(t, err)
	s, ok = sampler.(*latencyStatusSampler)
	require.True(t, ok)
	assert.Equal(t, int64(defaultThresholdMs), s.thresholdMs)
	assert.Equal(t, defaultSuccessValue, s.successValue)

	// Malformed JSON must fail open at construction (no sampler registered).
	_, err = NewSampler([]byte(`{"thresholdMs": "not-a-number"}`))
	require.Error(t, err)
}

// TestLatencyStatusSampler_ExplicitZeroValuesHonored proves an explicitly
// configured zero (thresholdMs:0 / successValue:"") is used verbatim, not
// silently overridden by the default — the reason samplerConfig uses pointer
// fields. thresholdMs:0 makes the "duration < threshold" test unsatisfiable,
// so nothing is ever dropped on the duration criterion.
func TestLatencyStatusSampler_ExplicitZeroValuesHonored(t *testing.T) {
	sampler, err := NewSampler([]byte(`{"thresholdMs":0,"successValue":""}`))
	require.NoError(t, err)
	s, ok := sampler.(*latencyStatusSampler)
	require.True(t, ok)
	assert.Equal(t, int64(0), s.thresholdMs, "explicit thresholdMs:0 must not be overridden by the default")
	assert.Equal(t, "", s.successValue, "explicit successValue:\"\" must not be overridden by the default")

	// With thresholdMs=0, a would-otherwise-drop trace (duration 100, matching
	// status) is KEPT: 100 < 0 is false, so the drop condition never holds.
	block, buildErr := sdktest.NewTrace("t1").Tag("duration", int64(100)).Tag("status", "").Build()
	require.NoError(t, buildErr)
	verdict, report := sdktest.Run(sampler, sdktest.Batch(block))
	require.NoError(t, report.Err)
	assert.Equal(t, []bool{true}, verdict.Keep, "thresholdMs:0 must keep everything on the duration test")
}

func TestLatencyStatusSampler_Project(t *testing.T) {
	sampler, err := NewSampler(nil)
	require.NoError(t, err)
	proj := sampler.Project()
	assert.ElementsMatch(t, []string{"duration", "status"}, proj.Tags)
	assert.False(t, proj.SpanIDs)
	assert.False(t, proj.Spans)
}
