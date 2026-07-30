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

// Offline verify-before-you-build-a-.so test for zipkin-trace-sampler,
// exercising the Scenario 6.2 config against the sdktest fixture kit.
package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/pb/v1/valuetype"
	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk/sdktest"
)

// config62 is the Scenario 6.2 gating config, with healthySampleRate pinned to
// 0 so every keep here is attributable to a sure-keep rule (deterministic).
const config62 = `{
  "durationThresholdMs": 1000,
  "healthySampleRate": 0,
  "keepTagRules": [
    { "tagKey": "query", "regex": "http\\.status_code=5\\d\\d" }
  ]
}`

func TestZipkinTraceSampler_Scenario62(t *testing.T) {
	sampler, err := NewSampler([]byte(config62))
	require.NoError(t, err)

	// Slow mesh call: envelope 30.7s (duration µs) ≥ 1s → sure-keep on duration.
	slow, e := sdktest.NewTrace("slow-mesh").
		TagAs("timestamp_millis", valuetype.ValueTypeTimestamp, int64(0)).
		Tag("duration", int64(30_700_000)).
		Tag("query", []string{"http.status_code", "http.status_code=101"}).Build()
	require.NoError(t, e)
	// Every trace carries timestamp_millis: ZipkinSpanRecord stores it as a primitive
	// column, so a real span always has one, and a trace missing it is a can't-tell the
	// duration rule deliberately fails open on. Omitting it here would keep these traces by
	// that fallback and stop them exercising the rules they are named for.
	//
	// Fast 5xx span: sure-keep on the query regex rule.
	serverError, e := sdktest.NewTrace("server-error").
		TagAs("timestamp_millis", valuetype.ValueTypeTimestamp, int64(0)).
		Tag("duration", int64(3_000)).
		Tag("query", []string{"http.status_code", "http.status_code=500"}).Build()
	require.NoError(t, e)
	// Fast p50 span, matches nothing, rate 0 → dropped.
	p50, e := sdktest.NewTrace("p50").
		TagAs("timestamp_millis", valuetype.ValueTypeTimestamp, int64(0)).
		Tag("duration", int64(2_000)).
		Tag("query", []string{"http.status_code", "http.status_code=200"}).Build()
	require.NoError(t, e)

	verdict, report := sdktest.Run(sampler, sdktest.Batch(slow, serverError, p50))
	require.NoError(t, report.Err)
	require.NoError(t, report.ProjectionErr)
	assert.Empty(t, report.ProjectionDivergedIDs,
		"zipkin-trace-sampler must only read the columns it projects (timestamp_millis, duration, query)")
	assert.Equal(t, []bool{true, true, false}, verdict.Keep)
}

// TestZipkinTraceSampler_KeepErrors covers keepErrors on a schema with no error
// column: it detects Zipkin's conventional "error" span tag, which OAP writes into
// "query" as both a bare key and "error=<message>". healthySampleRate is 0 so a
// keep can only come from the error signal.
func TestZipkinTraceSampler_KeepErrors(t *testing.T) {
	sampler, err := NewSampler([]byte(`{"keepErrors":true,"healthySampleRate":0}`))
	require.NoError(t, err)

	// OAP's normal output: the bare key plus "error=<message>".
	both, e := sdktest.NewTrace("err-both").
		Tag("query", []string{"error", "error=Connection refused", "http.method=GET"}).Build()
	require.NoError(t, e)
	// An empty error message still marks the span as failed.
	empty, e := sdktest.NewTrace("err-empty").Tag("query", []string{"error="}).Build()
	require.NoError(t, e)
	// A tag whose key merely starts with "error" must NOT count.
	lookalike, e := sdktest.NewTrace("lookalike").
		Tag("query", []string{"error_rate=0", "http.method=GET"}).Build()
	require.NoError(t, e)
	// A healthy span has no error entry at all.
	healthy, e := sdktest.NewTrace("healthy").Tag("query", []string{"http.status_code=200"}).Build()
	require.NoError(t, e)

	verdict, report := sdktest.Run(sampler, sdktest.Batch(both, empty, lookalike, healthy))
	require.NoError(t, report.Err)
	assert.Empty(t, report.ProjectionDivergedIDs,
		"keepErrors must read only the projected query column")
	assert.Equal(t, []bool{true, true, false, false}, verdict.Keep)

	// keepErrors projects the array column, not a separate error column.
	assert.ElementsMatch(t, []string{"query"}, sampler.Project().Tags)
}

func TestZipkinTraceSampler_Project(t *testing.T) {
	sampler, err := NewSampler([]byte(config62))
	require.NoError(t, err)
	proj := sampler.Project()
	assert.ElementsMatch(t, []string{"duration", "timestamp_millis", "query"}, proj.Tags)
	assert.False(t, proj.SpanIDs)
	assert.False(t, proj.Spans)
}
