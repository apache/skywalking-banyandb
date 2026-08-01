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

// Offline verify-before-you-build-a-.so test for sw-trace-sampler, exercising
// the Scenario 6.1 config against the sdktest fixture kit (no .so, no cluster).
package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/pb/v1/valuetype"
	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk/sdktest"
)

// config61 is the Scenario 6.1 gating config, with healthySampleRate pinned to
// 0 so every keep here is attributable to a sure-keep rule (deterministic).
const config61 = `{
  "durationThresholdMs": 500,
  "keepErrors": true,
  "healthySampleRate": 0,
  "keepTagRules": [
    { "tagKey": "db.type",  "equals": "PostgreSQL" },
    { "tagKey": "mq.queue", "equals": "queue-songs-ping" }
  ]
}`

func TestSWTraceSampler_Scenario61(t *testing.T) {
	sampler, err := NewSampler([]byte(config61))
	require.NoError(t, err)

	// Slow healthy trace: envelope 2802ms ≥ 500ms → sure-keep on duration.
	slow, e := sdktest.NewTrace("slow-healthy").
		TagAs("start_time", valuetype.ValueTypeTimestamp, int64(0)).
		Tag("latency", int64(2802)).Build()
	require.NoError(t, e)
	// Every trace carries start_time: SegmentRecord stores it as a primitive column, so a
	// real segment always has one, and a trace missing it is a can't-tell the duration rule
	// deliberately fails open on. Omitting it here would keep these traces by that fallback
	// and stop them exercising the rules they are named for.
	//
	// Fast error trace: sure-keep on keepErrors.
	errTrace, e := sdktest.NewTrace("error").
		TagAs("start_time", valuetype.ValueTypeTimestamp, int64(0)).
		Tag("latency", int64(4)).Tag("is_error", int64(1)).Build()
	require.NoError(t, e)
	// Fast PostgreSQL trace: sure-keep on the db.type tag rule.
	pg, e := sdktest.NewTrace("postgres").
		TagAs("start_time", valuetype.ValueTypeTimestamp, int64(0)).
		Tag("latency", int64(4)).
		Tag("is_error", int64(0)).
		Tag("tags", []string{"http.method=GET", "db.type=PostgreSQL"}).Build()
	require.NoError(t, e)
	// Fast healthy trace, matches nothing, rate 0 → dropped.
	fast, e := sdktest.NewTrace("fast-healthy").
		TagAs("start_time", valuetype.ValueTypeTimestamp, int64(0)).
		Tag("latency", int64(6)).
		Tag("is_error", int64(0)).
		Tag("tags", []string{"http.method=GET"}).Build()
	require.NoError(t, e)

	verdict, report := sdktest.Run(sampler, sdktest.Batch(slow, errTrace, pg, fast))
	require.NoError(t, report.Err)
	require.NoError(t, report.ProjectionErr)
	assert.Empty(t, report.ProjectionDivergedIDs,
		"sw-trace-sampler must only read the columns it projects (start_time, latency, is_error, tags)")
	assert.Equal(t, []bool{true, true, true, false}, verdict.Keep)
}

func TestSWTraceSampler_Project(t *testing.T) {
	sampler, err := NewSampler([]byte(config61))
	require.NoError(t, err)
	proj := sampler.Project()
	assert.ElementsMatch(t, []string{"latency", "start_time", "is_error", "tags"}, proj.Tags)
	assert.False(t, proj.SpanIDs)
	assert.False(t, proj.Spans)
}
