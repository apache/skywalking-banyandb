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

package tracesampler

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/pb/v1/valuetype"
	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk"
	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk/sdktest"
)

func TestDecideAggregatesMixedReasonsByBatch(t *testing.T) {
	plugin, newErr := New([]byte(`{
		"durationThresholdMs":500,"keepErrors":true,"healthySampleRate":0.5,
		"keepTagRules":[
			{"tagKey":"missing","exists":true},
			{"tagKey":"db.type","equals":"PostgreSQL"},
			{"tagKey":"http.method","in":["POST","PUT"]},
			{"tagKey":"http.status_code","regex":"5\\d\\d"}
		]}`), segmentSchema)
	require.NoError(t, newErr)
	sampler := plugin.(*Sampler)
	counter := &recordingCounter{}
	sampler.UseHost(&testHost{meter: &testMeter{counter: counter}})
	rowCounter := &recordingCounter{}
	droppedRowCounter := &recordingCounter{}
	unavailableCounter := &recordingCounter{}
	sampler.rowsTotal = rowCounter
	sampler.rowsDropped = droppedRowCounter
	sampler.rowCountUnavailable = unavailableCounter

	healthyID := traceIDForSampleResult(true, 0.5)
	rejectedID := traceIDForSampleResult(false, 0.5)
	batch := &sdk.TraceBatch{Traces: []sdk.TraceBlock{
		metricSegmentBlock(t, "duration", 600, 0, nil),
		metricSegmentBlock(t, "error", 1, 1, nil),
		metricSegmentBlock(t, "tag-equals-1", 1, 0, []string{"db.type=PostgreSQL"}),
		metricSegmentBlock(t, "tag-equals-2", 1, 0, []string{"db.type=PostgreSQL"}),
		metricSegmentBlock(t, "tag-regex", 1, 0, []string{"http.status_code=503"}),
		metricSegmentBlock(t, healthyID, 1, 0, nil),
		metricSegmentBlock(t, rejectedID, 1, 0, nil),
	}}
	verdict, decideErr := sampler.Decide(batch)
	require.NoError(t, decideErr)
	assert.Equal(t, []bool{true, true, true, true, true, true, false}, verdict.Keep)

	got := counterCallsByLabels(counter.calls)
	assert.Equal(t, float64(1), got[decisionVerdictKeep+"/"+decisionReasonDurationLabel])
	assert.Equal(t, float64(1), got[decisionVerdictKeep+"/"+decisionReasonErrorLabel])
	assert.Equal(t, float64(2), got[decisionVerdictKeep+"/"+decisionReasonTag01Label])
	assert.Equal(t, float64(1), got[decisionVerdictKeep+"/"+decisionReasonTag03Label])
	assert.Equal(t, float64(1), got[decisionVerdictKeep+"/"+decisionReasonHealthySampleLabel])
	assert.Equal(t, float64(1), got[decisionVerdictDrop+"/"+decisionReasonHealthyRejectedLabel])
	assert.Equal(t, float64(len(batch.Traces)), sumCounterDeltas(counter.calls))
	require.Len(t, rowCounter.calls, 1)
	assert.Equal(t, float64(len(batch.Traces)), rowCounter.calls[0].delta)
	assert.Empty(t, rowCounter.calls[0].labels)
	require.Len(t, droppedRowCounter.calls, 1)
	assert.Equal(t, float64(1), droppedRowCounter.calls[0].delta)
	assert.Equal(t, []string{decisionReasonHealthyRejectedLabel}, droppedRowCounter.calls[0].labels)
	assert.Empty(t, unavailableCounter.calls)
}

func TestDecideReportsUnavailableRowCount(t *testing.T) {
	plugin, newErr := New([]byte(`{"healthySampleRate":1}`), segmentSchema)
	require.NoError(t, newErr)
	sampler := plugin.(*Sampler)
	sampler.decisions = &recordingCounter{}
	sampler.rowsTotal = &recordingCounter{}
	sampler.rowsDropped = &recordingCounter{}
	unavailableCounter := &recordingCounter{}
	sampler.rowCountUnavailable = unavailableCounter

	_, decideErr := sampler.Decide(&sdk.TraceBatch{Traces: []sdk.TraceBlock{{TraceID: "metadata-only"}}})
	require.NoError(t, decideErr)
	assert.Empty(t, sampler.rowsTotal.(*recordingCounter).calls)
	assert.Empty(t, sampler.rowsDropped.(*recordingCounter).calls)
	require.Len(t, unavailableCounter.calls, 1)
	assert.Equal(t, float64(1), unavailableCounter.calls[0].delta)
	assert.Empty(t, unavailableCounter.calls[0].labels)
}

func TestDecideTraceSuccessfulReasonsAndPrecedence(t *testing.T) {
	allMatchBlock := metricSegmentBlock(t, "all", 600, 1, []string{"db.type=PostgreSQL"})
	errorAndTagBlock := metricSegmentBlock(t, "error-tag", 1, 1, []string{"db.type=PostgreSQL"})
	tests := []struct {
		name   string
		config string
		block  sdk.TraceBlock
		want   traceDecision
	}{
		{name: "duration wins over error and tag", config: `{"durationThresholdMs":500,"keepErrors":true,"keepTagRules":[{"tagKey":"db.type","exists":true}]}`,
			block: allMatchBlock, want: traceDecision{keep: true, reason: decisionReasonDuration}},
		{name: "error wins over tag", config: `{"keepErrors":true,"keepTagRules":[{"tagKey":"db.type","exists":true}]}`,
			block: errorAndTagBlock, want: traceDecision{keep: true, reason: decisionReasonError}},
		{name: "tag decode failure", config: `{"keepTagRules":[{"tagKey":"db.type","exists":true}]}`,
			block: malformedMetricTagBlock(), want: traceDecision{keep: true, reason: decisionReasonDecodeTags}},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			plugin, newErr := New([]byte(testCase.config), segmentSchema)
			require.NoError(t, newErr)
			assert.Equal(t, testCase.want, plugin.(*Sampler).decideTrace(&testCase.block))
		})
	}
}

func TestDecideTraceEveryTagMatcherReason(t *testing.T) {
	tests := []struct {
		name  string
		rule  string
		entry string
	}{
		{name: "exists", rule: `{"tagKey":"debug","exists":true}`, entry: "debug"},
		{name: "equals", rule: `{"tagKey":"db.type","equals":"PostgreSQL"}`, entry: "db.type=PostgreSQL"},
		{name: "in", rule: `{"tagKey":"http.method","in":["POST","PUT"]}`, entry: "http.method=PUT"},
		{name: "regex", rule: `{"tagKey":"http.status_code","regex":"5\\d\\d"}`, entry: "http.status_code=503"},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			config := `{"keepTagRules":[{"tagKey":"never","exists":true},` + testCase.rule + `]}`
			plugin, newErr := New([]byte(config), segmentSchema)
			require.NoError(t, newErr)
			block := metricSegmentBlock(t, testCase.name, 1, 0, []string{testCase.entry})
			want := traceDecision{keep: true, reason: decisionReasonTagRule, tagRuleIndex: 1}
			assert.Equal(t, want, plugin.(*Sampler).decideTrace(&block))
		})
	}
}

func TestDecideTraceZipkinErrorReason(t *testing.T) {
	plugin, newErr := New([]byte(`{"keepErrors":true}`), zipkinSchema)
	require.NoError(t, newErr)
	block, buildErr := sdktest.NewTrace("zipkin-error").Tag("query", []string{"error", "error=boom"}).Build()
	require.NoError(t, buildErr)
	assert.Equal(t, traceDecision{keep: true, reason: decisionReasonError}, plugin.(*Sampler).decideTrace(&block))
}

func TestDecideEmitsHighestFixedTagSlot(t *testing.T) {
	rules := make([]string, maxKeepTagRules)
	for ruleIndex := range rules {
		rules[ruleIndex] = fmt.Sprintf(`{"tagKey":"key%d","exists":true}`, ruleIndex)
	}
	plugin, newErr := New([]byte(`{"keepTagRules":[`+strings.Join(rules, ",")+`]}`), segmentSchema)
	require.NoError(t, newErr)
	sampler := plugin.(*Sampler)
	counter := &recordingCounter{}
	sampler.UseHost(&testHost{meter: &testMeter{counter: counter}})
	block := metricSegmentBlock(t, "last-slot", 1, 0, []string{"key31=value"})
	_, decideErr := sampler.Decide(&sdk.TraceBatch{Traces: []sdk.TraceBlock{block}})
	require.NoError(t, decideErr)
	require.Len(t, counter.calls, 1)
	assert.Equal(t, []string{decisionVerdictKeep, decisionReasonTag31Label}, counter.calls[0].labels)
}

func metricSegmentBlock(t *testing.T, traceID string, durationMs, errorValue int64, entries []string) sdk.TraceBlock {
	t.Helper()
	builder := sdktest.NewTrace(traceID).
		TagAs("start_time", valuetype.ValueTypeTimestamp, int64(0)).
		Tag("latency", durationMs).
		Tag("is_error", errorValue)
	if entries != nil {
		builder.Tag("tags", entries)
	}
	block, buildErr := builder.Build()
	require.NoError(t, buildErr)
	return block
}

func malformedMetricTagBlock() sdk.TraceBlock {
	return sdk.TraceBlock{TraceID: "malformed", Tags: []sdk.TagColumn{{
		Name: "tags", ValueType: valuetype.ValueTypeStrArr, Values: [][]byte{[]byte("unterminated")},
	}}}
}

func traceIDForSampleResult(keep bool, rate float64) string {
	for candidateIndex := 0; ; candidateIndex++ {
		traceID := fmt.Sprintf("sample-%d", candidateIndex)
		if (sampleFraction(traceID) < rate) == keep {
			return traceID
		}
	}
}

func counterCallsByLabels(calls []counterCall) map[string]float64 {
	result := make(map[string]float64, len(calls))
	for _, call := range calls {
		result[strings.Join(call.labels, "/")] += call.delta
	}
	return result
}

func sumCounterDeltas(calls []counterCall) float64 {
	var total float64
	for _, call := range calls {
		total += call.delta
	}
	return total
}
