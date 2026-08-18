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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/encoding/vararray"
	"github.com/apache/skywalking-banyandb/pkg/pb/v1/valuetype"
	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk"
)

type counterCall struct {
	labels []string
	delta  float64
}

type recordingCounter struct{ calls []counterCall }

func (r *recordingCounter) Inc(delta float64, labels ...string) {
	r.calls = append(r.calls, counterCall{delta: delta, labels: append([]string(nil), labels...)})
}

type testMeter struct {
	counter    *recordingCounter
	registered map[string][]string
}

func (m *testMeter) Counter(name string, labelNames ...string) sdk.Counter {
	if m.registered == nil {
		m.registered = make(map[string][]string)
	}
	m.registered[name] = append([]string(nil), labelNames...)
	if name == decisionMetricName {
		return m.counter
	}
	return discardCounter{}
}
func (m *testMeter) Gauge(_ string, _ ...string) sdk.Gauge { return nil }
func (m *testMeter) Histogram(_ string, _ []float64, _ ...string) sdk.Histogram {
	return nil
}

type testHost struct{ meter *testMeter }

type discardCounter struct{}

func (discardCounter) Inc(_ float64, _ ...string) {}

func (h *testHost) Meter() sdk.Meter   { return h.meter }
func (h *testHost) Logger() sdk.Logger { return nil }

func TestSamplerImplementsHostAware(t *testing.T) {
	var _ sdk.HostAware = (*Sampler)(nil)
}

func TestUseHostRegistersDecisionMetricContract(t *testing.T) {
	plugin, newErr := New([]byte(`{"healthySampleRate":0}`), segmentSchema)
	require.NoError(t, newErr)
	meter := &testMeter{counter: &recordingCounter{}}
	plugin.(*Sampler).UseHost(&testHost{meter: meter})
	assert.Equal(t, []string{decisionLabelVerdict, decisionLabelRule}, meter.registered[decisionMetricName])
	assert.Empty(t, meter.registered[rowMetricName])
	assert.Equal(t, []string{decisionLabelRule}, meter.registered[rowDroppedMetricName])
	assert.Empty(t, meter.registered[rowCountUnavailableMetricName])
}

func TestDecideAggregatesDecisionMetricsByBatch(t *testing.T) {
	plugin, newErr := New([]byte(`{"healthySampleRate":0}`), segmentSchema)
	require.NoError(t, newErr)
	sampler := plugin.(*Sampler)
	counter := &recordingCounter{}
	sampler.UseHost(&testHost{meter: &testMeter{counter: counter}})

	verdict, decideErr := sampler.Decide(&sdk.TraceBatch{Traces: []sdk.TraceBlock{{TraceID: "a"}, {TraceID: "b"}, {TraceID: "c"}}})
	require.NoError(t, decideErr)
	assert.Equal(t, []bool{false, false, false}, verdict.Keep)
	require.Len(t, counter.calls, 1, "one metric write represents every trace with the same reason")
	assert.Equal(t, float64(3), counter.calls[0].delta)
	assert.Equal(t, []string{decisionVerdictDrop, decisionReasonNoKeepRuleLabel}, counter.calls[0].labels)
}

func TestDecideWithoutHostPreservesVerdict(t *testing.T) {
	plugin, newErr := New([]byte(`{"healthySampleRate":1}`), segmentSchema)
	require.NoError(t, newErr)
	verdict, decideErr := plugin.Decide(&sdk.TraceBatch{Traces: []sdk.TraceBlock{{TraceID: "a"}}})
	require.NoError(t, decideErr)
	assert.Equal(t, []bool{true}, verdict.Keep)
}

func TestNewRejectsMoreThanFixedTagSlots(t *testing.T) {
	rules := make([]string, maxKeepTagRules+1)
	for ruleIndex := range rules {
		rules[ruleIndex] = `{"tagKey":"key","exists":true}`
	}
	_, newErr := New([]byte(`{"keepTagRules":[`+strings.Join(rules, ",")+`]}`), segmentSchema)
	require.Error(t, newErr)
	assert.Contains(t, newErr.Error(), "maximum is 32")
}

func TestDecideTraceReasons(t *testing.T) {
	encodedTag := vararray.MarshalVarArray(nil, []byte("db.type=PostgreSQL"))
	tagBlock := sdk.TraceBlock{TraceID: "tag", Tags: []sdk.TagColumn{{
		Name: "tags", ValueType: valuetype.ValueTypeStrArr, Values: [][]byte{encodedTag},
	}}}
	tests := []struct {
		name   string
		config string
		block  sdk.TraceBlock
		want   traceDecision
	}{
		{
			name: "tag rule", config: `{"keepTagRules":[{"tagKey":"missing","exists":true},{"tagKey":"db.type","equals":"PostgreSQL"}]}`,
			block: tagBlock, want: traceDecision{keep: true, reason: decisionReasonTagRule, tagRuleIndex: 1},
		},
		{
			name: "duration decode failure", config: `{"durationThresholdMs":1}`,
			block: sdk.TraceBlock{}, want: traceDecision{keep: true, reason: decisionReasonDecodeDuration},
		},
		{
			name: "error decode failure", config: `{"keepErrors":true}`,
			block: sdk.TraceBlock{}, want: traceDecision{keep: true, reason: decisionReasonDecodeError},
		},
		{
			name: "healthy sample", config: `{"healthySampleRate":1}`,
			block: sdk.TraceBlock{TraceID: "sample"}, want: traceDecision{keep: true, reason: decisionReasonHealthySample},
		},
		{
			name: "healthy rejected", config: `{"healthySampleRate":0.000000001}`,
			block: sdk.TraceBlock{TraceID: "reject"}, want: traceDecision{reason: decisionReasonHealthyRejected},
		},
		{
			name: "no keep rule", config: `{"healthySampleRate":0}`,
			block: sdk.TraceBlock{TraceID: "drop"}, want: traceDecision{reason: decisionReasonNoKeepRule},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			plugin, newErr := New([]byte(test.config), segmentSchema)
			require.NoError(t, newErr)
			assert.Equal(t, test.want, plugin.(*Sampler).decideTrace(&test.block))
		})
	}
}
