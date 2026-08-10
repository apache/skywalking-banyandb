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

// White-box tests for the shared sampler engine, verified offline against the
// sdktest fixture kit (no .so build, no cluster) per plugins/README.md.
package tracesampler

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/encoding/vararray"
	"github.com/apache/skywalking-banyandb/pkg/pb/v1/valuetype"
	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk"
	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk/sdktest"
)

// segmentSchema and zipkinSchema mirror the two first-party plugins' Schema
// values so the engine is exercised exactly as they configure it — including
// FirstClassColumns, since a stale copy here would let a never-match rule pass the
// tests while the shipped plugin rejects it.
var (
	segmentColumns = []string{
		"segment_id", "trace_id", "service_id", "service_instance_id",
		"endpoint_id", "start_time", "latency", "is_error", "data_binary",
	}
	zipkinColumns = []string{
		"trace_id", "span_id", "parent_id", "name", "duration", "kind",
		"timestamp_millis", "timestamp", "local_endpoint_service_name",
		"local_endpoint_port", "remote_endpoint_service_name",
		"remote_endpoint_port", "annotations", "tags", "debug", "shared",
	}
	segmentSchema = Schema{
		ArrayTagColumn: "tags", ErrorTag: "is_error",
		DurationTag: "latency", StartTimeTag: "start_time", DurationTagNanosPerUnit: 1_000_000,
		FirstClassColumns: segmentColumns,
	}
	zipkinSchema = Schema{
		ArrayTagColumn: "query", ErrorTag: "error", ErrorTagInArray: true,
		DurationTag: "duration", StartTimeTag: "timestamp_millis", DurationTagNanosPerUnit: 1_000,
		FirstClassColumns: zipkinColumns,
	}
	// noErrorSchema has no error signal at all, so keepErrors must be rejected.
	noErrorSchema = Schema{ArrayTagColumn: "tags"}
)

func TestNew_ConfigValidation(t *testing.T) {
	cases := []struct {
		name   string
		config string
		schema Schema
	}{
		{"invalid JSON", `{`, segmentSchema},
		{"rate below 0", `{"healthySampleRate":-0.1}`, segmentSchema},
		{"rate above 1", `{"healthySampleRate":1.1}`, segmentSchema},
		{"empty tagKey", `{"keepTagRules":[{"equals":"x"}]}`, segmentSchema},
		{"no matcher", `{"keepTagRules":[{"tagKey":"db.type"}]}`, segmentSchema},
		{"bad regex", `{"keepTagRules":[{"tagKey":"db.type","regex":"("}]}`, segmentSchema},
		{"bad duration type", `{"durationThresholdMs":"nope"}`, segmentSchema},
		{"negative duration", `{"durationThresholdMs":-5}`, segmentSchema},
		{"keepErrors without error signal", `{"keepErrors":true}`, noErrorSchema},
		// A rule on a first-class column can never match, since every rule resolves to
		// the flattened array column. Rejecting beats silently never firing.
		{"rule on is_error column", `{"keepTagRules":[{"tagKey":"is_error","equals":"true"}]}`, segmentSchema},
		{"rule on latency column", `{"keepTagRules":[{"tagKey":"latency","exists":true}]}`, segmentSchema},
		{"rule on start_time column", `{"keepTagRules":[{"tagKey":"start_time","exists":true}]}`, segmentSchema},
		{"rule on zipkin duration column", `{"keepTagRules":[{"tagKey":"duration","exists":true}]}`, zipkinSchema},
		// The errorTag override is read as a COLUMN, so a rule on the same key is the same
		// silent no-op. Checking schema.ErrorTag alone would have accepted this.
		{
			"rule on overridden error column",
			`{"keepErrors":true,"errorTag":"custom","keepTagRules":[{"tagKey":"custom","exists":true}]}`,
			segmentSchema,
		},
		// is_error stays a column even when keepErrors is off or points elsewhere.
		{"rule on is_error with keepErrors off", `{"keepTagRules":[{"tagKey":"is_error","exists":true}]}`, segmentSchema},
		{
			"rule on is_error when errorTag overridden",
			`{"keepErrors":true,"errorTag":"custom","keepTagRules":[{"tagKey":"is_error","exists":true}]}`,
			segmentSchema,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := New([]byte(c.config), c.schema)
			require.Error(t, err)
		})
	}
}

func TestNew_Projection(t *testing.T) {
	// Segment: keepErrors projects is_error; both flattened-tag rules collapse
	// onto the single "tags" array column.
	s, err := New([]byte(`{
		"keepErrors": true,
		"keepTagRules": [
			{"tagKey":"db.type","equals":"PostgreSQL"},
			{"tagKey":"mq.queue","equals":"q"}
		]
	}`), segmentSchema)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"is_error", "tags"}, s.Project().Tags)
	assert.False(t, s.Project().SpanIDs)

	// Zipkin declares no first-class columns, so every rule — whether it names the
	// array column itself or a logical key inside it — collapses onto "query".
	z, err := New([]byte(`{
		"keepTagRules": [
			{"tagKey":"query","regex":"http\\.status_code=5\\d\\d"},
			{"tagKey":"http.method","equals":"GET"}
		]
	}`), zipkinSchema)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"query"}, z.Project().Tags)

	// A duration threshold opts into the schema's duration + start-time columns.
	d, err := New([]byte(`{"durationThresholdMs":500}`), segmentSchema)
	require.NoError(t, err)
	assert.Contains(t, d.Project().Tags, "latency")
	assert.Contains(t, d.Project().Tags, "start_time")
}

// TestNew_CompactKeepTagRules covers the compact string grammar, which is what an
// operator realistically types into an environment variable.
func TestNew_CompactKeepTagRules(t *testing.T) {
	s, err := New([]byte(`{"keepTagRules":"db.type=PostgreSQL, http.status_code=~5\\d{2,3}, mq.queue"}`), segmentSchema)
	require.NoError(t, err)
	parsed := s.(*Sampler).rules
	require.Len(t, parsed, 3, "the comma inside {2,3} must NOT split the regex rule")

	assert.Equal(t, "db.type", parsed[0].TagKey)
	assert.Equal(t, "PostgreSQL", parsed[0].Equals)

	assert.Equal(t, "http.status_code", parsed[1].TagKey)
	assert.Equal(t, `5\d{2,3}`, parsed[1].Regex)

	assert.Equal(t, "mq.queue", parsed[2].TagKey)
	assert.True(t, parsed[2].Exists, "a bare key is an exists rule")

	// A value may contain "=" — only the first one separates key from value.
	u, err := New([]byte(`{"keepTagRules":"url=http://x?a=b"}`), segmentSchema)
	require.NoError(t, err)
	assert.Equal(t, "http://x?a=b", u.(*Sampler).rules[0].Equals)

	// The array form still works, and an empty string yields no rules.
	a, err := New([]byte(`{"keepTagRules":[{"tagKey":"db.type","in":["A","B"]}]}`), segmentSchema)
	require.NoError(t, err)
	assert.Equal(t, []string{"A", "B"}, a.(*Sampler).rules[0].In)
	e, err := New([]byte(`{"keepTagRules":""}`), segmentSchema)
	require.NoError(t, err)
	assert.Empty(t, e.(*Sampler).rules)

	// Malformed rules are rejected at admission rather than silently ignored, and the
	// message must name the actual mistake — an empty operand used to surface as the
	// misleading "has no matcher", which describes a rule the operator did not write.
	for _, tc := range []struct {
		name, config, wantMsg string
	}{
		{"empty tagKey", `{"keepTagRules":"=novalue"}`, "empty tagKey"},
		{"empty value", `{"keepTagRules":"db.type="}`, "empty value"},
		{"empty regex", `{"keepTagRules":"db.type=~"}`, "empty regex"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, cErr := New([]byte(tc.config), segmentSchema)
			require.Error(t, cErr)
			assert.Contains(t, cErr.Error(), tc.wantMsg)
			assert.NotContains(t, cErr.Error(), "has no matcher",
				"the generic matcher error hides which operand was empty")
		})
	}

	// The empty-value error points at the exists form, so the suggested fix must work.
	fix, err := New([]byte(`{"keepTagRules":"db.type"}`), segmentSchema)
	require.NoError(t, err)
	assert.True(t, fix.(*Sampler).rules[0].Exists)
}

// TestDecide_CompactRulesMatch proves the compact form produces the same verdicts
// as the array form it desugars to.
func TestDecide_CompactRulesMatch(t *testing.T) {
	s, err := New([]byte(`{"keepTagRules":"db.type=PostgreSQL,http.status_code=~5\\d{2,3}"}`), segmentSchema)
	require.NoError(t, err)

	pg, e := sdktest.NewTrace("pg").Tag("tags", []string{"db.type=PostgreSQL"}).Build()
	require.NoError(t, e)
	fiveXX, e := sdktest.NewTrace("5xx").Tag("tags", []string{"http.status_code=503"}).Build()
	require.NoError(t, e)
	miss, e := sdktest.NewTrace("miss").Tag("tags", []string{"db.type=MySQL", "http.status_code=200"}).Build()
	require.NoError(t, e)

	verdict, report := sdktest.Run(s, sdktest.Batch(pg, fiveXX, miss))
	require.NoError(t, report.Err)
	assert.Empty(t, report.ProjectionDivergedIDs)
	assert.Equal(t, []bool{true, true, false}, verdict.Keep)
}

// TestDecide_EscapedEntriesSurviveMultipleRules is a regression test for in-place
// decoding. vararray.UnmarshalVarArray rewrites its source slice, shifting bytes left
// past every escape, so a column decoded a SECOND time yields corrupted entries. The
// engine used to decode the flattened array once per rule, which silently broke every
// rule after the first whenever a tag value contained "|" or "\" — a retention filter
// dropping data it was told to keep. Values here need escaping and the rule that must
// match is deliberately LAST.
func TestDecide_EscapedEntriesSurviveMultipleRules(t *testing.T) {
	s, err := New([]byte(`{
		"keepTagRules": [
			{"tagKey":"never.matches","equals":"nothing"},
			{"tagKey":"db.type","equals":"MySQL"},
			{"tagKey":"db.statement","equals":"SELECT a|b"}
		]
	}`), segmentSchema)
	require.NoError(t, err)

	// "SELECT a|b" contains the entity delimiter and "C:\\tmp" the escape character,
	// so both are stored escaped and are rewritten on decode.
	hit, e := sdktest.NewTrace("escaped").
		Tag("tags", []string{`db.type=PostgreSQL`, `url=C:\tmp`, `db.statement=SELECT a|b`}).Build()
	require.NoError(t, e)
	miss, e := sdktest.NewTrace("miss").
		Tag("tags", []string{`db.type=PostgreSQL`, `db.statement=SELECT c|d`}).Build()
	require.NoError(t, e)

	verdict, report := sdktest.Run(s, sdktest.Batch(hit, miss))
	require.NoError(t, report.Err)
	assert.Empty(t, report.ProjectionDivergedIDs)
	assert.Equal(t, []bool{true, false}, verdict.Keep,
		"the third rule must still see intact entries after the first two were evaluated")
}

// TestDecide_ExistsMatchesBareKey pins the exists semantics after unifying it with
// keepErrors: the key counts whether the array holds it as a bare entry or as
// "key=value". The comparison is exact, so a longer key must NOT satisfy it —
// that guard is what stops "error_rate=0" from looking like an error.
func TestDecide_ExistsMatchesBareKey(t *testing.T) {
	s, err := New([]byte(`{"keepTagRules":[{"tagKey":"error","exists":true}]}`), zipkinSchema)
	require.NoError(t, err)

	bare, e := sdktest.NewTrace("bare").Tag("query", []string{"error", "http.method=GET"}).Build()
	require.NoError(t, e)
	withValue, e := sdktest.NewTrace("with-value").Tag("query", []string{"error=boom"}).Build()
	require.NoError(t, e)
	emptyValue, e := sdktest.NewTrace("empty-value").Tag("query", []string{"error="}).Build()
	require.NoError(t, e)
	lookalike, e := sdktest.NewTrace("lookalike").
		Tag("query", []string{"error_rate=0", "errors=3", "http.method=GET"}).Build()
	require.NoError(t, e)

	verdict, report := sdktest.Run(s, sdktest.Batch(bare, withValue, emptyValue, lookalike))
	require.NoError(t, report.Err)
	assert.Empty(t, report.ProjectionDivergedIDs)
	assert.Equal(t, []bool{true, true, true, false}, verdict.Keep,
		"exists must accept the bare key and key=value, but never a longer key")
}

// TestDecide_KeepErrorsEqualsExistsRule is the point of the unification: on an
// in-array schema, keepErrors and an explicit exists rule on the same key must
// produce identical verdicts, since the docs present them as equivalent.
func TestDecide_KeepErrorsEqualsExistsRule(t *testing.T) {
	viaFlag, err := New([]byte(`{"keepErrors":true,"healthySampleRate":0}`), zipkinSchema)
	require.NoError(t, err)
	viaRule, err := New([]byte(`{"keepTagRules":[{"tagKey":"error","exists":true}],"healthySampleRate":0}`), zipkinSchema)
	require.NoError(t, err)

	for _, tc := range []struct {
		name  string
		query []string
	}{
		{"bare", []string{"error"}},
		{"with-value", []string{"error=boom"}},
		{"empty-value", []string{"error="}},
		{"both-forms", []string{"error", "error=boom"}},
		{"lookalike", []string{"error_rate=0"}},
		{"healthy", []string{"http.status_code=200"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			block, e := sdktest.NewTrace(tc.name).Tag("query", tc.query).Build()
			require.NoError(t, e)
			flagVerdict, _ := sdktest.Run(viaFlag, sdktest.Batch(block))
			ruleVerdict, _ := sdktest.Run(viaRule, sdktest.Batch(block))
			assert.Equal(t, flagVerdict.Keep, ruleVerdict.Keep,
				"keepErrors and the equivalent exists rule must agree")
		})
	}
}

func TestDecide_FlattenedTagMatchers(t *testing.T) {
	s, err := New([]byte(`{
		"keepTagRules": [
			{"tagKey":"db.type","equals":"PostgreSQL"},
			{"tagKey":"http.method","in":["GET","POST"]},
			{"tagKey":"mq.queue","exists":true}
		]
	}`), segmentSchema)
	require.NoError(t, err)

	equalsHit, e := sdktest.NewTrace("equals").Tag("tags", []string{"db.type=PostgreSQL"}).Build()
	require.NoError(t, e)
	inHit, e := sdktest.NewTrace("in").Tag("tags", []string{"http.method=POST"}).Build()
	require.NoError(t, e)
	existsHit, e := sdktest.NewTrace("exists").Tag("tags", []string{"mq.queue=whatever"}).Build()
	require.NoError(t, e)
	miss, e := sdktest.NewTrace("miss").Tag("tags", []string{"db.type=MySQL", "http.method=DELETE"}).Build()
	require.NoError(t, e)

	verdict, report := sdktest.Run(s, sdktest.Batch(equalsHit, inHit, existsHit, miss))
	require.NoError(t, report.Err)
	assert.Empty(t, report.ProjectionDivergedIDs)
	assert.Equal(t, []bool{true, true, true, false}, verdict.Keep)
}

// TestDecide_RawArrayVsPrefixedMatch covers the two ways a rule resolves against
// the flattened array: a tagKey naming the array itself ("query") matches whole
// raw entries, while any other tagKey matches only the value part of its
// "tagKey=" entries — so a rule on "http.status_code" must not be satisfied by
// some other key that happens to contain the same text.
func TestDecide_RawArrayVsPrefixedMatch(t *testing.T) {
	z, err := New([]byte(`{
		"keepTagRules": [
			{"tagKey":"query","regex":"http\\.status_code=5\\d\\d"},
			{"tagKey":"http.method","equals":"POST"}
		]
	}`), zipkinSchema)
	require.NoError(t, err)

	// Raw-entry regex hit on the array column's own name.
	fiveXX, e := sdktest.NewTrace("5xx").
		Tag("query", []string{"http.status_code", "http.status_code=503"}).Build()
	require.NoError(t, e)
	// Prefixed hit: the "http.method=" entry's value equals POST.
	post, e := sdktest.NewTrace("post").
		Tag("query", []string{"http.method", "http.method=POST"}).Build()
	require.NoError(t, e)
	// Near miss: another key's VALUE is "POST", but the http.method rule is prefix
	// scoped, so it must not match.
	otherKey, e := sdktest.NewTrace("other-key").
		Tag("query", []string{"rpc.verb=POST", "http.status_code=200"}).Build()
	require.NoError(t, e)

	verdict, report := sdktest.Run(z, sdktest.Batch(fiveXX, post, otherKey))
	require.NoError(t, report.Err)
	assert.Empty(t, report.ProjectionDivergedIDs)
	assert.Equal(t, []bool{true, true, false}, verdict.Keep)
}

// TestDecide_Duration exercises the end-to-end trace envelope: the decision is
// max(start_time + latency) - min(start_time) over the trace's segments. Segment
// start_time is a timestamp tag in unix ns; latency is ms (units-per-ns = 1e6).
// The sequential case is the one the old per-segment "max latency" rule missed.
func TestDecide_Duration(t *testing.T) {
	s, err := New([]byte(`{"durationThresholdMs":500}`), segmentSchema)
	require.NoError(t, err)

	const ms = int64(1_000_000) // ns per ms, for start_time timestamps

	// Single segment, latency 600ms ≥ 500ms → kept.
	single, e := sdktest.NewTrace("slow-single").
		TagAs("start_time", valuetype.ValueTypeTimestamp, int64(0)).
		Tag("latency", int64(600)).Build()
	require.NoError(t, e)

	// Two sequential segments, each 300ms, staggered by 300ms: envelope
	// = (300ms + 300ms) - 0 = 600ms ≥ 500ms → kept. Max single latency is 300ms,
	// so the old per-segment rule would have WRONGLY dropped this slow trace.
	sequential, e := sdktest.NewTrace("slow-sequential").
		TagAs("start_time", valuetype.ValueTypeTimestamp, int64(0)).
		TagAs("start_time", valuetype.ValueTypeTimestamp, 300*ms).
		Tag("latency", int64(300)).
		Tag("latency", int64(300)).Build()
	require.NoError(t, e)

	// Two segments, envelope = (100ms + 100ms) - 0 = 200ms < 500ms → dropped.
	fast, e := sdktest.NewTrace("fast").
		TagAs("start_time", valuetype.ValueTypeTimestamp, int64(0)).
		TagAs("start_time", valuetype.ValueTypeTimestamp, 100*ms).
		Tag("latency", int64(100)).
		Tag("latency", int64(100)).Build()
	require.NoError(t, e)

	verdict, report := sdktest.Run(s, sdktest.Batch(single, sequential, fast))
	require.NoError(t, report.Err)
	assert.Empty(t, report.ProjectionDivergedIDs)
	assert.Equal(t, []bool{true, true, false}, verdict.Keep)
}

// TestDecide_DurationZipkin: the Zipkin duration tag is microseconds
// (nanos-per-unit = 1000) and timestamp_millis is a ns timestamp; the envelope
// is computed in ns and compared to the ms threshold.
func TestDecide_DurationZipkin(t *testing.T) {
	z, err := New([]byte(`{"durationThresholdMs":1000}`), zipkinSchema)
	require.NoError(t, err)

	slow, e := sdktest.NewTrace("slow").
		TagAs("timestamp_millis", valuetype.ValueTypeTimestamp, int64(0)).
		Tag("duration", int64(1_500_000)).Build() // 1.5s (µs) ≥ 1s → kept
	require.NoError(t, e)
	fast, e := sdktest.NewTrace("fast").
		TagAs("timestamp_millis", valuetype.ValueTypeTimestamp, int64(0)).
		Tag("duration", int64(2_000)).Build() // 2ms → dropped
	require.NoError(t, e)

	verdict, report := sdktest.Run(z, sdktest.Batch(slow, fast))
	require.NoError(t, report.Err)
	assert.Empty(t, report.ProjectionDivergedIDs)
	assert.Equal(t, []bool{true, false}, verdict.Keep)
}

// The Zipkin "error" signal is an array entry rather than a column, so a rule on it
// stays legal — the first-class-column rejection must not over-reach.
func TestNew_ZipkinErrorTagRuleAllowed(t *testing.T) {
	_, err := New([]byte(`{"keepTagRules":[{"tagKey":"error","exists":true}]}`), zipkinSchema)
	require.NoError(t, err)
}

// A rule's verdict must not depend on the link's POSITION in a chain. The engine hands
// the same *TraceBatch to every link, and the SDK's string-array decode rewrites its
// source in place, so a link ahead that also reads the array column used to corrupt the
// bytes this one decodes. Only values containing "|" or "\" carry an escape, so the
// corruption is data-dependent — TestDecide_EscapedEntriesSurviveMultipleRules covers the
// same hazard between rules inside one sampler; this covers it between links.
func TestDecide_EscapedEntriesSurviveChainedLinks(t *testing.T) {
	// Both links keep, so a false verdict can only come from corruption, not from the
	// chain ANDing in an unrelated drop.
	target := func() sdk.Sampler {
		s, err := New([]byte(`{"keepTagRules":[{"tagKey":"db.statement","equals":"SELECT a|b"}]}`), segmentSchema)
		require.NoError(t, err)
		return s
	}
	ahead := func() sdk.Sampler {
		s, err := New([]byte(`{"keepTagRules":[{"tagKey":"http.method","equals":"GET"}]}`), segmentSchema)
		require.NoError(t, err)
		return s
	}
	block := func() sdk.TraceBlock {
		b, e := sdktest.NewTrace("escaped").
			Tag("tags", []string{`db.statement=SELECT a|b`, "http.method=GET"}).Build()
		require.NoError(t, e)
		return b
	}

	alone, report := sdktest.Run(target(), sdktest.Batch(block()))
	require.NoError(t, report.Err)
	require.Equal(t, []bool{true}, alone.Keep, "precondition: the rule matches when it runs alone")

	behind, chainReport := sdktest.RunChain([]sdk.Sampler{ahead(), target()}, sdktest.Batch(block()))
	assert.Empty(t, chainReport.Bypassed)
	assert.Equal(t, []bool{true}, behind.Keep,
		"a link ahead that decodes the same array column must not corrupt it for this one")

	first, chainReport := sdktest.RunChain([]sdk.Sampler{target(), ahead()}, sdktest.Batch(block()))
	assert.Empty(t, chainReport.Bypassed)
	assert.Equal(t, []bool{true}, first.Keep, "and the verdict must not depend on chain position")
}

// An empty config sets no keep rule at all, so it would drop the whole group. It is
// reachable by omission: pipeline_loader substitutes {} for an unset SamplerPlugin.config.
func TestNew_RejectsEmptyConfig(t *testing.T) {
	for _, cfg := range []string{`{}`, `   {}  `, ``} {
		_, err := New([]byte(cfg), segmentSchema)
		require.Error(t, err, "empty config %q must be rejected", cfg)
	}
	// A config with a key stays valid even when it sets no keep criteria — rate 0 with
	// no rules is a supported setting, not a mistake.
	_, err := New([]byte(`{"healthySampleRate":0}`), segmentSchema)
	require.NoError(t, err)
}

// Every option this plugin has is a KEEP rule, so a key that silently misses yields a
// sampler with no rules that drops the whole group. Unknown keys must therefore be a
// decode error — most importantly for a snake_case config copied from the _example
// plugin, which shares none of these spellings.
func TestNew_RejectsUnknownConfigKeys(t *testing.T) {
	for _, cfg := range []string{
		`{"duration_threshold_ms":500,"keep_errors":true}`,
		`{"keepErrorz":true}`,
		`{"durationThresholdMs":500,"typo":1}`,
		`{"keepTagRules":[{"tagKey":"db.type","equalz":"PostgreSQL"}]}`,
	} {
		_, err := New([]byte(cfg), segmentSchema)
		require.Error(t, err, "config %s must be rejected, not silently ignored", cfg)
	}

	// Go matches field names case-insensitively, so this is accepted rather than
	// rejected — the guard catches wrong words, not wrong capitalization.
	_, err := New([]byte(`{"durationthresholdms":500}`), segmentSchema)
	require.NoError(t, err)
}

// A Zipkin errorTag override still names an ARRAY entry, so a rule on it stays legal —
// only an override read as a column is rejected.
func TestNew_ZipkinErrorTagOverrideRuleAllowed(t *testing.T) {
	_, err := New([]byte(`{"keepErrors":true,"errorTag":"otel.status_code",`+
		`"keepTagRules":[{"tagKey":"otel.status_code","exists":true}]}`), zipkinSchema)
	require.NoError(t, err)
}

// The escape hatch the rejection message points at has to actually work: a searchable tag
// whose key collides with a column name stays reachable via a rule on the array column.
func TestDecide_ArrayColumnEscapeHatch(t *testing.T) {
	z, err := New([]byte(`{"keepTagRules":[{"tagKey":"query","regex":"^duration=slow$"}]}`), zipkinSchema)
	require.NoError(t, err)

	hit, e := sdktest.NewTrace("collide").
		Tag("query", []string{"http.method=GET", "duration=slow"}).Build()
	require.NoError(t, e)
	miss, e := sdktest.NewTrace("no-collide").
		Tag("query", []string{"http.method=GET"}).Build()
	require.NoError(t, e)

	verdict, report := sdktest.Run(z, sdktest.Batch(hit, miss))
	require.NoError(t, report.Err)
	assert.Equal(t, []bool{true, false}, verdict.Keep)
}

// A plain Int64 start time carries no unit, so it is skipped rather than assumed to be
// nanoseconds. The trace then has no computable envelope and fails open — a 4ms latency
// under a 500ms threshold would otherwise have been dropped.
func TestDecide_Int64StartTimeFailsOpen(t *testing.T) {
	s, err := New([]byte(`{"durationThresholdMs":500}`), segmentSchema)
	require.NoError(t, err)

	tr, e := sdktest.NewTrace("int64-start").
		Tag("start_time", int64(0)).
		Tag("latency", int64(4)).Build()
	require.NoError(t, e)

	verdict, report := sdktest.Run(s, sdktest.Batch(tr))
	require.NoError(t, report.Err)
	assert.Equal(t, []bool{true}, verdict.Keep)
}

// The never-match guard has to cover the schema's WHOLE column inventory, not just the
// three the engine reads. "Keep everything from the payment service", written as a rule
// on service_id, is the motivating case: it admits cleanly and then drops exactly the
// traces it was meant to save.
func TestNew_RejectsRulesOnAnyFirstClassColumn(t *testing.T) {
	for _, c := range []struct {
		name   string
		cols   []string
		schema Schema
	}{
		{name: "segment", schema: segmentSchema, cols: segmentColumns},
		{name: "zipkin", schema: zipkinSchema, cols: zipkinColumns},
	} {
		for _, col := range c.cols {
			t.Run(c.name+"/"+col, func(t *testing.T) {
				cfg := `{"keepTagRules":[{"tagKey":"` + col + `","exists":true}]}`
				_, err := New([]byte(cfg), c.schema)
				require.Error(t, err, "a rule on the %s column %q can never match", c.name, col)
				assert.Contains(t, err.Error(), "could never match")
			})
		}
	}

	// The array column itself stays legal — it is the escape hatch the error names.
	for _, c := range []struct {
		key    string
		schema Schema
	}{
		{"tags", segmentSchema},
		{"query", zipkinSchema},
	} {
		cfg := `{"keepTagRules":[{"tagKey":"` + c.key + `","regex":"^db\\."}]}`
		_, err := New([]byte(cfg), c.schema)
		require.NoError(t, err, "a rule on the array column %q must stay legal", c.key)
	}
}

// keepErrors must fail the same way as the duration rule: an absent error column is a
// can't-tell, not "no error". Failing closed here would make {"keepErrors": true} on a
// mis-paired group drop every trace it was enabled to save.
func TestDecide_ErrorColumnMissingFailsOpen(t *testing.T) {
	s, err := New([]byte(`{"keepErrors":true}`), segmentSchema)
	require.NoError(t, err)

	// Zipkin-shaped rows under the segment plugin: no is_error column anywhere.
	wrongSchema, e := sdktest.NewTrace("wrong-schema").
		Tag("query", []string{"http.method=GET"}).Build()
	require.NoError(t, e)

	verdict, report := sdktest.Run(s, sdktest.Batch(wrongSchema))
	require.NoError(t, report.Err)
	assert.Equal(t, []bool{true}, verdict.Keep)

	// A column that is PRESENT but not truthy stays fail-closed — an unset flag really
	// does mean "not an error", unlike a missing measurement.
	healthy, e := sdktest.NewTrace("healthy").Tag("is_error", int64(0)).Build()
	require.NoError(t, e)
	verdict, report = sdktest.Run(s, sdktest.Batch(healthy))
	require.NoError(t, report.Err)
	assert.Equal(t, []bool{false}, verdict.Keep)
}

// A span carrying a start but no duration must still anchor the envelope's left edge.
// Zipkin's duration is optional, so dropping such a row would measure the trace from a
// later span and understate how long it took.
func TestDecide_StartWithoutDurationStillBoundsEnvelope(t *testing.T) {
	const ms = int64(1_000_000)
	z, err := New([]byte(`{"durationThresholdMs":1000}`), zipkinSchema)
	require.NoError(t, err)

	// Earliest span has no duration; the later one is only 10ms long. Measured from the
	// earliest start the envelope is 1.51s and the trace is slow; measured from the
	// later span alone it is 10ms and would be dropped.
	trace, e := sdktest.NewTrace("one-way-first").
		TagAs("timestamp_millis", valuetype.ValueTypeTimestamp, int64(0)).
		TagAs("timestamp_millis", valuetype.ValueTypeTimestamp, 1500*ms).
		TagAs("duration", valuetype.ValueTypeInt64, nil).
		Tag("duration", int64(10_000)).Build()
	require.NoError(t, e)

	verdict, report := sdktest.Run(z, sdktest.Batch(trace))
	require.NoError(t, report.Err)
	assert.Equal(t, []bool{true}, verdict.Keep,
		"the earliest start must anchor minStart even though that span has no duration")

	// With NO duration on any row the trace's end is unknown — a can't-tell, so kept.
	noDuration, e := sdktest.NewTrace("all-one-way").
		TagAs("timestamp_millis", valuetype.ValueTypeTimestamp, int64(0)).
		TagAs("duration", valuetype.ValueTypeInt64, nil).Build()
	require.NoError(t, e)
	verdict, report = sdktest.Run(z, sdktest.Batch(noDuration))
	require.NoError(t, report.Err)
	assert.Equal(t, []bool{true}, verdict.Keep)
}

// A trace whose duration columns are absent cannot be evaluated at all, so the
// threshold rule fails open. Answering "not slow" instead would make a plugin
// pointed at the wrong group's schema silently drop every trace it was configured
// to keep.
func TestDecide_DurationMissingColumnsFailOpen(t *testing.T) {
	s, err := New([]byte(`{"durationThresholdMs":500}`), segmentSchema)
	require.NoError(t, err)

	// Zipkin-shaped rows under the segment plugin: no start_time, no latency.
	wrongSchema, e := sdktest.NewTrace("wrong-schema").
		TagAs("timestamp_millis", valuetype.ValueTypeTimestamp, int64(0)).
		Tag("duration", int64(1)).Build()
	require.NoError(t, e)

	verdict, report := sdktest.Run(s, sdktest.Batch(wrongSchema))
	require.NoError(t, report.Err)
	assert.Equal(t, []bool{true}, verdict.Keep)
}

func TestDecide_KeepErrors(t *testing.T) {
	s, err := New([]byte(`{"keepErrors":true}`), segmentSchema)
	require.NoError(t, err)

	intErr, e := sdktest.NewTrace("int-error").Tag("is_error", int64(1)).Build()
	require.NoError(t, e)
	strErr, e := sdktest.NewTrace("str-error").Tag("is_error", "true").Build()
	require.NoError(t, e)
	ok, e := sdktest.NewTrace("ok").Tag("is_error", int64(0)).Build()
	require.NoError(t, e)

	verdict, report := sdktest.Run(s, sdktest.Batch(intErr, strErr, ok))
	require.NoError(t, report.Err)
	assert.Empty(t, report.ProjectionDivergedIDs)
	assert.Equal(t, []bool{true, true, false}, verdict.Keep)
}

// TestNew_HealthySampleRateAcceptsString covers the ${ENV:default} path: OAP's
// config loader stringifies float placeholders, so the rate can arrive quoted.
func TestNew_HealthySampleRateAcceptsString(t *testing.T) {
	quoted, err := New([]byte(`{"healthySampleRate":"0"}`), segmentSchema)
	require.NoError(t, err)
	block, e := sdktest.NewTrace("x").Build()
	require.NoError(t, e)
	verdict, _ := sdktest.Run(quoted, sdktest.Batch(block))
	assert.Equal(t, []bool{false}, verdict.Keep, `"0" must disable healthy sampling, not fail the config`)

	// A quoted out-of-range rate is still range-checked; a non-numeric string is rejected.
	_, err = New([]byte(`{"healthySampleRate":"1.5"}`), segmentSchema)
	require.Error(t, err)
	_, err = New([]byte(`{"healthySampleRate":"abc"}`), segmentSchema)
	require.Error(t, err)

	// A blank value in bydb.yml reaches the plugin as JSON null; treat it as "not set"
	// (zero value) rather than failing the whole config.
	nulled, err := New([]byte(`{"healthySampleRate":null}`), segmentSchema)
	require.NoError(t, err)
	verdict, _ = sdktest.Run(nulled, sdktest.Batch(block))
	assert.Equal(t, []bool{false}, verdict.Keep, "null must behave as an unset rate, not fail the config")
}

func TestDecide_HealthySampleRate(t *testing.T) {
	// Rate 1.0 keeps every trace (sampleFraction is strictly < 1).
	all, err := New([]byte(`{"healthySampleRate":1.0}`), segmentSchema)
	require.NoError(t, err)
	for _, id := range []string{"a", "b", "c", "trace-42", ""} {
		block, e := sdktest.NewTrace(id).Build()
		require.NoError(t, e)
		verdict, _ := sdktest.Run(all, sdktest.Batch(block))
		assert.Equal(t, []bool{true}, verdict.Keep, "rate 1.0 must keep %q", id)
	}

	// Rate 0 disables healthy sampling: a trace matching no sure-keep rule drops.
	none, err := New([]byte(`{"healthySampleRate":0}`), segmentSchema)
	require.NoError(t, err)
	block, e := sdktest.NewTrace("x").Build()
	require.NoError(t, e)
	verdict, _ := sdktest.Run(none, sdktest.Batch(block))
	assert.Equal(t, []bool{false}, verdict.Keep)
}

// TestArrayEntries_LeavesBlockBytesIntact pins the precondition that lets
// arrayEntries alias the block's own bytes instead of copying every row: the
// decode must leave col.Values byte-identical.
//
// An escape-free row is decoded in place and the entries alias it, which is only
// sound because UnmarshalVarArray does not write when there is no escape to
// remove. An escaped row is still copied first, because that decode DOES write.
// If either half regresses, the engine hands the SAME batch to every link of a
// chain, so a later rule would read rewritten bytes and its verdict would depend
// on its position in the chain — silent and data-dependent.
//
// The mixed column is deliberate: the choice is per row, not per column.
func TestArrayEntries_LeavesBlockBytesIntact(t *testing.T) {
	rows := [][]string{
		{"db.type=PostgreSQL", "http.method=GET"}, // escape-free
		{`db.statement=SELECT a|b`, `url=C:\tmp`}, // both bytes needing an escape
		{"plain"}, // escape-free, single entry
	}
	col := &sdk.TagColumn{Name: "tags", ValueType: valuetype.ValueTypeStrArr}
	for _, entries := range rows {
		var encoded []byte
		for _, e := range entries {
			encoded = vararray.MarshalVarArray(encoded, []byte(e))
		}
		col.Values = append(col.Values, encoded)
	}
	before := make([][]byte, len(col.Values))
	for i, v := range col.Values {
		before[i] = append([]byte(nil), v...)
	}

	entriesPtr, stablePtr, err := arrayEntries(col)
	require.NoError(t, err)
	defer releaseEntries(entriesPtr)
	defer releaseStableBuf(stablePtr)

	assert.Equal(t, []string{
		"db.type=PostgreSQL", "http.method=GET",
		`db.statement=SELECT a|b`, `url=C:\tmp`,
		"plain",
	}, *entriesPtr, "every row's entries must decode, escaped or not")

	for i := range col.Values {
		assert.Equal(t, before[i], col.Values[i],
			"row %d was rewritten; a later chain link would read corrupted bytes", i)
	}
}

// TestArrayEntries_SplitPathMatchesCodec pins the one place the sampler assumes
// the var-array encoding. The escape-free branch splits on the delimiter itself
// instead of calling UnmarshalVarArray, to skip that function's redundant
// per-entry escape scan. It must therefore agree with the codec exactly — same
// entries, same error — on every row shape, or a rule's verdict would depend on
// which branch decoded the row.
func TestArrayEntries_SplitPathMatchesCodec(t *testing.T) {
	rows := []struct {
		name    string
		encoded []byte
	}{
		{"empty", nil},
		{"single entry", []byte("abc|")},
		{"two entries", []byte("a|b|")},
		{"empty entry first", []byte("|a|")},
		{"empty entry last", []byte("a||")},
		{"all empty entries", []byte("|||")},
		{"unterminated", []byte("abc")},
		{"unterminated after a good entry", []byte("a|bc")},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			// Reference: decode via the codec, which is what the escaped branch does.
			var (
				want    []string
				wantErr error
			)
			ref := append([]byte(nil), row.encoded...)
			for idx := 0; idx < len(ref); {
				end, next, err := vararray.UnmarshalVarArray(ref, idx)
				if err != nil {
					wantErr = err
					break
				}
				want = append(want, string(ref[idx:end]))
				idx = next
			}

			col := &sdk.TagColumn{
				Name: "tags", ValueType: valuetype.ValueTypeStrArr,
				Values: [][]byte{append([]byte(nil), row.encoded...)},
			}
			got, stablePtr, err := arrayEntries(col)

			if wantErr != nil {
				require.Error(t, err, "codec rejected this row, so the split path must too")
				assert.Contains(t, err.Error(), wantErr.Error(), "both paths must report the same condition")
				return
			}
			require.NoError(t, err)
			defer releaseEntries(got)
			defer releaseStableBuf(stablePtr)
			// Compared by content: a row yielding nothing gives the codec a nil
			// slice and arrayEntries a zero-length pooled one, which is the same
			// value to every caller (matchEntries iterates the length).
			if len(want) == 0 {
				assert.Empty(t, *got)
				return
			}
			assert.Equal(t, want, *got)
		})
	}
}

// TestArrayEntries_ErrorAfterGrownStableBuffer drives the error path in the state
// that makes buffer bookkeeping easy to get wrong: an escaped row has already
// appended to (and possibly reallocated) the stable buffer when a later row turns
// out to be malformed, so the local slice header has diverged from the pooled one
// and must be written back before the buffer is released.
//
// The assertions here only pin the observable contract — the error surfaces and
// nothing panics or double-releases. That the grown header is persisted is
// structural: the persist and the release both live in one deferred block, so an
// error site cannot skip it.
func TestArrayEntries_ErrorAfterGrownStableBuffer(t *testing.T) {
	escaped := vararray.MarshalVarArray(nil, []byte(`db.statement=SELECT a|b`))
	col := &sdk.TagColumn{
		Name: "tags", ValueType: valuetype.ValueTypeStrArr,
		Values: [][]byte{
			escaped,                // grows the stable buffer
			[]byte("unterminated"), // no trailing delimiter -> error
		},
	}
	got, stablePtr, err := arrayEntries(col)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid variable array")
	assert.Nil(t, got, "the entries slice is released on the error path")
	assert.Nil(t, stablePtr, "the stable buffer is released here, not handed to the caller")

	// The same shape must also fail open through the public path rather than drop.
	s, newErr := New([]byte(`{"keepTagRules":[{"tagKey":"db.statement","exists":true}]}`), segmentSchema)
	require.NoError(t, newErr)
	batch := &sdk.TraceBatch{Traces: []sdk.TraceBlock{{
		TraceID: "malformed-after-escaped",
		Tags:    []sdk.TagColumn{*col},
	}}}
	verdict, decideErr := s.Decide(batch)
	require.NoError(t, decideErr)
	assert.Equal(t, []bool{true}, verdict.Keep, "a decode error must fail open (keep), never drop")
}

// TestDecide_FailOpenOnDecodeError proves a malformed tag value keeps the trace
// (fail open) rather than dropping it or erroring the whole batch: an is_error
// column whose raw int64 is not 8 bytes fails to decode, and keepErrors keeps
// the trace anyway.
func TestDecide_FailOpenOnDecodeError(t *testing.T) {
	s, err := New([]byte(`{"keepErrors":true}`), segmentSchema)
	require.NoError(t, err)

	batch := &sdk.TraceBatch{Traces: []sdk.TraceBlock{{
		TraceID: "malformed",
		Tags: []sdk.TagColumn{{
			Name:      "is_error",
			ValueType: valuetype.ValueTypeInt64,
			Values:    [][]byte{{0x01, 0x02, 0x03}}, // not 8 bytes → decode error
		}},
	}}}
	verdict, err := s.Decide(batch)
	require.NoError(t, err)
	assert.Equal(t, []bool{true}, verdict.Keep, "a decode error must fail open (keep), never drop")
}

// TestDecide_MissingTagArrayColumn is a regression test for the nil-pool-element
// dereference. arrayEntries must return a non-nil *[]string (an empty slice) when
// the array column is missing from the block, so keepTrace can dereference it
// without crashing. The pre-pool version of arrayEntries returned a nil slice and
// matchEntries(nil, ...) was safe by accident; the pool-typed version is not.
func TestDecide_MissingTagArrayColumn(t *testing.T) {
	s, err := New([]byte(`{"keepErrors":true,"keepTagRules":[{"tagKey":"db.type","equals":"PostgreSQL"}]}`), segmentSchema)
	require.NoError(t, err)

	// Block with NO tags column at all — the array-column read returns nil.
	missing, e := sdktest.NewTrace("missing").
		TagAs("is_error", valuetype.ValueTypeInt64, int64(0)).Build()
	require.NoError(t, e)

	// Block with an EMPTY tags column — the column exists but every row's value is nil.
	empty, e := sdktest.NewTrace("empty").
		TagAs("is_error", valuetype.ValueTypeInt64, int64(0)).
		Tag("tags", []string(nil)).Build()
	require.NoError(t, e)

	verdict, report := sdktest.Run(s, sdktest.Batch(missing, empty))
	require.NoError(t, report.Err)
	assert.Equal(t, []bool{false, false}, verdict.Keep,
		"missing/empty tag arrays must not crash and must drop (no rule matches)")
}

// TestScalarInt64_MatchesDecodeTagValue is the oracle for the fast scalar reader.
// scalarInt64 exists to avoid materializing sdk.Value, so it must agree with the
// SDK decode it replaces on every cell shape — value, null-ness, AND error text,
// since a malformed cell fails open and the message is what gets logged.
func TestScalarInt64_MatchesDecodeTagValue(t *testing.T) {
	cells := [][]byte{
		nil,
		{},
		{0x01},
		make([]byte, 7),
		convert.Int64ToBytes(0),
		convert.Int64ToBytes(1),
		convert.Int64ToBytes(-1),
		convert.Int64ToBytes(1 << 62),
		make([]byte, 9),
	}
	for _, vt := range []struct {
		typeName string
		valueTyp valuetype.ValueType
	}{
		{"int64", valuetype.ValueTypeInt64},
		{"timestamp", valuetype.ValueTypeTimestamp},
	} {
		for i, cell := range cells {
			t.Run(fmt.Sprintf("%s/cell=%d", vt.typeName, i), func(t *testing.T) {
				col := &sdk.TagColumn{Name: "c", ValueType: vt.valueTyp, Values: [][]byte{cell}}

				wantVal, wantErr := col.At(0)
				gotVal, gotOK, gotErr := scalarInt64(col, 0, vt.typeName)

				if wantErr != nil {
					require.Error(t, gotErr, "SDK rejected this cell, so the fast reader must too")
					assert.Equal(t, wantErr.Error(), gotErr.Error(), "error text must match: it is logged verbatim")
					return
				}
				require.NoError(t, gotErr)
				assert.Equal(t, !wantVal.IsNull(), gotOK, "null-ness must match")
				if !wantVal.IsNull() {
					assert.Equal(t, wantVal.Int64(), gotVal)
				}
			})
		}
	}
}

// TestHasSlowTrace_TypeMismatchKeepsSlowPath pins the narrow gating. The per-row
// type checks look hoistable — ValueType is a column property — but a wrong-typed
// column can still hold a cell that DecodeTagValue rejects on length, and that
// error must keep propagating rather than being skipped as "wrong type".
func TestHasSlowTrace_TypeMismatchKeepsSlowPath(t *testing.T) {
	s, err := New([]byte(`{"durationThresholdMs":500}`), segmentSchema)
	require.NoError(t, err)
	sampler := s.(*Sampler)

	block := func(startType, durType valuetype.ValueType, startCell, durCell []byte) *sdk.TraceBlock {
		return &sdk.TraceBlock{
			TraceID: "t",
			Tags: []sdk.TagColumn{
				{Name: "start_time", ValueType: startType, Values: [][]byte{startCell}},
				{Name: "latency", ValueType: durType, Values: [][]byte{durCell}},
			},
		}
	}
	good := convert.Int64ToBytes(1)

	t.Run("malformed cell in a float-typed start column still errors", func(t *testing.T) {
		_, slowErr := sampler.hasSlowTrace(block(valuetype.ValueTypeFloat64, valuetype.ValueTypeInt64, []byte{0x01}, good))
		require.Error(t, slowErr, "a length-checked wrong type must not be silently skipped")
		assert.Contains(t, slowErr.Error(), "float64: expected 8 bytes")
	})

	t.Run("well-formed float start column is a can't-tell, not slow", func(t *testing.T) {
		_, slowErr := sampler.hasSlowTrace(block(valuetype.ValueTypeFloat64, valuetype.ValueTypeInt64, make([]byte, 8), good))
		assert.ErrorIs(t, slowErr, errNoDurationEnvelope, "a non-timestamp start carries no unit, so it cannot bound the envelope")
	})

	t.Run("str-typed duration column bounds only the left edge", func(t *testing.T) {
		_, slowErr := sampler.hasSlowTrace(block(valuetype.ValueTypeTimestamp, valuetype.ValueTypeStr, good, []byte("nope")))
		assert.ErrorIs(t, slowErr, errNoDurationEnvelope, "a start with no usable duration leaves the right edge unknown")
	})
}

// TestHasErrorColumn_TypeCoverage covers all three branches the error-column
// reader now has: the two fast paths for the types that actually carry an error
// signal, and the At-based fallback for every other type.
//
// The fallback matters precisely because it is the one that can still surface a
// decode error: Float64 and Timestamp are 8-byte-checked by DecodeTagValue, so a
// malformed cell in such a column must keep failing open rather than being
// skipped as "no error signal this schema understands".
func TestHasErrorColumn_TypeCoverage(t *testing.T) {
	s, err := New([]byte(`{"keepErrors":true,"healthySampleRate":0}`), segmentSchema)
	require.NoError(t, err)
	sampler := s.(*Sampler)

	block := func(vt valuetype.ValueType, cells ...[]byte) *sdk.TraceBlock {
		return &sdk.TraceBlock{
			TraceID: "t",
			Tags:    []sdk.TagColumn{{Name: "is_error", ValueType: vt, Values: cells}},
		}
	}

	t.Run("int64 fast path", func(t *testing.T) {
		hit, hitErr := sampler.hasErrorColumn(block(valuetype.ValueTypeInt64, convert.Int64ToBytes(0), convert.Int64ToBytes(1)))
		require.NoError(t, hitErr)
		assert.True(t, hit, "a non-zero int64 is an error")

		none, noneErr := sampler.hasErrorColumn(block(valuetype.ValueTypeInt64, convert.Int64ToBytes(0), nil))
		require.NoError(t, noneErr)
		assert.False(t, none, "zero and null carry no error signal")
	})

	t.Run("str fast path", func(t *testing.T) {
		for _, truthy := range []string{"true", "1"} {
			hit, hitErr := sampler.hasErrorColumn(block(valuetype.ValueTypeStr, []byte(truthy)))
			require.NoError(t, hitErr)
			assert.True(t, hit, "%q is an error", truthy)
		}
		for _, falsy := range []string{"false", "0", "", "TRUE"} {
			none, noneErr := sampler.hasErrorColumn(block(valuetype.ValueTypeStr, []byte(falsy)))
			require.NoError(t, noneErr)
			assert.False(t, none, "%q is not an error — the comparison is exact", falsy)
		}
		none, noneErr := sampler.hasErrorColumn(block(valuetype.ValueTypeStr, nil))
		require.NoError(t, noneErr)
		assert.False(t, none, "a null cell carries no error signal")
	})

	t.Run("fallback propagates a decode error", func(t *testing.T) {
		_, errColErr := sampler.hasErrorColumn(block(valuetype.ValueTypeFloat64, []byte{0x01}))
		require.Error(t, errColErr, "a malformed cell in a length-checked column must not be skipped as an unknown type")
		assert.Contains(t, errColErr.Error(), "float64: expected 8 bytes")
	})

	t.Run("fallback reports no signal for a well-formed unknown type", func(t *testing.T) {
		hit, hitErr := sampler.hasErrorColumn(block(valuetype.ValueTypeFloat64, make([]byte, 8)))
		require.NoError(t, hitErr)
		assert.False(t, hit, "no other type carries an error signal this schema understands")
	})

	t.Run("decode error in the fallback fails open through Decide", func(t *testing.T) {
		verdict, decideErr := sampler.Decide(&sdk.TraceBatch{Traces: []sdk.TraceBlock{*block(valuetype.ValueTypeFloat64, []byte{0x01})}})
		require.NoError(t, decideErr)
		assert.Equal(t, []bool{true}, verdict.Keep, "a decode error must fail open (keep), never drop")
	})
}

// TestHasSlowTrace_NullCells pins how null cells shape the duration envelope on
// the fast scalar path. Both skips are load-bearing in a way that decides whether
// a trace is KEPT or DROPPED:
//
//   - a null start must not contribute to minStart, or the envelope is measured
//     from zero and every trace looks slow enough to keep;
//   - a null duration must leave the right edge unknown, which is a can't-tell
//     that fails open — treating it as a zero-length end instead reports "not
//     slow" and drops the trace.
func TestHasSlowTrace_NullCells(t *testing.T) {
	s, err := New([]byte(`{"durationThresholdMs":500}`), segmentSchema)
	require.NoError(t, err)
	sampler := s.(*Sampler)

	// segmentSchema scales latency by 1e6 (milliseconds -> nanoseconds).
	const sixHundredMillisInNanos = 600 * 1_000_000
	rows := func(starts, durations [][]byte) *sdk.TraceBlock {
		return &sdk.TraceBlock{
			TraceID: "t",
			Tags: []sdk.TagColumn{
				{Name: "start_time", ValueType: valuetype.ValueTypeTimestamp, Values: starts},
				{Name: "latency", ValueType: valuetype.ValueTypeInt64, Values: durations},
			},
		}
	}

	t.Run("null start does not drag minStart to zero", func(t *testing.T) {
		// One null row and one real row 600ms in. Counting the null row's start as
		// 0 would measure a 600ms envelope and call the trace slow.
		slow, slowErr := sampler.hasSlowTrace(rows(
			[][]byte{nil, convert.Int64ToBytes(sixHundredMillisInNanos)},
			[][]byte{nil, convert.Int64ToBytes(1)},
		))
		require.NoError(t, slowErr)
		assert.False(t, slow, "the envelope spans only the rows that carry a start, so it is 1ms, not 600ms")
	})

	t.Run("null duration leaves the right edge unknown", func(t *testing.T) {
		_, slowErr := sampler.hasSlowTrace(rows(
			[][]byte{convert.Int64ToBytes(sixHundredMillisInNanos)},
			[][]byte{nil},
		))
		assert.ErrorIs(t, slowErr, errNoDurationEnvelope,
			"a start with no duration cannot bound the end; that is a can't-tell, which fails open")
	})

	t.Run("a start without duration still bounds the left edge", func(t *testing.T) {
		// Row 0 carries only a start, 600ms before row 1's start+duration. The
		// envelope must span from row 0's start, making the trace slow.
		slow, slowErr := sampler.hasSlowTrace(rows(
			[][]byte{convert.Int64ToBytes(0), convert.Int64ToBytes(sixHundredMillisInNanos)},
			[][]byte{nil, convert.Int64ToBytes(1)},
		))
		require.NoError(t, slowErr)
		assert.True(t, slow, "min start comes from the duration-less row, so the envelope is ~601ms")
	})
}

// TestHasSlowTrace_RaggedColumns pins the bounds discipline that the fast scalar
// reader depends on. scalarInt64 does not bounds-check — that is the point, since
// TagColumn.At's check is part of what it avoids — so hasSlowTrace must never
// index past the shorter of the two columns.
//
// A ragged block is malformed input, not a programming error, so the outcome has
// to be a verdict rather than a panic: it must fail open like any other
// can't-tell. Losing the clamp would turn that into a panic inside a merge.
func TestHasSlowTrace_RaggedColumns(t *testing.T) {
	s, err := New([]byte(`{"durationThresholdMs":500}`), segmentSchema)
	require.NoError(t, err)
	sampler := s.(*Sampler)

	ragged := func(starts, durations [][]byte) *sdk.TraceBlock {
		return &sdk.TraceBlock{
			TraceID: "ragged",
			Tags: []sdk.TagColumn{
				{Name: "start_time", ValueType: valuetype.ValueTypeTimestamp, Values: starts},
				{Name: "latency", ValueType: valuetype.ValueTypeInt64, Values: durations},
			},
		}
	}
	three := [][]byte{convert.Int64ToBytes(1), convert.Int64ToBytes(2), convert.Int64ToBytes(3)}
	one := [][]byte{convert.Int64ToBytes(1)}

	for _, tc := range []struct {
		name      string
		starts    [][]byte
		durations [][]byte
	}{
		{"more starts than durations", three, one},
		{"more durations than starts", one, three},
		{"no durations at all", three, nil},
		{"no starts at all", nil, three},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.NotPanics(t, func() {
				_, _ = sampler.hasSlowTrace(ragged(tc.starts, tc.durations))
			}, "a ragged block must produce a verdict, not an index panic")
		})
	}

	// Through the public entry point: clamping to zero rows leaves the envelope
	// unmeasurable, which is a can't-tell and must fail open.
	noOverlap, decideErr := sampler.Decide(&sdk.TraceBatch{Traces: []sdk.TraceBlock{*ragged(three, nil)}})
	require.NoError(t, decideErr)
	assert.Equal(t, []bool{true}, noOverlap.Keep, "no row carries both edges, so the envelope is unmeasurable and fails open")

	// Where the clamp still leaves a complete row, the verdict is real rather than
	// fail-open: that row's envelope is 1ms, far below the 500ms threshold.
	measurable, measurableErr := sampler.Decide(&sdk.TraceBatch{Traces: []sdk.TraceBlock{*ragged(three, one)}})
	require.NoError(t, measurableErr)
	assert.Equal(t, []bool{false}, measurable.Keep, "a clamped-but-complete row still yields a genuine not-slow verdict")
}
