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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/pb/v1/valuetype"
	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk"
	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk/sdktest"
)

// segmentSchema and zipkinSchema mirror the two first-party plugins' Schema
// values so the engine is exercised exactly as they configure it.
var (
	segmentSchema = Schema{
		ArrayTagColumn: "tags", ErrorTag: "is_error",
		DurationTag: "latency", StartTimeTag: "start_time", DurationTagNanosPerUnit: 1_000_000,
	}
	zipkinSchema = Schema{
		ArrayTagColumn: "query", ErrorTag: "error", ErrorTagInArray: true,
		DurationTag: "duration", StartTimeTag: "timestamp_millis", DurationTagNanosPerUnit: 1_000,
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
