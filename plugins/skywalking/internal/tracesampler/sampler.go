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

// Package tracesampler is the shared post-trace sampler engine behind the
// first-party sw-trace-sampler and zipkin-trace-sampler plugins. Both plugins
// implement the same keep logic from docs/design/post-trace-pipeline.md
// (Scenario 6.1 for SkyWalking segments, 6.2 for Zipkin) — a duration
// threshold, sure-keep error and tag rules, and a deterministic healthy sample
// — and differ only in how each schema physically stores the columns those
// rules read. That per-schema knowledge is a Schema value passed to New;
// everything else lives here so the two plugins stay a few lines each and
// cannot drift apart.
//
// Tag matching accounts for the real BanyanDB trace layout SkyWalking writes:
// searchable tags are not first-class columns but "key=value" entries flattened
// into one string-array column ("tags" for segments, "query" for Zipkin), so
// every keepTagRules entry is matched against that array.
package tracesampler

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/encoding/vararray"
	"github.com/apache/skywalking-banyandb/pkg/pb/v1/valuetype"
	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk"
	"github.com/apache/skywalking-banyandb/pkg/pool"
)

// Schema captures the per-plugin storage facts the shared engine needs: where a
// trace's searchable tags live, which columns carry the duration envelope, and
// how (or whether) "error" is expressed. The two first-party plugins differ only
// in the Schema they pass to New.
//
// Only the columns named below are ever read as columns. Every keepTagRules entry
// resolves to ArrayTagColumn, so a rule can only match a searchable tag — never a
// first-class column such as local_endpoint_service_name. New rejects a tagKey
// naming one of the columns it does know to be first-class, rather than letting
// the rule silently never fire.
type Schema struct {
	// ArrayTagColumn is the flattened searchable-tag column: a string array of
	// "key=value" entries — "tags" for the SkyWalking segment schema, "query"
	// for the Zipkin schema. Every keepTagRules entry resolves here: a rule whose
	// tagKey is this column's own name matches raw entries, and any other tagKey
	// matches the value of the "tagKey=" entries.
	ArrayTagColumn string
	// ErrorTag is what keepErrors reads. An empty ErrorTag means the schema has no
	// error signal at all and keepErrors is rejected at construction.
	ErrorTag string
	// DurationTag and StartTimeTag drive the durationThresholdMs rule, which
	// keeps a trace whose end-to-end envelope reaches the threshold. The envelope
	// is max(start + duration) - min(start) over the trace's rows, computed from
	// these two per-row tags. This is the true trace duration: it catches traces
	// that are slow only through sequential segments.
	//
	// DO NOT replace this with the intrinsic MaxTS - MinTS, however tempting the
	// saving. sdk.TraceBlock documents MaxTS as "the latest span end ... trace
	// duration is MaxTS - MinTS" and _example/segment-tail-sampler computes it that
	// way, but both are WRONG: banyand/trace/block.go fills MinTS and MaxTS from the
	// same per-row timestamp column, so they are the spread of span/segment START
	// times. That understates any trace whose last span outlives the last span to
	// begin, and is exactly 0 for a single-row trace — which would silently drop
	// every single-segment slow trace. Reading the two tags costs a decode; being
	// correct is worth it.
	//
	// DurationTag is the per-row duration column: "latency" (segment duration, ms)
	// for the segment schema, "duration" (span duration, µs) for Zipkin.
	DurationTag string
	// StartTimeTag is the per-row start timestamp column: "start_time" for the
	// segment schema, "timestamp_millis" for Zipkin. Both are stored as timestamp
	// tags (unix nanoseconds), so the plugin reads them as int64 ns.
	StartTimeTag string
	// DurationTagNanosPerUnit converts one DurationTag unit to nanoseconds so the
	// envelope math is ns-consistent with StartTimeTag: 1_000_000 for a millisecond
	// tag (segment latency), 1_000 for a microsecond tag (Zipkin duration).
	// FirstClassColumns is the schema's full column inventory — every tag OAP stores
	// as a real column rather than as an entry of ArrayTagColumn. It exists purely to
	// reject rules that could never match: "keep everything from the payment service",
	// written as {tagKey: service_id, equals: ...}, is admitted by a schema that does
	// not list its columns and then drops exactly those traces.
	//
	// List every column INCLUDING the duration, start-time and error tags named above
	// (the dedicated checks simply produce a better message for those), but NOT
	// ArrayTagColumn itself — a rule on that is the documented escape hatch for a
	// searchable tag whose key collides with a column name.
	FirstClassColumns       []string
	DurationTagNanosPerUnit int64
	// ErrorTagInArray says the error signal is a KEY INSIDE ArrayTagColumn rather
	// than a column of its own. The segment schema has a real is_error column
	// (false); Zipkin has none, but OAP flattens every span tag into "query" as both
	// a bare key and "key=value", so a span carrying Zipkin's conventional "error"
	// tag is detectable there (true).
	//
	// Note this is a tag CONVENTION, not an authoritative field: instrumentations
	// that signal failure only through http.status_code 5xx or otel.status_code are
	// not covered, and need an explicit keepTagRules entry.
	//
	// It is also subject to an OAP-side truncation. SpanForward.java skips BOTH the
	// bare key and "key=value" when either the value or the joined string exceeds
	// Tag.TAG_LENGTH (256 chars), so a Zipkin "error" tag carrying a long exception
	// message writes nothing into the array and keepErrors cannot see it — precisely
	// the loudest errors go missing. Catching those needs a keepTagRules entry on a
	// short-valued tag (an http.status_code regex, say) until OAP records the bare key
	// before the length check.
	ErrorTagInArray bool
}

// firstClassColumn reports the config option covering tagKey when the schema
// stores it as a real column rather than an entry of ArrayTagColumn, or "" when
// a rule on tagKey is legitimate.
//
// errorColumn is the error column read at RUNTIME — schema.ErrorTag or the
// operator's errorTag override — and "" when keepErrors is off or the signal is an
// array entry. It is a separate parameter because checking schema.ErrorTag alone
// would miss an override: keepErrors would read the override as a column while a
// rule on the same key matched array entries, the exact silent no-op this guard
// exists to prevent.
//
// Coverage is only as good as Schema.FirstClassColumns: a column the schema does not
// list is still admitted and then silently never matches. Callers reject an empty
// tagKey before calling this, which also stops an unset Schema field from aliasing
// every rule.
func (s Schema) firstClassColumn(tagKey, errorColumn string) string {
	if tagKey == s.ArrayTagColumn {
		// The escape hatch, not a column: a rule here sees the raw "key=value" entries.
		return ""
	}
	switch tagKey {
	case s.DurationTag, s.StartTimeTag:
		return "the durationThresholdMs option reads it"
	case errorColumn:
		return "the keepErrors option reads it"
	}
	// The schema's own error column stays first-class even when keepErrors is off or
	// overridden — a rule still cannot reach it.
	if tagKey == s.ErrorTag && !s.ErrorTagInArray {
		return "the keepErrors option reads it"
	}
	for _, col := range s.FirstClassColumns {
		if tagKey == col {
			return "no sampler option reads it"
		}
	}
	return ""
}

// rule is one sure-keep tag predicate. Exactly one matcher is honored, checked
// in the order exists, equals, in, regex.
type rule struct {
	re *regexp.Regexp
	// prefix is the "tagKey=" that an entry must carry for this rule to apply,
	// precomputed at admission because matchEntries runs per rule per trace and
	// the concatenation showed up as runtime.concatstrings in the tag-rule
	// profile. Empty when the rule targets the array column itself, which is the
	// case that matches a bare entry rather than a "key=value" one.
	prefix string
	Regex  string   `json:"regex"`
	TagKey string   `json:"tagKey"`
	Equals string   `json:"equals"`
	In     []string `json:"in"`
	Exists bool     `json:"exists"`
}

// rules is a keepTagRules list that accepts either the explicit array form or a
// single compact string. The compact form exists because the array-of-objects
// form is unwieldy in an environment variable and, written inline in bydb.yml,
// has to be quoted (its ": " would otherwise start a nested mapping):
//
//	keepTagRules: ${...:http.method=GET,http.status_code=~5\d\d}
//
// Grammar — rules separated by commas, each one of:
//
//	key=value    equals   (split on the FIRST "=", so values may contain "=")
//	key=~regex   regex
//	key          exists
//
// Commas inside (), [] or {} do not separate rules, so a quantifier such as
// 5\d{2,3} survives. A value containing a top-level comma needs the array form.
type rules []rule

// UnmarshalJSON accepts the array form verbatim, or a string in the compact form.
func (r *rules) UnmarshalJSON(data []byte) error {
	trimmed := strings.TrimSpace(string(data))
	if trimmed == "null" {
		return nil
	}
	if !strings.HasPrefix(trimmed, `"`) {
		// Array form: decode through a plain alias so this method is not re-entered.
		// Strict for the same reason as the top-level config — a misspelled matcher key
		// would otherwise leave a rule that silently matches nothing.
		var explicit []rule
		dec := json.NewDecoder(bytes.NewReader(data))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&explicit); err != nil {
			return err
		}
		*r = explicit
		return nil
	}
	var compact string
	if err := json.Unmarshal(data, &compact); err != nil {
		return err
	}
	parsed, err := parseCompactRules(compact)
	if err != nil {
		return err
	}
	*r = parsed
	return nil
}

// parseCompactRules parses the compact "key=value,key=~regex,key" grammar.
func parseCompactRules(s string) (rules, error) {
	var out rules
	for _, part := range splitTopLevel(s, ',') {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		eq := strings.Index(part, "=")
		if eq == 0 {
			return nil, fmt.Errorf("rule %q has an empty tagKey", part)
		}
		if eq < 0 {
			out = append(out, rule{TagKey: part, Exists: true})
			continue
		}
		key, value := part[:eq], part[eq+1:]
		// Report an empty operand here, where the operator's actual mistake is still
		// visible. Left to the generic validation below it would surface as the
		// misleading "has no matcher", since an empty Equals/Regex is indistinguishable
		// from an unset one.
		if strings.HasPrefix(value, "~") {
			if value == "~" {
				return nil, fmt.Errorf("rule %q has an empty regex after %q", part, "=~")
			}
			out = append(out, rule{TagKey: key, Regex: value[1:]})
			continue
		}
		if value == "" {
			return nil, fmt.Errorf("rule %q has an empty value; write %q to keep on the tag's "+
				"presence regardless of value", part, key)
		}
		out = append(out, rule{TagKey: key, Equals: value})
	}
	return out, nil
}

// splitTopLevel splits on sep, ignoring separators nested in (), [] or {} — so a
// regex quantifier like {2,3} is not mistaken for a rule boundary.
func splitTopLevel(s string, sep rune) []string {
	var parts []string
	depth, start := 0, 0
	for i, c := range s {
		switch c {
		case '(', '[', '{':
			depth++
		case ')', ']', '}':
			if depth > 0 {
				depth--
			}
		case sep:
			if depth == 0 {
				parts = append(parts, s[start:i])
				start = i + len(string(sep))
			}
		}
	}
	return append(parts, s[start:])
}

// flexFloat is a float64 that also accepts a JSON string ("0.1"). SkyWalking's
// bydb.yml resolves a ${ENV:default} placeholder through a converter that keeps
// only String/Integer/Long/Boolean types, so a float written as a placeholder
// arrives as a quoted string in the config Struct. Accepting both keeps float
// options env-overridable instead of failing the whole config at admission.
type flexFloat float64

// UnmarshalJSON accepts a JSON number or a numeric JSON string. A JSON null is a
// no-op leaving the zero value, per the encoding/json convention — unlike the
// default decoder, a custom Unmarshaler is handed null rather than skipped, and a
// blank value in bydb.yml reaches the plugin as null.
func (f *flexFloat) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		return nil
	}
	v, err := strconv.ParseFloat(strings.Trim(string(data), `"`), 64)
	if err != nil {
		return fmt.Errorf("expected a number, got %s", string(data))
	}
	*f = flexFloat(v)
	return nil
}

// config is the JSON shape the operator sets in SamplerPlugin.config.
// Field order here (and in the other structs in this file) is chosen to satisfy
// govet's fieldalignment: pointer-bearing fields first, the slice last among them,
// then plain scalars.
type config struct {
	ErrorTag     string `json:"errorTag"`
	KeepTagRules rules  `json:"keepTagRules"`
	// DurationThresholdMs keeps a trace whose end-to-end duration (the envelope of
	// its rows' start+duration, see Schema.DurationTag/StartTimeTag) reaches this
	// many milliseconds. Milliseconds match SkyWalking's own latency unit. 0 (or
	// omitted) disables it.
	DurationThresholdMs int64     `json:"durationThresholdMs"`
	HealthySampleRate   flexFloat `json:"healthySampleRate"`
	KeepErrors          bool      `json:"keepErrors"`
}

const (
	decisionMetricName                 = "trace_sampler_decisions_total"
	rowMetricName                      = "trace_sampler_rows_total"
	rowDroppedMetricName               = "trace_sampler_rows_dropped_total"
	rowCountUnavailableMetricName      = "trace_sampler_row_count_unavailable_total"
	decisionLabelVerdict               = "verdict"
	decisionLabelRule                  = "rule"
	decisionVerdictKeep                = "keep"
	decisionVerdictDrop                = "drop"
	decisionReasonDurationLabel        = "duration"
	decisionReasonErrorLabel           = "error"
	decisionReasonHealthySampleLabel   = "healthy_sample"
	decisionReasonHealthyRejectedLabel = "healthy_rejected"
	decisionReasonNoKeepRuleLabel      = "no_keep_rule"
	decisionReasonDecodeDurationLabel  = "decode_failure_duration"
	decisionReasonDecodeErrorLabel     = "decode_failure_error"
	decisionReasonDecodeTagsLabel      = "decode_failure_tags"
	decisionReasonTag00Label           = "tag_00"
	decisionReasonTag01Label           = "tag_01"
	decisionReasonTag02Label           = "tag_02"
	decisionReasonTag03Label           = "tag_03"
	decisionReasonTag04Label           = "tag_04"
	decisionReasonTag05Label           = "tag_05"
	decisionReasonTag06Label           = "tag_06"
	decisionReasonTag07Label           = "tag_07"
	decisionReasonTag08Label           = "tag_08"
	decisionReasonTag09Label           = "tag_09"
	decisionReasonTag10Label           = "tag_10"
	decisionReasonTag11Label           = "tag_11"
	decisionReasonTag12Label           = "tag_12"
	decisionReasonTag13Label           = "tag_13"
	decisionReasonTag14Label           = "tag_14"
	decisionReasonTag15Label           = "tag_15"
	decisionReasonTag16Label           = "tag_16"
	decisionReasonTag17Label           = "tag_17"
	decisionReasonTag18Label           = "tag_18"
	decisionReasonTag19Label           = "tag_19"
	decisionReasonTag20Label           = "tag_20"
	decisionReasonTag21Label           = "tag_21"
	decisionReasonTag22Label           = "tag_22"
	decisionReasonTag23Label           = "tag_23"
	decisionReasonTag24Label           = "tag_24"
	decisionReasonTag25Label           = "tag_25"
	decisionReasonTag26Label           = "tag_26"
	decisionReasonTag27Label           = "tag_27"
	decisionReasonTag28Label           = "tag_28"
	decisionReasonTag29Label           = "tag_29"
	decisionReasonTag30Label           = "tag_30"
	decisionReasonTag31Label           = "tag_31"
)

var tagDecisionReasonLabels = [...]string{
	decisionReasonTag00Label, decisionReasonTag01Label, decisionReasonTag02Label, decisionReasonTag03Label,
	decisionReasonTag04Label, decisionReasonTag05Label, decisionReasonTag06Label, decisionReasonTag07Label,
	decisionReasonTag08Label, decisionReasonTag09Label, decisionReasonTag10Label, decisionReasonTag11Label,
	decisionReasonTag12Label, decisionReasonTag13Label, decisionReasonTag14Label, decisionReasonTag15Label,
	decisionReasonTag16Label, decisionReasonTag17Label, decisionReasonTag18Label, decisionReasonTag19Label,
	decisionReasonTag20Label, decisionReasonTag21Label, decisionReasonTag22Label, decisionReasonTag23Label,
	decisionReasonTag24Label, decisionReasonTag25Label, decisionReasonTag26Label, decisionReasonTag27Label,
	decisionReasonTag28Label, decisionReasonTag29Label, decisionReasonTag30Label, decisionReasonTag31Label,
}

const maxKeepTagRules = len(tagDecisionReasonLabels)

type decisionReason uint8

const (
	decisionReasonDuration decisionReason = iota
	decisionReasonError
	decisionReasonHealthySample
	decisionReasonHealthyRejected
	decisionReasonNoKeepRule
	decisionReasonDecodeDuration
	decisionReasonDecodeError
	decisionReasonDecodeTags
	decisionReasonBuiltInCount
	decisionReasonTagRule
)

type decisionMetricLabels struct {
	verdict string
	rule    string
}

var builtInDecisionMetricLabels = [...]decisionMetricLabels{
	{verdict: decisionVerdictKeep, rule: decisionReasonDurationLabel},
	{verdict: decisionVerdictKeep, rule: decisionReasonErrorLabel},
	{verdict: decisionVerdictKeep, rule: decisionReasonHealthySampleLabel},
	{verdict: decisionVerdictDrop, rule: decisionReasonHealthyRejectedLabel},
	{verdict: decisionVerdictDrop, rule: decisionReasonNoKeepRuleLabel},
	{verdict: decisionVerdictKeep, rule: decisionReasonDecodeDurationLabel},
	{verdict: decisionVerdictKeep, rule: decisionReasonDecodeErrorLabel},
	{verdict: decisionVerdictKeep, rule: decisionReasonDecodeTagsLabel},
}

var (
	_ [len(builtInDecisionMetricLabels) - int(decisionReasonBuiltInCount)]struct{}
	_ [int(decisionReasonBuiltInCount) - len(builtInDecisionMetricLabels)]struct{}
)

type traceDecision struct {
	reason       decisionReason
	tagRuleIndex uint8
	keep         bool
}

// Sampler keeps a trace when any sure-keep rule matches, and otherwise admits a
// deterministic fraction of the healthy remainder. It implements sdk.Sampler.
type Sampler struct {
	arrayColumn             string
	errorTag                string
	durationTag             string
	startTimeTag            string
	rules                   []rule
	requiredTags            []string
	decisions               sdk.Counter
	rowsTotal               sdk.Counter
	rowsDropped             sdk.Counter
	rowCountUnavailable     sdk.Counter
	errorRule               rule
	durationThresholdMs     int64
	durationTagNanosPerUnit int64
	healthySampleRate       float64
	keepErrors              bool
	errorTagInArray         bool
}

// New parses and validates the operator config against the given Schema,
// compiles any regex matchers, and computes the projection. A returned error
// rejects the plugin at admission.
func New(configJSON []byte, schema Schema) (sdk.Sampler, error) {
	var c config
	if len(configJSON) > 0 {
		// Strict: an unrecognized key is an error, not a silent no-op. Ignoring one is
		// catastrophic here rather than merely untidy — every option this plugin has is
		// a KEEP rule, so a config whose keys all miss (a snake_case config copied from
		// the _example plugin, say) yields a sampler with no rules at all, which drops
		// every trace in the group. Note Go matches field names case-insensitively, so
		// this catches wrong words, not wrong capitalization.
		dec := json.NewDecoder(bytes.NewReader(configJSON))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&c); err != nil {
			// Not necessarily malformed JSON: a compact keepTagRules string that fails
			// its own grammar also surfaces here, via rules.UnmarshalJSON.
			return nil, fmt.Errorf("tracesampler: invalid config: %w", err)
		}
	}
	// A config carrying no keys at all leaves every option at its zero value, which for
	// this plugin means no keep rule of any kind: it would drop the entire group. That
	// state is reachable by OMISSION, not just by writing {} — pipeline_loader.go
	// substitutes []byte("{}") when SamplerPlugin.config is unset — so a plugin
	// registered without a config would silently delete the group's traces.
	//
	// This deliberately tests for an absent KEY, not for absent keep criteria: rate 0
	// with no rules is a supported setting, so {"healthySampleRate": 0} stays valid.
	var present map[string]json.RawMessage
	if len(configJSON) > 0 {
		// Already known well-formed: the strict decode above would have rejected it.
		_ = json.Unmarshal(configJSON, &present)
	}
	if len(present) == 0 {
		return nil, errors.New("tracesampler: config is empty, so no keep rule is set and every " +
			"trace in the group would be dropped; set at least one option (or leave the pipeline " +
			"disabled if retaining nothing is the intent)")
	}
	if c.HealthySampleRate < 0 || c.HealthySampleRate > 1 {
		return nil, fmt.Errorf("tracesampler: healthySampleRate %v out of [0,1]", c.HealthySampleRate)
	}
	if len(c.KeepTagRules) > maxKeepTagRules {
		return nil, fmt.Errorf("tracesampler: keepTagRules contains %d rules, maximum is %d", len(c.KeepTagRules), maxKeepTagRules)
	}
	s := &Sampler{
		arrayColumn:       schema.ArrayTagColumn,
		rules:             c.KeepTagRules,
		healthySampleRate: float64(c.HealthySampleRate),
		keepErrors:        c.KeepErrors,
	}
	if c.KeepErrors {
		s.errorTag = schema.ErrorTag
		if c.ErrorTag != "" {
			s.errorTag = c.ErrorTag
		}
		if s.errorTag == "" {
			return nil, fmt.Errorf("tracesampler: keepErrors is set but this schema has no error signal; "+
				"catch errors with a keepTagRules entry on %q instead", schema.ArrayTagColumn)
		}
		s.errorTagInArray = schema.ErrorTagInArray
		// The in-array error signal is just an exists rule on the error key, evaluated
		// by the same matcher as keepTagRules so the two can never diverge.
		// Built here rather than through validateRules, so its prefix has to be
		// set explicitly — matchEntries reads the precomputed field for every rule.
		s.errorRule = rule{TagKey: s.errorTag, Exists: true}
		s.errorRule.setPrefix(s.arrayColumn)
	}
	if c.DurationThresholdMs < 0 {
		return nil, fmt.Errorf("tracesampler: durationThresholdMs must be >= 0, got %d", c.DurationThresholdMs)
	}
	s.durationThresholdMs = c.DurationThresholdMs
	if s.durationThresholdMs > 0 {
		if schema.DurationTag == "" || schema.StartTimeTag == "" || schema.DurationTagNanosPerUnit <= 0 {
			return nil, fmt.Errorf("tracesampler: durationThresholdMs is set but this schema has no duration/start-time tag configured")
		}
		s.durationTag = schema.DurationTag
		s.startTimeTag = schema.StartTimeTag
		s.durationTagNanosPerUnit = schema.DurationTagNanosPerUnit
	}

	// Build the projection: the error column (when keepErrors is set), the duration
	// envelope columns, and — for any rule at all — the flattened array column, the
	// only place a rule can match. Compile regex matchers once, here, not per batch.
	tagSet := make(map[string]struct{})
	// errorColumn is set only when the error signal is read as a real column, so it
	// carries the errorTag override that schema.ErrorTag would not.
	errorColumn := ""
	if s.keepErrors {
		if s.errorTagInArray {
			tagSet[s.arrayColumn] = struct{}{}
		} else {
			tagSet[s.errorTag] = struct{}{}
			errorColumn = s.errorTag
		}
	}
	if s.durationTag != "" {
		tagSet[s.durationTag] = struct{}{}
		tagSet[s.startTimeTag] = struct{}{}
	}
	if err := validateRules(s.rules, schema, errorColumn); err != nil {
		return nil, err
	}
	if len(s.rules) > 0 {
		// Every rule resolves to the flattened array column, whatever its tagKey.
		tagSet[s.arrayColumn] = struct{}{}
	}
	s.requiredTags = make([]string, 0, len(tagSet))
	for k := range tagSet {
		s.requiredTags = append(s.requiredTags, k)
	}
	// Stable order keeps Project() reproducible across runs (Go map iteration is
	// randomized); the engine treats Tags as a set, but logs, tests, and caches
	// benefit from determinism.
	sort.Strings(s.requiredTags)
	return s, nil
}

// validateRules rejects any rule that could never match and compiles each regex in
// place, so the cost is paid once at admission rather than per batch. errorColumn is
// the error column read at runtime; see Schema.firstClassColumn.
func validateRules(rs []rule, schema Schema, errorColumn string) error {
	for i := range rs {
		r := &rs[i]
		if r.TagKey == "" {
			return fmt.Errorf("tracesampler: keepTagRules[%d] has empty tagKey", i)
		}
		if !r.Exists && r.Equals == "" && len(r.In) == 0 && r.Regex == "" {
			return fmt.Errorf("tracesampler: keepTagRules[%d] (tagKey %q) has no matcher; "+
				"set one of exists/equals/in/regex", i, r.TagKey)
		}
		if hint := schema.firstClassColumn(r.TagKey, errorColumn); hint != "" {
			// A hard error, not a warning: plugins have no log channel, and silently
			// accepting the rule is the failure mode this check exists to remove. The
			// array-column escape hatch matters when a searchable tag legitimately shares
			// a name with a column (a Zipkin span tag literally called "duration").
			return fmt.Errorf("tracesampler: keepTagRules[%d] targets %q, which this schema stores as a "+
				"first-class column, not as an entry of %q, so the rule could never match (%s). To match a "+
				"searchable tag that happens to share the name, write the rule against %q itself, "+
				"e.g. {tagKey: %q, regex: \"^%s=\"}",
				i, r.TagKey, schema.ArrayTagColumn, hint,
				schema.ArrayTagColumn, schema.ArrayTagColumn, r.TagKey)
		}
		if r.Regex != "" {
			re, err := regexp.Compile(r.Regex)
			if err != nil {
				return fmt.Errorf("tracesampler: keepTagRules[%d] bad regex %q: %w", i, r.Regex, err)
			}
			r.re = re
		}
		r.setPrefix(schema.ArrayTagColumn)
	}
	return nil
}

// setPrefix precomputes the rule's entry prefix against the flattened array
// column. A rule naming the array column itself matches a bare entry, so it has
// no prefix; every other rule matches "tagKey=value" entries.
func (r *rule) setPrefix(arrayColumn string) {
	if r.TagKey != arrayColumn {
		r.prefix = r.TagKey + "="
	}
}

// Kind reports the sampler kind, satisfying the generic sdk.Plugin interface
// that sdk.Sampler embeds.
func (s *Sampler) Kind() sdk.Kind { return sdk.KindSampler }

// Project declares the columns the verdict reads: the duration and start-time
// columns (when a duration threshold is set), the error column (when keepErrors
// is set), the flattened array column (when any tag rule is configured, or the
// error signal lives there), plus the span-id column only when a span-count rule
// is configured.
//
// Spans stays false, so the verdict never reads span bodies. The merge can copy
// a unique raw block unchanged while decoding only these projected columns for
// evaluation. A trace split across physical blocks still takes the ordinary
// decoded path because compaction must consolidate its output blocks.
func (s *Sampler) Project() sdk.Projection {
	return sdk.Projection{Tags: s.requiredTags}
}

// Close releases resources; this sampler holds none.
func (s *Sampler) Close() error { return nil }

// UseHost binds the sampler's decision counter to its group-scoped host.
func (s *Sampler) UseHost(host sdk.Host) {
	meter := host.Meter()
	s.decisions = meter.Counter(decisionMetricName, decisionLabelVerdict, decisionLabelRule)
	s.rowsTotal = meter.Counter(rowMetricName)
	s.rowsDropped = meter.Counter(rowDroppedMetricName, decisionLabelRule)
	s.rowCountUnavailable = meter.Counter(rowCountUnavailableMetricName)
}

// Decide returns a keep-mask aligned to batch.Traces. The batch is read-only.
// It never returns an error: a per-row decode failure fails open for that trace
// (kept), so one malformed value can never make the sampler drop data.
func (s *Sampler) Decide(batch *sdk.TraceBatch) (sdk.Verdict, error) {
	keep := make([]bool, len(batch.Traces))
	if s.decisions == nil {
		for traceIndex := range batch.Traces {
			keep[traceIndex] = s.decideTrace(&batch.Traces[traceIndex]).keep
		}
		return sdk.Verdict{Keep: keep}, nil
	}

	var reasonCounts [decisionReasonBuiltInCount]uint64
	var droppedReasonRowCounts [decisionReasonBuiltInCount]uint64
	var tagReasonCounts [maxKeepTagRules]uint64
	var totalRows uint64
	var unavailableRowCounts uint64
	for traceIndex := range batch.Traces {
		decision := s.decideTrace(&batch.Traces[traceIndex])
		keep[traceIndex] = decision.keep
		rowCount := batch.Traces[traceIndex].Len()
		if decision.reason == decisionReasonTagRule {
			tagReasonCounts[decision.tagRuleIndex]++
		} else {
			reasonCounts[decision.reason]++
		}
		if rowCount > 0 {
			totalRows += uint64(rowCount)
			if !decision.keep {
				droppedReasonRowCounts[decision.reason] += uint64(rowCount)
			}
		} else {
			unavailableRowCounts++
		}
	}
	emitSamplerCounts(s.decisions, reasonCounts, tagReasonCounts)
	if totalRows > 0 {
		s.rowsTotal.Inc(float64(totalRows))
	}
	for reasonIndex, rowCount := range droppedReasonRowCounts {
		if rowCount > 0 {
			s.rowsDropped.Inc(float64(rowCount), builtInDecisionMetricLabels[reasonIndex].rule)
		}
	}
	if unavailableRowCounts > 0 {
		s.rowCountUnavailable.Inc(float64(unavailableRowCounts))
	}
	return sdk.Verdict{Keep: keep}, nil
}

func emitSamplerCounts(counter sdk.Counter, builtInCounts [decisionReasonBuiltInCount]uint64,
	tagCounts [maxKeepTagRules]uint64,
) {
	for reasonIndex, count := range builtInCounts {
		if count == 0 {
			continue
		}
		labels := builtInDecisionMetricLabels[reasonIndex]
		counter.Inc(float64(count), labels.verdict, labels.rule)
	}
	for ruleIndex, count := range tagCounts {
		if count > 0 {
			counter.Inc(float64(count), decisionVerdictKeep, tagDecisionReasonLabels[ruleIndex])
		}
	}
}

// decideTrace applies the sure-keep rules, then the deterministic healthy sample.
// Any decode error encountered while evaluating a sure-keep predicate keeps the
// trace (fail open), never drops it.
//
// The flattened-tag-array decode is deferred until a rule that needs it is
// reached: arrayEntries runs at most once per call, cached via the entriesPtr
// nil-check, and never fires when no rule asks for it. For a realistic-mix
// workload where the error rule keeps most traces, this skips the decode for
// every error-kept trace and is the dominant cost on the hot path.
//
// The single-call guarantee is an optimization, not a correctness requirement.
// arrayEntries already copies each row before decoding (see its per-row copy
// comment), so repeated calls would not corrupt the engine buffer. The
// inlined lazy decode exists to avoid repeated decode work and the per-row
// allocations it triggers, and to keep failure semantics consistent across
// every rule in the trace.
func (s *Sampler) decideTrace(traceBlock *sdk.TraceBlock) traceDecision {
	// Duration keep: the trace's end-to-end envelope reaches the threshold.
	// The early return keeps entriesPtr nil, so no pool release is needed.
	if s.durationThresholdMs > 0 {
		hit, durErr := s.hasSlowTrace(traceBlock)
		if durErr != nil {
			return traceDecision{keep: true, reason: decisionReasonDecodeDuration}
		}
		if hit {
			return traceDecision{keep: true, reason: decisionReasonDuration}
		}
	}
	// Lazy tag-array decode. Entries are computed on first call and reused for
	// every subsequent rule in this trace, so a multi-rule config still pays
	// the decode cost exactly once. A decode error is cached and returned to
	// every caller, preserving fail-open semantics at every call site.
	//
	// The entries slice is acquired from entriesSlicePool inside arrayEntries
	// and released back at the single exit below. The lazy decode is inlined
	// (not factored into a closure) so the captures — entriesPtr and the err —
	// stay on the stack: a closure value that captures variables forces those
	// variables onto the heap even when the closure itself never escapes the
	// function. The two call sites stay in sync via the nil-check on
	// entriesPtr, which the inlined blocks both perform.
	//
	// entriesPtr is a *[]string: the pool element itself. Storing a pointer
	// (rather than a []string whose address would have to be taken for the
	// release) keeps this variable on the stack — the pool owns the backing
	// storage, so taking the pointer's address is unnecessary.
	//
	// stableBufPtr is the *[]byte that backs the entry strings of any ESCAPED
	// row, which arrayEntries acquires and hands back only when it needed one.
	// Those strings alias its backing array via convert.BytesToString, so the
	// array must outlive every matchEntries call below; it is released at the
	// single exit. A nil stableBufPtr means no row needed unescaping (the
	// common case) or nothing was decoded at all, and there is nothing to
	// release.
	//
	// decoded, not a nil-check on entriesPtr, is what makes the decode fire
	// once: arrayEntries returns a nil entriesPtr on error, so keying the
	// second call site off entriesPtr would decode twice — and overwrite (leak)
	// the stable buffer of the first attempt.
	var (
		entriesPtr   *[]string
		stableBufPtr *[]byte
		entriesErr   error
		decoded      bool
	)
	decision := traceDecision{reason: decisionReasonNoKeepRule}
	// Error keep.
	if s.keepErrors {
		if s.errorTagInArray {
			if !decoded {
				decoded = true
				entriesPtr, stableBufPtr, entriesErr = arrayEntries(traceBlock.Tag(s.arrayColumn))
			}
			if entriesErr != nil {
				decision = traceDecision{keep: true, reason: decisionReasonDecodeError}
			} else if matchEntries(*entriesPtr, &s.errorRule) {
				decision = traceDecision{keep: true, reason: decisionReasonError}
			}
		} else {
			hit, errColErr := s.hasErrorColumn(traceBlock)
			if errColErr != nil {
				decision = traceDecision{keep: true, reason: decisionReasonDecodeError}
			} else if hit {
				decision = traceDecision{keep: true, reason: decisionReasonError}
			}
		}
	}
	// Sure-keep tag rules. The decode is hoisted out of the loop so the
	// lazy fetch fires exactly once even when multiple rules are configured.
	if !decision.keep && len(s.rules) > 0 {
		if !decoded {
			decoded = true
			entriesPtr, stableBufPtr, entriesErr = arrayEntries(traceBlock.Tag(s.arrayColumn))
		}
		if entriesErr != nil {
			decision = traceDecision{keep: true, reason: decisionReasonDecodeTags}
		} else {
			for ruleIndex := range s.rules {
				if matchEntries(*entriesPtr, &s.rules[ruleIndex]) {
					decision = traceDecision{keep: true, reason: decisionReasonTagRule, tagRuleIndex: uint8(ruleIndex)}
					break
				}
			}
		}
	}
	// Healthy remainder: deterministic hash(trace_id) < rate, stable across
	// re-evaluation at merge and finalization.
	if !decision.keep && s.healthySampleRate > 0 {
		if sampleFraction(traceBlock.TraceID) < s.healthySampleRate {
			decision = traceDecision{keep: true, reason: decisionReasonHealthySample}
		} else {
			decision.reason = decisionReasonHealthyRejected
		}
	}
	// Release only what a decode acquired. Keying this off decoded rather than
	// off the pointers is what keeps the flag honest: it is the same flag both
	// call sites above test, so a future third site cannot quietly skip it.
	if decoded {
		releaseEntries(entriesPtr)
		releaseStableBuf(stableBufPtr)
	}
	return decision
}

// nanosPerMillis converts the millisecond threshold to nanoseconds for the
// envelope comparison, matching the nanosecond start-time tags.
const nanosPerMillis = int64(1_000_000)

// errNoDurationEnvelope reports that no row yielded a usable start/duration pair,
// so the envelope could not be computed at all. decideTrace treats any error as a
// keep, which is the intended reading: "can't tell", not "not slow". The columns
// are declared by the Schema, so their absence means the block was written under a
// different schema — typically the wrong plugin for the group — and answering
// "not slow" there would silently drop every trace the operator asked to keep.
//
// This is deliberately the opposite of an absent ArrayTagColumn, which stays
// fail-closed: a trace carrying no searchable tags is ordinary data, and keeping
// it would make every tagless trace survive and the tag rules pointless.
var errNoDurationEnvelope = errors.New("no row carries both a start-time and a duration value")

// hasSlowTrace reports whether the trace's end-to-end envelope reaches the
// threshold. The envelope is max(start + duration) - min(start) over the rows,
// where start comes from StartTimeTag (a timestamp tag, unix ns) and duration
// from DurationTag (scaled to ns by DurationTagNanosPerUnit). This is the true
// trace duration — it catches traces slow only through sequential segments —
// and reads two cheap tag columns, never the span bodies.
// scalarInt64 reads one cell of an 8-byte-encoded column WITHOUT materializing
// the sdk.Value that TagColumn.At returns. That value is 112 bytes and is
// returned by value, so reading a column per row through At cost more in
// runtime.duffcopy than in decoding: 54.4% of the duration path, 37.5% of the
// dedicated-error path, and 22.3% of the full realistic scenario.
//
// It reproduces sdk.DecodeTagValue's contract exactly, including the error text,
// because a malformed cell must keep failing open with the same message the
// engine would have logged:
//
//	nil cell        -> (0, false, nil)   — null, the caller skips the row
//	non-8-byte cell -> (0, false, error) — malformed
//
// Callers must have checked col.ValueType themselves; see the narrow-gating note
// at each call site for why the type check cannot simply be hoisted.
//
// PRECONDITION: row must be in range. Unlike TagColumn.At this does NOT
// bounds-check — an out-of-range row panics rather than returning an error. Both
// callers guarantee it, one by clamping to the shorter of the two columns and
// the other by ranging over the column it reads; TestHasSlowTrace_RaggedColumns
// pins the clamp, because losing it would turn a malformed block into a panic
// inside a merge rather than a fail-open keep.
func scalarInt64(col *sdk.TagColumn, row int, typeName string) (int64, bool, error) {
	raw := col.Values[row]
	if raw == nil {
		return 0, false, nil
	}
	if len(raw) != 8 {
		return 0, false, fmt.Errorf("%s: expected 8 bytes, got %d", typeName, len(raw))
	}
	return convert.BytesToInt64(raw), true, nil
}

func (s *Sampler) hasSlowTrace(b *sdk.TraceBlock) (bool, error) {
	startCol := b.Tag(s.startTimeTag)
	durCol := b.Tag(s.durationTag)
	if startCol == nil || durCol == nil {
		return false, errNoDurationEnvelope
	}
	rows := len(durCol.Values)
	if len(startCol.Values) < rows {
		rows = len(startCol.Values)
	}
	// The fast path is gated NARROWLY on both columns carrying exactly the type
	// this rule accepts, rather than hoisting the per-row type checks below out
	// of the loop. The checks look hoistable — ValueType is a property of the
	// COLUMN, not the row — but a wrong-typed column can still hold a malformed
	// cell that DecodeTagValue rejects on length (Float64 and Timestamp are both
	// 8-byte-checked), and that error currently propagates. Skipping such a row
	// on type alone would swallow it. Anything not matching both expected types
	// keeps the original At-based loop, so every edge case stays bit-identical.
	fastScalars := startCol.ValueType == valuetype.ValueTypeTimestamp && durCol.ValueType == valuetype.ValueTypeInt64
	var minStart, maxEnd int64
	haveStart, haveEnd := false, false
	var startValue, durationValue sdk.Value
	for row := 0; row < rows; row++ {

		if fastScalars {
			start, startOK, startErr := scalarInt64(startCol, row, "timestamp")
			if startErr != nil {
				return false, startErr
			}
			dur, durOK, durErr := scalarInt64(durCol, row, "int64")
			if durErr != nil {
				return false, durErr
			}
			if !startOK {
				continue
			}
			if !haveStart || start < minStart {
				minStart, haveStart = start, true
			}
			if !durOK {
				continue
			}
			if end := start + dur*s.durationTagNanosPerUnit; !haveEnd || end > maxEnd {
				maxEnd, haveEnd = end, true
			}
			continue
		}
		if sErr := startCol.AtInto(row, &startValue); sErr != nil {
			return false, sErr
		}
		if durationErr := durCol.AtInto(row, &durationValue); durationErr != nil {
			return false, durationErr
		}
		// StartTimeTag must be a timestamp column, which BanyanDB stores as unix ns
		// (write_standalone.go: GetTimestamp().AsTime().UnixNano()) — that is what makes
		// reading it as ns correct. A plain Int64 start time is deliberately NOT accepted:
		// it carries no unit, and unlike DurationTag there is no nanos-per-unit to scale
		// it by, so treating it as ns would silently mis-measure a millisecond column.
		// Skipping the row instead makes it a can't-tell, which fails open.
		if startValue.IsNull() || startValue.ValueType() != valuetype.ValueTypeTimestamp {
			continue
		}
		// A start with no duration still bounds the envelope's LEFT edge. Zipkin's
		// duration is optional (incomplete and one-way spans carry none), so requiring
		// both would let the earliest span drop out of minStart and measure the trace
		// from a later one — understating the envelope and under-keeping slow traces.
		start := startValue.Int64()
		if !haveStart || start < minStart {
			minStart, haveStart = start, true
		}
		// Duration is an int in the tag's own unit, scaled to ns here.
		if durationValue.IsNull() || durationValue.ValueType() != valuetype.ValueTypeInt64 {
			continue
		}
		if end := start + durationValue.Int64()*s.durationTagNanosPerUnit; !haveEnd || end > maxEnd {
			maxEnd, haveEnd = end, true
		}
	}
	// Both edges are required: without any duration the trace's end is unknown, which
	// is a can't-tell rather than "not slow".
	if !haveStart || !haveEnd {
		return false, errNoDurationEnvelope
	}
	return maxEnd-minStart >= s.durationThresholdMs*nanosPerMillis, nil
}

// entriesSlicePool reuses the result slice of arrayEntries across calls so the
// per-trace make([]string, 0, ...) is paid once, not on every Decide call.
// Bounded by aggregate capacity: each retained slice's cap is counted at 8 bytes
// per string header, so a 64 KiB cap supports ~8000 cap-1 slices or ~340 cap-24
// slices (the realistic 3-row × 8-entry trace). The pool is checked-out at
// arrayEntries entry and released by decideTrace once matchEntries is done with
// it; the call site owns the slice for the lifetime of the rule evaluation.
//
// Bounded[T] requires T to be comparable, so the pool element is *[]string
// rather than []string: the slice header pointer is comparable and pooling the
// pointer avoids copying the slice header on every checkout/release. The
// dereferenced slice is what the caller appends into.
var entriesSlicePool = pool.RegisterBounded[*[]string](
	"sw-trace-sampler-entries",
	64*1024,
	func() *[]string { s := []string(nil); return &s },
	func(s *[]string) int64 { return int64(cap(*s)) * 8 },
)

// copyBufPool reuses the per-row copy buffer that arrayEntries sizes to the
// longest row of the column. Each call grows the buffer on demand (append
// doubles), so the steady-state cap converges to the longest row the schema
// produces; for the realistic 3-row × 8-entry trace that lands in the low
// hundreds of bytes. The 16 KiB cap is well above what fits a single SkyWalking
// or Zipkin trace row while keeping the aggregate footprint negligible.
var copyBufPool = pool.RegisterBounded[*[]byte](
	"sw-trace-sampler-copy-buf",
	16*1024,
	func() *[]byte { b := []byte(nil); return &b },
	func(b *[]byte) int64 { return int64(cap(*b)) },
)

// stableBufPool backs the cumulative decoded-entry bytes that the per-row
// strings alias via convert.BytesToString. The strings live across rows (the
// caller consumes them after the whole column is decoded), so the backing must
// not be reset between rows — only appended to. Each call grows the buffer
// once on demand; for the realistic 3-row × 8-entry trace that lands near 500
// bytes, well within the 16 KiB cap. The pool element is *[]byte for the same
// reason as entriesSlicePool: Bounded[T] requires T comparable and pooling the
// pointer avoids copying the slice header on every checkout/release.
var stableBufPool = pool.RegisterBounded[*[]byte](
	"sw-trace-sampler-stable-buf",
	16*1024,
	func() *[]byte { b := []byte(nil); return &b },
	func(b *[]byte) int64 { return int64(cap(*b)) },
)

// acquireEntries returns a pooled slice pointer with length zero. The cap
// reflects the largest slice ever returned to the pool, so a steady-state trace
// acquires a slice that fits without growing. releaseEntries is the symmetric
// counterpart.
func acquireEntries() *[]string {
	p := entriesSlicePool.Get()
	*p = (*p)[:0]
	return p
}

// releaseEntries returns the slice pointer to the pool with length reset to
// zero so the next caller does not see stale entries from the prior decode. nil
// pointers and zero-cap slices are dropped on the floor rather than stored,
// since they carry no backing array to reuse.
func releaseEntries(p *[]string) {
	if p == nil || cap(*p) == 0 {
		return
	}
	*p = (*p)[:0]
	entriesSlicePool.Put(p)
}

// acquireCopyBuf returns a pooled byte slice pointer with length zero. The
// caller is expected to grow the slice via append; releaseCopyBuf returns the
// (possibly grown) backing array to the pool.
func acquireCopyBuf() *[]byte {
	b := copyBufPool.Get()
	*b = (*b)[:0]
	return b
}

// releaseCopyBuf returns the byte slice pointer to the pool with length reset
// to zero so the next caller starts from a clean slice. nil pointers and zero-
// cap slices are dropped, since they carry no backing array to reuse.
func releaseCopyBuf(b *[]byte) {
	if b == nil || cap(*b) == 0 {
		return
	}
	*b = (*b)[:0]
	copyBufPool.Put(b)
}

// acquireStableBuf returns a pooled byte slice pointer with length zero. The
// caller grows the slice via append; the backing array stays stable for the
// lifetime of the returned slice, which is what makes convert.BytesToString of
// the appended segments safe. releaseStableBuf is the symmetric counterpart.
func acquireStableBuf() *[]byte {
	b := stableBufPool.Get()
	*b = (*b)[:0]
	return b
}

// releaseStableBuf returns the byte slice pointer to the pool with length
// reset to zero so the next caller starts from a clean slice. nil pointers
// and zero-cap slices are dropped, since they carry no backing array to
// reuse. Callers MUST only release after every convert.BytesToString-derived
// string into the backing array has been consumed — the strings alias it.
func releaseStableBuf(b *[]byte) {
	if b == nil || cap(*b) == 0 {
		return
	}
	*b = (*b)[:0]
	stableBufPool.Put(b)
}

// arrayEntries decodes every entry of the flattened tag array, flattened across
// rows. All tag predicates are existential over rows and entries, so collapsing
// the rows loses nothing — and decoding once is what keeps the in-place string
// array decode from corrupting later reads (see decideTrace).
//
// The returned *[]string is a pool-managed slice. The pointer is what the
// caller stores; passing it by value (not by address) keeps the caller's
// variable on the stack, avoiding the per-call heap allocation that taking
// the address of a []string local would impose. releaseEntries takes the
// pointer by value too.
//
// The second return value is the stable buffer backing the decoded entry
// strings, or nil when no row needed one. Only an ESCAPED row needs it: its
// bytes are unescaped into a scratch copy, so the resulting strings cannot alias
// either that scratch (it is reused per row) or the block (it still holds the
// escaped form), and are instead appended to the stable buffer and aliased via
// convert.BytesToString — skipping the string header allocation that
// string(buf[idx:end]) would impose. The buffer is only ever appended to, never
// reset, so earlier strings stay valid. An escape-free row aliases the block
// directly and needs no buffer at all, which is the common case, so the buffer
// is acquired lazily and a whole column of unescaped rows returns nil here.
//
// The caller MUST releaseStableBuf whatever is returned, and only after the
// strings are no longer read — they alias it.
//
// errInvalidVarArray mirrors the error vararray.UnmarshalVarArray returns for an
// entry with no terminating delimiter. The escape-free split path reports the
// same condition, so it must read identically in logs whichever path decoded
// the row.
var errInvalidVarArray = errors.New("invalid variable array")

func arrayEntries(col *sdk.TagColumn) (*[]string, *[]byte, error) {
	if col == nil {
		// Always return a non-nil pool element so the caller can dereference
		// without a nil check. matchEntries on an empty slice returns false,
		// which matches the pre-pool behavior of "no entries" → no match.
		return acquireEntries(), nil, nil
	}
	// Early exit for a column whose every row is empty: it can yield no entry, so
	// the row loop below would do nothing. This needs no decode — only the raw
	// byte lengths.
	widest := 0
	for _, raw := range col.Values {
		if len(raw) > widest {
			widest = len(raw)
		}
	}
	if widest == 0 {
		return acquireEntries(), nil, nil
	}
	// Acquire the result slice from the pool. The backing array is reused across
	// calls; the length is reset to zero so stale entries from the previous decode
	// never leak into the new one (matchEntries iterates whatever the length says).
	//
	// Ownership note: arrayEntries returns the pool pointer to its caller
	// (decideTrace), which calls releaseEntries after matchEntries no longer
	// references it. The error path here releases locally because the slice
	// never escapes to decideTrace.
	out := acquireEntries()
	// The per-row copy buffer is acquired LAZILY, on the first row that actually
	// carries an escape. A column whose rows are all escape-free never needs one,
	// and the pool checkout/release is not free: releaseCopyBuf's Bounded.Put was
	// 8.7% flat in the tag-rule CPU profile. If its cap is smaller than the row
	// being copied, append allocates a fresh backing array; the pool then sees a
	// larger buffer on release and the steady-state cap converges to the widest
	// escaped row the schema produces.
	// The stable buffer is acquired lazily too, and only an escaped row needs it.
	var (
		bufPtr    *[]byte
		buf       []byte
		stablePtr *[]byte
		stable    []byte
	)
	// failed distinguishes the two owners of the stable buffer: on success the
	// CALLER releases it (the returned strings still alias it), on failure this
	// function does. Either way the grown slice header has to be written back
	// first, or the pool takes back the pre-append header and the growth is lost
	// — so both the persist and the release live here rather than at each of the
	// error sites, where the persist was previously missed.
	failed := false
	defer func() {
		if bufPtr != nil {
			// Persist any growth so the next caller sees the wider cap.
			*bufPtr = buf
			releaseCopyBuf(bufPtr)
		}
		if stablePtr != nil {
			*stablePtr = stable
			if failed {
				releaseStableBuf(stablePtr)
			}
		}
	}()
	for row := range col.Values {
		if col.Values[row] == nil {
			continue
		}
		// A row with no escape byte is decoded straight out of the block, with the
		// entries aliasing it — no row copy, no stable-buffer append.
		//
		// Two facts make that safe, and BOTH are required:
		//
		//  1. No write. vararray.UnmarshalVarArray only shifts bytes when it
		//     consumes an escape; with none present it does not touch the buffer,
		//     so the corruption hazard described below cannot arise.
		//  2. No recycling. assembleTraceBlock (banyand/trace/pipeline_chain.go)
		//     deep-copies every value into block-owned storage precisely "so the
		//     merge loop may recycle/overwrite those buffers while an abandoned
		//     Decide goroutine still reads the block". The bytes therefore outlive
		//     this call even on the timeout path, so aliasing them cannot observe
		//     recycled memory.
		//
		// The escape check is per row and must stay that way: it is a property of
		// the DATA, not of the column or the schema.
		if raw := col.Values[row]; bytes.IndexByte(raw, vararray.Escape) < 0 {
			// The row is known escape-free, so entries are exactly the
			// delimiter-separated runs and UnmarshalVarArray's own escape scan
			// would be redundant work on every entry — it was 34.6% flat in the
			// tag-rule profile. Split on the delimiter directly instead. This is
			// the ONE place that may assume the encoding, and only because the
			// check above proves no escape can be present: with an escape, a
			// delimiter byte may be escaped and this loop would split mid-value.
			idx := 0
			for idx < len(raw) {
				rel := bytes.IndexByte(raw[idx:], vararray.EntityDelimiter)
				if rel < 0 {
					// Same condition, and the same message, that
					// UnmarshalVarArray reports for an unterminated entry.
					releaseEntries(out)
					failed = true
					return nil, nil, fmt.Errorf("str array: %w", errInvalidVarArray)
				}
				*out = append(*out, convert.BytesToString(raw[idx:idx+rel]))
				idx += rel + 1
			}
			continue
		}
		// Escaped row: decode a COPY, never the block's buffer. vararray.UnmarshalVarArray
		// shifts bytes left past every escape in place, TraceBlock slices are documented
		// read-only, and the engine hands the SAME TraceBatch to every link of a chain
		// (sdk.applyChainLink). Decoding in place would therefore corrupt the value for
		// every later link, making a rule's verdict depend on its position in the chain.
		// Only values containing "|" or "\" carry an escape, so the damage is silent and
		// data-dependent.
		//
		// The decoded entries cannot alias buf: buf is reused across rows
		// (append(buf[:0], ...)) and is mutated by the in-place decode above. The
		// strings produced below therefore append the decoded bytes into stable
		// (which is only ever appended to, never reset, so prior bytes stay valid)
		// and then alias stable via convert.BytesToString — skipping the per-entry
		// string-header allocation that string(buf[idx:end]) would impose.
		if bufPtr == nil {
			bufPtr = acquireCopyBuf()
			buf = *bufPtr
			stablePtr = acquireStableBuf()
			stable = *stablePtr
		}
		buf = append(buf[:0], col.Values[row]...)
		idx := 0
		for idx < len(buf) {
			end, next, err := vararray.UnmarshalVarArray(buf, idx)
			if err != nil {
				releaseEntries(out)
				failed = true
				return nil, nil, fmt.Errorf("str array: %w", err)
			}
			start := len(stable)
			stable = append(stable, buf[idx:end]...)
			*out = append(*out, convert.BytesToString(stable[start:]))
			idx = next
		}
	}
	// Both pointers stay nil when no row carried an escape, which is the common
	// case; the deferred block above persists whatever growth did happen.
	return out, stablePtr, nil
}

// errNoErrorColumn reports that the schema's error column is absent from the block.
// decideTrace treats any error as a keep, which is the intended reading: the column is
// schema-declared, so its absence means the block was written under a different
// schema — typically the wrong plugin attached to the group — and answering "no
// error" there would drop every trace keepErrors was enabled to save. This mirrors
// errNoDurationEnvelope; the two sure-keep rules must fail the same way.
//
// A column that is PRESENT but null on a row is different and stays fail-closed: the
// duration envelope is a measurement, so an absent one cannot be judged, whereas
// is_error is a flag, and an unset flag legitimately means "not an error".
var errNoErrorColumn = errors.New("the schema's error column is absent from this block")

// hasErrorColumn reports whether a dedicated error column is truthy on any row.
func (s *Sampler) hasErrorColumn(b *sdk.TraceBlock) (bool, error) {
	col := b.Tag(s.errorTag)
	if col == nil {
		return false, errNoErrorColumn
	}

	// Both fast paths are gated on the column's exact type, for the same reason
	// as hasSlowTrace: only a type this rule actually reads may skip At.
	switch col.ValueType {
	case valuetype.ValueTypeInt64:
		for row := range col.Values {
			v, ok, err := scalarInt64(col, row, "int64")
			if err != nil {
				return false, err
			}
			if ok && v != 0 {
				return true, nil
			}
		}
		return false, nil
	case valuetype.ValueTypeStr:
		// DecodeTagValue would allocate a string per row via string(raw) purely
		// to compare it against two literals. Aliasing the bytes compares the
		// same characters with no allocation; the strings never outlive the loop.
		for row := range col.Values {
			raw := col.Values[row]
			if raw == nil {
				continue
			}
			if str := convert.BytesToString(raw); str == "true" || str == "1" {
				return true, nil
			}
		}
		return false, nil
	default:
	}
	var value sdk.Value
	for row := range col.Values {
		if decodeErr := col.AtInto(row, &value); decodeErr != nil {
			return false, decodeErr
		}
		if value.IsNull() {
			continue
		}
		switch value.ValueType() {
		case valuetype.ValueTypeInt64:
			if value.Int64() != 0 {
				return true, nil
			}
		case valuetype.ValueTypeStr:
			if str := value.Str(); str == "true" || str == "1" {
				return true, nil
			}
		default:
			// No other type carries an error signal this schema understands.
		}
	}
	return false, nil
}

// matchEntries reports whether the rule matches any decoded entry. A tagKey naming
// the array column itself matches raw entries; any other tagKey matches the value
// part of its "tagKey=" entries.
//
// An exists rule additionally accepts the BARE key, because the array holds a tag
// as both "key" and "key=value" (and Zipkin annotations appear as raw values with
// no "="). The comparison is exact, so a longer key such as "error_rate=0" never
// satisfies an exists rule on "error". keepErrors routes through here too, so the
// two cannot drift apart.
func matchEntries(entries []string, r *rule) bool {
	prefix := r.prefix
	if prefix == "" {
		// A rule naming the array column itself matches the whole entry, so there
		// is no prefix to strip and no bare-key case distinct from it.
		for _, entry := range entries {
			if r.Exists || matchValue(r, entry) {
				return true
			}
		}
		return false
	}
	// Most entries belong to some other tag, so the common outcome is rejection.
	// Reject on length and first byte before HasPrefix's memequal call. The bare-key
	// test must stay ahead of that guard: prefix is TagKey+"=", so a bare key is one
	// byte too short and the length check would skip it.
	first, plen := prefix[0], len(prefix)
	for _, entry := range entries {
		if r.Exists && entry == r.TagKey {
			return true
		}
		if len(entry) < plen || entry[0] != first {
			continue
		}
		if !strings.HasPrefix(entry, prefix) {
			continue
		}
		if r.Exists || matchValue(r, entry[plen:]) {
			return true
		}
	}
	return false
}

// matchValue applies the rule's active string matcher (equals, then in, then
// regex) to a candidate. The exists matcher is handled by the caller, which
// knows whether the value or entry is present.
func matchValue(r *rule, candidate string) bool {
	switch {
	case r.Equals != "":
		return candidate == r.Equals
	case len(r.In) > 0:
		for _, want := range r.In {
			if candidate == want {
				return true
			}
		}
		return false
	case r.re != nil:
		return r.re.MatchString(candidate)
	default:
		return false
	}
}

// sampleFraction maps a trace_id to a stable fraction in [0,1) via FNV-1a, so
// the keep decision is deterministic and reproducible across passes. The top 53
// bits fill a float64 mantissa exactly (the technique math/rand uses), so the
// result is strictly below 1 and a healthySampleRate of 1.0 keeps every trace.
func sampleFraction(traceID string) float64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(traceID))
	return float64(h.Sum64()>>11) / (1 << 53)
}
