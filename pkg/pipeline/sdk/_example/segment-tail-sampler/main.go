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

// Command segment-tail-sampler is the reference post-trace sampler plugin from
// docs/design/post-trace-pipeline.md §6.1. It is a worked example of the
// pkg/pipeline/sdk contract and illustrates the three things a real plugin
// must do:
//
//  1. Parse the operator-supplied config. SamplerPlugin.config is a
//     google.protobuf.Struct; the engine serializes it to canonical JSON and
//     hands it to NewSampler as bytes, which this plugin unmarshals into its
//     own typed config.
//  2. Declare a projection (Project) so the engine materializes only the
//     columns the verdict reads — here a handful of tag columns, plus the
//     span-id column only when a span-count rule is configured.
//  3. Extract tags and spans from the vectorized batch in Decide — decoding tag
//     values by their ValueType and reading the span-id column.
//
// Build it as a Go plugin (it is deliberately under an _example directory so
// `go build ./...` and the linters skip it):
//
//	go build -buildmode=plugin -trimpath \
//	  -o segment-tail-sampler.so \
//	  ./pkg/pipeline/sdk/_example/segment-tail-sampler
//
// It must be built with the same Go toolchain and the same pinned
// pkg/pipeline/sdk as the running data node (see §2.5).
package main

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"regexp"
	"sort"
	"strconv"
	"time"

	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk"
)

// ABIVersion re-exports the SDK ABI version. The engine refuses to load the
// plugin unless this equals its own compiled sdk.ABIVersion.
var ABIVersion = sdk.ABIVersion

// tagRule is one sure-keep tag predicate. Exactly one matcher field is honored,
// checked in the order exists, equals, in, regex.
type tagRule struct {
	re     *regexp.Regexp
	In     []string `json:"in"`
	Regex  string   `json:"regex"`
	TagKey string   `json:"tag_key"`
	Equals string   `json:"equals"`
	Exists bool     `json:"exists"`
}

// config is the JSON shape the operator sets in SamplerPlugin.config.
type config struct {
	DurationThreshold string    `json:"duration_threshold"`
	ErrorTag          string    `json:"error_tag"`
	KeepTagRules      []tagRule `json:"keep_tag_rules"`
	HealthySampleRate float64   `json:"healthy_sample_rate"`
	MinSpanCount      int       `json:"min_span_count"`
	KeepErrors        bool      `json:"keep_errors"`
}

// segmentTailSampler keeps a trace when any sure-keep rule matches, and
// otherwise admits a deterministic fraction of the healthy remainder.
type segmentTailSampler struct {
	errorTag          string
	rules             []tagRule
	requiredTags      []string
	durationThreshold time.Duration
	healthySampleRate float64
	minSpanCount      int
	keepErrors        bool
	wantSpanIDs       bool
}

// NewSampler is the constructor symbol the engine looks up. It parses and
// validates the operator config, compiles any regex matchers, and computes the
// projection. A returned error rejects the plugin at admission.
func NewSampler(configJSON []byte) (sdk.Sampler, error) {
	var c config
	if len(configJSON) > 0 {
		if err := json.Unmarshal(configJSON, &c); err != nil {
			return nil, fmt.Errorf("segment-tail-sampler: invalid config JSON: %w", err)
		}
	}
	if c.HealthySampleRate < 0 || c.HealthySampleRate > 1 {
		return nil, fmt.Errorf("segment-tail-sampler: healthy_sample_rate %v out of [0,1]", c.HealthySampleRate)
	}
	s := &segmentTailSampler{
		rules:             c.KeepTagRules,
		healthySampleRate: c.HealthySampleRate,
		keepErrors:        c.KeepErrors,
		errorTag:          c.ErrorTag,
		minSpanCount:      c.MinSpanCount,
	}
	if s.errorTag == "" {
		s.errorTag = "is_error"
	}
	if c.DurationThreshold != "" {
		d, err := time.ParseDuration(c.DurationThreshold)
		if err != nil {
			return nil, fmt.Errorf("segment-tail-sampler: invalid duration_threshold %q: %w", c.DurationThreshold, err)
		}
		if d <= 0 {
			return nil, fmt.Errorf("segment-tail-sampler: duration_threshold must be positive, got %v", d)
		}
		s.durationThreshold = d
	}

	// Build the projection: the error tag (when keep_errors is set) and every
	// rule's tag key. Compile regex matchers once, here, not per batch.
	tagSet := make(map[string]struct{})
	if s.keepErrors {
		tagSet[s.errorTag] = struct{}{}
	}
	for i := range s.rules {
		if s.rules[i].TagKey == "" {
			return nil, fmt.Errorf("segment-tail-sampler: keep_tag_rules[%d] has empty tag_key", i)
		}
		if s.rules[i].Regex != "" {
			re, err := regexp.Compile(s.rules[i].Regex)
			if err != nil {
				return nil, fmt.Errorf("segment-tail-sampler: keep_tag_rules[%d] bad regex %q: %w", i, s.rules[i].Regex, err)
			}
			s.rules[i].re = re
		}
		tagSet[s.rules[i].TagKey] = struct{}{}
	}
	s.requiredTags = make([]string, 0, len(tagSet))
	for k := range tagSet {
		s.requiredTags = append(s.requiredTags, k)
	}
	// Stable order keeps Project() reproducible across runs (Go map iteration is
	// randomized); the engine treats Tags as a set, but logs, tests, and caches
	// benefit from determinism.
	sort.Strings(s.requiredTags)
	// A span-count rule reads the span-id column, so project it on demand.
	s.wantSpanIDs = s.minSpanCount > 0
	return s, nil
}

// Kind reports the sampler kind, satisfying the generic sdk.Plugin interface
// that sdk.Sampler embeds.
func (s *segmentTailSampler) Kind() sdk.Kind { return sdk.KindSampler }

// Project declares the columns the verdict reads: the rule/error tag columns,
// plus the span-id column only when a span-count rule is configured. Span
// bodies are never read, so Spans stays false and the merge raw fast path is
// preserved.
func (s *segmentTailSampler) Project() sdk.Projection {
	return sdk.Projection{Tags: s.requiredTags, SpanIDs: s.wantSpanIDs}
}

// Decide returns a keep-mask aligned to batch.Traces. The batch is read-only.
func (s *segmentTailSampler) Decide(batch *sdk.TraceBatch) (sdk.Verdict, error) {
	keep := make([]bool, len(batch.Traces))
	for i := range batch.Traces {
		k, err := s.keepTrace(&batch.Traces[i])
		if err != nil {
			return sdk.Verdict{}, err
		}
		keep[i] = k
	}
	return sdk.Verdict{Keep: keep}, nil
}

// Close releases resources; this sampler holds none.
func (s *segmentTailSampler) Close() error { return nil }

// keepTrace applies the sure-keep rules, then the deterministic healthy sample.
func (s *segmentTailSampler) keepTrace(b *sdk.TraceBlock) (bool, error) {
	// Duration is free from the intrinsic MinTS/MaxTS — no decode.
	if s.durationThreshold > 0 && time.Duration(b.MaxTS-b.MinTS) >= s.durationThreshold {
		return true, nil
	}
	// Error keep: decode the error tag column and look for any truthy row.
	if s.keepErrors {
		hit, err := s.hasError(b)
		if err != nil {
			return false, err
		}
		if hit {
			return true, nil
		}
	}
	// Sure-keep tag rules.
	for i := range s.rules {
		hit, err := matchRule(b, &s.rules[i])
		if err != nil {
			return false, err
		}
		if hit {
			return true, nil
		}
	}
	// Span-count rule: read the span-id column the projection requested.
	if s.minSpanCount > 0 && b.Len() >= s.minSpanCount {
		return true, nil
	}
	// Healthy remainder: deterministic hash(trace_id) < rate, stable across
	// re-evaluation at merge and finalization.
	if s.healthySampleRate > 0 && sampleFraction(b.TraceID) < s.healthySampleRate {
		return true, nil
	}
	return false, nil
}

// hasError reports whether the error tag is truthy on any span row.
func (s *segmentTailSampler) hasError(b *sdk.TraceBlock) (bool, error) {
	col := b.Tag(s.errorTag)
	if col == nil {
		return false, nil
	}
	for row := range col.Values {
		v, err := col.At(row)
		if err != nil {
			return false, err
		}
		if v.IsNull() {
			continue
		}
		switch v.ValueType() {
		case pbv1.ValueTypeInt64:
			if v.Int64() != 0 {
				return true, nil
			}
		case pbv1.ValueTypeStr:
			if str := v.Str(); str == "true" || str == "1" {
				return true, nil
			}
		default:
		}
	}
	return false, nil
}

// matchRule reports whether the rule matches any span row of the trace.
func matchRule(b *sdk.TraceBlock, r *tagRule) (bool, error) {
	col := b.Tag(r.TagKey)
	if col == nil {
		return false, nil
	}
	for row := range col.Values {
		v, err := col.At(row)
		if err != nil {
			return false, err
		}
		if v.IsNull() {
			continue
		}
		if r.Exists {
			return true, nil
		}
		str := stringOf(v)
		switch {
		case r.Equals != "":
			if str == r.Equals {
				return true, nil
			}
		case len(r.In) > 0:
			for _, candidate := range r.In {
				if str == candidate {
					return true, nil
				}
			}
		case r.re != nil:
			if r.re.MatchString(str) {
				return true, nil
			}
		}
	}
	return false, nil
}

// stringOf renders a decoded value as a string for matching against the rule's
// string predicates.
func stringOf(v sdk.Value) string {
	switch v.ValueType() {
	case pbv1.ValueTypeStr:
		return v.Str()
	case pbv1.ValueTypeInt64, pbv1.ValueTypeTimestamp:
		return strconv.FormatInt(v.Int64(), 10)
	case pbv1.ValueTypeFloat64:
		return strconv.FormatFloat(v.Float64(), 'g', -1, 64)
	case pbv1.ValueTypeBinaryData:
		return string(v.Bytes())
	default:
		return ""
	}
}

// sampleFraction maps a trace_id to a stable fraction in [0,1) via FNV-1a, so
// the keep decision is deterministic and reproducible across passes. The top 53
// bits fill a float64 mantissa exactly (the technique math/rand uses), so the
// result is strictly below 1 and a healthy_sample_rate of 1.0 keeps every trace.
func sampleFraction(traceID string) float64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(traceID))
	return float64(h.Sum64()>>11) / (1 << 53)
}

func main() {}
