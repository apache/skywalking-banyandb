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

package model

import (
	"fmt"
	"slices"
	"unicode/utf8"

	"google.golang.org/protobuf/proto"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/native"
)

// MaxIndexedPatterns bounds the number of pattern conjuncts in one source scan.
const MaxIndexedPatterns = native.MaxSQLPatterns

// MaxIndexedPatternIdentityBytes bounds each source tag and family name.
const MaxIndexedPatternIdentityBytes = 255

// IndexedPattern is a bound source-WHERE LIKE or ILIKE predicate. Escape is
// explicit: backslash is the language default and an empty string disables it.
type IndexedPattern struct {
	TagFamily  string
	TagName    string
	Pattern    string
	Escape     string
	IgnoreCase bool
}

// Validate checks the wire-independent pattern shape without opening storage.
func (p IndexedPattern) Validate() error {
	if p.TagName == "" || len(p.TagName) > MaxIndexedPatternIdentityBytes || len(p.TagFamily) > MaxIndexedPatternIdentityBytes ||
		!utf8.ValidString(p.TagName) || !utf8.ValidString(p.TagFamily) {
		return fmt.Errorf("LIKE requires a valid source tag identity")
	}
	return native.ValidateWildcardPattern(native.WildcardPattern{
		Pattern: p.Pattern, Escape: p.Escape, IgnoreCase: p.IgnoreCase, Syntax: native.WildcardSyntaxSQL,
	})
}

// ResolveIndexedPatterns validates live schemas and bindings and requires an
// unambiguous whole-value index rule on coordinator and workers.
func ResolveIndexedPatterns(measure *databasev1.Measure, rules []*databasev1.IndexRule, patterns []IndexedPattern) ([]native.TermSetRequest, error) {
	if len(patterns) > MaxIndexedPatterns {
		return nil, fmt.Errorf("too many LIKE predicates: %w", native.ErrQueryLimit)
	}
	availableTags := make(map[string]bool)
	for _, family := range measure.GetTagFamilies() {
		for _, tag := range family.GetTags() {
			availableTags[tag.GetName()] = true
		}
	}
	requests := make([]native.TermSetRequest, 0, len(patterns))
	for _, pattern := range patterns {
		if err := pattern.Validate(); err != nil {
			return nil, err
		}
		var found *databasev1.TagSpec
		var family string
		count := 0
		for _, tf := range measure.GetTagFamilies() {
			for _, tag := range tf.GetTags() {
				if tag.GetName() == pattern.TagName {
					found = tag
					family = tf.GetName()
					count++
				}
			}
		}
		if count != 1 || (pattern.TagFamily != "" && family != pattern.TagFamily) {
			return nil, fmt.Errorf("LIKE tag %s.%s is missing or ambiguous", pattern.TagFamily, pattern.TagName)
		}
		if found.GetType() != databasev1.TagType_TAG_TYPE_STRING {
			return nil, fmt.Errorf("LIKE tag %s must be a scalar STRING", pattern.TagName)
		}
		var selected *databasev1.IndexRule
		for _, rule := range rules {
			if !slices.Contains(rule.GetTags(), pattern.TagName) {
				continue
			}
			// Writers discard rules containing removed tags, then use the first
			// covering rule. Reject competing valid rules regardless of their
			// eligibility because binding order can differ across nodes.
			if slices.ContainsFunc(rule.GetTags(), func(tag string) bool { return !availableTags[tag] }) {
				continue
			}
			if selected != nil {
				if samePatternIndexRule(selected, rule) {
					continue
				}
				return nil, fmt.Errorf("LIKE tag %s has multiple active index rules", pattern.TagName)
			}
			selected = rule
		}
		if selected == nil || selected.GetType() != databasev1.IndexRule_TYPE_INVERTED || len(selected.GetTags()) != 1 ||
			(selected.GetAnalyzer() != "" && selected.GetAnalyzer() != "keyword") {
			return nil, fmt.Errorf("LIKE tag %s requires an active single-tag whole-value inverted index (empty or keyword analyzer)", pattern.TagName)
		}
		field := index.FieldKey{IndexRuleID: selected.GetMetadata().GetId()}
		requests = append(requests, native.TermSetRequest{
			Field: field.Marshal(),
			WildcardPatterns: []native.WildcardPattern{{
				Pattern: pattern.Pattern, Escape: pattern.Escape, IgnoreCase: pattern.IgnoreCase, Syntax: native.WildcardSyntaxSQL,
			}},
		})
	}
	return requests, nil
}

func samePatternIndexRule(left, right *databasev1.IndexRule) bool {
	if left == right {
		return true
	}
	metadata := left.GetMetadata()
	// Repeated bindings may load separate copies of one rule. Incomplete
	// metadata cannot establish identity, even when both definitions match.
	return metadata.GetGroup() != "" && metadata.GetName() != "" && metadata.GetId() != 0 && proto.Equal(left, right)
}

// ValidateIndexedPatterns checks current schema/index eligibility without opening storage.
func ValidateIndexedPatterns(measure *databasev1.Measure, rules []*databasev1.IndexRule, patterns []IndexedPattern) error {
	_, err := ResolveIndexedPatterns(measure, rules, patterns)
	return err
}
