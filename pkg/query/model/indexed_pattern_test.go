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
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/index"
)

func TestIndexedPatternEligibility(t *testing.T) {
	measure := &databasev1.Measure{TagFamilies: []*databasev1.TagFamilySpec{{
		Name: "default", Tags: []*databasev1.TagSpec{{Name: "name", Type: databasev1.TagType_TAG_TYPE_STRING}},
	}}}
	rule := &databasev1.IndexRule{Metadata: &commonv1.Metadata{Name: "names", Id: 7}, Tags: []string{"name"}, Type: databasev1.IndexRule_TYPE_INVERTED}
	pattern := IndexedPattern{TagFamily: "default", TagName: "name", Pattern: "%a%", Escape: "\\"}
	for _, analyzer := range []string{"", "keyword"} {
		rule.Analyzer = analyzer
		requests, err := ResolveIndexedPatterns(measure, []*databasev1.IndexRule{rule}, []IndexedPattern{pattern})
		require.NoError(t, err)
		key := index.FieldKey{IndexRuleID: 7}
		require.Equal(t, key.Marshal(), requests[0].Field)
	}
	for _, analyzer := range []string{"simple", "standard", "url"} {
		rule.Analyzer = analyzer
		require.Error(t, ValidateIndexedPatterns(measure, []*databasev1.IndexRule{rule}, []IndexedPattern{pattern}))
	}
	rule.Analyzer = ""
	require.Error(t, ValidateIndexedPatterns(measure, nil, []IndexedPattern{pattern}))
	pattern.TagFamily = "wrong"
	require.Error(t, ValidateIndexedPatterns(measure, []*databasev1.IndexRule{rule}, []IndexedPattern{pattern}))
	pattern.TagFamily = "default"
	duplicate := proto.Clone(measure).(*databasev1.Measure)
	duplicate.TagFamilies = append(duplicate.TagFamilies, &databasev1.TagFamilySpec{
		Name: "other", Tags: []*databasev1.TagSpec{{Name: "name", Type: databasev1.TagType_TAG_TYPE_STRING}},
	})
	require.Error(t, ValidateIndexedPatterns(duplicate, []*databasev1.IndexRule{rule}, []IndexedPattern{pattern}))
	measure.TagFamilies[0].Tags[0].Type = databasev1.TagType_TAG_TYPE_STRING_ARRAY
	require.Error(t, ValidateIndexedPatterns(measure, []*databasev1.IndexRule{rule}, []IndexedPattern{pattern}))
}

func TestIndexedPatternsRejectAmbiguousCoveringRules(t *testing.T) {
	measure := &databasev1.Measure{TagFamilies: []*databasev1.TagFamilySpec{{
		Name: "default", Tags: []*databasev1.TagSpec{
			{Name: "name", Type: databasev1.TagType_TAG_TYPE_STRING},
			{Name: "other", Type: databasev1.TagType_TAG_TYPE_STRING},
		},
	}}}
	keyword := &databasev1.IndexRule{
		Metadata: &commonv1.Metadata{Group: "metrics", Name: "names", Id: 9},
		Tags:     []string{"name"}, Type: databasev1.IndexRule_TYPE_INVERTED, Analyzer: "keyword",
	}
	pattern := IndexedPattern{TagFamily: "default", TagName: "name", Pattern: "%a%", Escape: "\\"}
	for _, test := range []struct {
		name     string
		analyzer string
		tags     []string
		typeOf   databasev1.IndexRule_Type
	}{
		{name: "two keyword indexes", analyzer: "keyword", tags: []string{"name"}, typeOf: databasev1.IndexRule_TYPE_INVERTED},
		{name: "tokenized index", analyzer: "standard", tags: []string{"name"}, typeOf: databasev1.IndexRule_TYPE_INVERTED},
		{name: "multi-tag index", analyzer: "keyword", tags: []string{"name", "other"}, typeOf: databasev1.IndexRule_TYPE_INVERTED},
		{name: "unsupported index type", tags: []string{"name"}, typeOf: databasev1.IndexRule_TYPE_TREE},
	} {
		t.Run(test.name, func(t *testing.T) {
			other := &databasev1.IndexRule{
				Metadata: &commonv1.Metadata{Group: "metrics", Name: "other_names", Id: 1},
				Tags:     test.tags, Type: test.typeOf, Analyzer: test.analyzer,
			}
			for _, rules := range [][]*databasev1.IndexRule{{keyword, other}, {other, keyword}} {
				requests, err := ResolveIndexedPatterns(measure, rules, []IndexedPattern{pattern})
				require.ErrorContains(t, err, "multiple active index rules")
				require.Nil(t, requests)
			}
		})
	}
}

func TestIndexedPatternsDuplicateBindings(t *testing.T) {
	measure := &databasev1.Measure{TagFamilies: []*databasev1.TagFamilySpec{{
		Name: "default", Tags: []*databasev1.TagSpec{{Name: "name", Type: databasev1.TagType_TAG_TYPE_STRING}},
	}}}
	rule := &databasev1.IndexRule{
		Metadata: &commonv1.Metadata{Group: "metrics", Name: "names", Id: 9},
		Tags:     []string{"name"}, Type: databasev1.IndexRule_TYPE_INVERTED,
	}
	pattern := IndexedPattern{TagName: "name", Pattern: "%", Escape: "\\"}
	for _, duplicate := range []*databasev1.IndexRule{rule, proto.Clone(rule).(*databasev1.IndexRule)} {
		requests, err := ResolveIndexedPatterns(measure, []*databasev1.IndexRule{rule, duplicate}, []IndexedPattern{pattern})
		require.NoError(t, err)
		require.Len(t, requests, 1)
		key := index.FieldKey{IndexRuleID: 9}
		require.Equal(t, key.Marshal(), requests[0].Field)
	}
	for _, edit := range []func(*databasev1.IndexRule){
		func(other *databasev1.IndexRule) { other.Metadata = nil },
		func(other *databasev1.IndexRule) { other.Metadata.Group = "" },
		func(other *databasev1.IndexRule) { other.Metadata.Name = "" },
		func(other *databasev1.IndexRule) { other.Metadata.Id = 0 },
	} {
		incomplete := proto.Clone(rule).(*databasev1.IndexRule)
		edit(incomplete)
		duplicate := proto.Clone(incomplete).(*databasev1.IndexRule)
		require.ErrorContains(t, ValidateIndexedPatterns(measure, []*databasev1.IndexRule{incomplete, duplicate}, []IndexedPattern{pattern}),
			"multiple active index rules")
	}
	conflict := proto.Clone(rule).(*databasev1.IndexRule)
	conflict.Analyzer = "standard"
	require.ErrorContains(t, ValidateIndexedPatterns(measure, []*databasev1.IndexRule{rule, conflict}, []IndexedPattern{pattern}),
		"multiple active index rules")
}

func TestIndexedPatternsIgnoreRulesReferencingRemovedTags(t *testing.T) {
	measure := &databasev1.Measure{TagFamilies: []*databasev1.TagFamilySpec{{
		Name: "default", Tags: []*databasev1.TagSpec{{Name: "name", Type: databasev1.TagType_TAG_TYPE_STRING}},
	}}}
	rule := &databasev1.IndexRule{
		Metadata: &commonv1.Metadata{Group: "metrics", Name: "names", Id: 9},
		Tags:     []string{"name"}, Type: databasev1.IndexRule_TYPE_INVERTED,
	}
	removed := &databasev1.IndexRule{
		Metadata: &commonv1.Metadata{Group: "metrics", Name: "removed", Id: 1},
		Tags:     []string{"name", "removed"}, Type: databasev1.IndexRule_TYPE_INVERTED,
	}
	pattern := IndexedPattern{TagName: "name", Pattern: "%", Escape: "\\"}
	for _, rules := range [][]*databasev1.IndexRule{{nil, removed, rule}, {rule, removed, nil}} {
		requests, err := ResolveIndexedPatterns(measure, rules, []IndexedPattern{pattern})
		require.NoError(t, err)
		key := index.FieldKey{IndexRuleID: 9}
		require.Equal(t, key.Marshal(), requests[0].Field)
	}
	require.ErrorContains(t, ValidateIndexedPatterns(measure, []*databasev1.IndexRule{removed}, []IndexedPattern{pattern}),
		"requires an active single-tag whole-value inverted index")
}
