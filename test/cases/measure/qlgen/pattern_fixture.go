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
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

package qlgen

import (
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

// PatternFixtureName identifies the neutral whole-value pattern fixture.
const PatternFixtureName = "ql_patterns"

// PatternFixtureNames includes existing names that remain valid contextual identifiers.
func PatternFixtureNames() []string {
	return []string{PatternFixtureName, "like", "ilike", "escape", "like-rate", "ilike-rate", "escape-group"}
}

func patternFixtureGroup(name string) string {
	if name == PatternFixtureName {
		return FixtureGroup
	}
	return name
}

func patternAmbiguousTags() []string {
	return []string{"keyword_high_first", "keyword_low_first", "standard_first", "keyword_before_standard", "multitag_first", "keyword_before_multitag"}
}

type patternRow struct {
	Name               any
	Category           string
	RID, Millis, Value int64
}

func patternRows() []patternRow {
	names := []any{"Amber-Bird", "amber-bird", "Amber-Boat", "a_b", "a-b", "20%", "a*b", "a?b", "a中b", "a\nb", "", nil, "a\\b", "Σ", "ς"}
	rows := make([]patternRow, 0, len(names)+1)
	for i, name := range names {
		category := "one"
		if i%2 != 0 {
			category = "two"
		}
		rows = append(rows, patternRow{Name: name, Category: category, RID: int64(i + 1), Value: int64(i % 3), Millis: 0})
	}
	rows = append(rows, patternRow{Name: names[0], Category: "one", RID: 1, Value: 4, Millis: 1000})
	return rows
}

// PatternFixtureSchema preserves names and uses a separate identity per series.
func PatternFixtureSchema(name string) *databasev1.Measure {
	schema := ParentChildFixtureSchema(name)
	schema.Metadata.Group = patternFixtureGroup(name)
	schema.Entity.TagNames = []string{"rid"}
	schema.TagFamilies[0].Tags = []*databasev1.TagSpec{
		{Name: "rid", Type: databasev1.TagType_TAG_TYPE_INT},
		{Name: "name", Type: databasev1.TagType_TAG_TYPE_STRING},
		{Name: "plain_name", Type: databasev1.TagType_TAG_TYPE_STRING},
		{Name: "token_name", Type: databasev1.TagType_TAG_TYPE_STRING},
		{Name: "unindexed_name", Type: databasev1.TagType_TAG_TYPE_STRING},
		{Name: "category", Type: databasev1.TagType_TAG_TYPE_STRING},
	}
	if name == PatternFixtureName {
		for _, tag := range append(patternAmbiguousTags(), "ambiguity_extra") {
			schema.TagFamilies[0].Tags = append(schema.TagFamilies[0].Tags, &databasev1.TagSpec{Name: tag, Type: databasev1.TagType_TAG_TYPE_STRING})
		}
	}
	return schema
}

// PatternIndexRules covers explicit keyword, omitted analysis and tokenized data.
func PatternIndexRules(fixture string) []*databasev1.IndexRule {
	var rules []*databasev1.IndexRule
	for _, name := range []string{"rid", "name", "plain_name", "token_name", "category"} {
		analyzer := "keyword"
		if name == "rid" || name == "plain_name" {
			analyzer = ""
		}
		if name == "token_name" {
			analyzer = "simple"
		}
		rules = append(rules, &databasev1.IndexRule{
			Metadata: &commonv1.Metadata{Name: fixture + "_" + name, Group: patternFixtureGroup(fixture)},
			Tags:     []string{name}, Type: databasev1.IndexRule_TYPE_INVERTED, Analyzer: analyzer,
		})
	}
	if fixture == PatternFixtureName {
		for i, tag := range patternAmbiguousTags() {
			first := &databasev1.IndexRule{
				Metadata: &commonv1.Metadata{Name: fixture + "_" + tag + "_first", Group: FixtureGroup, Id: uint32(52000 + i*2)},
				Tags:     []string{tag}, Type: databasev1.IndexRule_TYPE_INVERTED, Analyzer: "keyword",
			}
			second := &databasev1.IndexRule{
				Metadata: &commonv1.Metadata{Name: fixture + "_" + tag + "_second", Group: FixtureGroup, Id: uint32(52001 + i*2)},
				Tags:     []string{tag}, Type: databasev1.IndexRule_TYPE_INVERTED, Analyzer: "keyword",
			}
			if tag == "keyword_high_first" {
				first.Metadata.Id, second.Metadata.Id = second.Metadata.Id, first.Metadata.Id
			}
			other := first
			if strings.HasPrefix(tag, "keyword_before_") {
				other = second
			}
			if strings.Contains(tag, "standard") {
				other.Analyzer = "standard"
			}
			if strings.Contains(tag, "multitag") {
				other.Tags = append(other.Tags, "ambiguity_extra")
			}
			rules = append(rules, first, second)
		}
	}
	return rules
}

// PatternIndexBinding installs every compatible or rejection-control rule.
func PatternIndexBinding(name string) *databasev1.IndexRuleBinding {
	binding := ParentChildIndexBinding(name)
	binding.Metadata.Group = patternFixtureGroup(name)
	binding.Rules = nil
	for _, rule := range PatternIndexRules(name) {
		binding.Rules = append(binding.Rules, rule.Metadata.Name)
	}
	return binding
}

// PatternFixtureWrites repeats one identity across timestamps to exercise distinct counts.
func PatternFixtureWrites(name string, base time.Time) []*measurev1.WriteRequest {
	var writes []*measurev1.WriteRequest
	for i, row := range patternRows() {
		tagNames := []string{"rid", "name", "plain_name", "token_name", "unindexed_name", "category"}
		tags := []*modelv1.TagValue{tagValue(row.RID), tagValue(row.Name), tagValue(row.Name), tagValue(row.Name), tagValue(row.Name), tagValue(row.Category)}
		if name == PatternFixtureName {
			for _, tag := range append(patternAmbiguousTags(), "ambiguity_extra") {
				tagNames = append(tagNames, tag)
				tags = append(tags, tagValue(row.Name))
			}
		}
		writes = append(writes, &measurev1.WriteRequest{
			Metadata: PatternFixtureSchema(name).Metadata, MessageId: uint64(i + 1),
			DataPointSpec: &measurev1.DataPointSpec{
				TagFamilySpec: []*measurev1.TagFamilySpec{{
					Name:     "default",
					TagNames: tagNames,
				}},
				FieldNames: []string{"value"},
			},
			DataPoint: &measurev1.DataPointValue{
				Timestamp:   timestamppb.New(base.Add(time.Duration(row.Millis) * time.Millisecond)),
				TagFamilies: []*modelv1.TagFamilyForWrite{{Tags: tags}},
				Fields:      []*modelv1.FieldValue{fieldValue(row.Value)},
			},
		})
	}
	return writes
}

func patternSchemas() []*databasev1.Measure {
	var schemas []*databasev1.Measure
	for _, name := range PatternFixtureNames() {
		schemas = append(schemas, PatternFixtureSchema(name))
	}
	return schemas
}

func patternAllRules() []*databasev1.IndexRule {
	var rules []*databasev1.IndexRule
	for _, name := range PatternFixtureNames() {
		rules = append(rules, PatternIndexRules(name)...)
	}
	return rules
}

func patternBindings() []*databasev1.IndexRuleBinding {
	var bindings []*databasev1.IndexRuleBinding
	for _, name := range PatternFixtureNames() {
		bindings = append(bindings, PatternIndexBinding(name))
	}
	return bindings
}
