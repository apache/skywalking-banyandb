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

package stream

import (
	"testing"
	"time"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
)

func TestAnalyzeAddsHiddenCriteriaTags(t *testing.T) {
	schema := mustBuildTestStreamSchema(t)
	metadata := &commonv1.Metadata{Name: "svc", Group: "default"}
	plan := tagFilter(
		time.Unix(0, 0),
		time.Unix(1, 0),
		metadata,
		buildEqualityCriteria("filter_tag", "match"),
		[][]*logical.Tag{logical.NewTags("default", "projected_tag")},
		nil,
	)
	resolved, err := plan.Analyze(schema)
	if err != nil {
		t.Fatalf("analyze tag filter: %v", err)
	}

	tfPlan, ok := resolved.(*tagFilterPlan)
	if !ok {
		t.Fatalf("expected tagFilterPlan, got %T", resolved)
	}
	if _, exists := tfPlan.hidden["filter_tag"]; !exists {
		t.Fatalf("expected filter_tag to be tracked as hidden")
	}

	parent, ok := tfPlan.parent.(*localIndexScan)
	if !ok {
		t.Fatalf("expected localIndexScan as parent, got %T", tfPlan.parent)
	}
	found := false
	for _, family := range parent.projectionTags {
		for _, tagName := range family.Names {
			if tagName == "filter_tag" {
				found = true
				break
			}
		}
	}
	if !found {
		t.Fatalf("expected filter_tag to be added to projection for evaluation")
	}
}

func TestStripHiddenTagsRemovesSensitiveValues(t *testing.T) {
	tfPlan := &tagFilterPlan{
		hidden: map[string]struct{}{
			"hidden": {},
		},
	}
	element := &streamv1.Element{
		TagFamilies: []*modelv1.TagFamily{
			{
				Name: "default",
				Tags: []*modelv1.Tag{
					{Key: "visible", Value: buildStringValue("keep")},
					{Key: "hidden", Value: buildStringValue("drop")},
				},
			},
			{
				Name: "second",
				Tags: []*modelv1.Tag{
					{Key: "hidden", Value: buildStringValue("drop")},
				},
			},
		},
	}

	tfPlan.stripHiddenTags(element)

	if len(element.GetTagFamilies()) != 1 {
		t.Fatalf("expected only one tag family after stripping, got %d", len(element.GetTagFamilies()))
	}
	remainingTags := element.GetTagFamilies()[0].GetTags()
	if len(remainingTags) != 1 || remainingTags[0].GetKey() != "visible" {
		t.Fatalf("unexpected tags remaining after stripping: %+v", remainingTags)
	}
}

func mustBuildTestStreamSchema(t *testing.T) logical.Schema {
	t.Helper()
	stream := &databasev1.Stream{
		Entity: &databasev1.Entity{
			TagNames: []string{"entity_id"},
		},
		TagFamilies: []*databasev1.TagFamilySpec{
			{
				Name: "default",
				Tags: []*databasev1.TagSpec{
					{Name: "filter_tag", Type: databasev1.TagType_TAG_TYPE_STRING},
					{Name: "projected_tag", Type: databasev1.TagType_TAG_TYPE_STRING},
				},
			},
		},
	}
	schema, err := BuildSchema(stream, nil)
	if err != nil {
		t.Fatalf("build schema: %v", err)
	}
	return schema
}

func buildEqualityCriteria(tagName, value string) *modelv1.Criteria {
	return &modelv1.Criteria{
		Exp: &modelv1.Criteria_Condition{
			Condition: &modelv1.Condition{
				Name: tagName,
				Op:   modelv1.Condition_BINARY_OP_EQ,
				Value: &modelv1.TagValue{
					Value: &modelv1.TagValue_Str{
						Str: &modelv1.Str{Value: value},
					},
				},
			},
		},
	}
}

func buildStringValue(val string) *modelv1.TagValue {
	return &modelv1.TagValue{
		Value: &modelv1.TagValue_Str{
			Str: &modelv1.Str{Value: val},
		},
	}
}
