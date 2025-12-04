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

package measure

import (
	"testing"
	"time"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
)

func TestLocalIndexScanSchemaRetainsNonProjectedTags(t *testing.T) {
	measureMeta := &databasev1.Measure{
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
	indexRules := []*databasev1.IndexRule{
		{
			Metadata: &commonv1.Metadata{Name: "filter-tag-rule"},
			Type:     databasev1.IndexRule_TYPE_INVERTED,
			Tags:     []string{"filter_tag"},
		},
	}
	schema, err := BuildSchema(measureMeta, indexRules)
	if err != nil {
		t.Fatalf("build schema: %v", err)
	}

	criteria := &modelv1.Criteria{
		Exp: &modelv1.Criteria_Condition{
			Condition: &modelv1.Condition{
				Name: "filter_tag",
				Op:   modelv1.Condition_BINARY_OP_EQ,
				Value: &modelv1.TagValue{
					Value: &modelv1.TagValue_Str{
						Str: &modelv1.Str{Value: "match"},
					},
				},
			},
		},
	}
	metadata := &commonv1.Metadata{Name: "test", Group: "default"}

	plan, err := indexScan(
		time.Unix(0, 0),
		time.Unix(1, 0),
		metadata,
		[][]*logical.Tag{logical.NewTags("default", "projected_tag")},
		nil,
		false,
		criteria,
		nil,
	).Analyze(schema)
	if err != nil {
		t.Fatalf("analyze plan: %v", err)
	}

	if plan.Schema().FindTagSpecByName("filter_tag") == nil {
		t.Fatalf("expected schema to retain definition for filter_tag even when it is not projected")
	}
}

func TestLocalIndexScanSchemaProjectionInIndexMode(t *testing.T) {
	measureMeta := &databasev1.Measure{
		Entity: &databasev1.Entity{
			TagNames: []string{"entity_id"},
		},
		TagFamilies: []*databasev1.TagFamilySpec{
			{
				Name: "default",
				Tags: []*databasev1.TagSpec{
					{Name: "entity_id", Type: databasev1.TagType_TAG_TYPE_STRING},
					{Name: "filter_tag", Type: databasev1.TagType_TAG_TYPE_STRING},
					{Name: "projected_tag", Type: databasev1.TagType_TAG_TYPE_STRING},
					{Name: "non_projected_tag", Type: databasev1.TagType_TAG_TYPE_STRING},
				},
			},
		},
		IndexMode: true,
	}
	indexRules := []*databasev1.IndexRule{
		{
			Metadata: &commonv1.Metadata{Name: "filter-tag-rule"},
			Type:     databasev1.IndexRule_TYPE_INVERTED,
			Tags:     []string{"filter_tag"},
		},
	}
	schema, err := BuildSchema(measureMeta, indexRules)
	if err != nil {
		t.Fatalf("build schema: %v", err)
	}

	criteria := &modelv1.Criteria{
		Exp: &modelv1.Criteria_Condition{
			Condition: &modelv1.Condition{
				Name: "filter_tag",
				Op:   modelv1.Condition_BINARY_OP_EQ,
				Value: &modelv1.TagValue{
					Value: &modelv1.TagValue_Str{
						Str: &modelv1.Str{Value: "match"},
					},
				},
			},
		},
	}
	metadata := &commonv1.Metadata{Name: "test", Group: "default"}

	plan, err := indexScan(
		time.Unix(0, 0),
		time.Unix(1, 0),
		metadata,
		[][]*logical.Tag{logical.NewTags("default", "projected_tag")},
		nil,
		false,
		criteria,
		nil,
	).Analyze(schema)
	if err != nil {
		t.Fatalf("analyze plan: %v", err)
	}

	projectedSchema := plan.Schema()

	// Verify that projected_tag is present in the schema
	if projectedSchema.FindTagSpecByName("projected_tag") == nil {
		t.Fatalf("expected schema to include projected_tag")
	}

	// Verify that non_projected_tag is NOT present in the projected schema
	// This is the key test - without the fix, projectedSchema would be the full schema
	// and non_projected_tag would incorrectly be present
	if projectedSchema.FindTagSpecByName("non_projected_tag") != nil {
		t.Fatalf("expected schema to exclude non_projected_tag when using projection")
	}
}
