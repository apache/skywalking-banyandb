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

package partition

import (
	"testing"

	"github.com/stretchr/testify/assert"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
)

func TestParseIndexRuleLocators(t *testing.T) {
	tests := []struct {
		name       string
		entity     *databasev1.Entity
		families   []*databasev1.TagFamilySpec
		indexRules []*databasev1.IndexRule
		indexMode  bool
		wantLen    int
	}{
		{
			name: "normal case with all tags present",
			entity: &databasev1.Entity{
				TagNames: []string{"entity_tag"},
			},
			families: []*databasev1.TagFamilySpec{
				{
					Name: "family1",
					Tags: []*databasev1.TagSpec{
						{Name: "tag1", Type: databasev1.TagType_TAG_TYPE_STRING},
						{Name: "tag2", Type: databasev1.TagType_TAG_TYPE_STRING},
					},
				},
			},
			indexRules: []*databasev1.IndexRule{
				{
					Metadata: &commonv1.Metadata{Name: "index1", Id: 1},
					Tags:     []string{"tag1"},
				},
			},
			indexMode: false,
			wantLen:   1,
		},
		{
			name: "removed tag case - index rule references non-existent tag",
			entity: &databasev1.Entity{
				TagNames: []string{"entity_tag"},
			},
			families: []*databasev1.TagFamilySpec{
				{
					Name: "family1",
					Tags: []*databasev1.TagSpec{
						{Name: "tag1", Type: databasev1.TagType_TAG_TYPE_STRING},
					},
				},
			},
			indexRules: []*databasev1.IndexRule{
				{
					Metadata: &commonv1.Metadata{Name: "index1", Id: 1},
					Tags:     []string{"tag1", "removed_tag"},
				},
			},
			indexMode: false,
			wantLen:   1,
		},
		{
			name: "all tags removed from index rule",
			entity: &databasev1.Entity{
				TagNames: []string{"entity_tag"},
			},
			families: []*databasev1.TagFamilySpec{
				{
					Name: "family1",
					Tags: []*databasev1.TagSpec{
						{Name: "new_tag", Type: databasev1.TagType_TAG_TYPE_STRING},
					},
				},
			},
			indexRules: []*databasev1.IndexRule{
				{
					Metadata: &commonv1.Metadata{Name: "index1", Id: 1},
					Tags:     []string{"removed_tag1", "removed_tag2"},
				},
			},
			indexMode: false,
			wantLen:   1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			locators, fil := ParseIndexRuleLocators(tt.entity, tt.families, tt.indexRules, tt.indexMode)
			assert.NotNil(t, locators.EntitySet)
			assert.NotNil(t, fil)
			assert.Len(t, locators.TagFamilyTRule, tt.wantLen)
		})
	}
}

func TestParseIndexRuleLocators_RemovedTagsIgnored(t *testing.T) {
	entity := &databasev1.Entity{
		TagNames: []string{"entity_tag"},
	}
	families := []*databasev1.TagFamilySpec{
		{
			Name: "family1",
			Tags: []*databasev1.TagSpec{
				{Name: "existing_tag", Type: databasev1.TagType_TAG_TYPE_STRING},
			},
		},
	}
	indexRules := []*databasev1.IndexRule{
		{
			Metadata: &commonv1.Metadata{Name: "index_with_removed_tag", Id: 1},
			Tags:     []string{"existing_tag", "removed_tag"},
		},
	}

	locators, _ := ParseIndexRuleLocators(entity, families, indexRules, false)

	assert.Len(t, locators.TagFamilyTRule, 1)
	assert.Len(t, locators.TagFamilyTRule[0], 1)
	assert.NotNil(t, locators.TagFamilyTRule[0]["existing_tag"])
	_, hasRemovedTag := locators.TagFamilyTRule[0]["removed_tag"]
	assert.False(t, hasRemovedTag, "removed tag should not be in locator")
}
