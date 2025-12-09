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

func TestParseIndexRuleLocators_Normal(t *testing.T) {
	entity := &databasev1.Entity{
		TagNames: []string{"entity_tag"},
	}
	families := []*databasev1.TagFamilySpec{
		{
			Name: "family",
			Tags: []*databasev1.TagSpec{
				{Name: "tag1", Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: "tag2", Type: databasev1.TagType_TAG_TYPE_STRING},
			},
		},
	}
	indexRules := []*databasev1.IndexRule{
		{
			Metadata: &commonv1.Metadata{Name: "index", Id: 1},
			Tags:     []string{"tag1"},
		},
	}

	locators, fil := ParseIndexRuleLocators(entity, families, indexRules, false)

	assert.NotNil(t, locators.EntitySet)
	assert.NotNil(t, fil)
	assert.Len(t, locators.TagFamilyTRule, 1)
	assert.Len(t, locators.TagFamilyTRule[0], 1)
	assert.NotNil(t, locators.TagFamilyTRule[0]["tag1"])
	assert.Equal(t, "index", locators.TagFamilyTRule[0]["tag1"].GetMetadata().GetName())
}

func TestParseIndexRuleLocators_Invalid(t *testing.T) {
	entity := &databasev1.Entity{
		TagNames: []string{"entity_tag"},
	}
	families := []*databasev1.TagFamilySpec{
		{
			Name: "family",
			Tags: []*databasev1.TagSpec{
				{Name: "tag1", Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: "tag2", Type: databasev1.TagType_TAG_TYPE_STRING},
			},
		},
	}
	indexRules := []*databasev1.IndexRule{
		{
			Metadata: &commonv1.Metadata{Name: "valid_index", Id: 1},
			Tags:     []string{"tag1"},
		},
		{
			Metadata: &commonv1.Metadata{Name: "invalid_index1", Id: 2},
			Tags:     []string{"non_existent_tag"},
		},
		{
			Metadata: &commonv1.Metadata{Name: "invalid_index2", Id: 3},
			Tags:     []string{"tag2", "removed_tag"},
		},
	}

	locators, _ := ParseIndexRuleLocators(entity, families, indexRules, false)

	assert.Len(t, locators.TagFamilyTRule, 1)
	assert.Len(t, locators.TagFamilyTRule[0], 1)
	assert.NotNil(t, locators.TagFamilyTRule[0]["tag1"])
	assert.Equal(t, "valid_index", locators.TagFamilyTRule[0]["tag1"].GetMetadata().GetName())
	_, hasNonExistentTag := locators.TagFamilyTRule[0]["non_existent_tag"]
	assert.False(t, hasNonExistentTag)
	_, hasTag2 := locators.TagFamilyTRule[0]["tag2"]
	assert.False(t, hasTag2)
}
