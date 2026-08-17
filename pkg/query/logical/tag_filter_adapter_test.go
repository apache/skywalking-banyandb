// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. The ASF licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logical

import (
	"testing"

	"github.com/stretchr/testify/require"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

func TestTagFilterMatcherUsesConditionSchema(t *testing.T) {
	const conditionTag = "condition"
	registry := TagSpecMap{
		conditionTag: {
			Spec: &databasev1.TagSpec{
				Name: conditionTag,
				Type: databasev1.TagType_TAG_TYPE_STRING,
			},
			TagFamilyIdx: 0,
			TagIdx:       0,
		},
	}
	filter, err := BuildSimpleTagFilter(stringEqualityCriteria(conditionTag, "match"))
	require.NoError(t, err)
	matcher := NewTagFilterMatcher(filter, registry, nil)

	tests := []struct {
		name      string
		tags      []*modelv1.Tag
		wantMatch bool
		wantErr   bool
	}{
		{
			name: "unrelated projected tag before matching condition",
			tags: []*modelv1.Tag{
				stringTag("projected", "not-match"),
				stringTag(conditionTag, "match"),
			},
			wantMatch: true,
		},
		{
			name: "unrelated projected tag cannot satisfy condition",
			tags: []*modelv1.Tag{
				stringTag("projected", "match"),
				stringTag(conditionTag, "not-match"),
			},
		},
		{
			name: "null condition remains a value",
			tags: []*modelv1.Tag{
				{Key: conditionTag, Value: &modelv1.TagValue{Value: &modelv1.TagValue_Null{}}},
			},
		},
		{
			name: "absent condition remains absent",
			tags: []*modelv1.Tag{
				stringTag("projected", "match"),
			},
			wantErr: true,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			matched, matchErr := matcher.Match(testCase.tags)
			if testCase.wantErr {
				require.Error(t, matchErr)
				return
			}
			require.NoError(t, matchErr)
			require.Equal(t, testCase.wantMatch, matched)
		})
	}
}

func stringEqualityCriteria(tagName, value string) *modelv1.Criteria {
	return &modelv1.Criteria{
		Exp: &modelv1.Criteria_Condition{
			Condition: &modelv1.Condition{
				Name:  tagName,
				Op:    modelv1.Condition_BINARY_OP_EQ,
				Value: stringTagValue(value),
			},
		},
	}
}

func stringTag(name, value string) *modelv1.Tag {
	return &modelv1.Tag{Key: name, Value: stringTagValue(value)}
}

func stringTagValue(value string) *modelv1.TagValue {
	return &modelv1.TagValue{
		Value: &modelv1.TagValue_Str{
			Str: &modelv1.Str{Value: value},
		},
	}
}
