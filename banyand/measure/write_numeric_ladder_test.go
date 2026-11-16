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

	"github.com/stretchr/testify/require"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/index"
)

// Only a scalar int index-rule tag that is sort-marked is offered to the sort ladder: arrays rank by their smallest value,
// no_sort tags are not sorted, and entity tags are written as separate fields.
func TestIndexModeIndexFieldsMarkOnlyScalarSortableIntsNumeric(t *testing.T) {
	const ruleCode, ruleIDs, rulePort, ruleInst = uint32(21), uint32(22), uint32(23), uint32(24)
	rules := []*databasev1.IndexRule{
		{Metadata: &commonv1.Metadata{Group: exportIMGroup, Name: "code", Id: ruleCode}, Tags: []string{"code"}, Type: databasev1.IndexRule_TYPE_INVERTED},
		{Metadata: &commonv1.Metadata{Group: exportIMGroup, Name: "ids", Id: ruleIDs}, Tags: []string{"ids"}, Type: databasev1.IndexRule_TYPE_INVERTED},
		{Metadata: &commonv1.Metadata{Group: exportIMGroup, Name: "port", Id: rulePort}, Tags: []string{"port"}, Type: databasev1.IndexRule_TYPE_INVERTED, NoSort: true},
		{Metadata: &commonv1.Metadata{Group: exportIMGroup, Name: "inst", Id: ruleInst}, Tags: []string{"inst"}, Type: databasev1.IndexRule_TYPE_INVERTED},
	}
	m := newExportTestMeasure(exportIndexModeMeasure(), rules)
	str := func(v string) *modelv1.TagValue {
		return &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: v}}}
	}
	integer := func(v int64) *modelv1.TagValue {
		return &modelv1.TagValue{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: v}}}
	}
	req := &measurev1.WriteRequest{
		Metadata: m.schema.GetMetadata(),
		DataPoint: &measurev1.DataPointValue{
			Version: 1,
			TagFamilies: []*modelv1.TagFamilyForWrite{
				{Tags: []*modelv1.TagValue{
					str("svc"), str("inst-1"), integer(-42),
					{Value: &modelv1.TagValue_StrArray{StrArray: &modelv1.StrArray{}}},
					{Value: &modelv1.TagValue_IntArray{IntArray: &modelv1.IntArray{Value: []int64{5, 9}}}},
					{Value: &modelv1.TagValue_BinaryData{BinaryData: []byte("b")}},
					integer(8080),
				}},
				{Tags: []*modelv1.TagValue{str("r")}},
			},
		},
	}
	is := m.indexSchema.Load().(indexSchema)
	byRule := map[uint32][]index.Field{}
	for _, field := range handleIndexMode(m.schema, req, is.indexRuleLocators, nil) {
		byRule[field.Key.IndexRuleID] = append(byRule[field.Key.IndexRuleID], field)
	}
	require.Len(t, byRule[ruleCode], 1)
	require.True(t, byRule[ruleCode][0].Numeric)
	require.Equal(t, int64(-42), byRule[ruleCode][0].NumericKey)
	require.Len(t, byRule[ruleIDs], 2)
	for _, field := range byRule[ruleIDs] {
		require.False(t, field.Numeric, "arrays are not ladder eligible")
	}
	require.Len(t, byRule[rulePort], 1)
	require.False(t, byRule[rulePort][0].Numeric, "no_sort tags are not ladder eligible")
	require.Len(t, byRule[ruleInst], 1)
	require.False(t, byRule[ruleInst][0].Numeric, "string tags are not ladder eligible")
}
