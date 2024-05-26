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

// Package measure implements execution operations for querying measure data.
package measure

import (
	"context"
	"fmt"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
)

// BuildTopNSchema returns Schema loaded from the metadata repository.
func BuildTopNSchema(md *databasev1.Measure) (logical.Schema, error) {
	md.GetEntity()

	ms := &schema{
		common: &logical.CommonSchema{
			TagSpecMap: make(map[string]*logical.TagSpec),
			EntityList: md.GetEntity().GetTagNames(),
		},
		measure:  md,
		fieldMap: make(map[string]*logical.FieldSpec),
	}

	ms.common.RegisterTagFamilies(md.GetTagFamilies())

	for fieldIdx, spec := range md.GetFields() {
		ms.registerField(fieldIdx, spec)
	}

	return ms, nil
}

// TopNAnalyze converts logical expressions to executable operation tree represented by Plan.
func TopNAnalyze(_ context.Context, criteria *measurev1.TopNRequest, schema *databasev1.Measure,
	sourceMeasureSchema *databasev1.Measure, topNAggregation *databasev1.TopNAggregation, s logical.Schema,
) (logical.Plan, error) {
	groupByProjectionTags := sourceMeasureSchema.GetEntity().GetTagNames()
	groupByTags := make([][]*logical.Tag, len(schema.GetTagFamilies()))
	tagFamily := schema.GetTagFamilies()[0]
	groupByTags[0] = logical.NewTags(tagFamily.GetName(), groupByProjectionTags...)

	if len(schema.GetFields()) != 1 {
		return nil, fmt.Errorf("topN schema fields count should be 1 but got %d", len(schema.GetFields()))
	}
	projectionFields := make([]*logical.Field, 1)
	fieldName := schema.GetFields()[0].GetName()
	projectionFields[0] = logical.NewField(fieldName)
	// parse fields
	plan := parse(criteria, schema.GetMetadata(), projectionFields, groupByTags)

	if criteria.GetAgg() != 0 {
		plan = newUnresolvedGroupBy(plan, groupByTags, false)
		plan = newUnresolvedAggregation(plan,
			projectionFields[0],
			criteria.GetAgg(),
			true)
	}

	plan = top(plan, &measurev1.QueryRequest_Top{
		Number:         criteria.GetTopN(),
		FieldName:      fieldName,
		FieldValueSort: criteria.GetFieldValueSort(),
	})
	p, err := plan.Analyze(s)
	if err != nil {
		return nil, err
	}
	return p, nil
}

func parse(criteria *measurev1.TopNRequest, metadata *commonv1.Metadata,
	projFields []*logical.Field, projTags [][]*logical.Tag,
) logical.UnresolvedPlan {
	timeRange := criteria.GetTimeRange()
	return local(timeRange.GetBegin().AsTime(), timeRange.GetEnd().AsTime(),
		metadata, projTags, projFields, buildConditions(criteria), criteria.GetFieldValueSort())
}

func buildConditions(criteria *measurev1.TopNRequest) []*modelv1.Condition {
	return append([]*modelv1.Condition{
		{
			Name: "sortDirection",
			Op:   modelv1.Condition_BINARY_OP_EQ,
			Value: &modelv1.TagValue{
				Value: &modelv1.TagValue_Int{
					Int: &modelv1.Int{
						Value: int64(criteria.GetFieldValueSort().Number()),
					},
				},
			},
		},
	}, criteria.GetConditions()...)
}
