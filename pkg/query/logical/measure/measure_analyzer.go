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
	"context"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"math"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	"github.com/apache/skywalking-banyandb/banyand/measure"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
)

const defaultLimit int64 = 100

// BuildSchema returns Schema loaded from the metadata repository.
func BuildSchema(measureSchema measure.Measure) (logical.Schema, error) {
	md := measureSchema.GetSchema()
	md.GetEntity()

	ms := &schema{
		common: &logical.CommonSchema{
			IndexRules: measureSchema.GetIndexRules(),
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

// Analyze converts logical expressions to executable operation tree represented by Plan.
func Analyze(_ context.Context, request *WrapRequest, metadata *commonv1.Metadata, s logical.Schema) (logical.Plan, error) {
	groupByEntity := true
	var groupByTags [][]*logical.Tag
	if request.GetGroupBy() != nil {
		groupByProjectionTags := request.GetGroupBy().GetTagProjection()
		groupByTags = make([][]*logical.Tag, len(groupByProjectionTags.GetTagFamilies()))
		tags := make([]string, 0)
		for i, tagFamily := range groupByProjectionTags.GetTagFamilies() {
			groupByTags[i] = logical.NewTags(tagFamily.GetName(), tagFamily.GetTags()...)
			tags = append(tags, tagFamily.GetTags()...)
		}
		if !logical.StringSlicesEqual(s.EntityList(), tags) {
			groupByEntity = false
		}
	}

	// parse fields
	plan := parseFields(request, metadata, groupByEntity)

	// parse limit and offset
	limitParameter := request.GetLimit()
	if limitParameter == 0 {
		limitParameter = defaultLimit
	}
	pushedLimit := limitParameter + request.GetOffset()

	if request.GetGroupBy() != nil {
		plan = newUnresolvedGroupBy(plan, groupByTags, groupByEntity)
		pushedLimit = math.MaxInt
	}

	if request.GetAgg() != nil &&
		request.GetAgg().GetFunction() != modelv1.AggregationFunction_AGGREGATION_FUNCTION_UNSPECIFIED {
		plan = newUnresolvedAggregation(plan,
			logical.NewField(request.GetAgg().GetFieldName()),
			request.GetAgg().GetFunction(),
			request.GetGroupBy() != nil,
			request.IsTop(),
		)
		pushedLimit = math.MaxInt
	}

	if request.GetTop() != nil {
		plan = top(plan, request.GetTop())
	}

	if request.GetLimit() != -1 {
		plan = limit(plan, uint32(request.GetOffset()), uint32(limitParameter))
	}

	p, err := plan.Analyze(s)
	if err != nil {
		return nil, err
	}

	if !request.IsTop() {
		rules := []logical.OptimizeRule{
			logical.NewPushDownOrder(request.GetOrderBy()),
			logical.NewPushDownMaxSize(int(pushedLimit)),
		}
		if err := logical.ApplyRules(p, rules...); err != nil {
			return nil, err
		}
	}
	return p, nil
}

// parseFields parses the query request to decide which kind of plan should be generated
// Basically,
// 1 - If no criteria is given, we can only scan all shards
// 2 - If criteria is given, but all of those fields exist in the "entity" definition.
func parseFields(criteria *WrapRequest, metadata *commonv1.Metadata, groupByEntity bool) logical.UnresolvedPlan {
	projFields := make([]*logical.Field, len(criteria.GetFieldProjection()))
	for i, fieldNameProj := range criteria.GetFieldProjection() {
		projFields[i] = logical.NewField(fieldNameProj)
	}
	timeRange := criteria.GetTimeRange()
	return indexScan(timeRange.GetBegin().AsTime(), timeRange.GetEnd().AsTime(), metadata,
		logical.ToTags(criteria.GetTagProjection()), projFields, groupByEntity, criteria.GetCriteria(), criteria.GetFieldValueSort(), criteria.IsTop())
}

type WrapRequest struct {
	*measurev1.QueryRequest
	*measurev1.TopNRequest
	projField string
	projTag   []string
}

func (wr *WrapRequest) GetMetadata() *commonv1.Metadata {
	if wr.QueryRequest != nil {
		return wr.QueryRequest.Metadata
	} else {
		return wr.TopNRequest.Metadata
	}
}

func (wr *WrapRequest) GetTimeRange() *modelv1.TimeRange {
	if wr.QueryRequest != nil {
		return wr.QueryRequest.TimeRange
	} else {
		return wr.TopNRequest.TimeRange
	}
}

func (wr *WrapRequest) IsTop() bool {
	if wr.QueryRequest != nil {
		return false
	} else {
		return true
	}
}

func (wr *WrapRequest) GetFieldProjection() []string {
	if wr.QueryRequest != nil {
		return wr.QueryRequest.GetFieldProjection().GetNames()
	} else {
		return []string{wr.projField}
	}
}

func (wr *WrapRequest) SetFieldProjection(fieldName string) {
	wr.projField = fieldName
}

func (wr *WrapRequest) GetTagProjection() *modelv1.TagProjection {
	if wr.QueryRequest != nil {
		return wr.QueryRequest.GetTagProjection()
	} else {
		return &modelv1.TagProjection{
			TagFamilies: []*modelv1.TagProjection_TagFamily{
				{
					Name: measure.TopNTagFamily,
					Tags: wr.projTag,
				},
			},
		}
	}
}

func (wr *WrapRequest) SetTagProjection(projTag []string) {
	wr.projTag = projTag
}

func (wr *WrapRequest) GetLimit() int64 {
	if wr.QueryRequest != nil {
		return int64(wr.QueryRequest.GetLimit())
	} else {
		return -1
	}
}

func (wr *WrapRequest) GetGroupBy() *measurev1.QueryRequest_GroupBy {
	if wr.QueryRequest != nil {
		return wr.QueryRequest.GetGroupBy()
	} else {
		return nil
	}
}

func (wr *WrapRequest) GetAgg() *measurev1.QueryRequest_Aggregation {
	if wr.QueryRequest != nil {
		return wr.QueryRequest.GetAgg()
	} else {
		return &measurev1.QueryRequest_Aggregation{
			FieldName: wr.projField,
			Function:  wr.TopNRequest.GetAgg(),
		}
	}
}

func (wr *WrapRequest) GetTop() *measurev1.QueryRequest_Top {
	if wr.QueryRequest != nil {
		return wr.QueryRequest.GetTop()
	} else {
		return &measurev1.QueryRequest_Top{
			Number:         wr.TopNRequest.TopN,
			FieldName:      wr.projField,
			FieldValueSort: wr.TopNRequest.GetFieldValueSort(),
		}
	}
}

func (wr *WrapRequest) GetOffset() int64 {
	if wr.QueryRequest != nil {
		return int64(wr.QueryRequest.GetOffset())
	} else {
		return 0
	}
}

func (wr *WrapRequest) GetOrderBy() *modelv1.QueryOrder {
	if wr.QueryRequest != nil {
		return wr.QueryRequest.GetOrderBy()
	} else {
		return nil
	}
}

func (wr *WrapRequest) GetCriteria() *modelv1.Criteria {
	if wr.QueryRequest != nil {
		return wr.QueryRequest.GetCriteria()
	} else {
		return buildCriteria(wr.TopNRequest.GetConditions())
	}
}

func (wr *WrapRequest) GetFieldValueSort() modelv1.Sort {
	if wr.QueryRequest != nil {
		return modelv1.Sort_SORT_UNSPECIFIED
	} else {
		return wr.TopNRequest.GetFieldValueSort()
	}
}

func buildCriteria(conditions []*modelv1.Condition) *modelv1.Criteria {
	var criteria *modelv1.Criteria
	for _, cond := range conditions {
		sub := &modelv1.Criteria{
			Exp: &modelv1.Criteria_Le{
				Le: &modelv1.LogicalExpression{
					Op: modelv1.LogicalExpression_LOGICAL_OP_AND,
					Left: &modelv1.Criteria{
						Exp: &modelv1.Criteria_Condition{
							Condition: cond,
						},
					},
					Right: criteria,
				},
			},
		}
		criteria = sub
	}
	return criteria
}
