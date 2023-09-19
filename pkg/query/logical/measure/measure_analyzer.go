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
	"math"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/measure"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
)

const defaultLimit uint32 = 100

// BuildSchema returns Schema loaded from the metadata repository.
func BuildSchema(md *databasev1.Measure, indexRules []*databasev1.IndexRule, entityList []string) (logical.Schema, error) {
	md.GetEntity()

	ms := &schema{
		common: &logical.CommonSchema{
			IndexRules: indexRules,
			TagSpecMap: make(map[string]*logical.TagSpec),
			EntityList: entityList,
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
func Analyze(_ context.Context, criteria *WrapRequest, metadata *commonv1.Metadata, s logical.Schema) (logical.Plan, error) {
	groupByEntity := false
	var groupByTags [][]*logical.Tag
	if criteria.GetGroupBy() != nil {
		groupByProjectionTags := criteria.GetGroupBy().GetTagProjection()
		groupByTags = make([][]*logical.Tag, len(groupByProjectionTags.GetTagFamilies()))
		tags := make([]string, 0)
		for i, tagFamily := range groupByProjectionTags.GetTagFamilies() {
			groupByTags[i] = logical.NewTags(tagFamily.GetName(), tagFamily.GetTags()...)
			tags = append(tags, tagFamily.GetTags()...)
		}
		if logical.StringSlicesEqual(s.EntityList(), tags) {
			groupByEntity = true
		}
	}

	// parse fields
	plan := parseFields(criteria, metadata, groupByEntity)

	// parse limit and offset
	limitParameter := criteria.GetLimit()
	if limitParameter == 0 {
		limitParameter = defaultLimit
	}
	pushedLimit := int(limitParameter + criteria.GetOffset())

	if criteria.GetGroupBy() != nil {
		plan = newUnresolvedGroupBy(plan, groupByTags, groupByEntity, criteria.IsTop())
		pushedLimit = math.MaxInt
	}

	if criteria.GetAgg() != nil {
		plan = newUnresolvedAggregation(plan,
			logical.NewField(criteria.GetAgg().GetFieldName()),
			criteria.GetAgg().GetFunction(),
			criteria.GetGroupBy() != nil,
			criteria.IsTop(),
		)
		pushedLimit = math.MaxInt
	}

	if criteria.GetTop() != nil {
		plan = top(plan, criteria.GetTop(), !criteria.hasAgg())
	}

	plan = limit(plan, criteria.GetOffset(), limitParameter)
	p, err := plan.Analyze(s)
	if err != nil {
		return nil, err
	}
	rules := []logical.OptimizeRule{
		logical.NewPushDownOrder(criteria.GetOrderBy()),
		logical.NewPushDownMaxSize(pushedLimit),
	}
	if err := logical.ApplyRules(p, rules...); err != nil {
		return nil, err
	}
	return p, nil
}

// DistributedAnalyze converts logical expressions to executable operation tree represented by Plan.
func DistributedAnalyze(criteria *WrapRequest, s logical.Schema) (logical.Plan, error) {
	groupByEntity := false
	var groupByTags [][]*logical.Tag
	if criteria.GetGroupBy() != nil {
		groupByProjectionTags := criteria.GetGroupBy().GetTagProjection()
		groupByTags = make([][]*logical.Tag, len(groupByProjectionTags.GetTagFamilies()))
		tags := make([]string, 0)
		for i, tagFamily := range groupByProjectionTags.GetTagFamilies() {
			groupByTags[i] = logical.NewTags(tagFamily.GetName(), tagFamily.GetTags()...)
			tags = append(tags, tagFamily.GetTags()...)
		}
		if logical.StringSlicesEqual(s.EntityList(), tags) {
			groupByEntity = true
		}
	}

	// parse fields
	plan := newUnresolvedDistributed(criteria)

	// parse limit and offset
	limitParameter := criteria.GetLimit()
	if limitParameter == 0 {
		limitParameter = defaultLimit
	}
	pushedLimit := int(limitParameter + criteria.GetOffset())

	if criteria.GetGroupBy() != nil {
		plan = newUnresolvedGroupBy(plan, groupByTags, groupByEntity, criteria.IsTop())
		pushedLimit = math.MaxInt
	}

	if criteria.GetAgg() != nil {
		plan = newUnresolvedAggregation(plan,
			logical.NewField(criteria.GetAgg().GetFieldName()),
			criteria.GetAgg().GetFunction(),
			criteria.GetGroupBy() != nil,
			criteria.IsTop(),
		)
		pushedLimit = math.MaxInt
	}

	if criteria.GetTop() != nil {
		plan = top(plan, criteria.GetTop(), !criteria.hasAgg())
	}

	plan = limit(plan, criteria.GetOffset(), limitParameter)
	p, err := plan.Analyze(s)
	if err != nil {
		return nil, err
	}
	rules := []logical.OptimizeRule{
		logical.NewPushDownOrder(criteria.OrderBy),
		logical.NewPushDownMaxSize(pushedLimit),
	}
	if err := logical.ApplyRules(p, rules...); err != nil {
		return nil, err
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

// WrapRequest is used to represent TopNRequest and QueryRequest.
// projField and projTag are used to wrap TopNRequest.
type WrapRequest struct {
	*measurev1.QueryRequest
	*measurev1.TopNRequest
	projField string
	projTag   []string
}

// WrapTopNRequest wrap topNRequest to WrapRequest.
func WrapTopNRequest(request *measurev1.TopNRequest, topNSchema *databasev1.TopNAggregation, sourceMeasure measure.Measure) *WrapRequest {
	return &WrapRequest{
		TopNRequest: request,
		projField:   topNSchema.GetFieldName(),
		projTag:     sourceMeasure.GetSchema().GetEntity().GetTagNames(),
	}
}

// WrapQueryRequest wrap queryRequest to WrapRequest.
func WrapQueryRequest(request *measurev1.QueryRequest) *WrapRequest {
	return &WrapRequest{
		QueryRequest: request,
	}
}

// GetMetadata return commonv1.Metadata.
func (wr *WrapRequest) GetMetadata() *commonv1.Metadata {
	if wr.QueryRequest != nil {
		return wr.QueryRequest.Metadata
	}
	return wr.TopNRequest.Metadata
}

// GetTimeRange return modelv1.TimeRange.
func (wr *WrapRequest) GetTimeRange() *modelv1.TimeRange {
	if wr.QueryRequest != nil {
		return wr.QueryRequest.TimeRange
	}
	return wr.TopNRequest.TimeRange
}

// IsTop determine whether current WrapRequest is wrapping TopNRequest.
func (wr *WrapRequest) IsTop() bool {
	return wr.QueryRequest == nil
}

// GetFieldProjection return fields which want to query.
func (wr *WrapRequest) GetFieldProjection() []string {
	if wr.QueryRequest != nil {
		return wr.QueryRequest.GetFieldProjection().GetNames()
	}
	return []string{wr.projField}
}

// SetFieldProjection set field which want to query.
func (wr *WrapRequest) SetFieldProjection(fieldName string) {
	wr.projField = fieldName
}

// GetTagProjection return modelv1.TagProject.
func (wr *WrapRequest) GetTagProjection() *modelv1.TagProjection {
	if wr.QueryRequest != nil {
		return wr.QueryRequest.GetTagProjection()
	}
	return &modelv1.TagProjection{
		TagFamilies: []*modelv1.TagProjection_TagFamily{
			{
				Name: measure.TopNTagFamily,
				Tags: wr.projTag,
			},
		},
	}
}

// SetTagProjection set tags used to filter iterators.
func (wr *WrapRequest) SetTagProjection(projTag []string) {
	wr.projTag = projTag
}

// GetLimit return datapoint number.
func (wr *WrapRequest) GetLimit() uint32 {
	if wr.QueryRequest != nil {
		return wr.QueryRequest.GetLimit()
	}
	return 0
}

// GetGroupBy return measurev1.QueryRequest_GroupBy.
func (wr *WrapRequest) GetGroupBy() *measurev1.QueryRequest_GroupBy {
	if wr.QueryRequest != nil {
		return wr.QueryRequest.GetGroupBy()
	} else if !wr.hasAgg() {
		return &measurev1.QueryRequest_GroupBy{
			FieldName:     wr.projField,
			TagProjection: wr.GetTagProjection(),
		}
	}
	return nil
}

// GetAgg return measurev1.QueryRequest_Aggregation.
func (wr *WrapRequest) GetAgg() *measurev1.QueryRequest_Aggregation {
	if wr.QueryRequest != nil {
		return wr.QueryRequest.GetAgg()
	} else if wr.hasAgg() {
		return &measurev1.QueryRequest_Aggregation{
			FieldName: wr.projField,
			Function:  wr.TopNRequest.GetAgg(),
		}
	}
	return nil
}

// GetTop return measurev1.QueryRequest_Top.
func (wr *WrapRequest) GetTop() *measurev1.QueryRequest_Top {
	if wr.QueryRequest != nil {
		return wr.QueryRequest.GetTop()
	}
	return &measurev1.QueryRequest_Top{
		Number:         wr.TopNRequest.TopN,
		FieldName:      wr.projField,
		FieldValueSort: wr.TopNRequest.GetFieldValueSort(),
	}
}

// GetOffset return current offset number.
func (wr *WrapRequest) GetOffset() uint32 {
	if wr.QueryRequest != nil {
		return wr.QueryRequest.GetOffset()
	}
	return 0
}

// GetOrderBy return modelv1.QueryOrder.
func (wr *WrapRequest) GetOrderBy() *modelv1.QueryOrder {
	if wr.QueryRequest != nil {
		return wr.QueryRequest.GetOrderBy()
	}
	return &modelv1.QueryOrder{
		Sort: wr.TopNRequest.GetFieldValueSort(),
	}
}

// GetCriteria return modelv1.Criteria.
func (wr *WrapRequest) GetCriteria() *modelv1.Criteria {
	if wr.QueryRequest != nil {
		return wr.QueryRequest.GetCriteria()
	}
	return buildCriteria(wr.TopNRequest.GetConditions())
}

// GetFieldValueSort return modelv1.Sort which determine how to sort datapoint according to field value.
func (wr *WrapRequest) GetFieldValueSort() modelv1.Sort {
	if wr.QueryRequest != nil {
		return modelv1.Sort_SORT_UNSPECIFIED
	}
	return wr.TopNRequest.GetFieldValueSort()
}

func (wr *WrapRequest) hasAgg() bool {
	if wr.QueryRequest != nil {
		return true
	}
	function := wr.TopNRequest.GetAgg()
	return function != 0
}

// buildCriteria transform Condition array to Criteria tree which is used to generate tsdb.Entity later.
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
