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
	"fmt"
	"math"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
)

const defaultLimit uint32 = 100

// BuildSchema returns Schema loaded from the metadata repository.
func BuildSchema(md *databasev1.Measure, indexRules []*databasev1.IndexRule) (logical.Schema, error) {
	md.GetEntity()

	ms := &schema{
		common: &logical.CommonSchema{
			IndexRules: indexRules,
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
func Analyze(
	criteria *measurev1.QueryRequest,
	metadata []*commonv1.Metadata,
	ss []logical.Schema,
	ecc []executor.MeasureExecutionContext,
	isDistributed bool,
) (logical.Plan, error) {
	if len(metadata) != len(ss) {
		return nil, fmt.Errorf("number of schemas %d not equal to metadata count %d", len(ss), len(metadata))
	}
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
		if logical.StringSlicesEqual(ss[0].EntityList(), tags) {
			groupByEntity = true
		}
	}
	var plan logical.UnresolvedPlan
	var s logical.Schema
	tagProjection := logical.ToTags(criteria.GetTagProjection())
	if len(metadata) == 1 {
		plan = parseFields(criteria, metadata[0], ecc[0], groupByEntity, tagProjection)
		s = ss[0]
	} else {
		var err error
		if s, err = mergeSchema(ss); err != nil {
			return nil, err
		}
		plan = &unresolvedMerger{
			criteria:      criteria,
			metadata:      metadata,
			ecc:           ecc,
			tagProjection: tagProjection,
			groupByEntity: groupByEntity,
		}
	}

	ms := s.(*schema)
	if len(tagProjection) > 0 {
		if err := ms.common.ValidateProjectionTags(tagProjection...); err != nil {
			return nil, err
		}
	}
	fieldProjection := criteria.GetFieldProjection().GetNames()
	if len(fieldProjection) > 0 {
		if err := ms.ValidateProjectionFields(fieldProjection...); err != nil {
			return nil, err
		}
	}

	// parse limit and offset
	limitParameter := criteria.GetLimit()
	if limitParameter == 0 {
		limitParameter = defaultLimit
	}
	pushedLimit := int(limitParameter + criteria.GetOffset())

	if criteria.GetGroupBy() != nil {
		plan = newUnresolvedGroupBy(plan, groupByTags, groupByEntity)
		pushedLimit = math.MaxInt
	}

	if criteria.GetAgg() != nil {
		aggrFunc := criteria.GetAgg().GetFunction()
		// Use DISTRIBUTED_MEAN for distributed MEAN; liaison merges and outputs to agg fieldName for Top to sort.
		if isDistributed && aggrFunc == modelv1.AggregationFunction_AGGREGATION_FUNCTION_MEAN {
			aggrFunc = modelv1.AggregationFunction_AGGREGATION_FUNCTION_DISTRIBUTED_MEAN
		}
		plan = newUnresolvedAggregation(plan,
			logical.NewField(criteria.GetAgg().GetFieldName()),
			aggrFunc,
			criteria.GetGroupBy() != nil,
		)
		pushedLimit = math.MaxInt
	}

	if criteria.GetTop() != nil {
		plan = top(plan, criteria.GetTop())
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

// DistributedAnalyze converts logical expressions to executable operation tree represented by Plan.
func DistributedAnalyze(criteria *measurev1.QueryRequest, ss []logical.Schema) (logical.Plan, error) {
	var groupByTags [][]*logical.Tag
	if criteria.GetGroupBy() != nil {
		groupByProjectionTags := criteria.GetGroupBy().GetTagProjection()
		groupByTags = make([][]*logical.Tag, len(groupByProjectionTags.GetTagFamilies()))
		for i, tagFamily := range groupByProjectionTags.GetTagFamilies() {
			groupByTags[i] = logical.NewTags(tagFamily.GetName(), tagFamily.GetTags()...)
		}
	}

	needCompletePushDownAgg := criteria.GetAgg() != nil

	// parse fields
	plan := newUnresolvedDistributed(criteria, needCompletePushDownAgg)

	// parse limit and offset
	limitParameter := criteria.GetLimit()
	if limitParameter == 0 {
		limitParameter = defaultLimit
	}
	pushedLimit := int(limitParameter + criteria.GetOffset())

	if criteria.GetGroupBy() != nil {
		plan = newUnresolvedGroupBy(plan, groupByTags, false)
		pushedLimit = math.MaxInt
	}

	if criteria.GetAgg() != nil {
		aggrFunc := criteria.GetAgg().GetFunction()
		if needCompletePushDownAgg {
			if aggrFunc == modelv1.AggregationFunction_AGGREGATION_FUNCTION_COUNT {
				aggrFunc = modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM
			} else if aggrFunc == modelv1.AggregationFunction_AGGREGATION_FUNCTION_MEAN {
				aggrFunc = modelv1.AggregationFunction_AGGREGATION_FUNCTION_DISTRIBUTED_MEAN
			}
		}
		plan = newUnresolvedAggregation(plan,
			logical.NewField(criteria.GetAgg().GetFieldName()),
			aggrFunc,
			criteria.GetGroupBy() != nil,
		)
		pushedLimit = math.MaxInt
	}

	if criteria.GetTop() != nil {
		plan = top(plan, criteria.GetTop())
	}

	plan = limit(plan, criteria.GetOffset(), limitParameter)

	var s logical.Schema
	var err error
	if len(ss) == 1 {
		s = ss[0]
	} else {
		if s, err = mergeSchema(ss); err != nil {
			return nil, err
		}
	}

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

func parseFields(criteria *measurev1.QueryRequest, metadata *commonv1.Metadata, ec executor.MeasureExecutionContext,
	groupByEntity bool, tagProjection [][]*logical.Tag,
) logical.UnresolvedPlan {
	projFields := make([]*logical.Field, len(criteria.GetFieldProjection().GetNames()))
	for i, fieldNameProj := range criteria.GetFieldProjection().GetNames() {
		projFields[i] = logical.NewField(fieldNameProj)
	}
	timeRange := criteria.GetTimeRange()
	return indexScan(timeRange.GetBegin().AsTime(), timeRange.GetEnd().AsTime(), metadata,
		tagProjection, projFields, groupByEntity, criteria.GetCriteria(), ec)
}
