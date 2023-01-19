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

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	"github.com/apache/skywalking-banyandb/banyand/measure"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
)

const defaultLimit uint32 = 100

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
func Analyze(_ context.Context, criteria *measurev1.QueryRequest, metadata *commonv1.Metadata, s logical.Schema) (logical.Plan, error) {
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

	if criteria.GetGroupBy() != nil {
		plan = newUnresolvedGroupBy(plan, groupByTags, groupByEntity)
	}

	if criteria.GetAgg() != nil {
		plan = newUnresolvedAggregation(plan,
			logical.NewField(criteria.GetAgg().GetFieldName()),
			criteria.GetAgg().GetFunction(),
			criteria.GetGroupBy() != nil,
		)
	}

	if criteria.GetTop() != nil {
		plan = top(plan, criteria.GetTop())
	}

	// parse limit and offset
	limitParameter := criteria.GetLimit()
	if limitParameter == 0 {
		limitParameter = defaultLimit
	}
	plan = limit(plan, criteria.GetOffset(), limitParameter)
	p, err := plan.Analyze(s)
	if err != nil {
		return nil, err
	}
	rules := []logical.OptimizeRule{
		logical.NewPushDownOrder(criteria.OrderBy),
		logical.NewPushDownMaxSize(int(limitParameter + criteria.GetOffset())),
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
func parseFields(criteria *measurev1.QueryRequest, metadata *commonv1.Metadata, groupByEntity bool) logical.UnresolvedPlan {
	projFields := make([]*logical.Field, len(criteria.GetFieldProjection().GetNames()))
	for i, fieldNameProj := range criteria.GetFieldProjection().GetNames() {
		projFields[i] = logical.NewField(fieldNameProj)
	}
	timeRange := criteria.GetTimeRange()
	return indexScan(timeRange.GetBegin().AsTime(), timeRange.GetEnd().AsTime(), metadata,
		logical.ToTags(criteria.GetTagProjection()), projFields, groupByEntity, criteria.GetCriteria())
}
