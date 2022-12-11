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
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
)

// BuildSchema returns Schema loaded from the metadata repository.
func BuildSchema(measureSchema measure.Measure) (logical.Schema, error) {
	md := measureSchema.GetSchema()
	md.GetEntity()

	ms := &schema{
		common: &logical.CommonSchema{
			IndexRules: measureSchema.GetIndexRules(),
			TagMap:     make(map[string]*logical.TagSpec),
			EntityList: md.GetEntity().GetTagNames(),
		},
		measure:  md,
		fieldMap: make(map[string]*logical.FieldSpec),
	}

	for tagFamilyIdx, tagFamily := range md.GetTagFamilies() {
		for tagIdx, spec := range tagFamily.GetTags() {
			ms.registerTag(tagFamilyIdx, tagIdx, spec)
		}
	}

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
	plan, err := parseFields(criteria, metadata, s, groupByEntity)
	if err != nil {
		return nil, err
	}

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
		limitParameter = logical.DefaultLimit
	}
	plan = limit(plan, criteria.GetOffset(), limitParameter)

	return plan.Analyze(s)
}

// parseFields parses the query request to decide which kind of plan should be generated
// Basically,
// 1 - If no criteria is given, we can only scan all shards
// 2 - If criteria is given, but all of those fields exist in the "entity" definition.
func parseFields(criteria *measurev1.QueryRequest, metadata *commonv1.Metadata, s logical.Schema, groupByEntity bool) (logical.UnresolvedPlan, error) {
	timeRange := criteria.GetTimeRange()

	projTags := make([][]*logical.Tag, len(criteria.GetTagProjection().GetTagFamilies()))
	for i, tagFamily := range criteria.GetTagProjection().GetTagFamilies() {
		var projTagInFamily []*logical.Tag
		for _, tagName := range tagFamily.GetTags() {
			projTagInFamily = append(projTagInFamily, logical.NewTag(tagFamily.GetName(), tagName))
		}
		projTags[i] = projTagInFamily
	}

	projFields := make([]*logical.Field, len(criteria.GetFieldProjection().GetNames()))
	for i, fieldNameProj := range criteria.GetFieldProjection().GetNames() {
		projFields[i] = logical.NewField(fieldNameProj)
	}

	entityList := s.EntityList()
	entityMap := make(map[string]int)
	entity := make([]tsdb.Entry, len(entityList))
	for idx, e := range entityList {
		entityMap[e] = idx
		// fill AnyEntry by default
		entity[idx] = tsdb.AnyEntry
	}
	filter, entities, err := logical.BuildLocalFilter(criteria.GetCriteria(), s, entityMap, entity)
	if err != nil {
		return nil, err
	}

	// parse orderBy
	queryOrder := criteria.GetOrderBy()
	var unresolvedOrderBy *logical.UnresolvedOrderBy
	if queryOrder != nil {
		unresolvedOrderBy = logical.NewOrderBy(queryOrder.GetIndexRuleName(), queryOrder.GetSort())
	}

	return indexScan(timeRange.GetBegin().AsTime(), timeRange.GetEnd().AsTime(), metadata,
		filter, entities, projTags, projFields, groupByEntity, unresolvedOrderBy), nil
}
