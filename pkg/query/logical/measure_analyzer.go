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

package logical

import (
	"context"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
)

type Field struct {
	name string
}

func NewField(name string) *Field {
	return &Field{name: name}
}

type MeasureAnalyzer struct {
	metadataRepoImpl metadata.Repo
}

func CreateMeasureAnalyzerFromMetaService(metaSvc metadata.Service) (*MeasureAnalyzer, error) {
	return &MeasureAnalyzer{
		metaSvc,
	}, nil
}

func (a *MeasureAnalyzer) BuildMeasureSchema(ctx context.Context, metadata *commonv1.Metadata) (Schema, error) {
	group, err := a.metadataRepoImpl.GroupRegistry().GetGroup(ctx, metadata.GetGroup())
	if err != nil {
		return nil, err
	}
	measure, err := a.metadataRepoImpl.MeasureRegistry().GetMeasure(ctx, metadata)
	if err != nil {
		return nil, err
	}

	indexRules, err := a.metadataRepoImpl.IndexRules(context.TODO(), metadata)
	if err != nil {
		return nil, err
	}

	ms := &measureSchema{
		common: &commonSchema{
			group:      group,
			indexRules: indexRules,
			tagMap:     make(map[string]*tagSpec),
			entityList: measure.GetEntity().GetTagNames(),
		},
		measure:  measure,
		fieldMap: make(map[string]*fieldSpec),
	}

	for tagFamilyIdx, tagFamily := range measure.GetTagFamilies() {
		for tagIdx, spec := range tagFamily.GetTags() {
			ms.registerTag(tagFamilyIdx, tagIdx, spec)
		}
	}

	for fieldIdx, spec := range measure.GetFields() {
		ms.registerField(fieldIdx, spec)
	}

	return ms, nil
}

func (a *MeasureAnalyzer) Analyze(_ context.Context, criteria *measurev1.QueryRequest, metadata *commonv1.Metadata, s Schema) (Plan, error) {
	groupByEntity := false
	var groupByTags [][]*Tag
	if criteria.GetGroupBy() != nil {
		groupByProjectionTags := criteria.GetGroupBy().GetTagProjection()
		groupByTags = make([][]*Tag, len(groupByProjectionTags.GetTagFamilies()))
		tags := make([]string, 0)
		for i, tagFamily := range groupByProjectionTags.GetTagFamilies() {
			groupByTags[i] = NewTags(tagFamily.GetName(), tagFamily.GetTags()...)
			tags = append(tags, tagFamily.GetTags()...)
		}
		if stringSlicesEqual(s.EntityList(), tags) {
			groupByEntity = true
		}
	}

	// parse fields
	plan, err := parseMeasureFields(criteria, metadata, s, groupByEntity)
	if err != nil {
		return nil, err
	}

	if criteria.GetGroupBy() != nil {
		plan = GroupBy(plan, groupByTags, groupByEntity)
	}

	if criteria.GetAgg() != nil {
		plan = Aggregation(plan,
			NewField(criteria.GetAgg().GetFieldName()),
			criteria.GetAgg().GetFunction(),
			criteria.GetGroupBy() != nil,
		)
	}

	if criteria.GetTop() != nil {
		plan = Top(plan, criteria.GetTop())
	}

	// parse limit and offset
	limitParameter := criteria.GetLimit()
	if limitParameter == 0 {
		limitParameter = DefaultLimit
	}
	plan = MeasureLimit(plan, criteria.GetOffset(), limitParameter)

	return plan.Analyze(s)
}

func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}

// parseMeasureFields parses the query request to decide which kind of plan should be generated
// Basically,
// 1 - If no criteria is given, we can only scan all shards
// 2 - If criteria is given, but all of those fields exist in the "entity" definition
func parseMeasureFields(criteria *measurev1.QueryRequest, metadata *commonv1.Metadata, s Schema, groupByEntity bool) (UnresolvedPlan, error) {
	timeRange := criteria.GetTimeRange()

	projTags := make([][]*Tag, len(criteria.GetTagProjection().GetTagFamilies()))
	for i, tagFamily := range criteria.GetTagProjection().GetTagFamilies() {
		var projTagInFamily []*Tag
		for _, tagName := range tagFamily.GetTags() {
			projTagInFamily = append(projTagInFamily, NewTag(tagFamily.GetName(), tagName))
		}
		projTags[i] = projTagInFamily
	}

	projFields := make([]*Field, len(criteria.GetFieldProjection().GetNames()))
	for i, fieldNameProj := range criteria.GetFieldProjection().GetNames() {
		projFields[i] = NewField(fieldNameProj)
	}

	entityList := s.EntityList()
	entityMap := make(map[string]int)
	entity := make([]tsdb.Entry, len(entityList))
	for idx, e := range entityList {
		entityMap[e] = idx
		// fill AnyEntry by default
		entity[idx] = tsdb.AnyEntry
	}
	predicator, entities, err := buildLocalFilter(criteria.Criteria, s, entityMap, entity)
	if err != nil {
		return nil, err
	}

	// parse orderBy
	queryOrder := criteria.GetOrderBy()
	var unresolvedOrderBy *UnresolvedOrderBy
	if queryOrder != nil {
		unresolvedOrderBy = OrderBy(queryOrder.GetIndexRuleName(), queryOrder.GetSort())
	}

	return MeasureIndexScan(timeRange.GetBegin().AsTime(), timeRange.GetEnd().AsTime(), metadata,
		predicator, entities, projTags, projFields, groupByEntity, unresolvedOrderBy), nil
}
