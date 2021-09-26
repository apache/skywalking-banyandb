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

	"github.com/pkg/errors"

	commonv2 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v2"
	modelv2 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v2"
	streamv2 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v2"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	metadataSchema "github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/apache/skywalking-banyandb/pkg/convert"
)

var (
	ErrFieldNotDefined            = errors.New("field is not defined")
	ErrInvalidConditionType       = errors.New("invalid pair type")
	ErrIncompatibleQueryCondition = errors.New("incompatible query condition type")
	ErrInvalidSchema              = errors.New("invalid schema")
	ErrIndexNotDefined            = errors.New("index is not define for the field")
	ErrTraceIDWrongType           = errors.New("trace id type should be string")
)

var (
	DefaultLimit uint32 = 20
)

type Tag struct {
	familyName, name string
}

func NewTag(family, name string) *Tag {
	return &Tag{
		familyName: family,
		name:       name,
	}
}

// NewTags create an array of Tag within a TagFamily
func NewTags(family string, tagNames ...string) []*Tag {
	tags := make([]*Tag, len(tagNames))
	for i, name := range tagNames {
		tags[i] = NewTag(family, name)
	}
	return tags
}

// GetCompoundName is only used for error message
func (t *Tag) GetCompoundName() string {
	return t.familyName + ":" + t.name
}

func (t *Tag) GetTagName() string {
	return t.name
}

func (t *Tag) GetFamilyName() string {
	return t.familyName
}

type Analyzer struct {
	indexRuleRepo        metadataSchema.IndexRule
	indexRuleBindingRepo metadataSchema.IndexRuleBinding
	metadataRepoImpl     metadata.Repo
}

func DefaultAnalyzer() *Analyzer {
	indexRule, _ := metadataSchema.NewIndexRule()
	indexRuleBinding, _ := metadataSchema.NewIndexRuleBinding()
	metadataService, _ := metadata.NewService(context.TODO())

	return &Analyzer{
		indexRule,
		indexRuleBinding,
		metadataService,
	}
}

func (a *Analyzer) BuildStreamSchema(ctx context.Context, metadata *commonv2.Metadata) (Schema, error) {
	stream, err := a.metadataRepoImpl.Stream().Get(ctx, metadata)

	if err != nil {
		return nil, err
	}

	indexRules, err := a.metadataRepoImpl.IndexRules(context.TODO(), metadata)

	if err != nil {
		return nil, err
	}

	s := &schema{
		stream:     stream,
		indexRules: indexRules,
		fieldMap:   make(map[string]*tagSpec),
		entityList: stream.GetEntity().GetTagNames(),
	}

	// generate the schema of the fields for the traceSeries
	for tagFamilyIdx, tagFamily := range stream.GetTagFamilies() {
		for tagIdx, spec := range tagFamily.GetTags() {
			s.registerField(spec.GetName(), tagFamilyIdx, tagIdx, spec)
		}
	}

	return s, nil
}

func (a *Analyzer) Analyze(_ context.Context, criteria *streamv2.QueryRequest, metadata *commonv2.Metadata, s Schema) (Plan, error) {
	// parse fields
	plan, err := parseFields(criteria, metadata, s)
	if err != nil {
		return nil, err
	}

	if plan.Type() == PlanTraceIDFetch {
		return plan.Analyze(s)
	}

	// parse orderBy
	queryOrder := criteria.GetOrderBy()
	if queryOrder != nil {
		switch plan.Type() {
		case PlanIndexScan:
			plan.(*unresolvedIndexScan).unresolvedOrderBy = OrderBy(queryOrder.GetIndexRuleName(), queryOrder.GetSort())
		}
	}

	// parse offset
	plan = Offset(plan, criteria.GetOffset())

	// parse limit
	limitParameter := criteria.GetLimit()
	if limitParameter == 0 {
		limitParameter = DefaultLimit
	}
	plan = Limit(plan, limitParameter)

	return plan.Analyze(s)
}

// parseFields parses the query request to decide which kind of plan should be generated
// Basically,
// 1 - If no criteria is given, we can only scan all shards
// 2 - If criteria is given, but all of those fields exist in the "entity" definition,
//     i.e. they are top-level sharding keys. For example, for the current skywalking's schema,
//     we use service_id + service_instance_id + state as the compound sharding keys.
func parseFields(criteria *streamv2.QueryRequest, metadata *commonv2.Metadata, s Schema) (UnresolvedPlan, error) {
	timeRange := criteria.GetTimeRange()

	projTags := make([][]*Tag, len(criteria.GetProjection().GetTagFamilies()))
	for i, tagFamily := range criteria.GetProjection().GetTagFamilies() {
		var projTagInFamily []*Tag
		for _, tagName := range tagFamily.GetTags() {
			projTagInFamily = append(projTagInFamily, NewTag(tagFamily.GetName(), tagName))
		}
		projTags[i] = projTagInFamily
	}

	var plan UnresolvedPlan
	var fieldExprs []Expr

	entityList := s.EntityList()
	entityMap := make(map[string]int)
	entity := make([]tsdb.Entry, len(entityList))
	for idx, e := range entityList {
		entityMap[e] = idx
		// fill AnyEntry by default
		entity[idx] = tsdb.AnyEntry
	}

fieldsLoop:
	for _, criteriaFamily := range criteria.GetCriteria() {
		for _, pairQuery := range criteriaFamily.GetConditions() {
			op := pairQuery.GetOp()
			typedTagValue := pairQuery.GetValue()
			switch pairQuery.GetName() {
			case s.TraceIDFieldName():
				traceIDPair := typedTagValue.GetStr()
				if traceIDPair == nil {
					return nil, ErrTraceIDWrongType
				}
				plan = TraceIDFetch(typedTagValue.GetStr().GetValue(), metadata, projTags...)
				break fieldsLoop
			}
			var e Expr
			switch v := typedTagValue.GetValue().(type) {
			case *modelv2.TagValue_Str:
				if entityIdx, ok := entityMap[pairQuery.GetName()]; ok {
					entity[entityIdx] = []byte(v.Str.GetValue())
				} else {
					e = &strLiteral{
						string: v.Str.GetValue(),
					}
				}
			case *modelv2.TagValue_StrArray:
				e = &strArrLiteral{
					arr: v.StrArray.GetValue(),
				}
			case *modelv2.TagValue_Int:
				if entityIdx, ok := entityMap[pairQuery.GetName()]; ok {
					entity[entityIdx] = convert.Int64ToBytes(v.Int.GetValue())
				} else {
					e = &int64Literal{
						int64: v.Int.GetValue(),
					}
				}
			case *modelv2.TagValue_IntArray:
				e = &int64ArrLiteral{
					arr: v.IntArray.GetValue(),
				}
			default:
				return nil, ErrInvalidConditionType
			}
			// we collect Condition only if it is not a part of entity
			if e != nil {
				fieldExprs = append(fieldExprs, binaryOpFactory[op](NewFieldRef(criteriaFamily.GetTagFamilyName(), pairQuery.GetName()), e))
			}
		}
	}

	// if plan is already assigned, skip
	if plan != nil {
		return plan, nil
	}

	return IndexScan(timeRange.GetBegin().AsTime(), timeRange.GetEnd().AsTime(), metadata,
		fieldExprs, entity, nil, projTags...), nil
}
