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
	"time"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

var _ UnresolvedPlan = (*unresolvedTagFilter)(nil)

type unresolvedTagFilter struct {
	unresolvedOrderBy *UnresolvedOrderBy
	startTime         time.Time
	endTime           time.Time
	metadata          *commonv1.Metadata
	conditions        []Expr
	projectionTags    [][]*Tag
	entity            tsdb.Entity
}

func (uis *unresolvedTagFilter) Analyze(s Schema) (Plan, error) {
	ctx := newStreamAnalyzerContext(s)
	for _, cond := range uis.conditions {
		resolvable, ok := cond.(ResolvableExpr)
		if !ok {
			continue
		}
		err := resolvable.Resolve(s)
		if err != nil {
			return nil, err
		}
		bCond, ok := cond.(*binaryExpr)
		if !ok {
			continue
		}
		tag := bCond.l.(*TagRef).tag
		defined, indexObj := s.IndexDefined(tag)
		if !defined {
			ctx.tagFilters = append(ctx.tagFilters, bCond)
			continue
		}
		switch indexObj.GetLocation() {
		case databasev1.IndexRule_LOCATION_SERIES:
			if v, exist := ctx.localConditionMap[indexObj]; exist {
				v = append(v, cond)
				ctx.localConditionMap[indexObj] = v
			} else {
				ctx.localConditionMap[indexObj] = []Expr{cond}
			}
		case databasev1.IndexRule_LOCATION_GLOBAL:
			ctx.globalConditions = append(ctx.globalConditions, indexObj, cond)
		}
	}

	if len(uis.projectionTags) > 0 {
		var err error
		ctx.projTagsRefs, err = s.CreateTagRef(uis.projectionTags...)
		if err != nil {
			return nil, err
		}
	}
	plan, err := uis.selectIndexScanner(ctx)
	if err != nil {
		return nil, err
	}
	if len(ctx.tagFilters) > 0 {
		plan = NewTagFilter(s, plan, ctx.tagFilters)
	}
	return plan, err
}

func (uis *unresolvedTagFilter) selectIndexScanner(ctx *streamAnalyzeContext) (Plan, error) {
	if len(ctx.globalConditions) > 0 {
		if len(ctx.globalConditions)/2 > 1 {
			return nil, ErrMultipleGlobalIndexes
		}
		return &globalIndexScan{
			schema:            ctx.s,
			projectionTagRefs: ctx.projTagsRefs,
			metadata:          uis.metadata,
			globalIndexRule:   ctx.globalConditions[0].(*databasev1.IndexRule),
			expr:              ctx.globalConditions[1].(Expr),
		}, nil
	}

	// resolve sub-plan with the projected view of streamSchema
	orderBySubPlan, err := uis.unresolvedOrderBy.analyze(ctx.s.ProjTags(ctx.projTagsRefs...))
	if err != nil {
		return nil, err
	}

	return &localIndexScan{
		orderBy:           orderBySubPlan,
		timeRange:         timestamp.NewInclusiveTimeRange(uis.startTime, uis.endTime),
		schema:            ctx.s,
		projectionTagRefs: ctx.projTagsRefs,
		metadata:          uis.metadata,
		conditionMap:      ctx.localConditionMap,
		entity:            uis.entity,
	}, nil
}

func TagFilter(startTime, endTime time.Time, metadata *commonv1.Metadata, conditions []Expr, entity tsdb.Entity,
	orderBy *UnresolvedOrderBy, projection ...[]*Tag,
) UnresolvedPlan {
	return &unresolvedTagFilter{
		unresolvedOrderBy: orderBy,
		startTime:         startTime,
		endTime:           endTime,
		metadata:          metadata,
		conditions:        conditions,
		projectionTags:    projection,
		entity:            entity,
	}
}

// GlobalIndexScan is a short-handed method for composing a globalIndexScan plan
func GlobalIndexScan(metadata *commonv1.Metadata, conditions []Expr, projection ...[]*Tag) UnresolvedPlan {
	return &unresolvedTagFilter{
		metadata:       metadata,
		conditions:     conditions,
		projectionTags: projection,
	}
}

type streamAnalyzeContext struct {
	s                 Schema
	localConditionMap map[*databasev1.IndexRule][]Expr
	globalConditions  []interface{}
	projTagsRefs      [][]*TagRef
	tagFilters        []*binaryExpr
}

func newStreamAnalyzerContext(s Schema) *streamAnalyzeContext {
	return &streamAnalyzeContext{
		localConditionMap: make(map[*databasev1.IndexRule][]Expr),
		globalConditions:  make([]interface{}, 0),
		s:                 s,
	}
}

var (
	_ Plan                      = (*tagFilter)(nil)
	_ executor.StreamExecutable = (*tagFilter)(nil)
)

type tagFilter struct {
	s          Schema
	parent     Plan
	tagFilters []*binaryExpr
}

func NewTagFilter(s Schema, parent Plan, tagFilters []*binaryExpr) Plan {
	return &tagFilter{
		s:          s,
		parent:     parent,
		tagFilters: tagFilters,
	}
}

func (t *tagFilter) Execute(ec executor.StreamExecutionContext) ([]*streamv1.Element, error) {
	entities, err := t.parent.(executor.StreamExecutable).Execute(ec)
	if err != nil {
		return nil, err
	}
	filteredElements := make([]*streamv1.Element, 0)
	for _, e := range entities {
		if t.check(e) {
			filteredElements = append(filteredElements, e)
		}
	}
	return filteredElements, nil
}

func (*tagFilter) String() string {
	panic("unimplemented")
}

func (t *tagFilter) Children() []Plan {
	return []Plan{t.parent}
}

func (*tagFilter) Equal(another Plan) bool {
	panic("unimplemented")
}

func (t *tagFilter) Schema() Schema {
	return t.s
}

func (*tagFilter) Type() PlanType {
	return PlanTagFilter
}

func (t *tagFilter) check(element *streamv1.Element) bool {
	compare := func(ce ComparableExpr, tagValue *modelv1.TagValue, exp func(result int) bool) bool {
		c, b := ce.Compare(tagValue)
		if b {
			return exp(c)
		}
		return false
	}
	for _, filter := range t.tagFilters {
		l := filter.l.(*TagRef)
		tagValue, exist := tagValue(element, l)
		if !exist {
			return false
		}
		r, ok := filter.r.(ComparableExpr)
		if !ok {
			continue
		}
		switch filter.op {
		case modelv1.Condition_BINARY_OP_EQ:
			return compare(r, tagValue, func(c int) bool { return c == 0 })
		case modelv1.Condition_BINARY_OP_GE:
			return compare(r, tagValue, func(c int) bool { return c >= 0 })
		case modelv1.Condition_BINARY_OP_GT:
			return compare(r, tagValue, func(c int) bool { return c > 0 })
		case modelv1.Condition_BINARY_OP_LE:
			return compare(r, tagValue, func(c int) bool { return c <= 0 })
		case modelv1.Condition_BINARY_OP_LT:
			return compare(r, tagValue, func(c int) bool { return c < 0 })
		case modelv1.Condition_BINARY_OP_HAVING:
			return r.BelongTo(tagValue)
		case modelv1.Condition_BINARY_OP_NOT_HAVING:
			return !r.BelongTo(tagValue)
		}
	}

	return true
}

func tagValue(element *streamv1.Element, tagRef *TagRef) (*modelv1.TagValue, bool) {
	for _, tf := range element.TagFamilies {
		if tf.Name != tagRef.tag.familyName {
			continue
		}
		for _, t := range tf.Tags {
			if t.Key == tagRef.tag.name {
				return t.Value, true
			}
		}
	}
	return nil, false
}
