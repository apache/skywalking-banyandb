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
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

var _ UnresolvedPlan = (*unresolvedTagFilter)(nil)

type unresolvedTagFilter struct {
	unresolvedOrderBy *UnresolvedOrderBy
	startTime         time.Time
	endTime           time.Time
	metadata          *commonv1.Metadata
	projectionTags    [][]*Tag
	criteria          *modelv1.Criteria
}

func (uis *unresolvedTagFilter) Analyze(s Schema) (Plan, error) {
	ctx := newStreamAnalyzerContext(s)
	entityList := s.EntityList()
	entityDict := make(map[string]int)
	entity := make([]tsdb.Entry, len(entityList))
	for idx, e := range entityList {
		entityDict[e] = idx
		// fill AnyEntry by default
		entity[idx] = tsdb.AnyEntry
	}
	var err error
	ctx.filter, ctx.entities, err = buildLocalFilter(uis.criteria, s, entityDict, entity)
	if err != nil {
		if ge, ok := err.(*globalIndexError); ok {
			ctx.globalConditions = append(ctx.globalConditions, ge.indexRule, ge.expr)
		} else {
			return nil, err
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
	tagFilter, err := buildTagFilter(uis.criteria, s)
	if err != nil {
		return nil, err
	}
	plan = NewTagFilter(s, plan, tagFilter)
	return plan, err
}

func (uis *unresolvedTagFilter) selectIndexScanner(ctx *streamAnalyzeContext) (Plan, error) {
	if len(ctx.globalConditions) > 0 {
		if len(ctx.globalConditions) > 2 {
			return nil, ErrMultipleGlobalIndexes
		}
		return &globalIndexScan{
			schema:            ctx.s,
			projectionTagRefs: ctx.projTagsRefs,
			metadata:          uis.metadata,
			globalIndexRule:   ctx.globalConditions[0].(*databasev1.IndexRule),
			expr:              ctx.globalConditions[1].(LiteralExpr),
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
		filter:            ctx.filter,
		entities:          ctx.entities,
	}, nil
}

func TagFilter(startTime, endTime time.Time, metadata *commonv1.Metadata, criteria *modelv1.Criteria,
	orderBy *UnresolvedOrderBy, projection ...[]*Tag,
) UnresolvedPlan {
	return &unresolvedTagFilter{
		unresolvedOrderBy: orderBy,
		startTime:         startTime,
		endTime:           endTime,
		metadata:          metadata,
		criteria:          criteria,
		projectionTags:    projection,
	}
}

type streamAnalyzeContext struct {
	s                Schema
	filter           index.Filter
	entities         []tsdb.Entity
	globalConditions []interface{}
	projTagsRefs     [][]*TagRef
}

func newStreamAnalyzerContext(s Schema) *streamAnalyzeContext {
	return &streamAnalyzeContext{
		globalConditions: make([]interface{}, 0),
		s:                s,
	}
}

var (
	_ Plan                      = (*tagFilterPlan)(nil)
	_ executor.StreamExecutable = (*tagFilterPlan)(nil)
)

type tagFilterPlan struct {
	s         Schema
	parent    Plan
	tagFilter tagFilter
}

func NewTagFilter(s Schema, parent Plan, tagFilter tagFilter) Plan {
	return &tagFilterPlan{
		s:         s,
		parent:    parent,
		tagFilter: tagFilter,
	}
}

func (t *tagFilterPlan) Execute(ec executor.StreamExecutionContext) ([]*streamv1.Element, error) {
	entities, err := t.parent.(executor.StreamExecutable).Execute(ec)
	if err != nil {
		return nil, err
	}
	filteredElements := make([]*streamv1.Element, 0)
	for _, e := range entities {
		ok, err := t.tagFilter.match(e.TagFamilies)
		if err != nil {
			return nil, err
		}
		if ok {
			filteredElements = append(filteredElements, e)
		}
	}
	return filteredElements, nil
}

func (t *tagFilterPlan) String() string {
	return t.tagFilter.String()
}

func (t *tagFilterPlan) Children() []Plan {
	return []Plan{t.parent}
}

func (t *tagFilterPlan) Schema() Schema {
	return t.s
}
