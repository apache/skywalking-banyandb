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

package stream

import (
	"fmt"
	"time"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

var _ logical.UnresolvedPlan = (*unresolvedTagFilter)(nil)

type unresolvedTagFilter struct {
	unresolvedOrderBy *logical.UnresolvedOrderBy
	startTime         time.Time
	endTime           time.Time
	metadata          *commonv1.Metadata
	projectionTags    [][]*logical.Tag
	criteria          *modelv1.Criteria
}

func (uis *unresolvedTagFilter) Analyze(s logical.Schema) (logical.Plan, error) {
	ctx := newAnalyzerContext(s)
	entityList := s.EntityList()
	entityDict := make(map[string]int)
	entity := make([]tsdb.Entry, len(entityList))
	for idx, e := range entityList {
		entityDict[e] = idx
		// fill AnyEntry by default
		entity[idx] = tsdb.AnyEntry
	}
	var err error
	ctx.filter, ctx.entities, err = logical.BuildLocalFilter(uis.criteria, s, entityDict, entity)
	if err != nil {
		if ge, ok := err.(*logical.GlobalIndexError); ok {
			ctx.globalConditions = append(ctx.globalConditions, ge.IndexRule, ge.Expr)
		} else {
			return nil, err
		}
	}

	if len(uis.projectionTags) > 0 {
		var errProject error
		ctx.projTagsRefs, errProject = s.CreateTagRef(uis.projectionTags...)
		if errProject != nil {
			return nil, errProject
		}
	}
	plan, err := uis.selectIndexScanner(ctx)
	if err != nil {
		return nil, err
	}
	if uis.criteria != nil {
		tagFilter, errFilter := logical.BuildTagFilter(uis.criteria, entityDict, s, len(ctx.globalConditions) > 1)
		if errFilter != nil {
			return nil, errFilter
		}
		if tagFilter != logical.BypassFilter {
			plan = NewTagFilter(s, plan, tagFilter)
		}
	}
	return plan, err
}

func (uis *unresolvedTagFilter) selectIndexScanner(ctx *analyzeContext) (logical.Plan, error) {
	if len(ctx.globalConditions) > 0 {
		if len(ctx.globalConditions) > 2 {
			return nil, logical.ErrMultipleGlobalIndexes
		}
		return &globalIndexScan{
			schema:            ctx.s,
			projectionTagRefs: ctx.projTagsRefs,
			metadata:          uis.metadata,
			globalIndexRule:   ctx.globalConditions[0].(*databasev1.IndexRule),
			expr:              ctx.globalConditions[1].(logical.LiteralExpr),
		}, nil
	}

	// resolve sub-plan with the projected view of streamSchema
	orderBySubPlan, err := uis.unresolvedOrderBy.Analyze(ctx.s.ProjTags(ctx.projTagsRefs...))
	if err != nil {
		return nil, err
	}

	return &localIndexScan{
		OrderBy:           orderBySubPlan,
		timeRange:         timestamp.NewInclusiveTimeRange(uis.startTime, uis.endTime),
		schema:            ctx.s,
		projectionTagRefs: ctx.projTagsRefs,
		metadata:          uis.metadata,
		filter:            ctx.filter,
		entities:          ctx.entities,
		l:                 logger.GetLogger("query", "stream", "local-index"),
	}, nil
}

func TagFilter(startTime, endTime time.Time, metadata *commonv1.Metadata, criteria *modelv1.Criteria,
	orderBy *logical.UnresolvedOrderBy, projection ...[]*logical.Tag,
) logical.UnresolvedPlan {
	return &unresolvedTagFilter{
		unresolvedOrderBy: orderBy,
		startTime:         startTime,
		endTime:           endTime,
		metadata:          metadata,
		criteria:          criteria,
		projectionTags:    projection,
	}
}

type analyzeContext struct {
	s                logical.Schema
	filter           index.Filter
	entities         []tsdb.Entity
	globalConditions []interface{}
	projTagsRefs     [][]*logical.TagRef
}

func newAnalyzerContext(s logical.Schema) *analyzeContext {
	return &analyzeContext{
		globalConditions: make([]interface{}, 0),
		s:                s,
	}
}

var (
	_ logical.Plan              = (*tagFilterPlan)(nil)
	_ executor.StreamExecutable = (*tagFilterPlan)(nil)
)

type tagFilterPlan struct {
	s         logical.Schema
	parent    logical.Plan
	tagFilter logical.TagFilter
}

func NewTagFilter(s logical.Schema, parent logical.Plan, tagFilter logical.TagFilter) logical.Plan {
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
		ok, err := t.tagFilter.Match(e.TagFamilies)
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
	return fmt.Sprintf("%s tag-filter:%s", t.parent, t.tagFilter.String())
}

func (t *tagFilterPlan) Children() []logical.Plan {
	return []logical.Plan{t.parent}
}

func (t *tagFilterPlan) Schema() logical.Schema {
	return t.s
}
