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
	"context"
	"fmt"
	"time"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

var _ logical.UnresolvedPlan = (*unresolvedTagFilter)(nil)

type unresolvedTagFilter struct {
	startTime      time.Time
	endTime        time.Time
	ec             executor.StreamExecutionContext
	metadata       *commonv1.Metadata
	criteria       *modelv1.Criteria
	projectionTags [][]*logical.Tag
}

func (uis *unresolvedTagFilter) Analyze(s logical.Schema) (logical.Plan, error) {
	ctx := newAnalyzerContext(s)
	entityList := s.EntityList()
	entityDict := make(map[string]int)
	entity := make([]*modelv1.TagValue, len(entityList))
	for idx, e := range entityList {
		entityDict[e] = idx
		// fill AnyEntry by default
		entity[idx] = pbv1.AnyTagValue
	}
	var err error
	ctx.filter, ctx.entities, err = buildLocalFilter(uis.criteria, s, entityDict, entity)
	if err != nil {
		return nil, err
	}

	projTags := make([]model.TagProjection, len(uis.projectionTags))
	if len(uis.projectionTags) > 0 {
		for i := range uis.projectionTags {
			for _, tag := range uis.projectionTags[i] {
				projTags[i].Family = tag.GetFamilyName()
				projTags[i].Names = append(projTags[i].Names, tag.GetTagName())
			}
		}
		var errProject error
		ctx.projTagsRefs, errProject = s.CreateTagRef(uis.projectionTags...)
		if errProject != nil {
			return nil, errProject
		}
	}
	ctx.projectionTags = projTags
	plan := uis.selectIndexScanner(ctx, uis.ec)
	if uis.criteria != nil {
		tagFilter, errFilter := logical.BuildTagFilter(uis.criteria, entityDict, s, len(ctx.globalConditions) > 1)
		if errFilter != nil {
			return nil, errFilter
		}
		if tagFilter != logical.DummyFilter {
			// create tagFilter with a projected view
			plan = newTagFilter(s.ProjTags(ctx.projTagsRefs...), plan, tagFilter)
		}
	}
	return plan, err
}

func (uis *unresolvedTagFilter) selectIndexScanner(ctx *analyzeContext, ec executor.StreamExecutionContext) logical.Plan {
	return &localIndexScan{
		timeRange:         timestamp.NewInclusiveTimeRange(uis.startTime, uis.endTime),
		schema:            ctx.s,
		projectionTagRefs: ctx.projTagsRefs,
		projectionTags:    ctx.projectionTags,
		metadata:          uis.metadata,
		filter:            ctx.filter,
		entities:          ctx.entities,
		l:                 logger.GetLogger("query", "stream", "local-index"),
		ec:                ec,
	}
}

func tagFilter(startTime, endTime time.Time, metadata *commonv1.Metadata, criteria *modelv1.Criteria,
	projection [][]*logical.Tag, ec executor.StreamExecutionContext,
) logical.UnresolvedPlan {
	return &unresolvedTagFilter{
		startTime:      startTime,
		endTime:        endTime,
		metadata:       metadata,
		criteria:       criteria,
		projectionTags: projection,
		ec:             ec,
	}
}

type analyzeContext struct {
	s                logical.Schema
	filter           index.Filter
	entities         [][]*modelv1.TagValue
	projectionTags   []model.TagProjection
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

func (t *tagFilterPlan) Close() {
	t.parent.(executor.StreamExecutable).Close()
}

func newTagFilter(s logical.Schema, parent logical.Plan, tagFilter logical.TagFilter) logical.Plan {
	return &tagFilterPlan{
		s:         s,
		parent:    parent,
		tagFilter: tagFilter,
	}
}

func (t *tagFilterPlan) Execute(ec context.Context) ([]*streamv1.Element, error) {
	var filteredElements []*streamv1.Element

	for {
		entities, err := t.parent.(executor.StreamExecutable).Execute(ec)
		if err != nil {
			return nil, err
		}
		if len(entities) == 0 {
			break
		}
		for _, e := range entities {
			ok, err := t.tagFilter.Match(logical.TagFamilies(e.TagFamilies), t.s)
			if err != nil {
				return nil, err
			}
			if ok {
				filteredElements = append(filteredElements, e)
			}
		}
		if len(filteredElements) > 0 {
			break
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
