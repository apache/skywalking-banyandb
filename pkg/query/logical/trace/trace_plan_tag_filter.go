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

package trace

import (
	"context"
	"fmt"
	"time"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

var _ logical.UnresolvedPlan = (*unresolvedTraceTagFilter)(nil)

type unresolvedTraceTagFilter struct {
	startTime      time.Time
	endTime        time.Time
	ec             executor.TraceExecutionContext
	metadata       *commonv1.Metadata
	criteria       *modelv1.Criteria
	projectionTags [][]*logical.Tag
}

func (uis *unresolvedTraceTagFilter) Analyze(s logical.Schema) (logical.Plan, error) {
	ctx := newTraceAnalyzerContext(s)
	entityList := s.EntityList()
	entityDict := make(map[string]int)
	for idx, e := range entityList {
		entityDict[e] = idx
	}
	var err error
	// For trace, we only use skipping filter (no inverted filter or entities)
	ctx.skippingFilter, err = buildTraceFilter(uis.criteria, s, entityDict)
	if err != nil {
		return nil, err
	}

	if len(uis.projectionTags) > 0 {
		// For trace, we use a single TagProjection since traces don't have tag families
		ctx.projectionTags = &model.TagProjection{
			Family: "", // Empty family name for trace
			Names:  make([]string, 0),
		}
		for i := range uis.projectionTags {
			for _, tag := range uis.projectionTags[i] {
				ctx.projectionTags.Names = append(ctx.projectionTags.Names, tag.GetTagName())
			}
		}
		var errProject error
		ctx.projTagsRefs, errProject = s.CreateTagRef(uis.projectionTags...)
		if errProject != nil {
			return nil, errProject
		}
	}
	plan := uis.selectTraceScanner(ctx, uis.ec)
	if uis.criteria != nil {
		tagFilter, errFilter := logical.BuildTagFilter(uis.criteria, entityDict, s, len(ctx.globalConditions) > 1)
		if errFilter != nil {
			return nil, errFilter
		}
		if tagFilter != logical.DummyFilter {
			// create tagFilter with a projected view
			plan = newTraceTagFilter(s.ProjTags(ctx.projTagsRefs...), plan, tagFilter)
		}
	}
	return plan, err
}

func (uis *unresolvedTraceTagFilter) selectTraceScanner(ctx *traceAnalyzeContext, ec executor.TraceExecutionContext) logical.Plan {
	return &localScan{
		timeRange:         timestamp.NewInclusiveTimeRange(uis.startTime, uis.endTime),
		schema:            ctx.s,
		projectionTagRefs: ctx.projTagsRefs,
		projectionTags:    ctx.projectionTags,
		metadata:          uis.metadata,
		skippingFilter:    ctx.skippingFilter,
		l:                 logger.GetLogger("query", "trace", "local-scan"),
		ec:                ec,
	}
}

func traceTagFilter(startTime, endTime time.Time, metadata *commonv1.Metadata, criteria *modelv1.Criteria,
	projection [][]*logical.Tag, ec executor.TraceExecutionContext,
) logical.UnresolvedPlan {
	return &unresolvedTraceTagFilter{
		startTime:      startTime,
		endTime:        endTime,
		metadata:       metadata,
		criteria:       criteria,
		projectionTags: projection,
		ec:             ec,
	}
}

type traceAnalyzeContext struct {
	s                logical.Schema
	skippingFilter   index.Filter
	projectionTags   *model.TagProjection
	globalConditions []interface{}
	projTagsRefs     [][]*logical.TagRef
}

func newTraceAnalyzerContext(s logical.Schema) *traceAnalyzeContext {
	return &traceAnalyzeContext{
		globalConditions: make([]interface{}, 0),
		s:                s,
	}
}

// buildTraceFilter builds a filter for trace queries.
// Unlike stream, trace only needs skipping filter.
func buildTraceFilter(criteria *modelv1.Criteria, s logical.Schema, entityDict map[string]int) (index.Filter, error) {
	if criteria == nil {
		return nil, nil
	}
	// Create a map of valid tag names from the schema
	tagNames := make(map[string]bool)
	// For trace schema, tags are stored directly (no tag families)
	for tagName := range s.(*schema).common.TagSpecMap {
		tagNames[tagName] = true
	}

	filter, _, err := buildFilter(criteria, tagNames, entityDict, nil)
	return filter, err
}

var (
	_ logical.Plan             = (*traceTagFilterPlan)(nil)
	_ executor.TraceExecutable = (*traceTagFilterPlan)(nil)
)

type traceTagFilterPlan struct {
	s         logical.Schema
	parent    logical.Plan
	tagFilter logical.TagFilter
}

func (t *traceTagFilterPlan) Close() {
	t.parent.(executor.TraceExecutable).Close()
}

func newTraceTagFilter(s logical.Schema, parent logical.Plan, tagFilter logical.TagFilter) logical.Plan {
	return &traceTagFilterPlan{
		s:         s,
		parent:    parent,
		tagFilter: tagFilter,
	}
}

func (t *traceTagFilterPlan) Execute(ctx context.Context) (model.TraceResult, error) {
	result, err := t.parent.(executor.TraceExecutable).Execute(ctx)
	if err != nil {
		return model.TraceResult{}, err
	}

	// For trace, we need to check if the result matches the tag filter
	// Convert trace tags to TagFamilies format for filter matching
	var tagFamilies []*modelv1.TagFamily
	if len(result.Tags) > 0 {
		// Create a single tag family for trace tags (trace has no families)
		family := &modelv1.TagFamily{
			Name: "",
			Tags: make([]*modelv1.Tag, len(result.Tags)),
		}
		for i, tag := range result.Tags {
			family.Tags[i] = &modelv1.Tag{
				Key:   tag.Name,
				Value: tag.Values[0], // Use the first value from the tag's Values slice
			}
		}
		tagFamilies = []*modelv1.TagFamily{family}
	}

	ok, err := t.tagFilter.Match(logical.TagFamilies(tagFamilies), t.s)
	if err != nil {
		return model.TraceResult{}, err
	}
	if !ok {
		return model.TraceResult{}, nil
	}

	return result, nil
}

func (t *traceTagFilterPlan) String() string {
	return fmt.Sprintf("%s trace-tag-filter:%s", t.parent, t.tagFilter.String())
}

func (t *traceTagFilterPlan) Children() []logical.Plan {
	return []logical.Plan{t.parent}
}

func (t *traceTagFilterPlan) Schema() logical.Schema {
	return t.s
}
