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
	traceIDTagName string
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
	var conditionTagNames []string
	var traceIDs []string
	// For trace, we only use skipping filter (no inverted filter or entities)
	ctx.skippingFilter, conditionTagNames, traceIDs, err = buildTraceFilter(uis.criteria, s, entityDict, uis.traceIDTagName)
	if err != nil {
		return nil, err
	}

	// Initialize projectionTags even if no explicit projection tags are provided
	ctx.projectionTags = &model.TagProjection{
		Family: "", // Empty family name for trace
		Names:  make([]string, 0),
	}

	// Add explicitly requested projection tags
	if len(uis.projectionTags) > 0 {
		for i := range uis.projectionTags {
			for _, tag := range uis.projectionTags[i] {
				ctx.projectionTags.Names = append(ctx.projectionTags.Names, tag.GetTagName())
			}
		}
	}

	// Add tag names from filter conditions to projection
	if len(conditionTagNames) > 0 {
		ctx.projectionTags.Names = append(ctx.projectionTags.Names, conditionTagNames...)
	}

	// Deduplicate tag names
	ctx.projectionTags.Names = deduplicateStrings(ctx.projectionTags.Names)

	// Create tag references if we have any projection tags
	if len(ctx.projectionTags.Names) > 0 {
		var errProject error
		ctx.projTagsRefs, errProject = s.CreateTagRef(uis.projectionTags...)
		if errProject != nil {
			return nil, errProject
		}
	}
	plan := uis.selectTraceScanner(ctx, uis.ec, traceIDs)
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

func (uis *unresolvedTraceTagFilter) selectTraceScanner(ctx *traceAnalyzeContext, ec executor.TraceExecutionContext, traceIDs []string) logical.Plan {
	return &localScan{
		timeRange:         timestamp.NewInclusiveTimeRange(uis.startTime, uis.endTime),
		schema:            ctx.s,
		projectionTagRefs: ctx.projTagsRefs,
		projectionTags:    ctx.projectionTags,
		metadata:          uis.metadata,
		skippingFilter:    ctx.skippingFilter,
		l:                 logger.GetLogger("query", "trace", "local-scan"),
		ec:                ec,
		traceIDs:          traceIDs,
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

// deduplicateStrings removes duplicate strings from a slice while preserving order.
func deduplicateStrings(strings []string) []string {
	seen := make(map[string]bool)
	var result []string
	for _, str := range strings {
		if !seen[str] {
			seen[str] = true
			result = append(result, str)
		}
	}
	return result
}

// buildTraceFilter builds a filter for trace queries and returns both the filter and collected tag names.
// Unlike stream, trace only needs skipping filter.
func buildTraceFilter(criteria *modelv1.Criteria, s logical.Schema, entityDict map[string]int, traceIDTagName string) (index.Filter, []string, []string, error) {
	if criteria == nil {
		return nil, nil, nil, nil
	}
	// Create a map of valid tag names from the schema
	tagNames := make(map[string]bool)
	// For trace schema, tags are stored directly (no tag families)
	for tagName := range s.(*schema).common.TagSpecMap {
		tagNames[tagName] = true
	}

	filter, _, collectedTagNames, traceIDs, err := buildFilter(criteria, tagNames, entityDict, nil, traceIDTagName)
	return filter, collectedTagNames, traceIDs, err
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

	// If there are no tags, no need to check the filter
	if len(result.Tags) == 0 {
		return model.TraceResult{}, nil
	}

	// For trace, we need to check if ANY row matches the tag filter
	// Since result.Tags is column-based, we need to check each row (combination of tag values at same index)
	// First, determine the number of rows by finding the maximum length of Values slices
	maxRows := 0
	for _, tag := range result.Tags {
		if len(tag.Values) > maxRows {
			maxRows = len(tag.Values)
		}
	}

	// Check each row to see if any matches the filter
	for rowIdx := 0; rowIdx < maxRows; rowIdx++ {
		// Create TagFamilies for this specific row
		family := &modelv1.TagFamily{
			Name: "",
			Tags: make([]*modelv1.Tag, 0, len(result.Tags)),
		}

		// Build the row by taking the value at rowIdx from each tag (if it exists)
		for _, tag := range result.Tags {
			if rowIdx < len(tag.Values) {
				family.Tags = append(family.Tags, &modelv1.Tag{
					Key:   tag.Name,
					Value: tag.Values[rowIdx],
				})
			}
		}

		tagFamilies := []*modelv1.TagFamily{family}
		ok, err := t.tagFilter.Match(logical.TagFamilies(tagFamilies), t.s)
		if err != nil {
			return model.TraceResult{}, err
		}

		// If ANY row matches, return the entire result
		if ok {
			return result, nil
		}
	}

	// No rows matched the filter
	return model.TraceResult{}, nil
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
