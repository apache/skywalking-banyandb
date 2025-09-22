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
	"math"
	"time"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/iter"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
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
	orderByTag     string
	projectionTags [][]*logical.Tag
}

func (uis *unresolvedTraceTagFilter) Analyze(s logical.Schema) (logical.Plan, error) {
	ctx := newTraceAnalyzerContext(s)
	entityList := s.EntityList()
	entityDict := make(map[string]int)
	entity := make([]*modelv1.TagValue, len(entityList))
	for idx, e := range entityList {
		entityDict[e] = idx
		// fill AnyEntry by default
		entity[idx] = pbv1.AnyTagValue
	}
	var err error
	var conditionTagNames []string
	var traceIDs []string
	var entities [][]*modelv1.TagValue
	var minVal, maxVal int64
	// For trace, we use skipping filter and capture entities for query optimization
	ctx.skippingFilter, entities, conditionTagNames, traceIDs, minVal, maxVal, err = buildTraceFilter(
		uis.criteria, s, entityDict, entity, uis.traceIDTagName, uis.orderByTag)
	if err != nil {
		return nil, err
	}
	if uis.orderByTag == "" {
		minVal = uis.startTime.UnixNano()
		maxVal = uis.endTime.UnixNano()
	}
	ctx.entities = entities

	// Initialize projectionTags even if no explicit projection tags are provided
	ctx.projectionTags = &model.TagProjection{
		Family: "", // Empty family name for trace
		Names:  make([]string, 0),
	}

	// Add explicitly requested projection tags
	if len(uis.projectionTags) > 0 {
		for i := range uis.projectionTags {
			for _, tag := range uis.projectionTags[i] {
				if tag.GetTagName() == uis.traceIDTagName {
					continue
				}
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
		tags := make([]*logical.Tag, len(ctx.projectionTags.Names))
		for i := range ctx.projectionTags.Names {
			tags[i] = logical.NewTag("", ctx.projectionTags.Names[i])
		}
		var errProject error
		ctx.projTagsRefs, errProject = s.CreateTagRef(tags)
		if errProject != nil {
			return nil, errProject
		}
	}
	plan := uis.selectTraceScanner(ctx, uis.ec, traceIDs, minVal, maxVal)
	if uis.criteria != nil {
		tagFilter, errFilter := logical.BuildTagFilter(uis.criteria, entityDict, s, s, len(traceIDs) > 0, uis.traceIDTagName)
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

func (uis *unresolvedTraceTagFilter) selectTraceScanner(ctx *traceAnalyzeContext,
	ec executor.TraceExecutionContext, traceIDs []string, minVal, maxVal int64,
) logical.Plan {
	return &localScan{
		timeRange:         timestamp.NewInclusiveTimeRange(uis.startTime, uis.endTime),
		schema:            ctx.s,
		projectionTagRefs: ctx.projTagsRefs,
		projectionTags:    ctx.projectionTags,
		metadata:          uis.metadata,
		skippingFilter:    ctx.skippingFilter,
		entities:          ctx.entities,
		l:                 logger.GetLogger("query", "trace", "local-scan"),
		ec:                ec,
		traceIDs:          traceIDs,
		minVal:            minVal,
		maxVal:            maxVal,
	}
}

type traceAnalyzeContext struct {
	s              logical.Schema
	skippingFilter index.Filter
	projectionTags *model.TagProjection
	projTagsRefs   [][]*logical.TagRef
	entities       [][]*modelv1.TagValue
}

func newTraceAnalyzerContext(s logical.Schema) *traceAnalyzeContext {
	return &traceAnalyzeContext{
		s: s,
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
// Returns min/max int64 values for the orderByTag if provided, otherwise returns math.MaxInt64, math.MinInt64.
func buildTraceFilter(criteria *modelv1.Criteria, s logical.Schema, entityDict map[string]int,
	entity []*modelv1.TagValue, traceIDTagName string, orderByTag string,
) (index.Filter, [][]*modelv1.TagValue, []string, []string, int64, int64, error) {
	if criteria == nil {
		return nil, [][]*modelv1.TagValue{entity}, nil, nil, math.MinInt64, math.MaxInt64, nil
	}
	// Create a map of valid tag names from the schema
	tagNames := make(map[string]bool)
	// For trace schema, tags are stored directly (no tag families)
	for tagName := range s.(*schema).common.TagSpecMap {
		tagNames[tagName] = true
	}

	filter, entities, collectedTagNames, traceIDs, minVal, maxVal, err := buildFilter(criteria, s, tagNames, entityDict, entity, traceIDTagName, orderByTag)
	return filter, entities, collectedTagNames, traceIDs, minVal, maxVal, err
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

func (t *traceTagFilterPlan) Execute(ctx context.Context) (iter.Iterator[model.TraceResult], error) {
	resultIterator, err := t.parent.(executor.TraceExecutable).Execute(ctx)
	if err != nil {
		return iter.Empty[model.TraceResult](), err
	}

	// Return a lazy filtering iterator that processes results on demand
	return &traceTagFilterIterator{
		sourceIterator: resultIterator,
		tagFilter:      t.tagFilter,
		schema:         t.s,
	}, nil
}

// traceTagFilterIterator implements iter.Iterator[model.TraceResult] by lazily
// filtering results from the source iterator using the tag filter.
type traceTagFilterIterator struct {
	sourceIterator iter.Iterator[model.TraceResult]
	tagFilter      logical.TagFilter
	schema         logical.Schema
	err            error
}

func (tfti *traceTagFilterIterator) Next() (model.TraceResult, bool) {
	if tfti.err != nil {
		return model.TraceResult{}, false
	}

	for {
		result, hasNext := tfti.sourceIterator.Next()
		if !hasNext {
			return model.TraceResult{}, false
		}

		// If there are no spans, skip this result
		if len(result.Spans) == 0 {
			continue
		}

		maxRows := len(result.Spans)
		matched := false

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
			ok, err := tfti.tagFilter.Match(logical.TagFamilies(tagFamilies), tfti.schema)
			if err != nil {
				tfti.err = err
				return model.TraceResult{}, false
			}

			// If ANY row matches, return this result
			if ok {
				matched = true
				break
			}
		}

		if matched {
			return result, true
		}

		// If no match, continue to the next result
	}
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
