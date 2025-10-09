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

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
	"github.com/apache/skywalking-banyandb/pkg/iter"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
)

const defaultLimit uint32 = 20

// Analyze converts logical expressions to executable operation tree represented by Plan.
func Analyze(criteria *tracev1.QueryRequest, metadata []*commonv1.Metadata, ss []logical.Schema,
	ecc []executor.TraceExecutionContext, traceIDTagNames, spanIDTagNames []string,
) (logical.Plan, error) {
	if len(metadata) != len(ss) {
		return nil, fmt.Errorf("number of schemas %d not equal to number of metadata %d", len(ss), len(metadata))
	}
	if len(traceIDTagNames) != len(metadata) {
		return nil, fmt.Errorf("number of traceIDTagNames %d not equal to number of metadata %d", len(traceIDTagNames), len(metadata))
	}

	var plan logical.UnresolvedPlan
	var s logical.Schema
	tagProjection := convertStringProjectionToTags(criteria.GetTagProjection())
	if len(metadata) == 1 {
		var orderByTag string
		if criteria.OrderBy != nil && criteria.OrderBy.IndexRuleName != "" {
			ok, indexRule := ss[0].IndexRuleDefined(criteria.OrderBy.IndexRuleName)
			if !ok {
				return nil, fmt.Errorf("index rule %s not found", criteria.OrderBy.IndexRuleName)
			}
			orderByTag = indexRule.Tags[len(indexRule.Tags)-1]
		}
		plan = parseTraceTags(criteria, metadata[0], ecc[0], tagProjection, traceIDTagNames[0], spanIDTagNames[0], orderByTag, 0)
		s = ss[0]
	} else {
		var err error
		if s, err = mergeSchema(ss); err != nil {
			return nil, err
		}
		plan = &unresolvedTraceMerger{
			criteria:        criteria,
			metadata:        metadata,
			ecc:             ecc,
			tagProjection:   tagProjection,
			traceIDTagNames: traceIDTagNames,
			spanIDTagNames:  spanIDTagNames,
		}
	}

	// parse limit
	limitParameter := criteria.GetLimit()
	if limitParameter == 0 {
		limitParameter = defaultLimit
	}
	plan = newTraceLimit(plan, criteria.GetOffset(), limitParameter)

	p, err := plan.Analyze(s)
	if err != nil {
		return nil, err
	}
	rules := []logical.OptimizeRule{
		logical.NewPushDownOrder(criteria.OrderBy),
		logical.NewPushDownMaxSize(int(limitParameter + criteria.GetOffset())),
	}
	if err := logical.ApplyRules(p, rules...); err != nil {
		return nil, err
	}
	return p, nil
}

// DistributedAnalyze converts logical expressions to executable operation tree represented by Plan.
func DistributedAnalyze(criteria *tracev1.QueryRequest, ss []logical.Schema) (logical.Plan, error) {
	// parse fields
	var s logical.Schema
	if len(ss) == 1 {
		s = ss[0]
	} else {
		var err error
		if s, err = mergeSchema(ss); err != nil {
			return nil, err
		}
	}
	plan := newUnresolvedTraceDistributed(criteria)

	// parse limit
	limitParameter := criteria.GetLimit()
	if limitParameter == 0 {
		limitParameter = defaultLimit
	}
	plan = newDistributedTraceLimit(plan, criteria.Offset, limitParameter)
	return plan.Analyze(s)
}

var (
	_ logical.Plan             = (*traceLimit)(nil)
	_ logical.UnresolvedPlan   = (*traceLimit)(nil)
	_ executor.TraceExecutable = (*traceLimit)(nil)
)

// Parent refers to a parent node in the execution tree(plan).
type Parent struct {
	UnresolvedInput logical.UnresolvedPlan
	Input           logical.Plan
}

type traceLimit struct {
	*Parent
	limitNum  uint32
	offsetNum uint32
}

func (l *traceLimit) Close() {
	l.Parent.Input.(executor.TraceExecutable).Close()
}

func (l *traceLimit) Execute(ctx context.Context) (iter.Iterator[model.TraceResult], error) {
	// Apply offset and limit to trace results (not spans within each trace)
	resultIterator, err := l.Parent.Input.(executor.TraceExecutable).Execute(ctx)
	if err != nil {
		return iter.Empty[model.TraceResult](), err
	}

	// Return a lazy iterator that handles offset and limit at the result level
	return &traceLimitIterator{
		sourceIterator: resultIterator,
		offset:         int(l.offsetNum),
		limit:          int(l.limitNum),
		currentIndex:   0,
		returned:       0,
	}, nil
}

// traceLimitIterator implements iter.Iterator[model.TraceResult] by applying
// offset and limit to the number of trace results (not spans within results).
type traceLimitIterator struct {
	sourceIterator iter.Iterator[model.TraceResult]
	offset         int
	limit          int
	currentIndex   int
	returned       int
}

func (tli *traceLimitIterator) Next() (model.TraceResult, bool) {
	// If we've already returned the maximum number of results, stop
	if tli.limit > 0 && tli.returned >= tli.limit {
		return model.TraceResult{}, false
	}

	for {
		result, hasNext := tli.sourceIterator.Next()
		if result.Error != nil {
			return model.TraceResult{Error: result.Error}, false
		}
		if !hasNext {
			return model.TraceResult{}, false
		}

		// Skip results until we reach the offset
		if tli.currentIndex < tli.offset {
			tli.currentIndex++
			continue
		}

		// We're past the offset, return this result and increment counters
		tli.currentIndex++
		tli.returned++
		return result, true
	}
}

func (l *traceLimit) Analyze(s logical.Schema) (logical.Plan, error) {
	var err error
	l.Input, err = l.UnresolvedInput.Analyze(s)
	if err != nil {
		return nil, err
	}
	return l, nil
}

func (l *traceLimit) Schema() logical.Schema {
	return l.Input.Schema()
}

func (l *traceLimit) String() string {
	return fmt.Sprintf("%s TraceLimit: %d", l.Input.String(), l.limitNum)
}

func (l *traceLimit) Children() []logical.Plan {
	return []logical.Plan{l.Input}
}

func newTraceLimit(input logical.UnresolvedPlan, offset, num uint32) logical.UnresolvedPlan {
	return &traceLimit{
		Parent: &Parent{
			UnresolvedInput: input,
		},
		offsetNum: offset,
		limitNum:  num,
	}
}

func parseTraceTags(criteria *tracev1.QueryRequest, metadata *commonv1.Metadata,
	ec executor.TraceExecutionContext, tagProjection [][]*logical.Tag, traceIDTagName, spanIDTagName, orderByTag string, groupIndex int,
) logical.UnresolvedPlan {
	timeRange := criteria.GetTimeRange()
	return &unresolvedTraceTagFilter{
		startTime:      timeRange.GetBegin().AsTime(),
		endTime:        timeRange.GetEnd().AsTime(),
		metadata:       metadata,
		criteria:       criteria.Criteria,
		projectionTags: tagProjection,
		ec:             ec,
		traceIDTagName: traceIDTagName,
		spanIDTagName:  spanIDTagName,
		orderByTag:     orderByTag,
		groupIndex:     groupIndex,
	}
}

// convertStringProjectionToTags converts a string array projection to tag projection format.
// For trace, we create a single tag family with empty name since traces don't have families.
func convertStringProjectionToTags(tagNames []string) [][]*logical.Tag {
	if len(tagNames) == 0 {
		return nil
	}

	// For trace, create a single tag family (empty name) with all tags
	tags := make([]*logical.Tag, len(tagNames))
	for i, tagName := range tagNames {
		tags[i] = logical.NewTag("", tagName) // Empty family name for trace
	}

	return [][]*logical.Tag{tags}
}
