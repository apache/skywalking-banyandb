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
	ecc []executor.TraceExecutionContext, traceIDTagName, timestampTagName string,
) (logical.Plan, error) {
	// parse fields
	if len(metadata) != len(ss) {
		return nil, fmt.Errorf("number of schemas %d not equal to number of metadata %d", len(ss), len(metadata))
	}
	var orderByTag string
	if criteria.OrderBy != nil {
		indexRuleName := criteria.OrderBy.IndexRuleName
		ok, indexRule := ss[0].IndexRuleDefined(indexRuleName)
		if !ok {
			return nil, fmt.Errorf("index rule %s not found", indexRuleName)
		}
		ot := indexRule.Tags[len(indexRule.Tags)-1]
		if ot != timestampTagName {
			orderByTag = ot
		}
	}
	var plan logical.UnresolvedPlan
	var s logical.Schema
	tagProjection := convertStringProjectionToTags(criteria.GetTagProjection())
	if len(metadata) == 1 {
		plan = parseTraceTags(criteria, metadata[0], ecc[0], tagProjection, traceIDTagName, orderByTag)
		s = ss[0]
	} else {
		var err error
		if s, err = mergeSchema(ss); err != nil {
			return nil, err
		}
		plan = &unresolvedTraceMerger{
			criteria:      criteria,
			metadata:      metadata,
			ecc:           ecc,
			tagProjection: tagProjection,
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
	ec executor.TraceExecutionContext, tagProjection [][]*logical.Tag, traceIDTagName, orderByTag string,
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
		orderByTag:     orderByTag,
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

// Placeholder implementations for distributed query components.
var _ logical.UnresolvedPlan = (*unresolvedTraceMerger)(nil)

type unresolvedTraceMerger struct {
	criteria      *tracev1.QueryRequest
	metadata      []*commonv1.Metadata
	ecc           []executor.TraceExecutionContext
	tagProjection [][]*logical.Tag
}

func (utm *unresolvedTraceMerger) Analyze(_ logical.Schema) (logical.Plan, error) {
	// TODO: Implement trace merger logic
	return nil, fmt.Errorf("trace merger not implemented yet")
}

func newUnresolvedTraceDistributed(criteria *tracev1.QueryRequest) logical.UnresolvedPlan {
	// TODO: Implement distributed trace query
	return &unresolvedTraceDistributed{criteria: criteria}
}

var _ logical.UnresolvedPlan = (*unresolvedTraceDistributed)(nil)

type unresolvedTraceDistributed struct {
	criteria *tracev1.QueryRequest
}

func (utd *unresolvedTraceDistributed) Analyze(_ logical.Schema) (logical.Plan, error) {
	// TODO: Implement distributed trace analysis
	return nil, fmt.Errorf("distributed trace query not implemented yet")
}

func newDistributedTraceLimit(input logical.UnresolvedPlan, offset, num uint32) logical.UnresolvedPlan {
	// TODO: Implement distributed trace limit
	return &distributedTraceLimit{
		input:     input,
		offsetNum: offset,
		limitNum:  num,
	}
}

var _ logical.UnresolvedPlan = (*distributedTraceLimit)(nil)

type distributedTraceLimit struct {
	input     logical.UnresolvedPlan
	offsetNum uint32
	limitNum  uint32
}

func (dtl *distributedTraceLimit) Analyze(_ logical.Schema) (logical.Plan, error) {
	// TODO: Implement distributed trace limit analysis
	return nil, fmt.Errorf("distributed trace limit not implemented yet")
}
