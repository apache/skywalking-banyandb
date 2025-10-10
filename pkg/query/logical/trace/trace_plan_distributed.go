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
	"slices"
	"time"

	"go.uber.org/multierr"
	"google.golang.org/protobuf/proto"

	"github.com/apache/skywalking-banyandb/api/data"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/iter"
	"github.com/apache/skywalking-banyandb/pkg/iter/sort"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
)

const defaultQueryTimeout = 30 * time.Second

var _ logical.UnresolvedPlan = (*unresolvedTraceDistributed)(nil)

type unresolvedTraceDistributed struct {
	originalQuery *tracev1.QueryRequest
}

func newUnresolvedTraceDistributed(query *tracev1.QueryRequest) logical.UnresolvedPlan {
	return &unresolvedTraceDistributed{
		originalQuery: query,
	}
}

func (t *unresolvedTraceDistributed) Analyze(s logical.Schema) (logical.Plan, error) {
	if t.originalQuery.TagProjection == nil {
		t.originalQuery.TagProjection = []string{}
	}
	tagProjections := convertStringProjectionToTags(t.originalQuery.GetTagProjection())
	if len(tagProjections) > 0 {
		var err error
		projTagsRefs, err := s.CreateTagRef(tagProjections...)
		if err != nil {
			return nil, err
		}
		s = s.ProjTags(projTagsRefs...)
	}
	limit := t.originalQuery.GetLimit()
	if limit == 0 {
		limit = defaultLimit
	}
	temp := &tracev1.QueryRequest{
		TagProjection: t.originalQuery.TagProjection,
		Name:          t.originalQuery.Name,
		Groups:        t.originalQuery.Groups,
		Criteria:      t.originalQuery.Criteria,
		Limit:         limit + t.originalQuery.Offset,
		OrderBy:       t.originalQuery.OrderBy,
	}
	if t.originalQuery.OrderBy == nil {
		return &distributedPlan{
			queryTemplate: temp,
			s:             s,
			sortByTraceID: true,
		}, nil
	}
	if t.originalQuery.OrderBy.IndexRuleName == "" {
		result := &distributedPlan{
			queryTemplate: temp,
			s:             s,
			sortByTraceID: true,
		}
		if t.originalQuery.OrderBy.Sort == modelv1.Sort_SORT_DESC {
			result.desc = true
		}
		return result, nil
	}
	ok, indexRule := s.IndexRuleDefined(t.originalQuery.OrderBy.IndexRuleName)
	if !ok {
		return nil, fmt.Errorf("index rule %s not found", t.originalQuery.OrderBy.IndexRuleName)
	}
	tags := indexRule.GetTags()
	if len(tags) == 0 {
		return nil, fmt.Errorf("index rule %s has no tags", t.originalQuery.OrderBy.IndexRuleName)
	}
	result := &distributedPlan{
		queryTemplate: temp,
		s:             s,
		sortByTraceID: false,
	}
	if t.originalQuery.OrderBy.Sort == modelv1.Sort_SORT_DESC {
		result.desc = true
	}
	return result, nil
}

var _ executor.TraceExecutable = (*distributedPlan)(nil)

type distributedPlan struct {
	s             logical.Schema
	queryTemplate *tracev1.QueryRequest
	sortByTraceID bool
	desc          bool
	maxTraceSize  uint32
}

func (p *distributedPlan) Close() {}

func (p *distributedPlan) Execute(ctx context.Context) (iter.Iterator[model.TraceResult], error) {
	dctx := executor.FromDistributedExecutionContext(ctx)
	queryRequest := proto.Clone(p.queryTemplate).(*tracev1.QueryRequest)
	queryRequest.TimeRange = dctx.TimeRange()
	if p.maxTraceSize > 0 {
		queryRequest.Limit = p.maxTraceSize
	}
	tracer := query.GetTracer(ctx)
	var span *query.Span
	var err error
	if tracer != nil {
		span, _ = tracer.StartSpan(ctx, "distributed-client")
		queryRequest.Trace = true
		span.Tag("request", convert.BytesToString(logger.Proto(queryRequest)))
		defer func() {
			if err != nil {
				span.Error(err)
			} else {
				span.Stop()
			}
		}()
	}
	ff, err := dctx.Broadcast(defaultQueryTimeout, data.TopicTraceQuery,
		bus.NewMessageWithNodeSelectors(bus.MessageID(dctx.TimeRange().Begin.Nanos), dctx.NodeSelectors(), dctx.TimeRange(), queryRequest))
	if err != nil {
		return iter.Empty[model.TraceResult](), err
	}
	var allErr error
	var st []sort.Iterator[*comparableTrace]
	for _, f := range ff {
		if m, getErr := f.Get(); getErr != nil {
			allErr = multierr.Append(allErr, getErr)
		} else {
			d := m.Data()
			if d == nil {
				continue
			}
			resp := d.(*tracev1.InternalQueryResponse)
			if span != nil {
				span.AddSubTrace(resp.TraceQueryResult)
			}
			st = append(st,
				newSortableTraces(resp.InternalTraces, p.sortByTraceID))
		}
	}
	sortIter := sort.NewItemIter(st, p.desc)
	var result []*tracev1.InternalTrace
	seen := make(map[string]*tracev1.InternalTrace)
	for sortIter.Next() {
		trace := sortIter.Val().InternalTrace
		if _, ok := seen[trace.TraceId]; !ok {
			seen[trace.TraceId] = trace
			result = append(result, trace)
		} else {
			for _, spanID := range trace.SpanIds {
				if !slices.Contains(seen[trace.TraceId].SpanIds, spanID) {
					seen[trace.TraceId].SpanIds = append(seen[trace.TraceId].SpanIds, spanID)
					seen[trace.TraceId].Spans = append(seen[trace.TraceId].Spans, trace.Spans...)
				}
			}
		}
	}

	return &distributedTraceResultIterator{
		traces: result,
		index:  0,
		err:    allErr,
	}, nil
}

func (p *distributedPlan) String() string {
	return fmt.Sprintf("distributed:%s", p.queryTemplate.String())
}

func (p *distributedPlan) Children() []logical.Plan {
	return []logical.Plan{}
}

func (p *distributedPlan) Schema() logical.Schema {
	return p.s
}

func (p *distributedPlan) Limit(maxVal int) {
	p.maxTraceSize = uint32(maxVal)
}

var _ sort.Comparable = (*comparableTrace)(nil)

type comparableTrace struct {
	*tracev1.InternalTrace
	sortField []byte
}

func newComparableTrace(t *tracev1.InternalTrace, sortByTraceID bool) (*comparableTrace, error) {
	var sortField []byte
	if sortByTraceID {
		sortField = []byte(t.TraceId)
	} else {
		sortField = convert.Int64ToBytes(t.Key)
	}

	return &comparableTrace{
		InternalTrace: t,
		sortField:     sortField,
	}, nil
}

func (t *comparableTrace) SortedField() []byte {
	return t.sortField
}

var _ sort.Iterator[*comparableTrace] = (*sortableTraces)(nil)

type sortableTraces struct {
	cur             *comparableTrace
	traces          []*tracev1.InternalTrace
	index           int
	isSortByTraceID bool
}

func newSortableTraces(traces []*tracev1.InternalTrace, isSortByTraceID bool) *sortableTraces {
	return &sortableTraces{
		traces:          traces,
		isSortByTraceID: isSortByTraceID,
	}
}

func (*sortableTraces) Close() error {
	return nil
}

func (t *sortableTraces) Next() bool {
	return t.iter(func(it *tracev1.InternalTrace) (*comparableTrace, error) {
		return newComparableTrace(it, t.isSortByTraceID)
	})
}

func (t *sortableTraces) Val() *comparableTrace {
	return t.cur
}

func (t *sortableTraces) iter(fn func(*tracev1.InternalTrace) (*comparableTrace, error)) bool {
	if t.index >= len(t.traces) {
		return false
	}
	cur, err := fn(t.traces[t.index])
	t.index++
	if err != nil {
		return t.iter(fn)
	}
	t.cur = cur
	return true
}

var _ executor.TraceExecutable = (*distributedTraceLimit)(nil)

type distributedTraceLimit struct {
	*Parent
	limit  uint32
	offset uint32
}

func (l *distributedTraceLimit) Close() {
	l.Parent.Input.(executor.TraceExecutable).Close()
}

func (l *distributedTraceLimit) Execute(ec context.Context) (iter.Iterator[model.TraceResult], error) {
	resultIter, err := l.Parent.Input.(executor.TraceExecutable).Execute(ec)
	if err != nil {
		return iter.Empty[model.TraceResult](), err
	}

	// Apply offset and limit to trace results (not spans within each trace)
	return &traceLimitIterator{
		sourceIterator: resultIter,
		offset:         int(l.offset),
		limit:          int(l.limit),
		currentIndex:   0,
		returned:       0,
	}, nil
}

func (l *distributedTraceLimit) Analyze(s logical.Schema) (logical.Plan, error) {
	var err error
	l.Input, err = l.UnresolvedInput.Analyze(s)
	if err != nil {
		return nil, err
	}
	return l, nil
}

func (l *distributedTraceLimit) Schema() logical.Schema {
	return l.Input.Schema()
}

func (l *distributedTraceLimit) String() string {
	return fmt.Sprintf("%s Distributed Limit: %d, %d", l.Input.String(), l.offset, l.limit)
}

func (l *distributedTraceLimit) Children() []logical.Plan {
	return []logical.Plan{l.Input}
}

func newDistributedTraceLimit(input logical.UnresolvedPlan, offset, limit uint32) logical.UnresolvedPlan {
	return &distributedTraceLimit{
		Parent: &Parent{
			UnresolvedInput: input,
		},
		offset: offset,
		limit:  limit,
	}
}

type distributedTraceResultIterator struct {
	err    error
	traces []*tracev1.InternalTrace
	index  int
}

func (t *distributedTraceResultIterator) Next() (model.TraceResult, bool) {
	if t.index >= len(t.traces) {
		if t.err != nil {
			return model.TraceResult{Error: t.err}, false
		}
		return model.TraceResult{}, false
	}

	trace := t.traces[t.index]
	t.index++

	result := model.TraceResult{
		TID: trace.TraceId,
	}

	// Extract tags and spans from all spans in this trace
	var allSpans [][]byte
	tagMap := make(map[string][]*modelv1.TagValue)

	for _, span := range trace.Spans {
		// Add span data
		allSpans = append(allSpans, span.Span)

		// Extract tags from this span and aggregate by name
		for _, tag := range span.Tags {
			tagMap[tag.Key] = append(tagMap[tag.Key], tag.Value)
		}
	}

	// Convert tagMap to []model.Tag
	var allTags []model.Tag
	for tagName, values := range tagMap {
		allTags = append(allTags, model.Tag{
			Name:   tagName,
			Values: values,
		})
	}

	result.Spans = allSpans
	result.Tags = allTags

	return result, true
}
