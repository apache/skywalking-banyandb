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

func (utd *unresolvedTraceDistributed) Analyze(s logical.Schema) (logical.Plan, error) {
	if utd.originalQuery.TagProjection == nil {
		utd.originalQuery.TagProjection = []string{}
	}
	tagProjections := convertStringProjectionToTags(utd.originalQuery.GetTagProjection())
	if len(tagProjections) > 0 {
		var err error
		projTagsRefs, err := s.CreateTagRef(tagProjections...)
		if err != nil {
			return nil, err
		}
		s = s.ProjTags(projTagsRefs...)
	}
	limit := utd.originalQuery.GetLimit()
	if limit == 0 {
		limit = defaultLimit
	}
	temp := &tracev1.QueryRequest{
		TagProjection: utd.originalQuery.TagProjection,
		Name:          utd.originalQuery.Name,
		Groups:        utd.originalQuery.Groups,
		Criteria:      utd.originalQuery.Criteria,
		Limit:         limit + utd.originalQuery.Offset,
		OrderBy:       utd.originalQuery.OrderBy,
	}
	if utd.originalQuery.OrderBy == nil {
		return &distributedPlan{
			queryTemplate: temp,
			s:             s,
			sortByTime:    true,
		}, nil
	}
	if utd.originalQuery.OrderBy.IndexRuleName == "" {
		result := &distributedPlan{
			queryTemplate: temp,
			s:             s,
			sortByTime:    true,
		}
		if utd.originalQuery.OrderBy.Sort == modelv1.Sort_SORT_DESC {
			result.desc = true
		}
		return result, nil
	}
	ok, indexRule := s.IndexRuleDefined(utd.originalQuery.OrderBy.IndexRuleName)
	if !ok {
		return nil, fmt.Errorf("index rule %s not found", utd.originalQuery.OrderBy.IndexRuleName)
	}
	tags := indexRule.GetTags()
	if len(tags) == 0 {
		return nil, fmt.Errorf("index rule %s has no tags", utd.originalQuery.OrderBy.IndexRuleName)
	}
	sortTagName := tags[len(tags)-1]
	sortTagSpec := s.FindTagSpecByName(sortTagName)
	if sortTagSpec == nil {
		return nil, fmt.Errorf("tag %s not found", sortTagName)
	}
	result := &distributedPlan{
		queryTemplate: temp,
		s:             s,
		sortByTime:    false,
		sortTagSpec:   *sortTagSpec,
	}
	if utd.originalQuery.OrderBy.Sort == modelv1.Sort_SORT_DESC {
		result.desc = true
	}
	return result, nil
}

var _ executor.TraceExecutable = (*distributedPlan)(nil)

type distributedPlan struct {
	s              logical.Schema
	queryTemplate  *tracev1.QueryRequest
	sortTagSpec    logical.TagSpec
	sortByTime     bool
	desc           bool
	maxElementSize uint32
}

func (t *distributedPlan) Close() {}

func (t *distributedPlan) Execute(ctx context.Context) (iter.Iterator[model.TraceResult], error) {
	dctx := executor.FromDistributedExecutionContext(ctx)
	queryRequest := proto.Clone(t.queryTemplate).(*tracev1.QueryRequest)
	queryRequest.TimeRange = dctx.TimeRange()
	if t.maxElementSize > 0 {
		queryRequest.Limit = t.maxElementSize
	}
	tracer := query.GetTracer(ctx)
	var span *query.Span
	if tracer != nil {
		span, _ = tracer.StartSpan(ctx, "distributed-client")
		queryRequest.Trace = true
		span.Tag("request", convert.BytesToString(logger.Proto(queryRequest)))
		defer func() {
			// TODO: handle error
			span.Stop()
		}()
	}
	ff, err := dctx.Broadcast(defaultQueryTimeout, data.TopicTraceQuery,
		bus.NewMessageWithNodeSelectors(bus.MessageID(dctx.TimeRange().Begin.Nanos), dctx.NodeSelectors(), dctx.TimeRange(), queryRequest))
	if err != nil {
		return iter.Empty[model.TraceResult](), err
	}
	var allErr error
	var see []sort.Iterator[*comparableElement]
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
			see = append(see,
				newSortableElements(resp.InternalTraces, t.sortByTime, t.sortTagSpec))
		}
	}
	sortIter := sort.NewItemIter(see, t.desc)
	var result []*tracev1.InternalTrace
	seen := make(map[string]bool)
	for sortIter.Next() {
		element := sortIter.Val().InternalTrace
		if !seen[element.TraceId] {
			seen[element.TraceId] = true
			result = append(result, element)
		}
	}

	return &distributedTraceResultIterator{
		traces: result,
		index:  0,
		err:    allErr,
	}, nil
}

func (t *distributedPlan) String() string {
	return fmt.Sprintf("distributed:%s", t.queryTemplate.String())
}

func (t *distributedPlan) Children() []logical.Plan {
	return []logical.Plan{}
}

func (t *distributedPlan) Schema() logical.Schema {
	return t.s
}

func (t *distributedPlan) Limit(maxVal int) {
	t.maxElementSize = uint32(maxVal)
}

var _ sort.Comparable = (*comparableElement)(nil)

type comparableElement struct {
	*tracev1.InternalTrace
	sortField []byte
}

func newComparableElement(e *tracev1.InternalTrace, sortByTime bool, sortTagSpec logical.TagSpec) (*comparableElement, error) {
	var sortField []byte
	if sortByTime {
		// For traces, we use trace ID as sort field when sorting by time
		sortField = []byte(e.TraceId)
	} else {
		sortField = convert.Int64ToBytes(e.Key)
	}

	return &comparableElement{
		InternalTrace: e,
		sortField:     sortField,
	}, nil
}

func (e *comparableElement) SortedField() []byte {
	return e.sortField
}

var _ sort.Iterator[*comparableElement] = (*sortableElements)(nil)

type sortableElements struct {
	cur          *comparableElement
	elements     []*tracev1.InternalTrace
	sortTagSpec  logical.TagSpec
	index        int
	isSortByTime bool
}

func newSortableElements(elements []*tracev1.InternalTrace, isSortByTime bool, sortTagSpec logical.TagSpec) *sortableElements {
	return &sortableElements{
		elements:     elements,
		isSortByTime: isSortByTime,
		sortTagSpec:  sortTagSpec,
	}
}

func (*sortableElements) Close() error {
	return nil
}

func (s *sortableElements) Next() bool {
	return s.iter(func(e *tracev1.InternalTrace) (*comparableElement, error) {
		return newComparableElement(e, s.isSortByTime, s.sortTagSpec)
	})
}

func (s *sortableElements) Val() *comparableElement {
	return s.cur
}

func (s *sortableElements) iter(fn func(*tracev1.InternalTrace) (*comparableElement, error)) bool {
	if s.index >= len(s.elements) {
		return false
	}
	cur, err := fn(s.elements[s.index])
	s.index++
	if err != nil {
		return s.iter(fn)
	}
	s.cur = cur
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
	traces []*tracev1.InternalTrace
	index  int
	err    error
}

func (tri *distributedTraceResultIterator) Next() (model.TraceResult, bool) {
	if tri.index >= len(tri.traces) {
		if tri.err != nil {
			return model.TraceResult{Error: tri.err}, false
		}
		return model.TraceResult{}, false
	}

	trace := tri.traces[tri.index]
	tri.index++

	result := model.TraceResult{
		TID:         trace.TraceId,
		TraceIDName: trace.TraceIdName,
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
