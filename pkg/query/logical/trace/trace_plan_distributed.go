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
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
)

const defaultQueryTimeout = 5 * time.Second

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
	var responseCount int
	var st []sort.Iterator[*comparableTraceResult]
	for _, f := range ff {
		m, getErr := f.Get()
		if getErr != nil {
			allErr = multierr.Append(allErr, getErr)
			continue
		}
		d := m.Data()
		if d == nil {
			continue
		}
		nodeResults, decodeErr := p.decodeNodeResults(d, span)
		if decodeErr != nil {
			allErr = multierr.Append(allErr, decodeErr)
			continue
		}
		responseCount++
		st = append(st, newSortableTraceResults(iter.FromSlice(nodeResults), p.sortByTraceID))
	}
	if span != nil {
		span.Tagf("response_count", "%d", responseCount)
	}
	sortIter := sort.NewItemIter(st, p.desc)
	var result []model.TraceResult
	seen := make(map[string]int)
	for sortIter.Next() {
		tr := sortIter.Val().result
		if idx, ok := seen[tr.TID]; !ok {
			seen[tr.TID] = len(result)
			result = append(result, tr)
		} else {
			mergeTraceResultSpans(&result[idx], &tr)
		}
	}
	if span != nil {
		span.Tagf("trace_id_count", "%d", len(seen))
	}

	return &distributedTraceResultIterator{
		traces: result,
		index:  0,
		err:    allErr,
	}, nil
}

// decodeNodeResults converts a single node response into columnar
// []model.TraceResult, accepting either the proto *tracev1.InternalQueryResponse
// (flag-off / tracing path) or the native columnar frame ([]byte). The proto
// branch is converted via internalTraceToResult so both paths feed a unified
// model.TraceResult merge.
func (p *distributedPlan) decodeNodeResults(d any, span *query.Span) ([]model.TraceResult, error) {
	switch payload := d.(type) {
	case *tracev1.InternalQueryResponse:
		if span != nil {
			span.AddSubTrace(payload.TraceQueryResult)
		}
		results := make([]model.TraceResult, 0, len(payload.InternalTraces))
		for _, internalTrace := range payload.InternalTraces {
			results = append(results, internalTraceToResult(internalTrace))
		}
		return results, nil
	case []byte:
		results, decodeErr := DecodeTraceResultFrame(payload)
		if decodeErr != nil {
			return nil, fmt.Errorf("decode trace result frame: %w", decodeErr)
		}
		return results, nil
	default:
		return nil, fmt.Errorf("unexpected trace query response type %T", d)
	}
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

// internalTraceToResult converts a proto *tracev1.InternalTrace (row-oriented:
// per-span tag lists) into a columnar model.TraceResult (per-tag value column
// aligned across spans, NullTagValue for spans missing a tag). It is the inverse
// of the data-node frame build and matches the legacy iterator's column-building.
func internalTraceToResult(trace *tracev1.InternalTrace) model.TraceResult {
	result := model.TraceResult{
		TID:     trace.TraceId,
		Key:     trace.Key,
		Spans:   make([][]byte, 0, len(trace.Spans)),
		SpanIDs: make([]string, 0, len(trace.Spans)),
	}
	tagValuesByName := make(map[string][]*modelv1.TagValue)
	var tagOrder []string
	for spanIdx, span := range trace.Spans {
		result.Spans = append(result.Spans, span.Span)
		result.SpanIDs = append(result.SpanIDs, span.SpanId)
		spanTags := make(map[string]*modelv1.TagValue, len(span.Tags))
		for _, tag := range span.Tags {
			if _, exists := tagValuesByName[tag.Key]; !exists {
				tagOrder = append(tagOrder, tag.Key)
				tagValuesByName[tag.Key] = make([]*modelv1.TagValue, spanIdx)
				for fillIdx := range tagValuesByName[tag.Key] {
					tagValuesByName[tag.Key][fillIdx] = pbv1.NullTagValue
				}
			}
			spanTags[tag.Key] = tag.Value
		}
		for _, tagName := range tagOrder {
			tagValue, exists := spanTags[tagName]
			if !exists {
				tagValue = pbv1.NullTagValue
			}
			tagValuesByName[tagName] = append(tagValuesByName[tagName], tagValue)
		}
	}
	for _, tagName := range tagOrder {
		result.Tags = append(result.Tags, model.Tag{Name: tagName, Values: tagValuesByName[tagName]})
	}
	return result
}

// mergeTraceResultSpans merges spans from src into dst (same TID): a span is
// appended only when its SpanID is not already present in dst, and the matching
// per-tag value column is extended in lockstep. This mirrors the legacy
// InternalTrace span-dedup but on the columnar model.TraceResult. Tag columns
// present only in src are first unioned into dst (NullTagValue-backfilled for the
// spans dst already holds) so no src-only tag column is dropped during the merge.
func mergeTraceResultSpans(dst, src *model.TraceResult) {
	existing := make(map[string]struct{}, len(dst.SpanIDs))
	for _, spanID := range dst.SpanIDs {
		existing[spanID] = struct{}{}
	}
	for _, srcTag := range src.Tags {
		if _, ok := findTag(dst.Tags, srcTag.Name); ok {
			continue
		}
		backfill := make([]*modelv1.TagValue, len(dst.SpanIDs))
		for fillIdx := range backfill {
			backfill[fillIdx] = pbv1.NullTagValue
		}
		dst.Tags = append(dst.Tags, model.Tag{Name: srcTag.Name, Values: backfill})
	}
	for srcIdx, srcSpanID := range src.SpanIDs {
		if _, ok := existing[srcSpanID]; ok {
			continue
		}
		existing[srcSpanID] = struct{}{}
		dst.SpanIDs = append(dst.SpanIDs, srcSpanID)
		dst.Spans = append(dst.Spans, src.Spans[srcIdx])
		for tagIdx := range dst.Tags {
			value := pbv1.NullTagValue
			if srcTag, ok := findTag(src.Tags, dst.Tags[tagIdx].Name); ok && srcIdx < len(srcTag.Values) {
				value = srcTag.Values[srcIdx]
			}
			dst.Tags[tagIdx].Values = append(dst.Tags[tagIdx].Values, value)
		}
	}
}

func findTag(tags []model.Tag, name string) (model.Tag, bool) {
	for _, tag := range tags {
		if tag.Name == name {
			return tag, true
		}
	}
	return model.Tag{}, false
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
	traces []model.TraceResult
	index  int
}

func (t *distributedTraceResultIterator) Next() (model.TraceResult, bool) {
	if t.index >= len(t.traces) {
		if t.err != nil {
			return model.TraceResult{Error: t.err}, false
		}
		return model.TraceResult{}, false
	}

	result := t.traces[t.index]
	t.index++
	return result, true
}
