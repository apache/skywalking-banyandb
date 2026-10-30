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
	"errors"
	"fmt"
	"io"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/iter/sort"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
	vstream "github.com/apache/skywalking-banyandb/pkg/query/vectorized/stream"
	streamframe "github.com/apache/skywalking-banyandb/pkg/query/vectorized/stream/frame"
)

var _ logical.UnresolvedPlan = (*unresolvedDistributed)(nil)

type unresolvedDistributed struct {
	originalQuery *streamv1.QueryRequest
}

func newUnresolvedDistributed(query *streamv1.QueryRequest) logical.UnresolvedPlan {
	return &unresolvedDistributed{
		originalQuery: query,
	}
}

func (ud *unresolvedDistributed) Analyze(s logical.Schema) (logical.Plan, error) {
	if ud.originalQuery.Projection == nil {
		return nil, fmt.Errorf("projection is required")
	}
	projectionTags := logical.ToTags(ud.originalQuery.GetProjection())
	if len(projectionTags) > 0 {
		var err error
		projTagsRefs, err := s.CreateTagRef(projectionTags...)
		if err != nil {
			return nil, err
		}
		s = s.ProjTags(projTagsRefs...)
	}
	limit := ud.originalQuery.GetLimit()
	if limit == 0 {
		limit = defaultLimit
	}
	window, validWindow := query.AddWindow(limit, ud.originalQuery.Offset)
	if !validWindow {
		return nil, fmt.Errorf("query limit and offset exceed supported range")
	}
	temp := &streamv1.QueryRequest{
		Projection: ud.originalQuery.Projection,
		Name:       ud.originalQuery.Name,
		Groups:     ud.originalQuery.Groups,
		Criteria:   ud.originalQuery.Criteria,
		Limit:      uint32(window),
		OrderBy:    ud.originalQuery.OrderBy,
	}
	if ud.originalQuery.OrderBy == nil {
		return &distributedPlan{
			queryTemplate: temp,
			s:             s,
			sortByTime:    true,
		}, nil
	}
	if ud.originalQuery.OrderBy.IndexRuleName == "" {
		result := &distributedPlan{
			queryTemplate: temp,
			s:             s,
			sortByTime:    true,
		}
		if ud.originalQuery.OrderBy.Sort == modelv1.Sort_SORT_DESC {
			result.desc = true
		}
		return result, nil
	}
	ok, indexRule := s.IndexRuleDefined(ud.originalQuery.OrderBy.IndexRuleName)
	if !ok {
		return nil, fmt.Errorf("index rule %s not found", ud.originalQuery.OrderBy.IndexRuleName)
	}
	if len(indexRule.Tags) != 1 {
		return nil, fmt.Errorf("index rule %s should have only one tag", ud.originalQuery.OrderBy.IndexRuleName)
	}
	sortTagSpec := s.FindTagSpecByName(indexRule.Tags[0])
	if sortTagSpec == nil {
		return nil, fmt.Errorf("tag %s not found", indexRule.Tags[0])
	}
	result := &distributedPlan{
		queryTemplate: temp,
		s:             s,
		sortByTime:    false,
		sortTagSpec:   *sortTagSpec,
	}
	if ud.originalQuery.OrderBy.Sort == modelv1.Sort_SORT_DESC {
		result.desc = true
	}
	return result, nil
}

var _ executor.StreamExecutable = (*distributedPlan)(nil)

type distributedPlan struct {
	s              logical.Schema
	queryTemplate  *streamv1.QueryRequest
	sortTagSpec    logical.TagSpec
	sortByTime     bool
	desc           bool
	maxElementSize uint32
}

func (t *distributedPlan) Close() {}

func (t *distributedPlan) Execute(ctx context.Context) (ee []*streamv1.Element, err error) {
	dctx := executor.FromDistributedExecutionContext(ctx)
	queryRequest := proto.Clone(t.queryTemplate).(*streamv1.QueryRequest)
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
			if err != nil {
				span.Error(err)
			} else {
				span.Stop()
			}
		}()
	}
	// The scan streams live as long as the caller's context; each node stream is
	// bounded by the scatter idle timeout and its heartbeats rather than a fixed deadline.
	scanCtx, cancelScan := context.WithCancel(ctx)
	defer cancelScan()
	streams, err := dctx.Scatter(scanCtx, data.TopicStreamQuery,
		bus.NewMessageWithNodeSelectors(bus.MessageID(dctx.TimeRange().Begin.Nanos), dctx.NodeSelectors(), dctx.TimeRange(), queryRequest),
		bus.ScatterOptions{})
	if err != nil {
		return nil, err
	}
	defer func() {
		for _, ns := range streams {
			ns.Close()
		}
	}()
	nodes := make([]nodeStreamElements, len(streams))
	var wg sync.WaitGroup
	var errOnce sync.Once
	var firstErr error
	for idx := range streams {
		wg.Add(1)
		go func() {
			defer wg.Done()
			nodes[idx] = t.readNodeStream(scanCtx, streams[idx])
			if nodes[idx].err != nil {
				errOnce.Do(func() {
					firstErr = nodes[idx].err
					cancelScan()
				})
			}
		}()
	}
	wg.Wait()
	if firstErr != nil {
		err = firstErr
		return nil, firstErr
	}
	var see []sort.Iterator[*comparableElement]
	var responseCount int
	for idx := range nodes {
		if !nodes[idx].responded {
			continue
		}
		responseCount++
		if span != nil {
			span.AddSubTrace(nodes[idx].subTrace)
		}
		see = append(see, newSortableElements(nodes[idx].elements, t.sortByTime, t.sortTagSpec))
	}
	iter := sort.NewItemIter(see, t.desc)
	var result []*streamv1.Element
	seen := make(map[string]bool)
	for iter.Next() {
		element := iter.Val().Element
		if !seen[element.ElementId] {
			seen[element.ElementId] = true
			result = append(result, element)
		}
	}
	if span != nil {
		span.Tagf("response_count", "%d", responseCount)
		span.Tagf("element_id_count", "%d", len(seen))
	}

	return result, nil
}

// nodeStreamElements is everything one node stream delivered.
type nodeStreamElements struct {
	err       error
	subTrace  *commonv1.Trace
	elements  []*streamv1.Element
	responded bool
}

// readNodeStream reads a node stream to its end, decoding one chunk at a time, so
// only the decoded elements of the node are held, never its encoded chunks.
func (t *distributedPlan) readNodeStream(ctx context.Context, ns bus.NodeStream) nodeStreamElements {
	var out nodeStreamElements
	for {
		m, nextErr := ns.Next(ctx)
		if errors.Is(nextErr, io.EOF) {
			return out
		}
		if nextErr != nil {
			out.err = fmt.Errorf("stream query on node %s: %w", ns.Node(), nextErr)
			return out
		}
		d := m.Data()
		if d == nil {
			continue
		}
		chunk, subTrace, decodeErr := t.decodeNodeElements(d)
		if decodeErr != nil {
			out.err = fmt.Errorf("stream query on node %s: %w", ns.Node(), decodeErr)
			return out
		}
		out.responded = true
		out.elements = append(out.elements, chunk...)
		if subTrace != nil {
			out.subTrace = subTrace
		}
	}
}

// decodeNodeElements converts a single node chunk into []*streamv1.Element,
// accepting either the proto *streamv1.QueryResponse (flag-off / tracing / a data
// node that has not rolled onto the vec path) or the native columnar frame
// ([]byte, only when the raw wire mode is on). Both feed the SAME
// sortableElements/comparableElement k-way merge + seen-dedup + distributedLimit
// downstream, so mixed proto/frame bodies in one broadcast (rolling deploy) merge
// correctly.
func (t *distributedPlan) decodeNodeElements(d any) ([]*streamv1.Element, *commonv1.Trace, error) {
	switch payload := d.(type) {
	case *streamv1.QueryResponse:
		return payload.Elements, payload.Trace, nil
	case []byte:
		if !data.StreamWireModeRaw() {
			return nil, nil, fmt.Errorf("unexpected raw stream frame body while wire mode is off")
		}
		batch, decodeErr := streamframe.Decode(payload)
		if decodeErr != nil {
			return nil, nil, fmt.Errorf("decode stream result frame: %w", decodeErr)
		}
		elements, buildErr := vstream.BuildElementsFromBatch(batch, vstream.ProjectionFromBatchSchema(batch))
		if buildErr != nil {
			return nil, nil, fmt.Errorf("build elements from stream frame: %w", buildErr)
		}
		return elements, nil, nil
	default:
		return nil, nil, fmt.Errorf("unexpected stream query response type %T", d)
	}
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
	*streamv1.Element
	sortField []byte
}

func newComparableElement(e *streamv1.Element, sortByTime bool, sortTagSpec logical.TagSpec) (*comparableElement, error) {
	var sortField []byte
	if sortByTime {
		sortField = convert.Uint64ToBytes(uint64(e.Timestamp.AsTime().UnixNano()))
	} else {
		var err error
		sortField, err = pbv1.MarshalTagValue(e.TagFamilies[sortTagSpec.TagFamilyIdx].Tags[sortTagSpec.TagIdx].Value)
		if err != nil {
			return nil, err
		}
	}

	return &comparableElement{
		Element:   e,
		sortField: sortField,
	}, nil
}

func (e *comparableElement) SortedField() []byte {
	return e.sortField
}

var _ sort.Iterator[*comparableElement] = (*sortableElements)(nil)

type sortableElements struct {
	cur          *comparableElement
	elements     []*streamv1.Element
	sortTagSpec  logical.TagSpec
	index        int
	isSortByTime bool
}

func newSortableElements(elements []*streamv1.Element, isSortByTime bool, sortTagSpec logical.TagSpec) *sortableElements {
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
	return s.iter(func(e *streamv1.Element) (*comparableElement, error) {
		return newComparableElement(e, s.isSortByTime, s.sortTagSpec)
	})
}

func (s *sortableElements) Val() *comparableElement {
	return s.cur
}

func (s *sortableElements) iter(fn func(*streamv1.Element) (*comparableElement, error)) bool {
	if s.index >= len(s.elements) {
		return false
	}
	cur, err := fn(s.elements[s.index])
	s.index++
	if err != nil {
		return s.iter(fn)
	}
	s.cur = cur
	return s.index <= len(s.elements)
}

var _ executor.StreamExecutable = (*distributedLimit)(nil)

type distributedLimit struct {
	*Parent
	limit  uint32
	offset uint32
}

func (l *distributedLimit) Close() {
	l.Parent.Input.(executor.StreamExecutable).Close()
}

func (l *distributedLimit) Execute(ec context.Context) ([]*streamv1.Element, error) {
	entities, err := l.Parent.Input.(executor.StreamExecutable).Execute(ec)
	if err != nil {
		return nil, err
	}

	start := int(l.offset)
	if start > len(entities) {
		return []*streamv1.Element{}, nil
	}

	end := start + int(l.limit)
	if end > len(entities) {
		end = len(entities)
	}
	return entities[start:end], nil
}

func (l *distributedLimit) Analyze(s logical.Schema) (logical.Plan, error) {
	var err error
	l.Input, err = l.UnresolvedInput.Analyze(s)
	if err != nil {
		return nil, err
	}
	return l, nil
}

func (l *distributedLimit) Schema() logical.Schema {
	return l.Input.Schema()
}

func (l *distributedLimit) String() string {
	return fmt.Sprintf("%s Distributed Limit: %d, %d", l.Input.String(), l.offset, l.limit)
}

func (l *distributedLimit) Children() []logical.Plan {
	return []logical.Plan{l.Input}
}

func newDistributedLimit(input logical.UnresolvedPlan, offset, limit uint32) logical.UnresolvedPlan {
	return &distributedLimit{
		Parent: &Parent{
			UnresolvedInput: input,
		},
		offset: offset,
		limit:  limit,
	}
}
