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
	"math"
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
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
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
	result := &distributedPlan{
		queryTemplate: temp,
		s:             s,
		sortByTime:    true,
		offset:        ud.originalQuery.Offset,
		limit:         limit,
	}
	if ud.originalQuery.OrderBy == nil {
		return result, nil
	}
	if ud.originalQuery.OrderBy.IndexRuleName == "" {
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
	result.sortByTime = false
	result.sortTagSpec = *sortTagSpec
	if ud.originalQuery.OrderBy.Sort == modelv1.Sort_SORT_DESC {
		result.desc = true
	}
	return result, nil
}

var _ executor.StreamExecutable = (*distributedPlan)(nil)

type distributedPlan struct {
	// Overrides used by tests and instrumentation. Nil means the native frame decoder
	// (streamframe.Decode) or the shared aligned-batch egress
	// (vstream.BuildElementsFromBatch).
	decodeFrame    func([]byte) (*vectorized.RecordBatch, error)
	buildElements  vstream.ElementBuilder
	queryTemplate  *streamv1.QueryRequest
	s              logical.Schema
	sortTagSpec    logical.TagSpec
	maxElementSize uint32
	offset         uint32
	limit          uint32
	sortByTime     bool
	desc           bool
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
	nodes := make([]nodeStreamResult, len(streams))
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
	// Frames require distributed execution, raw wire mode and no tracing. Multi-group
	// queries still return proto replies, and so does a node that does not host a
	// requested group (an empty body). Any proto reply keeps the row merge for the whole
	// execution, including mixed replies during a rolling upgrade.
	var staged []*vectorized.RecordBatch
	var responseCount int
	rowPath := false
	for idx := range nodes {
		if !nodes[idx].responded {
			continue
		}
		responseCount++
		if span != nil {
			span.AddSubTrace(nodes[idx].subTrace)
		}
		for _, chunk := range nodes[idx].chunks {
			if chunk.batch != nil {
				staged = append(staged, chunk.batch)
			}
		}
		if nodes[idx].proto {
			rowPath = true
		}
	}
	// Release every reply batch on every exit, including a decode or scatter error, a merge
	// error and cancellation. ReleaseOwners is idempotent, so the pipeline releasing a batch it
	// consumed does not conflict with this.
	defer func() {
		for _, batch := range staged {
			batch.ReleaseOwners()
		}
	}()
	if firstErr != nil {
		err = firstErr
		return nil, firstErr
	}
	if rowPath {
		return t.mergeRowReplies(nodes, responseCount, span)
	}
	return t.mergeBatchReplies(ctx, staged, responseCount, span)
}

// nodeStreamResult is everything one node stream delivered.
type nodeStreamResult struct {
	err       error
	subTrace  *commonv1.Trace
	chunks    []nodeStreamChunk
	proto     bool
	responded bool
}

// nodeStreamChunk preserves wire order even when a node mixes proto and frame replies.
type nodeStreamChunk struct {
	batch    *vectorized.RecordBatch
	elements []*streamv1.Element
}

// readNodeStream reads a node stream to its end, decoding one chunk at a time. A proto chunk
// is kept as Elements; a native frame chunk is kept as the batch it decodes to, because the
// frame merge must never materialize rows it may discard.
func (t *distributedPlan) readNodeStream(ctx context.Context, ns bus.NodeStream) nodeStreamResult {
	var out nodeStreamResult
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
		decodeErr := t.decodeNodeChunk(d, &out)
		if decodeErr != nil {
			out.err = fmt.Errorf("stream query on node %s: %w", ns.Node(), decodeErr)
			return out
		}
		out.responded = true
	}
}

// decodeNodeChunk appends a single node chunk to out: the proto *streamv1.QueryResponse
// (flag-off / tracing / a data node that has not rolled onto the vec path) as Elements, and
// the native columnar frame ([]byte, only when the raw wire mode is on) as a batch. A frame
// is NOT materialized here; only the merge or the row fallback decides what reaches Elements.
// An index-ordered frame without the order-key column is rejected: sorting it by timestamp
// would silently change the order the client asked for.
func (t *distributedPlan) decodeNodeChunk(d any, out *nodeStreamResult) error {
	switch payload := d.(type) {
	case *streamv1.QueryResponse:
		out.proto = true
		out.chunks = append(out.chunks, nodeStreamChunk{elements: payload.Elements})
		if payload.Trace != nil {
			out.subTrace = payload.Trace
		}
		return nil
	case []byte:
		if !data.StreamWireModeRaw() {
			return fmt.Errorf("unexpected raw stream frame body while wire mode is off")
		}
		decode := t.decodeFrame
		if decode == nil {
			decode = streamframe.Decode
		}
		batch, decodeErr := decode(payload)
		if decodeErr != nil {
			return fmt.Errorf("decode stream result frame: %w", decodeErr)
		}
		if !t.sortByTime && batch.Schema.OrderKeyIndex() < 0 {
			batch.ReleaseOwners()
			return fmt.Errorf("index-ordered stream frame lacks an order key column")
		}
		out.chunks = append(out.chunks, nodeStreamChunk{batch: batch})
		return nil
	default:
		return fmt.Errorf("unexpected stream query response type %T", d)
	}
}

// mergeRowReplies handles proto and mixed replies, including traced and multi-group
// queries. Frame chunks are materialized here in wire order because the row merge
// cannot consume batches.
func (t *distributedPlan) mergeRowReplies(nodes []nodeStreamResult, responseCount int,
	span *query.Span,
) ([]*streamv1.Element, error) {
	var see []sort.Iterator[*comparableElement]
	for idx := range nodes {
		if !nodes[idx].responded {
			continue
		}
		var elements []*streamv1.Element
		for _, chunk := range nodes[idx].chunks {
			if chunk.batch != nil {
				batch := chunk.batch
				batchElements, buildErr := t.materialize(batch, vstream.ProjectionFromBatchSchema(batch))
				if buildErr != nil {
					return nil, fmt.Errorf("build elements from stream frame: %w", buildErr)
				}
				elements = append(elements, batchElements...)
			} else {
				elements = append(elements, chunk.elements...)
			}
		}
		see = append(see, newSortableElements(elements, t.sortByTime, t.sortTagSpec))
	}
	iter := sort.NewItemIter(see, t.desc)
	var merged []*streamv1.Element
	seen := make(map[string]bool)
	for iter.Next() {
		element := iter.Val().Element
		if !seen[element.ElementId] {
			seen[element.ElementId] = true
			merged = append(merged, element)
		}
	}
	if span != nil {
		span.Tagf("response_count", "%d", responseCount)
		span.Tagf("element_id_count", "%d", len(seen))
	}
	return t.applyWindow(merged), nil
}

// mergeBatchReplies is the frame-only path: the replies of every node are rebuilt onto one
// schema, merged, deduplicated and windowed as batches, and only the surviving rows are
// materialized into Elements.
func (t *distributedPlan) mergeBatchReplies(ctx context.Context, staged []*vectorized.RecordBatch, responseCount int,
	span *query.Span,
) ([]*streamv1.Element, error) {
	groups, alignErr := vstream.AlignStreamBatches(ctx, staged, t.sortByTime)
	if alignErr != nil {
		return nil, fmt.Errorf("align the stream frame replies: %w", alignErr)
	}
	// BatchSource drops its reference to a batch as it yields it, so keep a copy of the slice
	// to release the aligned batches, including any the pipeline never pulled.
	aligned := append([]*vectorized.RecordBatch(nil), groups.Batches...)
	defer func() {
		for _, batch := range aligned {
			batch.ReleaseOwners()
		}
	}()
	offset, limit := t.window()
	merged, mergeErr := groups.MergeCapped(ctx, t.desc, offset, limit)
	if mergeErr != nil {
		return nil, fmt.Errorf("merge the stream frame replies: %w", mergeErr)
	}
	elements, buildErr := groups.BuildElements(merged, t.materialize)
	if buildErr != nil {
		return nil, fmt.Errorf("materialize the merged stream batches: %w", buildErr)
	}
	if span != nil {
		span.Tagf("response_count", "%d", responseCount)
		// Keep the pre-pagination distinct count, including when a frame is supplied
		// under tracing. Normal traced replies use the row path.
		seen := make(map[int64]struct{})
		for _, batch := range staged {
			ids := batch.Columns[batch.Schema.ElementIDIndex()].(*vectorized.TypedColumn[int64]).Data()
			if batch.Selection == nil {
				for _, id := range ids[:batch.Len] {
					seen[id] = struct{}{}
				}
			} else {
				for _, row := range batch.Selection {
					seen[ids[row]] = struct{}{}
				}
			}
		}
		span.Tagf("element_id_count", "%d", len(seen))
	}
	return elements, nil
}

// materialize is the single batch-to-Element entry point for both reply paths.
func (t *distributedPlan) materialize(batch *vectorized.RecordBatch, projection []model.TagProjection) ([]*streamv1.Element, error) {
	build := t.buildElements
	if build == nil {
		build = vstream.BuildElementsFromBatch
	}
	return build(batch, projection)
}

// window returns the pipeline's offset/limit. A plan built without Analyze carries no limit,
// which must mean unbounded rather than the pipeline's "emit nothing".
func (t *distributedPlan) window() (uint32, uint32) {
	if t.limit == 0 {
		return 0, math.MaxUint32
	}
	return t.offset, t.limit
}

// applyWindow slices the row path's merged result, which is the same offset/limit the batch
// path applies in its pipeline. The batch path owns the window there, so the two never both
// slice one result.
func (t *distributedPlan) applyWindow(merged []*streamv1.Element) []*streamv1.Element {
	if t.limit == 0 {
		return merged
	}
	start := int(t.offset)
	if start > len(merged) {
		return []*streamv1.Element{}
	}
	end := start + int(t.limit)
	if end > len(merged) {
		end = len(merged)
	}
	return merged[start:end]
}

func (t *distributedPlan) String() string {
	return fmt.Sprintf("distributed:%s offset:%d limit:%d", t.queryTemplate.String(), t.offset, t.limit)
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
