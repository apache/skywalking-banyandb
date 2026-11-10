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
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
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
	// (streamframe.Decode) or the frame row builder
	// (vstream.BuildElementsFromBatch).
	decodeFrame    func([]byte) (*vectorized.RecordBatch, error)
	buildElement   func(*vectorized.RecordBatch, []model.TagProjection, int) (*streamv1.Element, error)
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
	// Keep decoded batches alive through materialization, including when a
	// node reports an error after sending a batch.
	defer func() {
		for _, node := range nodes {
			for _, chunk := range node.chunks {
				if chunk.batch != nil {
					chunk.batch.ReleaseOwners()
				}
			}
		}
	}()
	if firstErr != nil {
		return nil, firstErr
	}
	return t.mergeRowReplies(ctx, nodes, span)
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
// is NOT materialized here; only the row merge decides which rows become Elements.
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
		if batch.Schema.TimestampIndex() < 0 || batch.Schema.ElementIDIndex() < 0 {
			batch.ReleaseOwners()
			return fmt.Errorf("stream frame lacks a timestamp or element ID column")
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

// mergeRowReplies uses one iterator per node, regardless of the wire format.
func (t *distributedPlan) mergeRowReplies(ctx context.Context, nodes []nodeStreamResult, span *query.Span) ([]*streamv1.Element, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	iters := make([]sort.Iterator[*distributedRow], 0, len(nodes))
	frameOnly := true
	for _, node := range nodes {
		if !node.responded {
			continue
		}
		frameOnly = frameOnly && !node.proto
		if span != nil {
			span.AddSubTrace(node.subTrace)
		}
		iters = append(iters, &sortableNodeRows{chunks: node.chunks, plan: t})
	}
	iter := sort.NewItemIter(iters, t.desc)
	frameIDs := make(map[uint64]struct{})
	elementIDs := make(map[string]struct{})
	var result []*streamv1.Element
	var distinct uint64
	window := uint64(t.offset) + uint64(t.limit)
	for iter.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		row := iter.Val()
		if frameOnly {
			if _, seen := frameIDs[row.id]; seen {
				continue
			}
			frameIDs[row.id] = struct{}{}
		} else {
			id := row.elementID()
			if _, seen := elementIDs[id]; seen {
				continue
			}
			elementIDs[id] = struct{}{}
		}
		distinct++
		if distinct > uint64(t.offset) && (t.limit == 0 || distinct <= window) {
			element, err := t.materialize(row)
			if err != nil {
				return nil, fmt.Errorf("build element from stream frame: %w", err)
			}
			result = append(result, element)
		}
		// Tracing reports the full distinct count even when the page is full.
		if span == nil && t.limit != 0 && distinct == window {
			break
		}
	}
	if span != nil {
		span.Tagf("response_count", "%d", len(iters))
		span.Tagf("element_id_count", "%d", distinct)
	}
	return result, nil
}

func (t *distributedPlan) materialize(row *distributedRow) (*streamv1.Element, error) {
	if row.frame == nil {
		return row.element, nil
	}
	build := t.buildElement
	if build == nil {
		build = vstream.BuildElementFromBatchRow
	}
	element, err := build(row.frame, row.projection, row.index)
	if err == nil && row.frame.Owners.Len() != 0 {
		// The response outlives the batch. Detach any borrowed tag values.
		element = proto.CloneOf(element)
		for _, family := range element.GetTagFamilies() {
			for _, tag := range family.GetTags() {
				if str := tag.GetValue().GetStr(); str != nil {
					str.Value = strings.Clone(str.Value)
				}
				if arr := tag.GetValue().GetStrArray(); arr != nil {
					for i, str := range arr.Value {
						arr.Value[i] = strings.Clone(str)
					}
				}
			}
		}
	}
	return element, err
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

type distributedRow struct {
	frame      *vectorized.RecordBatch
	element    *streamv1.Element
	projection []model.TagProjection
	sortField  []byte
	timeKey    [8]byte
	id         uint64
	index      int
}

func (r *distributedRow) SortedField() []byte { return r.sortField }

func (r *distributedRow) elementID() string {
	if r.frame == nil {
		return r.element.GetElementId()
	}
	return hex.EncodeToString(convert.Uint64ToBytes(r.id))
}

type sortableNodeRows struct {
	chunks        []nodeStreamChunk
	plan          *distributedPlan
	cur           *distributedRow
	projection    []model.TagProjection
	chunk, active int
}

func (*sortableNodeRows) Close() error           { return nil }
func (s *sortableNodeRows) Val() *distributedRow { return s.cur }

func (s *sortableNodeRows) Next() bool {
	for s.chunk < len(s.chunks) {
		chunk := s.chunks[s.chunk]
		if chunk.batch != nil {
			if s.active < chunk.batch.ActiveLen() {
				s.cur = s.frameRow(chunk.batch)
				s.active++
				return true
			}
		} else if s.active < len(chunk.elements) {
			element := chunk.elements[s.active]
			s.active++
			row, err := s.protoRow(element)
			if err != nil {
				continue
			}
			s.cur = row
			return true
		}
		s.chunk++
		s.active = 0
	}
	return false
}

func (s *sortableNodeRows) frameRow(batch *vectorized.RecordBatch) *distributedRow {
	if s.active == 0 {
		s.projection = vstream.ProjectionFromBatchSchema(batch)
	}
	index := s.active
	if batch.Selection != nil {
		index = int(batch.Selection[index])
	}
	id := batch.Columns[batch.Schema.ElementIDIndex()].(*vectorized.TypedColumn[int64]).Data()[index]
	row := &distributedRow{frame: batch, projection: s.projection, id: vstream.ColumnToElementID(id), index: index}
	if s.plan.sortByTime {
		ts := batch.Columns[batch.Schema.TimestampIndex()].(*vectorized.TypedColumn[int64]).Data()[index]
		row.setTimeKey(ts)
	} else {
		row.sortField = batch.Columns[batch.Schema.OrderKeyIndex()].(*vectorized.TypedColumn[[]byte]).Data()[index]
	}
	return row
}

func (s *sortableNodeRows) protoRow(element *streamv1.Element) (*distributedRow, error) {
	row := &distributedRow{element: element}
	if s.plan.sortByTime {
		row.setTimeKey(element.GetTimestamp().AsTime().UnixNano())
		return row, nil
	}
	spec := s.plan.sortTagSpec
	families := element.GetTagFamilies()
	if spec.TagFamilyIdx >= len(families) || spec.TagIdx >= len(families[spec.TagFamilyIdx].GetTags()) {
		return nil, fmt.Errorf("stream element lacks its sort tag")
	}
	var err error
	row.sortField, err = pbv1.MarshalTagValue(families[spec.TagFamilyIdx].GetTags()[spec.TagIdx].GetValue())
	return row, err
}

func (r *distributedRow) setTimeKey(ts int64) {
	binary.BigEndian.PutUint64(r.timeKey[:], uint64(ts))
	r.sortField = r.timeKey[:]
}
