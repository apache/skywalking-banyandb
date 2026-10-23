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

package query

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
	logical_stream "github.com/apache/skywalking-banyandb/pkg/query/logical/stream"
	"github.com/apache/skywalking-banyandb/pkg/query/tracelabels"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	vstream "github.com/apache/skywalking-banyandb/pkg/query/vectorized/stream"
	streamframe "github.com/apache/skywalking-banyandb/pkg/query/vectorized/stream/frame"
)

const (
	// defaultStreamChunkRows is the default cap of elements in one scan chunk.
	defaultStreamChunkRows = 4096
	// defaultStreamChunkBytes is the default cap of the estimated encoded size of one scan chunk.
	defaultStreamChunkBytes = 4 << 20
)

var _ bus.StreamListener = (*streamQueryProcessor)(nil)

// RevStream implements bus.StreamListener. It admits, plans and executes the query as
// Rev does, then sends the result to the sink in chunks as the plan produces its output
// batches, instead of collecting the whole response first.
func (p *streamQueryProcessor) RevStream(ctx context.Context, message bus.Message, sink bus.Sink) (err error) {
	queryCriteria, ok := message.Data().(*streamv1.QueryRequest)
	if !ok {
		return errors.New("invalid event data type")
	}
	budget := p.queryBudget
	if budget == nil {
		budget = protector.QueryBudgetFor(nil)
	}
	admittedCtx, release, admissionErr := budget.AdmitRequestContext(ctx, queryCriteria.GetLimit(), queryCriteria.GetOffset(), 20)
	if admissionErr != nil {
		return streamFailure(fmt.Errorf("query admission failed: %w", admissionErr))
	}
	defer release()
	defer func() {
		if r := recover(); r != nil {
			p.log.Error().Interface("err", r).RawJSON("req", logger.Proto(queryCriteria)).Str("stack", string(debug.Stack())).Msg("panic")
			err = fmt.Errorf("stream query panicked: %v", r)
		}
	}()
	return p.executeStreamQuery(admittedCtx, message.ID(), queryCriteria, sink)
}

func (p *streamQueryProcessor) executeStreamQuery(ctx context.Context, id bus.MessageID, queryCriteria *streamv1.QueryRequest, sink bus.Sink) error {
	n := time.Now()
	plan, err := p.buildStreamPlan(queryCriteria)
	if err != nil {
		return streamFailure(err)
	}
	defer plan.(executor.StreamCloser).Close()
	var tracer *query.Tracer
	var span *query.Span
	if queryCriteria.Trace {
		tracer, ctx = query.NewTracer(ctx, n.Format(time.RFC3339Nano))
		span, ctx = tracer.StartSpan(ctx, "data-%s", p.queryService.nodeID)
		span.Tag(tracelabels.TagPlan, plan.String())
	}
	limits := bus.ChunkLimitsFrom(ctx)
	maxRows, maxBytes := int(limits.MaxRows), limits.MaxBytes
	if maxRows <= 0 {
		maxRows = defaultStreamChunkRows
	}
	if maxBytes == 0 {
		maxBytes = defaultStreamChunkBytes
	}
	chunker := &streamChunker{sink: sink, id: id, maxRows: maxRows, maxBytes: maxBytes, hold: tracer != nil}
	count, runErr := p.streamPlanChunks(ctx, plan, queryCriteria, chunker)
	if runErr == nil {
		runErr = chunker.finish(ctx, func(last *streamv1.QueryResponse) {
			if tracer != nil {
				span.Tag(tracelabels.TagRespCount, fmt.Sprintf("%d", count))
				span.Stop()
				last.Trace = tracer.ToProto()
			}
		})
	}
	if runErr != nil {
		p.log.Error().Err(runErr).RawJSON("req", logger.Proto(queryCriteria)).Msg("fail to stream the stream query results")
		if span != nil {
			span.Tag(tracelabels.TagRespCount, fmt.Sprintf("%d", count))
			span.Error(runErr)
			span.Stop()
		}
		return streamFailure(runErr)
	}
	if !queryCriteria.Trace && p.slowQuery > 0 && time.Since(n) > p.slowQuery {
		p.log.Warn().Dur("latency", time.Since(n)).RawJSON("req", logger.Proto(queryCriteria)).Int("resp_count", count).Msg("stream slow query")
	}
	return nil
}

// streamPlanChunks runs the plan and feeds the chunker, returning the number of elements sent.
func (p *streamQueryProcessor) streamPlanChunks(ctx context.Context, plan logical.Plan, queryCriteria *streamv1.QueryRequest,
	chunker *streamChunker,
) (int, error) {
	if merge, isMerge := logical_stream.VecMergeExecutable(plan); isMerge {
		return p.streamMergeChunks(ctx, merge, queryCriteria, chunker)
	}
	vecExec := logical_stream.VecExecutable(plan)
	if vecExec == nil {
		reason := logical_stream.VecDeclineReason(plan)
		p.log.Error().Str("plan", plan.String()).Str("reason", reason).
			RawJSON("req", logger.Proto(queryCriteria)).Msg("no vectorized execution path for the query plan")
		return 0, common.NewError("no execution path for stream %s: %s; row execution was removed in 0.12.0 (apache/skywalking#13998)",
			queryCriteria.GetName(), reason)
	}
	batches, schema, openErr := vecExec.OpenVectorized(ctx)
	if openErr != nil {
		return 0, streamFailure(fmt.Errorf("execute the vectorized query plan for stream %s: %w", queryCriteria.GetName(), openErr))
	}
	defer func() {
		_ = batches.Close()
	}()
	criteria, hasFilter := logical_stream.VecTagFilter(plan)
	asFrame := streamVecEmitAsFrame(p.distributed, data.StreamWireModeRaw(), chunker.hold)
	var window *rowWindow
	if asFrame {
		// The liaison applies the global offset and limit, so a frame carries every row.
		chunker.encode = func(batch *vectorized.RecordBatch) (any, error) {
			return streamframe.Encode(batch)
		}
	} else {
		chunker.encode = encodeStreamElements
		if offset, limit, ok := logical_stream.VecOffsetLimit(plan); ok {
			window = &rowWindow{skip: int(offset), remaining: int(limit)}
		}
	}
	stage := func(batch *vectorized.RecordBatch) (*vectorized.RecordBatch, error) {
		if hasFilter && !criteria.PreMerged {
			if filterErr := filterStreamBatches(ctx, schema, vecExec.ProjectionTags(), criteria, []*vectorized.RecordBatch{batch}); filterErr != nil {
				return nil, streamFailure(fmt.Errorf("filter the vectorized stream batches for stream %s: %w", queryCriteria.GetName(), filterErr))
			}
		}
		dense, mergeErr := mergeStreamBatches(schema, []*vectorized.RecordBatch{batch}, criteria.HiddenTags, vecExec.HiddenOrderTag())
		if mergeErr != nil {
			return nil, streamFailure(fmt.Errorf("merge the vectorized stream batches for stream %s: %w", queryCriteria.GetName(), mergeErr))
		}
		return dense, nil
	}
	return pumpStreamBatches(ctx, batches, stage, window, chunker)
}

// streamMergeChunks runs a multi-group query. Its cross-group merge is element-level, so
// the merged elements are cut into proto chunks.
func (p *streamQueryProcessor) streamMergeChunks(ctx context.Context, merge *logical_stream.VecMerge, queryCriteria *streamv1.QueryRequest,
	chunker *streamChunker,
) (int, error) {
	_, msg := p.tryVecMergeDispatch(ctx, merge, queryCriteria, time.Now().UnixNano())
	switch d := msg.Data().(type) {
	case *streamv1.QueryResponse:
		for _, chunk := range splitElementChunks(d.Elements, chunker.maxRows, chunker.maxBytes) {
			if sendErr := chunker.sendResponse(ctx, chunk); sendErr != nil {
				return 0, sendErr
			}
		}
		return len(d.Elements), nil
	case *common.Error:
		return 0, d
	default:
		return 0, fmt.Errorf("unexpected multi-group stream result type %T", d)
	}
}

// streamFailure returns the error a stream scan ends with. A query that ran out of memory
// carries STATUS_RESOURCE_EXHAUSTED so the liaison can report it as such; every other error
// is returned as it is, so its identity (such as context cancellation) is kept.
func streamFailure(err error) error {
	if failure := protector.QueryFailure(err, "%v", err); failure.Status() == modelv1.Status_STATUS_RESOURCE_EXHAUSTED {
		return failure
	}
	return err
}

// rowWindow is the offset:offset+limit window over the rows of a response.
type rowWindow struct {
	skip      int
	remaining int
}

// take returns the part [lo, hi) of the next n rows that falls inside the window.
func (w *rowWindow) take(n int) (lo, hi int) {
	lo = min(w.skip, n)
	w.skip -= lo
	hi = lo + min(w.remaining, n-lo)
	w.remaining -= hi - lo
	return lo, hi
}

func (w *rowWindow) exhausted() bool { return w != nil && w.remaining <= 0 }

// pumpStreamBatches pulls the output batches, stages each into a dense batch and feeds the
// chunker until the plan ends, the window is full, the context is canceled or an error
// occurs. It returns the number of rows handed to the chunker.
func pumpStreamBatches(ctx context.Context, batches executor.BatchIterator,
	stage func(*vectorized.RecordBatch) (*vectorized.RecordBatch, error), window *rowWindow, chunker *streamChunker,
) (int, error) {
	count := 0
	for !window.exhausted() {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return count, ctxErr
		}
		batch, nextErr := batches.NextBatch(ctx)
		if nextErr != nil {
			return count, nextErr
		}
		if batch == nil {
			return count, nil
		}
		dense, stageErr := stage(batch)
		if stageErr != nil {
			return count, stageErr
		}
		if dense == nil || dense.Len == 0 {
			continue
		}
		lo, hi := 0, dense.Len
		if window != nil {
			lo, hi = window.take(dense.Len)
		}
		if hi <= lo {
			continue
		}
		if addErr := chunker.add(ctx, dense, lo, hi); addErr != nil {
			return count, addErr
		}
		count += hi - lo
	}
	return count, nil
}

func encodeStreamElements(batch *vectorized.RecordBatch) (any, error) {
	elements, err := vstream.BuildElementsFromBatch(batch, vstream.ProjectionFromBatchSchema(batch))
	if err != nil {
		return nil, fmt.Errorf("materialize vectorized stream elements: %w", err)
	}
	return &streamv1.QueryResponse{Elements: elements}, nil
}

// splitElementChunks cuts elements into responses bounded by a row count and the proto size.
// An element that alone exceeds the byte cap goes in its own response. It returns one empty
// response for no elements, so the query trace always has a response to ride on.
func splitElementChunks(elements []*streamv1.Element, maxRows int, maxBytes uint64) []*streamv1.QueryResponse {
	var out []*streamv1.QueryResponse
	var cur []*streamv1.Element
	var size uint64
	flush := func() {
		if len(cur) > 0 {
			out = append(out, &streamv1.QueryResponse{Elements: cur})
			cur, size = nil, 0
		}
	}
	for _, element := range elements {
		est := uint64(proto.Size(element))
		if len(cur) > 0 && size+est > maxBytes {
			flush()
		}
		cur = append(cur, element)
		size += est
		if len(cur) >= maxRows || size >= maxBytes {
			flush()
		}
	}
	flush()
	if len(out) == 0 {
		out = append(out, &streamv1.QueryResponse{})
	}
	return out
}

// streamChunker groups dense batches into chunks bounded by a row count and an estimated
// encoded size. A row that alone exceeds the byte cap goes in its own chunk.
type streamChunker struct {
	sink bus.Sink
	// encode turns a full chunk into the payload to send.
	encode func(batch *vectorized.RecordBatch) (any, error)
	// held is the chunk kept back when hold is set, so the last chunk can carry the query trace.
	held     *streamv1.QueryResponse
	pending  *vectorized.RecordBatch
	maxBytes uint64
	size     uint64
	id       bus.MessageID
	maxRows  int
	hold     bool
}

// add appends rows [lo, hi) of the dense batch, sending a chunk whenever one fills up.
func (c *streamChunker) add(ctx context.Context, dense *vectorized.RecordBatch, lo, hi int) error {
	for lo < hi {
		if c.pending == nil {
			c.pending = vectorized.NewRecordBatch(dense.Schema, min(c.maxRows, hi-lo))
		}
		end, est := lo, uint64(0)
		for end < hi && c.pending.Len+(end-lo) < c.maxRows {
			rowEst := estimateRowBytes(dense, end)
			if c.pending.Len+(end-lo) > 0 && c.size+est+rowEst > c.maxBytes {
				break
			}
			est += rowEst
			end++
		}
		moved := end - lo
		if moved > 0 {
			for colIdx := range dense.Columns {
				if appendErr := vectorized.AppendColumnRange(c.pending.Columns[colIdx], dense.Columns[colIdx], lo, end-lo); appendErr != nil {
					return appendErr
				}
			}
			c.pending.Len += moved
			c.size += est
			lo = end
		}
		if c.pending.Len >= c.maxRows || c.size >= c.maxBytes || moved == 0 {
			if flushErr := c.flush(ctx); flushErr != nil {
				return flushErr
			}
		}
	}
	return nil
}

func (c *streamChunker) flush(ctx context.Context) error {
	if c.pending == nil || c.pending.Len == 0 {
		return nil
	}
	payload, err := c.encode(c.pending)
	c.pending, c.size = nil, 0
	if err != nil {
		return err
	}
	if !c.hold {
		return c.send(ctx, payload)
	}
	resp, ok := payload.(*streamv1.QueryResponse)
	if !ok {
		return fmt.Errorf("a traced stream chunk must be a query response, got %T", payload)
	}
	prev := c.held
	c.held = resp
	if prev == nil {
		return nil
	}
	return c.send(ctx, prev)
}

// finish sends the remaining rows. When the chunker holds the last chunk back, attach
// receives it before it is sent, and an empty chunk is made if nothing was produced.
func (c *streamChunker) finish(ctx context.Context, attach func(last *streamv1.QueryResponse)) error {
	if err := c.flush(ctx); err != nil {
		return err
	}
	if !c.hold {
		return nil
	}
	if c.held == nil {
		c.held = &streamv1.QueryResponse{}
	}
	last := c.held
	c.held = nil
	attach(last)
	return c.send(ctx, last)
}

// sendResponse sends a ready proto chunk, holding it back while the trace is pending.
func (c *streamChunker) sendResponse(ctx context.Context, resp *streamv1.QueryResponse) error {
	if !c.hold {
		return c.send(ctx, resp)
	}
	prev := c.held
	c.held = resp
	if prev == nil {
		return nil
	}
	return c.send(ctx, prev)
}

func (c *streamChunker) send(ctx context.Context, payload any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := query.ChargeResponse(ctx, payload); err != nil {
		return streamFailure(fmt.Errorf("query response memory exhausted: %w", err))
	}
	return c.sink.Send(ctx, bus.NewMessage(c.id, payload))
}

// estimateRowBytes estimates the encoded size of one row of a dense batch.
func estimateRowBytes(batch *vectorized.RecordBatch, row int) uint64 {
	var size uint64
	for _, column := range batch.Columns {
		size++
		if column.IsNull(row) {
			continue
		}
		switch typed := column.(type) {
		case *vectorized.TypedColumn[int64], *vectorized.TypedColumn[float64]:
			size += 8
		case *vectorized.TypedColumn[string]:
			size += 4 + uint64(len(typed.Data()[row]))
		case *vectorized.TypedColumn[[]byte]:
			size += 4 + uint64(len(typed.Data()[row]))
		case *vectorized.TypedColumn[*modelv1.TagValue]:
			size += 4
			if value := typed.Data()[row]; value != nil {
				size += uint64(proto.Size(value))
			}
		default:
			size += 16
		}
	}
	return size
}
