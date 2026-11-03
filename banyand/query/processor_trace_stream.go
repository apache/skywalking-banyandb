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

	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/iter"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	logical_trace "github.com/apache/skywalking-banyandb/pkg/query/logical/trace"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
)

const (
	// defaultTraceChunkRows is the default cap of traces in one scan chunk.
	defaultTraceChunkRows = 256
	// defaultTraceChunkBytes is the default cap of the estimated encoded size of one scan chunk.
	defaultTraceChunkBytes = 4 << 20
)

var _ bus.StreamListener = (*traceQueryProcessor)(nil)

// RevStream implements bus.StreamListener. It admits, plans and executes the query as
// Rev does, then sends the traces to the sink in chunks as the iterator produces them.
func (p *traceQueryProcessor) RevStream(ctx context.Context, message bus.Message, sink bus.Sink) (err error) {
	queryCriteria, ok := message.Data().(*tracev1.QueryRequest)
	if !ok {
		return errors.New("invalid event data type")
	}
	budget := p.queryBudget
	if budget == nil {
		budget = protector.QueryBudgetFor(nil)
	}
	admittedCtx, release, admissionErr := budget.AdmitRequestContext(ctx, queryCriteria.GetLimit(), queryCriteria.GetOffset(), 20)
	if admissionErr != nil {
		return fmt.Errorf("query admission failed: %w", admissionErr)
	}
	defer release()
	defer func() {
		if r := recover(); r != nil {
			p.log.Error().Interface("err", r).RawJSON("req", logger.Proto(queryCriteria)).Str("stack", string(debug.Stack())).Msg("panic")
			err = fmt.Errorf("trace query panicked: %v", r)
		}
	}()
	return p.executeQueryStream(admittedCtx, message.ID(), queryCriteria, sink)
}

func (p *traceQueryProcessor) executeQueryStream(ctx context.Context, id bus.MessageID, queryCriteria *tracev1.QueryRequest, sink bus.Sink) error {
	n := time.Now()
	execPlan, setupErr := p.setupTraceExecutionPlan(queryCriteria)
	if setupErr != nil {
		return setupErr
	}
	traceExecContexts := make([]executor.TraceExecutionContext, len(execPlan.executionContexts))
	for i, ec := range execPlan.executionContexts {
		traceExecContexts[i] = ec
	}
	plan, err := logical_trace.Analyze(queryCriteria, execPlan.metadata, execPlan.schemas, traceExecContexts,
		execPlan.traceIDTagNames, execPlan.spanIDTagNames, execPlan.timestampTagNames)
	if err != nil {
		return fmt.Errorf("fail to analyze the query request for trace %s: %w", queryCriteria.GetName(), err)
	}
	ctx, traceMonitor := p.setupTraceMonitor(ctx, queryCriteria, plan, n)
	te := plan.(executor.TraceExecutable)
	defer te.Close()
	resultIterator, err := te.Execute(ctx)
	if err != nil {
		p.log.Error().Err(err).RawJSON("req", logger.Proto(queryCriteria)).Msg("fail to execute the trace query plan")
		return finishStreamTrace(traceMonitor, 0, fmt.Errorf("execute the query plan for trace %s: %w", queryCriteria.GetName(), err))
	}

	limits := bus.ChunkLimitsFrom(ctx)
	maxRows, maxBytes := int(limits.MaxRows), limits.MaxBytes
	if maxRows <= 0 {
		maxRows = defaultTraceChunkRows
	}
	if maxBytes == 0 {
		maxBytes = defaultTraceChunkBytes
	}
	chunker := &traceChunker{sink: sink, id: id, maxRows: maxRows, maxBytes: maxBytes}
	var builder *traceFrameBuilder
	if p.distributed && traceMonitor == nil {
		builder = newTraceFrameBuilder(queryCriteria, execPlan)
		chunker.encode = func(rows []model.TraceResult) (any, error) {
			return logical_trace.EncodeTraceResultFrame(rows), nil
		}
	} else {
		// The traced and standalone path keeps the proto body, as the unary path does.
		chunker.hold = traceMonitor != nil
		chunker.encode = func(rows []model.TraceResult) (any, error) {
			traces, procErr := p.processTraceResults(iter.FromSlice(rows), queryCriteria, execPlan)
			if procErr != nil {
				return nil, procErr
			}
			return &tracev1.InternalQueryResponse{InternalTraces: traces}, nil
		}
	}
	spanCount, scanErr := scanTraceResults(ctx, resultIterator, builder, chunker)
	if scanErr != nil {
		p.log.Error().Err(scanErr).RawJSON("req", logger.Proto(queryCriteria)).Msg("fail to stream trace results")
		return finishStreamTrace(traceMonitor, spanCount, fmt.Errorf("process trace results for trace %s: %w", queryCriteria.GetName(), scanErr))
	}
	finishErr := chunker.finish(ctx, func(last any) {
		if resp, ok := last.(*tracev1.InternalQueryResponse); ok && traceMonitor != nil {
			traceMonitor.span.Tag("resp_count", fmt.Sprintf("%d", spanCount))
			traceMonitor.span.Stop()
			resp.TraceQueryResult = traceMonitor.tracer.ToProto()
		}
	})
	if finishErr != nil {
		return finishStreamTrace(traceMonitor, spanCount, finishErr)
	}
	p.logSlowTraceQuery(queryCriteria, spanCount, n)
	return nil
}

// scanTraceResults pulls the results into the chunker until the iterator ends, the
// context is canceled or an error occurs. It returns the number of spans read.
func scanTraceResults(ctx context.Context, resultIterator iter.Iterator[model.TraceResult], builder *traceFrameBuilder, chunker *traceChunker) (int, error) {
	spanCount := 0
	for {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return spanCount, ctxErr
		}
		result, hasNext := resultIterator.Next()
		if result.Error != nil {
			return spanCount, result.Error
		}
		if !hasNext {
			return spanCount, nil
		}
		if result.TID == "" {
			continue
		}
		spanCount += len(result.Spans)
		if builder != nil {
			result = builder.build(&result)
		}
		if addErr := chunker.add(ctx, result); addErr != nil {
			return spanCount, addErr
		}
	}
}

// finishStreamTrace closes the data node's trace span on a failed scan and returns err.
func finishStreamTrace(tm *traceMonitor, count int, err error) error {
	if tm != nil {
		tm.span.Tag("resp_count", fmt.Sprintf("%d", count))
		tm.span.Error(err)
		tm.span.Stop()
	}
	return err
}

// traceChunker groups traces into chunks bounded by a row count and an estimated
// encoded size. A trace that alone exceeds the byte cap goes in its own chunk.
type traceChunker struct {
	sink bus.Sink
	// encode turns a full chunk into the payload to send.
	encode func(rows []model.TraceResult) (any, error)
	// pending is the chunk held back when hold is set, so the last chunk can carry the query trace.
	pending  any
	rows     []model.TraceResult
	maxBytes uint64
	size     uint64
	id       bus.MessageID
	maxRows  int
	hold     bool
}

func (c *traceChunker) add(ctx context.Context, result model.TraceResult) error {
	est := uint64(logical_trace.ResultFrameSize(&result))
	if len(c.rows) > 0 && c.size+est > c.maxBytes {
		if err := c.flush(ctx); err != nil {
			return err
		}
	}
	c.rows = append(c.rows, result)
	c.size += est
	if len(c.rows) >= c.maxRows || c.size >= c.maxBytes {
		return c.flush(ctx)
	}
	return nil
}

func (c *traceChunker) flush(ctx context.Context) error {
	if len(c.rows) == 0 {
		return nil
	}
	payload, err := c.encode(c.rows)
	clear(c.rows)
	c.rows = c.rows[:0]
	c.size = 0
	if err != nil {
		return err
	}
	if !c.hold {
		return c.send(ctx, payload)
	}
	prev := c.pending
	c.pending = payload
	if prev == nil {
		return nil
	}
	return c.send(ctx, prev)
}

// finish sends the remaining rows. When the chunker holds the last chunk back, attach
// receives it before it is sent, and an empty chunk is made if no trace was produced.
func (c *traceChunker) finish(ctx context.Context, attach func(last any)) error {
	if err := c.flush(ctx); err != nil {
		return err
	}
	if !c.hold {
		return nil
	}
	if c.pending == nil {
		empty, err := c.encode(nil)
		if err != nil {
			return err
		}
		c.pending = empty
	}
	attach(c.pending)
	last := c.pending
	c.pending = nil
	return c.send(ctx, last)
}

func (c *traceChunker) send(ctx context.Context, payload any) error {
	if err := query.ChargeResponse(ctx, payload); err != nil {
		return fmt.Errorf("query response memory exhausted: %w", err)
	}
	return c.sink.Send(ctx, bus.NewMessage(c.id, payload))
}
