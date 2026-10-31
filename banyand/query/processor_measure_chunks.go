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

	"github.com/apache/skywalking-banyandb/api/common"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/index/native"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/tracelabels"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	vmeasure "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure"
)

const (
	// defaultMeasureChunkRows is the default cap of rows in one scan chunk of a measure query.
	defaultMeasureChunkRows = 4096
	// defaultMeasureChunkBytes is the default cap of the estimated size of one scan chunk of a measure query.
	defaultMeasureChunkBytes = 4 << 20
)

var _ bus.StreamListener = (*measureInternalQueryProcessor)(nil)

// RevStream implements bus.StreamListener. It admits, plans and executes the internal query as Rev does, then
// sends the result to the sink in frames as the pipeline produces its batches, instead of draining the whole
// result into one body. A time-bucketed aggregation therefore sends its partials bucket by bucket, in time order,
// and every other aggregation sends its partial when the scan ends, because the plan produces it only then.
func (p *measureInternalQueryProcessor) RevStream(ctx context.Context, message bus.Message, sink bus.Sink) (err error) {
	internalRequest, patterns, decodeCtx, releaseDecode, decodeErr := p.decodeInternalRequest(ctx, message)
	if decodeErr != nil {
		return decodeErr
	}
	defer releaseDecode()
	ctx = decodeCtx
	queryCriteria := internalRequest.GetRequest()
	if queryCriteria == nil {
		return errors.New("query request is nil")
	}
	budget := p.queryBudget
	if budget == nil {
		budget = protector.QueryBudgetFor(nil)
	}
	admittedCtx, release, admissionErr := budget.AdmitMeasureContext(ctx)
	if admissionErr != nil {
		return measureStreamFailure(fmt.Errorf("query admission failed: %w", admissionErr))
	}
	defer release()
	defer func() {
		if r := recover(); r != nil {
			p.log.Error().Interface("err", r).RawJSON("req", logger.Proto(queryCriteria)).Str("stack", string(debug.Stack())).Msg("panic")
			err = fmt.Errorf("measure stream query panicked: %v", r)
		}
	}()
	// The scan reserves and releases memory for every block it reads; the lease has to follow what it holds now.
	admittedCtx = vectorized.WithLiveLeaseAccounting(admittedCtx)
	return p.executeMeasureStream(admittedCtx, message.ID(), internalRequest, patterns, sink)
}

// decodeInternalRequest returns the internal request of a message, which is either the decoded request or the
// envelope of a pattern query.
func (p *measureInternalQueryProcessor) decodeInternalRequest(ctx context.Context, message bus.Message) (
	*measurev1.InternalQueryRequest, []model.IndexedPattern, context.Context, func(), error,
) {
	noop := func() {}
	if body, isEnvelope := message.Data().([]byte); isEnvelope {
		decodeCtx, releaseDecode, reserveErr := p.reservePatternDecode(ctx, body)
		if reserveErr != nil {
			return nil, nil, ctx, noop, common.NewError("%v", reserveErr)
		}
		request, patterns, decodeErr := vmeasure.DecodeMeasurePatternRequest(body)
		if decodeErr != nil {
			releaseDecode()
			return nil, nil, ctx, noop, common.NewError("%v", decodeErr)
		}
		return request, patterns, decodeCtx, releaseDecode, nil
	}
	request, ok := message.Data().(*measurev1.InternalQueryRequest)
	if !ok {
		return nil, nil, ctx, noop, errors.New("invalid event data type")
	}
	return request, nil, ctx, noop, nil
}

func (p *measureInternalQueryProcessor) executeMeasureStream(ctx context.Context, id bus.MessageID, internalRequest *measurev1.InternalQueryRequest,
	patterns []model.IndexedPattern, sink bus.Sink,
) error {
	n := time.Now()
	queryCriteria := internalRequest.GetRequest()
	mctx, buildErr := buildMeasureContext(p.measureService, p.log, queryCriteria, "internal-measure", patterns...)
	if buildErr != nil {
		return measureStreamFailure(buildErr)
	}
	var tracer *query.Tracer
	var span *query.Span
	if queryCriteria.Trace {
		tracer, ctx = query.NewTracer(ctx, n.Format(time.RFC3339Nano))
		span, ctx = tracer.StartSpan(ctx, "data-%s", p.queryService.nodeID)
	}
	mIterator, planStr, execErr := executeMeasurePlan(ctx, queryCriteria, mctx, internalRequest.GetAggReturnPartial(), patterns...)
	if execErr != nil {
		if span != nil {
			span.Error(execErr)
			span.Stop()
		}
		return measureStreamFailure(execErr)
	}
	defer func() {
		if closeErr := mIterator.Close(); closeErr != nil {
			mctx.ml.Error().Err(closeErr).Dur("latency", time.Since(n)).RawJSON("req", logger.Proto(queryCriteria)).Msg("fail to close the query plan")
		}
	}()
	emitter, ok := mIterator.(vmeasure.FrameStreamEmitter)
	if !ok {
		emitErr := fmt.Errorf("measure stream query requires a FrameStreamEmitter iterator; got %T", mIterator)
		if span != nil {
			span.Error(emitErr)
			span.Stop()
		}
		return measureStreamFailure(emitErr)
	}
	limits := bus.ChunkLimitsFrom(ctx)
	frameLimits := vmeasure.FrameLimits{MaxRows: int(limits.MaxRows), MaxBytes: limits.MaxBytes}
	if frameLimits.MaxRows <= 0 {
		frameLimits.MaxRows = defaultMeasureChunkRows
	}
	if frameLimits.MaxBytes == 0 {
		frameLimits.MaxBytes = defaultMeasureChunkBytes
	}
	chunker := &measureChunker{sink: sink, id: id, hold: tracer != nil}
	if span != nil {
		span.Tag(tracelabels.TagPlan, planStr)
		span.Tag(tracelabels.TagRespKind, "raw-frame")
	}
	runErr := emitter.EmitFrames(ctx, frameLimits, func(body []byte) error { return chunker.add(ctx, body) })
	if runErr == nil {
		runErr = chunker.finish(ctx, func() *measurev1.InternalQueryResponse {
			span.Tagf(tracelabels.TagBytesOut, "%d", chunker.bytes)
			span.Tagf(tracelabels.TagRespCount, "%d", chunker.frames)
			span.Stop()
			return &measurev1.InternalQueryResponse{Trace: tracer.ToProto()}
		})
	}
	if runErr != nil {
		if !errors.Is(runErr, context.Canceled) {
			p.log.Error().Err(runErr).RawJSON("req", logger.Proto(queryCriteria)).Msg("fail to stream the measure query results")
		}
		if span != nil {
			span.Error(runErr)
			span.Stop()
		}
		return measureStreamFailure(runErr)
	}
	if !queryCriteria.Trace && p.slowQuery > 0 && time.Since(n) > p.slowQuery {
		p.log.Warn().Dur("latency", time.Since(n)).RawJSON("req", logger.Proto(queryCriteria)).Int("frames", chunker.frames).Msg("internal measure slow query (stream)")
	}
	return nil
}

// measureStreamFailure returns the error a measure scan ends with. A query that ran out of memory, whether of its
// lease, of its scan budget or of the node, carries STATUS_RESOURCE_EXHAUSTED so the liaison can report it as such;
// every other error keeps its identity, so cancellation stays cancellation.
func measureStreamFailure(err error) error {
	if errors.Is(err, vectorized.ErrMemoryBudgetExceeded) || errors.Is(err, native.ErrQueryLimit) {
		return common.NewErrorWithStatus(modelv1.Status_STATUS_RESOURCE_EXHAUSTED, err.Error())
	}
	return streamFailure(err)
}

// measureChunker sends frames to the sink. When the query is traced it keeps the last frame back, so that the
// frame can carry the data node's trace.
type measureChunker struct {
	sink   bus.Sink
	held   []byte
	id     bus.MessageID
	bytes  int
	frames int
	hold   bool
}

func (c *measureChunker) add(ctx context.Context, body []byte) error {
	if len(body) == 0 {
		return nil
	}
	c.bytes += len(body)
	c.frames++
	if !c.hold {
		return c.send(ctx, body)
	}
	prev := c.held
	c.held = body
	if prev == nil {
		return nil
	}
	return c.send(ctx, prev)
}

// finish sends the frame held back, wrapped with the trace the attach function builds.
func (c *measureChunker) finish(ctx context.Context, attach func() *measurev1.InternalQueryResponse) error {
	if !c.hold {
		return nil
	}
	last := attach()
	last.RawFrameBody = c.held
	c.held = nil
	return c.send(ctx, last)
}

func (c *measureChunker) send(ctx context.Context, payload any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return c.sink.Send(ctx, bus.NewMessage(c.id, payload))
}
