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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/banyand/trace"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	logical_trace "github.com/apache/skywalking-banyandb/pkg/query/logical/trace"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

type recordingSink struct {
	hook   func(n int)
	chunks [][]byte
}

func (r *recordingSink) Send(ctx context.Context, chunk bus.Message) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.chunks = append(r.chunks, chunk.Data().([]byte))
	if r.hook != nil {
		r.hook(len(r.chunks))
	}
	return nil
}

func frameChunker(sink bus.Sink, maxRows int, maxBytes uint64) *traceChunker {
	return &traceChunker{
		sink:     sink,
		maxRows:  maxRows,
		maxBytes: maxBytes,
		encode: func(rows []model.TraceResult) (any, error) {
			return logical_trace.EncodeTraceResultFrame(rows), nil
		},
	}
}

func spanPayloadResult(tid string, payloadSize int) model.TraceResult {
	return model.TraceResult{TID: tid, Spans: [][]byte{make([]byte, payloadSize)}, SpanIDs: []string{tid}}
}

func decodeChunkTIDs(t *testing.T, chunks [][]byte) [][]string {
	t.Helper()
	out := make([][]string, 0, len(chunks))
	for _, c := range chunks {
		rows, err := logical_trace.DecodeTraceResultFrame(c)
		require.NoError(t, err)
		tids := make([]string, 0, len(rows))
		for i := range rows {
			tids = append(tids, rows[i].TID)
		}
		out = append(out, tids)
	}
	return out
}

func TestTraceChunkerRowCap(t *testing.T) {
	sink := &recordingSink{}
	chunker := frameChunker(sink, 3, 1<<20)
	for i := 0; i < 7; i++ {
		require.NoError(t, chunker.add(context.Background(), spanPayloadResult(fmt.Sprintf("t%d", i), 8)))
	}
	require.NoError(t, chunker.finish(context.Background(), nil))
	require.Equal(t, [][]string{{"t0", "t1", "t2"}, {"t3", "t4", "t5"}, {"t6"}}, decodeChunkTIDs(t, sink.chunks))
}

func TestTraceChunkerByteCap(t *testing.T) {
	sink := &recordingSink{}
	one := spanPayloadResult("t0", 100)
	oneSize := uint64(logical_trace.ResultFrameSize(&one))
	// Room for two traces but not three.
	chunker := frameChunker(sink, 1000, oneSize*2+oneSize/2)
	for i := 0; i < 5; i++ {
		require.NoError(t, chunker.add(context.Background(), spanPayloadResult(fmt.Sprintf("t%d", i), 100)))
	}
	require.NoError(t, chunker.finish(context.Background(), nil))
	require.Equal(t, [][]string{{"t0", "t1"}, {"t2", "t3"}, {"t4"}}, decodeChunkTIDs(t, sink.chunks))
}

func TestTraceChunkerOversizedTraceGoesAlone(t *testing.T) {
	sink := &recordingSink{}
	chunker := frameChunker(sink, 1000, 1024)
	for _, r := range []model.TraceResult{
		spanPayloadResult("small-1", 10),
		spanPayloadResult("huge", 8192),
		spanPayloadResult("small-2", 10),
		spanPayloadResult("small-3", 10),
	} {
		require.NoError(t, chunker.add(context.Background(), r))
	}
	require.NoError(t, chunker.finish(context.Background(), nil))
	require.Equal(t, [][]string{{"small-1"}, {"huge"}, {"small-2", "small-3"}}, decodeChunkTIDs(t, sink.chunks))
}

func TestTraceChunkerHoldsLastChunkForTheQueryTrace(t *testing.T) {
	var sent []*tracev1.InternalQueryResponse
	sink := sinkFunc(func(_ context.Context, m bus.Message) error {
		sent = append(sent, m.Data().(*tracev1.InternalQueryResponse))
		return nil
	})
	chunker := &traceChunker{
		sink: sink, maxRows: 2, maxBytes: 1 << 20, hold: true,
		encode: func(rows []model.TraceResult) (any, error) {
			resp := &tracev1.InternalQueryResponse{}
			for i := range rows {
				resp.InternalTraces = append(resp.InternalTraces, &tracev1.InternalTrace{TraceId: rows[i].TID})
			}
			return resp, nil
		},
	}
	for i := 0; i < 5; i++ {
		require.NoError(t, chunker.add(context.Background(), spanPayloadResult(fmt.Sprintf("t%d", i), 8)))
	}
	require.Len(t, sent, 1, "the newest full chunk is held back until the next one or finish")
	attached := 0
	require.NoError(t, chunker.finish(context.Background(), func(last any) {
		attached++
		last.(*tracev1.InternalQueryResponse).TraceQueryResult = nil
	}))
	require.Equal(t, 1, attached)
	require.Len(t, sent, 3)
	require.Len(t, sent[2].InternalTraces, 1, "the last chunk carries the remaining trace")

	// With no results the held chunk is made empty so the trace still has a carrier.
	sent = nil
	empty := &traceChunker{sink: sink, hold: true, encode: chunker.encode}
	require.NoError(t, empty.finish(context.Background(), func(any) { attached++ }))
	require.Len(t, sent, 1)
	require.Equal(t, 2, attached)
}

type traceIterFunc func() (model.TraceResult, bool)

func (f traceIterFunc) Next() (model.TraceResult, bool) { return f() }

type sinkFunc func(ctx context.Context, m bus.Message) error

func (f sinkFunc) Send(ctx context.Context, m bus.Message) error { return f(ctx, m) }

func TestScanTraceResultsStopsOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pulled := 0
	source := traceIterFunc(func() (model.TraceResult, bool) {
		pulled++
		return spanPayloadResult(fmt.Sprintf("t%d", pulled), 8), true
	})
	sink := &recordingSink{hook: func(n int) {
		if n == 2 {
			cancel()
		}
	}}
	_, err := scanTraceResults(ctx, source, nil, frameChunker(sink, 1, 1<<20))
	require.ErrorIs(t, err, context.Canceled)
	require.Len(t, sink.chunks, 2)
	require.LessOrEqual(t, pulled, 3, "the scan must stop right after the cancellation")
}

func TestScanTraceResultsPropagatesIteratorError(t *testing.T) {
	boom := fmt.Errorf("iterator failed")
	calls := 0
	source := traceIterFunc(func() (model.TraceResult, bool) {
		calls++
		if calls == 3 {
			return model.TraceResult{Error: boom}, false
		}
		return spanPayloadResult(fmt.Sprintf("t%d", calls), 8), true
	})
	_, err := scanTraceResults(context.Background(), source, nil, frameChunker(&recordingSink{}, 10, 1<<20))
	require.ErrorIs(t, err, boom)
}

func TestRevStreamRecoversPanicIntoError(t *testing.T) {
	// A nil trace service panics while the plan is set up.
	p := &traceQueryProcessor{queryService: &queryService{log: logger.GetLogger("test")}}
	req := &tracev1.QueryRequest{Name: "t", Groups: []string{"g"}}
	err := p.RevStream(context.Background(), bus.NewMessage(1, req), &recordingSink{})
	require.ErrorContains(t, err, "panicked")
}

func TestRevStreamRejectsWrongPayload(t *testing.T) {
	p := &traceQueryProcessor{queryService: &queryService{log: logger.GetLogger("test")}}
	err := p.RevStream(context.Background(), bus.NewMessage(1, "not a request"), &recordingSink{})
	require.Error(t, err)
}

func TestTraceRevStreamReportsResourceExhaustion(t *testing.T) {
	budget := protector.NewQueryBudget(&protector.Nop{})
	// A released reservation fails admission with ErrQueryResourceExhausted.
	ctx, release, admitErr := budget.AdmitContext(context.Background(), 10, 0, 20)
	require.NoError(t, admitErr)
	release()
	p := &traceQueryProcessor{queryService: &queryService{log: logger.GetLogger("test"), queryBudget: budget}}
	req := &tracev1.QueryRequest{Name: "t", Groups: []string{"g"}}

	err := p.RevStream(ctx, bus.NewMessage(1, req), &recordingSink{})
	var failure *common.Error
	require.ErrorAs(t, err, &failure)
	require.Equal(t, modelv1.Status_STATUS_RESOURCE_EXHAUSTED, failure.Status())

	err = p.RevStream(context.Background(), bus.NewMessage(1, "not a request"), &recordingSink{})
	require.EqualError(t, err, "invalid event data type")
	require.False(t, errors.As(err, &failure))
}

// failingTraceService serves one trace whose storage query fails with queryErr.
type failingTraceService struct {
	trace.Service
	queryErr error
}

func (s *failingTraceService) Trace(metadata *commonv1.Metadata) (trace.Trace, error) {
	return &failingTrace{metadata: metadata, queryErr: s.queryErr}, nil
}

type failingTrace struct {
	metadata *commonv1.Metadata
	queryErr error
}

func (f *failingTrace) GetSchema() *databasev1.Trace {
	return &databasev1.Trace{
		Metadata:         f.metadata,
		TraceIdTagName:   "trace_id",
		SpanIdTagName:    "span_id",
		TimestampTagName: "timestamp",
		Tags: []*databasev1.TraceTagSpec{
			{Name: "trace_id", Type: databasev1.TagType_TAG_TYPE_STRING},
			{Name: "span_id", Type: databasev1.TagType_TAG_TYPE_STRING},
			{Name: "timestamp", Type: databasev1.TagType_TAG_TYPE_TIMESTAMP},
		},
	}
}

func (f *failingTrace) GetIndexRules() []*databasev1.IndexRule { return nil }

func (f *failingTrace) Query(context.Context, model.TraceQueryOptions) (model.TraceQueryResult, error) {
	return nil, f.queryErr
}

// TestTraceRevStreamReportsExecutionResourceExhaustion pins that a query admitted and planned, whose storage
// execution runs out of memory, fails with RESOURCE_EXHAUSTED, while any other execution error keeps its identity.
func TestTraceRevStreamReportsExecutionResourceExhaustion(t *testing.T) {
	run := func(queryErr error) error {
		p := &traceQueryProcessor{traceService: &failingTraceService{queryErr: queryErr}, queryService: &queryService{log: logger.GetLogger("test")}}
		req := &tracev1.QueryRequest{Name: "t", Groups: []string{"g"}, TimeRange: &modelv1.TimeRange{
			Begin: timestamppb.New(time.Unix(0, 0)), End: timestamppb.New(time.Unix(3600, 0)),
		}}
		return p.RevStream(context.Background(), bus.NewMessage(1, req), &recordingSink{})
	}

	err := run(fmt.Errorf("load spans: %w", vectorized.ErrMemoryBudgetExceeded))
	var failure *common.Error
	require.ErrorAs(t, err, &failure)
	require.Equal(t, modelv1.Status_STATUS_RESOURCE_EXHAUSTED, failure.Status())

	diskErr := errors.New("disk read failed")
	err = run(diskErr)
	require.ErrorIs(t, err, diskErr)
	require.False(t, errors.As(err, &failure), "an ordinary execution error must not be turned into a status")
}
