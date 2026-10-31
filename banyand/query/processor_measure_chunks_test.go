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

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/measure"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/index/native"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// payloadSink records the payloads of the chunks it receives.
type payloadSink struct {
	payloads []any
	failAt   int
}

func (s *payloadSink) Send(ctx context.Context, chunk bus.Message) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.payloads = append(s.payloads, chunk.Data())
	if s.failAt > 0 && len(s.payloads) >= s.failAt {
		return errors.New("client is gone")
	}
	return nil
}

func TestMeasureChunkerSendsEveryFrameAsItIsProduced(t *testing.T) {
	sink := &payloadSink{}
	chunker := &measureChunker{sink: sink, id: 1}
	for i := range 3 {
		require.NoError(t, chunker.add(t.Context(), []byte{0x00, byte(i)}))
		require.Len(t, sink.payloads, i+1, "an untraced frame is sent at once")
	}
	require.NoError(t, chunker.add(t.Context(), nil), "an empty frame is skipped")
	require.NoError(t, chunker.finish(t.Context(), nil))
	require.Len(t, sink.payloads, 3)
	require.Equal(t, 3, chunker.frames)
}

func TestMeasureChunkerCarriesTheTraceOnTheLastFrame(t *testing.T) {
	sink := &payloadSink{}
	chunker := &measureChunker{sink: sink, id: 1, hold: true}
	require.NoError(t, chunker.add(t.Context(), []byte{0x00, 1}))
	require.Empty(t, sink.payloads, "a traced query keeps the last frame back")
	require.NoError(t, chunker.add(t.Context(), []byte{0x00, 2}))
	require.Len(t, sink.payloads, 1)
	require.NoError(t, chunker.finish(t.Context(), func() *measurev1.InternalQueryResponse {
		return &measurev1.InternalQueryResponse{Trace: &commonv1.Trace{TraceId: "t"}}
	}))
	require.Len(t, sink.payloads, 2)
	require.Equal(t, []byte{0x00, 1}, sink.payloads[0])
	last, ok := sink.payloads[1].(*measurev1.InternalQueryResponse)
	require.True(t, ok)
	require.Equal(t, []byte{0x00, 2}, last.GetRawFrameBody())
	require.Equal(t, "t", last.GetTrace().GetTraceId())
}

func TestMeasureChunkerSendsTheTraceEvenWithoutAFrame(t *testing.T) {
	sink := &payloadSink{}
	chunker := &measureChunker{sink: sink, id: 1, hold: true}
	require.NoError(t, chunker.finish(t.Context(), func() *measurev1.InternalQueryResponse {
		return &measurev1.InternalQueryResponse{Trace: &commonv1.Trace{TraceId: "t"}}
	}))
	require.Len(t, sink.payloads, 1)
	last := sink.payloads[0].(*measurev1.InternalQueryResponse)
	require.Empty(t, last.GetRawFrameBody())
	require.NotNil(t, last.GetTrace())
}

func TestMeasureChunkerStopsWhenTheRequesterIsGone(t *testing.T) {
	sink := &payloadSink{failAt: 1}
	chunker := &measureChunker{sink: sink, id: 1}
	require.Error(t, chunker.add(t.Context(), []byte{0x00}))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, (&measureChunker{sink: &payloadSink{}}).add(ctx, []byte{0x00}), context.Canceled)
}

func TestMeasureStreamFailureKeepsTheResourceExhaustedStatus(t *testing.T) {
	for name, cause := range map[string]error{
		"lease":  protector.ErrQueryResourceExhausted,
		"budget": vectorized.ErrMemoryBudgetExceeded,
		"limit":  native.ErrQueryLimit,
	} {
		t.Run(name, func(t *testing.T) {
			var failure *common.Error
			require.ErrorAs(t, measureStreamFailure(fmt.Errorf("scan: %w", cause)), &failure)
			require.Equal(t, modelv1.Status_STATUS_RESOURCE_EXHAUSTED, failure.Status())
		})
	}
	plain := errors.New("plain")
	require.Same(t, plain, measureStreamFailure(plain))
	require.ErrorIs(t, measureStreamFailure(context.Canceled), context.Canceled)
}

func TestMeasureRevStreamRejectsAnInvalidRequest(t *testing.T) {
	p := &measureInternalQueryProcessor{queryService: &queryService{log: logger.GetLogger("test")}}
	require.Error(t, p.RevStream(t.Context(), bus.NewMessage(1, "not a request"), &payloadSink{}))
	require.ErrorContains(t, p.RevStream(t.Context(), bus.NewMessage(1, &measurev1.InternalQueryRequest{}), &payloadSink{}), "nil")
}

func TestMeasureRevStreamReportsAnExhaustedQueryPoolAsResourceExhausted(t *testing.T) {
	// A pool smaller than the fixed admission overhead admits nothing.
	budget := protector.NewQueryBudget(&tinyPoolMemory{})
	p := &measureInternalQueryProcessor{queryService: &queryService{log: logger.GetLogger("test"), queryBudget: budget}}
	req := &measurev1.InternalQueryRequest{Request: &measurev1.QueryRequest{Name: "m", Groups: []string{"g"}}}
	err := p.RevStream(t.Context(), bus.NewMessage(1, req), &payloadSink{})
	var failure *common.Error
	require.ErrorAs(t, err, &failure)
	require.Equal(t, modelv1.Status_STATUS_RESOURCE_EXHAUSTED, failure.Status())
}

// tinyPoolMemory is a protector whose limit leaves a query less than the fixed admission overhead.
type tinyPoolMemory struct{ protector.Memory }

func (*tinyPoolMemory) GetLimit() uint64      { return 4 << 20 }
func (*tinyPoolMemory) AvailableBytes() int64 { return 4 << 20 }

func findSpan(spans []*commonv1.Span, name string) *commonv1.Span {
	for _, span := range spans {
		if span.GetMessage() == name {
			return span
		}
		if child := findSpan(span.GetChildren(), name); child != nil {
			return child
		}
	}
	return nil
}

func spanTag(span *commonv1.Span, key string) string {
	for _, tag := range span.GetTags() {
		if tag.GetKey() == key {
			return tag.GetValue()
		}
	}
	return ""
}

// TestMeasureStreamTraceEndsTheOperatorSpansBeforeItIsSent pins that the data node closes its pipeline before it
// builds the trace of the last chunk: closing is what ends the operator spans and records their tags.
func TestMeasureStreamTraceEndsTheOperatorSpansBeforeItIsSent(t *testing.T) {
	previous := data.MeasureWireModeRaw()
	data.SetMeasureWireModeRaw(true)
	t.Cleanup(func() { data.SetMeasureWireModeRaw(previous) })
	group, _ := mgOracleGroups()
	mctx := mgContext(t, group)
	mctx.ml = logger.GetLogger("test")
	req := mgRequest(modelv1.Sort_SORT_ASC, "", 0, 1)
	req.Groups = []string{mgGroupA}
	req.TagProjection.TagFamilies[0].Tags = []string{mgTagSvc}
	req.Trace = true
	p := &measureInternalQueryProcessor{queryService: &queryService{nodeID: "node-1", log: logger.GetLogger("test")}}
	sink := &payloadSink{}
	err := p.streamMeasure(t.Context(), 1, &measurev1.InternalQueryRequest{Request: req}, nil, mctx, sink, time.Now())
	require.NoError(t, err)
	require.NotEmpty(t, sink.payloads)
	last, ok := sink.payloads[len(sink.payloads)-1].(*measurev1.InternalQueryResponse)
	require.True(t, ok, "a traced query ends with the response that carries the trace")
	require.NotEmpty(t, last.GetRawFrameBody())
	root := findSpan(last.GetTrace().GetSpans(), "data-node-1")
	require.NotNil(t, root)
	require.NotNil(t, root.GetEndTime())
	for _, name := range []string{"scan", "limit"} {
		span := findSpan(root.GetChildren(), name)
		require.NotNil(t, span, "operator span %s", name)
		require.NotNil(t, span.GetEndTime(), "operator span %s must be ended before the trace is sent", name)
	}
	require.Equal(t, "1", spanTag(findSpan(root.GetChildren(), "limit"), "rows_out"))
}

// mgService serves one fake group to the data-node processor.
type mgService struct {
	measure.Service
	group *mgGroup
}

func (s *mgService) Measure(*commonv1.Metadata) (measure.Measure, error) {
	return mgMeasureOf{s.group}, nil
}

type mgMeasureOf struct{ *mgGroup }

func (mgMeasureOf) GetIndexRules() []*databasev1.IndexRule { return nil }

// bigPoolMemory is a protector with ample memory for the query pool.
type bigPoolMemory struct{ protector.Memory }

func (*bigPoolMemory) GetLimit() uint64      { return 8 << 30 }
func (*bigPoolMemory) AvailableBytes() int64 { return 8 << 30 }

// TestMeasureRevStreamReturnsEverythingItAdmittedAndCharged runs the data-node scan many times over every way a query
// can end and checks that the query pool is back to zero after each: a leak would drain the pool until a small query
// is refused.
func TestMeasureRevStreamReturnsEverythingItAdmittedAndCharged(t *testing.T) {
	previous := data.MeasureWireModeRaw()
	data.SetMeasureWireModeRaw(true)
	t.Cleanup(func() { data.SetMeasureWireModeRaw(previous) })
	rows := func(n int) [][]mgRow {
		batches := make([][]mgRow, 0, n)
		for i := range n {
			batches = append(batches, []mgRow{{
				ts: int64(10 + i), sid: uint64(i + 1), version: 1, value: int64(i),
				tags: map[string]string{mgTagSvc: fmt.Sprintf("s%d", i), mgTagRegion: "r"},
			}})
		}
		return batches
	}
	type scenario struct {
		cancelAfter int
		pullErrAt   int
		batches     int
		runs        int
		limit       uint32
		wantErr     bool
	}
	scenarios := map[string]scenario{
		"completed":    {batches: 20, limit: 100000, runs: 200},
		"limit":        {batches: 20, limit: 1, runs: 50},
		"empty":        {batches: 0, limit: 100000, runs: 50},
		"cancel":       {batches: 20, limit: 100000, cancelAfter: 2, wantErr: true, runs: 50},
		"storage fail": {batches: 20, limit: 100000, pullErrAt: 3, wantErr: true, runs: 50},
	}
	for name, sc := range scenarios {
		t.Run(name, func(t *testing.T) {
			budget := protector.NewQueryBudget(&bigPoolMemory{})
			p := &measureInternalQueryProcessor{
				measureService: &mgService{},
				queryService:   &queryService{nodeID: "node-1", log: logger.GetLogger("test"), queryBudget: budget},
			}
			for run := range sc.runs {
				group := &mgGroup{schema: mgMeasure(mgGroupA, mgTagSvc, mgTagRegion), batches: rows(sc.batches), pullErrAt: sc.pullErrAt}
				p.measureService = &mgService{group: group}
				req := mgRequest(modelv1.Sort_SORT_ASC, "", 0, sc.limit)
				req.Groups = []string{mgGroupA}
				req.TagProjection.TagFamilies[0].Tags = []string{mgTagSvc}
				req.Criteria = nil
				ctx, cancel := context.WithCancel(t.Context())
				sink := &cancelSink{cancel: cancel, after: sc.cancelAfter}
				err := p.RevStream(ctx, bus.NewMessage(1, &measurev1.InternalQueryRequest{Request: req}), sink)
				cancel()
				if sc.wantErr {
					require.Error(t, err, "run %d", run)
				} else {
					require.NoError(t, err, "run %d", run)
				}
				require.Zero(t, budget.Reserved(), "run %d left query pool reservations behind", run)
			}
		})
	}
}

// cancelSink cancels the query after a number of chunks.
type cancelSink struct {
	cancel context.CancelFunc
	after  int
	seen   int
}

func (s *cancelSink) Send(ctx context.Context, _ bus.Message) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.seen++
	if s.after > 0 && s.seen >= s.after {
		s.cancel()
	}
	return nil
}
