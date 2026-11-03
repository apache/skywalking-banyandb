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

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	vstream "github.com/apache/skywalking-banyandb/pkg/query/vectorized/stream"
	streamframe "github.com/apache/skywalking-banyandb/pkg/query/vectorized/stream/frame"
)

// chunkTestSchema is a minimal dense schema: timestamp, element ID, series ID and one tag column.
func chunkTestSchema() *vectorized.BatchSchema {
	return vstream.BuildStreamBatchSchema([]model.TagProjection{{Family: "f", Names: []string{"svc"}}}, "", "")
}

// chunkTestBatch builds a dense batch whose row i carries ID first+i and a tag of payload bytes.
func chunkTestBatch(first, rows, payload int) *vectorized.RecordBatch {
	batch := vectorized.NewRecordBatch(chunkTestSchema(), rows)
	for i := range rows {
		batch.Columns[0].(*vectorized.TypedColumn[int64]).Append(int64(first + i))
		batch.Columns[1].(*vectorized.TypedColumn[int64]).Append(int64(first + i))
		batch.Columns[2].(*vectorized.TypedColumn[int64]).Append(1)
		batch.Columns[3].(*vectorized.TypedColumn[*modelv1.TagValue]).Append(
			&modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: string(make([]byte, payload))}}})
		batch.Len++
	}
	return batch
}

func newFrameChunker(sink bus.Sink, maxRows int, maxBytes uint64) *streamChunker {
	return &streamChunker{
		sink: sink, maxRows: maxRows, maxBytes: maxBytes,
		encode: func(batch *vectorized.RecordBatch) (any, error) { return streamframe.Encode(batch) },
	}
}

func decodeChunkRows(t *testing.T, chunks [][]byte) []int {
	t.Helper()
	out := make([]int, 0, len(chunks))
	for _, c := range chunks {
		batch, err := streamframe.Decode(c)
		require.NoError(t, err)
		out = append(out, batch.ActiveLen())
	}
	return out
}

func TestStreamChunkerRowCap(t *testing.T) {
	sink := &recordingSink{}
	chunker := newFrameChunker(sink, 3, 1<<20)
	// Two batches whose rows must be re-cut across the batch boundary.
	require.NoError(t, chunker.add(context.Background(), chunkTestBatch(0, 5, 8), 0, 5))
	require.NoError(t, chunker.add(context.Background(), chunkTestBatch(5, 4, 8), 0, 4))
	require.NoError(t, chunker.finish(context.Background(), nil))
	require.Equal(t, []int{3, 3, 3}, decodeChunkRows(t, sink.chunks))
}

func TestStreamChunkerKeepsRowOrderAcrossChunks(t *testing.T) {
	sink := &recordingSink{}
	chunker := newFrameChunker(sink, 4, 1<<20)
	require.NoError(t, chunker.add(context.Background(), chunkTestBatch(0, 10, 8), 0, 10))
	require.NoError(t, chunker.finish(context.Background(), nil))
	var ids []int64
	for _, c := range sink.chunks {
		batch, err := streamframe.Decode(c)
		require.NoError(t, err)
		ids = append(ids, batch.Columns[0].(*vectorized.TypedColumn[int64]).Data()...)
	}
	require.Equal(t, []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, ids)
}

func TestStreamChunkerByteCap(t *testing.T) {
	sink := &recordingSink{}
	batch := chunkTestBatch(0, 5, 100)
	rowSize := estimateRowBytes(batch, 0)
	// Room for two rows but not three.
	chunker := newFrameChunker(sink, 1000, rowSize*2+rowSize/2)
	require.NoError(t, chunker.add(context.Background(), batch, 0, 5))
	require.NoError(t, chunker.finish(context.Background(), nil))
	require.Equal(t, []int{2, 2, 1}, decodeChunkRows(t, sink.chunks))
}

func TestStreamChunkerOversizedRowGoesAlone(t *testing.T) {
	sink := &recordingSink{}
	chunker := newFrameChunker(sink, 1000, 1024)
	require.NoError(t, chunker.add(context.Background(), chunkTestBatch(0, 1, 10), 0, 1))
	require.NoError(t, chunker.add(context.Background(), chunkTestBatch(1, 1, 8192), 0, 1))
	require.NoError(t, chunker.add(context.Background(), chunkTestBatch(2, 2, 10), 0, 2))
	require.NoError(t, chunker.finish(context.Background(), nil))
	require.Equal(t, []int{1, 1, 2}, decodeChunkRows(t, sink.chunks))
}

func TestStreamChunkerHoldsLastChunkForTheQueryTrace(t *testing.T) {
	var sent []*streamv1.QueryResponse
	sink := sinkFunc(func(_ context.Context, m bus.Message) error {
		sent = append(sent, m.Data().(*streamv1.QueryResponse))
		return nil
	})
	chunker := &streamChunker{sink: sink, maxRows: 2, maxBytes: 1 << 20, hold: true, encode: encodeStreamElements}
	require.NoError(t, chunker.add(context.Background(), chunkTestBatch(0, 5, 8), 0, 5))
	require.Len(t, sent, 1, "the newest full chunk is held back until the next one or finish")
	attached := 0
	require.NoError(t, chunker.finish(context.Background(), func(last *streamv1.QueryResponse) {
		attached++
		last.Trace = &commonv1.Trace{TraceId: "t"}
	}))
	require.Equal(t, 1, attached)
	require.Len(t, sent, 3)
	require.Len(t, sent[2].Elements, 1, "the last chunk carries the remaining element")
	require.NotNil(t, sent[2].Trace)
	require.Nil(t, sent[0].Trace)

	// With no rows the held chunk is made empty so the trace still has a carrier.
	sent = nil
	empty := &streamChunker{sink: sink, hold: true, encode: encodeStreamElements}
	require.NoError(t, empty.finish(context.Background(), func(*streamv1.QueryResponse) { attached++ }))
	require.Len(t, sent, 1)
	require.Equal(t, 2, attached)
}

type sliceBatchIterator struct {
	batches []*vectorized.RecordBatch
	pulled  int
	closed  bool
}

func (s *sliceBatchIterator) NextBatch(context.Context) (*vectorized.RecordBatch, error) {
	if s.pulled >= len(s.batches) {
		return nil, nil
	}
	s.pulled++
	return s.batches[s.pulled-1], nil
}

func (s *sliceBatchIterator) Close() error { s.closed = true; return nil }

func identityStage(batch *vectorized.RecordBatch) (*vectorized.RecordBatch, error) { return batch, nil }

func TestPumpStreamBatchesStopsOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var batches []*vectorized.RecordBatch
	for i := range 10 {
		batches = append(batches, chunkTestBatch(i, 1, 8))
	}
	source := &sliceBatchIterator{batches: batches}
	sink := &recordingSink{hook: func(n int) {
		if n == 2 {
			cancel()
		}
	}}
	_, err := pumpStreamBatches(ctx, source, identityStage, nil, newFrameChunker(sink, 1, 1<<20))
	require.ErrorIs(t, err, context.Canceled)
	require.Len(t, sink.chunks, 2)
	require.LessOrEqual(t, source.pulled, 3, "the pump must stop right after the cancellation")
}

func TestPumpStreamBatchesPropagatesErrors(t *testing.T) {
	boom := errors.New("stage failed")
	source := &sliceBatchIterator{batches: []*vectorized.RecordBatch{chunkTestBatch(0, 2, 8), chunkTestBatch(2, 2, 8)}}
	calls := 0
	stage := func(batch *vectorized.RecordBatch) (*vectorized.RecordBatch, error) {
		calls++
		if calls == 2 {
			return nil, boom
		}
		return batch, nil
	}
	_, err := pumpStreamBatches(context.Background(), source, stage, nil, newFrameChunker(&recordingSink{}, 10, 1<<20))
	require.ErrorIs(t, err, boom)
}

func TestPumpStreamBatchesAppliesTheWindowAndStopsEarly(t *testing.T) {
	var batches []*vectorized.RecordBatch
	for i := range 10 {
		batches = append(batches, chunkTestBatch(i*3, 3, 8))
	}
	source := &sliceBatchIterator{batches: batches}
	var got []*streamv1.QueryResponse
	sink := sinkFunc(func(_ context.Context, m bus.Message) error {
		got = append(got, m.Data().(*streamv1.QueryResponse))
		return nil
	})
	chunker := &streamChunker{sink: sink, maxRows: 4, maxBytes: 1 << 20, encode: encodeStreamElements}
	count, err := pumpStreamBatches(context.Background(), source, identityStage, &rowWindow{skip: 4, remaining: 7}, chunker)
	require.NoError(t, err)
	require.NoError(t, chunker.finish(context.Background(), nil))
	require.Equal(t, 7, count)
	var rows int
	for _, g := range got {
		rows += len(g.Elements)
	}
	require.Equal(t, 7, rows)
	require.Less(t, source.pulled, 10, "the scan must stop once the window is full")
}

func TestSplitElementChunks(t *testing.T) {
	elements := make([]*streamv1.Element, 5)
	for i := range elements {
		elements[i] = &streamv1.Element{ElementId: fmt.Sprintf("e%d", i)}
	}
	chunks := splitElementChunks(elements, 2, 1<<20)
	require.Len(t, chunks, 3)
	require.Len(t, chunks[2].Elements, 1)
	require.Len(t, splitElementChunks(nil, 2, 1<<20), 1, "an empty result still yields one chunk")
}

func TestStreamQueryRevStreamRecoversPanicIntoError(t *testing.T) {
	// A nil stream service panics while the plan is built.
	p := &streamQueryProcessor{queryService: &queryService{log: logger.GetLogger("test")}}
	req := &streamv1.QueryRequest{Name: "s", Groups: []string{"g"}}
	err := p.RevStream(context.Background(), bus.NewMessage(1, req), &recordingSink{})
	require.ErrorContains(t, err, "panicked")
}

func TestStreamQueryRevStreamRejectsWrongPayload(t *testing.T) {
	p := &streamQueryProcessor{queryService: &queryService{log: logger.GetLogger("test")}}
	require.Error(t, p.RevStream(context.Background(), bus.NewMessage(1, "not a request"), &recordingSink{}))
}

func TestStreamFailureKeepsTheResourceExhaustedStatus(t *testing.T) {
	exhausted := streamFailure(fmt.Errorf("scan: %w", protector.ErrQueryResourceExhausted))
	var failure *common.Error
	require.ErrorAs(t, exhausted, &failure)
	require.Equal(t, modelv1.Status_STATUS_RESOURCE_EXHAUSTED, failure.Status())

	plain := errors.New("plain")
	require.Same(t, plain, streamFailure(plain), "other errors keep their identity")
	require.ErrorIs(t, streamFailure(context.Canceled), context.Canceled)
}

func TestStreamPlanChunksMatchesTheUnaryFrame(t *testing.T) {
	for _, maxRows := range []int{1, 2, 4096} {
		t.Run(fmt.Sprintf("rows=%d", maxRows), func(t *testing.T) {
			p := newOracleProcessor(t)
			req := oracleRequest([]string{oracleClientTag}, true, "")
			req.Limit = oracleCapLimit
			unary := newOracleCapPlan(t, req)
			handled, resp := p.tryStreamVecDispatch(context.Background(), unary, req, false)
			require.True(t, handled)
			unaryBatch, err := streamframe.Decode(resp.Data().([]byte))
			require.NoError(t, err)
			want := oracleFrameServices(t, unaryBatch)
			require.Equal(t, []string{"cap-1", "cap-3"}, want)

			plan := newOracleCapPlan(t, req)
			sink := &recordingSink{}
			chunker := newFrameChunker(sink, maxRows, 1<<20)
			count, runErr := p.streamPlanChunks(context.Background(), plan, req, chunker)
			require.NoError(t, runErr)
			require.NoError(t, chunker.finish(context.Background(), nil))
			require.Equal(t, len(want), count)
			var got []string
			for _, c := range sink.chunks {
				batch, decErr := streamframe.Decode(c)
				require.NoError(t, decErr)
				got = append(got, oracleFrameServices(t, batch)...)
			}
			require.Equal(t, want, got)
			wantChunks := (len(want) + maxRows - 1) / maxRows
			require.Len(t, sink.chunks, wantChunks)
		})
	}
}

func TestStreamPlanChunksIndexOrderAcrossBatches(t *testing.T) {
	p := newOracleProcessor(t)
	req := oracleRequest([]string{oracleClientTag}, true, oracleIndexRule)
	plan, _ := newOraclePlanWithCorpus(t, req, []string{oracleClientTag, oracleCriteriaTag, oracleOrderTag}, oracleOrderTag, 1)
	sink := &recordingSink{}
	chunker := newFrameChunker(sink, 1, 1<<20)
	_, err := p.streamPlanChunks(context.Background(), plan, req, chunker)
	require.NoError(t, err)
	require.NoError(t, chunker.finish(context.Background(), nil))
	var got []string
	for _, c := range sink.chunks {
		batch, decErr := streamframe.Decode(c)
		require.NoError(t, decErr)
		got = append(got, oracleFrameServices(t, batch)...)
	}
	require.Equal(t, []string{"new", "other"}, got)
	require.Len(t, sink.chunks, 2)
}

func TestStreamPlanChunksTracedAndStandaloneStayProto(t *testing.T) {
	for name, p := range map[string]*streamQueryProcessor{
		"traced":     newOracleProcessor(t),
		"standalone": newOracleStandaloneProcessor(t),
	} {
		t.Run(name, func(t *testing.T) {
			req := oracleRequest([]string{oracleClientTag}, true, oracleIndexRule)
			plan := newOraclePlan(t, req, []string{oracleClientTag, oracleCriteriaTag, oracleOrderTag})
			var sent []*streamv1.QueryResponse
			sink := sinkFunc(func(_ context.Context, m bus.Message) error {
				resp, ok := m.Data().(*streamv1.QueryResponse)
				require.True(t, ok, "%s emitted %T, not a proto response", name, m.Data())
				sent = append(sent, resp)
				return nil
			})
			chunker := &streamChunker{sink: sink, maxRows: 1, maxBytes: 1 << 20, hold: name == "traced"}
			_, err := p.streamPlanChunks(context.Background(), plan, req, chunker)
			require.NoError(t, err)
			require.NoError(t, chunker.finish(context.Background(), func(last *streamv1.QueryResponse) {
				last.Trace = &commonv1.Trace{TraceId: "data"}
			}))
			var elements []*streamv1.Element
			for _, s := range sent {
				elements = append(elements, s.Elements...)
			}
			require.Equal(t, []string{"new", "other"}, oracleProtoServices(t, elements))
			require.Len(t, sent, 2)
		})
	}
}
