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
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/index/native"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// payloadSink records the payloads of the chunks it receives.
type payloadSink struct {
	failAt   int
	payloads []any
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
