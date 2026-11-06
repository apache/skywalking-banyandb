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
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

package stream

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/data"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	vstream "github.com/apache/skywalking-banyandb/pkg/query/vectorized/stream"
	streamframe "github.com/apache/skywalking-banyandb/pkg/query/vectorized/stream/frame"
)

type analyzedDistributedContext struct {
	*fakeDistributedContext
	request *streamv1.QueryRequest
}

func (f *analyzedDistributedContext) Scatter(ctx context.Context, topic bus.Topic, message bus.Message,
	options bus.ScatterOptions,
) ([]bus.NodeStream, error) {
	f.request = message.Data().(*streamv1.QueryRequest)
	return f.fakeDistributedContext.Scatter(ctx, topic, message, options)
}

func analyzedDistributedPlan(t *testing.T, offset, limit uint32) (executor.StreamExecutable, *distributedPlan) {
	t.Helper()
	s, err := BuildSchema(&databasev1.Stream{}, nil)
	require.NoError(t, err)
	plan, err := DistributedAnalyze(&streamv1.QueryRequest{
		Name: "s", Projection: &modelv1.TagProjection{}, Offset: offset, Limit: limit,
	}, []logical.Schema{s})
	require.NoError(t, err)
	executable, ok := plan.(executor.StreamExecutable)
	require.True(t, ok)
	distributed, ok := plan.(*distributedPlan)
	require.True(t, ok, "the analyzed plan owns the client window")
	return executable, distributed
}

func analyzedNodeChunks(t *testing.T, rows []nodeRow, frames bool) []any {
	t.Helper()
	var chunks []any
	const rowsPerChunk = 512
	for begin := 0; begin < len(rows); begin += rowsPerChunk {
		end := min(begin+rowsPerChunk, len(rows))
		if frames {
			chunks = append(chunks, frameChunk(t, rows[begin:end]))
		} else {
			chunks = append(chunks, protoResponseForRows(rows[begin:end]))
		}
	}
	return chunks
}

func TestDistributedAnalyzedWindowAndNodeRequest(t *testing.T) {
	originalRaw := data.StreamWireModeRaw()
	data.SetStreamWireModeRaw(true)
	t.Cleanup(func() { data.SetStreamWireModeRaw(originalRaw) })
	for _, kind := range []string{"frames", "proto", "mixed"} {
		for _, tc := range []struct {
			name                 string
			offset, limit, count uint32
		}{
			{name: "default limit", offset: 3, count: 20},
			{name: "explicit limit", offset: 2, limit: 4, count: 4},
		} {
			t.Run(kind+"/"+tc.name, func(t *testing.T) {
				executable, distributed := analyzedDistributedPlan(t, tc.offset, tc.limit)
				one := make([]nodeRow, 0, 15)
				two := []nodeRow{{ts: 1, elementID: 1}} // Replica is first in node two's sorted stream.
				for id := uint64(1); id <= 30; id++ {
					row := nodeRow{ts: int64(id), elementID: id}
					if id%2 == 1 {
						one = append(one, row)
					} else {
						two = append(two, row)
					}
				}
				// Split each node across two chunks, independently of the larger helper's batch size.
				encode := func(rows []nodeRow, frames bool) []any {
					middle := len(rows) / 2
					return append(analyzedNodeChunks(t, rows[:middle], frames), analyzedNodeChunks(t, rows[middle:], frames)...)
				}
				frameOne, frameTwo := kind != "proto", kind == "frames"
				fake := &analyzedDistributedContext{fakeDistributedContext: &fakeDistributedContext{streams: []*fakeNodeStream{
					{name: "one", chunks: encode(one, frameOne)},
					{name: "two", chunks: encode(two, frameTwo)},
				}}}
				builtRows := 0
				distributed.buildElements = func(batch *vectorized.RecordBatch, projection []model.TagProjection) ([]*streamv1.Element, error) {
					builtRows += batch.ActiveLen()
					return vstream.BuildElementsFromBatch(batch, projection)
				}
				got, err := executable.Execute(executor.WithDistributedExecutionContext(context.Background(), fake))
				require.NoError(t, err)
				var want []string
				for id := tc.offset + 1; id <= tc.offset+tc.count; id++ {
					want = append(want, hexElementID(uint64(id)))
				}
				require.Equal(t, want, elementIDSeq(got))
				require.NotNil(t, fake.request)
				require.Equal(t, tc.offset+tc.count, fake.request.Limit, "nodes receive the analyzed limit+offset window")
				require.Zero(t, fake.request.Offset)
				switch kind {
				case "frames":
					require.EqualValues(t, tc.count, builtRows, "only the final window may become Elements")
				case "mixed":
					require.Equal(t, len(one), builtRows, "the frame fallback uses the same builder")
				default:
					require.Zero(t, builtRows)
				}
			})
		}
	}
}

func TestDistributedAnalyzedMaterializationSpansOutputBatches(t *testing.T) {
	originalRaw := data.StreamWireModeRaw()
	data.SetStreamWireModeRaw(true)
	t.Cleanup(func() { data.SetStreamWireModeRaw(originalRaw) })
	const offset = uint32(7)
	const limit = uint32(vectorized.DefaultBatchSize + 37)
	executable, distributed := analyzedDistributedPlan(t, offset, limit)
	var one, two []nodeRow
	two = append(two, nodeRow{ts: 1, elementID: 1})
	for id := uint64(1); id <= uint64(offset+limit+100); id++ {
		row := nodeRow{ts: int64(id), elementID: id}
		if id%2 == 1 {
			one = append(one, row)
		} else {
			two = append(two, row)
		}
	}
	fake := &analyzedDistributedContext{fakeDistributedContext: &fakeDistributedContext{streams: []*fakeNodeStream{
		{name: "one", chunks: analyzedNodeChunks(t, one, true)},
		{name: "two", chunks: analyzedNodeChunks(t, two, true)},
	}}}
	builtRows, calls := 0, 0
	distributed.buildElements = func(batch *vectorized.RecordBatch, projection []model.TagProjection) ([]*streamv1.Element, error) {
		calls++
		builtRows += batch.ActiveLen()
		return vstream.BuildElementsFromBatch(batch, projection)
	}
	got, err := executable.Execute(executor.WithDistributedExecutionContext(context.Background(), fake))
	require.NoError(t, err)
	require.Len(t, got, int(limit))
	for idx, element := range got {
		require.Equal(t, hexElementID(uint64(offset)+uint64(idx)+1), element.ElementId)
	}
	require.EqualValues(t, limit, builtRows)
	require.GreaterOrEqual(t, calls, 2, "the proof allows one builder call per output batch")
	require.Equal(t, offset+limit, fake.request.Limit)
}

func TestDistributedAnalyzedMaterializerHonorsSelection(t *testing.T) {
	originalRaw := data.StreamWireModeRaw()
	data.SetStreamWireModeRaw(true)
	t.Cleanup(func() { data.SetStreamWireModeRaw(originalRaw) })
	executable, distributed := analyzedDistributedPlan(t, 1, 1)
	distributed.decodeFrame = func(payload []byte) (*vectorized.RecordBatch, error) {
		batch, err := streamframe.Decode(payload)
		if err == nil {
			batch.Selection = []uint16{1, 3}
		}
		return batch, err
	}
	calls, builtRows := 0, 0
	sawNarrowedSelection := false
	distributed.buildElements = func(batch *vectorized.RecordBatch, projection []model.TagProjection) ([]*streamv1.Element, error) {
		calls++
		builtRows += batch.ActiveLen()
		sawNarrowedSelection = sawNarrowedSelection || batch.ActiveLen() < batch.Len
		return vstream.BuildElementsFromBatch(batch, projection)
	}
	fake := &fakeDistributedContext{streams: []*fakeNodeStream{
		{name: "one", chunks: []any{frameChunk(t, []nodeRow{
			{ts: 1, elementID: 1}, {ts: 2, elementID: 2}, {ts: 3, elementID: 3}, {ts: 4, elementID: 4},
		})}},
	}}
	got, err := executable.Execute(executor.WithDistributedExecutionContext(context.Background(), fake))
	require.NoError(t, err)
	require.Equal(t, []string{hexElementID(4)}, elementIDSeq(got))
	require.Positive(t, calls)
	require.Equal(t, 1, builtRows)
	require.True(t, sawNarrowedSelection, "Len includes rows the window does not materialize")
}

func TestDistributedAnalyzedResponseCountIncludesEmptyDecodedBodies(t *testing.T) {
	originalRaw := data.StreamWireModeRaw()
	data.SetStreamWireModeRaw(true)
	t.Cleanup(func() { data.SetStreamWireModeRaw(originalRaw) })
	executable, _ := analyzedDistributedPlan(t, 0, 2)
	tracer, ctx := query.NewTracer(context.Background(), "root")
	fake := &analyzedDistributedContext{fakeDistributedContext: &fakeDistributedContext{streams: []*fakeNodeStream{
		{name: "frame", chunks: []any{frameChunk(t, []nodeRow{{ts: 1, elementID: 1}})}},
		{name: "empty-frame", chunks: []any{frameChunk(t, nil)}},
		{name: "empty-proto", chunks: []any{&streamv1.QueryResponse{}}},
		{name: "nil-body", chunks: []any{nil}},
		{name: "proto", chunks: []any{protoResponseForRows([]nodeRow{{ts: 2, elementID: 2}})}},
	}}}
	got, err := executable.Execute(executor.WithDistributedExecutionContext(ctx, fake))
	require.NoError(t, err)
	require.Equal(t, []string{hexElementID(1), hexElementID(2)}, elementIDSeq(got))
	tags := distributedSpanTags(tracer.ToProto().Spans)
	require.Equal(t, "4", tags["response_count"], fmt.Sprintf("tags: %v", tags))
	require.Equal(t, "2", tags["element_id_count"])
	require.True(t, fake.request.Trace)
}
