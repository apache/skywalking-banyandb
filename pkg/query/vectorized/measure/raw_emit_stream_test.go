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

package measure

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure/frame"
)

func streamPipeline(t *testing.T, schema *vectorized.BatchSchema, tracker *vectorized.MemoryTracker, batches ...*vectorized.RecordBatch) *vectorized.Pipeline {
	t.Helper()
	builder := vectorized.NewPipelineBuilder().From(&batchesSource{schema: schema, batches: batches})
	if tracker != nil {
		builder = builder.WithMemoryTracker(tracker)
	}
	pipeline, err := builder.Build()
	require.NoError(t, err)
	require.NoError(t, pipeline.Init(t.Context()))
	t.Cleanup(func() { _ = pipeline.Close() })
	return pipeline
}

func collectFrames(t *testing.T, pipeline *vectorized.Pipeline, schema *vectorized.BatchSchema, limits FrameLimits) []*vectorized.RecordBatch {
	t.Helper()
	var decoded []*vectorized.RecordBatch
	err := streamPipelineFrames(t.Context(), pipeline, schema, limits, func(body []byte) error {
		batch, decodeErr := frame.Decode(body)
		require.NoError(t, decodeErr)
		decoded = append(decoded, batch)
		return nil
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		for _, batch := range decoded {
			batch.ReleaseOwners()
		}
	})
	return decoded
}

func rowCount(batches []*vectorized.RecordBatch) int {
	var rows int
	for _, batch := range batches {
		rows += batch.ActiveLen()
	}
	return rows
}

func TestStreamPipelineFramesSendsOneFramePerBatchAndReleasesEach(t *testing.T) {
	schema := drainSchema()
	owner := &pinOwner{}
	tracker := vectorized.NewMemoryTracker(64 << 20)
	pipeline := streamPipeline(t, schema, tracker,
		drainBatch(schema, 100, 10, owner), drainBatch(schema, 200, 7, owner), drainBatch(schema, 300, 3, owner))
	frames := collectFrames(t, pipeline, schema, FrameLimits{})
	require.Len(t, frames, 3, "a batch is one frame while the caps allow it")
	require.Equal(t, []int{10, 7, 3}, []int{frames[0].ActiveLen(), frames[1].ActiveLen(), frames[2].ActiveLen()})
	require.Zero(t, owner.refs, "storage borrowed by a batch is released once its frame is sent")
	require.Zero(t, tracker.Used(), "the frame reservations are returned")
}

func TestStreamPipelineFramesSplitsABatchByRowAndByteCaps(t *testing.T) {
	schema := drainSchema()
	for name, limits := range map[string]FrameLimits{
		"rows":  {MaxRows: 3},
		"bytes": {MaxBytes: 1},
		"both":  {MaxRows: 4, MaxBytes: 256},
	} {
		t.Run(name, func(t *testing.T) {
			pipeline := streamPipeline(t, schema, vectorized.NewMemoryTracker(64<<20), drainBatch(schema, 100, 10, nil))
			frames := collectFrames(t, pipeline, schema, limits)
			require.Equal(t, 10, rowCount(frames), "no row is lost or repeated")
			require.Greater(t, len(frames), 1, "the caps must split the batch")
			var ts []int64
			for _, batch := range frames {
				if limits.MaxRows > 0 {
					require.LessOrEqual(t, batch.ActiveLen(), limits.MaxRows)
				}
				col := batch.Columns[batch.Schema.TimestampIndex()].(*vectorized.TypedColumn[int64])
				ts = append(ts, col.Data()...)
			}
			for i := range ts {
				require.Equal(t, int64(100+i), ts[i], "frames keep the order of the batch")
			}
		})
	}
	pipeline := streamPipeline(t, schema, nil, drainBatch(schema, 100, 5, nil))
	require.Len(t, collectFrames(t, pipeline, schema, FrameLimits{MaxBytes: 1}), 5, "a row over the byte cap goes alone")
}

func TestStreamPipelineFramesKeepsOneLayoutWhenAColumnIsAllNullInOneBatch(t *testing.T) {
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleTimestamp, Name: "_timestamp", Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "svc", Type: vectorized.ColumnTypeTagValue},
	})
	build := func(ts int64, values ...*modelv1.TagValue) *vectorized.RecordBatch {
		batch := vectorized.NewRecordBatch(schema, len(values))
		for i, value := range values {
			batch.Columns[0].(*vectorized.TypedColumn[int64]).Append(ts + int64(i))
			batch.Columns[1].(*vectorized.TypedColumn[*modelv1.TagValue]).Append(value)
			batch.Len++
		}
		return batch
	}
	str := func(v string) *modelv1.TagValue {
		return &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: v}}}
	}
	null := &modelv1.TagValue{Value: &modelv1.TagValue_Null{}}
	pipeline := streamPipeline(t, schema, vectorized.NewMemoryTracker(64<<20), build(1, null, null), build(10, str("a"), str("b")), build(20, null, str("c")))
	frames := collectFrames(t, pipeline, schema, FrameLimits{})
	require.Len(t, frames, 3)
	for _, batch := range frames {
		require.Equal(t, vectorized.ColumnTypeTagValue, batch.Schema.Columns[1].Type,
			"a stream of frames has one layout: an all-null frame must not change the column type, nor a uniform one narrow it")
	}
}

func TestStreamPipelineFramesStopsAtASendFailureAndReleasesTheBatch(t *testing.T) {
	schema := drainSchema()
	owner := &pinOwner{}
	tracker := vectorized.NewMemoryTracker(64 << 20)
	pipeline := streamPipeline(t, schema, tracker, drainBatch(schema, 100, 10, owner), drainBatch(schema, 200, 10, nil))
	boom := errors.New("client is gone")
	var sent int
	err := streamPipelineFrames(t.Context(), pipeline, schema, FrameLimits{MaxRows: 4}, func([]byte) error {
		sent++
		return boom
	})
	require.ErrorIs(t, err, boom)
	require.Equal(t, 1, sent, "nothing is sent after a failure")
	require.Zero(t, tracker.Used())
	_ = pipeline.Close()
}

func TestStreamPipelineFramesReportsAnExhaustedBudgetBeforeEncoding(t *testing.T) {
	schema := drainSchema()
	pipeline := streamPipeline(t, schema, vectorized.NewMemoryTracker(16), drainBatch(schema, 100, 10, nil))
	err := streamPipelineFrames(t.Context(), pipeline, schema, FrameLimits{}, func([]byte) error { return nil })
	require.ErrorIs(t, err, vectorized.ErrMemoryBudgetExceeded)
}

func TestStreamPipelineFramesStopsOnCancellation(t *testing.T) {
	schema := drainSchema()
	pipeline := streamPipeline(t, schema, nil, drainBatch(schema, 100, 10, nil))
	ctx, cancel := contextWithCancel(t)
	cancel()
	err := streamPipelineFrames(ctx, pipeline, schema, FrameLimits{}, func([]byte) error { return nil })
	require.Error(t, err)
}

func contextWithCancel(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	return context.WithCancel(t.Context())
}
