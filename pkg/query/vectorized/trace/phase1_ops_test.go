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

package trace

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

func TestIDDecode(t *testing.T) {
	batch := phase1TestBatch([]*MergeItem{
		NewMergeItem(1, 10, 100, append([]byte{idFormatV1}, []byte("trace-a")...)),
		NewMergeItem(2, 20, 200, append([]byte{idFormatV1}, []byte("trace-b")...)),
	})
	op := NewIDDecode(batch.Schema)
	require.NoError(t, op.Init(context.Background()))
	require.NoError(t, op.Process(context.Background(), batch))
	require.Equal(t, []string{"trace-a", "trace-b"}, phase1TraceIDs(batch).Data())
	require.NoError(t, op.Close())
}

func TestIDDecodeRejectsMalformedPayload(t *testing.T) {
	batch := phase1TestBatch([]*MergeItem{
		NewMergeItem(1, 10, 100, []byte{0xff, 'x'}),
	})
	op := NewIDDecode(batch.Schema)
	require.NoError(t, op.Init(context.Background()))
	require.Error(t, op.Process(context.Background(), batch))
}

func TestDistinctTraceIDCarriesPartAndKey(t *testing.T) {
	batch := phase1TestBatch([]*MergeItem{
		{Key: 10, SeriesID: 1, PartID: 100, Payload: []byte("a"), TraceID: "trace-a"},
		{Key: 20, SeriesID: 2, PartID: 200, Payload: []byte("b"), TraceID: "trace-b"},
		{Key: 30, SeriesID: 3, PartID: 100, Payload: []byte("c"), TraceID: "trace-c"},
	})
	op := NewDistinctTraceID(batch.Schema)
	require.NoError(t, op.Init(context.Background()))
	require.NoError(t, op.Process(context.Background(), batch))
	require.Equal(t, []string{"trace-a", "trace-b", "trace-c"}, op.Order())
	require.Equal(t, map[int64][]string{100: {"trace-a", "trace-c"}, 200: {"trace-b"}}, op.TraceIDsByPart())
	require.Equal(t, map[string]int64{"trace-a": 10, "trace-b": 20, "trace-c": 30}, op.Keys())
	require.Nil(t, batch.Selection)
}

func TestLimitSelectsFirstMaxRows(t *testing.T) {
	batch := phase1TestBatch([]*MergeItem{
		NewMergeItem(1, 1, 1, []byte("a")),
		NewMergeItem(2, 1, 1, []byte("b")),
		NewMergeItem(3, 1, 1, []byte("c")),
	})
	op := NewLimit(batch.Schema, 2)
	require.NoError(t, op.Init(context.Background()))
	require.ErrorIs(t, op.Process(context.Background(), batch), vectorized.ErrLimitExhausted)
	require.Equal(t, []uint16{0, 1}, batch.Selection)
}

func TestLimitZeroIsUnbounded(t *testing.T) {
	batch := phase1TestBatch([]*MergeItem{
		NewMergeItem(1, 1, 1, []byte("a")),
		NewMergeItem(2, 1, 1, []byte("b")),
	})
	op := NewLimit(batch.Schema, 0)
	require.NoError(t, op.Init(context.Background()))
	require.NoError(t, op.Process(context.Background(), batch))
	require.Equal(t, []uint16{0, 1}, batch.Selection)
}

func phase1TestBatch(items []*MergeItem) *vectorized.RecordBatch {
	batch := vectorized.NewRecordBatch(NewPhase1Schema(), len(items))
	for _, item := range items {
		appendMergeItem(batch, item)
	}
	return batch
}
