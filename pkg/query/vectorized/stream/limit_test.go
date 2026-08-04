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

package stream

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// runLimit feeds batches through Limit, stopping at ErrLimitExhausted, and
// returns the surviving element ids plus whether the exhausted signal fired.
func runLimit(t *testing.T, schema *vectorized.BatchSchema, offset, limit uint32, batches ...*vectorized.RecordBatch) ([]uint64, bool) {
	t.Helper()
	op := NewLimit(schema, offset, limit)
	require.NoError(t, op.Init(context.Background()))
	defer func() { require.NoError(t, op.Close()) }()
	var got []uint64
	exhausted := false
	for _, batch := range batches {
		err := op.Process(context.Background(), batch)
		idData := streamElementIDs(batch).Data()
		for _, rowIdx := range activeIndices(batch) {
			got = append(got, ColumnToElementID(idData[rowIdx]))
		}
		if errors.Is(err, vectorized.ErrLimitExhausted) {
			exhausted = true
			break
		}
		require.NoError(t, err)
	}
	return got, exhausted
}

func batchIDs(schema *vectorized.BatchSchema, ids ...uint64) *vectorized.RecordBatch {
	rows := make([]testRow, len(ids))
	for i, id := range ids {
		rows[i] = testRow{ts: int64(i), elemID: id}
	}
	return buildBatch(schema, rows)
}

func TestLimitOffsetZero(t *testing.T) {
	schema := tsSchema()
	got, exhausted := runLimit(t, schema, 0, 3, batchIDs(schema, 1, 2, 3, 4, 5))
	require.Equal(t, []uint64{1, 2, 3}, got)
	require.True(t, exhausted)
}

func TestLimitOffsetSpanningBatchBoundary(t *testing.T) {
	schema := tsSchema()
	b1 := batchIDs(schema, 1, 2, 3)
	b2 := batchIDs(schema, 4, 5, 6)
	got, exhausted := runLimit(t, schema, 2, 3, b1, b2)
	// Skip 1,2; emit 3,4,5.
	require.Equal(t, []uint64{3, 4, 5}, got)
	require.True(t, exhausted)
}

func TestLimitLargerThanInput(t *testing.T) {
	schema := tsSchema()
	got, exhausted := runLimit(t, schema, 0, 100, batchIDs(schema, 1, 2, 3))
	require.Equal(t, []uint64{1, 2, 3}, got)
	require.False(t, exhausted)
}

func TestLimitZeroEmitsNothing(t *testing.T) {
	schema := tsSchema()
	got, exhausted := runLimit(t, schema, 0, 0, batchIDs(schema, 1, 2, 3))
	require.Empty(t, got)
	require.True(t, exhausted)
}

func TestLimitOffsetBeyondInput(t *testing.T) {
	schema := tsSchema()
	got, exhausted := runLimit(t, schema, 10, 5, batchIDs(schema, 1, 2, 3))
	require.Empty(t, got)
	require.False(t, exhausted)
}

func TestLimitExactBoundary(t *testing.T) {
	schema := tsSchema()
	got, exhausted := runLimit(t, schema, 0, 3, batchIDs(schema, 1, 2, 3))
	require.Equal(t, []uint64{1, 2, 3}, got)
	require.True(t, exhausted)
}

func TestLimitPipelineOffsetAcrossMergedStream(t *testing.T) {
	// End-to-end merge -> distinct -> limit with offset + duplicates.
	schema := tsSchema()
	b1 := buildBatch(schema, []testRow{{ts: 10, elemID: 1}, {ts: 30, elemID: 3}})
	b2 := buildBatch(schema, []testRow{{ts: 20, elemID: 1}, {ts: 40, elemID: 4}, {ts: 50, elemID: 5}})
	src := newStaticBatchSource(schema, b1, b2)
	pipe, err := BuildStreamMergePipeline(src, schema, false, 1, 2, 8, 0)
	require.NoError(t, err)
	defer func() { require.NoError(t, pipe.Close()) }()
	// Merged+distinct order by ts: 1(10),3(30),4(40),5(50) [dup 1@20 dropped].
	// offset=1 -> skip 1; limit=2 -> emit 3,4.
	_, ids, _ := drainRows(t, pipe)
	require.Equal(t, []uint64{3, 4}, ids)
}
