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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// runDistinct runs Distinct over the batches and returns surviving element ids.
func runDistinct(t *testing.T, schema *vectorized.BatchSchema, batches ...*vectorized.RecordBatch) []uint64 {
	t.Helper()
	op := NewDistinct(schema)
	require.NoError(t, op.Init(context.Background()))
	defer func() { require.NoError(t, op.Close()) }()
	var got []uint64
	for _, batch := range batches {
		require.NoError(t, op.Process(context.Background(), batch))
		idData := streamElementIDs(batch).Data()
		for _, rowIdx := range activeIndices(batch) {
			got = append(got, ColumnToElementID(idData[rowIdx]))
		}
	}
	return got
}

func TestDistinctKeepsFirstAcrossBatches(t *testing.T) {
	schema := tsSchema()
	b1 := buildBatch(schema, []testRow{{ts: 1, elemID: 1}, {ts: 2, elemID: 2}, {ts: 3, elemID: 1}})
	b2 := buildBatch(schema, []testRow{{ts: 4, elemID: 2}, {ts: 5, elemID: 3}, {ts: 6, elemID: 1}})
	require.Equal(t, []uint64{1, 2, 3}, runDistinct(t, schema, b1, b2))
}

func TestDistinctAllUniquePassthrough(t *testing.T) {
	schema := tsSchema()
	b1 := buildBatch(schema, []testRow{{ts: 1, elemID: 1}, {ts: 2, elemID: 2}})
	b2 := buildBatch(schema, []testRow{{ts: 3, elemID: 3}, {ts: 4, elemID: 4}})
	require.Equal(t, []uint64{1, 2, 3, 4}, runDistinct(t, schema, b1, b2))
}

func TestDistinctEmptyBatch(t *testing.T) {
	schema := tsSchema()
	empty := buildBatch(schema, nil)
	got := runDistinct(t, schema, empty)
	require.Empty(t, got)
}

func TestDistinctPreservesOrderWithinBatch(t *testing.T) {
	schema := tsSchema()
	b1 := buildBatch(schema, []testRow{{ts: 1, elemID: 5}, {ts: 2, elemID: 5}, {ts: 3, elemID: 7}, {ts: 4, elemID: 5}})
	require.Equal(t, []uint64{5, 7}, runDistinct(t, schema, b1))
}

func TestDistinctHighBitElementID(t *testing.T) {
	// ElementID uint64 high-bit must round-trip through the int64 column key.
	schema := tsSchema()
	high := uint64(1) << 63
	b1 := buildBatch(schema, []testRow{{ts: 1, elemID: high}, {ts: 2, elemID: high}, {ts: 3, elemID: high + 1}})
	require.Equal(t, []uint64{high, high + 1}, runDistinct(t, schema, b1))
}
