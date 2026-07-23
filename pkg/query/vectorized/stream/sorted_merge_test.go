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

	"github.com/apache/skywalking-banyandb/pkg/convert"
)

func mergePipeline(t *testing.T, source *staticBatchSource, desc bool, batchSize int) []int64 {
	t.Helper()
	pipe, err := BuildStreamMergePipeline(source, source.schema, desc, 0, uint32(1<<20), batchSize, 0)
	require.NoError(t, err)
	defer func() { require.NoError(t, pipe.Close()) }()
	tss, _, _ := drainRows(t, pipe)
	return tss
}

func TestSortedMergeTimeOrderAscending(t *testing.T) {
	schema := tsSchema()
	b1 := buildBatch(schema, []testRow{{ts: 10, elemID: 1}, {ts: 30, elemID: 3}, {ts: 50, elemID: 5}})
	b2 := buildBatch(schema, []testRow{{ts: 20, elemID: 2}, {ts: 40, elemID: 4}, {ts: 60, elemID: 6}})
	src := newStaticBatchSource(schema, b1, b2)
	require.Equal(t, []int64{10, 20, 30, 40, 50, 60}, mergePipeline(t, src, false, 4))
}

func TestSortedMergeTimeOrderDescending(t *testing.T) {
	schema := tsSchema()
	b1 := buildBatch(schema, []testRow{{ts: 10, elemID: 1}, {ts: 30, elemID: 3}})
	b2 := buildBatch(schema, []testRow{{ts: 20, elemID: 2}, {ts: 40, elemID: 4}})
	src := newStaticBatchSource(schema, b1, b2)
	require.Equal(t, []int64{40, 30, 20, 10}, mergePipeline(t, src, true, 8))
}

func TestSortedMergeTimeOrderMatchesUint64ByteOrder(t *testing.T) {
	// Direct int64 compare must match the row path's convert.Uint64ToBytes order.
	schema := tsSchema()
	rows := []testRow{{ts: 5}, {ts: 1000}, {ts: 42}, {ts: 999999}}
	src := newStaticBatchSource(schema, buildBatch(schema, rows))
	got := mergePipeline(t, src, false, 16)
	for i := 1; i < len(got); i++ {
		require.Negative(t, bytesCompare(convert.Uint64ToBytes(uint64(got[i-1])), convert.Uint64ToBytes(uint64(got[i]))))
	}
}

func TestSortedMergeIndexOrderByteLexicographic(t *testing.T) {
	schema := idxSchema()
	b1 := buildBatch(schema, []testRow{
		{ts: 1, elemID: 1, orderKey: []byte("apple"), tag: "svc-apple"},
		{ts: 2, elemID: 2, orderKey: []byte("cherry"), tag: "svc-cherry"},
	})
	b2 := buildBatch(schema, []testRow{
		{ts: 3, elemID: 3, orderKey: []byte("banana"), tag: "svc-banana"},
		{ts: 4, elemID: 4, orderKey: []byte("date"), tag: "svc-date"},
	})
	src := newStaticBatchSource(schema, b1, b2)
	pipe, err := BuildStreamMergePipeline(src, schema, false, 0, 1<<20, 8, 0)
	require.NoError(t, err)
	defer func() { require.NoError(t, pipe.Close()) }()
	_, ids, tags := drainRows(t, pipe)
	// apple(1), banana(3), cherry(2), date(4)
	require.Equal(t, []uint64{1, 3, 2, 4}, ids)
	// Tag cells travel with their row through the gather.
	require.Equal(t, []string{"svc-apple", "svc-banana", "svc-cherry", "svc-date"}, tags)
}

func TestSortedMergeIndexOrderDescending(t *testing.T) {
	schema := idxSchema()
	b1 := buildBatch(schema, []testRow{{ts: 1, elemID: 1, orderKey: []byte("a")}, {ts: 2, elemID: 2, orderKey: []byte("c")}})
	b2 := buildBatch(schema, []testRow{{ts: 3, elemID: 3, orderKey: []byte("b")}})
	src := newStaticBatchSource(schema, b1, b2)
	pipe, err := BuildStreamMergePipeline(src, schema, true, 0, 1<<20, 8, 0)
	require.NoError(t, err)
	defer func() { require.NoError(t, pipe.Close()) }()
	_, ids, _ := drainRows(t, pipe)
	require.Equal(t, []uint64{2, 3, 1}, ids)
}

func TestSortedMergeStableTieHandling(t *testing.T) {
	// Equal keys must preserve input arrival order (b1 rows before b2 rows).
	schema := tsSchema()
	b1 := buildBatch(schema, []testRow{{ts: 100, elemID: 10}, {ts: 100, elemID: 11}})
	b2 := buildBatch(schema, []testRow{{ts: 100, elemID: 20}, {ts: 100, elemID: 21}})
	src := newStaticBatchSource(schema, b1, b2)
	pipe, err := BuildStreamMergePipeline(src, schema, false, 0, 1<<20, 8, 0)
	require.NoError(t, err)
	defer func() { require.NoError(t, pipe.Close()) }()
	_, ids, _ := drainRows(t, pipe)
	require.Equal(t, []uint64{10, 11, 20, 21}, ids)
}

func TestSortedMergeStableTieHandlingDescending(t *testing.T) {
	// Descending reverses key order but ties still keep arrival order.
	schema := tsSchema()
	b1 := buildBatch(schema, []testRow{{ts: 100, elemID: 10}, {ts: 100, elemID: 11}})
	b2 := buildBatch(schema, []testRow{{ts: 100, elemID: 20}})
	src := newStaticBatchSource(schema, b1, b2)
	pipe, err := BuildStreamMergePipeline(src, schema, true, 0, 1<<20, 8, 0)
	require.NoError(t, err)
	defer func() { require.NoError(t, pipe.Close()) }()
	_, ids, _ := drainRows(t, pipe)
	require.Equal(t, []uint64{10, 11, 20}, ids)
}

func TestSortedMergeEmptyInputEOF(t *testing.T) {
	schema := tsSchema()
	src := newStaticBatchSource(schema)
	pipe, err := BuildStreamMergePipeline(src, schema, false, 0, 1<<20, 8, 0)
	require.NoError(t, err)
	defer func() { require.NoError(t, pipe.Close()) }()
	require.NoError(t, pipe.Init(context.Background()))
	batch, nextErr := pipe.Next(context.Background())
	require.NoError(t, nextErr)
	require.Nil(t, batch)
}

func TestSortedMergeEmitsMultipleBatches(t *testing.T) {
	schema := tsSchema()
	rows := make([]testRow, 10)
	for i := range rows {
		rows[i] = testRow{ts: int64(10 - i), elemID: uint64(i)}
	}
	src := newStaticBatchSource(schema, buildBatch(schema, rows))
	got := mergePipeline(t, src, false, 3)
	require.Equal(t, []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, got)
}

// TestSortedMergeCapKeepsInOrderTopN proves the merge cap keeps the first N rows
// in SORT order (top-N), not in arbitrary storage/arrival order. Descending
// ts-order over interleaved batches with cap=3 must keep the 3 NEWEST rows.
func TestSortedMergeCapKeepsInOrderTopN(t *testing.T) {
	schema := tsSchema()
	b1 := buildBatch(schema, []testRow{{ts: 10, elemID: 1}, {ts: 30, elemID: 3}, {ts: 50, elemID: 5}})
	b2 := buildBatch(schema, []testRow{{ts: 20, elemID: 2}, {ts: 40, elemID: 4}, {ts: 60, elemID: 6}})
	src := newStaticBatchSource(schema, b1, b2)
	// desc, cap=3, a big client limit so Limit does not further trim.
	pipe, err := BuildStreamMergePipeline(src, schema, true, 0, 1<<20, 8, 3)
	require.NoError(t, err)
	defer func() { require.NoError(t, pipe.Close()) }()
	tss, _, _ := drainRows(t, pipe)
	require.Equal(t, []int64{60, 50, 40}, tss, "cap must keep the newest 3, not the oldest/storage-order 3")
}

// TestSortedMergeCapCountsDistinctElementIDs proves the cap counts UNIQUE
// ElementIDs (matching the row oracle MergeStreamResults, which stops at
// mergedResult.Len() < topN over deduped ids), not raw rows. With a duplicate
// ElementID inside the retained window, a raw-row cap of 3 would keep only 2
// unique elements; the distinct-aware cap keeps 3 unique (dupes dropped by the
// downstream Distinct).
func TestSortedMergeCapCountsDistinctElementIDs(t *testing.T) {
	schema := tsSchema()
	// asc ts order: (10,id1) (20,id1-dup) (30,id2) (40,id3) (50,id4). Cap=3 unique.
	b := buildBatch(schema, []testRow{
		{ts: 10, elemID: 1},
		{ts: 20, elemID: 1},
		{ts: 30, elemID: 2},
		{ts: 40, elemID: 3},
		{ts: 50, elemID: 4},
	})
	src := newStaticBatchSource(schema, b)
	pipe, err := BuildStreamMergePipeline(src, schema, false, 0, 1<<20, 8, 3)
	require.NoError(t, err)
	defer func() { require.NoError(t, pipe.Close()) }()
	_, ids, _ := drainRows(t, pipe)
	// 3 distinct ids kept in sort order; the ts=20 duplicate of id1 is removed by
	// Distinct, and id4 (ts=50) is beyond the 3rd distinct id so it is excluded.
	require.Equal(t, []uint64{1, 2, 3}, ids)
}

// TestSortedMergeHugeCapDoesNotOverAllocate is the regression for the "max limit"
// query (limit ≈ MaxUint32): the cap must not size the distinct-ID map by maxRows,
// or make(map, ~4e9) allocates multiple GB and hangs the query (observed as a
// distributed DeadlineExceeded). With a huge cap and few rows, every row is
// returned in sort order and the call completes immediately.
func TestSortedMergeHugeCapDoesNotOverAllocate(t *testing.T) {
	schema := tsSchema()
	b := buildBatch(schema, []testRow{
		{ts: 10, elemID: 1},
		{ts: 20, elemID: 2},
		{ts: 30, elemID: 3},
	})
	src := newStaticBatchSource(schema, b)
	hugeCap := int(^uint32(0)) // MaxUint32, as a client "max limit" produces
	pipe, err := BuildStreamMergePipeline(src, schema, false, 0, 1<<20, 8, hugeCap)
	require.NoError(t, err)
	defer func() { require.NoError(t, pipe.Close()) }()
	tss, ids, _ := drainRows(t, pipe)
	require.Equal(t, []int64{10, 20, 30}, tss, "huge cap must return all rows, not truncate")
	require.Equal(t, []uint64{1, 2, 3}, ids)
}

func bytesCompare(a, b []byte) int {
	switch {
	case string(a) < string(b):
		return -1
	case string(a) > string(b):
		return 1
	default:
		return 0
	}
}
