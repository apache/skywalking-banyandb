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
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
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

// TestSortedMergePruneMatchesFullSort is the differential oracle for the
// incremental prune: over a corpus large enough that Consume prunes many times,
// the capped merge must return exactly what an UNCAPPED merge returns truncated
// to the first mergeCap distinct ElementIDs. It deliberately seeds colliding
// timestamps (ties, resolved by arrival order) and repeated ElementIDs (one
// element spanning parts), since those are the two cases a naive prune gets
// wrong, and sweeps an ID cardinality BELOW the cap, which is the case that
// backs the prune threshold off instead of truncating.
func TestSortedMergePruneMatchesFullSort(t *testing.T) {
	for _, seed := range []int64{1, 7, 42} {
		for _, mergeCap := range []int{1, 3, 20, 137} {
			for _, idCardinality := range []int{pruneParityRows / 2, 8} {
				for _, desc := range []bool{false, true} {
					runPruneParityCase(t, seed, mergeCap, idCardinality, desc)
				}
			}
		}
	}
}

const (
	pruneParityRows  = 2000
	pruneParityBatch = 64
)

// runPruneParityCase asserts one (seed, cap, cardinality, direction) of the
// prune differential oracle.
func runPruneParityCase(t *testing.T, seed int64, mergeCap, idCardinality int, desc bool) {
	t.Helper()
	name := fmt.Sprintf("seed%d/cap%d/ids%d/desc%v", seed, mergeCap, idCardinality, desc)
	rng := rand.New(rand.NewSource(seed)) //nolint:gosec // fixed seed: a failing case must reproduce exactly
	rows := make([]testRow, pruneParityRows)
	for rowIdx := range rows {
		rows[rowIdx] = testRow{ts: int64(rng.Intn(pruneParityRows / 4)), elemID: uint64(rng.Intn(idCardinality))}
	}
	// SortedMerge validates batch schemas by POINTER identity, so every batch in
	// one case must share a single instance.
	schema := tsSchema()
	var batches []*vectorized.RecordBatch
	for begin := 0; begin < len(rows); begin += pruneParityBatch {
		batches = append(batches, buildBatch(schema, rows[begin:min(begin+pruneParityBatch, len(rows))]))
	}

	uncapped, err := BuildStreamMergePipeline(newStaticBatchSource(schema, batches...), schema, desc, 0, 1<<20, pruneParityBatch, 0)
	require.NoError(t, err, name)
	wantTS, wantIDs, _ := drainRows(t, uncapped)
	require.NoError(t, uncapped.Close(), name)
	if len(wantIDs) > mergeCap {
		wantTS, wantIDs = wantTS[:mergeCap], wantIDs[:mergeCap]
	}

	capped, err := BuildStreamMergePipeline(newStaticBatchSource(schema, batches...), schema, desc, 0, uint32(mergeCap), pruneParityBatch, mergeCap)
	require.NoError(t, err, name)
	gotTS, gotIDs, _ := drainRows(t, capped)
	require.NoError(t, capped.Close(), name)

	require.Equal(t, wantIDs, gotIDs, name)
	require.Equal(t, wantTS, gotTS, name)
}

// TestSortedMergePruneBoundsRetainedState asserts the bound the prune exists to
// provide, white-box: after consuming far more rows than the cap, the merge holds
// O(cap) row refs pinning O(cap) source batches — not one ref per scanned row
// pinning every batch. It also checks the dropped tail of the backing array is
// zeroed, since a stale batch pointer there is invisible to len() but still keeps
// its whole batch reachable for the GC.
func TestSortedMergePruneBoundsRetainedState(t *testing.T) {
	const (
		mergeCap  = 20
		batchRows = 64
		batchCnt  = 100
	)
	// Distinct-ElementID cardinality is swept, not fixed. An all-distinct corpus
	// is the EASY case: the cap boundary is re-established by every batch. The
	// adversarial case is a cardinality just above the cap — the cap can only fire
	// when a new distinct id appears, so a buffer that carried duplicates would
	// grow for thousands of rows between boundaries. mergeCap+1 is that worst case;
	// a cardinality below the cap (the cap can never fire at all) is also covered.
	for _, cardinality := range []int{mergeCap + 1, mergeCap + 5, mergeCap / 2, batchCnt * batchRows} {
		t.Run(fmt.Sprintf("cardinality%d", cardinality), func(t *testing.T) {
			runPruneBoundsCase(t, mergeCap, batchRows, batchCnt, cardinality)
		})
	}
}

// runPruneBoundsCase asserts the retained-state bound for one ElementID cardinality.
func runPruneBoundsCase(t *testing.T, mergeCap, batchRows, batchCnt, cardinality int) {
	t.Helper()
	schema := tsSchema()
	// Shuffled, NOT ascending: with the scan already in sort order every survivor
	// comes from the newest batch, so the pinned-batch assertion below would hold
	// at 1 no matter how badly the prune leaked. Uncorrelated scan order is the
	// real worst case (a seriesID-major scan interleaves series, so global sort
	// order is not the arrival order) and is what makes the bound load-bearing.
	all := make([]testRow, batchCnt*batchRows)
	for rowIdx := range all {
		all[rowIdx] = testRow{ts: int64(rowIdx), elemID: uint64(rowIdx % cardinality)}
	}
	rng := rand.New(rand.NewSource(1)) //nolint:gosec // fixed seed: a failing case must reproduce exactly
	rng.Shuffle(len(all), func(leftIdx, rightIdx int) { all[leftIdx], all[rightIdx] = all[rightIdx], all[leftIdx] })
	merge := NewSortedMergeWithCap(schema, true, batchRows, mergeCap)
	require.NoError(t, merge.Init(context.Background()))
	for batchIdx := 0; batchIdx < batchCnt; batchIdx++ {
		require.NoError(t, merge.Consume(context.Background(), buildBatch(schema, all[batchIdx*batchRows:(batchIdx+1)*batchRows])))
		// Checked DURING consume, not after Finalize: the final sort-then-cap
		// bounds the buffer either way, so only the mid-stream size proves the
		// prune is doing the work.
		//
		// mergeCap+batchRows is the exact bound for THIS configuration, not the
		// general one. batchRows exceeds mergeCap here, so every Consume crosses
		// the 2*mergeCap threshold and prunes; a config with batchRows < mergeCap
		// would take several batches to reach it and peak just under
		// 2*mergeCap+batchRows instead.
		require.LessOrEqual(t, len(merge.rows), mergeCap+batchRows,
			"merge state must stay O(cap) while scanning, not O(scanned rows)")
	}
	require.NoError(t, merge.Finalize(context.Background()))

	pinned := make(map[*vectorized.RecordBatch]struct{})
	for _, ref := range merge.rows {
		pinned[ref.batch] = struct{}{}
	}
	require.LessOrEqual(t, len(pinned), mergeCap, "retained refs must not pin every consumed batch")
	for _, ref := range merge.rows[len(merge.rows):cap(merge.rows)] {
		require.Nil(t, ref.batch, "dropped tail must be cleared so its batches are collectable")
	}
	require.NoError(t, merge.Close())
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
