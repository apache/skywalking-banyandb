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
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

const (
	boundMergeCap = 25
	boundBatchRow = 64
	boundBatchCnt = 100
	// boundSelectivity is the 1-in-N match rate of the criteria. It is low on
	// purpose: the whole point of #14056 is that a selective predicate must not
	// force the merge to buffer every scanned row before it can be applied.
	boundSelectivity = 64
	boundHotTag      = "hot"
	boundColdTag     = "cold"
)

// filteredMergeState replays a shuffled corpus through the merge exactly as the
// filtered index-order stream pipeline runs it, and returns the merge plus the
// peak retained-row count observed mid-stream.
//
// This helper is the ONE place the pipeline's shape is encoded. Today the tag
// filter runs at egress, AFTER the merge, so the merge is built uncapped
// (mergeCap 0 at stream_plan_indexscan_local_vectorized.go:329-331) and every
// scanned row is buffered. Moving the filter ahead of the merge as a fusible that
// writes batch.Selection makes the cap sound; flipping this helper to select
// pre-merge and pass boundMergeCap is what turns the assertions below green.
func filteredMergeState(t *testing.T, schema *vectorized.BatchSchema, corpus []testRow) (*SortedMerge, int) {
	t.Helper()
	ctx := context.Background()
	merge := NewSortedMergeWithCap(schema, false, boundBatchRow, 0)
	require.NoError(t, merge.Init(ctx))
	peak := 0
	for batchIdx := 0; batchIdx < boundBatchCnt; batchIdx++ {
		batch := buildBatch(schema, corpus[batchIdx*boundBatchRow:(batchIdx+1)*boundBatchRow])
		require.NoError(t, merge.Consume(ctx, batch))
		if len(merge.rows) > peak {
			peak = len(merge.rows)
		}
	}
	require.NoError(t, merge.Finalize(ctx))
	return merge, peak
}

// TestTagFilterBoundsRetainedMergeState is the #14056 R2 assertion: a filtered
// index-order query must keep merge state O(limit+offset), not O(scanned rows).
//
// The corpus is shuffled rather than pre-sorted for the reason
// TestSortedMergePruneBoundsRetainedState records: with input already in sort
// order every survivor comes from the newest batch and the pinned-batch bound
// holds at 1 however badly the retained state leaks.
func TestTagFilterBoundsRetainedMergeState(t *testing.T) {
	// One schema instance for every batch: SortedMerge validates batch.Schema by
	// pointer, so a per-batch idxSchema() call fails as a foreign schema rather
	// than as a merge-bound violation.
	schema := idxSchema()
	corpus := make([]testRow, boundBatchCnt*boundBatchRow)
	for rowIdx := range corpus {
		tag := boundColdTag
		if rowIdx%boundSelectivity == 0 {
			tag = boundHotTag
		}
		corpus[rowIdx] = testRow{
			ts:       int64(rowIdx),
			elemID:   uint64(rowIdx),
			tag:      tag,
			orderKey: []byte(orderKeyFor(rowIdx)),
		}
	}
	rng := rand.New(rand.NewSource(1)) //nolint:gosec // fixed seed: a failing case must reproduce exactly
	rng.Shuffle(len(corpus), func(leftIdx, rightIdx int) { corpus[leftIdx], corpus[rightIdx] = corpus[rightIdx], corpus[leftIdx] })

	merge, peak := filteredMergeState(t, schema, corpus)

	require.LessOrEqual(t, peak, boundMergeCap+boundBatchRow,
		"filtered merge buffered %d rows while scanning %d; state must stay O(limit+offset)", peak, len(corpus))
	pinned := make(map[*vectorized.RecordBatch]struct{})
	for _, ref := range merge.rows {
		pinned[ref.batch] = struct{}{}
	}
	require.LessOrEqual(t, len(pinned), boundMergeCap, "retained refs must not pin every consumed batch")
	for _, ref := range merge.rows[len(merge.rows):cap(merge.rows)] {
		require.Nil(t, ref.batch, "dropped tail must be cleared so its batches are collectable")
	}
	require.NoError(t, merge.Close())
}

// orderKeyFor renders a zero-padded index-order key so lexicographic byte order
// equals numeric order.
func orderKeyFor(n int) string {
	digits := make([]byte, 6)
	for pos := 5; pos >= 0; pos-- {
		digits[pos] = byte('0' + n%10)
		n /= 10
	}
	return string(digits)
}
