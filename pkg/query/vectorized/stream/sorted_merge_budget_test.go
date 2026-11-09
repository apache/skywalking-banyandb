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
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

var errTestMergeBudget = errors.New("test merge budget: reservation exhausted")

type mergeBudgetLease struct {
	reject   error
	refunds  []uint64
	limit    uint64
	used     uint64
	peak     uint64
	attempts int
	rejectAt int
}

func (l *mergeBudgetLease) Charge(bytes uint64) error {
	l.attempts++
	if l.attempts == l.rejectAt {
		return l.reject
	}
	if bytes > l.limit-l.used {
		return errTestMergeBudget
	}
	l.used += bytes
	l.peak = max(l.peak, l.used)
	return nil
}

func (l *mergeBudgetLease) Refund(bytes uint64) {
	if bytes > l.used {
		panic("test merge budget: refund exceeds accepted charges")
	}
	l.refunds = append(l.refunds, bytes)
	l.used -= bytes
}

func (l *mergeBudgetLease) Limit() uint64 { return l.limit }
func (l *mergeBudgetLease) Used() uint64  { return l.used }
func (l *mergeBudgetLease) Owner() any    { return l }

func newBudgetedSortedMerge(t *testing.T, schema *vectorized.BatchSchema, capRows int,
	lease *mergeBudgetLease,
) (context.Context, *SortedMerge) {
	t.Helper()
	initial := lease.Used()
	ctx := query.WithBudgetLease(context.Background(), lease)
	merge := NewSortedMergeWithCap(schema, false, 8, capRows)
	require.NoError(t, merge.Init(ctx))
	t.Cleanup(func() {
		require.NoError(t, merge.Close())
		require.Equal(t, initial, lease.Used(), "Close must preserve unrelated reservation usage")
		require.LessOrEqual(t, lease.peak, lease.limit)
	})
	return ctx, merge
}

func budgetedMergeOutput(ctx context.Context, t *testing.T, merge *SortedMerge) ([]uint64, []string) {
	t.Helper()
	require.NoError(t, merge.Finalize(ctx))
	var ids []uint64
	var tags []string
	for {
		batch, err := merge.NextBatch(ctx)
		require.NoError(t, err)
		if batch == nil {
			return ids, tags
		}
		idData := streamElementIDs(batch).Data()
		tagIdx, ok := batch.Schema.TagIndex(testTagFamily, testTagName)
		require.True(t, ok)
		tagData := batch.Columns[tagIdx].(*vectorized.TypedColumn[*modelv1.TagValue]).Data()
		for _, idx := range activeIndices(batch) {
			ids = append(ids, ColumnToElementID(idData[idx]))
			tags = append(tags, tagData[idx].GetStr().GetValue())
		}
	}
}

func TestSortedMergeBudgetPrunesBeforeRejectingNextCharge(t *testing.T) {
	schema := tsSchema()
	old := buildBatch(schema, []testRow{{ts: 100, elemID: 100}})
	best := buildBatch(schema, []testRow{{ts: 1, elemID: 1}, {ts: 2, elemID: 2}})
	incoming := buildBatch(schema, []testRow{{ts: 0, elemID: 0}})
	oldBytes := vectorized.EstimatedRetainedBytes(old)
	bestBytes := vectorized.EstimatedRetainedBytes(best)
	require.Equal(t, oldBytes, vectorized.EstimatedRetainedBytes(incoming))
	const unrelated = uint64(4096)
	lease := &mergeBudgetLease{limit: unrelated + oldBytes + bestBytes, used: unrelated}
	ctx, merge := newBudgetedSortedMerge(t, schema, 2, lease)
	require.NoError(t, merge.Consume(ctx, old))
	require.NoError(t, merge.Consume(ctx, best))
	require.Len(t, merge.rows, 3, "three rows have not reached the normal four-row prune threshold")
	require.NoError(t, merge.Consume(ctx, incoming), "pre-charge pruning can drop the whole old batch")
	require.Equal(t, []uint64{oldBytes}, lease.refunds)
	require.Equal(t, 3, lease.attempts, "each input must be charged exactly once")
	ids, _ := budgetedMergeOutput(ctx, t, merge)
	require.Equal(t, []uint64{0, 1}, ids)
}

func TestSortedMergeBudgetCannotRefundPartiallyRetainedBatches(t *testing.T) {
	schema := tsSchema()
	first := buildBatch(schema, []testRow{{ts: 1, elemID: 1}})
	second := buildBatch(schema, []testRow{{ts: 2, elemID: 2}, {ts: 100, elemID: 100}})
	incoming := buildBatch(schema, []testRow{{ts: 0, elemID: 0}})
	budget := vectorized.EstimatedRetainedBytes(first) + vectorized.EstimatedRetainedBytes(second)
	lease := &mergeBudgetLease{limit: budget}
	ctx, merge := newBudgetedSortedMerge(t, schema, 2, lease)
	require.NoError(t, merge.Consume(ctx, first))
	require.NoError(t, merge.Consume(ctx, second))
	require.ErrorIs(t, merge.Consume(ctx, incoming), errTestMergeBudget)
	require.Equal(t, budget, lease.Used())
	require.Empty(t, lease.refunds, "both batches still have a top-window row")
	require.Equal(t, 3, lease.attempts, "do not loop over an unreclaimable reservation")
}

func TestSortedMergeBudgetOversizedIncomingBatchStillFails(t *testing.T) {
	schema := tsSchema()
	incoming := buildBatch(schema, []testRow{{ts: 0, elemID: 0, tag: strings.Repeat("x", 4096)}})
	lease := &mergeBudgetLease{limit: vectorized.EstimatedRetainedBytes(incoming) - 1}
	ctx, merge := newBudgetedSortedMerge(t, schema, 2, lease)
	require.ErrorIs(t, merge.Consume(ctx, incoming), errTestMergeBudget)
	require.Empty(t, merge.rows)
	require.Empty(t, merge.live)
	require.Empty(t, lease.refunds)
	require.Zero(t, lease.Used())
	require.Equal(t, 1, lease.attempts)
}

func TestSortedMergeBudgetCancellationDoesNotPrune(t *testing.T) {
	schema := tsSchema()
	old := buildBatch(schema, []testRow{{ts: 100, elemID: 100}})
	best := buildBatch(schema, []testRow{{ts: 1, elemID: 1}, {ts: 2, elemID: 2}})
	incoming := buildBatch(schema, []testRow{{ts: 0, elemID: 0}})
	lease := &mergeBudgetLease{limit: vectorized.EstimatedRetainedBytes(old) + vectorized.EstimatedRetainedBytes(best)}
	ctx, merge := newBudgetedSortedMerge(t, schema, 2, lease)
	require.NoError(t, merge.Consume(ctx, old))
	require.NoError(t, merge.Consume(ctx, best))
	used := lease.Used()
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	require.ErrorIs(t, merge.Consume(canceled, incoming), context.Canceled)
	require.Len(t, merge.rows, 3)
	require.Empty(t, lease.refunds)
	require.Equal(t, used, lease.Used())
	require.Equal(t, 2, lease.attempts, "cancellation must stop before another lease operation")
}

func TestSortedMergeBudgetNonBudgetErrorIsNotRetried(t *testing.T) {
	schema := tsSchema()
	old := buildBatch(schema, []testRow{{ts: 100, elemID: 100}})
	best := buildBatch(schema, []testRow{{ts: 1, elemID: 1}, {ts: 2, elemID: 2}})
	incoming := buildBatch(schema, []testRow{{ts: 0, elemID: 0}})
	rejected := errors.New("custom reservation policy rejected the charge")
	lease := &mergeBudgetLease{limit: 1 << 20, rejectAt: 3, reject: rejected}
	ctx, merge := newBudgetedSortedMerge(t, schema, 2, lease)
	require.NoError(t, merge.Consume(ctx, old))
	require.NoError(t, merge.Consume(ctx, best))
	used := lease.Used()
	require.ErrorIs(t, merge.Consume(ctx, incoming), rejected)
	require.Equal(t, used, lease.Used())
	require.Empty(t, lease.refunds, "ample byte headroom must not trigger pruning on an arbitrary error")
	require.Len(t, merge.rows, 3)
	require.Equal(t, 3, lease.attempts)
}

func TestSortedMergeBudgetUncappedMergeDoesNotDropRows(t *testing.T) {
	schema := tsSchema()
	old := buildBatch(schema, []testRow{{ts: 100, elemID: 100}})
	best := buildBatch(schema, []testRow{{ts: 1, elemID: 1}, {ts: 2, elemID: 2}})
	incoming := buildBatch(schema, []testRow{{ts: 0, elemID: 0}})
	lease := &mergeBudgetLease{limit: vectorized.EstimatedRetainedBytes(old) + vectorized.EstimatedRetainedBytes(best)}
	ctx, merge := newBudgetedSortedMerge(t, schema, 0, lease)
	require.NoError(t, merge.Consume(ctx, old))
	require.NoError(t, merge.Consume(ctx, best))
	require.ErrorIs(t, merge.Consume(ctx, incoming), errTestMergeBudget)
	require.Len(t, merge.rows, 3)
	require.Empty(t, lease.refunds)
}

func TestSortedMergeBudgetSelectionAndEqualKeyWinner(t *testing.T) {
	schema := tsSchema()
	old := buildBatch(schema, []testRow{{ts: 100, elemID: 100, tag: "oldxx"}, {ts: -100, elemID: 999, tag: "skipx"}})
	old.Selection = []uint16{0}
	best := buildBatch(schema, []testRow{{ts: 1, elemID: 1, tag: "first"}, {ts: 1, elemID: 2, tag: "other"}})
	incoming := buildBatch(schema, []testRow{{ts: -200, elemID: 998, tag: "skipx"}, {ts: 1, elemID: 1, tag: "later"}})
	incoming.Selection = []uint16{1}
	require.Equal(t, vectorized.EstimatedRetainedBytes(old), vectorized.EstimatedRetainedBytes(incoming))
	lease := &mergeBudgetLease{limit: vectorized.EstimatedRetainedBytes(old) + vectorized.EstimatedRetainedBytes(best)}
	ctx, merge := newBudgetedSortedMerge(t, schema, 2, lease)
	require.NoError(t, merge.Consume(ctx, old))
	require.NoError(t, merge.Consume(ctx, best))
	require.NoError(t, merge.Consume(ctx, incoming))
	ids, tags := budgetedMergeOutput(ctx, t, merge)
	require.Equal(t, []uint64{1, 2}, ids)
	require.Equal(t, []string{"first", "other"}, tags, "equal-key duplicates retain the earlier consumed row")
}

func TestSortedMergeBudgetDuplicateOnlyRowsBelowCapCanBePruned(t *testing.T) {
	schema := tsSchema()
	first := buildBatch(schema, []testRow{{ts: 1, elemID: 1, tag: "first"}})
	duplicate := buildBatch(schema, []testRow{{ts: 2, elemID: 1, tag: "later"}})
	incoming := buildBatch(schema, []testRow{{ts: 0, elemID: 2, tag: "fresh"}})
	budget := vectorized.EstimatedRetainedBytes(first) + vectorized.EstimatedRetainedBytes(duplicate)
	lease := &mergeBudgetLease{limit: budget}
	ctx, merge := newBudgetedSortedMerge(t, schema, 4, lease)
	require.NoError(t, merge.Consume(ctx, first))
	require.NoError(t, merge.Consume(ctx, duplicate))
	require.LessOrEqual(t, len(merge.rows), merge.maxRows)
	require.NoError(t, merge.Consume(ctx, incoming), "deduplication can free a whole batch even below the row cap")
	require.Equal(t, []uint64{vectorized.EstimatedRetainedBytes(duplicate)}, lease.refunds)
	ids, tags := budgetedMergeOutput(ctx, t, merge)
	require.Equal(t, []uint64{2, 1}, ids)
	require.Equal(t, []string{"fresh", "first"}, tags)
}

func TestSortedMergeBudgetWithoutLeaseKeepsNormalRowPruning(t *testing.T) {
	schema := tsSchema()
	ctx := context.Background()
	merge := NewSortedMergeWithCap(schema, false, 8, 2)
	require.NoError(t, merge.Init(ctx))
	t.Cleanup(func() { require.NoError(t, merge.Close()) })
	require.NoError(t, merge.Consume(ctx, buildBatch(schema, []testRow{{ts: 100, elemID: 100}})))
	require.NoError(t, merge.Consume(ctx, buildBatch(schema, []testRow{{ts: 1, elemID: 1}, {ts: 2, elemID: 2}})))
	require.Len(t, merge.rows, 3, "without a lease, do not prune before the ordinary row threshold")
	require.Nil(t, merge.lease)
	require.NoError(t, merge.Consume(ctx, buildBatch(schema, []testRow{{ts: 0, elemID: 0}})))
	require.Nil(t, merge.lease)
	ids, _ := budgetedMergeOutput(ctx, t, merge)
	require.Equal(t, []uint64{0, 1}, ids)
}

func TestSortedMergeBudgetRandomizedFiniteLeaseOracle(t *testing.T) {
	var successes, rejections, pressureRecoveries int
	for _, seed := range []int64{1, 7, 42, 97} {
		for _, byIndex := range []bool{false, true} {
			for _, desc := range []bool{false, true} {
				for _, selected := range []bool{false, true} {
					for _, cardinality := range []int{6, 30} {
						for _, allowanceBatches := range []uint64{3, 6, 12} {
							name := fmt.Sprintf("seed%d/index%v/desc%v/selection%v/ids%d/budget%d",
								seed, byIndex, desc, selected, cardinality, allowanceBatches)
							t.Run(name, func(t *testing.T) {
								outcome, recovered := runBudgetedMergeOracleCase(t, seed, byIndex, desc, selected, cardinality, allowanceBatches)
								if outcome {
									successes++
								} else {
									rejections++
								}
								pressureRecoveries += recovered
							})
						}
					}
				}
			}
		}
	}
	require.Positive(t, successes, "the oracle must compare successful constrained executions")
	require.Positive(t, rejections, "the corpus must include genuine finite-lease rejection")
	require.Positive(t, pressureRecoveries, "some inputs must free whole batches before the row threshold")
	t.Logf("successful=%d budget_rejected=%d pre-threshold_recoveries=%d", successes, rejections, pressureRecoveries)
}

func runBudgetedMergeOracleCase(t *testing.T, seed int64, byIndex, desc, selected bool,
	cardinality int, allowanceBatches uint64,
) (bool, int) {
	t.Helper()
	rng := rand.New(rand.NewSource(seed)) //nolint:gosec // Deterministic differential corpus.
	schema := tsSchema()
	if byIndex {
		schema = idxSchema()
	}
	var batches []*vectorized.RecordBatch
	var active []testRow
	var largest uint64
	for batchIdx := 0; batchIdx < 18; batchIdx++ {
		rows := make([]testRow, 1+rng.Intn(4))
		for rowIdx := range rows {
			rows[rowIdx] = testRow{
				ts:       int64(rng.Intn(13)),
				elemID:   uint64(1 + rng.Intn(cardinality)),
				orderKey: []byte{byte(rng.Intn(8))},
				tag:      fmt.Sprintf("b%d-r%d-", batchIdx, rowIdx) + strings.Repeat("p", rng.Intn(513)),
			}
		}
		batch := buildBatch(schema, rows)
		if selected {
			batch.Selection = make([]uint16, 0, len(rows))
			for rowIdx := range rows {
				if rng.Intn(3) != 0 {
					batch.Selection = append(batch.Selection, uint16(rowIdx))
				}
			}
			if rng.Intn(2) == 0 {
				slices.Reverse(batch.Selection)
			}
		}
		for _, rowIdx := range activeIndices(batch) {
			active = append(active, rows[rowIdx])
		}
		largest = max(largest, vectorized.EstimatedRetainedBytes(batch))
		batches = append(batches, batch)
	}
	const capRows = 8
	wantIDs, wantTags := budgetIndependentTopRows(active, byIndex, desc, capRows)
	lease := &mergeBudgetLease{limit: largest * allowanceBatches}
	ctx := query.WithBudgetLease(context.Background(), lease)
	merge := NewSortedMergeWithCap(schema, desc, 8, capRows)
	require.NoError(t, merge.Init(ctx))
	t.Cleanup(func() {
		require.NoError(t, merge.Close())
		require.Zero(t, lease.Used(), "all accepted charges must be refunded, including on failure")
		require.LessOrEqual(t, lease.peak, lease.limit)
	})
	var recovered int
	for _, batch := range batches {
		beforeRows, beforeRefunds := len(merge.rows), len(lease.refunds)
		err := merge.Consume(ctx, batch)
		if err != nil {
			require.ErrorIs(t, err, errTestMergeBudget)
			return false, recovered
		}
		if beforeRows+batch.ActiveLen() < merge.pruneAt && len(lease.refunds) > beforeRefunds {
			recovered++
		}
	}
	gotIDs, gotTags := budgetedMergeOutput(ctx, t, merge)
	require.Equal(t, wantIDs, gotIDs)
	require.Equal(t, wantTags, gotTags, "the chosen duplicate/equal-key row must match stable full sorting")
	return true, recovered
}

func budgetIndependentTopRows(rows []testRow, byIndex, desc bool, capRows int) ([]uint64, []string) {
	ordered := append([]testRow(nil), rows...)
	sort.SliceStable(ordered, func(left, right int) bool {
		comparison := cmp.Compare(ordered[left].ts, ordered[right].ts)
		if byIndex {
			comparison = bytes.Compare(ordered[left].orderKey, ordered[right].orderKey)
		}
		if desc {
			return comparison > 0
		}
		return comparison < 0
	})
	seen := make(map[uint64]struct{})
	var ids []uint64
	var tags []string
	for _, row := range ordered {
		if _, ok := seen[row.elemID]; ok {
			continue
		}
		seen[row.elemID] = struct{}{}
		ids = append(ids, row.elemID)
		tags = append(tags, row.tag)
		if len(ids) == capRows {
			break
		}
	}
	return ids, tags
}

func TestSortedMergeBudgetKeepsTopWindowAcrossTwoHundredThousandUniqueIDs(t *testing.T) {
	const totalRows, windowRows, batchRows = 200_000, 100_000, 1024
	schema := tsSchema()
	lease := &mergeBudgetLease{limit: 256 << 20}
	ctx := query.WithBudgetLease(context.Background(), lease)
	merge := NewSortedMergeWithCap(schema, false, batchRows, windowRows)
	require.NoError(t, merge.Init(ctx))
	t.Cleanup(func() {
		require.NoError(t, merge.Close())
		require.Zero(t, lease.Used())
	})
	// The later half of the input contains every better-ranked record, with no
	// duplicate ID available to make room. Correct top-N must replace the first half.
	for begin := 0; begin < totalRows; begin += batchRows {
		rows := make([]testRow, 0, min(batchRows, totalRows-begin))
		for idx := begin; idx < min(begin+batchRows, totalRows); idx++ {
			id := uint64((idx + windowRows) % totalRows)
			rows = append(rows, testRow{ts: int64(id), elemID: id})
		}
		require.NoError(t, merge.Consume(ctx, buildBatch(schema, rows)))
	}
	ids, _ := budgetedMergeOutput(ctx, t, merge)
	require.Len(t, ids, windowRows)
	for idx, id := range ids {
		require.Equal(t, uint64(idx), id)
	}
	require.LessOrEqual(t, lease.peak, lease.limit)
}
