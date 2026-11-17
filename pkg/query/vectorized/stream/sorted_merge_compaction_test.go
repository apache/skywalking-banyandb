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
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

func drainMerge(t *testing.T, m *SortedMerge) (ts, ids []int64) {
	t.Helper()
	for {
		batch, err := m.NextBatch(context.Background())
		require.NoError(t, err)
		if batch == nil {
			return ts, ids
		}
		tsCol := batch.Columns[m.tsIdx].(*vectorized.TypedColumn[int64]).Data()
		idCol := batch.Columns[m.elemIdx].(*vectorized.TypedColumn[int64]).Data()
		ts = append(ts, tsCol[:batch.Len]...)
		ids = append(ids, idCol[:batch.Len]...)
	}
}

// A top-N over many series keeps N rows, so what it holds must follow N and not the number of
// batches those rows came from.
func TestSortedMergeChargeFollowsRetainedRowsNotTheirBatches(t *testing.T) {
	const series, rowsPerBatch = 400, 2048
	schema := tsSchema()
	for _, limit := range []int{20, 100} {
		t.Run(fmt.Sprintf("limit=%d", limit), func(t *testing.T) {
			lease := &countingLease{}
			ctx := query.WithBudgetLease(context.Background(), lease)
			m := NewSortedMergeWithCap(schema, true, rowsPerBatch, limit)
			require.NoError(t, m.Init(ctx))
			batches := spreadBatches(schema, series, rowsPerBatch)
			for _, batch := range batches {
				require.NoError(t, m.Consume(ctx, batch))
			}
			require.NoError(t, m.Finalize(ctx))

			ideal := compactBatchBytes(schema, limit)
			// A little head-room for the batches that are still being built; a pinned batch is about
			// 2,048 rows, orders of magnitude more.
			require.LessOrEqualf(t, lease.Used(), 8*ideal,
				"limit %d: %d bytes held for rows that need about %d", limit, lease.Used(), ideal)
			// At no point does the merge hold more than the batch it is consuming, the compact survivors, and
			// the copy it is about to swap in; never a batch per retained row.
			oneBatch := vectorized.EstimatedRetainedBytes(batches[0])
			require.LessOrEqualf(t, lease.peak.Load(), 3*oneBatch+16*ideal,
				"limit %d: the charge peaked at %d, one consumed batch is %d", limit, lease.peak.Load(), oneBatch)

			ts, _ := drainMerge(t, m)
			require.Len(t, ts, limit)
			for i := range ts {
				require.Equal(t, int64((rowsPerBatch-1)*series+series-1-i), ts[i], "row %d of the newest %d", i, limit)
			}
			require.NoError(t, m.Close())
			require.Zero(t, lease.Used(), "every charge is refunded on Close")
		})
	}
}

// compactBatchBytes is what rows retained rows cost when they sit in one batch of their own.
func compactBatchBytes(schema *vectorized.BatchSchema, rows int) uint64 {
	batch := vectorized.NewRecordBatch(schema, rows)
	for r := 0; r < rows; r++ {
		tsCol := batch.Columns[schema.TimestampIndex()].(*vectorized.TypedColumn[int64])
		tsCol.Append(int64(r))
		batch.Columns[schema.ElementIDIndex()].(*vectorized.TypedColumn[int64]).Append(int64(r))
		batch.Columns[schema.SeriesIDIndex()].(*vectorized.TypedColumn[int64]).Append(0)
		tagIdx, _ := schema.TagIndex(testTagFamily, testTagName)
		batch.Columns[tagIdx].(*vectorized.TypedColumn[*modelv1.TagValue]).Append(strTagValue(strings.Repeat("x", 50)))
		batch.Len++
	}
	return vectorized.EstimatedRetainedBytes(batch)
}

// Compaction only moves rows between batches: the emitted rows, their order and the duplicate rule are
// those of a plain sort of everything, for both orders and both sort keys.
func TestSortedMergeCompactionKeepsTheTopNExactly(t *testing.T) {
	for _, tc := range []struct {
		schema func() *vectorized.BatchSchema
		name   string
		desc   bool
	}{
		{tsSchema, "time-asc", false},
		{tsSchema, "time-desc", true},
		{idxSchema, "index-asc", false},
		{idxSchema, "index-desc", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for seed := int64(1); seed <= 6; seed++ {
				rng := rand.New(rand.NewSource(seed)) //nolint:gosec // fixed seed: a failing case must reproduce exactly
				schema := tc.schema()
				const batchRows = 64
				limit := 5 + rng.Intn(40)
				type rec struct {
					key    string
					ts, id int64
					arrive int
				}
				var all []rec
				lease := &countingLease{}
				ctx := query.WithBudgetLease(context.Background(), lease)
				m := NewSortedMergeWithCap(schema, tc.desc, batchRows, limit)
				require.NoError(t, m.Init(ctx))
				arrive := 0
				for b := 0; b < 80; b++ {
					rows := make([]testRow, batchRows)
					for r := range rows {
						ts := int64(rng.Intn(5000))
						id := uint64(1 + rng.Intn(1200)) // duplicates across and inside batches
						rows[r] = testRow{tag: fmt.Sprintf("t%d", rng.Intn(9)), ts: ts, elemID: id, orderKey: []byte(fmt.Sprintf("%08d", rng.Intn(3000)))}
						all = append(all, rec{ts: ts, id: int64(id), key: string(rows[r].orderKey), arrive: arrive})
						arrive++
					}
					require.NoError(t, m.Consume(ctx, buildBatch(schema, rows)))
				}
				require.NoError(t, m.Finalize(ctx))
				require.Positivef(t, m.compactions, "seed %d: the run never compacted, so it proves nothing about compaction", seed)

				indexOrder := schema.OrderKeyIndex() >= 0
				less := func(a, b rec) bool {
					cmp := 0
					switch {
					case indexOrder:
						cmp = strings.Compare(a.key, b.key)
					case a.ts < b.ts:
						cmp = -1
					case a.ts > b.ts:
						cmp = 1
					}
					if cmp == 0 {
						return a.arrive < b.arrive
					}
					if tc.desc {
						return cmp > 0
					}
					return cmp < 0
				}
				sort.SliceStable(all, func(i, j int) bool { return less(all[i], all[j]) })
				seen := map[int64]struct{}{}
				var wantTS, wantID []int64
				for _, r := range all {
					if _, dup := seen[r.id]; dup {
						continue
					}
					if len(seen) == limit {
						break
					}
					seen[r.id] = struct{}{}
					wantTS = append(wantTS, r.ts)
					wantID = append(wantID, r.id)
				}
				gotTS, gotID := drainMerge(t, m)
				require.Equalf(t, wantID, gotID, "seed %d limit %d: element order", seed, limit)
				require.Equalf(t, wantTS, gotTS, "seed %d limit %d: timestamps", seed, limit)
				require.NoError(t, m.Close())
				require.Zerof(t, lease.Used(), "seed %d: charges balance after Close", seed)
			}
		})
	}
}
