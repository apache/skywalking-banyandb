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

package ql

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

func TestNativeAggregateMergeResourceBoundsAndRecovery(t *testing.T) {
	for _, groups := range []int{100_000, 1_000_000} {
		t.Run(fmt.Sprintf("groups=%d", groups), func(t *testing.T) {
			fixture := makeNativeMergeKernelFixture(groups, true)
			for _, limitMiB := range []int64{16, 256} {
				t.Run(fmt.Sprintf("memory=%dMiB", limitMiB), func(t *testing.T) {
					ctx := vectorized.WithQueryMemoryTracker(t.Context(), limitMiB<<20)
					stats, completed := verifyNativeMergeResourceResult(ctx, t, fixture)
					outcome := "complete"
					if !completed {
						outcome = "memory-budget-exceeded"
					}
					t.Logf("groups=%d memory=%dMiB outcome=%s input-group-rows=%d retained-groups=%d key-capacity=%d state-capacity=%d",
						groups, limitMiB, outcome, stats.InputRows, stats.Groups, stats.KeyCapacity, stats.StateCapacity)
					_, recovered := verifyNativeMergeResourceResult(ctx, t, makeNativeMergeKernelFixture(3, true))
					require.True(t, recovered, "a small query must recover using the same released memory tracker")
				})
			}
		})
	}
}

// Fixture inputs and the independent expected-value map are caller-owned. The
// tracker limits only the real merge operator's retained and output allocations.
func verifyNativeMergeResourceResult(ctx context.Context, t *testing.T, fixture nativeMergeKernelFixture) (NativeAggregateStats, bool) {
	t.Helper()
	var sources []*rawSource
	op, err := MergeNativeAggregate(ctx, fixture.plan, func(_ context.Context, leaf NativeAggregateLeaf) (vectorized.PullOperator, error) {
		branch := 0
		if leaf.Function == AggregateCount {
			branch = 1
		}
		batches := fixture.batches[branch]
		source := &rawSource{schema: batches[0].Schema, batches: append([]*vectorized.RecordBatch(nil), batches...)}
		sources = append(sources, source)
		return source, nil
	})
	require.NoError(t, err)
	require.NoError(t, op.Init(ctx))
	var expected map[nativeMergeKernelKey]nativeMergeKernelResult
	var rows int
	completed := true
	for {
		batch, pullErr := op.NextBatch(ctx)
		if pullErr != nil {
			require.True(t, errors.Is(pullErr, vectorized.ErrMemoryBudgetExceeded), "unexpected query failure: %v", pullErr)
			require.Nil(t, batch, "a rejected merge cannot return partial data")
			require.Zero(t, rows, "a rejected merge cannot emit earlier partial batches")
			completed = false
			break
		}
		if batch == nil {
			break
		}
		if expected == nil {
			// The scalar oracle reads the two independent native input lists;
			// it does not inspect the merge's key encoding, lanes or index.
			expected = nativeMergeKernelReference(fixture)
		}
		for row := range batch.Len {
			key := nativeMergeKernelKey{
				high: batch.Columns[0].(*vectorized.TypedColumn[int64]).Data()[row],
				low:  batch.Columns[1].(*vectorized.TypedColumn[int64]).Data()[row],
			}
			want, exists := expected[key]
			require.True(t, exists, "group %+v is unexpected or repeated", key)
			count := batch.Columns[3].(*vectorized.TypedColumn[int64]).Data()[row]
			require.Equal(t, want.count, count, "COUNT for %+v", key)
			require.Equal(t, count == 0, batch.Columns[2].IsNull(row), "SUM nullness for %+v", key)
			require.Equal(t, count == 0, batch.Columns[4].IsNull(row), "AVG nullness for %+v", key)
			if count != 0 {
				require.Equal(t, want.sum, batch.Columns[2].(*vectorized.TypedColumn[float64]).Data()[row], "SUM for %+v", key)
				require.Equal(t, want.sum/float64(want.count), batch.Columns[4].(*vectorized.TypedColumn[float64]).Data()[row], "AVG for %+v", key)
			}
			delete(expected, key)
			rows++
		}
	}
	if completed {
		require.Equal(t, len(fixture.sums), rows, "every expected group must be emitted")
		require.Empty(t, expected, "no expected group may be omitted")
	}
	stats, ok := NativeAggregateStatistics(op)
	require.True(t, ok)
	require.NoError(t, op.Close())
	for _, source := range sources {
		require.Equal(t, 1, source.closes)
	}
	require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used(), "Close must release every reservation")
	return stats, completed
}
