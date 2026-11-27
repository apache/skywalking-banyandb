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
	"errors"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/banyand/internal/sidx"
	"github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
)

// groupTestLease is a query lease with a fixed limit that records the highest usage it reaches.
type groupTestLease struct {
	limit uint64
	used  atomic.Uint64
	peak  atomic.Uint64
}

func (l *groupTestLease) Charge(bytes uint64) error {
	used := l.used.Add(bytes)
	if used > l.limit {
		l.used.Add(^(bytes - 1))
		return errors.New("test lease exhausted")
	}
	for {
		peak := l.peak.Load()
		if used <= peak || l.peak.CompareAndSwap(peak, used) {
			return nil
		}
	}
}

func (l *groupTestLease) Refund(bytes uint64) {
	l.used.Add(^(bytes - 1))
}

func (l *groupTestLease) Limit() uint64 { return l.limit }
func (l *groupTestLease) Used() uint64  { return l.used.Load() }
func (l *groupTestLease) Owner() any    { return l }

// groupTestScanBatch returns a scan batch over the three traces of the parity fixture, and the span bytes of each trace.
func groupTestScanBatch(t *testing.T) (*scanBatch, queryOptions, map[string]uint64) {
	t.Helper()
	tst, cleanup := newParityTSTable(t, tsTS1)
	t.Cleanup(cleanup)
	ctx := context.Background()
	qo := queryOptions{
		TraceQueryOptions: model.TraceQueryOptions{TagProjection: allTagProjections},
		schemaTagTypes:    testSchemaTagTypes,
		traceIDs:          []string{"trace1", "trace2", "trace3"},
	}
	phase1, err := newParityTrace().buildVectorizedPhase1TraceBatch(ctx, qo, nil, sidx.QueryRequest{}, false, 0)
	require.NoError(t, err)
	batch, err := newParityTrace().buildVectorizedScanBatch(ctx, []*tsTable{tst}, qo, phase1)
	require.NoError(t, err)
	sizes := make(map[string]uint64)
	for _, cursor := range batch.cursors {
		sizes[cursor.bm.traceID] += cursor.bm.uncompressedSpanSizeBytes
	}
	return batch, qo, sizes
}

// A query loads its traces a group at a time, sized by the budget it has left: the spans it holds at once stay within that
// budget however many traces the query asks for, the traces come back the same and in the same order, and every charge is
// refunded when the query is released.
func TestTraceGroupLoaderFollowsTheBudget(t *testing.T) {
	reference, qo, sizes := groupTestScanBatch(t)
	want, err := materializeVectorizedTraceResults(context.Background(), reference, qo)
	require.NoError(t, err)
	releaseVectorizedScanBatch(reference)
	require.Len(t, want, 3)

	var largest, total uint64
	for _, size := range sizes {
		largest = max(largest, size)
		total += size
	}
	require.Greater(t, total, largest, "the fixture needs more than one trace")

	// A group may use a quarter of the free budget, so this lease lets a group hold the largest trace and no more than that.
	lease := &groupTestLease{limit: largest * traceGroupBudgetShare}
	ctx := query.WithBudgetLease(context.Background(), lease)
	batch, _, _ := groupTestScanBatch(t)
	loader := newTraceGroupLoader(batch, qo)
	var got []*model.TraceResult
	groups := 0
	for !loader.exhausted() {
		group, loadErr := loader.load(ctx)
		require.NoError(t, loadErr)
		groups++
		require.LessOrEqual(t, lease.Used(), largest, "a group is within its share of the budget")
		got = append(got, group...)
	}
	loader.release(ctx)
	require.Greater(t, groups, 1, "the traces do not fit one group")
	require.Zero(t, lease.Used(), "every charge is refunded")
	require.LessOrEqual(t, lease.peak.Load(), largest)
	require.Less(t, lease.peak.Load(), total, "the spans held at once are fewer than the spans of all the traces")
	require.Equal(t, resultTraceIDs(want), resultTraceIDs(got))
	for i := range want {
		require.Equal(t, want[i].Spans, got[i].Spans)
		require.Equal(t, want[i].SpanIDs, got[i].SpanIDs)
	}
}

// Through the result a query pulls: the traces are the same, a lease that is too small for all the spans at once is enough,
// and nothing stays charged after Release.
func TestVectorizedTraceQueryResultPullsGroupByGroup(t *testing.T) {
	reference, qo, sizes := groupTestScanBatch(t)
	want, err := materializeVectorizedTraceResults(context.Background(), reference, qo)
	require.NoError(t, err)
	releaseVectorizedScanBatch(reference)

	var largest, total uint64
	for _, size := range sizes {
		largest = max(largest, size)
		total += size
	}
	lease := &groupTestLease{limit: largest * traceGroupBudgetShare}
	require.Less(t, lease.limit, total*traceGroupBudgetShare, "the lease is smaller than what the old path would have charged")
	ctx := query.WithBudgetLease(context.Background(), lease)
	batch, _, _ := groupTestScanBatch(t)
	result, err := newVectorizedTraceQueryResult(ctx, batch, qo, nil, nil, nil, nil)
	require.NoError(t, err)
	var got []*model.TraceResult
	for next := result.Pull(); next != nil; next = result.Pull() {
		require.NoError(t, next.Error)
		got = append(got, next)
	}
	result.Release()
	require.Equal(t, resultTraceIDs(want), resultTraceIDs(got))
	require.Zero(t, lease.Used())
	require.Less(t, lease.peak.Load(), total)
}

func resultTraceIDs(results []*model.TraceResult) []string {
	ids := make([]string, 0, len(results))
	for _, result := range results {
		ids = append(ids, result.TID)
	}
	return ids
}
