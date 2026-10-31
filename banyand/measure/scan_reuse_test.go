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

package measure

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

type scanReuseLease struct {
	used  atomic.Uint64
	limit uint64
}

func reuseTestResult(ctx context.Context, t *testing.T, fx *fxFixture, q *fxQuery, limit uint64,
) (*queryResult, *vectorized.MemoryTracker, *scanReuseLease, func()) {
	t.Helper()
	lease := &scanReuseLease{limit: limit}
	ctx = vectorized.WithQueryMemoryTracker(query.WithBudgetLease(ctx, lease), int64(limit))
	tracker := vectorized.QueryMemoryTracker(ctx, 0)
	result := fx.newResult(t, q, q.schema(t))
	result.ctx = ctx
	result.budget = newScanBudget(int64(limit), result.dp)
	closeResult := sync.OnceFunc(result.Release)
	t.Cleanup(closeResult)
	require.NoError(t, result.budget.attachShared(ctx, len(result.batchSchema.Columns), 1024, len(result.data)))
	return result, tracker, lease, closeResult
}

func TestSeriesScanTransfersActualDecodedBacking(t *testing.T) {
	fx := manySeries(t, 3, 256)
	q := prefetchQuery(0, 300, false, true)
	result, tracker, lease, closeResult := reuseTestResult(t.Context(), t, fx, q, 32<<20)
	result.dp = nil // Only the released first window can supply the second decode.
	firstCursor, secondCursor := result.data[0], result.data[1]
	first, err := result.PullBatch(t.Context())
	require.NoError(t, err)
	require.NotNil(t, first)
	timestamps := &firstCursor.timestamps[0]
	versions := &firstCursor.versions[0]
	fields := &firstCursor.fields.columns[0].int64s[0]
	values := &firstCursor.tagFamilies[0].columns[0].values[0]
	slot := firstCursor.res.reuse
	require.NotNil(t, slot)
	arena := &slot.arena[0]
	charged := lease.Used()
	first.Release()
	second, err := result.PullBatch(t.Context())
	require.NoError(t, err)
	require.NotNil(t, second)
	closeSecond := sync.OnceFunc(second.Release)
	defer closeSecond()
	require.Same(t, timestamps, &secondCursor.timestamps[0])
	require.Same(t, versions, &secondCursor.versions[0])
	require.Same(t, fields, &secondCursor.fields.columns[0].int64s[0])
	require.Same(t, values, &secondCursor.tagFamilies[0].columns[0].values[0])
	require.Same(t, arena, &secondCursor.res.reuse.arena[0])
	require.Equal(t, charged, lease.Used(), "actual storage transfer consumes no new admission credit")
	require.Equal(t, "/u/2/1/1", second.Tags[0].(*vectorized.TypedColumn[string]).Data()[0])
	closeResult()
	require.Positive(t, tracker.Used(), "borrowed second window still owns its prepaid storage")
	closeSecond()
	require.Zero(t, tracker.Used())
}

func TestSeriesScanPinnedWindowSurvivesReuseCancellationAndClose(t *testing.T) {
	fx := manySeries(t, 6, 256)
	q := prefetchQuery(0, 300, false, true)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	result, tracker, _, closeResult := reuseTestResult(ctx, t, fx, q, 32<<20)
	result.dp = nil
	first, err := result.PullBatch(ctx)
	require.NoError(t, err)
	require.NotNil(t, first)
	closeFirst := sync.OnceFunc(first.Release)
	defer closeFirst()
	url := first.Tags[0].(*vectorized.TypedColumn[string]).Data()
	want := strings.Clone(url[0])
	for range 3 {
		next, nextErr := result.PullBatch(ctx)
		require.NoError(t, nextErr)
		require.NotNil(t, next)
		next.Release()
		require.Equal(t, want, url[0], "later decodes cannot reuse a pinned window")
	}
	cancel()
	_, err = result.PullBatch(ctx)
	require.ErrorIs(t, err, context.Canceled)
	closeResult()
	require.Equal(t, want, url[0], "source close cannot release borrowed values")
	require.EqualValues(t, 1, first.Timestamps[0])
	require.Positive(t, tracker.Used())
	closeFirst()
	require.Zero(t, tracker.Used())
}

func TestSeriesScanReuseKeepsNewestDuplicateVersion(t *testing.T) {
	var older, newer fxPart
	for sid := common.SeriesID(1); sid <= 24; sid++ {
		older = append(older, fxRange{sid: sid, from: 1, to: 256, version: 1})
		newer = append(newer, fxRange{sid: sid, from: 128, to: 256, version: 2})
	}
	fx := newFixture(t, older, newer)
	q := prefetchQuery(0, 300, false, true)
	reference := fx.newResult(t, q, q.schema(t))
	want := drainRows(t, reference)
	reference.Release()
	result, tracker, _, closeResult := reuseTestResult(t.Context(), t, fx, q, 32<<20)
	got, err := drainRowsErr(result)
	require.NoError(t, err)
	require.Equal(t, want, got)
	require.Len(t, got, 24*256)
	closeResult()
	require.Zero(t, tracker.Used())
}

func TestSeriesScanReuseRejectsBeforeUnbudgetedDecode(t *testing.T) {
	fx := manySeries(t, 4, 256)
	q := prefetchQuery(0, 300, false, true)
	result, tracker, lease, closeResult := reuseTestResult(t.Context(), t, fx, q, 32<<20)
	remaining := int64(lease.Limit()-lease.Used()) - 1
	require.NoError(t, tracker.Reserve(remaining))
	result.scan = newWindowScan(result)
	decoded := false
	result.scan.load = func(context.Context, *blockCursor, bool) (bool, error) { decoded = true; return true, nil }
	_, err := result.PullBatch(t.Context())
	require.Error(t, err)
	require.False(t, decoded, "slot storage must be admitted before decode")
	tracker.Release(remaining)
	closeResult()
	require.Zero(t, tracker.Used())
}

func TestTimeOrderedScanRetainsPerCursorAdmission(t *testing.T) {
	fx := manySeries(t, 128, 32)
	q := prefetchQuery(0, 100, true, true)
	result, tracker, _, closeResult := reuseTestResult(t.Context(), t, fx, q, 16<<20)
	_, err := drainRowsErr(result)
	require.Error(t, err)
	require.Nil(t, result.budget.reuse)
	closeResult()
	require.Zero(t, tracker.Used())
}

func TestSeriesScanReuseReleasesFailedPrefetch(t *testing.T) {
	fx := manySeries(t, 16, 256)
	q := prefetchQuery(0, 300, false, true)
	result, tracker, _, closeResult := reuseTestResult(t.Context(), t, fx, q, 32<<20)
	result.scan = newWindowScan(result)
	failure := errors.New("decode failed after allocating storage")
	result.scan.load = func(ctx context.Context, cursor *blockCursor, desc bool) (bool, error) {
		if _, err := decodeCursor(ctx, cursor, desc); err != nil {
			return false, err
		}
		return false, failure
	}
	_, err := result.PullBatch(t.Context())
	require.ErrorIs(t, err, failure)
	closeResult()
	require.Zero(t, tracker.Used())
}

func TestSeriesScanDuplicateProjectionKeepsPerCursorAccounting(t *testing.T) {
	fx := manySeries(t, 3, 256)
	q := prefetchQuery(0, 300, false, true)
	q.fields = []string{fxFldInt, fxFldInt}
	result, tracker, _, closeResult := reuseTestResult(t.Context(), t, fx, q, 32<<20)
	result.scan = newWindowScan(result)
	require.Nil(t, result.budget.reuse, "a block's payload bound counts each stored field once")
	closeResult()
	require.Zero(t, tracker.Used())
}

func TestSeriesScanLargerWindowNeedsNewReservation(t *testing.T) {
	fx := newFixture(t, fxPart{{sid: 1, from: 1, to: 256, version: 1}, {sid: 2, from: 1, to: 512, version: 1}})
	q := prefetchQuery(0, 600, false, true)
	result, tracker, lease, closeResult := reuseTestResult(t.Context(), t, fx, q, 32<<20)
	result.dp = nil
	first, err := result.PullBatch(t.Context())
	require.NoError(t, err)
	require.NotNil(t, first)
	first.Release()
	charged := lease.Used()
	second, err := result.PullBatch(t.Context())
	require.NoError(t, err)
	require.NotNil(t, second)
	closeSecond := sync.OnceFunc(second.Release)
	defer closeSecond()
	require.Greater(t, lease.Used(), charged, "smaller idle storage does not fund a larger decode")
	closeSecond()
	closeResult()
	require.Zero(t, tracker.Used())
}

func (l *scanReuseLease) Charge(bytes uint64) error {
	for {
		current := l.used.Load()
		if bytes > l.limit-current {
			return fmt.Errorf("monotonic scan lease exceeded")
		}
		if l.used.CompareAndSwap(current, current+bytes) {
			return nil
		}
	}
}

// Refund leaves this test lease monotonic so reuse cannot manufacture admission credit.
func (*scanReuseLease) Refund(uint64) {}

func (l *scanReuseLease) Limit() uint64 { return l.limit }
func (l *scanReuseLease) Used() uint64  { return l.used.Load() }
func (l *scanReuseLease) Owner() any    { return l }

func TestSeriesScanReusesDecodedStorageWithinMonotonicLease(t *testing.T) {
	fx := manySeries(t, 128, 32)
	q := prefetchQuery(0, 100, false, true)
	schema := q.schema(t)
	expected := fx.newResult(t, q, schema)
	want := drainRows(t, expected)
	expected.Release()

	lease := &scanReuseLease{limit: 16 << 20}
	ctx := vectorized.WithQueryMemoryTracker(query.WithBudgetLease(t.Context(), lease), 32<<20)
	tracker := vectorized.QueryMemoryTracker(ctx, 0)
	result := fx.newResult(t, q, schema)
	result.ctx = ctx
	result.budget = newScanBudget(32<<20, result.dp)
	require.NoError(t, result.budget.attachShared(ctx, len(schema.Columns), 1024, len(result.data)))
	defer func() {
		if result != nil {
			result.Release()
		}
	}()
	got, err := drainRowsErr(result)
	require.NoError(t, err)
	require.Equal(t, want, got)
	result.Release()
	result = nil
	require.Zero(t, tracker.Used())
	require.Positive(t, lease.Used(), "closing live owners must not refund cumulative admission")
}
