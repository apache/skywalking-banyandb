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
	"math/rand"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// drainRows pulls the query dry and returns one line per row (series, timestamp,
// version, the url tag and the int field), in emission order.
func drainRows(t testing.TB, qr *queryResult) []string {
	t.Helper()
	rows, err := drainRowsErr(qr)
	require.NoError(t, err)
	return rows
}

// drainRowsErr is drainRows for goroutines other than the test's: it returns the
// error of a failed pull instead of failing the test.
func drainRowsErr(qr *queryResult) ([]string, error) {
	var rows []string
	for {
		b, err := qr.PullBatch(context.Background())
		if err != nil {
			return nil, err
		}
		if b == nil {
			return rows, nil
		}
		url := b.Tags[0].(*vectorized.TypedColumn[string]).Data()
		fi := b.Fields[0].(*vectorized.TypedColumn[int64]).Data()
		visit := func(r int) {
			rows = append(rows, fmt.Sprintf("%d/%d/%d/%s/%d", b.SeriesIDs[r], b.Timestamps[r], b.Versions[r], url[r], fi[r]))
		}
		if b.Selection != nil {
			for _, r := range b.Selection {
				visit(int(r))
			}
		} else {
			for r := range b.RowCount() {
				visit(r)
			}
		}
		b.Release()
	}
}

// countGoroutines returns how many goroutines run a stack containing marker. The
// suite cannot use a process-wide goroutine count: other tests leave goroutines of
// their own coming and going, so a leak is looked for in the scan's own goroutines,
// the pool's workers and the decodes they run.
func countGoroutines(marker string) int {
	buf := make([]byte, 1<<22)
	buf = buf[:runtime.Stack(buf, true)]
	n := 0
	for _, g := range strings.Split(string(buf), "\n\n") {
		if strings.Contains(g, marker) {
			n++
		}
	}
	return n
}

const (
	workerMarker = "measure.(*decodePool).run("
	taskMarker   = "measure.(*decodeTask).run("
)

// sharedWorkers is the size of the fixture pool.
const sharedWorkers = 4

// onlyTheSharedPool starts the fixture pool and waits until it is the only pool
// with workers, so a test can count its own pools' workers exactly.
func onlyTheSharedPool(t testing.TB) {
	t.Helper()
	sharedFixturePool()
	waitWorkers(t, sharedWorkers)
}

// waitWorkers waits until exactly want decode workers are running.
func waitWorkers(t testing.TB, want int) {
	t.Helper()
	require.Eventually(t, func() bool { return countGoroutines(workerMarker) == want }, 5*time.Second, 5*time.Millisecond,
		"want %d decode workers running", want)
}

// waitNoDecodes waits until no worker is inside a decode.
func waitNoDecodes(t testing.TB) {
	t.Helper()
	require.Eventually(t, func() bool { return countGoroutines(taskMarker) == 0 }, 5*time.Second, 5*time.Millisecond,
		"a decode is still running")
}

// manySeries is one part with n series of 1..rows rows each: n windows of one block.
func manySeries(t testing.TB, n int, rows int64) *fxFixture {
	var p fxPart
	for sid := common.SeriesID(1); sid <= common.SeriesID(n); sid++ {
		p = append(p, fxRange{sid, 1, rows, 1})
	}
	return newFixture(t, p)
}

func prefetchQuery(minTS, maxTS int64, orderByTS, asc bool) *fxQuery {
	return &fxQuery{tags: []string{fxTagURL}, fields: []string{fxFldInt}, minTS: minTS, maxTS: maxTS, orderByTS: orderByTS, ascTS: asc}
}

// maxBlockBytes is the largest uncompressed block size among the query's cursors.
func maxBlockBytes(qr *queryResult) int64 {
	var size uint64
	for _, bc := range qr.data {
		size = max(size, bc.bm.uncompressedSizeBytes)
	}
	return int64(size)
}

// TestPrefetchMatchesSynchronousDecodeUnderRandomCompletionOrder runs queries of
// every scan shape with the decode pool, a loader that finishes later-requested
// decodes first, and checks the rows match the synchronous scan exactly.
func TestPrefetchMatchesSynchronousDecodeUnderRandomCompletionOrder(t *testing.T) {
	verifyNoScanLeaks(t)
	mixed := mixedFixture(t)
	oap := oapFixture(t, 24)
	stag := staggeredFixture(t, 12)
	tests := []struct {
		fx *fxFixture
		q  *fxQuery
	}{
		{mixed, prefetchQuery(0, 30000, false, true)},
		{mixed, prefetchQuery(120, 700, true, true)},
		{mixed, prefetchQuery(0, 30000, true, false)},
		{oap, prefetchQuery(0, 1<<20, true, true)},
		{oap, prefetchQuery(0, 1<<20, true, false)},
		{oap, prefetchQuery(0, 1<<20, false, true)},
		{stag, prefetchQuery(0, 1<<20, true, false)},
		{stag, prefetchQuery(0, 1<<20, true, true)},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprintf("query %d", i), func(t *testing.T) {
			schema := tt.q.schema(t)
			ref := tt.fx.newResult(t, tt.q, schema)
			ref.dp = nil
			want := drainRows(t, ref)
			ref.Release()
			require.NotEmpty(t, want)

			pool := newDecodePool(8, nil)
			defer pool.close()
			for seed := range 3 {
				qr := tt.fx.newResult(t, tt.q, schema)
				qr.dp = pool
				require.True(t, qr.useWindowScan())
				var started atomic.Int64
				var mu sync.Mutex
				var startOrder, finishOrder []int64
				qr.scan.load = func(ctx context.Context, bc *blockCursor, desc bool) (bool, error) {
					n := started.Add(1)
					mu.Lock()
					startOrder = append(startOrder, n)
					mu.Unlock()
					// Earlier decodes sleep longer, with a seeded jitter on top, so decodes finish
					// in a different order than they were requested in.
					jitter := rand.New(rand.NewSource(int64(seed)*1000 + n)).Intn(400) //nolint:gosec // deterministic jitter, not a security context
					time.Sleep(time.Duration(8-(n%8))*300*time.Microsecond + time.Duration(jitter)*time.Microsecond)
					ok, err := decodeCursor(ctx, bc, desc)
					mu.Lock()
					finishOrder = append(finishOrder, n)
					mu.Unlock()
					return ok, err
				}
				got := drainRows(t, qr)
				require.Equal(t, want, got, "seed %d: rows differ from the synchronous scan", seed)
				if len(startOrder) >= 8 {
					require.False(t, slices.IsSorted(finishOrder), "seed %d: decodes should have completed out of request order", seed)
				}
				qr.Release()
			}
		})
	}
}

// stubCursors returns n metadata-only cursors of the given uncompressed size.
func stubCursors(n int, size uint64) []*blockCursor {
	cursors := make([]*blockCursor, n)
	for i := range cursors {
		bc := generateBlockCursor()
		bc.bm.seriesID = common.SeriesID(i + 1)
		bc.bm.uncompressedSizeBytes = size
		cursors[i] = bc
	}
	return cursors
}

func instantLoad(_ context.Context, _ *blockCursor, _ bool) (bool, error) { return true, nil }

func TestPrefetchDepthGrowsWhileReservationsSucceedAndStopsAtTheCap(t *testing.T) {
	verifyNoScanLeaks(t)
	pool := newDecodePool(4, nil)
	defer pool.close()
	cursors := stubCursors(64, 100)
	defer releaseCursors(cursors)
	var stats scanStats
	load := cursorLoader(instantLoad)
	pf := newPrefetcher(context.Background(), pool, newScanBudget(-1, nil), nil, &stats, &load, false, cursors)

	pf.pump()
	require.Equal(t, 2, pf.next-pf.head, "a scan starts two cursors ahead")
	last := pf.depth
	maxDepth := last
	for i, bc := range cursors {
		ok, err := pf.take(context.Background(), bc)
		require.NoError(t, err)
		require.True(t, ok)
		require.GreaterOrEqual(t, pf.depth, last, "depth never shrinks while reservations succeed (take %d)", i)
		require.LessOrEqual(t, pf.next-pf.head, prefetchMaxAhead, "never more than the cap ahead")
		last = pf.depth
		maxDepth = max(maxDepth, last)
	}
	require.Equal(t, prefetchMaxAhead, maxDepth, "depth grows up to the cap and stops there")
	pf.close()
	require.EqualValues(t, prefetchMaxAhead, stats.prefetchPeak)
	require.Zero(t, stats.reservationWaits)
}

func TestPrefetchCapFollowsTheMergeFrontierUpToItsLimit(t *testing.T) {
	verifyNoScanLeaks(t)
	pool := newDecodePool(2, nil)
	defer pool.close()
	cursors := stubCursors(200, 10)
	defer releaseCursors(cursors)
	var stats scanStats
	load := cursorLoader(instantLoad)
	pf := newPrefetcher(context.Background(), pool, newScanBudget(-1, nil), nil, &stats, &load, false, cursors)
	require.Equal(t, prefetchMaxAhead, pf.maxAhead())
	pf.setFrontier(80)
	require.Equal(t, 20, pf.maxAhead())
	pf.setFrontier(20)
	require.Equal(t, prefetchMaxAhead, pf.maxAhead(), "a small frontier keeps the window cap")
	pf.setFrontier(500)
	require.Equal(t, prefetchMaxAheadMerge, pf.maxAhead())
	pf.setFrontier(0)
	require.Equal(t, prefetchMaxAhead, pf.maxAhead())
	pf.pump()
	pf.setFrontier(500)
	for _, bc := range cursors[:100] {
		_, err := pf.take(context.Background(), bc)
		require.NoError(t, err)
	}
	require.LessOrEqual(t, pf.next-pf.head, prefetchMaxAheadMerge)
	require.Greater(t, pf.next-pf.head, prefetchMaxAhead, "a wide merge frontier lets prefetch run past the window cap")
	pf.close()
}

func TestPrefetchWaitsUnderATightBudgetAndStillCompletes(t *testing.T) {
	verifyNoScanLeaks(t)
	fx := manySeries(t, 30, 100)
	q := prefetchQuery(0, 1000, false, true)
	schema := q.schema(t)
	pool := newDecodePool(4, nil)
	defer pool.close()

	ref := fx.newResult(t, q, schema)
	ref.dp = nil
	want := drainRows(t, ref)
	ref.Release()

	qr := fx.newResult(t, q, schema)
	block := maxBlockBytes(qr)
	budget := newScanBudget(block*2+block/2, pool)
	qr.dp, qr.budget = pool, budget
	got := drainRows(t, qr)
	require.Equal(t, want, got, "a tight budget slows the scan down, it does not change it")
	stats := qr.scan.stats
	qr.Release()
	require.Positive(t, stats.reservationWaits, "prefetch waited for room")
	require.Zero(t, stats.reservationFails, "the cursor the scan needed was always reservable")
	require.LessOrEqual(t, stats.prefetchPeak, int64(2), "two blocks fit, so at most the current and one ahead")
	require.Zero(t, budget.held.Load(), "every reservation is back")
	require.Zero(t, pool.reserved.Load())
}

func TestScanFailsWithTheQuotaErrorOnlyWhenTheCurrentCursorCannotBeReserved(t *testing.T) {
	verifyNoScanLeaks(t)
	fx := manySeries(t, 6, 300)
	q := prefetchQuery(0, 1000, false, true)
	schema := q.schema(t)
	pool := newDecodePool(2, nil)
	defer pool.close()
	before := blockCursorPool.RefsCount()

	// Less than one block: the very first cursor cannot be reserved.
	qr := fx.newResult(t, q, schema)
	block := maxBlockBytes(qr)
	budget := newScanBudget(block/2, pool)
	qr.dp, qr.budget = pool, budget
	_, err := qr.PullBatch(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "block scan quota exceeded: used")
	require.Contains(t, err.Error(), fmt.Sprintf("quota is %d bytes", block/2))
	require.EqualValues(t, 1, qr.scan.stats.reservationFails)
	qr.Release()
	require.Zero(t, budget.held.Load())
	require.Equal(t, before, blockCursorPool.RefsCount())

	// Room for one and a half blocks: the first batch is read and held by the
	// consumer, so its reservation stays and the second cursor cannot be reserved.
	qr = fx.newResult(t, q, schema)
	budget = newScanBudget(block+block/2, pool)
	qr.dp, qr.budget = pool, budget
	first, err := qr.PullBatch(context.Background())
	require.NoError(t, err)
	require.NotNil(t, first)
	_, err = qr.PullBatch(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "block scan quota exceeded")
	require.EqualValues(t, 1, qr.scan.stats.reservationFails)
	_, again := qr.PullBatch(context.Background())
	require.Error(t, again, "the error is sticky")
	qr.Release()
	require.Positive(t, budget.held.Load(), "the batch the consumer holds still pins its cursor")
	first.Release()
	require.Zero(t, budget.held.Load())
	require.Zero(t, pool.reserved.Load())
	require.Equal(t, before, blockCursorPool.RefsCount())

	// Releasing each batch is enough for the same budget to finish the scan.
	qr = fx.newResult(t, q, schema)
	budget = newScanBudget(block+block/2, pool)
	qr.dp, qr.budget = pool, budget
	require.Len(t, drainRows(t, qr), 6*300)
	qr.Release()
	require.Zero(t, budget.held.Load())
}

func TestScanReservationIsHeldUntilTheBorrowingBatchIsReleased(t *testing.T) {
	verifyNoScanLeaks(t)
	fx := manySeries(t, 1, 300)
	q := prefetchQuery(0, 1000, false, true)
	schema := q.schema(t)
	pool := newDecodePool(2, nil)
	defer pool.close()
	before := blockCursorPool.RefsCount()

	qr := fx.newResult(t, q, schema)
	block := maxBlockBytes(qr)
	require.Positive(t, block)
	budget := newScanBudget(1<<30, pool)
	qr.dp, qr.budget = pool, budget

	b, err := qr.PullBatch(context.Background())
	require.NoError(t, err)
	require.NotNil(t, b)
	require.EqualValues(t, block, budget.held.Load(), "a block pinned by several columns is counted once")
	require.EqualValues(t, block, pool.reserved.Load())
	// The scan drops its own reference as soon as it has lent the block.
	next, err := qr.PullBatch(context.Background())
	require.NoError(t, err)
	require.Nil(t, next)
	require.EqualValues(t, block, budget.held.Load(), "the batch still borrows the block")
	qr.Release()
	require.EqualValues(t, block, budget.held.Load(), "releasing the query does not free what a batch borrows")
	b.Release()
	require.Zero(t, budget.held.Load(), "the last borrower frees the reservation")
	require.Zero(t, pool.reserved.Load())
	require.Zero(t, budget.tracker.Used())
	require.Equal(t, before, blockCursorPool.RefsCount())
}

func TestScanReleasesReservationsOfPrefetchedCursorsOnEarlyClose(t *testing.T) {
	verifyNoScanLeaks(t)
	fx := manySeries(t, 40, 100)
	q := prefetchQuery(0, 1000, false, true)
	schema := q.schema(t)
	onlyTheSharedPool(t)
	pool := newDecodePool(4, nil)
	defer pool.close()
	waitWorkers(t, sharedWorkers+4)
	before := blockCursorPool.RefsCount()

	qr := fx.newResult(t, q, schema)
	budget := newScanBudget(1<<30, pool)
	qr.dp, qr.budget = pool, budget
	b, err := qr.PullBatch(context.Background())
	require.NoError(t, err)
	b.Release()
	require.Positive(t, qr.scan.pf.next-qr.scan.pf.head, "decodes are in flight or done ahead of the scan")
	require.Greater(t, budget.held.Load(), int64(0))
	qr.Release() // what a LIMIT does after its last row
	require.Zero(t, budget.held.Load())
	require.Zero(t, pool.reserved.Load())
	require.Equal(t, before, blockCursorPool.RefsCount(), "every cursor is back in the pool")
	waitNoDecodes(t)
}

func TestScanCancellationWhileDecodesAreInFlight(t *testing.T) {
	verifyNoScanLeaks(t)
	fx := manySeries(t, 40, 300)
	q := prefetchQuery(0, 1000, false, true)
	schema := q.schema(t)
	onlyTheSharedPool(t)
	pool := newDecodePool(4, nil)
	waitWorkers(t, sharedWorkers+4)
	before := blockCursorPool.RefsCount()

	ctx, cancel := context.WithCancel(context.Background())
	qr := fx.newResult(t, q, schema)
	budget := newScanBudget(1<<30, pool)
	qr.dp, qr.budget, qr.ctx = pool, budget, ctx
	require.True(t, qr.useWindowScan())
	gate := make(chan struct{})
	var running atomic.Int32
	first := qr.scan.pf.items[0].bc
	qr.scan.load = func(ctx context.Context, bc *blockCursor, desc bool) (bool, error) {
		// The first cursor of the plan, which the scan waits for, decodes at once; every
		// decode requested ahead of it blocks inside the loader, as a slow disk read
		// would. The cursor is chosen by identity: workers may start in any order.
		if bc != first {
			running.Add(1)
			<-gate
		}
		return decodeCursor(ctx, bc, desc)
	}
	b, err := qr.PullBatch(ctx)
	require.NoError(t, err)
	b.Release()
	require.Eventually(t, func() bool { return running.Load() > 0 }, 5*time.Second, time.Millisecond)
	require.Positive(t, budget.held.Load(), "the decodes in flight hold their reservations")
	cancel()
	released := make(chan struct{})
	go func() {
		qr.Release()
		close(released)
	}()
	select {
	case <-released:
		t.Fatal("Release returned while a decode was still reading the cursor")
	case <-time.After(100 * time.Millisecond):
	}
	close(gate)
	select {
	case <-released:
	case <-time.After(5 * time.Second):
		t.Fatal("Release did not return after the in-flight decode finished")
	}
	require.Zero(t, budget.held.Load())
	require.Zero(t, pool.reserved.Load())
	require.Equal(t, before, blockCursorPool.RefsCount())
	pool.close()
	waitWorkers(t, sharedWorkers)
}

func TestScanCancelBetweenPullsWaitsForTheRunningPrefetchedDecode(t *testing.T) {
	verifyNoScanLeaks(t)
	fx := manySeries(t, 40, 300)
	q := prefetchQuery(0, 1000, false, true)
	schema := q.schema(t)
	onlyTheSharedPool(t)
	pool := newDecodePool(4, nil)
	waitWorkers(t, sharedWorkers+4)
	before := blockCursorPool.RefsCount()

	qr := fx.newResult(t, q, schema)
	budget := newScanBudget(1<<30, pool)
	qr.dp, qr.budget = pool, budget
	require.True(t, qr.useWindowScan())
	gate := make(chan struct{})
	var running atomic.Int32
	var released atomic.Bool
	first := qr.scan.pf.items[0].bc
	qr.scan.load = func(ctx context.Context, bc *blockCursor, desc bool) (bool, error) {
		if bc == first {
			return decodeCursor(ctx, bc, desc)
		}
		running.Add(1)
		<-gate
		if released.Load() {
			panic("cursor released while its decode was running")
		}
		return decodeCursor(ctx, bc, desc)
	}
	b, err := qr.PullBatch(context.Background())
	require.NoError(t, err)
	b.Release()
	require.Eventually(t, func() bool { return running.Load() > 0 }, 5*time.Second, time.Millisecond)
	// The next cursor the scan takes is requested and running; cancel the pull.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	returned := make(chan error, 1)
	go func() {
		_, pullErr := qr.PullBatch(ctx)
		returned <- pullErr
	}()
	select {
	case <-returned:
		released.Store(true)
		t.Fatal("PullBatch returned, and released the cursor, while its decode was running")
	case <-time.After(100 * time.Millisecond):
	}
	close(gate)
	select {
	case pullErr := <-returned:
		require.ErrorIs(t, pullErr, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("PullBatch did not return after the decode finished")
	}
	qr.Release()
	require.Zero(t, budget.held.Load())
	require.Equal(t, before, blockCursorPool.RefsCount())
	pool.close()
	waitWorkers(t, sharedWorkers)
}

func TestScanReportsAContextCanceledWhileWaitingForADecode(t *testing.T) {
	verifyNoScanLeaks(t)
	fx := manySeries(t, 10, 100)
	q := prefetchQuery(0, 1000, false, true)
	schema := q.schema(t)
	pool := newDecodePool(1, nil)
	defer pool.close()
	before := blockCursorPool.RefsCount()

	ctx, cancel := context.WithCancel(context.Background())
	qr := fx.newResult(t, q, schema)
	qr.dp, qr.ctx = pool, ctx
	require.True(t, qr.useWindowScan())
	qr.scan.load = func(ctx context.Context, bc *blockCursor, desc bool) (bool, error) {
		cancel()
		return decodeCursor(ctx, bc, desc)
	}
	_, err := qr.PullBatch(ctx)
	require.Error(t, err)
	require.True(t, errors.Is(err, context.Canceled), "got %v", err)
	require.Contains(t, err.Error(), "interrupt")
	qr.Release()
	require.Equal(t, before, blockCursorPool.RefsCount())
}

// awaitTask waits for a task that was queued on a pool directly.
func awaitTask(t *decodeTask) {
	for !t.finished() {
		time.Sleep(time.Millisecond)
	}
}

func TestDecodePoolCloseCancelsQueuedTasksAndWaitsForRunningOnes(t *testing.T) {
	verifyNoScanLeaks(t)
	onlyTheSharedPool(t)
	pool := newDecodePool(1, nil)
	gate := make(chan struct{})
	started := make(chan struct{}, 1)
	slow := func(ctx context.Context, _ *blockCursor, _ bool) (bool, error) {
		started <- struct{}{}
		<-gate
		return true, ctx.Err()
	}
	cursors := stubCursors(3, 1)
	defer releaseCursors(cursors)
	tasks := make([]*decodeTask, len(cursors))
	wake := make(chan struct{}, 1)
	for i, bc := range cursors {
		tasks[i] = &decodeTask{}
		tasks[i].init(context.Background(), slow, bc, false, nil, wake)
		pool.submit(tasks[i], false)
	}
	<-started
	closed := make(chan struct{})
	go func() {
		pool.close()
		close(closed)
	}()
	select {
	case <-closed:
		t.Fatal("close returned while a decode was running")
	case <-time.After(50 * time.Millisecond):
	}
	close(gate)
	<-closed
	pool.close() // idempotent
	awaitTask(tasks[0])
	require.NoError(t, tasks[0].err)
	for _, tk := range tasks[1:] {
		awaitTask(tk)
		require.ErrorIs(t, tk.err, errDecodePoolClosed)
	}
	late := &decodeTask{}
	late.init(context.Background(), slow, cursors[0], false, nil, wake)
	pool.submit(late, false)
	awaitTask(late)
	require.ErrorIs(t, late.err, errDecodePoolClosed, "a closed pool cancels instead of queueing")
	waitWorkers(t, sharedWorkers)
}

func TestDecodePoolRecoversFromAPanicInTheLoader(t *testing.T) {
	verifyNoScanLeaks(t)
	pool := newDecodePool(1, nil)
	defer pool.close()
	cursors := stubCursors(2, 1)
	defer releaseCursors(cursors)
	var stats scanStats
	n := 0
	load := cursorLoader(func(_ context.Context, _ *blockCursor, _ bool) (bool, error) {
		n++
		if n == 1 {
			panic("boom")
		}
		return true, nil
	})
	pf := newPrefetcher(context.Background(), pool, newScanBudget(-1, nil), nil, &stats, &load, false, cursors)
	pf.pump()
	ok, err := pf.take(context.Background(), cursors[0])
	require.False(t, ok)
	require.ErrorContains(t, err, "boom")
	ok, err = pf.take(context.Background(), cursors[1])
	require.NoError(t, err)
	require.True(t, ok, "the worker survived")
	pf.close()
}

func TestPrefetchPropagatesLoaderErrorsAndFreesTheReservation(t *testing.T) {
	verifyNoScanLeaks(t)
	pool := newDecodePool(2, nil)
	defer pool.close()
	cursors := stubCursors(4, 50)
	var stats scanStats
	boom := errors.New("disk read failed")
	load := cursorLoader(func(_ context.Context, bc *blockCursor, _ bool) (bool, error) {
		if bc.bm.seriesID == 2 {
			return false, boom
		}
		return true, nil
	})
	budget := newScanBudget(1000, pool)
	pf := newPrefetcher(context.Background(), pool, budget, nil, &stats, &load, false, cursors)
	pf.pump()
	for i, bc := range cursors[:2] {
		ok, err := pf.take(context.Background(), bc)
		if i == 1 {
			require.ErrorIs(t, err, boom)
			require.False(t, ok)
		} else {
			require.NoError(t, err)
		}
	}
	pf.close()
	releaseCursors(cursors)
	require.Zero(t, budget.held.Load(), "releasing the cursors, decoded or not, frees every reservation")
	require.Zero(t, pool.reserved.Load())
}

// fakeMemory is a protector whose headroom a test controls.
type fakeMemory struct {
	protector.Nop
	avail atomic.Int64
}

func (f *fakeMemory) AvailableBytes() int64 { return f.avail.Load() }

func TestNodeHeadroomGatesPrefetchButNeverTheCursorTheScanNeeds(t *testing.T) {
	verifyNoScanLeaks(t)
	mem := &fakeMemory{}
	mem.avail.Store(250)
	pool := newDecodePool(2, mem)
	defer pool.close()
	b1 := newScanBudget(-1, pool)
	b2 := newScanBudget(-1, pool)

	var r1, r2, r3 scanReservation
	require.Equal(t, reserveOK, b1.tryReserve(100, true, &r1))
	require.Equal(t, reserveOK, b2.tryReserve(100, true, &r2))
	require.Equal(t, reserveOverNode, b2.tryReserve(100, true, &r3), "300 would pass the 250 bytes free: prefetch waits")
	require.EqualValues(t, 200, pool.reserved.Load(), "a refused reservation leaves no trace")
	require.Zero(t, b2.held.Load()-100)

	mem.avail.Store(1 << 20)
	require.NoError(t, b2.reserveCurrent(context.Background(), 100, &r3), "the current cursor is not refused")
	r1.release()
	r2.release()
	r3.release()
	r3.release() // idempotent
	require.Zero(t, pool.reserved.Load())
	require.Zero(t, b1.held.Load()+b2.held.Load())

	// With no memory left the scan still takes the reservation it needs; the
	// protector, not the counter, decides how long it waits (AcquireResource).
	mem.avail.Store(0)
	var r4 scanReservation
	require.Equal(t, reserveOK, b1.tryReserve(100, false, &r4))
	require.EqualValues(t, 100, pool.reserved.Load())
	r4.release()
	require.Zero(t, pool.reserved.Load())
}

// holdingMemory is a protector that never admits: AcquireResource waits for ctx.
type holdingMemory struct {
	protector.Nop
	acquires atomic.Int32
}

func (*holdingMemory) AvailableBytes() int64 { return 1 << 30 }

func (h *holdingMemory) AcquireResource(ctx context.Context, _ uint64) error {
	h.acquires.Add(1)
	<-ctx.Done()
	return ctx.Err()
}

func TestReserveCurrentWaitsOnTheProtectorOnlyWhileTheQueryHoldsNothing(t *testing.T) {
	verifyNoScanLeaks(t)
	mem := &holdingMemory{}
	pool := newDecodePool(1, mem)
	defer pool.close()
	b := newScanBudget(-1, pool)

	// Holding nothing: the query is being admitted, and waits for the protector.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	var r1 scanReservation
	require.ErrorIs(t, b.reserveCurrent(ctx, 100, &r1), context.DeadlineExceeded)
	require.Zero(t, b.held.Load(), "a refused admission holds nothing")

	// Holding memory: the current cursor must never block on the protector.
	var r2, r3 scanReservation
	require.Equal(t, reserveOK, b.tryReserve(100, false, &r2))
	acquires := mem.acquires.Load()
	done := make(chan error, 1)
	go func() { done <- b.reserveCurrent(context.Background(), 100, &r3) }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("reserveCurrent blocked on the protector while the query held memory")
	}
	require.Equal(t, acquires, mem.acquires.Load())
	r2.release()
	r3.release()
}

func TestTwoQueriesHoldingFrontiersBeyondTheHeadroomBothComplete(t *testing.T) {
	verifyNoScanLeaks(t)
	mem := &holdingMemory{}
	pool := newDecodePool(2, mem)
	defer pool.close()
	b1, b2 := newScanBudget(-1, pool), newScanBudget(-1, pool)
	// Each query is admitted (held == 0 at its first reservation only because the
	// fake admits through a context that ends at once), then holds a frontier.
	var first [2]scanReservation
	require.Equal(t, reserveOK, b1.tryReserve(100, false, &first[0]))
	require.Equal(t, reserveOK, b2.tryReserve(100, false, &first[1]))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var more [2][4]scanReservation
	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i, b := range []*scanBudget{b1, b2} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for k := range more[i] {
				if errs[i] = b.reserveCurrent(ctx, 100, &more[i][k]); errs[i] != nil {
					return
				}
			}
		}()
	}
	wg.Wait()
	require.NoError(t, errs[0])
	require.NoError(t, errs[1])
	require.EqualValues(t, 1000, pool.reserved.Load(), "beyond the headroom, yet nothing hung")
	for i := range more {
		first[i].release()
		for k := range more[i] {
			more[i][k].release()
		}
	}
	require.Zero(t, pool.reserved.Load())
}

func TestTakePromotesAQueuedPrefetchAheadOfTheNormalQueue(t *testing.T) {
	verifyNoScanLeaks(t)
	onlyTheSharedPool(t)
	pool := newDecodePool(1, nil)
	defer pool.close()
	cursors := stubCursors(4, 1)
	defer releaseCursors(cursors)
	blocker, foreign := generateBlockCursor(), generateBlockCursor()
	defer releaseBlockCursor(blocker)
	defer releaseBlockCursor(foreign)
	gate := make(chan struct{})
	started := make(chan *blockCursor, 16)
	var load cursorLoader = func(_ context.Context, bc *blockCursor, _ bool) (bool, error) {
		started <- bc
		if bc == blocker {
			<-gate
		}
		return true, nil
	}
	wake := make(chan struct{}, 8)
	var tb, tf decodeTask
	tb.init(context.Background(), load, blocker, false, nil, wake)
	pool.submit(&tb, false)
	require.Same(t, blocker, <-started, "the only worker is busy")

	// The foreign read-ahead is queued before the scan's, so it is served first
	// unless the scan's current cursor is promoted.
	tf.init(context.Background(), load, foreign, false, nil, wake)
	pool.submit(&tf, false)
	pf := newPrefetcher(context.Background(), pool, nil, nil, &scanStats{}, &load, false, cursors)
	pf.pump()
	require.True(t, pf.items[0].requested)

	done := make(chan struct{})
	go func() {
		ok, err := pf.take(context.Background(), cursors[0])
		require.NoError(t, err)
		require.True(t, ok)
		close(done)
	}()
	require.Eventually(t, func() bool {
		pool.mu.Lock()
		defer pool.mu.Unlock()
		return len(pool.urgent) == 1
	}, 5*time.Second, time.Millisecond)
	close(gate)
	<-done
	require.Same(t, cursors[0], <-started, "the promoted cursor runs before the older normal entry")
	require.Same(t, foreign, <-started)
	awaitTask(&tf)
	pf.close()
	// The stale normal-queue entry is a no-op: the promoted cursor decoded once.
	pool.close()
	for len(started) > 0 {
		require.NotSame(t, cursors[0], <-started)
	}
}

func TestScanSequenceMatchesTheOrderCursorsAreDecoded(t *testing.T) {
	verifyNoScanLeaks(t)
	fixtures := []struct {
		fx *fxFixture
		q  *fxQuery
	}{
		{mixedFixture(t), prefetchQuery(0, 30000, false, true)},
		{mixedFixture(t), prefetchQuery(0, 30000, true, true)},
		{mixedFixture(t), prefetchQuery(0, 30000, true, false)},
		{oapFixture(t, 16), prefetchQuery(0, 1<<20, true, true)},
		{oapFixture(t, 16), prefetchQuery(0, 1<<20, true, false)},
		{staggeredFixture(t, 9), prefetchQuery(0, 1<<20, true, false)},
		{staggeredFixture(t, 9), prefetchQuery(100, 500, true, true)},
	}
	type key struct {
		part uint64
		sid  common.SeriesID
		ts   int64
	}
	keyOf := func(bc *blockCursor) key { return key{bc.p.partMetadata.ID, bc.bm.seriesID, bc.bm.timestamps.min} }
	for i, tt := range fixtures {
		t.Run(fmt.Sprintf("query %d", i), func(t *testing.T) {
			qr := tt.fx.newResult(t, tt.q, tt.q.schema(t))
			qr.dp = nil
			require.True(t, qr.useWindowScan())
			var want, got []key
			for _, bc := range qr.scan.sequence() {
				want = append(want, keyOf(bc))
			}
			qr.scan.load = func(ctx context.Context, bc *blockCursor, desc bool) (bool, error) {
				got = append(got, keyOf(bc))
				return decodeCursor(ctx, bc, desc)
			}
			drainRows(t, qr)
			qr.Release()
			require.Equal(t, want, got, "the prefetch sequence is the order the scan decodes in")
		})
	}
}

// TestConcurrentQueriesShareTheNodePool runs many queries, some canceled and some
// closed early, over one pool and one node budget under the race detector.
func TestConcurrentQueriesShareTheNodePool(t *testing.T) {
	verifyNoScanLeaks(t)
	mixed := mixedFixture(t)
	oap := oapFixture(t, 20)
	queries := []struct {
		fx *fxFixture
		q  *fxQuery
	}{
		{mixed, prefetchQuery(0, 30000, false, true)},
		{mixed, prefetchQuery(0, 30000, true, false)},
		{oap, prefetchQuery(0, 1<<20, true, true)},
		{oap, prefetchQuery(0, 1<<20, false, true)},
	}
	want := make([][]string, len(queries))
	for i, tt := range queries {
		qr := tt.fx.newResult(t, tt.q, tt.q.schema(t))
		qr.dp = nil
		want[i] = drainRows(t, qr)
		qr.Release()
	}
	before := blockCursorPool.RefsCount()
	onlyTheSharedPool(t)
	mem := &fakeMemory{}
	mem.avail.Store(-1)
	pool := newDecodePool(4, mem)

	var wg sync.WaitGroup
	var budgets []*scanBudget
	var bmu sync.Mutex
	for w := range 24 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(w))) //nolint:gosec // deterministic test workload
			for round := range 6 {
				i := (w + round) % len(queries)
				tt := queries[i]
				qr := tt.fx.newResult(t, tt.q, tt.q.schema(t))
				ctx, cancel := context.WithCancel(context.Background())
				qr.ctx = ctx
				budget := newScanBudget(maxBlockBytes(qr)*int64(3+rng.Intn(20)), pool)
				bmu.Lock()
				budgets = append(budgets, budget)
				bmu.Unlock()
				qr.dp, qr.budget = pool, budget
				switch rng.Intn(3) {
				case 0: // full drain, which must match
					got, err := drainRowsErr(qr)
					switch {
					case err != nil && !strings.Contains(err.Error(), "block scan quota exceeded"):
						t.Errorf("worker %d: %v", w, err)
					case err == nil && !slices.Equal(want[i], got):
						t.Errorf("worker %d: rows differ from the synchronous scan", w)
					}
				case 1: // early close after a few batches, as a LIMIT does
					for range 1 + rng.Intn(3) {
						b, err := qr.PullBatch(ctx)
						if err != nil || b == nil {
							break
						}
						b.Release()
					}
				case 2: // cancel in the middle
					b, err := qr.PullBatch(ctx)
					if err == nil && b != nil {
						b.Release()
					}
					cancel()
					for {
						b, err = qr.PullBatch(ctx)
						if err != nil || b == nil {
							break
						}
						b.Release()
					}
				}
				qr.Release()
				cancel()
			}
		}()
	}
	wg.Wait()
	for _, b := range budgets {
		require.Zero(t, b.held.Load())
	}
	require.Zero(t, pool.reserved.Load())
	require.Equal(t, before, blockCursorPool.RefsCount())
	pool.close()
	waitWorkers(t, sharedWorkers)
	waitNoDecodes(t)
}

// TestConcurrentQueriesStayIdenticalToTheSynchronousScan checks the rows of full
// drains that share the pool with other queries.
func TestConcurrentQueriesStayIdenticalToTheSynchronousScan(t *testing.T) {
	verifyNoScanLeaks(t)
	oap := oapFixture(t, 20)
	q := prefetchQuery(0, 1<<20, true, true)
	schema := q.schema(t)
	sync0 := oap.newResult(t, q, schema)
	sync0.dp = nil
	want := drainRows(t, sync0)
	sync0.Release()
	pool := newDecodePool(3, nil)
	defer pool.close()
	var wg sync.WaitGroup
	for range 12 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			qr := oap.newResult(t, q, schema)
			qr.dp = pool
			got := drainRows(t, qr)
			qr.Release()
			if !slices.Equal(want, got) {
				t.Errorf("rows differ from the synchronous scan")
			}
		}()
	}
	wg.Wait()
}

func TestWholeResultAdmissionStillGuardsThePathsThatLoadEverything(t *testing.T) {
	verifyNoScanLeaks(t)
	fx := manySeries(t, 4, 100)
	q := prefetchQuery(0, 1000, false, true)
	schema := q.schema(t)
	quotaErr := errors.New("block scan quota exceeded: used 9 bytes, quota is 1 bytes")

	qr := fx.newResult(t, q, schema)
	qr.admit = func(context.Context) error { return quotaErr }
	r := qr.Pull()
	require.NotNil(t, r)
	require.ErrorIs(t, r.Error, quotaErr)
	qr.Release()

	defer setLegacyBatchScan(setLegacyBatchScan(true))
	qr = fx.newResult(t, q, schema)
	qr.admit = func(context.Context) error { return quotaErr }
	_, err := qr.PullBatch(context.Background())
	require.ErrorIs(t, err, quotaErr)
	qr.Release()

	calls := 0
	qr = fx.newResult(t, q, schema)
	qr.admit = func(context.Context) error { calls++; return nil }
	require.Len(t, drainRows(t, qr), 400)
	qr.Release()
	require.Equal(t, 1, calls, "admission runs once")
}

// TestPrefetchAddsAtMostAQuarterOfTheFrontierToTheMergePeak checks, through the
// reservations (the decoded memory a scan holds), that decoding ahead keeps a merge
// within the frontier it already needed plus the prefetch allowance.
func TestPrefetchAddsAtMostAQuarterOfTheFrontierToTheMergePeak(t *testing.T) {
	verifyNoScanLeaks(t)
	const (
		series = 40
		blocks = 12
		span   = 200
	)
	parts := make([]fxPart, blocks)
	for p := range parts {
		for sid := common.SeriesID(1); sid <= series; sid++ {
			parts[p] = append(parts[p], fxRange{sid, int64(p*span + 1), int64((p + 1) * span), int64(p + 1)})
		}
	}
	fx := newFixture(t, parts...)
	pool := newDecodePool(4, nil)
	defer pool.close()
	for _, asc := range []bool{true, false} {
		q := prefetchQuery(0, 1<<20, true, asc)
		schema := q.schema(t)
		qr := fx.newResult(t, q, schema)
		block := maxBlockBytes(qr)
		budget := newScanBudget(1<<30, pool)
		qr.dp, qr.budget = pool, budget
		var peakHeld int64
		rows := 0
		for {
			b, err := qr.PullBatch(context.Background())
			require.NoError(t, err)
			if b == nil {
				break
			}
			rows += b.RowCount()
			b.Release()
			peakHeld = max(peakHeld, budget.held.Load())
		}
		require.Equal(t, series*blocks*span, rows)
		bound := (series + series/4 + minFinishedLimit + max(prefetchMaxAhead, series/4)) * block
		require.LessOrEqual(t, peakHeld, bound, "asc=%v: reservations held between pulls", asc)
		require.Greater(t, peakHeld, int64(series)*block/2)
		qr.Release()
		require.Positive(t, qr.scan.stats.prefetchPeak, "the peak is recorded when the scan closes")
		var allowedAhead int64 = max(prefetchMaxAhead, series/4)
		require.LessOrEqual(t, qr.scan.stats.prefetchPeak, allowedAhead)
		require.Zero(t, budget.held.Load())
	}
}
