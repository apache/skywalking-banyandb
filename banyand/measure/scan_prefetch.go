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
	"sort"
)

// Prefetch (measure scan path v2, step 4b).
//
// The scan knows, before it reads a row, every cursor it will decode and the order
// it will ask for them: units in order, segments of a window in order, and inside a
// merge the cursors in the order of their first readable timestamp. The prefetcher
// holds that sequence and asks the node's decodePool to decode the next few cursors
// while the scan emits the current one. A decode is requested in sequence order and
// collected in sequence order (take), so which worker finishes first never changes
// what the scan sees.
//
// Depth is derived, not configured. It starts at prefetchMinDepth cursors ahead and
// grows by one each time the scan has the room it asked for and every reservation
// succeeded, up to a cap; a refused reservation stops the growth and pins the depth
// at what the budget actually allowed, and prefetch waits: nothing further is
// requested until a take (or the release of a reservation, seen at the next take)
// makes room. A slow consumer therefore stalls decoding instead of growing memory.
//
// The cap is prefetchMaxAhead cursors, one per window of a scan that lends one block
// per window. Inside a merge the cursors that become needed together scale with the
// series in the frontier, so the cap follows the widest frontier seen: a quarter of
// it, the same share finishedLimit lets a page hold on top of the frontier, so
// prefetch adds at most about that much decoded memory to the merge's own peak. It
// is limited to prefetchMaxAheadMerge; beyond that more decodes would only sit in
// memory. The reservations, not the cap, are what ultimately bound the memory.
const (
	prefetchMinDepth      = 2
	prefetchMaxAhead      = 8
	prefetchMaxAheadMerge = 32
)

// prefetchItem is one cursor of the scan sequence.
type prefetchItem struct {
	bc   *blockCursor
	task decodeTask
	res  scanReservation
	size uint64
	// requested is true from the reservation until the scan takes the result.
	requested bool
	// promoted is true once the task was also queued as urgent.
	promoted bool
}

// forget drops what the item still references once its decode ended, or was
// withdrawn, so the scan's items do not keep every consumed cursor and its decoded
// buffers reachable. The task's state stays: a stale queue entry may still look at it.
func (it *prefetchItem) forget() {
	it.bc = nil
	it.requested = false
	it.task.clear()
}

// prefetcher feeds one scan from the node's decode pool. It is used by one
// goroutine, the scan's.
type prefetcher struct {
	ctx    context.Context
	pool   *decodePool
	budget *scanBudget
	qm     *queryMetrics
	stats  *scanStats
	// load is the scan's decode function, read when a decode is requested.
	load *cursorLoader
	// wake is signaled by the scan's decodes as they end.
	wake  chan struct{}
	items []prefetchItem
	// head is the next item the scan takes; items [head, next) are requested.
	head, next int
	depth      int
	frontier   int
	peak       int
	desc       bool
	closed     bool
	// stalled is true from a refused reservation until a pump fits everything it
	// asked for: the budget is full, so the scan should hand back what it only
	// holds to coalesce (windowScan.next).
	stalled bool
}

// newPrefetcher plans the decodes of cursors, in the order the scan will take them.
// Without a pool, or with fewer than two cursors, nothing is requested ahead and
// take decodes inline, still through the reservation.
func newPrefetcher(ctx context.Context, pool *decodePool, budget *scanBudget, qm *queryMetrics, stats *scanStats,
	load *cursorLoader, desc bool, cursors []*blockCursor,
) *prefetcher {
	if budget == nil {
		budget = newScanBudget(-1, nil)
	}
	if len(cursors) < 2 {
		pool = nil
	}
	pf := &prefetcher{
		ctx: ctx, pool: pool, budget: budget, qm: qm, stats: stats, load: load, desc: desc,
		depth: prefetchMinDepth,
		items: make([]prefetchItem, len(cursors)),
	}
	if pool != nil {
		pf.wake = make(chan struct{}, 1)
	}
	for i, bc := range cursors {
		pf.items[i].bc = bc
		pf.items[i].size = bc.bm.uncompressedSizeBytes
		pf.items[i].res.sharedBytes = budget.sharedBlockBytes(bc)
		pf.items[i].res.cursor = bc
	}
	return pf
}

// orderPending sorts cursors the way a timeMerge orders its pending cursors: by the
// first timestamp the merge can read, latest first for a descending scan, arrival
// order breaking ties.
func orderPending(cursors []*blockCursor, desc bool) []*blockCursor {
	sorted := make([]*blockCursor, len(cursors))
	copy(sorted, cursors)
	sort.SliceStable(sorted, func(i, j int) bool {
		a, b := mergeStart(sorted[i], desc), mergeStart(sorted[j], desc)
		if desc {
			return a > b
		}
		return a < b
	})
	return sorted
}

// maxAhead is the cap on the depth.
func (pf *prefetcher) maxAhead() int {
	return min(max(pf.frontier/4, prefetchMaxAhead), prefetchMaxAheadMerge)
}

// waiting reports whether prefetch is held back by the memory budget.
func (pf *prefetcher) waiting() bool { return pf != nil && pf.stalled }

// setFrontier tells the prefetcher how many cursors the running merge holds in
// its heap; 0 says no merge is running.
func (pf *prefetcher) setFrontier(n int) {
	if pf != nil {
		pf.frontier = n
	}
}

// pump requests decodes while there is room within the depth and the reservations
// succeed.
func (pf *prefetcher) pump() {
	if pf.pool == nil || pf.closed {
		return
	}
	for pf.next < len(pf.items) && pf.next-pf.head < min(pf.depth, pf.maxAhead()) {
		it := &pf.items[pf.next]
		if out := pf.budget.tryReserve(it.size, true, &it.res); out != reserveOK {
			pf.stalled = true
			pf.stats.reservationWaits++
			if pf.qm != nil {
				pf.qm.scanReservationsWaiting.Inc(1)
			}
			// What the budget allows is the depth; the next successful pump grows it.
			pf.depth = max(prefetchMinDepth, pf.next-pf.head)
			pf.peak = max(pf.peak, pf.next-pf.head)
			return
		}
		pf.submit(it, false)
		pf.next++
	}
	pf.stalled = false
	pf.peak = max(pf.peak, pf.next-pf.head)
	if pf.next < len(pf.items) && pf.depth < pf.maxAhead() {
		pf.depth++
	}
}

// submit hands the reserved item to the pool; urgent is for the item the scan is
// waiting for.
func (pf *prefetcher) submit(it *prefetchItem, urgent bool) {
	if it.size > 0 {
		it.bc.res = &it.res
	}
	it.requested, it.promoted = true, urgent
	it.task.init(pf.ctx, *pf.load, it.bc, pf.desc, pf.qm, pf.wake)
	pf.pool.submit(&it.task, urgent)
}

// take returns the decode of bc, which must be the next cursor of the sequence. It
// has the contract of a cursorLoader: the caller keeps bc and releases it on false
// or an error. A cursor out of sequence is decoded inline.
func (pf *prefetcher) take(ctx context.Context, bc *blockCursor) (bool, error) {
	if pf.closed || pf.head >= len(pf.items) || pf.items[pf.head].bc != bc {
		return pf.loadDirect(ctx, bc)
	}
	it := &pf.items[pf.head]
	if pf.pool == nil {
		if err := pf.reserveCurrent(ctx, it); err != nil {
			pf.advance()
			return false, err
		}
		ok, err := (*pf.load)(ctx, bc, pf.desc)
		pf.advance()
		return ok, err
	}
	if !it.requested {
		if err := pf.reserveCurrent(ctx, it); err != nil {
			pf.advance()
			return false, err
		}
		pf.submit(it, true)
		pf.next = pf.head + 1
	}
	pf.promote(it)
	pf.wait(ctx, &it.task)
	ok, err := it.task.ok, it.task.err
	pf.advance()
	pf.pump()
	return ok, err
}

// promote queues a requested task that has not started a second time, as urgent,
// because the scan is now waiting for it: urgent tasks of every query are served
// before any read-ahead, so without it the cursor a query needs next could starve
// behind other queries' urgent decodes while it sits in the normal queue. The
// stale entry left in the normal queue is harmless: decodeTask.run starts a task
// only by moving it from queued to running, so whichever entry a worker pops
// second finds the task started (or canceled) and does nothing.
func (pf *prefetcher) promote(it *prefetchItem) {
	if it.promoted || it.task.state.Load() != taskQueued {
		return
	}
	it.promoted = true
	pf.pool.submit(&it.task, true)
}

// discard gives up the head cursor without decoding it, when the scan stops before
// asking for it. A decode already requested is withdrawn if it has not started, and
// waited for if it has, so the caller can release bc safely.
func (pf *prefetcher) discard(bc *blockCursor) {
	if pf == nil || pf.closed || pf.head >= len(pf.items) || pf.items[pf.head].bc != bc {
		return
	}
	it := &pf.items[pf.head]
	if it.requested && !it.task.cancel(errScanClosed) {
		pf.wait(pf.ctx, &it.task)
	}
	pf.advance()
}

// wait blocks until t has ended. If ctx ends first, a task that has not started is
// withdrawn; one that has is still waited for, since the caller releases the cursor
// as soon as take returns.
func (pf *prefetcher) wait(ctx context.Context, t *decodeTask) {
	ctxDone := ctx.Done()
	for !t.finished() {
		select {
		case <-pf.wake:
		case <-ctxDone:
			ctxDone = nil
			if t.cancel(ctx.Err()) {
				return
			}
		}
	}
}

// reserveCurrent reserves the memory the head item needs to be decoded now.
func (pf *prefetcher) reserveCurrent(ctx context.Context, it *prefetchItem) error {
	if err := pf.budget.reserveCurrent(ctx, it.size, &it.res); err != nil {
		pf.stats.reservationFails++
		if pf.qm != nil {
			pf.qm.scanReservationsFailed.Inc(1)
		}
		return err
	}
	if it.size > 0 {
		it.bc.res = &it.res
	}
	return nil
}

// advance moves past the head item; the scan owns its cursor from here.
func (pf *prefetcher) advance() {
	pf.items[pf.head].forget()
	pf.head++
	pf.next = max(pf.next, pf.head)
}

// loadDirect decodes a cursor that is not the next of the sequence, which the scan
// never asks for; it keeps the accounting right if that ever changes.
func (pf *prefetcher) loadDirect(ctx context.Context, bc *blockCursor) (bool, error) {
	res := new(scanReservation)
	res.cursor = bc
	res.sharedBytes = pf.budget.sharedBlockBytes(bc)
	if err := pf.budget.reserveCurrent(ctx, bc.bm.uncompressedSizeBytes, res); err != nil {
		return false, err
	}
	bc.res = res
	return (*pf.load)(ctx, bc, pf.desc)
}

// close withdraws every decode not yet taken and waits for those running, so that
// no worker reads a cursor, or the parts behind it, once it returns. The cursors
// stay with the scan, which releases them (and with them their reservations). It is
// idempotent.
func (pf *prefetcher) close() {
	if pf == nil || pf.closed {
		return
	}
	pf.closed = true
	for i := pf.head; i < pf.next; i++ {
		it := &pf.items[i]
		if it.requested && !it.task.cancel(errScanClosed) {
			pf.wait(pf.ctx, &it.task)
		}
		it.task.clear()
	}
	pf.items = nil
	pf.stats.prefetchPeak = int64(pf.peak)
	if pf.qm != nil && pf.peak > 0 {
		pf.qm.scanPrefetchDepth.Observe(float64(pf.peak))
	}
}
