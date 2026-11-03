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
	"fmt"
	"math"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// Node-wide decode pool and scan memory accounting (measure scan path v2, step 4b).
//
// One decodePool serves every query of a data node. A scan asks it to decode the
// cursors it will consume next (scan_prefetch.go); a worker runs the same loadData
// path the scan would run inline, so the pool changes when a block is decoded, never
// what is decoded. A pool per query would bring back the per-block fan-out the
// window scan removed as soon as queries run concurrently.
//
// Memory is accounted per cursor. Before a cursor is decoded the scan reserves the
// block's uncompressed size, known from block metadata, from the query's budget;
// the reservation is attached to the cursor and released when its last reference
// goes, that is after the scan and every batch that borrows its buffers are done
// with it. A buffer pinned by several batches is therefore counted once.
//
// protector.Memory.AcquireResource, which the one-shot check used to call for the
// whole result, is an admission gate: it waits until sampled process usage leaves
// room and holds nothing, so there is nothing to release. The reservation counters
// here are what the scan can account incrementally; the protector is still asked
// for headroom, and still has the last word.

var (
	errDecodePoolClosed = errors.New("measure decode pool is closed")
	errScanClosed       = errors.New("measure scan is closed")
)

type taskState = int32

const (
	taskQueued taskState = iota
	taskRunning
	taskFinished
	// taskCanceling is the instant between winning the cancellation and publishing its
	// error; the task counts as done only once it is taskCanceled.
	taskCanceling
	taskCanceled
)

// decodeTask is one cursor decode queued on a decodePool. It is owned by the
// prefetcher that created it; the pool only runs it.
type decodeTask struct {
	enqueued time.Time
	ctx      context.Context
	load     cursorLoader
	bc       *blockCursor
	qm       *queryMetrics
	// wake is signaled each time a task of the scan finishes or is canceled; one
	// channel serves the scan's tasks, whose consumer re-checks the one it waits for.
	wake  chan struct{}
	err   error
	state atomic.Int32
	ok    bool
	desc  bool
}

func (t *decodeTask) init(ctx context.Context, load cursorLoader, bc *blockCursor, desc bool, qm *queryMetrics, wake chan struct{}) {
	t.ctx, t.load, t.bc, t.desc, t.qm, t.wake = ctx, load, bc, desc, qm, wake
	t.enqueued = time.Now()
	liveDecodeTasks.Add(1)
}

// clear drops the references of a task that has ended and whose result was read, or
// that was withdrawn before it started. It leaves the state, which a stale queue
// entry may still check, so such an entry stays a no-op.
func (t *decodeTask) clear() {
	t.ctx, t.load, t.bc, t.qm, t.wake, t.err = nil, nil, nil, nil, nil, nil
}

// finished reports whether the task has ended, by running or by cancellation; its
// result is then readable.
func (t *decodeTask) finished() bool {
	s := t.state.Load()
	return s == taskFinished || s == taskCanceled
}

// signal wakes the consumer through wake, which the caller read while it owned the
// task, before it published the task's end: once the end is visible the consumer may clear the task.
func signal(wake chan struct{}) {
	select {
	case wake <- struct{}{}:
	default:
	}
}

// cancel stops a task that has not started. It reports whether it did; a task
// that is running or finished is left alone and ends by itself.
func (t *decodeTask) cancel(err error) bool {
	if !t.state.CompareAndSwap(taskQueued, taskCanceling) {
		return false
	}
	wake := t.wake
	t.err = err
	t.state.Store(taskCanceled)
	liveDecodeTasks.Add(-1)
	signal(wake)
	return true
}

func (t *decodeTask) run() {
	if !t.state.CompareAndSwap(taskQueued, taskRunning) {
		return
	}
	wake := t.wake
	if t.qm != nil {
		t.qm.scanDecodeQueueWait.Observe(time.Since(t.enqueued).Seconds())
	}
	defer func() {
		if r := recover(); r != nil {
			t.ok = false
			t.err = fmt.Errorf("panic while decoding a block: %v", r)
			logger.GetLogger("measure-decode-pool").Error().Interface("panic", r).
				Bytes("stack", debug.Stack()).Msg("panic in the decode pool")
		}
		t.state.Store(taskFinished)
		liveDecodeTasks.Add(-1)
		signal(wake)
	}()
	if err := t.ctx.Err(); err != nil {
		t.err = err
		return
	}
	t.ok, t.err = t.load(t.ctx, t.bc, t.desc)
}

// decodePool is the node-wide set of decode workers and the node's total of scan
// reservations.
type decodePool struct {
	pm   protector.Memory
	cond *sync.Cond
	// urgent holds the decodes a scan is waiting for right now; the workers take
	// them before the decodes requested ahead of need, so a query's progress is not
	// queued behind other queries' read-ahead.
	urgent   []*decodeTask
	queue    []*decodeTask
	mu       sync.Mutex
	wg       sync.WaitGroup
	reserved atomic.Int64
	head     int
	workers  int
	closed   bool
}

// newDecodePool starts workers goroutines (at least one). pm bounds the node's
// reservations; nil leaves them unbounded.
func newDecodePool(workers int, pm protector.Memory) *decodePool {
	workers = max(workers, 1)
	p := &decodePool{pm: pm, workers: workers}
	p.cond = sync.NewCond(&p.mu)
	p.wg.Add(workers)
	liveDecodeWorkers.Add(int64(workers))
	for range workers {
		go p.run()
	}
	return p
}

func (p *decodePool) run() {
	defer p.wg.Done()
	defer liveDecodeWorkers.Add(-1)
	for {
		p.mu.Lock()
		for len(p.urgent) == 0 && p.head == len(p.queue) && !p.closed {
			p.cond.Wait()
		}
		if p.closed {
			p.mu.Unlock()
			return
		}
		var t *decodeTask
		if n := len(p.urgent); n > 0 {
			t = p.urgent[0]
			p.urgent[0] = nil
			p.urgent = p.urgent[1:]
			if len(p.urgent) == 0 {
				p.urgent = nil
			}
		} else {
			t = p.queue[p.head]
			p.queue[p.head] = nil
			p.head++
			if p.head == len(p.queue) {
				p.queue, p.head = p.queue[:0], 0
			}
		}
		p.mu.Unlock()
		t.run()
	}
}

// submit queues t, ahead of the read-ahead tasks when urgent. On a closed pool the
// task is canceled instead.
func (p *decodePool) submit(t *decodeTask, urgent bool) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		t.cancel(errDecodePoolClosed)
		return
	}
	if urgent {
		p.urgent = append(p.urgent, t)
	} else {
		p.queue = append(p.queue, t)
	}
	p.mu.Unlock()
	p.cond.Signal()
}

// close cancels the queued tasks and waits for the running ones, so no worker
// touches a part once it returns. It is idempotent.
func (p *decodePool) close() {
	if p == nil {
		return
	}
	p.mu.Lock()
	if !p.closed {
		p.closed = true
		for _, t := range p.urgent {
			t.cancel(errDecodePoolClosed)
		}
		for i := p.head; i < len(p.queue); i++ {
			p.queue[i].cancel(errDecodePoolClosed)
			p.queue[i] = nil
		}
		p.urgent, p.queue, p.head = nil, nil, 0
	}
	p.mu.Unlock()
	p.cond.Broadcast()
	p.wg.Wait()
}

// headroom is what the protector says is still free, or a negative number when it
// sets no limit.
func (p *decodePool) headroom() int64 {
	if p.pm == nil {
		return -1
	}
	return p.pm.AvailableBytes()
}

// reserve adds n to the node's total. A reservation made ahead of need (prefetch)
// is refused when the total would pass the protector's headroom; the one a scan
// needs to make progress is always added, and waits on the protector instead
// (scanBudget.reserveCurrent), because refusing it could leave two queries each
// waiting for memory the other holds.
func (p *decodePool) reserve(n int64, ahead bool) bool {
	if !ahead {
		p.reserved.Add(n)
		return true
	}
	free := p.headroom()
	for {
		cur := p.reserved.Load()
		if free >= 0 && cur+n > free {
			return false
		}
		if p.reserved.CompareAndSwap(cur, cur+n) {
			return true
		}
	}
}

// scanReservation is the memory one cursor holds against its query's budget and the
// node's total. It lives in the prefetcher's item for the cursor and is released
// when the cursor's last reference goes.
type scanReservation struct {
	budget      *scanBudget
	cursor      *blockCursor
	reuse       *scanDecodeSlot
	failure     error
	bytes       int64
	sharedBytes int64
	released    atomic.Bool
}

// release returns the bytes. It is idempotent and safe after the scan is closed.
func (r *scanReservation) release() {
	if r.budget == nil || !r.released.CompareAndSwap(false, true) {
		return
	}
	r.cursor = nil
	r.budget.give(r.bytes)
	if r.budget.shared != nil && r.reuse == nil {
		r.budget.shared.Release(r.sharedBytes)
	}
}

type reserveOutcome uint8

const (
	reserveOK reserveOutcome = iota
	// reserveOverQuery means the query's own budget is exhausted.
	reserveOverQuery
	// reserveOverShared means the shared execution allowance is exhausted.
	reserveOverShared
	// reserveOverNode means the node's reservations leave no room.
	reserveOverNode
)

// scanBudget is one query's reservations. A query holds at most its quota at once;
// the node-wide total additionally keeps concurrent queries within the
// protector's headroom.
type scanBudget struct {
	tracker       *vectorized.MemoryTracker
	shared        *vectorized.MemoryTracker
	reuse         *scanDecodeReuse
	node          *decodePool
	held          atomic.Int64
	columns       uint64
	workspace     int64
	quota         int64
	active        atomic.Int64
	closing       atomic.Bool
	workspaceOnce sync.Once
}

// newScanBudget returns a budget of quota bytes (negative: no per-query limit)
// that also counts against node's total (nil: no node accounting).
func newScanBudget(quota int64, node *decodePool) *scanBudget {
	b := &scanBudget{quota: quota, node: node}
	if quota >= 0 {
		b.tracker = vectorized.NewMemoryTracker(quota)
	}
	return b
}

// tryReserve reserves size bytes into r, reporting why it could not.
func (b *scanBudget) tryReserve(size uint64, ahead bool, r *scanReservation) reserveOutcome {
	if size > math.MaxInt64 {
		return reserveOverQuery
	}
	n := int64(size)
	if b.tracker != nil && b.tracker.Reserve(n) != nil {
		return reserveOverQuery
	}
	if b.node != nil && !b.node.reserve(n, ahead) {
		if b.tracker != nil {
			b.tracker.Release(n)
		}
		return reserveOverNode
	}
	if b.shared != nil {
		amount := r.sharedBytes
		if amount == 0 {
			amount = n
		}
		var err error
		if b.reuse != nil && r.cursor != nil {
			r.reuse, err = b.reuse.acquire(r.cursor, amount)
		} else {
			err = b.shared.Reserve(amount)
		}
		if err != nil {
			r.failure = err
			if b.tracker != nil {
				b.tracker.Release(n)
			}
			if b.node != nil {
				b.node.reserved.Add(-n)
			}
			return reserveOverShared
		}
		r.sharedBytes = amount
	}
	b.held.Add(n)
	b.active.Add(1)
	r.failure = nil
	liveReservedBytes.Add(n)
	r.budget, r.bytes = b, n
	r.released.Store(false)
	if r.reuse != nil {
		r.cursor.res = r
	}
	return reserveOK
}

func (b *scanBudget) give(n int64) {
	b.held.Add(-n)
	b.active.Add(-1)
	b.finishWorkspace()
	liveReservedBytes.Add(-n)
	if b.tracker != nil {
		b.tracker.Release(n)
	}
	if b.node != nil {
		b.node.reserved.Add(-n)
	}
}

// reserveCurrent reserves what the scan needs to make progress. It fails, with the
// error the one-shot check used to return, only when the query alone would hold
// more than its quota.
//
// Progress rule: the wait on the protector (AcquireResource, which holds a slot
// while it backs off until process memory has room) is the admission of a query,
// so it happens only while the query holds nothing, before its first reservation.
// Once a query holds memory (a decoded frontier, borrowed batches, prefetched
// cursors) the current cursor never blocks on the protector: two queries that each
// waited for memory the other holds would stay stuck until their contexts end. The
// per-query quota still bounds what a query holds, and the node's headroom gates
// only the prefetch (decodePool.reserve).
func (b *scanBudget) reserveCurrent(ctx context.Context, size uint64, r *scanReservation) error {
	admit := b.held.Load() == 0
	outcome := b.tryReserve(size, false, r)
	if outcome == reserveOverShared {
		return r.failure
	}
	if outcome == reserveOverQuery {
		return fmt.Errorf("block scan quota exceeded: used %d bytes, quota is %d bytes", uint64(b.held.Load())+size, b.quota)
	}
	if admit && b.node != nil && b.node.pm != nil {
		if err := b.node.pm.AcquireResource(ctx, size); err != nil {
			r.release()
			return err
		}
	}
	return nil
}

// attachShared reserves bounded batch workspace before scan allocation and connects
// decoded cursor lifetimes to the same allowance used by operators and final egress.
func (b *scanBudget) attachShared(ctx context.Context, columns, batchRows, cursors int) error {
	tracker, ok := vectorized.QueryMemoryTrackerFromContext(ctx)
	if !ok {
		return nil
	}
	// Two storage slabs retain at most 2048 cells each; a copied source slab
	// needs 1024 cells. At 24 bytes per slice cell that is 122880 bytes per
	// column, below 2048*64. Lent source conversions instead retain 2048
	// 8-byte cells (114688 bytes total). The extra 128 bytes per row covers
	// metadata, validity and descriptors independently of projected columns.
	cellBytes := uint64(64)
	if batchRows > vectorized.DefaultBatchSize {
		// Larger configured source slabs need the general twofold-capacity bound.
		cellBytes = 128
	}
	perRow := uint64(columns)*cellBytes + 128
	rows := uint64(coalesceTargetRows + max(batchRows, vectorized.DefaultBatchSize))
	if rows > (math.MaxInt64-uint64(cursors)*512)/perRow {
		return vectorized.ErrMemoryBudgetExceeded
	}
	// A query cannot decode more cursors concurrently than it actually owns.
	workers := 1
	if b.node != nil {
		workers = min(max(b.node.workers, 1), max(cursors, 1), prefetchMaxAheadMerge)
	}
	scratch := uint64(workers) * 256 * 1024
	if rows*perRow > math.MaxInt64-scratch-uint64(cursors)*512 {
		return vectorized.ErrMemoryBudgetExceeded
	}
	workspace := int64(rows*perRow + uint64(cursors)*512 + scratch)
	if err := tracker.Reserve(workspace); err != nil {
		return err
	}
	b.shared = tracker
	b.columns = uint64(columns)
	b.workspace = workspace
	return nil
}

func (b *scanBudget) releaseWorkspace() {
	if b == nil {
		return
	}
	b.closing.Store(true)
	if b.reuse != nil {
		b.reuse.close()
	}
	b.finishWorkspace()
}

func (b *scanBudget) finishWorkspace() {
	if b.shared == nil || !b.closing.Load() || b.active.Load() != 0 {
		return
	}
	b.workspaceOnce.Do(func() { b.shared.Release(b.workspace); b.workspace = 0 })
}

// sharedBlockBytes bounds decode expansion and native batch copies before decoding.
// Borrowed batches keep the cursor reservation alive through their owner pins.
func (b *scanBudget) sharedBlockBytes(bc *blockCursor) int64 {
	if b.shared == nil {
		return 0
	}
	perRow := b.columns*256 + 256
	rows := max(bc.bm.count, uint64(16))
	if rows > math.MaxInt64/perRow {
		return math.MaxInt64
	}
	fixed := rows * perRow
	if bc.bm.uncompressedSizeBytes > (math.MaxInt64-fixed)/16 {
		return math.MaxInt64
	}
	return int64(fixed + bc.bm.uncompressedSizeBytes*16)
}
