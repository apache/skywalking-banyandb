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
	queued := time.Since(t.enqueued)
	if t.qm != nil {
		t.qm.scanDecodeQueueWait.Observe(queued.Seconds())
	}
	if res := t.bc.res; res != nil && res.budget != nil {
		res.budget.telemetry().queueNS.Add(queued.Nanoseconds())
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

// trueUp shrinks the cursor's shared reservation from the pre-decode worst case to
// what the decoded cursor keeps: the rows inside the query range, at the per-row
// envelope, plus twice the block's encoded bytes for the decode backing it retains.
// The expansion of the whole block, which was transient and is gone once the rows are
// copied, is handed back, so many cursors can be live at once (a merge over a series
// per endpoint) without each of them holding that expansion until it is released. It
// only ever lowers the reservation, and does nothing for a cursor with a reusable slot.
func (r *scanReservation) trueUp(rows int, blockRows uint64) {
	b := r.budget
	if b == nil || b.shared == nil || r.reuse != nil || r.released.Load() || r.sharedBytes <= 0 {
		return
	}
	kept := sharedRetainedBytes(b.perRowBytes(), uint64(rows), blockRows, uint64(r.bytes))
	if kept >= r.sharedBytes {
		return
	}
	b.shared.Release(r.sharedBytes - kept)
	r.sharedBytes = kept
}

// The shared envelope of a cursor is calibrated against measured decodes
// (TestCalibrateSharedEnvelope: a cluster-shaped one-tag block, a constant-valued
// block, and a wide 7-tag/5-field block with arrays, binary and nulls, at 4%-100% of the
// block in range). A live decoded cursor holds about a decoder-backing floor, its
// encoded block size once, and 10-50 bytes per row and column. The constants below give
// at least 1.4x the retained size and 2.0x the per-decode allocation measured in every
// one of those cases, so the figures are an upper bound with margin, not a mean. The per-row cost
// follows the projected column types (projectedRowBytes) rather than their number.
const (
	// cursorFloorBytes is the decoder backing a cursor keeps however few rows it holds.
	cursorFloorBytes uint64 = 32 << 10
	// cursorRowColumnBytes is what one row of one column keeps when the column types are unknown
	// (the 4 base columns count).
	cursorRowColumnBytes uint64 = 48
	// cursorRowFactor is the safety factor on projectedRowBytes.
	cursorRowFactor uint64 = 2
	// decodeTransientFactor bounds the garbage one decode allocates, as a multiple of the
	// block's encoded size; the worst measured is 5x (arrays and binary on a wide schema).
	decodeTransientFactor uint64 = 6
)

// compactedFloorBytes is what a compacted cursor (see blockCursor.compactValues) keeps however few
// rows it holds.
const compactedFloorBytes uint64 = 4 << 10

// sharedRetainedBytes is what a decoded cursor of rows in-range rows, out of a block of count
// rows and encodedBytes bytes, keeps while it is live. A cursor with under half the block in
// range is compacted: it owns only its rows' bytes, estimated as their share of the block.
// Any other cursor still views the decoder's whole backing array.
func sharedRetainedBytes(perRow, rows, count, encodedBytes uint64) int64 {
	rows = max(rows, 16)
	if perRow == 0 || rows > math.MaxInt64/perRow || encodedBytes > math.MaxInt64/4-rows*perRow-cursorFloorBytes {
		return math.MaxInt64
	}
	if count > 0 && rows*2 < count {
		return int64(compactedFloorBytes + encodedBytes*rows/count + rows*perRow)
	}
	return int64(cursorFloorBytes + encodedBytes + rows*perRow)
}

// perRowBytes is what one in-range row of one cursor keeps, from the projected columns' types when the
// scan knows them and from their count otherwise.
func (b *scanBudget) perRowBytes() uint64 {
	if b.rowBytes > 0 {
		return b.rowBytes * cursorRowFactor
	}
	return b.columns * cursorRowColumnBytes
}

// projectedRowBytes is what a row of the projected columns costs in the decoded cursor before the
// safety factor: the row's timestamp and version, then per column the value itself for a fixed-width
// type or a slice header for a string, binary or array (their bytes are covered by the encoded size).
func projectedRowBytes(schema *vectorized.BatchSchema) uint64 {
	if schema == nil {
		return 0
	}
	n := uint64(16)
	for _, col := range schema.Columns {
		if col.Role != vectorized.RoleTag && col.Role != vectorized.RoleField {
			continue
		}
		switch col.Type {
		case vectorized.ColumnTypeInt64, vectorized.ColumnTypeFloat64:
			n += 8
		default:
			n += 24
		}
	}
	return n
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
	tel       *scanTelemetry
	shared    *vectorized.MemoryTracker
	reuse     *scanDecodeReuse
	node      *decodePool
	tracker   *vectorized.MemoryTracker
	qm        *queryMetrics
	held      atomic.Int64
	workspace int64
	rowBytes  uint64
	columns   uint64
	// sliceNeed is what a time-ordered merge may keep live before it is cut into time slices, and
	// sliceBudget what one slice may keep live; zero disables slicing (see sliceMergeUnit).
	sliceNeed     int64
	sliceBudget   int64
	quota         int64
	active        atomic.Int64
	workspaceOnce sync.Once
	closing       atomic.Bool
}

// newScanBudget returns a budget of quota bytes (negative: no per-query limit)
// that also counts against node's total (nil: no node accounting).
func newScanBudget(quota int64, node *decodePool) *scanBudget {
	b := &scanBudget{quota: quota, node: node, tel: new(scanTelemetry)}
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
		return b.rejected(r.failure, rejectOther)
	}
	if outcome == reserveOverQuery {
		return b.rejected(fmt.Errorf("block scan quota exceeded: used %d bytes, quota is %d bytes", uint64(b.held.Load())+size, b.quota), rejectRawQuota)
	}
	if admit && b.node != nil && b.node.pm != nil {
		if err := b.node.pm.AcquireResource(ctx, size); err != nil {
			r.release()
			return err
		}
	}
	return nil
}

// rejected records a reservation the scan needed and could not get, then returns the error.
func (b *scanBudget) rejected(err error, fallback string) error {
	reason := b.telemetry().reject(err, fallback)
	if b.qm != nil {
		b.qm.scanReservationRejections.Inc(1, reason)
	}
	return err
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
	b.sliceNeed, b.sliceBudget = scanSliceBudget(ctx, tracker, workspace)
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

// inRangeRows estimates how many rows of the block fall in the query time range, by
// assuming the block's rows are spread evenly between its first and last timestamp.
// Only those rows are copied into batches; the rest of a decoded block is transient and
// covered by the uncompressed-size term of sharedBlockBytes. A block of a merged part
// spans hours, so charging all its rows to a query for minutes overstates it manyfold.
func inRangeRows(bc *blockCursor) uint64 {
	count := bc.bm.count
	lo, hi := bc.bm.timestamps.min, bc.bm.timestamps.max
	if bc.maxTimestamp == 0 || hi <= lo || count <= 1 {
		return count
	}
	from, to := max(lo, bc.minTimestamp), min(hi, bc.maxTimestamp)
	if to < from {
		return 0
	}
	span := uint64(hi-lo) + 1
	overlap := uint64(to-from) + 1
	if overlap >= span {
		return count
	}
	// Scale in float64: count*overlap can overflow uint64 for nanosecond spans.
	return min(count, uint64(math.Ceil(float64(count)*float64(overlap)/float64(span))))
}

// sharedBlockBytes bounds decode expansion and native batch copies before decoding.
// Borrowed batches keep the cursor reservation alive through their owner pins.
func (b *scanBudget) sharedBlockBytes(bc *blockCursor) int64 {
	if b.shared == nil {
		return 0
	}
	kept := sharedRetainedBytes(b.perRowBytes(), inRangeRows(bc), bc.bm.count, bc.bm.uncompressedSizeBytes)
	if bc.bm.uncompressedSizeBytes > uint64(math.MaxInt64-kept)/decodeTransientFactor {
		return math.MaxInt64
	}
	return kept + int64(bc.bm.uncompressedSizeBytes*decodeTransientFactor)
}
