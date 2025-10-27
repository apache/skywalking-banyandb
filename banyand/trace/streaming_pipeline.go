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
	"container/heap"
	"context"
	stdErrors "errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"

	pkgerrors "github.com/pkg/errors"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/sidx"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/query"
)

type traceBatch struct {
	err      error
	keys     map[string]int64
	traceIDs []string
	seq      int
}

type scanBatch struct {
	err       error
	cursorCh  <-chan scanCursorResult
	snapshots []*snapshot
	traceBatch
}

func newTraceBatch(seq int, capacity int) traceBatch {
	tb := traceBatch{
		seq:      seq,
		traceIDs: make([]string, 0, capacity),
	}
	if capacity > 0 {
		tb.keys = make(map[string]int64, capacity)
	}
	return tb
}

func staticTraceBatchSource(ctx context.Context, traceIDs []string, maxTraceSize int, keys map[string]int64) <-chan traceBatch {
	out := make(chan traceBatch)

	go func() {
		defer close(out)

		if len(traceIDs) == 0 {
			return
		}

		limit := len(traceIDs)
		if maxTraceSize > 0 && maxTraceSize < limit {
			limit = maxTraceSize
		}

		orderedIDs := append([]string(nil), traceIDs[:limit]...)

		// Determine batch size. When maxTraceSize is zero, emit everything in one batch.
		batchSize := maxTraceSize
		if batchSize <= 0 || batchSize > len(orderedIDs) {
			batchSize = len(orderedIDs)
		}

		seq := 0
		for start := 0; start < len(orderedIDs); start += batchSize {
			end := start + batchSize
			if end > len(orderedIDs) {
				end = len(orderedIDs)
			}

			select {
			case <-ctx.Done():
				return
			case out <- traceBatch{
				seq:      seq,
				traceIDs: append([]string(nil), orderedIDs[start:end]...),
				keys:     keys,
			}:
				seq++
			}
		}
	}()

	return out
}

const defaultTraceBatchSize = 64

type sidxStreamShard struct {
	results  <-chan *sidx.QueryResponse
	response *sidx.QueryResponse
	id       int
	idx      int
	done     bool
}

func (sh *sidxStreamShard) prepare(ctx context.Context) error {
	for {
		if sh.response != nil && sh.idx >= 0 && sh.idx < sh.response.Len() {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case resp, ok := <-sh.results:
			if !ok {
				sh.done = true
				sh.response = nil
				return nil
			}
			if resp == nil {
				continue
			}
			if resp.Error != nil {
				return resp.Error
			}
			if resp.Len() == 0 {
				continue
			}
			sh.response = resp
			sh.idx = 0
			return nil
		}
	}
}

func (sh *sidxStreamShard) currentKey() int64 {
	return sh.response.Keys[sh.idx]
}

func (sh *sidxStreamShard) currentData() []byte {
	return sh.response.Data[sh.idx]
}

func (sh *sidxStreamShard) advance(ctx context.Context) error {
	if sh.response == nil {
		return sh.prepare(ctx)
	}

	sh.idx++

	return sh.prepare(ctx)
}

type sidxStreamHeap struct {
	shards []*sidxStreamShard
	asc    bool
}

func (h sidxStreamHeap) Len() int {
	return len(h.shards)
}

func (h sidxStreamHeap) Less(i, j int) bool {
	left := h.shards[i].currentKey()
	right := h.shards[j].currentKey()
	if h.asc {
		return left < right
	}
	return left > right
}

func (h sidxStreamHeap) Swap(i, j int) {
	h.shards[i], h.shards[j] = h.shards[j], h.shards[i]
}

func (h *sidxStreamHeap) Push(x interface{}) {
	h.shards = append(h.shards, x.(*sidxStreamShard))
}

func (h *sidxStreamHeap) Pop() interface{} {
	n := len(h.shards)
	x := h.shards[n-1]
	h.shards = h.shards[:n-1]
	return x
}

type sidxStreamError struct {
	err   error
	index int
}

func decodeTraceID(data []byte) (string, error) {
	if len(data) == 0 {
		return "", fmt.Errorf("empty trace id payload")
	}
	if idFormat(data[0]) != idFormatV1 {
		return "", fmt.Errorf("invalid trace id format: %x", data[0])
	}
	return string(data[1:]), nil
}

func forwardSIDXError(ctx context.Context, idx int, errCh <-chan error, out chan<- sidxStreamError) {
	if errCh == nil {
		return
	}
	for err := range errCh {
		if err == nil {
			continue
		}
		select {
		case out <- sidxStreamError{index: idx, err: err}:
		case <-ctx.Done():
			select {
			case out <- sidxStreamError{index: idx, err: err}:
			default:
			}
		}
		return
	}
}

func (t *trace) streamSIDXTraceBatches(
	ctx context.Context,
	sidxInstances []sidx.SIDX,
	req sidx.QueryRequest,
	maxTraceSize int,
) <-chan traceBatch {
	out := make(chan traceBatch)

	if len(sidxInstances) == 0 {
		close(out)
		return out
	}

	tracer := query.GetTracer(ctx)
	tracingCtx := ctx
	var span *query.Span
	if tracer != nil {
		span, tracingCtx = tracer.StartSpan(ctx, "sidx-stream")
		tagSIDXStreamSpan(span, req, maxTraceSize, len(sidxInstances))
	}
	streamCtx, cancel := context.WithCancel(tracingCtx)
	runner := newSIDXStreamRunner(ctx, streamCtx, cancel, req, maxTraceSize)
	go func() {
		defer func() {
			if span != nil {
				span.Tagf("batches_emitted", "%d", runner.batchesEmitted.Load())
				span.Tagf("trace_ids_emitted", "%d", runner.total.Load())
				if dups := runner.duplicates.Load(); dups > 0 {
					span.Tagf("duplicate_trace_ids", "%d", dups)
				}
				if err := runner.getSpanErr(); err != nil && !stdErrors.Is(err, context.Canceled) {
					span.Error(err)
				}
				span.Stop()
			}
			cancel()
			close(out)
		}()

		if err := runner.prepare(sidxInstances); err != nil {
			runner.cancel()
			runner.emitError(out, err)
			return
		}

		runner.run(out)
	}()
	return out
}

type sidxStreamRunner struct {
	streamCtx      context.Context
	ctx            context.Context
	spanErr        atomic.Value // stores error
	cancelFunc     context.CancelFunc
	errEvents      chan sidxStreamError
	heap           *sidxStreamHeap
	seenTraceIDs   map[string]struct{}
	req            sidx.QueryRequest
	batch          traceBatch
	errWg          sync.WaitGroup
	maxTraceSize   int
	nextSeq        int
	total          atomic.Int64
	batchesEmitted atomic.Int64
	duplicates     atomic.Int64
	batchSize      int
}

func tagSIDXStreamSpan(span *query.Span, req sidx.QueryRequest, maxTraceSize int, instanceCount int) {
	if span == nil {
		return
	}
	if req.Filter != nil {
		span.Tag("filter_present", "true")
	} else {
		span.Tag("filter_present", "false")
	}
	if req.Order != nil {
		if req.Order.Index != nil && req.Order.Index.GetMetadata() != nil {
			span.Tag("order_index", req.Order.Index.GetMetadata().GetName())
		}
		span.Tag("order_sort", req.Order.Sort.String())
		span.Tagf("order_type", "%d", req.Order.Type)
	} else {
		span.Tag("order_sort", "none")
	}
	span.Tagf("series_id_candidates", "%d", len(req.SeriesIDs))
	span.Tagf("max_batch_size", "%d", req.MaxBatchSize)
	span.Tagf("max_trace_size", "%d", maxTraceSize)
	span.Tagf("sidx_instance_count", "%d", instanceCount)
}

func newSIDXStreamRunner(
	ctx context.Context,
	streamCtx context.Context,
	cancel context.CancelFunc,
	req sidx.QueryRequest,
	maxTraceSize int,
) *sidxStreamRunner {
	asc := true
	if req.Order != nil && req.Order.Sort == modelv1.Sort_SORT_DESC {
		asc = false
	}

	batchSize := req.MaxBatchSize
	if batchSize <= 0 {
		if maxTraceSize > 0 {
			batchSize = maxTraceSize
		} else {
			batchSize = defaultTraceBatchSize
		}
	}

	return &sidxStreamRunner{
		ctx:          ctx,
		streamCtx:    streamCtx,
		cancelFunc:   cancel,
		req:          req,
		maxTraceSize: maxTraceSize,
		batchSize:    batchSize,
		heap:         &sidxStreamHeap{asc: asc},
		seenTraceIDs: make(map[string]struct{}),
		batch:        newTraceBatch(0, batchSize),
		nextSeq:      1,
	}
}

func (r *sidxStreamRunner) prepare(instances []sidx.SIDX) error {
	type shardSource struct {
		shard *sidxStreamShard
		errCh <-chan error
		idx   int
	}

	// Track all error channels separately from shards with data
	type errChannelInfo struct {
		errCh <-chan error
		idx   int
	}
	allErrChannels := make([]errChannelInfo, 0, len(instances))

	sources := make([]shardSource, 0, len(instances))
	for idx, instance := range instances {
		resultsCh, errCh := instance.StreamingQuery(r.streamCtx, r.req)

		// Track error channel regardless of whether shard has data
		if errCh != nil {
			allErrChannels = append(allErrChannels, errChannelInfo{errCh: errCh, idx: idx})
		}

		shard := &sidxStreamShard{
			id:      idx,
			results: resultsCh,
		}
		if err := shard.prepare(r.streamCtx); err != nil {
			return fmt.Errorf("sidx[%d] prepare failed: %w", idx, err)
		}
		if shard.done {
			// Shard has no data, but we still track its error channel above
			continue
		}
		sources = append(sources, shardSource{
			shard: shard,
			errCh: errCh,
			idx:   idx,
		})
	}

	// Add shards with data to the heap
	for _, src := range sources {
		heap.Push(r.heap, src.shard)
	}

	// Create error event channel and start error forwarding goroutines
	// for ALL error channels, even from shards without data
	if len(allErrChannels) > 0 {
		r.errEvents = make(chan sidxStreamError, len(allErrChannels))

		for _, errSrc := range allErrChannels {
			r.errWg.Add(1)
			go func(index int, ch <-chan error) {
				defer r.errWg.Done()
				forwardSIDXError(r.streamCtx, index, ch, r.errEvents)
			}(errSrc.idx, errSrc.errCh)
		}

		go func() {
			r.errWg.Wait()
			close(r.errEvents)
		}()
	}

	return nil
}

func (r *sidxStreamRunner) run(out chan<- traceBatch) {
	// Always drain error events before returning, even on early exit
	// Cancel first to stop SIDX goroutines, then drain any pending errors
	defer func() {
		r.cancel()
		r.drainErrorEvents(out)
	}()

	if r.heap.Len() == 0 {
		return
	}

	for r.heap.Len() > 0 {
		if err := r.streamCtx.Err(); err != nil {
			r.recordSpanErr(err)
			return
		}

		if !r.pollErrEvents(out) {
			return
		}

		shard := heap.Pop(r.heap).(*sidxStreamShard)
		if err := r.ensureShardReady(shard); err != nil {
			r.emitError(out, err)
			return
		}
		if shard.done {
			continue
		}

		added, err := r.consumeShard(shard)
		if err != nil {
			r.emitError(out, err)
			return
		}

		if added && len(r.batch.traceIDs) >= r.batchSize {
			if !r.emitBatch(out) {
				return
			}
		}

		if err := r.advanceShard(shard); err != nil {
			r.emitError(out, err)
			return
		}
	}

	if len(r.batch.traceIDs) > 0 {
		if !r.emitBatch(out) {
			return
		}
	}
}

func (r *sidxStreamRunner) pollErrEvents(out chan<- traceBatch) bool {
	if r.errEvents == nil {
		return true
	}

	// Check for errors without blocking
	select {
	case ev, ok := <-r.errEvents:
		if !ok {
			r.errEvents = nil
			return true
		}
		if ev.err == nil {
			return true
		}
		eventErr := fmt.Errorf("sidx[%d] streaming error: %w", ev.index, ev.err)
		r.emitError(out, eventErr)
		return false
	default:
		// No error available yet, continue processing
		return true
	}
}

func (r *sidxStreamRunner) ensureShardReady(shard *sidxStreamShard) error {
	if shard.response == nil {
		if err := shard.prepare(r.streamCtx); err != nil {
			return fmt.Errorf("sidx[%d] prepare failed: %w", shard.id, err)
		}
		if shard.done {
			return nil
		}
	}
	return nil
}

func (r *sidxStreamRunner) consumeShard(shard *sidxStreamShard) (bool, error) {
	traceID, err := decodeTraceID(shard.currentData())
	if err != nil {
		return false, fmt.Errorf("sidx[%d] invalid trace id payload: %w", shard.id, err)
	}

	if _, exists := r.seenTraceIDs[traceID]; exists {
		r.duplicates.Add(1)
		return false, nil
	}

	r.seenTraceIDs[traceID] = struct{}{}
	r.batch.traceIDs = append(r.batch.traceIDs, traceID)
	if r.batch.keys == nil {
		r.batch.keys = make(map[string]int64)
	}
	r.batch.keys[traceID] = shard.currentKey()
	r.total.Add(1)

	return true, nil
}

func (r *sidxStreamRunner) emitBatch(out chan<- traceBatch) bool {
	select {
	case <-r.streamCtx.Done():
		r.recordSpanErr(r.streamCtx.Err())
		return false
	case out <- r.batch:
		r.batchesEmitted.Add(1)
		r.batch = newTraceBatch(r.nextSeq, r.batchSize)
		r.nextSeq++
		return true
	}
}

func (r *sidxStreamRunner) advanceShard(shard *sidxStreamShard) error {
	if err := shard.advance(r.streamCtx); err != nil {
		return fmt.Errorf("sidx[%d] advance failed: %w", shard.id, err)
	}
	if !shard.done {
		heap.Push(r.heap, shard)
	}
	return nil
}

func (r *sidxStreamRunner) emitError(out chan<- traceBatch, err error) {
	r.recordSpanErr(err)
	// Always try to send error even if context is canceled
	// The receiver needs to know about the error
	select {
	case out <- traceBatch{err: err}:
	case <-r.ctx.Done():
		// Context canceled, but still try to send error with non-blocking attempt
		select {
		case out <- traceBatch{err: err}:
		default:
			// Channel might be blocked or closed, can't send error
		}
	}
}

func (r *sidxStreamRunner) drainErrorEvents(out chan<- traceBatch) {
	if r.errEvents == nil {
		return
	}

	// Read from errEvents until it's closed or we find an error
	// The channel will be closed after all error forwarding goroutines finish
	for {
		select {
		case ev, ok := <-r.errEvents:
			if !ok {
				// Channel closed, all error forwarding goroutines finished
				return
			}
			if ev.err == nil {
				// No error in this event, continue reading
				continue
			}
			// Skip context.Canceled errors if we're shutting down normally
			// (these are expected cleanup errors, not actual failures)
			if stdErrors.Is(ev.err, context.Canceled) {
				continue
			}
			// Found a real error, emit it and return
			eventErr := fmt.Errorf("sidx[%d] streaming error: %w", ev.index, ev.err)
			r.emitError(out, eventErr)
			return
		case <-r.ctx.Done():
			// Context canceled, but still do non-blocking drain
			// to catch any errors that arrived before cancellation
			for {
				select {
				case ev, ok := <-r.errEvents:
					if !ok {
						return
					}
					if ev.err != nil && !stdErrors.Is(ev.err, context.Canceled) {
						eventErr := fmt.Errorf("sidx[%d] streaming error: %w", ev.index, ev.err)
						r.emitError(out, eventErr)
						return
					}
				default:
					// No more errors available
					r.recordSpanErr(r.ctx.Err())
					return
				}
			}
		}
	}
}

func (r *sidxStreamRunner) recordSpanErr(err error) {
	// Only store the first error
	if r.spanErr.Load() == nil {
		r.spanErr.Store(err)
	}
}

func (r *sidxStreamRunner) getSpanErr() error {
	if v := r.spanErr.Load(); v != nil {
		return v.(error)
	}
	return nil
}

func (r *sidxStreamRunner) cancel() {
	if r.cancelFunc != nil {
		r.cancelFunc()
	}
}

func (t *trace) startBlockScanStage(
	ctx context.Context,
	tables []*tsTable,
	qo queryOptions,
	batches <-chan traceBatch,
) <-chan *scanBatch {
	out := make(chan *scanBatch)

	go func() {
		defer close(out)

		for batch := range batches {
			if batch.err != nil {
				select {
				case <-ctx.Done():
					return
				case out <- &scanBatch{traceBatch: batch, err: batch.err}:
				}
				continue
			}

			// Acquire snapshots from all tables
			snapshots := make([]*snapshot, 0, len(tables))
			for _, table := range tables {
				s := table.currentSnapshot()
				if s == nil {
					continue
				}
				snapshots = append(snapshots, s)
			}

			// If no snapshots available, skip this batch
			if len(snapshots) == 0 {
				select {
				case <-ctx.Done():
					return
				case out <- &scanBatch{traceBatch: batch}:
				}
				continue
			}

			// Filter parts from snapshots based on batch.traceIDs using bloom filters
			parts := make([]*part, 0)
			for _, s := range snapshots {
				for _, pw := range s.parts {
					p := pw.p
					// Check bloom filter for traceIDs
					if !shouldScanPart(p, batch.traceIDs) {
						continue
					}
					parts = append(parts, p)
				}
			}

			// Create the cursor channel and scanBatch
			cursorCh := make(chan scanCursorResult)
			sb := &scanBatch{
				traceBatch: batch,
				cursorCh:   cursorCh,
				snapshots:  snapshots,
			}

			// Send batch downstream first so consumer can start reading
			select {
			case <-ctx.Done():
				close(cursorCh)
				// Release snapshots on early exit
				for _, s := range snapshots {
					s.decRef()
				}
				return
			case out <- sb:
			}

			// Now scan inline and populate the channel
			t.scanTraceIDsInline(ctx, parts, qo, batch.traceIDs, cursorCh)
			close(cursorCh)
		}
	}()

	return out
}

type scanCursorResult struct {
	cursor *blockCursor
	err    error
}

// shouldScanPart checks if a part should be scanned for the given traceIDs using bloom filter.
// Returns true if bloom filter is nil OR any traceID might be in the part.
// Returns false if bloom filter confirms no traceIDs exist in the part.
func shouldScanPart(p *part, traceIDs []string) bool {
	if p.traceIDFilter.filter == nil || len(traceIDs) == 0 {
		return true
	}
	for _, traceID := range traceIDs {
		if p.traceIDFilter.filter.MightContain(convert.StringToBytes(traceID)) {
			return true
		}
	}
	return false
}

func (t *trace) scanTraceIDsInline(ctx context.Context, parts []*part, qo queryOptions, traceIDs []string, out chan<- scanCursorResult) {
	if len(parts) == 0 || len(traceIDs) == 0 {
		return
	}

	sortedIDs := append([]string(nil), traceIDs...)
	sort.Strings(sortedIDs)

	// Track part selection performance
	ctx, finishPartSelection := startPartSelectionSpan(ctx, len(parts), len(sortedIDs))

	// Group trace IDs by part using bloom filters
	groupedIDs := make([][]string, len(parts))
	totalGroupedIDs := 0
	for _, traceID := range sortedIDs {
		for i, p := range parts {
			if p.traceIDFilter.filter == nil || p.traceIDFilter.filter.MightContain(convert.StringToBytes(traceID)) {
				groupedIDs[i] = append(groupedIDs[i], traceID)
				totalGroupedIDs++
			}
		}
	}

	// Compact parts and groupedIDs in-place to remove parts with no matching trace IDs
	partsBeforeCompact := len(parts)
	writeIdx := 0
	for i := range parts {
		if len(groupedIDs[i]) > 0 {
			parts[writeIdx] = parts[i]
			groupedIDs[writeIdx] = groupedIDs[i]
			writeIdx++
		}
	}
	parts = parts[:writeIdx]
	groupedIDs = groupedIDs[:writeIdx]

	if len(parts) == 0 {
		finishPartSelection(&partSelectionMetrics{
			partsBeforeCompact: partsBeforeCompact,
		}, 0)
		return
	}

	finishPartSelection(&partSelectionMetrics{
		totalGroupedIDs:    totalGroupedIDs,
		partsBeforeCompact: partsBeforeCompact,
	}, len(parts))

	recordBlock, finishSpan := startAggregatedBlockScanSpan(ctx, sortedIDs, parts)

	var (
		spanErr        error
		spanBlockBytes uint64
		cursorCount    int
	)

	defer func() {
		finishSpan(cursorCount, spanBlockBytes, spanErr)
	}()

	bma := generateBlockMetadataArray()
	defer releaseBlockMetadataArray(bma)

	tstIter := generateTstIter()
	defer releaseTstIter(tstIter)

	tstIter.init(bma, parts, groupedIDs)
	if initErr := tstIter.Error(); initErr != nil {
		spanErr = fmt.Errorf("cannot init tstIter: %w", initErr)
		select {
		case out <- scanCursorResult{err: spanErr}:
		case <-ctx.Done():
		}
		return
	}

	quota := t.pm.AvailableBytes()
	hit := 0

	for tstIter.nextBlock() {
		if hit%checkDoneEvery == 0 {
			select {
			case <-ctx.Done():
				spanErr = pkgerrors.WithMessagef(ctx.Err(), "interrupt: scanned %d blocks, remained %d/%d parts to scan",
					cursorCount, len(tstIter.piPool)-tstIter.idx, len(tstIter.piPool))
				return
			default:
			}
		}
		hit++

		// Create block cursor and get size before checking quota
		bc := generateBlockCursor()
		p := tstIter.piPool[tstIter.idx]
		bc.init(p.p, p.curBlock, qo)
		blockSize := bc.bm.uncompressedSpanSizeBytes

		// Check if adding this block would exceed quota
		if quota >= 0 && spanBlockBytes+blockSize > uint64(quota) {
			releaseBlockCursor(bc)
			if cursorCount > 0 {
				// Have results, return them successfully by just closing channel
				return
			}
			// No results, send error
			spanErr = fmt.Errorf("block scan quota exceeded: block size %d bytes, quota is %d bytes", blockSize, quota)
			select {
			case out <- scanCursorResult{err: spanErr}:
			case <-ctx.Done():
			}
			return
		}

		// Quota OK, send cursor
		if recordBlock != nil {
			recordBlock(bc, blockSize)
		}
		spanBlockBytes += blockSize
		cursorCount++

		select {
		case out <- scanCursorResult{cursor: bc}:
		case <-ctx.Done():
			releaseBlockCursor(bc)
			spanErr = pkgerrors.WithMessagef(ctx.Err(), "interrupt: scanned %d blocks", cursorCount)
			return
		}
	}

	if iterErr := tstIter.Error(); iterErr != nil {
		spanErr = fmt.Errorf("cannot iterate tstIter: %w", iterErr)
		select {
		case out <- scanCursorResult{err: spanErr}:
		case <-ctx.Done():
		}
		return
	}
}
