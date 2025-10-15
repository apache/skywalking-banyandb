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

	pkgerrors "github.com/pkg/errors"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/sidx"
	"github.com/apache/skywalking-banyandb/pkg/cgroups"
	"github.com/apache/skywalking-banyandb/pkg/query"
)

type traceBatch struct {
	err      error
	keys     map[string]int64
	traceIDs []string
	seq      int
}

type scanBatch struct {
	err     error
	cursors []*blockCursor
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
		var spanCtx context.Context
		span, spanCtx = tracer.StartSpan(ctx, "sidx-stream")
		tracingCtx = spanCtx
		tagSIDXStreamSpan(span, req, maxTraceSize, len(sidxInstances))
	}

	streamCtx, cancel := context.WithCancel(tracingCtx)
	runner := newSIDXStreamRunner(ctx, streamCtx, cancel, req, maxTraceSize, span)

	go func() {
		defer close(out)
		defer cancel()

		if err := runner.prepare(sidxInstances); err != nil {
			runner.cancel()
			runner.emitError(out, err)
			runner.finish()
			return
		}

		runner.run(out)
		runner.finish()
	}()

	return out
}

type sidxStreamRunner struct {
	streamCtx      context.Context
	ctx            context.Context
	spanErr        error
	cancelFunc     context.CancelFunc
	span           *query.Span
	errEvents      chan sidxStreamError
	heap           *sidxStreamHeap
	seenTraceIDs   map[string]struct{}
	req            sidx.QueryRequest
	batch          traceBatch
	errWg          sync.WaitGroup
	maxTraceSize   int
	nextSeq        int
	total          int
	batchesEmitted int
	duplicates     int
	batchSize      int
	limitReached   bool
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
	span.Tagf("max_element_size", "%d", req.MaxElementSize)
	span.Tagf("max_trace_size", "%d", maxTraceSize)
	span.Tagf("sidx_instance_count", "%d", instanceCount)
}

func newSIDXStreamRunner(
	ctx context.Context,
	streamCtx context.Context,
	cancel context.CancelFunc,
	req sidx.QueryRequest,
	maxTraceSize int,
	span *query.Span,
) *sidxStreamRunner {
	asc := true
	if req.Order != nil && req.Order.Sort == modelv1.Sort_SORT_DESC {
		asc = false
	}

	batchSize := req.MaxElementSize
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
		span:         span,
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
	// Drain BEFORE cancel to give error forwarding goroutines a chance
	defer func() {
		r.drainErrorEvents(out)
		r.cancel()
	}()

	if r.heap.Len() == 0 {
		return
	}

	for r.heap.Len() > 0 {
		if r.maxTraceSize > 0 && r.total >= r.maxTraceSize {
			r.limitReached = true
			break
		}

		if err := r.streamCtx.Err(); err != nil {
			if r.spanErr == nil {
				r.spanErr = err
			}
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

		if added && r.maxTraceSize > 0 && r.total >= r.maxTraceSize {
			r.limitReached = true
			break
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
		r.duplicates++
		return false, nil
	}

	r.seenTraceIDs[traceID] = struct{}{}
	r.batch.traceIDs = append(r.batch.traceIDs, traceID)
	if r.batch.keys == nil {
		r.batch.keys = make(map[string]int64)
	}
	r.batch.keys[traceID] = shard.currentKey()
	r.total++

	return true, nil
}

func (r *sidxStreamRunner) emitBatch(out chan<- traceBatch) bool {
	select {
	case <-r.streamCtx.Done():
		if r.spanErr == nil {
			r.spanErr = r.streamCtx.Err()
		}
		return false
	case out <- r.batch:
		r.batchesEmitted++
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
			// Found an error, emit it and return
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
					if ev.err != nil {
						eventErr := fmt.Errorf("sidx[%d] streaming error: %w", ev.index, ev.err)
						r.emitError(out, eventErr)
						return
					}
				default:
					// No more errors available
					if r.spanErr == nil {
						r.spanErr = r.ctx.Err()
					}
					return
				}
			}
		}
	}
}

func (r *sidxStreamRunner) recordSpanErr(err error) {
	if r.spanErr == nil {
		r.spanErr = err
	}
}

func (r *sidxStreamRunner) cancel() {
	if r.cancelFunc != nil {
		r.cancelFunc()
	}
}

func (r *sidxStreamRunner) finish() {
	r.finishSpan()
}

func (r *sidxStreamRunner) finishSpan() {
	if r.span == nil {
		return
	}
	r.span.Tagf("batches_emitted", "%d", r.batchesEmitted)
	r.span.Tagf("trace_ids_emitted", "%d", r.total)
	if r.duplicates > 0 {
		r.span.Tagf("duplicate_trace_ids", "%d", r.duplicates)
	}
	if r.limitReached {
		r.span.Tag("max_trace_limit_hit", "true")
	}
	if r.spanErr != nil && !stdErrors.Is(r.spanErr, context.Canceled) {
		r.span.Error(r.spanErr)
	}
	r.span.Stop()
}

func (t *trace) startBlockScanStage(
	ctx context.Context,
	parts []*part,
	qo queryOptions,
	batches <-chan traceBatch,
	maxTraceSize int,
) <-chan *scanBatch {
	out := make(chan *scanBatch)

	workerCount := cgroups.CPUs()
	if workerCount < 1 {
		workerCount = 1
	}

	jobCh := make(chan traceBatch)
	resultCh := make(chan *scanBatch)

	var workerGroup sync.WaitGroup
	workerGroup.Add(workerCount)

	t.launchBlockScanWorkers(ctx, parts, qo, workerCount, jobCh, resultCh, &workerGroup)

	go func() {
		workerGroup.Wait()
		close(resultCh)
	}()

	go func() {
		t.dispatchTraceBatches(ctx, batches, jobCh, maxTraceSize)
	}()

	go func() {
		defer close(out)
		t.collectScanResults(ctx, resultCh, out)
	}()

	return out
}

func (t *trace) launchBlockScanWorkers(
	ctx context.Context,
	parts []*part,
	qo queryOptions,
	workerCount int,
	jobCh <-chan traceBatch,
	resultCh chan<- *scanBatch,
	wg *sync.WaitGroup,
) {
	for i := 0; i < workerCount; i++ {
		go func() {
			defer wg.Done()
			for batch := range jobCh {
				sb := t.processTraceBatch(ctx, parts, qo, batch)
				if sb == nil {
					continue
				}

				select {
				case <-ctx.Done():
					if sb.err == nil {
						releaseBlockCursors(sb.cursors)
					}
					return
				case resultCh <- sb:
				}
			}
		}()
	}
}

func (t *trace) dispatchTraceBatches(
	ctx context.Context,
	batches <-chan traceBatch,
	jobCh chan<- traceBatch,
	maxTraceSize int,
) {
	defer close(jobCh)

	total := 0
	sequence := 0
	limitReached := false

	for batch := range batches {
		if limitReached {
			continue
		}

		if batch.err != nil {
			batch.seq = sequence
			sequence++
			if !sendTraceBatch(ctx, jobCh, batch) {
				return
			}
			limitReached = true
			continue
		}

		if len(batch.traceIDs) == 0 {
			continue
		}

		var reached bool
		batch, total, reached = limitTraceBatch(batch, maxTraceSize, total)
		if len(batch.traceIDs) == 0 {
			if reached {
				limitReached = true
			}
			continue
		}

		batch.seq = sequence
		sequence++
		if !sendTraceBatch(ctx, jobCh, batch) {
			return
		}
		if reached {
			limitReached = true
		}
	}
}

func limitTraceBatch(batch traceBatch, maxTraceSize int, total int) (traceBatch, int, bool) {
	if maxTraceSize <= 0 {
		total += len(batch.traceIDs)
		return batch, total, false
	}

	remaining := maxTraceSize - total
	if remaining <= 0 {
		batch.traceIDs = batch.traceIDs[:0]
		return batch, total, true
	}

	if len(batch.traceIDs) > remaining {
		batch.traceIDs = append([]string(nil), batch.traceIDs[:remaining]...)
	}

	total += len(batch.traceIDs)

	return batch, total, total >= maxTraceSize
}

func sendTraceBatch(ctx context.Context, jobCh chan<- traceBatch, batch traceBatch) bool {
	select {
	case <-ctx.Done():
		return false
	case jobCh <- batch:
		return true
	}
}

func (t *trace) collectScanResults(
	ctx context.Context,
	resultCh <-chan *scanBatch,
	out chan<- *scanBatch,
) {
	pending := make(map[int]*scanBatch)
	nextSeq := 0

	for {
		select {
		case <-ctx.Done():
			releasePendingBatches(pending)
			return
		case sb, ok := <-resultCh:
			if !ok {
				flushPendingBatches(out, pending, nextSeq)
				return
			}
			if sb == nil {
				continue
			}
			if !t.processScanBatch(ctx, sb, pending, &nextSeq, out) {
				releasePendingBatches(pending)
				return
			}
		}
	}
}

func (t *trace) processScanBatch(
	ctx context.Context,
	sb *scanBatch,
	pending map[int]*scanBatch,
	nextSeq *int,
	out chan<- *scanBatch,
) bool {
	if sb.err != nil {
		releasePendingBatches(pending)
		return sendScanBatch(ctx, out, sb)
	}

	if sb.seq == *nextSeq {
		if !sendScanBatch(ctx, out, sb) {
			return false
		}
		(*nextSeq)++
		return t.flushReadyBatches(ctx, pending, nextSeq, out)
	}

	pending[sb.seq] = sb
	return true
}

func (t *trace) flushReadyBatches(
	ctx context.Context,
	pending map[int]*scanBatch,
	nextSeq *int,
	out chan<- *scanBatch,
) bool {
	for {
		sb, exists := pending[*nextSeq]
		if !exists {
			return true
		}
		delete(pending, *nextSeq)
		if !sendScanBatch(ctx, out, sb) {
			releasePendingBatches(pending)
			return false
		}
		(*nextSeq)++
	}
}

func flushPendingBatches(out chan<- *scanBatch, pending map[int]*scanBatch, nextSeq int) {
	for {
		sb, exists := pending[nextSeq]
		if !exists {
			break
		}
		out <- sb
		delete(pending, nextSeq)
		nextSeq++
	}
}

func sendScanBatch(ctx context.Context, out chan<- *scanBatch, sb *scanBatch) bool {
	select {
	case <-ctx.Done():
		if sb.err == nil {
			releaseBlockCursors(sb.cursors)
		}
		return false
	case out <- sb:
		return true
	}
}

func releasePendingBatches(pending map[int]*scanBatch) {
	for seq, sb := range pending {
		if sb != nil && sb.err == nil {
			releaseBlockCursors(sb.cursors)
		}
		delete(pending, seq)
	}
}

func (t *trace) scanTraceIDs(ctx context.Context, parts []*part, qo queryOptions, traceIDs []string) (result []*blockCursor, err error) {
	if len(parts) == 0 || len(traceIDs) == 0 {
		return nil, nil
	}

	sortedIDs := append([]string(nil), traceIDs...)
	sort.Strings(sortedIDs)

	recordBlock, finish := startBlockScanSpan(ctx, sortedIDs, parts)
	if finish == nil {
		finish = func(error) {}
	}

	defer func() {
		finish(err)
		if err != nil {
			for i := range result {
				releaseBlockCursor(result[i])
			}
			result = nil
		}
	}()

	bma := generateBlockMetadataArray()
	defer releaseBlockMetadataArray(bma)

	tstIter := generateTstIter()
	defer releaseTstIter(tstIter)

	tstIter.init(bma, parts, sortedIDs)
	if initErr := tstIter.Error(); initErr != nil {
		err = fmt.Errorf("cannot init tstIter: %w", initErr)
		return
	}

	var (
		spanBlockBytes uint64
		hit            int
	)

	quota := t.pm.AvailableBytes()

	for tstIter.nextBlock() {
		if hit%checkDoneEvery == 0 {
			select {
			case <-ctx.Done():
				err = pkgerrors.WithMessagef(ctx.Err(), "interrupt: scanned %d blocks, remained %d/%d parts to scan",
					len(result), len(tstIter.piPool)-tstIter.idx, len(tstIter.piPool))
				return
			default:
			}
		}
		hit++

		bc := generateBlockCursor()
		p := tstIter.piPool[tstIter.idx]
		bc.init(p.p, p.curBlock, qo)
		result = append(result, bc)
		if recordBlock != nil {
			recordBlock(bc)
		}
		spanBlockBytes += bc.bm.uncompressedSpanSizeBytes

		if quota >= 0 && spanBlockBytes > uint64(quota) {
			err = fmt.Errorf("block scan quota exceeded: used %d bytes, quota is %d bytes", spanBlockBytes, quota)
			return
		}
	}

	if iterErr := tstIter.Error(); iterErr != nil {
		err = fmt.Errorf("cannot iterate tstIter: %w", iterErr)
		return
	}

	if acquireErr := t.pm.AcquireResource(ctx, spanBlockBytes); acquireErr != nil {
		err = fmt.Errorf("cannot acquire resource: %w", acquireErr)
		return
	}

	return
}

func (t *trace) processTraceBatch(ctx context.Context, parts []*part, qo queryOptions, batch traceBatch) *scanBatch {
	if batch.err != nil {
		return &scanBatch{traceBatch: batch, err: batch.err}
	}

	sb := &scanBatch{traceBatch: batch}

	cursors, err := t.scanTraceIDs(ctx, parts, qo, batch.traceIDs)
	if err != nil {
		sb.err = err
		return sb
	}

	sb.cursors = cursors
	return sb
}

func releaseBlockCursors(cursors []*blockCursor) {
	for _, bc := range cursors {
		releaseBlockCursor(bc)
	}
}
