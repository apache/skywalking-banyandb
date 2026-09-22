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
	"slices"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// bucketAggPoolCapacity bounds the internal output-pool capacity of each
// per-bucket BatchAggregation instance openBucket creates. That instance is
// throwaway — its output is copied into the shared, properly batchSize-sized
// outputBuilder (see appendOutput) and the instance is closed immediately
// after — so its own pool capacity is an internal working-buffer size, not
// the pipeline's output chunk size. Using bt.batchSize (often 1024) there
// would allocate a full-capacity batch for even a one-group bucket; a small
// bucket, the common sparse-time-series case, would waste most of it. A
// bucket with more groups than this simply pages internally (openBucket's
// caller already loops NextBatch to drain every page), so this only trades
// a little pagination overhead for dense buckets against a large reduction
// in wasted allocation for sparse ones.
const bucketAggPoolCapacity = 64

// bucketStart mirrors pkg/flow/streaming/sliding_window.go:302 getWindowStart
// — the formula the TopN pre-aggregation path already uses to assign
// tumbling windows. It is exactly DATE_BIN with a Unix-epoch origin.
//
// Negative timestamps are unreachable in practice (measure timestamps are
// Unix milliseconds and the write path rejects non-positive ones), but Go's
// % truncates toward zero rather than flooring, so getWindowStart's plain
// "ts - ts%width" skews by one bucket for ts < 0. This corrects for that
// rather than depending on a caller's invariant — the two formulas agree
// exactly for ts >= 0, which is the entire reachable domain.
func bucketStart(ts, width int64) int64 {
	remainder := ts % width
	if remainder < 0 {
		remainder += width
	}
	return ts - remainder
}

// floorTimestampColumn rewrites schema's RoleTimestamp column in place,
// replacing each non-null value with its bucket start. This is the "map
// operator that rewrites the timestamp column to its bucket start" from
// design §7.2: once floored, the column is an ordinary int64 group key and
// needs no special handling from computeKey/appendKeyComponent.
func floorTimestampColumn(b *vectorized.RecordBatch, timestampIdx int, widthNanos int64) {
	col, ok := b.Columns[timestampIdx].(*vectorized.TypedColumn[int64])
	if !ok {
		return
	}
	data := col.Data()
	for i, ts := range data {
		if col.IsNull(i) {
			continue
		}
		col.SetAt(i, bucketStart(ts, widthNanos))
	}
}

// BatchTimeBucket is the time-bucketed counterpart of BatchAggregation
// (design §7.2). It floors each incoming batch's timestamp column to the
// bucket start, then delegates the actual grouping/folding to a sequence of
// ordinary BatchAggregation instances — one per closed bucket in streaming
// mode, or a single instance covering the whole scan in map mode — so every
// bucket-aware behavior (the conditional D2 reversal, tag carry-forward,
// AggModeMap/Reduce partial shapes) is exactly BatchAggregation's existing
// behavior, never duplicated here.
//
// BatchTimeBucket is a PullOperator, not a BreakerOperator: it owns upstream
// directly and pulls from it lazily inside NextBatch, rather than being
// driven by breakerStage's "Consume the entire upstream, then serve"
// contract. That distinction is load-bearing for the memory-bound claim
// below — see NewBatchTimeBucket's wiring note.
//
// Streaming mode (the part-scan default) holds only the current bucket's
// groups live at a time: consumeBatch splits each upstream batch into
// contiguous same-bucket runs (the input is time-ascending, so a run, once
// left behind, never recurs) and flushes+closes the open BatchAggregation
// whenever the bucket advances, bounding live group state to one bucket's
// groups instead of buckets × tagGroups. Because NextBatch drains any
// already-flushed output before pulling the next upstream batch, the
// flushed-but-not-yet-emitted queue is bounded too: at most the buckets that
// closed while processing a single upstream batch (typically one, since a
// batch is usually a thin, densely-packed time slice), not every bucket the
// whole scan will ever produce. A row whose bucket regresses is a
// monotonicity violation — streaming is correct only while the input is
// time-ordered — and consumeBatch fails loudly rather than silently
// reopening a flushed bucket.
//
// Map mode (index-mode measures, which have no ascending-timestamp
// guarantee — design §7.2) never flushes mid-scan: every row folds into one
// persistent BatchAggregation keyed on (bucket, tags…) like any other
// GroupBy, and the whole result is produced once upstream is exhausted.
// This trades the memory bound for correctness on out-of-order input;
// NewBatchTimeBucket's streaming argument selects which mode a given
// operator instance runs.
type BatchTimeBucket struct {
	upstream      vectorized.PullOperator
	inputSchema   *vectorized.BatchSchema
	outputSchema  *vectorized.BatchSchema
	tracker       *vectorized.MemoryTracker
	currentAgg    *BatchAggregation
	outputPool    *vectorized.BatchPool
	outputBuilder *vectorized.RecordBatch
	pending       []*vectorized.RecordBatch
	aggKeyIndices []int
	aggs          []AggSpec
	widthNanos    int64
	currentBucket int64
	timestampIdx  int
	batchSize     int
	entrySize     int64
	mode          AggMode
	hasBucket     bool
	streaming     bool
	upstreamDone  bool
	mapDrainReady bool
	closed        bool
}

// NewBatchTimeBucket constructs a BatchTimeBucket wrapping upstream, the
// operator this one pulls batches from directly (see the type doc for why
// that — rather than a Consume/Finalize breaker contract — is what makes
// the memory bound real). upstream may be nil for a schema-only probe (no
// Init/NextBatch/Close call reaches it in that case) — GroupByAgg.Schema()
// uses this to read OutputSchema without a real pipeline.
//
// keyIndices are the tag GroupBy keys (may be empty for a bucket-only
// GroupBy); timestampIdx is the input schema's RoleTimestamp column;
// widthNanos is the resolved bucket width (design §5.3), always > 0.
// streaming selects the part-scan streaming path (true) or the index-mode
// map fallback (false) — decided once by the analyzer from
// measureSchema.GetIndexMode(), never guessed here. tracker and entrySize
// are forwarded to each underlying BatchAggregation unchanged.
func NewBatchTimeBucket(
	upstream vectorized.PullOperator,
	input *vectorized.BatchSchema, keyIndices []int, timestampIdx int, widthNanos int64,
	aggs []AggSpec, mode AggMode, batchSize int, tracker *vectorized.MemoryTracker, entrySize int64,
	streaming bool,
) *BatchTimeBucket {
	aggKeyIndices := append(append([]int(nil), keyIndices...), timestampIdx)
	// A throwaway instance only to read its precomputed OutputSchema — mirrors
	// GroupByAgg.Schema()'s existing pattern of probing shape without Init.
	probe := NewBatchAggregation(input, aggKeyIndices, aggs, mode, batchSize, tracker, entrySize)
	outputSchema := probe.OutputSchema()
	return &BatchTimeBucket{
		upstream:      upstream,
		inputSchema:   input,
		outputSchema:  outputSchema,
		tracker:       tracker,
		outputPool:    vectorized.NewBatchPool(outputSchema, batchSize),
		aggKeyIndices: aggKeyIndices,
		aggs:          slices.Clone(aggs),
		widthNanos:    widthNanos,
		timestampIdx:  timestampIdx,
		batchSize:     batchSize,
		entrySize:     entrySize,
		mode:          mode,
		streaming:     streaming,
	}
}

// Init initializes upstream. The first bucket opens lazily on the first
// NextBatch call.
func (bt *BatchTimeBucket) Init(ctx context.Context) error { return bt.upstream.Init(ctx) }

// OutputSchema returns the schema of emitted batches, precomputed at
// construction time.
func (bt *BatchTimeBucket) OutputSchema() *vectorized.BatchSchema { return bt.outputSchema }

// NextBatch drains any already-flushed output first; only once that queue is
// empty does it pull the next batch from upstream. This ordering is what
// bounds the flushed-but-unemitted queue to "whatever closed while
// processing one upstream batch" instead of "everything the whole scan will
// ever produce" — see the type doc.
//
// Map mode's terminal aggregator is the one exception a per-upstream-batch
// bound doesn't cover on its own: it can represent the entire scan, not one
// bucket. Once upstream is exhausted, drainMapModeAggregator takes over and
// pulls that aggregator one page at a time — interleaved with normal
// pending/commitBuilder draining — rather than flushCurrentBucket's
// loop-until-nil, which would materialize every page into pending before
// this call ever returns the first one.
func (bt *BatchTimeBucket) NextBatch(ctx context.Context) (*vectorized.RecordBatch, error) {
	for {
		if len(bt.pending) > 0 {
			nb := bt.pending[0]
			bt.pending = bt.pending[1:]
			return nb, nil
		}
		if bt.upstreamDone {
			if bt.currentAgg == nil {
				return nil, nil
			}
			drained, drainErr := bt.drainMapModeAggregator(ctx)
			if drainErr != nil {
				return nil, drainErr
			}
			if !drained {
				continue
			}
			bt.commitBuilder()
			continue
		}
		b, pullErr := bt.upstream.NextBatch(ctx)
		if pullErr != nil {
			return nil, pullErr
		}
		if b == nil {
			bt.upstreamDone = true
			if bt.streaming {
				// The scan has ended; flush whatever bucket is still
				// open — bounded to one bucket's groups, same as any
				// mid-scan advance-driven flush, so draining it in one
				// shot here is fine.
				if flushErr := bt.flushCurrentBucket(ctx); flushErr != nil {
					return nil, flushErr
				}
				bt.commitBuilder()
			}
			// Map mode leaves currentAgg open; the loop's upstreamDone
			// branch above drains it incrementally instead.
			continue
		}
		if consumeErr := bt.consumeBatch(ctx, b); consumeErr != nil {
			return nil, consumeErr
		}
		// Commit whatever the buckets that closed while processing this one
		// upstream batch compacted into the builder — see commitBuilder's
		// doc for why this boundary, not batchSize, is what must gate a
		// commit: it is the same point at which the pre-compaction code
		// already made queued output visible, so latency is unchanged.
		bt.commitBuilder()
	}
}

// drainMapModeAggregator pulls exactly one page from the terminal (map-mode)
// aggregator into the output builder, finalizing and sorting it first on the
// first call. Returns drained=true once the aggregator is exhausted and
// closed (currentAgg is nil at that point) — the caller should then check
// pending/commitBuilder's result rather than loop here, keeping this method
// symmetric with the main pull loop's one-unit-of-work-per-call discipline.
func (bt *BatchTimeBucket) drainMapModeAggregator(ctx context.Context) (drained bool, err error) {
	if !bt.mapDrainReady {
		if finalizeErr := bt.currentAgg.Finalize(ctx); finalizeErr != nil {
			return false, finalizeErr
		}
		bt.currentAgg.SortInsertionByBucket()
		bt.mapDrainReady = true
	}
	nb, nextErr := bt.currentAgg.NextBatch(ctx)
	if nextErr != nil {
		return false, nextErr
	}
	if nb == nil {
		closeErr := bt.currentAgg.Close()
		bt.currentAgg = nil
		return true, closeErr
	}
	bt.appendOutput(nb)
	return false, nil
}

// Close is idempotent; it closes any still-open bucket aggregator (the
// error path — the scan never reached EOF), releases the pending queue, and
// closes upstream.
func (bt *BatchTimeBucket) Close() error {
	if bt.closed {
		return nil
	}
	bt.closed = true
	var firstErr error
	if bt.currentAgg != nil {
		firstErr = bt.currentAgg.Close()
		bt.currentAgg = nil
	}
	bt.pending = nil
	bt.outputBuilder = nil
	if bt.upstream != nil {
		if closeErr := bt.upstream.Close(); closeErr != nil && firstErr == nil {
			firstErr = closeErr
		}
	}
	return firstErr
}

// consumeBatch floors b's timestamp column to bucket starts, then either
// feeds every active row into the single persistent aggregator (map mode)
// or splits the batch into contiguous same-bucket runs and flushes the open
// bucket whenever the run's bucket advances past it (streaming mode). A row
// whose bucket regresses in streaming mode is a monotonicity violation and
// fails loudly rather than reopening an already-flushed bucket.
func (bt *BatchTimeBucket) consumeBatch(ctx context.Context, b *vectorized.RecordBatch) error {
	active, activeErr := activeIndices(b)
	if activeErr != nil {
		return activeErr
	}
	if len(active) == 0 {
		return nil
	}
	floorTimestampColumn(b, bt.timestampIdx, bt.widthNanos)
	tsCol, ok := b.Columns[bt.timestampIdx].(*vectorized.TypedColumn[int64])
	if !ok {
		return fmt.Errorf("vectorized.measure: BatchTimeBucket: column %d is not a RoleTimestamp int64 column", bt.timestampIdx)
	}
	tsData := tsCol.Data()

	if !bt.streaming {
		if !bt.hasBucket {
			bt.hasBucket = true
			if openErr := bt.openBucket(ctx); openErr != nil {
				return openErr
			}
		}
		return bt.feedRun(ctx, b, active)
	}

	runStart := 0
	for i, rowIdx := range active {
		bucket := tsData[rowIdx]
		switch {
		case !bt.hasBucket:
			bt.hasBucket = true
			bt.currentBucket = bucket
			if openErr := bt.openBucket(ctx); openErr != nil {
				return openErr
			}
			runStart = i
		case bucket == bt.currentBucket:
			// Same run; keep accumulating.
		case bucket > bt.currentBucket:
			if feedErr := bt.feedRun(ctx, b, active[runStart:i]); feedErr != nil {
				return feedErr
			}
			if flushErr := bt.flushCurrentBucket(ctx); flushErr != nil {
				return flushErr
			}
			bt.currentBucket = bucket
			if openErr := bt.openBucket(ctx); openErr != nil {
				return openErr
			}
			runStart = i
		default:
			return fmt.Errorf("vectorized.measure: BatchTimeBucket: input is not time-ascending — row bucket %d precedes the current bucket %d", bucket, bt.currentBucket)
		}
	}
	return bt.feedRun(ctx, b, active[runStart:])
}

// openBucket constructs and initializes a fresh BatchAggregation to become
// the currently open bucket's aggregator.
func (bt *BatchTimeBucket) openBucket(ctx context.Context) error {
	agg := NewBatchAggregation(bt.inputSchema, bt.aggKeyIndices, bt.aggs, bt.mode, min(bt.batchSize, bucketAggPoolCapacity), bt.tracker, bt.entrySize)
	if initErr := agg.Init(ctx); initErr != nil {
		return initErr
	}
	bt.currentAgg = agg
	return nil
}

// feedRun consumes the rows named by rows (a Selection into b's columns,
// pre-floored) through the currently open bucket's aggregator.
func (bt *BatchTimeBucket) feedRun(ctx context.Context, b *vectorized.RecordBatch, rows []uint16) error {
	if len(rows) == 0 {
		return nil
	}
	view := &vectorized.RecordBatch{Schema: b.Schema, Columns: b.Columns, Selection: rows, Len: b.Len}
	return bt.currentAgg.Consume(ctx, view)
}

// flushCurrentBucket finalizes and drains a closed streaming-mode bucket's
// aggregator, compacting its output into the shared builder (see
// appendOutput), then closes it. A no-op when no bucket is open.
//
// Streaming-only: a streaming instance's aggregator holds exactly one
// bucket's groups by construction, so draining it in one shot here is
// bounded the same way the group map itself is. Map mode's terminal
// aggregator can span the whole scan and is drained incrementally instead
// by drainMapModeAggregator, which flushCurrentBucket is never called for.
//
// A closed bucket's own BatchAggregation.NextBatch pages come from a
// pool sized for a full batchSize batch regardless of how many groups the
// bucket actually held — a bucket with one group would otherwise queue one
// full-capacity, mostly-empty RecordBatch per bucket. When a single
// upstream batch spans many small buckets (e.g. one sample per bucket),
// that is O(buckets) such allocations queued before NextBatch ever returns
// one to the caller. appendOutput compacts them into shared
// batchSize-capacity batches instead, so queued output is proportional to
// row count, not bucket count.
func (bt *BatchTimeBucket) flushCurrentBucket(ctx context.Context) error {
	if bt.currentAgg == nil {
		return nil
	}
	if finalizeErr := bt.currentAgg.Finalize(ctx); finalizeErr != nil {
		return finalizeErr
	}
	for {
		nb, nextErr := bt.currentAgg.NextBatch(ctx)
		if nextErr != nil {
			return nextErr
		}
		if nb == nil {
			break
		}
		bt.appendOutput(nb)
	}
	closeErr := bt.currentAgg.Close()
	bt.currentAgg = nil
	return closeErr
}

// appendOutput compacts nb's rows into the shared output builder, queuing
// the builder to pending once it reaches batchSize. nb already at
// batchSize capacity (the common case for a bucket with many groups, or
// for map mode's single final drain) is queued directly with no copy —
// compaction only matters for the small/partial pages a sparse bucket
// produces.
func (bt *BatchTimeBucket) appendOutput(nb *vectorized.RecordBatch) {
	if bt.outputBuilder == nil && nb.Len >= bt.batchSize {
		bt.pending = append(bt.pending, nb)
		return
	}
	for i := 0; i < nb.Len; i++ {
		if bt.outputBuilder == nil {
			bt.outputBuilder = bt.outputPool.Get()
		}
		for c, col := range bt.outputBuilder.Columns {
			copyOneValue(col, nb.Columns[c], i)
		}
		bt.outputBuilder.Len++
		if bt.outputBuilder.Len >= bt.batchSize {
			bt.pending = append(bt.pending, bt.outputBuilder)
			bt.outputBuilder = nil
		}
	}
}

// commitBuilder queues whatever the builder has accumulated so far, even if
// under batchSize. Called at each NextBatch pull/consume boundary — the
// same point at which the pre-compaction code already made a bucket's
// flushed output visible — so compacting several small buckets into one
// batch does not add pull-ahead latency beyond what NextBatch already did.
func (bt *BatchTimeBucket) commitBuilder() {
	if bt.outputBuilder == nil || bt.outputBuilder.Len == 0 {
		return
	}
	bt.pending = append(bt.pending, bt.outputBuilder)
	bt.outputBuilder = nil
}
