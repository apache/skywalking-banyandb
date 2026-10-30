// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package stream

import (
	"context"
	"errors"
	"fmt"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/index"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	vstream "github.com/apache/skywalking-banyandb/pkg/query/vectorized/stream"
)

// ErrQueryMemoryBudgetExceeded is returned by the vec scan when a block would be
// decoded past the per-query QueryMemoryMiB soft budget (after the first-block
// exception). The scan fails loud rather than silently dropping the block, so the
// vec path preserves parity with the row path (which errors under memory pressure
// via the protector) — a partial result would break vec==row equivalence.
var ErrQueryMemoryBudgetExceeded = errors.New("stream: vectorized query memory budget exceeded")

// heldCursor is a loaded blockCursor together with the budget it holds: the bytes
// soft-reserved on the scan's memory tracker and the bytes charged to the query
// lease. Both are returned when the cursor is released, so the scan is accounted
// for the blocks it holds right now rather than for every block it ever read.
type heldCursor struct {
	bc         *blockCursor
	memBytes   int64
	leaseBytes uint64
}

// streamVecScan is a pull-based columnar scan source for the stream engine. It
// yields *vectorized.RecordBatch values built directly from blockCursor columns,
// never constructing a model.StreamResult. Each NextBatch call returns one batch
// (per block/cursor, per-cursor order preserved); NextBatch returns (nil, nil)
// when the source is exhausted.
//
// Scope note: this source produces correct per-cursor columnar batches only.
// Global cross-part/cross-shard ordering, dedup, and the in-order top-N cap are
// M4 operators (SortedMerge/Distinct/Limit) and are deliberately NOT implemented
// here — see .omc/plans/vectorized-stream-query.md §5.3. The scan therefore emits
// EVERY matching row (chunked into batchSize batches); it does NOT truncate at
// MaxElementSize, because a pre-merge storage-order truncation would keep the
// wrong rows (e.g. the oldest rows for an ORDER BY ts DESC query, since blocks
// are seriesID-major). The MaxElementSize cap is applied post-merge in the M4
// pipeline (the correct in-order top-N), matching the row path.
type streamVecScan struct {
	sm             *stream
	schema         *vectorized.BatchSchema
	tr             *index.RangeOpts
	ts             *blockScanner
	tmpBlock       *block
	mem            *vectorized.MemoryTracker
	lease          query.BudgetLease
	scanCh         chan *blockScanResultBatch
	scanDone       chan struct{}
	scanCancel     context.CancelFunc
	segments       []storage.Segment[*tsTable, option]
	series         []*pbv1.Series
	pending        []heldCursor
	cur            heldCursor
	orderTagFamily string
	orderTagName   string
	failErr        error
	qo             queryOptions
	batchSize      int
	curOff         int
	asc            bool
	indexOrder     bool
	budgetEngaged  bool
	closed         bool
}

// Schema returns the batch schema stamped on every emitted batch.
func (v *streamVecScan) Schema() *vectorized.BatchSchema { return v.schema }

// NextBatch returns the next columnar batch, or (nil, nil) when exhausted.
//
// A single blockCursor can hold more than batchSize rows (stream blocks are
// capped by uncompressed bytes, not row count), so the cursor is drained across
// MULTIPLE batches via an advancing offset (v.curOff); the cursor is released
// only once all its rows are consumed. This prevents silent row loss past
// batchSize.
func (v *streamVecScan) NextBatch(ctx context.Context) (*vectorized.RecordBatch, error) {
	if v.closed {
		return nil, nil
	}
	for {
		if v.cur.bc == nil {
			held, err := v.nextCursor(ctx)
			if err != nil {
				return nil, err
			}
			if held.bc == nil {
				return nil, nil
			}
			v.cur = held
			v.curOff = 0
		}
		batch, err := v.cursorToBatch(v.cur.bc, v.curOff)
		if err != nil {
			v.releaseHeld(v.cur)
			v.cur = heldCursor{}
			return nil, err
		}
		v.curOff += batch.Len
		if v.curOff >= len(v.cur.bc.timestamps) {
			v.releaseHeld(v.cur)
			v.cur = heldCursor{}
		}
		if batch.Len == 0 {
			continue
		}
		return batch, nil
	}
}

// releaseHeld releases a cursor and returns its soft reservation and its lease
// charge. It needs no context, so Release can call it on any path.
func (v *streamVecScan) releaseHeld(held heldCursor) {
	if held.bc != nil {
		releaseBlockCursor(held.bc)
	}
	if held.memBytes > 0 && v.mem != nil {
		v.mem.Release(held.memBytes)
	}
	if held.leaseBytes > 0 && v.lease != nil {
		v.lease.Refund(held.leaseBytes)
	}
}

// nextCursor returns the next loaded cursor, pulling ONE scanner result batch (at
// most blockScannerBatchSize blocks) at a time and only when no loaded cursor is
// left, so the scan holds a bounded number of decoded blocks instead of a whole
// segment. It reuses the exact scan helpers the row path uses (searchSeries,
// getBlockScanner, loadBlockCursor). A returned cursor is positioned at its first
// row (asc) or last row (desc); the caller must releaseHeld it.
func (v *streamVecScan) nextCursor(ctx context.Context) (heldCursor, error) {
	for {
		if len(v.pending) > 0 {
			held := v.pending[0]
			v.pending[0] = heldCursor{}
			v.pending = v.pending[1:]
			return held, nil
		}
		// A soft-budget or scan failure surfaces after the already-loaded cursors
		// drain, rather than returning a partial result (row-path parity — the row
		// path errors under memory pressure via the protector).
		if v.failErr != nil {
			return heldCursor{}, v.failErr
		}
		if v.scanCh != nil {
			if err := v.pullScanBatch(ctx); err != nil {
				return heldCursor{}, err
			}
			continue
		}
		if v.ts != nil {
			if len(v.ts.parts) == 0 {
				// Every part group of this segment is scanned.
				v.ts.close()
				v.ts = nil
				continue
			}
			v.startScanner(ctx)
			continue
		}
		if len(v.segments) == 0 {
			return heldCursor{}, nil
		}
		var segment storage.Segment[*tsTable, option]
		if v.asc {
			segment = v.segments[len(v.segments)-1]
			v.segments = v.segments[:len(v.segments)-1]
		} else {
			segment = v.segments[0]
			v.segments = v.segments[1:]
		}
		qo, err := searchSeries(ctx, v.qo, segment, v.series)
		if err != nil {
			return heldCursor{}, err
		}
		ts, err := getBlockScanner(ctx, segment, qo, v.sm.l, v.sm.pm, v.tr)
		if err != nil {
			return heldCursor{}, err
		}
		if ts == nil {
			continue
		}
		v.ts = ts
	}
}

// startScanner runs the block scanner for the next part group of the current
// segment on its own goroutine, under a context the scan can cancel.
func (v *streamVecScan) startScanner(ctx context.Context) {
	scanCtx, cancel := context.WithCancel(ctx)
	ch := make(chan *blockScanResultBatch, 1)
	done := make(chan struct{})
	scanner := v.ts
	go func() {
		defer close(done)
		scanner.scan(scanCtx, ch)
		close(ch)
	}()
	v.scanCh, v.scanDone, v.scanCancel = ch, done, cancel
}

// stopScanner cancels the running scanner, releases every result batch it has not
// delivered and waits for its goroutine. It is a no-op when none is running.
func (v *streamVecScan) stopScanner() {
	if v.scanCancel == nil {
		return
	}
	v.scanCancel()
	for batch := range v.scanCh {
		releaseBlockScanResultBatch(batch)
	}
	<-v.scanDone
	v.scanCh, v.scanDone, v.scanCancel = nil, nil, nil
}

// pullScanBatch takes one result batch from the running scanner and decodes its
// blocks into v.pending. When the scanner has no more batches it is stopped, which
// lets the caller move on to the next part group.
func (v *streamVecScan) pullScanBatch(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	batch, ok := <-v.scanCh
	if !ok {
		v.stopScanner()
		// A canceled context also ends the scanner early; do not mistake that for
		// the end of the data.
		return ctx.Err()
	}
	if batch.err != nil {
		scanErr := batch.err
		releaseBlockScanResultBatch(batch)
		v.failErr = scanErr
		v.stopScanner()
		return scanErr
	}
	is := v.sm.indexSchema.Load().(indexSchema)
	for i := range batch.bss {
		bs := &batch.bss[i]
		held, err := v.loadHeld(ctx, bs, is)
		if err != nil {
			v.failErr = err
			break
		}
		if held.bc != nil {
			v.pending = append(v.pending, held)
		}
	}
	releaseBlockScanResultBatch(batch)
	if v.failErr != nil {
		v.stopScanner()
	}
	return nil
}

// loadHeld charges, soft-reserves and decodes one scanned block. A failure returns
// an error with nothing held; a block with no matching row returns a zero cursor.
func (v *streamVecScan) loadHeld(ctx context.Context, bs *blockScanResult, is indexSchema) (heldCursor, error) {
	held := heldCursor{leaseBytes: bs.bm.uncompressedSizeBytes}
	if v.lease != nil && held.leaseBytes > 0 {
		if err := ctx.Err(); err != nil {
			return heldCursor{}, err
		}
		if err := v.lease.Charge(held.leaseBytes); err != nil {
			return heldCursor{}, err
		}
	} else {
		held.leaseBytes = 0
	}
	// Metadata preflight for the QueryMemoryMiB soft budget: predict whether
	// decoding this block would push the bytes this scan holds over the budget and,
	// if so, FAIL LOUD before the expensive decode. The first block always decodes
	// (first-block exception) so a too-small budget never rejects the whole query.
	// When no budget is set (v.mem == nil) every block is decoded; the block
	// scanner's protector.Memory quota still bounds it.
	if v.mem != nil {
		blockBytes := int64(bs.bm.uncompressedSizeBytes)
		if v.budgetEngaged {
			if reserveErr := v.mem.Reserve(blockBytes); reserveErr != nil {
				v.releaseHeld(held)
				return heldCursor{}, fmt.Errorf("%w: %w", ErrQueryMemoryBudgetExceeded, reserveErr)
			}
			held.memBytes = blockBytes
		} else {
			if v.mem.Reserve(blockBytes) == nil {
				held.memBytes = blockBytes
			}
			v.budgetEngaged = true
		}
	}
	bc := generateBlockCursor()
	bc.init(bs.p, &bs.bm, bs.qo)
	if !loadBlockCursor(bc, v.tmpBlock, bs.qo, is) {
		// loadBlockCursor has already released the cursor; only the budget is left.
		v.releaseHeld(held)
		return heldCursor{}, nil
	}
	if !v.asc {
		bc.idx = len(bc.timestamps) - 1
	} else {
		bc.idx = 0
	}
	held.bc = bc
	return held, nil
}

// cursorToBatch converts a window [off, off+n) of a loaded blockCursor into a
// columnar batch, where n is capped at batchSize. It reads the rows into the
// batch columns using the exact decode copyTo uses (schemaType match →
// mustDecodeTagValue; else NullTagValue). Reading a window (not always [0,len))
// lets NextBatch drain a cursor larger than batchSize across several batches
// without losing rows. Returns an error if an order-key tag value fails to marshal.
func (v *streamVecScan) cursorToBatch(bc *blockCursor, off int) (*vectorized.RecordBatch, error) {
	total := len(bc.timestamps)
	n := total - off
	if n > v.batchSize {
		n = v.batchSize
	}
	if n <= 0 {
		return &vectorized.RecordBatch{Schema: v.schema}, nil
	}
	end := off + n

	batch := vectorized.NewRecordBatch(v.schema, n)
	tsCol := batch.Columns[v.schema.TimestampIndex()].(*vectorized.TypedColumn[int64])
	elemCol := batch.Columns[v.schema.ElementIDIndex()].(*vectorized.TypedColumn[int64])
	seriesCol := batch.Columns[v.schema.SeriesIDIndex()].(*vectorized.TypedColumn[int64])

	seriesColVal := vstream.SeriesIDToColumn(uint64(bc.bm.seriesID))
	for row := off; row < end; row++ {
		tsCol.Append(bc.timestamps[row])
		elemCol.Append(vstream.ElementIDToColumn(bc.elementIDs[row]))
		seriesCol.Append(seriesColVal)
	}

	for _, cf := range bc.tagFamilies {
		for _, c := range cf.tags {
			colIdx, ok := v.schema.TagIndex(cf.name, c.name)
			if !ok {
				continue
			}
			tagCol := batch.Columns[colIdx].(*vectorized.TypedColumn[*modelv1.TagValue])
			schemaType, hasSchemaType := bc.schemaTagTypes[c.name]
			for row := off; row < end; row++ {
				if len(c.values) > row && hasSchemaType && c.valueType == schemaType {
					tagCol.Append(mustDecodeTagValue(c.valueType, c.values[row]))
				} else {
					tagCol.Append(pbv1.NullTagValue)
				}
			}
		}
	}

	// Any projected tag columns with no matching cursor tag are null-filled so
	// every column has exactly n rows (mirrors copyTo's NullTagValue fallback).
	for _, def := range v.schema.Columns {
		if def.Role != vectorized.RoleTag {
			continue
		}
		colIdx, _ := v.schema.TagIndex(def.TagFamily, def.Name)
		col := batch.Columns[colIdx].(*vectorized.TypedColumn[*modelv1.TagValue])
		for col.Len() < n {
			col.Append(pbv1.NullTagValue)
		}
	}

	if v.indexOrder {
		if okIdx := v.schema.OrderKeyIndex(); okIdx >= 0 {
			orderCol := batch.Columns[okIdx].(*vectorized.TypedColumn[[]byte])
			orderColIdx, hasOrderCol := v.schema.TagIndex(v.orderTagFamily, v.orderTagName)
			var orderTagCol *vectorized.TypedColumn[*modelv1.TagValue]
			if hasOrderCol {
				orderTagCol = batch.Columns[orderColIdx].(*vectorized.TypedColumn[*modelv1.TagValue])
			}
			for row := 0; row < n; row++ {
				var keyBytes []byte
				if orderTagCol != nil {
					b, mErr := pbv1.MarshalTagValue(orderTagCol.Data()[row])
					if mErr != nil {
						return nil, fmt.Errorf("stream vec scan: marshal order key for tag %s.%s: %w",
							v.orderTagFamily, v.orderTagName, mErr)
					}
					keyBytes = b
				}
				orderCol.Append(keyBytes)
			}
		}
	}

	batch.Len = n
	return batch, nil
}

// Release frees the scan source's resources: it stops the scanner goroutine,
// releases the current and pending cursors together with their budget, closes the
// block scanner, the temp block, and the retained segment references.
func (v *streamVecScan) Release() {
	if v.closed {
		return
	}
	v.closed = true
	v.stopScanner()
	if v.cur.bc != nil {
		v.releaseHeld(v.cur)
		v.cur = heldCursor{}
	}
	for _, held := range v.pending {
		v.releaseHeld(held)
	}
	v.pending = nil
	if v.ts != nil {
		v.ts.close()
		v.ts = nil
	}
	if v.tmpBlock != nil {
		releaseBlock(v.tmpBlock)
		v.tmpBlock = nil
	}
	for i := range v.segments {
		v.segments[i].DecRef()
	}
	v.segments = nil
}

// emptyVecScan is a source that yields nothing — used for bypass/empty-segment
// cases so callers never special-case a nil source. It still carries a schema so
// the M4 pipeline can be built over it (SortedMerge requires a schema even when no
// batch ever arrives).
type emptyVecScan struct {
	schema *vectorized.BatchSchema
}

// Schema returns the empty source's schema (may be nil for degenerate callers).
func (e emptyVecScan) Schema() *vectorized.BatchSchema { return e.schema }

// NextBatch always returns exhausted.
func (emptyVecScan) NextBatch(context.Context) (*vectorized.RecordBatch, error) { return nil, nil }

// Release is a no-op.
func (emptyVecScan) Release() {}

// vecScanSource is the pull contract shared by streamVecScan and emptyVecScan.
type vecScanSource interface {
	NextBatch(ctx context.Context) (*vectorized.RecordBatch, error)
	Schema() *vectorized.BatchSchema
	Release()
}

// queryVectorized is the vectorized counterpart of Query. It reuses Query's front
// half (validate, TSDB, segment selection, series prep, schema tag types, query
// options) and returns a pull-based columnar scan source.
//
// Deliberate deviation from the AC wording ("invoked from query.go"): this is a
// SEPARATE method rather than a branch inside Query. Query returns
// model.StreamQueryResult whose Pull() yields *model.StreamResult — a row tuple
// the vec path is forbidden to build. Routing the vec path through Query would
// therefore violate the no-row-tuple constraint. queryVectorized is wired into
// the data-node processor / standalone egress in M6.
func (s *stream) queryVectorized(ctx context.Context, sqo model.StreamQueryOptions) (vecScanSource, error) {
	if err := validateQueryInput(sqo); err != nil {
		return nil, err
	}
	tsdb, err := s.getTSDB()
	if err != nil {
		return nil, err
	}
	indexOrderEarly := sqo.Order != nil && sqo.Order.Index != nil
	var earlyOrderFamily, earlyOrderName string
	if indexOrderEarly {
		earlyOrderFamily, earlyOrderName = s.resolveOrderTag(sqo)
	}
	emptySchema := vstream.BuildStreamBatchSchema(sqo.TagProjection, earlyOrderFamily, earlyOrderName)

	segments, err := tsdb.SelectSegments(*sqo.TimeRange, true)
	if err != nil {
		return nil, err
	}
	if len(segments) < 1 {
		return emptyVecScan{schema: emptySchema}, nil
	}

	segmentsNeedRelease := true
	defer func() {
		if !segmentsNeedRelease {
			return
		}
		for i := range segments {
			segments[i].DecRef()
		}
	}()

	series := prepareSeriesData(sqo)
	schemaTagTypes := make(map[string]pbv1.ValueType)
	for _, tf := range s.schema.GetTagFamilies() {
		for _, tag := range tf.GetTags() {
			vt := pbv1.TagValueSpecToValueType(tag.GetType())
			if vt != pbv1.ValueTypeUnknown {
				schemaTagTypes[tag.GetName()] = vt
			}
		}
	}

	batchSize := s.vectorized.BatchSize
	if batchSize <= 0 {
		batchSize = vectorized.DefaultBatchSize
	}

	indexOrder := sqo.Order != nil && sqo.Order.Index != nil
	var orderTagFamily, orderTagName string
	if indexOrder {
		orderTagFamily, orderTagName = s.resolveOrderTag(sqo)
	}
	schema := vstream.BuildStreamBatchSchema(sqo.TagProjection, orderTagFamily, orderTagName)

	// Per-query soft byte budget: cap cumulative uncompressed block bytes decoded
	// by this scan at QueryMemoryMiB. Enforced as a metadata preflight in
	// loadHeld (first-block exception). This is independent of, and in
	// addition to, the block scanner's process-wide protector.Memory quota.
	var mem *vectorized.MemoryTracker
	if s.vectorized.QueryMemoryMiB > 0 {
		mem = vectorized.NewMemoryTracker(int64(s.vectorized.QueryMemoryMiB) << 20)
	}

	qo := prepareQueryOptions(sqo, schemaTagTypes)
	tr := index.NewIntRangeOpts(qo.minTimestamp, qo.maxTimestamp, true, true)

	asc := true
	if sqo.Order != nil && sqo.Order.Sort == modelv1.Sort_SORT_DESC {
		asc = false
	}

	scan := &streamVecScan{
		sm:             s,
		segments:       segments,
		series:         series,
		qo:             qo,
		tr:             &tr,
		schema:         schema,
		mem:            mem,
		batchSize:      batchSize,
		asc:            asc,
		indexOrder:     indexOrder,
		orderTagFamily: orderTagFamily,
		orderTagName:   orderTagName,
		tmpBlock:       generateBlock(),
	}
	// The lease is captured once so Release can refund without a context.
	if lease, admitted := query.BudgetLeaseFromContext(ctx); admitted {
		scan.lease = lease
	}
	segmentsNeedRelease = false
	return scan, nil
}

// resolveOrderTag returns the (family, name) of the single ordered tag for an
// index-order query. The order index carries exactly one tag name; its family is
// resolved against the query projection so the order-key column keys on the same
// passthrough cell used at egress.
func (s *stream) resolveOrderTag(sqo model.StreamQueryOptions) (family, name string) {
	if sqo.Order == nil || sqo.Order.Index == nil {
		return "", ""
	}
	tags := sqo.Order.Index.GetTags()
	if len(tags) != 1 {
		return "", ""
	}
	name = tags[0]
	for _, proj := range sqo.TagProjection {
		for _, projName := range proj.Names {
			if projName == name {
				return proj.Family, name
			}
		}
	}
	return "", name
}

// BuildElementsFromVecBatches drains a columnar scan source and materializes
// []*streamv1.Element from the batch columns. The Element shape is byte-for-byte
// identical to BuildElementsFromStreamResult: hex-encoded elementID, timestamppb
// timestamp, and tag families/tags in projection order (with NullTagValue for a
// missing projected tag).
//
// This is the standalone (§5.6b) in-process egress used by M6's local path. It
// does NOT apply cross-source dedup/order/limit — those are M4 operators applied
// upstream; this function preserves whatever order/dedup the batches already
// carry, matching the row egress' per-result behavior.
func BuildElementsFromVecBatches(ctx context.Context, source vecScanSource,
	projectionTags []model.TagProjection,
) (elements []*streamv1.Element, err error) {
	if source == nil {
		return nil, nil
	}
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		batch, batchErr := source.NextBatch(ctx)
		if batchErr != nil {
			return nil, batchErr
		}
		if batch == nil {
			return elements, nil
		}
		if batch.Len == 0 {
			continue
		}
		batchElements, buildErr := vstream.BuildElementsFromBatch(batch, projectionTags)
		if buildErr != nil {
			return nil, buildErr
		}
		elements = append(elements, batchElements...)
	}
}

// BuildElementsFromBatches materializes []*streamv1.Element from an already-drained
// slice of columnar batches (the M4 pipeline output). It is the egress used by the
// data-node standalone path — the batches hold the merged/deduped/limited result
// in memory rather than a live scan source. It delegates to the shared vec egress
// (vstream) so the columnar→proto conversion is identical to the liaison side.
func BuildElementsFromBatches(batches []*vectorized.RecordBatch,
	projectionTags []model.TagProjection,
) ([]*streamv1.Element, error) {
	return vstream.BuildElementsFromBatches(batches, projectionTags)
}
