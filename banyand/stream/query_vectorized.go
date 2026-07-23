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
	cur            *blockCursor
	segments       []storage.Segment[*tsTable, option]
	series         []*pbv1.Series
	pending        []*blockCursor
	orderTagFamily string
	orderTagName   string
	budgetErr      error
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
		if v.cur == nil {
			bc, err := v.nextCursor(ctx)
			if err != nil {
				return nil, err
			}
			if bc == nil {
				return nil, nil
			}
			v.cur = bc
			v.curOff = 0
		}
		batch, err := v.cursorToBatch(v.cur, v.curOff)
		if err != nil {
			releaseBlockCursor(v.cur)
			v.cur = nil
			return nil, err
		}
		v.curOff += batch.Len
		if v.curOff >= len(v.cur.timestamps) {
			releaseBlockCursor(v.cur)
			v.cur = nil
		}
		if batch.Len == 0 {
			continue
		}
		return batch, nil
	}
}

// nextCursor drains loaded blockCursors, refilling from the block scanner /
// segment list on demand. It reuses the exact scan helpers the row path uses
// (searchSeries, getBlockScanner, loadBlockCursor) — it does not duplicate their
// logic. Returned cursors are positioned at their first row (asc) or last row
// (desc); callers must releaseBlockCursor.
func (v *streamVecScan) nextCursor(ctx context.Context) (*blockCursor, error) {
	for {
		if len(v.pending) > 0 {
			bc := v.pending[0]
			v.pending = v.pending[1:]
			return bc, nil
		}
		// Soft byte budget exceeded: after the already-loaded cursors drain, fail
		// loud rather than returning a partial result (row-path parity — the row
		// path errors under memory pressure via the protector).
		if v.budgetErr != nil {
			return nil, v.budgetErr
		}
		if v.ts != nil {
			if err := v.fillFromScanner(ctx); err != nil {
				return nil, err
			}
			if len(v.pending) > 0 {
				continue
			}
			// Scanner drained for this segment.
			if len(v.ts.parts) == 0 {
				v.ts.close()
				v.ts = nil
			}
			continue
		}
		if len(v.segments) == 0 {
			return nil, nil
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
			return nil, err
		}
		ts, err := getBlockScanner(ctx, segment, qo, v.sm.l, v.sm.pm, v.tr)
		if err != nil {
			return nil, err
		}
		if ts == nil {
			continue
		}
		v.ts = ts
	}
}

// fillFromScanner pulls one round of block-scan batches and materializes them
// into loaded blockCursors appended to v.pending.
func (v *streamVecScan) fillFromScanner(ctx context.Context) error {
	batchCh := make(chan *blockScanResultBatch, 1)
	scanErrCh := make(chan struct{})
	go func() {
		defer close(scanErrCh)
		v.ts.scan(ctx, batchCh)
		close(batchCh)
	}()
	is := v.sm.indexSchema.Load().(indexSchema)
	var scanErr error
	for batch := range batchCh {
		if batch.err != nil {
			if scanErr == nil {
				scanErr = batch.err
			}
			releaseBlockScanResultBatch(batch)
			continue
		}
		for i := range batch.bss {
			bs := &batch.bss[i]
			// Metadata preflight for the QueryMemoryMiB soft budget: predict whether
			// decoding this block would push cumulative uncompressed bytes over the
			// budget and, if so, FAIL LOUD before the expensive decode. The first block
			// always decodes (first-block exception) so a too-small budget never
			// rejects the whole query. When no budget is set (v.mem == nil) every block
			// is decoded; the block scanner's protector.Memory quota still bounds it.
			if v.mem != nil {
				blockBytes := int64(bs.bm.uncompressedSizeBytes)
				if v.budgetEngaged {
					if reserveErr := v.mem.Reserve(blockBytes); reserveErr != nil {
						if v.budgetErr == nil {
							v.budgetErr = fmt.Errorf("%w: %w", ErrQueryMemoryBudgetExceeded, reserveErr)
						}
						continue
					}
				} else {
					_ = v.mem.Reserve(blockBytes)
					v.budgetEngaged = true
				}
			}
			bc := generateBlockCursor()
			bc.init(bs.p, &bs.bm, bs.qo)
			if loadBlockCursor(bc, v.tmpBlock, bs.qo, is) {
				if !v.asc {
					bc.idx = len(bc.timestamps) - 1
				} else {
					bc.idx = 0
				}
				v.pending = append(v.pending, bc)
			}
		}
		releaseBlockScanResultBatch(batch)
	}
	<-scanErrCh
	return scanErr
}

// cursorToBatch converts a window [off, off+n) of a loaded blockCursor into a
// columnar batch, where n is capped at batchSize. It reads the rows into the
// batch columns using the exact decode copyTo uses (schemaType match →
// mustDecodeTagValue; else NullTagValue). Reading a window (not always [0,len))
// lets NextBatch drain a cursor larger than batchSize across several batches
// without losing rows.
//
//nolint:unparam // error is part of the batch-builder contract and reserved for future decode failures.
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
					tv := orderTagCol.Data()[row]
					if b, mErr := pbv1.MarshalTagValue(tv); mErr == nil {
						keyBytes = b
					}
				}
				orderCol.Append(keyBytes)
			}
		}
	}

	batch.Len = n
	return batch, nil
}

// Release frees the scan source's resources: the block scanner, the temp block,
// any pending cursors, and the retained segment references.
func (v *streamVecScan) Release() {
	if v.closed {
		return
	}
	v.closed = true
	if v.cur != nil {
		releaseBlockCursor(v.cur)
		v.cur = nil
	}
	for _, bc := range v.pending {
		releaseBlockCursor(bc)
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
func (s *stream) queryVectorized(_ context.Context, sqo model.StreamQueryOptions) (vecScanSource, error) {
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
	// fillFromScanner (first-block exception). This is independent of, and in
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
