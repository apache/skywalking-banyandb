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

package plan

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"

	itersort "github.com/apache/skywalking-banyandb/pkg/iter/sort"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	measure "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure/frame"
)

// distributedRowsSpec configures mergeDistributedRows.
//
// OrderByFamily / OrderByTagName select the sort column when the request
// carries an OrderBy.IndexRuleName (Phase 2). Both empty means time-sort:
// the comparator uses the timestamp column encoded as 8-byte big-endian,
// matching the Phase 1.5 behaviour byte-for-byte. The merger looks up the
// column index from the merged-batch schema after frames have been decoded,
// so the spec only carries the names — not a column index.
//
// OrderByColIdx is an optional explicit column override used by unit tests
// that build their own RecordBatch and want to point the merger at a known
// column without going through TagIndex lookup. The merger prefers the
// explicit index when >= 0; otherwise it resolves from the names.
type distributedRowsSpec struct {
	OrderByFamily  string
	OrderByTagName string
	Tracker        *vectorized.MemoryTracker
	BatchSize      int
	OrderByColIdx  int
	Desc           bool
	IndexMode      bool
}

// distributedRowKey keys the per-window dedup map. Two rows with the same
// (sid, ts) inside one sort-group represent the same logical record arriving
// from different sources; the surviving row is the one with the highest
// version.
type distributedRowKey struct {
	sid int64
	ts  int64
}

// distributedRowItem is the unit the k-way heap merger emits — one per source
// row. SortedField() returns the encoded sort key: under time-sort
// (Phase 1.5) that is the timestamp encoded as 8-byte big-endian; under
// OrderBy-by-index-rule (Phase 2) that is the type-dispatched encoding
// produced by encodeSortKey for the resolved OrderBy column. The itersort
// heap's lex compare matches numeric compare for ascending order and, when
// desc=true, reverse-numeric for descending order.
type distributedRowItem struct {
	batch     *vectorized.RecordBatch
	sortField []byte
	rowIdx    int
	source    int
	seq       int
	sid       int64
	ts        int64
	ver       int64
}

// SortedField implements itersort.Comparable.
func (r *distributedRowItem) SortedField() []byte { return r.sortField }

// distributedRowSourceIter walks the active rows of one decoded source batch
// in order and emits one distributedRowItem per row. The merger across all
// sources is built by handing N of these to itersort.NewItemIter. When the
// merger is configured for OrderBy-by-index-rule, sortKeys caches the
// per-row encoded sort bytes so the local sort and Next()'s SortedField
// both reuse the same encoding without re-marshalling per call.
type distributedRowSourceIter struct {
	batch        *vectorized.RecordBatch
	cur          *distributedRowItem
	sortKeys     [][]byte
	indices      []int
	source       int
	sidIdx       int
	tsIdx        int
	verIdx       int
	sortColIdx   int
	pos          int
	seq          int
}

// newDistributedRowSourceIter constructs the per-source iterator. When
// sortColIdx is < 0 (time-sort, Phase 1.5 default), the iterator falls
// through to the original int64 timestamp comparison. Otherwise it
// pre-encodes each active row's sort key via encodeSortKey, stable-sorts
// indices by that encoding (respecting desc), and reuses the cached keys
// in Next() to avoid re-encoding per heap pop.
func newDistributedRowSourceIter(batch *vectorized.RecordBatch, schema *vectorized.BatchSchema, source int, desc bool, sortColIdx int) (*distributedRowSourceIter, error) {
	tsIdx := schema.TimestampIndex()
	indices := activeDistributedRows(batch)
	iter := &distributedRowSourceIter{
		batch:      batch,
		source:     source,
		sidIdx:     schema.SeriesIDIndex(),
		tsIdx:      tsIdx,
		verIdx:     schema.VersionIndex(),
		sortColIdx: sortColIdx,
		indices:    indices,
	}
	if sortColIdx >= 0 {
		sortKeys := make([][]byte, batch.Len)
		for _, rowIdx := range indices {
			key, encodeErr := encodeSortKey(batch.Columns[sortColIdx], rowIdx)
			if encodeErr != nil {
				return nil, encodeErr
			}
			sortKeys[rowIdx] = key
		}
		iter.sortKeys = sortKeys
		// Per-source local sort by the OrderBy column's encoded key so the
		// heap's lex compare gives the right global order. Data nodes emit
		// per-shard pre-sorted on this same column, but tests and unusual
		// scan modes may not respect that, so the defensive O(n log n) sort
		// keeps the heap invariant intact regardless of producer behaviour.
		sort.SliceStable(indices, func(i, j int) bool {
			cmp := bytes.Compare(sortKeys[indices[i]], sortKeys[indices[j]])
			if desc {
				return cmp > 0
			}
			return cmp < 0
		})
		return iter, nil
	}
	// Time-sort fallback (Phase 1.5 invariant): comparator uses the int64
	// timestamp column directly so the existing memory-ceiling regression
	// gate continues to hold and existing tests stay byte-equivalent.
	tsCol := batch.Columns[tsIdx].(*vectorized.TypedColumn[int64])
	tsData := tsCol.Data()
	sort.SliceStable(indices, func(i, j int) bool {
		if desc {
			return tsData[indices[i]] > tsData[indices[j]]
		}
		return tsData[indices[i]] < tsData[indices[j]]
	})
	return iter, nil
}

// Next implements itersort.Iterator.
//
// Per-row allocation cost — known follow-up. Each call allocates a fresh
// sortField []byte (8 bytes) and a fresh *distributedRowItem (~64 bytes).
// In-place mutation of s.cur is unsafe because the itersort heap retains
// previously-popped items in stored containers — reusing the struct would
// alias every stored item in the heap to the latest values. A per-source
// object pool plumbed through flushWindow's Release lifecycle would
// recycle these structs; that's deferred to phase 1.6 to keep the phase 1.5
// diff focused on the architectural shift (k-way heap merge + zero-copy
// column copy) rather than micro-allocation tuning.
// TODO(phase-1.6): pool *distributedRowItem and reuse a per-iterator
// sortField buffer ([8]byte) so the per-row alloc cost drops to zero in
// steady state.
func (s *distributedRowSourceIter) Next() bool {
	if s.pos >= len(s.indices) {
		return false
	}
	rowIdx := s.indices[s.pos]
	s.pos++
	sidCol := s.batch.Columns[s.sidIdx].(*vectorized.TypedColumn[int64])
	tsCol := s.batch.Columns[s.tsIdx].(*vectorized.TypedColumn[int64])
	verCol := s.batch.Columns[s.verIdx].(*vectorized.TypedColumn[int64])
	var sortField []byte
	if s.sortColIdx >= 0 {
		// Phase 2: reuse the cached OrderBy sort key. encodeSortKey was
		// already called in newDistributedRowSourceIter so the heap's
		// equal-window detection (bytes.Equal on sortField) sees the same
		// bytes the local sort compared on.
		sortField = s.sortKeys[rowIdx]
	} else {
		sortField = make([]byte, 8)
		binary.BigEndian.PutUint64(sortField, uint64(tsCol.Data()[rowIdx]))
	}
	s.cur = &distributedRowItem{
		batch:     s.batch,
		rowIdx:    rowIdx,
		source:    s.source,
		seq:       s.seq,
		sid:       sidCol.Data()[rowIdx],
		ts:        tsCol.Data()[rowIdx],
		ver:       verCol.Data()[rowIdx],
		sortField: sortField,
	}
	s.seq++
	return true
}

// Val implements itersort.Iterator.
func (s *distributedRowSourceIter) Val() *distributedRowItem { return s.cur }

// Close implements itersort.Iterator. No resources to release — source
// batches are owned by the caller (mergeDistributedRows) and freed when the
// returned output is consumed.
func (s *distributedRowSourceIter) Close() error { return nil }

// mergeDistributedRows is the liaison-side non-aggregation row merger. It
// k-way heap-merges per-source frame batches by timestamp, dedups (sid, ts)
// inside each equal-sort-field window keeping the highest-version row, and
// streams output batches via a BatchPool. Output columns reference source
// columns by value (typed scalars) or by reference (passthrough slice / proto
// pointer cells), preserving the BatchSourceFromBatchResult zero-copy
// contract.
//
// Memory bound: peak in-flight ref count is one sort-group's worth of rows
// plus one output batch — not total input rows. Each output batch reserves
// bytes from spec.Tracker on the boundary between build and append, so the
// tracker bounds the in-transit working set rather than under-counting the
// flat-sort scratch the previous shape allocated up-front.
func mergeDistributedRows(frames [][]byte, spec distributedRowsSpec) ([]*vectorized.RecordBatch, error) {
	sources, schema, decodeErr := decodeDistributedRowSources(frames)
	if decodeErr != nil {
		return nil, decodeErr
	}
	if schema == nil {
		return nil, nil
	}
	if schema.SeriesIDIndex() < 0 || schema.TimestampIndex() < 0 || schema.VersionIndex() < 0 {
		return nil, fmt.Errorf("row merge requires series-id, timestamp, and version columns")
	}
	batchSize := spec.BatchSize
	if batchSize <= 0 {
		batchSize = vectorized.DefaultBatchSize
	}

	sortColIdx, sortErr := resolveDistributedSortColumn(schema, spec)
	if sortErr != nil {
		return nil, sortErr
	}
	iters, buildErr := buildDistributedRowSourceIters(sources, schema, spec.Desc, sortColIdx)
	if buildErr != nil {
		return nil, buildErr
	}
	merger := itersort.NewItemIter(iters, spec.Desc)
	defer func() { _ = merger.Close() }()

	pool := vectorized.NewBatchPool(schema, batchSize)
	rowWidth := estimateRowWidth(schema)

	emitter := &distributedRowEmitter{
		pool:      pool,
		schema:    schema,
		batchSize: batchSize,
		tracker:   spec.Tracker,
		rowWidth:  rowWidth,
		seenSID:   make(map[int64]struct{}),
		indexMode: spec.IndexMode,
		window:    make(map[distributedRowKey]*distributedRowItem),
	}

	for merger.Next() {
		item := merger.Val()
		if emitter.windowKey != nil && !bytes.Equal(item.sortField, emitter.windowKey) {
			if err := emitter.flushWindow(); err != nil {
				return nil, err
			}
		}
		if emitter.windowKey == nil {
			emitter.windowKey = item.sortField
		}
		emitter.accept(item)
	}
	if err := emitter.flushWindow(); err != nil {
		return nil, err
	}
	if err := emitter.finalizeBatch(); err != nil {
		return nil, err
	}
	return emitter.output, nil
}

// buildDistributedRowSourceIters wires one iterator per non-empty source
// batch. The k-way merger consumes these directly. desc is forwarded so each
// per-source iterator sorts its own row indices in the same direction the
// heap expects (largest-first for desc, smallest-first for asc). sortColIdx
// selects the comparator: < 0 means time-sort (Phase 1.5), >= 0 means the
// OrderBy column at that index in the merged-batch schema (Phase 2).
func buildDistributedRowSourceIters(sources [][]*vectorized.RecordBatch, schema *vectorized.BatchSchema, desc bool, sortColIdx int) ([]itersort.Iterator[*distributedRowItem], error) {
	iters := make([]itersort.Iterator[*distributedRowItem], 0, len(sources))
	for sourceIdx, batches := range sources {
		for _, batch := range batches {
			iter, buildErr := newDistributedRowSourceIter(batch, schema, sourceIdx, desc, sortColIdx)
			if buildErr != nil {
				return nil, buildErr
			}
			iters = append(iters, iter)
		}
	}
	return iters, nil
}

// resolveDistributedSortColumn returns the column index used by the heap
// merger for the equal-sort-field window. Explicit spec.OrderByColIdx (>= 0)
// wins for tests that bypass schema lookup. Otherwise an OrderBy resolution
// uses (family, tag) → TagIndex on the merged-batch schema. Both empty
// means time-sort (Phase 1.5), returning -1 so the iterator falls through
// to the int64 timestamp comparator.
func resolveDistributedSortColumn(schema *vectorized.BatchSchema, spec distributedRowsSpec) (int, error) {
	if spec.OrderByColIdx >= 0 {
		if spec.OrderByColIdx >= len(schema.Columns) {
			return -1, fmt.Errorf("row merge: OrderByColIdx %d out of range (%d columns)", spec.OrderByColIdx, len(schema.Columns))
		}
		return spec.OrderByColIdx, nil
	}
	if spec.OrderByTagName == "" {
		return -1, nil
	}
	colIdx, ok := schema.TagIndex(spec.OrderByFamily, spec.OrderByTagName)
	if !ok {
		return -1, fmt.Errorf("row merge: OrderBy tag %s.%s not found in merged-batch schema", spec.OrderByFamily, spec.OrderByTagName)
	}
	return colIdx, nil
}

// distributedRowEmitter is the per-merge mutable state: the active sort-group
// dedup map, the current in-progress output batch, the cross-window seenSID
// guard for index-mode queries, and the returned batch list.
type distributedRowEmitter struct {
	pool         *vectorized.BatchPool
	schema       *vectorized.BatchSchema
	tracker      *vectorized.MemoryTracker
	seenSID      map[int64]struct{}
	window       map[distributedRowKey]*distributedRowItem
	current      *vectorized.RecordBatch
	windowKey    []byte
	output       []*vectorized.RecordBatch
	batchSize    int
	rowWidth     int64
	indexMode    bool
}

// accept adds an incoming row to the current sort-group dedup map. The window
// retains the highest-version row per (sid, ts) — matching
// sortedMIterator.loadOneGroup's semantics so flag-on output matches the
// row-path baseline.
func (e *distributedRowEmitter) accept(item *distributedRowItem) {
	key := distributedRowKey{sid: item.sid, ts: item.ts}
	existing, ok := e.window[key]
	if !ok || item.ver > existing.ver {
		e.window[key] = item
	}
}

// flushWindow dedups the current sort-group and appends surviving rows to the
// output stream in stable (source, seq) order, applying index-mode sid
// suppression across groups when configured.
//
// Emit-order note: sortedMIterator.loadOneGroup in the row-path baseline
// (pkg/query/logical/measure/measure_plan_distributed.go) iterates its
// uniqueData map in Go map order, which is intentionally randomised by the
// runtime. The vec path here imposes a deterministic (source, seq) order via
// sort.SliceStable on the window's surviving rows so equal-sort-field output
// is reproducible across reruns and process restarts. This is strictly
// stronger than the row-path baseline; both shapes are correct, but the vec
// shape is the one a fixture diff can rely on.
func (e *distributedRowEmitter) flushWindow() error {
	if len(e.window) == 0 {
		return nil
	}
	emit := make([]*distributedRowItem, 0, len(e.window))
	for _, row := range e.window {
		emit = append(emit, row)
	}
	sort.SliceStable(emit, func(i, j int) bool {
		if emit[i].source == emit[j].source {
			return emit[i].seq < emit[j].seq
		}
		return emit[i].source < emit[j].source
	})
	for _, row := range emit {
		if e.indexMode {
			if _, dup := e.seenSID[row.sid]; dup {
				continue
			}
			e.seenSID[row.sid] = struct{}{}
		}
		if err := e.appendRowToCurrent(row); err != nil {
			return err
		}
	}
	clear(e.window)
	e.windowKey = nil
	return nil
}

// appendRowToCurrent zero-copies one source row into the active output batch
// via measure.AppendColumnRange. When the batch fills, it is finalised (which
// reserves memory tracker bytes, appends to the output, then releases the
// reservation so the tracker bounds in-transit batches without
// double-counting once they reach the caller).
func (e *distributedRowEmitter) appendRowToCurrent(row *distributedRowItem) error {
	if e.current == nil {
		e.current = e.pool.Get()
	}
	for colIdx, srcCol := range row.batch.Columns {
		if err := measure.AppendColumnRange(e.current.Columns[colIdx], srcCol, row.rowIdx, 1); err != nil {
			return fmt.Errorf("merge column %d: %w", colIdx, err)
		}
	}
	e.current.Len++
	if e.current.Len >= e.batchSize {
		return e.finalizeBatch()
	}
	return nil
}

// finalizeBatch reserves working-set bytes for the current batch, appends it
// to the output, and releases the reservation. Net effect: while the merger
// holds an in-progress batch we have one batch worth of bytes reserved.
func (e *distributedRowEmitter) finalizeBatch() error {
	if e.current == nil || e.current.Len == 0 {
		if e.current != nil {
			e.pool.Put(e.current)
			e.current = nil
		}
		return nil
	}
	estBytes := int64(e.current.Len) * e.rowWidth
	if e.tracker != nil {
		if reserveErr := e.tracker.Reserve(estBytes); reserveErr != nil {
			e.pool.Put(e.current)
			e.current = nil
			return reserveErr
		}
	}
	e.output = append(e.output, e.current)
	if e.tracker != nil {
		e.tracker.Release(estBytes)
	}
	e.current = nil
	return nil
}

// estimateRowWidth returns a conservative per-row byte estimate for memory
// accounting. Fixed-width column types use their exact width; variable-width
// columns charge a small constant so the tracker's bound stays predictable
// across cardinalities.
func estimateRowWidth(schema *vectorized.BatchSchema) int64 {
	var width int64
	for _, def := range schema.Columns {
		switch def.Type {
		case vectorized.ColumnTypeInt64, vectorized.ColumnTypeFloat64:
			width += 8
		case vectorized.ColumnTypeString, vectorized.ColumnTypeBytes:
			width += 32
		case vectorized.ColumnTypeInt64Array, vectorized.ColumnTypeStrArray:
			width += 48
		case vectorized.ColumnTypeTagValue, vectorized.ColumnTypeFieldValue:
			width += 16
		default:
			width += 16
		}
	}
	if width == 0 {
		width = 16
	}
	return width
}

// decodeDistributedRowSources decodes every non-empty frame body into a
// RecordBatch and groups them per source. Schema parity across sources is
// asserted — a frame produced under a divergent schema means a producer
// mis-rollout, not a recoverable data error.
func decodeDistributedRowSources(frames [][]byte) ([][]*vectorized.RecordBatch, *vectorized.BatchSchema, error) {
	sources := make([][]*vectorized.RecordBatch, 0, len(frames))
	var schema *vectorized.BatchSchema
	for frameIdx, body := range frames {
		if len(body) == 0 {
			continue
		}
		batch, decodeErr := frame.Decode(body)
		if decodeErr != nil {
			return nil, nil, fmt.Errorf("decode frame %d: %w", frameIdx, decodeErr)
		}
		if batch == nil || batch.ActiveLen() == 0 {
			continue
		}
		if schema == nil {
			schema = batch.Schema
		} else if !distributedRowSchemasEqual(schema, batch.Schema) {
			return nil, nil, fmt.Errorf("frame %d schema mismatch", frameIdx)
		}
		sources = append(sources, []*vectorized.RecordBatch{batch})
	}
	return sources, schema, nil
}

// activeDistributedRows returns the in-order list of active row indices for a
// decoded batch. Selection-aware: when Selection is nil every row in
// [0, Len) is active; otherwise the listed indices in their declared order.
func activeDistributedRows(batch *vectorized.RecordBatch) []int {
	if batch.Selection == nil {
		rows := make([]int, batch.Len)
		for rowIdx := range rows {
			rows[rowIdx] = rowIdx
		}
		return rows
	}
	rows := make([]int, len(batch.Selection))
	for i, rowIdx := range batch.Selection {
		rows[i] = int(rowIdx)
	}
	return rows
}

// distributedRowSchemasEqual asserts two BatchSchemas describe the same
// column layout. Used as a defensive cross-source shape check.
func distributedRowSchemasEqual(a, b *vectorized.BatchSchema) bool {
	if a == b {
		return true
	}
	if a == nil || b == nil || len(a.Columns) != len(b.Columns) {
		return false
	}
	for i, ac := range a.Columns {
		bc := b.Columns[i]
		if ac.Name != bc.Name || ac.TagFamily != bc.TagFamily || ac.Role != bc.Role || ac.Type != bc.Type {
			return false
		}
	}
	return true
}
