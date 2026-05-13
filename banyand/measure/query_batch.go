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
	"container/heap"
	"context"
	"fmt"
	"sort"

	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/api/common"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	vmeasure "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure"
)

// Compile-time assertions that both result types satisfy the columnar
// interface alongside MeasureQueryResult.
var (
	_ model.MeasureBatchResult = (*queryResult)(nil)
	_ model.MeasureBatchResult = (*indexSortResult)(nil)
)

// PullBatch implements model.MeasureBatchResult for queryResult.
//
// G5b strict — single-block fast path: when the query produced exactly
// one block cursor, copyAllToBatch decodes raw stored bytes directly into
// the typed columns of *MeasureBatch, bypassing the *modelv1.TagValue /
// *modelv1.FieldValue intermediate that the row path's copyAllTo
// constructs. Multi-block queries fall back to the heap-merge that Pull()
// already implements and convert the resulting *MeasureResult via
// vmeasure.BuildMeasureBatchFromResult; the merge variant for batches is
// US-003 strict's remaining piece (planned alongside G6c operator wiring
// when batch-aware version-replacement / TopN aggregation become urgent).
//
// Mixing Pull() and PullBatch() on the same queryResult is undefined.
// They share qr.loaded / qr.data / qr.heap state; each advances the
// underlying cursors. Callers must pick one.
//
// Sticky-error contract matches Pull(): a context cancellation surfaces
// before load with the "interrupt: hit %d" form and during load with the
// "interrupt: blank/total=%d/%d" form. PullBatch propagates these as
// errors instead of as *MeasureResult{Error: …} — its return type is
// (*MeasureBatch, error).
func (qr *queryResult) PullBatch(_ context.Context) (*model.MeasureBatch, error) {
	if qr.batchSchema == nil {
		return nil, fmt.Errorf("queryResult.PullBatch: batchSchema not initialized; " +
			"the underlying query did not record a vectorized BatchSchema (likely a schema-build error at Query time)")
	}
	select {
	case <-qr.ctx.Done():
		return nil, errors.WithMessagef(qr.ctx.Err(), "interrupt: hit %d", qr.hit)
	default:
	}
	if loadErr := qr.loadCursorsForBatch(); loadErr != nil {
		return nil, loadErr
	}
	if len(qr.data) == 0 {
		return nil, nil
	}
	if len(qr.data) == 1 {
		bc := qr.data[0]
		b := newMeasureBatchForSchema(qr.batchSchema, len(bc.timestamps))
		bc.copyAllToBatch(b, qr.batchSchema, qr.storedIndexValue, qr.orderByTimestampDesc())
		qr.data = qr.data[:0]
		releaseBlockCursor(bc)
		return b, nil
	}
	// Multi-block: TopN aggregation requires mergeTopNResult which rewrites
	// field binary payloads via the row path's PostProcessor. The vectorized
	// batch path defers TopN merge support; fall back to the row path so
	// correctness is preserved. Non-topN queries take the native batch merge.
	if qr.topNQueryOptions != nil {
		r := qr.merge(qr.storedIndexValue, qr.tagProjection)
		if r == nil {
			return nil, nil
		}
		if r.Error != nil {
			return nil, r.Error
		}
		return vmeasure.BuildMeasureBatchFromResult(r, qr.batchSchema)
	}
	return qr.mergeBatch(qr.storedIndexValue, qr.batchSchema)
}

// mergeBatch is the batch-aware counterpart of queryResult.merge (query.go).
// It consumes the heap with the same semantics — version-replacement for
// duplicate timestamps within the same series, series-boundary stop — but
// writes directly into a *model.MeasureBatch via the typed column helpers in
// block_batch.go, bypassing the *modelv1.TagValue / *modelv1.FieldValue row
// intermediate that the original merge → BuildMeasureBatchFromResult path
// allocates.
//
// TopN aggregation (mergeTopNResult) is NOT implemented here; callers must
// check topNQueryOptions and fall back to the row path before calling
// mergeBatch. See the PullBatch dispatch above.
//
// Batch sizing: mergeBatch emits at most mergeBatchMaxRows rows per call so
// that large multi-series results do not exceed a reasonable working-set.
// The batchsource (BatchSourceFromBatchResult) slices the returned batch
// further to its configured batchSize, so any value here that is at least as
// large as the caller's batchSize is fine. 4096 is chosen conservatively.
const mergeBatchMaxRows = 4096

func (qr *queryResult) mergeBatch(
	storedIndexValue map[common.SeriesID]map[string]*modelv1.TagValue,
	schema *vectorized.BatchSchema,
) (*model.MeasureBatch, error) {
	if qr.Len() == 0 {
		return nil, nil
	}
	step := 1
	if qr.orderByTimestampDesc() {
		step = -1
	}

	b := newMeasureBatchForSchema(schema, mergeBatchMaxRows)
	var lastVersion int64
	var lastSid common.SeriesID

	for qr.Len() > 0 && b.RowCount() < mergeBatchMaxRows {
		topBC := qr.data[0]
		// Series boundary: stop and let the caller call again for the next series.
		if lastSid != 0 && topBC.bm.seriesID != lastSid {
			break
		}
		lastSid = topBC.bm.seriesID

		if b.RowCount() > 0 &&
			topBC.timestamps[topBC.idx] == b.Timestamps[len(b.Timestamps)-1] {
			// Duplicate timestamp within the same series: keep the higher version.
			if topBC.versions[topBC.idx] > lastVersion {
				topBC.replaceInBatch(b, schema, storedIndexValue)
				lastVersion = topBC.versions[topBC.idx]
			}
		} else {
			topBC.copyToBatch(b, schema, storedIndexValue)
			lastVersion = topBC.versions[topBC.idx]
		}

		topBC.idx += step

		if qr.orderByTimestampDesc() {
			if topBC.idx < 0 {
				heap.Pop(qr)
			} else {
				heap.Fix(qr, 0)
			}
		} else {
			if topBC.idx >= len(topBC.timestamps) {
				heap.Pop(qr)
			} else {
				heap.Fix(qr, 0)
			}
		}
	}

	if b.RowCount() == 0 {
		return nil, nil
	}
	return b, nil
}

// loadCursorsForBatch is the lazy load step replicated from Pull(). It is
// duplicated here (rather than extracted) so Pull's byte-identical
// behavior is unaffected by the G5b refactor — Pull and PullBatch are
// documented as mutually exclusive on a single queryResult, so the
// duplicated state machinery never observes a contended state.
func (qr *queryResult) loadCursorsForBatch() error {
	if qr.loaded {
		return nil
	}
	if len(qr.data) == 0 {
		return nil
	}
	cursorChan := make(chan int, len(qr.data))
	for i := 0; i < len(qr.data); i++ {
		go func(i int) {
			select {
			case <-qr.ctx.Done():
				cursorChan <- i
				return
			default:
			}
			tmpBlock := generateBlock()
			defer releaseBlock(tmpBlock)
			if !qr.data[i].loadData(tmpBlock) {
				cursorChan <- i
				return
			}
			if qr.orderByTimestampDesc() {
				qr.data[i].idx = len(qr.data[i].timestamps) - 1
			}
			cursorChan <- -1
		}(i)
	}
	blankCursorList := []int{}
	for completed := 0; completed < len(qr.data); completed++ {
		result := <-cursorChan
		if result != -1 {
			blankCursorList = append(blankCursorList, result)
		}
	}
	select {
	case <-qr.ctx.Done():
		return errors.WithMessagef(qr.ctx.Err(),
			"interrupt: blank/total=%d/%d", len(blankCursorList), len(qr.data))
	default:
	}
	sort.Slice(blankCursorList, func(i, j int) bool {
		return blankCursorList[i] > blankCursorList[j]
	})
	for _, index := range blankCursorList {
		qr.data = append(qr.data[:index], qr.data[index+1:]...)
	}
	qr.loaded = true
	heap.Init(qr)
	return nil
}

// PullBatch implements model.MeasureBatchResult for indexSortResult.
//
// Typed-column variant: appends rows from the segResult heap directly into a
// *model.MeasureBatch via copyToBatch, bypassing the *modelv1.TagValue /
// *MeasureResult intermediate that the row-path Pull() constructs. This
// mirrors what queryResult.mergeBatch does for blockCursor (committed in
// 36140ca9) but on the segResult side. Pull() and copyTo() are untouched —
// the row path remains byte-identical for legacy consumers (C1 invariant).
func (iqr *indexSortResult) PullBatch(_ context.Context) (*model.MeasureBatch, error) {
	if iqr.batchSchema == nil {
		return nil, fmt.Errorf("indexSortResult.PullBatch: batchSchema not initialized; " +
			"the underlying query did not record a vectorized BatchSchema (likely a schema-build error at Query time)")
	}
	return iqr.mergeSegResultsBatch(iqr.batchSchema, mergeBatchMaxRows)
}

// mergeSegResultsBatch is the batch-aware counterpart of indexSortResult.Pull.
// It pops from the segResult heap up to batchSize rows, calling copyToBatch
// for each row, then pushes the segResult back if it has more rows. This
// mirrors the heap-merge loop in Pull() but writes into typed columns instead
// of building *MeasureResult values.
func (iqr *indexSortResult) mergeSegResultsBatch(schema *vectorized.BatchSchema, batchSize int) (*model.MeasureBatch, error) {
	if len(iqr.segResults.results) == 0 {
		return nil, nil
	}
	b := newMeasureBatchForSchema(schema, batchSize)
	if len(iqr.segResults.results) == 1 {
		sr := iqr.segResults.results[0]
		for b.RowCount() < batchSize && sr.i < len(sr.SeriesList) {
			iqr.copyToBatch(b, schema, sr)
			sr.i++
		}
		if sr.i >= len(sr.SeriesList) {
			iqr.segResults.results = iqr.segResults.results[:0]
		}
		if b.RowCount() == 0 {
			return nil, nil
		}
		return b, nil
	}
	for b.RowCount() < batchSize && iqr.segResults.Len() > 0 {
		top := heap.Pop(&iqr.segResults)
		sr := top.(*segResult)
		iqr.copyToBatch(b, schema, sr)
		sr.i++
		if sr.i < len(sr.SeriesList) {
			heap.Push(&iqr.segResults, sr)
		}
	}
	if b.RowCount() == 0 {
		return nil, nil
	}
	return b, nil
}

// copyToBatch appends the single row at src.i into b's parallel slices and
// typed columns. It is the batch-path mirror of indexSortResult.copyTo
// (query.go) and is used by mergeSegResultsBatch when emitting rows.
//
// Tag resolution order matches copyTo:
//  1. If the tag name has an entry in tfl[i].projectedEntityOffsets, the value
//     is a pre-decoded *modelv1.TagValue from EntityValues — project via
//     appendTagValueAt (passthrough) or appendTagValueAsTyped (native).
//  2. Otherwise, look up the tag name in tfl[i].fieldToValueType to get the
//     raw bytes from the field result and decode via fillTagCell.
//
// Schema iteration follows the same tagIdx/fieldIdx convention as
// blockCursor.copyToBatch: Tags[tagIdx] aligns with the tagIdx-th RoleTag
// entry in schema.Columns, Fields[fieldIdx] with the fieldIdx-th RoleField.
func (iqr *indexSortResult) copyToBatch(b *model.MeasureBatch, schema *vectorized.BatchSchema, src *segResult) {
	rowIdx := src.i
	b.Timestamps = append(b.Timestamps, src.Timestamps[rowIdx])
	b.Versions = append(b.Versions, src.Versions[rowIdx])
	b.SeriesIDs = append(b.SeriesIDs, src.SeriesList[rowIdx].ID)
	// indexSortResult has no shardID; use zero value (matches copyTo behavior).
	b.ShardIDs = append(b.ShardIDs, 0)

	var fr storage.FieldResult
	if src.Fields != nil {
		fr = src.Fields[rowIdx]
	}

	// Build a fast lookup: family+tag → tfl entry index, for schema walking.
	// We walk schema.Columns in order to maintain tagIdx/fieldIdx alignment,
	// resolving each column via the tfl projection tables.
	tagIdx, fieldIdx := 0, 0
	for _, def := range schema.Columns {
		switch def.Role {
		case vectorized.RoleTag:
			col := b.Tags[tagIdx]
			tagIdx++
			iqr.fillSegTagCell(col, def, src, rowIdx, fr)
		case vectorized.RoleField:
			col := b.Fields[fieldIdx]
			fieldIdx++
			iqr.fillSegFieldCell(col, def, fr)
		case vectorized.RoleTimestamp, vectorized.RoleVersion,
			vectorized.RoleSeriesID, vectorized.RoleShardID:
			// Metadata handled via the parallel slices above.
		}
	}
}

// fillSegTagCell resolves one tag column cell for the given schema column def
// from a segResult row. Resolution order matches copyTo:
//  1. projectedEntityOffsets: pre-decoded *modelv1.TagValue from EntityValues.
//  2. fieldToValueType: raw bytes from the field result map.
//  3. Neither found: null-fill.
func (iqr *indexSortResult) fillSegTagCell(col vectorized.Column, def vectorized.ColumnDef,
	src *segResult, rowIdx int, fr storage.FieldResult,
) {
	for tfIdx := range iqr.tfl {
		if iqr.tfl[tfIdx].name != def.TagFamily {
			continue
		}
		tfl := &iqr.tfl[tfIdx]
		if offset, ok := tfl.projectedEntityOffsets[def.Name]; ok {
			appendTagValueAt(col, def, src.SeriesList[rowIdx].EntityValues[offset])
			return
		}
		if fr == nil {
			appendNullTagN(col, def, 1)
			return
		}
		if tnt, ok := tfl.fieldToValueType[def.Name]; ok {
			fillTagCell(col, def, tnt.typ, fr[tnt.fieldName])
			return
		}
		appendNullTagN(col, def, 1)
		return
	}
	appendNullTagN(col, def, 1)
}

// fillSegFieldCell resolves one field column cell. Fields are stored in the
// FieldResult map; the schema def carries the field name and column type.
// If the field is absent or fr is nil, null-fill.
func (iqr *indexSortResult) fillSegFieldCell(col vectorized.Column, def vectorized.ColumnDef, fr storage.FieldResult) {
	if fr == nil {
		appendNullFieldN(col, def, 1)
		return
	}
	// Fields in the FieldResult are keyed by the marshaled index key (tnt.fieldName).
	// Walk all tfl entries to find the field's valueType.
	for tfIdx := range iqr.tfl {
		if tnt, ok := iqr.tfl[tfIdx].fieldToValueType[def.Name]; ok {
			fillFieldCell(col, def, tnt.typ, fr[tnt.fieldName])
			return
		}
	}
	appendNullFieldN(col, def, 1)
}
