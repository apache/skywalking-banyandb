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
// indexSortResult.Pull() yields single-row MeasureResults; the conversion
// shape is identical to queryResult.PullBatch's multi-block fallback —
// single-row batches that the vectorized adapter accumulates into larger
// pipeline batches. A direct typed-column variant (skipping the
// *modelv1.TagValue intermediate) is deferred to a follow-on alongside
// the queryResult batch-merge work; indexSortResult uses storage's
// segResult instead of blockCursor, so the typed emit path here is its
// own ~150 LOC parallel implementation.
func (iqr *indexSortResult) PullBatch(_ context.Context) (*model.MeasureBatch, error) {
	if iqr.batchSchema == nil {
		return nil, fmt.Errorf("indexSortResult.PullBatch: batchSchema not initialized; " +
			"the underlying query did not record a vectorized BatchSchema (likely a schema-build error at Query time)")
	}
	r := iqr.Pull()
	if r == nil {
		return nil, nil
	}
	if r.Error != nil {
		return nil, r.Error
	}
	return vmeasure.BuildMeasureBatchFromResult(r, iqr.batchSchema)
}
