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

	"github.com/apache/skywalking-banyandb/pkg/query/model"
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
	// Multi-block: heap-merge via Pull's existing path, then convert.
	// The protobuf intermediate is transient here — eliminating it
	// requires a batch-aware merge variant deferred to a follow-on.
	r := qr.merge(qr.storedIndexValue, qr.tagProjection)
	if r == nil {
		return nil, nil
	}
	if r.Error != nil {
		return nil, r.Error
	}
	return vmeasure.BuildMeasureBatchFromResult(r, qr.batchSchema)
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
