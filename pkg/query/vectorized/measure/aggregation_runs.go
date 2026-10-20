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
	"github.com/apache/skywalking-banyandb/pkg/query/aggregation"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// expandConstInputs returns b, or a view of b in which every aggregation input
// column that is a ConstColumn (an entity tag that is the aggregation target, or a
// partial column of a reduce) is replaced by its expanded typed column. Key columns
// are not touched: they are encoded straight from the constant. The view shares b's
// rows, selection, hints and owners; the expanded columns are garbage collected.
func (a *BatchAggregation) expandConstInputs(b *vectorized.RecordBatch) *vectorized.RecordBatch {
	countIdx := []int(nil)
	if a.mode == AggModeReduce {
		countIdx = a.aggInputCountIdx
	}
	return expandConstColumns(b, a.aggs, countIdx)
}

// expandConstColumns is expandConstInputs for a list of aggregates and the optional
// partial-count column of each.
func expandConstColumns(b *vectorized.RecordBatch, aggs []AggSpec, countIdx []int) *vectorized.RecordBatch {
	var view *vectorized.RecordBatch
	expand := func(idx int) {
		if idx < 0 || idx >= len(b.Columns) {
			return
		}
		constCol, isConst := vectorized.AsConst(b.Columns[idx])
		if !isConst {
			return
		}
		if view == nil {
			cp := *b
			cp.Columns = append([]vectorized.Column(nil), b.Columns...)
			view = &cp
		}
		view.Columns[idx] = constCol.Expand()
	}
	for i, spec := range aggs {
		expand(spec.InputCol)
		if countIdx != nil {
			expand(countIdx[i])
		}
	}
	if view == nil {
		return b
	}
	return view
}

// consumeRuns folds b through the runs fast path when every key column is constant
// within the runs b describes. It reports false, having changed nothing, when it
// does not apply and the caller must fold row by row.
//
// It applies to AggModeAll and AggModeMap when no key is a time bucket (a bucketed
// aggregation is driven run by run by BatchTimeBucket) and each key column is a
// ConstColumn or an entity tag of a batch that knows where its series end. With no
// key at all the whole batch is one run.
func (a *BatchAggregation) consumeRuns(b *vectorized.RecordBatch) (bool, error) {
	if runsDisabled.Load() || a.mode == AggModeReduce || a.bucketIdx >= 0 {
		return false, nil
	}
	if b.Selection == nil && b.Len > maxNilSelectionLen {
		// Let the per-row path report the oversized batch.
		return false, nil
	}
	if a.keyEntity == nil {
		a.keyEntity = keyEntityFlags(a.inputSchema, a.keyIndices)
	}
	series, ok := keyConstancy(b, a.keyIndices, a.keyEntity, -1)
	if !ok || (series && tooFragmented(b)) {
		return false, nil
	}
	n := b.ActiveLen()
	if n == 0 {
		return true, nil
	}
	runs := a.runScratch[:0]
	if series {
		runs, _ = b.SeriesRuns(runs)
	} else {
		runs = append(runs, n)
	}
	a.runScratch = runs
	start := 0
	for _, end := range runs {
		if foldErr := a.foldRun(b, start, end, 0); foldErr != nil {
			return true, foldErr
		}
		start = end
	}
	return true, nil
}

// foldRun folds the active rows at positions [start, end) of b, which share one
// group, into that group: one key, one lookup, then a typed loop per aggregate.
// When the aggregation is bucketed, bucket is the (already floored) bucket start
// the run belongs to; the run's rows are not read for it.
func (a *BatchAggregation) foldRun(b *vectorized.RecordBatch, start, end int, bucket int64) error {
	sel := b.Selection
	a.runsFolded++
	if a.span != nil {
		a.rowsIn += int64(end - start)
	}
	keyRow := rowAt(sel, start)
	buf := a.keyScratch[:0]
	for _, kIdx := range a.keyIndices {
		if kIdx == a.bucketIdx {
			buf = appendIntKey(buf, bucket)
			continue
		}
		buf = appendKeyComponent(buf, b.Columns[kIdx], keyRow)
	}
	a.keyScratch = buf
	group, created, groupErr := a.groupFor(b, keyRow, buf)
	if groupErr != nil {
		return groupErr
	}
	if created && a.bucketIdx >= 0 {
		group.timeValue = bucket
	}
	for slotIdx, spec := range a.aggs {
		if foldErr := a.foldRange(b, start, end, &group.slots[slotIdx], spec); foldErr != nil {
			return foldErr
		}
	}
	return nil
}

// foldRange folds the values of spec's input column over the active rows at
// positions [start, end) of b into slot. The values are added in the order the
// rows are read, exactly as one fold call per row would add them, so the result is
// the same bit for bit. A column that cannot hold a null, which is the usual case,
// is folded by a typed loop with no per-row validity check.
func (a *BatchAggregation) foldRange(b *vectorized.RecordBatch, start, end int, slot *aggSlot, spec AggSpec) error {
	col := b.Columns[spec.InputCol]
	sel := b.Selection
	colType := a.inputSchema.Columns[spec.InputCol].Type
	numeric := colType == vectorized.ColumnTypeInt64 || colType == vectorized.ColumnTypeFloat64
	switch {
	case spec.Func == AggCountDistinct:
		return a.foldDistinctRange(col, sel, start, end, slot)
	case spec.Func == AggCount && !numeric:
		// COUNT over a string/bytes column counts the non-null rows and reads no value.
		cnt := end - start
		if mayHaveNulls(col) {
			cnt = 0
			for pos := start; pos < end; pos++ {
				if !col.IsNull(rowAt(sel, pos)) {
					cnt++
				}
			}
		}
		a.foldCount(slot, cnt)
		return nil
	case !numeric:
		return a.foldRows(b, start, end, slot, spec)
	}
	nullable := mayHaveNulls(col)
	switch {
	case slot.intMap != nil && !slot.inputIsFloat:
		foldNumericRange(slot.intMap, col.(*vectorized.TypedColumn[int64]).Data(), col, nullable, sel, start, end)
	case slot.floatMap != nil && slot.inputIsFloat:
		foldNumericRange(slot.floatMap, col.(*vectorized.TypedColumn[float64]).Data(), col, nullable, sel, start, end)
	case slot.floatMap != nil:
		// An int input accumulated as float (MEAN over an int tag).
		data := col.(*vectorized.TypedColumn[int64]).Data()
		for pos := start; pos < end; pos++ {
			row := rowAt(sel, pos)
			if nullable && col.IsNull(row) {
				continue
			}
			slot.floatMap.In(float64(data[row]))
		}
	default:
		return a.foldRows(b, start, end, slot, spec)
	}
	return nil
}

// foldRows is the per-row fold over positions [start, end): the fallback for a
// shape foldRange has no typed loop for.
func (a *BatchAggregation) foldRows(b *vectorized.RecordBatch, start, end int, slot *aggSlot, spec AggSpec) error {
	for pos := start; pos < end; pos++ {
		if foldErr := a.fold(b, rowAt(b.Selection, pos), slot, spec); foldErr != nil {
			return foldErr
		}
	}
	return nil
}

// foldDistinctRange inserts the non-null values of col at positions [start, end)
// into slot's distinct set, in row order.
func (a *BatchAggregation) foldDistinctRange(col vectorized.Column, sel []uint16, start, end int, slot *aggSlot) error {
	for pos := start; pos < end; pos++ {
		row := rowAt(sel, pos)
		if col.IsNull(row) {
			continue
		}
		if foldErr := a.foldDistinct(col, row, slot); foldErr != nil {
			return foldErr
		}
	}
	return nil
}

// foldCount adds cnt values to a COUNT slot over a column that has no numeric
// value to read.
func (a *BatchAggregation) foldCount(slot *aggSlot, cnt int) {
	if cnt <= 0 {
		return
	}
	if rf, ok := slot.intMap.(aggregation.RangeFolder[int64]); ok {
		for len(a.zeroScratch) < cnt {
			a.zeroScratch = append(a.zeroScratch, 0)
		}
		rf.InSlice(a.zeroScratch[:cnt])
		return
	}
	for range cnt {
		slot.intMap.In(0)
	}
}

// foldNumericRange folds the rows at positions [start, end) of data into m. A
// column with no null folds through the Map's range fold when it has one.
func foldNumericRange[N aggregation.Number](m aggregation.Map[N], data []N, col vectorized.Column, nullable bool,
	sel []uint16, start, end int,
) {
	if !nullable {
		if rf, ok := m.(aggregation.RangeFolder[N]); ok {
			if sel == nil {
				rf.InSlice(data[start:end])
			} else {
				rf.InSelected(data, sel[start:end])
			}
			return
		}
	}
	for pos := start; pos < end; pos++ {
		row := rowAt(sel, pos)
		if nullable && col.IsNull(row) {
			continue
		}
		m.In(data[row])
	}
}

// mayHaveNulls reports whether the per-row validity check can be skipped for col:
// false only for a column that proves it has no null.
func mayHaveNulls(col vectorized.Column) bool {
	if c, ok := col.(interface{ MayHaveNulls() bool }); ok {
		return c.MayHaveNulls()
	}
	return true
}
