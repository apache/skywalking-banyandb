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
	"fmt"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// appendBorrowedRow delays output allocation while winners form one contiguous
// range. Interleaving materializes the pending range into the existing copy path
// so irregular winners still produce bounded, full-sized output batches.
func (e *distributedRowEmitter) appendBorrowedRow(row *distributedRowItem) (bool, error) {
	switch {
	case e.pendingRows == 0:
		e.pending, e.pendingStart, e.pendingRows = row.batch, row.rowIdx, 1
	case e.pending == row.batch && row.rowIdx == e.pendingStart+e.pendingRows:
		e.pendingRows++
	default:
		e.current = e.pool.Get()
		e.current.Owners.PinAll(&e.pending.Owners)
		for col, source := range e.pending.Columns {
			if err := appendColumnRangeCoerced(e.current.Columns[col], source, e.pendingStart, e.pendingRows); err != nil {
				return false, err
			}
		}
		e.current.Len = e.pendingRows
		e.pending, e.pendingRows = nil, 0
		return false, nil
	}
	if e.pendingRows == e.batchSize {
		return true, e.finalizeBorrowedRun()
	}
	return true, nil
}

func (e *distributedRowEmitter) finalizeBorrowedRun() error {
	amount := int64(e.pendingRows) * e.rowWidth
	if e.tracker != nil {
		if err := e.tracker.Reserve(amount); err != nil {
			return err
		}
		defer e.tracker.Release(amount)
	}
	batch := vectorized.NewRecordBatch(e.schema, 0)
	batch.Len = e.pendingRows
	batch.Owners.PinAll(&e.pending.Owners)
	for col, source := range e.pending.Columns {
		if err := borrowScalarRange(batch.Columns[col], source, e.pendingStart, e.pendingRows); err != nil {
			batch.ReleaseOwners()
			return err
		}
	}
	e.output = append(e.output, batch)
	e.pending, e.pendingRows = nil, 0
	return nil
}

func borrowScalarRange(destination, source vectorized.Column, start, rows int) error {
	end := start + rows
	switch typed := source.(type) {
	case *vectorized.TypedColumn[int64]:
		destination.(*vectorized.TypedColumn[int64]).Borrow(typed.Data()[start:end:end])
	case *vectorized.TypedColumn[float64]:
		destination.(*vectorized.TypedColumn[float64]).Borrow(typed.Data()[start:end:end])
	case *vectorized.TypedColumn[string]:
		destination.(*vectorized.TypedColumn[string]).Borrow(typed.Data()[start:end:end])
	case *vectorized.TypedColumn[[]byte]:
		destination.(*vectorized.TypedColumn[[]byte]).Borrow(typed.Data()[start:end:end])
	default:
		return fmt.Errorf("native scalar borrow cannot consume %T", source)
	}
	for row := range rows {
		if source.IsNull(start + row) {
			destination.MarkNullAt(row)
		}
	}
	return nil
}
