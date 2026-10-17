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
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

package ql

import (
	"context"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

type filterOperator struct {
	source vectorized.PullOperator
	// The kernel supplies compilation, scratch and reservation accounting only.
	// The filter owns source lifecycle and releases kernel state without Close.
	kernel         *expressionOperator
	predicate      *expressionNode
	schema         *vectorized.BatchSchema
	selection      []uint16
	view           vectorized.RecordBatch
	selectionBytes int64
	stats          FilterStats
	started        bool
	closed         bool
}

// WrapFilter owns source on every return path and lends its columns with a
// narrowed physical selection. Only TRUE passes; FALSE and NULL do not pass.
func WrapFilter(ctx context.Context, source vectorized.PullOperator, predicate *Expression) (_ vectorized.PullOperator, err error) {
	defer func() {
		if err != nil && source != nil {
			_ = source.Close()
		}
	}()
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	if source == nil || source.OutputSchema() == nil {
		return nil, expressionError("filter requires a source schema")
	}
	kernel := &expressionOperator{source: source, tracker: vectorized.QueryMemoryTracker(ctx, 256<<20), filter: true}
	defer func() {
		if err != nil {
			kernel.release()
		}
	}()
	if err = kernel.reserve(2048); err != nil {
		return nil, err
	}
	bound, boundErr := expressionCloneBound(predicate, 0)
	if boundErr != nil {
		return nil, boundErr
	}
	if err = kernel.reserve(bound); err != nil {
		return nil, err
	}
	resolved, resolveErr := ResolvePredicate(predicate)
	if resolveErr != nil {
		return nil, resolveErr
	}
	node, compileErr := kernel.compile(resolved)
	if compileErr != nil {
		return nil, compileErr
	}
	// Compilation borrows the source schema to resolve column offsets. Pulls
	// thereafter use the filter's owned source and lend its columns until the
	// next pull or Close; the kernel has no source lifecycle responsibility.
	kernel.source = nil
	return &filterOperator{source: source, kernel: kernel, predicate: node, schema: source.OutputSchema()}, nil
}

func (o *filterOperator) Init(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		_ = o.Close()
		return err
	}
	if err := o.source.Init(ctx); err != nil {
		_ = o.Close()
		return err
	}
	return nil
}

func (o *filterOperator) OutputSchema() *vectorized.BatchSchema { return o.schema }

func (o *filterOperator) Close() error {
	if o.closed {
		return nil
	}
	o.closed = true
	o.view = vectorized.RecordBatch{}
	o.selection = nil
	o.predicate = nil
	o.schema = nil
	o.kernel.release()
	return o.source.Close()
}

func (o *filterOperator) NextBatch(ctx context.Context) (*vectorized.RecordBatch, error) {
	for {
		if err := ctx.Err(); err != nil {
			_ = o.Close()
			return nil, err
		}
		if o.closed {
			return nil, nil
		}
		o.view = vectorized.RecordBatch{}
		o.kernel.clearBatch()
		batch, err := o.source.NextBatch(ctx)
		if err != nil {
			_ = o.Close()
			return nil, err
		}
		if err = ctx.Err(); err != nil {
			_ = o.Close()
			return nil, err
		}
		if batch == nil {
			_ = o.Close()
			return nil, nil
		}
		if batch.Len < 0 || batch.Len > 65536 {
			_ = o.Close()
			return nil, expressionError("batch size exceeds selection range")
		}
		if batch.ActiveLen() == 0 {
			continue
		}
		for _, row := range batch.Selection {
			if int(row) >= batch.Len {
				_ = o.Close()
				return nil, expressionError("selection index exceeds batch rows")
			}
		}
		if !o.started {
			o.started = true
			o.record(FilterStats{Executions: 1})
		}
		o.record(FilterStats{Batches: 1, InputRows: uint64(batch.ActiveLen())})
		value, evalErr := o.predicate.eval(ctx, batch, batch.Selection)
		if evalErr != nil {
			_ = o.Close()
			return nil, evalErr
		}
		selection, selectErr := o.selectRows(ctx, batch, value)
		if selectErr != nil {
			_ = o.Close()
			return nil, selectErr
		}
		if selection != nil && len(selection) == 0 {
			continue
		}
		o.record(FilterStats{OutputRows: uint64(expressionRows(batch, selection))})
		o.view = vectorized.RecordBatch{
			Schema: batch.Schema, Columns: batch.Columns, Len: batch.Len,
			Selection: selection, Hints: batch.Hints, Lent: true,
		}
		return &o.view, nil
	}
}

func (o *filterOperator) selectRows(ctx context.Context, batch *vectorized.RecordBatch, value *expressionVector) ([]uint16, error) {
	rows := batch.ActiveLen()
	var counts FilterStats
	if value.constant {
		switch {
		case value.isNull(0):
			counts.UnknownRows = uint64(rows)
		case value.boolAt(0):
			counts.TrueRows = uint64(rows)
		default:
			counts.FalseRows = uint64(rows)
		}
	} else {
		for pos := 0; pos < rows; pos++ {
			if pos%1024 == 0 {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
			}
			row := expressionRow(batch.Selection, pos)
			switch {
			case value.isNull(row):
				counts.UnknownRows++
			case value.boolAt(row):
				counts.TrueRows++
			default:
				counts.FalseRows++
			}
		}
	}
	o.record(counts)
	if counts.TrueRows == uint64(rows) {
		return batch.Selection, nil
	}
	if counts.TrueRows == 0 {
		return []uint16{}, nil
	}
	passed := int(counts.TrueRows)
	if cap(o.selection) < passed {
		bytes := int64(passed) * 2
		if err := o.kernel.reserve(bytes); err != nil {
			return nil, err
		}
		previous := o.selectionBytes
		o.selection = make([]uint16, 0, passed)
		o.selectionBytes = bytes
		o.stats.SelectionBytes += uint64(bytes)
		recordFilterWork(FilterStats{SelectionBytes: uint64(bytes)})
		if previous != 0 {
			o.kernel.unreserve(previous)
		}
	}
	o.selection = o.selection[:0]
	for pos := 0; pos < rows; pos++ {
		if pos%1024 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		row := expressionRow(batch.Selection, pos)
		if !value.isNull(row) && value.boolAt(row) {
			o.selection = append(o.selection, uint16(row))
		}
	}
	return o.selection, nil
}
