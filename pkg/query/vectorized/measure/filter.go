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

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// BatchColumnFilter is a FusibleOperator that keeps the rows whose value in one
// column satisfies a predicate. It narrows the selection and copies nothing. A
// predicate on a vectorized.ConstColumn, such as an entity tag of a batch lent from one
// series, is evaluated once per batch: the whole batch is kept or dropped.
//
// The planner does not insert it today (a measure query's criteria are resolved
// against the series index before rows are read); it is the operator a row filter on
// scanned tags would use.
type BatchColumnFilter struct {
	schema *vectorized.BatchSchema
	keep   func(col vectorized.Column, row int) bool
	colIdx int
	closed bool
}

// NewBatchColumnFilter builds a filter on column colIdx of schema. keep must be a pure
// function of the cell it is given.
func NewBatchColumnFilter(schema *vectorized.BatchSchema, colIdx int, keep func(col vectorized.Column, row int) bool) *BatchColumnFilter {
	return &BatchColumnFilter{schema: schema, colIdx: colIdx, keep: keep}
}

// Init is a no-op: the filter keeps no state.
func (f *BatchColumnFilter) Init(context.Context) error { return nil }

// OutputSchema returns the unchanged input schema.
func (f *BatchColumnFilter) OutputSchema() *vectorized.BatchSchema { return f.schema }

// Process narrows b's selection to the rows the predicate keeps.
func (f *BatchColumnFilter) Process(_ context.Context, b *vectorized.RecordBatch) error {
	vectorized.SelectRows(b, f.colIdx, f.keep)
	return nil
}

// Close is idempotent and a no-op.
func (f *BatchColumnFilter) Close() error {
	f.closed = true
	return nil
}
