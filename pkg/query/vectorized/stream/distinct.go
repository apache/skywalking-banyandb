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

package stream

import (
	"context"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// Distinct deduplicates stream rows by ElementID, keeping the first occurrence
// in the (already merged) input order. It is stateful across batches via a seen
// set keyed by the raw uint64 element id, mirroring the row path's first-seen
// dedup (stream_plan_distributed.go seen map keyed by ElementId). It MUST run
// downstream of SortedMerge so "first occurrence" reflects the global order.
type Distinct struct {
	schema  *vectorized.BatchSchema
	seen    map[uint64]struct{}
	elemIdx int
	closed  bool
}

// NewDistinct constructs a stream first-seen dedup fusible.
func NewDistinct(schema *vectorized.BatchSchema) *Distinct {
	return &Distinct{schema: schema, elemIdx: schema.ElementIDIndex()}
}

// Init resets the seen set.
func (d *Distinct) Init(context.Context) error {
	d.seen = make(map[uint64]struct{})
	return nil
}

// OutputSchema returns the unchanged input schema.
func (d *Distinct) OutputSchema() *vectorized.BatchSchema {
	return d.schema
}

// Process rewrites the selection to keep only rows whose ElementID is seen for
// the first time. Later duplicates are dropped by omission from the selection.
func (d *Distinct) Process(_ context.Context, batch *vectorized.RecordBatch) error {
	ids := batch.Columns[d.elemIdx].(*vectorized.TypedColumn[int64]).Data()
	active := activeIndices(batch)
	out := make([]uint16, 0, len(active))
	for _, rowIdx := range active {
		id := ColumnToElementID(ids[rowIdx])
		if _, ok := d.seen[id]; ok {
			continue
		}
		d.seen[id] = struct{}{}
		out = append(out, rowIdx)
	}
	batch.Selection = out
	return nil
}

// Close is idempotent and a no-op.
func (d *Distinct) Close() error {
	if d.closed {
		return nil
	}
	d.closed = true
	return nil
}
