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

package trace

import (
	"context"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// DistinctTraceID carries ordered trace IDs, part hints, and keys into Phase 2.
type DistinctTraceID struct {
	schema         *vectorized.BatchSchema
	traceIDsByPart map[int64][]string
	keys           map[string]int64
	order          []string
	closed         bool
}

// NewDistinctTraceID constructs the Phase-1 carry-forward fusible.
func NewDistinctTraceID(schema *vectorized.BatchSchema) *DistinctTraceID {
	return &DistinctTraceID{schema: schema}
}

// Init resets carry-forward state.
func (d *DistinctTraceID) Init(context.Context) error {
	d.traceIDsByPart = make(map[int64][]string)
	d.keys = make(map[string]int64)
	d.order = d.order[:0]
	return nil
}

// OutputSchema returns the unchanged input schema.
func (d *DistinctTraceID) OutputSchema() *vectorized.BatchSchema {
	return d.schema
}

// Process records active rows without rewriting the selection vector.
func (d *DistinctTraceID) Process(_ context.Context, batch *vectorized.RecordBatch) error {
	keys := phase1Keys(batch).Data()
	partIDs := phase1PartIDs(batch).Data()
	traceIDs := phase1TraceIDs(batch).Data()
	for _, rowIdx := range activeIndices(batch) {
		traceID := traceIDs[rowIdx]
		partID := partIDs[rowIdx]
		d.traceIDsByPart[partID] = append(d.traceIDsByPart[partID], traceID)
		d.keys[traceID] = keys[rowIdx]
		d.order = append(d.order, traceID)
	}
	return nil
}

// Close is idempotent and a no-op.
func (d *DistinctTraceID) Close() error {
	if d.closed {
		return nil
	}
	d.closed = true
	return nil
}

// TraceIDsByPart returns the part-hint mapping collected so far.
func (d *DistinctTraceID) TraceIDsByPart() map[int64][]string {
	return d.traceIDsByPart
}

// Keys returns the traceID to sort-key mapping collected so far.
func (d *DistinctTraceID) Keys() map[string]int64 {
	return d.keys
}

// Order returns trace IDs in Phase-1 arrival order.
func (d *DistinctTraceID) Order() []string {
	return d.order
}
