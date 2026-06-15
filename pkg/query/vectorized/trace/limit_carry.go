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

// LimitedDistinctTraceID applies MaxTraceSize while carrying kept rows.
type LimitedDistinctTraceID struct {
	schema *vectorized.BatchSchema
	carry  *DistinctTraceID
	max    uint32
	seen   uint32
	closed bool
}

// NewLimitedDistinctTraceID constructs the Phase-1 limit and carry fusible.
func NewLimitedDistinctTraceID(schema *vectorized.BatchSchema, maxRows uint32) *LimitedDistinctTraceID {
	return &LimitedDistinctTraceID{
		schema: schema,
		carry:  NewDistinctTraceID(schema),
		max:    maxRows,
	}
}

// Init resets carry-forward state.
func (l *LimitedDistinctTraceID) Init(ctx context.Context) error {
	l.seen = 0
	return l.carry.Init(ctx)
}

// OutputSchema returns the unchanged input schema.
func (l *LimitedDistinctTraceID) OutputSchema() *vectorized.BatchSchema {
	return l.schema
}

// Process rewrites selection to kept rows and records only those rows.
func (l *LimitedDistinctTraceID) Process(_ context.Context, batch *vectorized.RecordBatch) error {
	if l.max == 0 {
		out := activeIndices(batch)
		batch.Selection = out
		keys := phase1Keys(batch).Data()
		partIDs := phase1PartIDs(batch).Data()
		traceIDs := phase1TraceIDs(batch).Data()
		for _, rowIdx := range out {
			traceID := traceIDs[rowIdx]
			if _, seen := l.carry.keys[traceID]; seen {
				continue
			}
			partID := partIDs[rowIdx]
			l.carry.traceIDsByPart[partID] = append(l.carry.traceIDsByPart[partID], traceID)
			l.carry.keys[traceID] = keys[rowIdx]
			l.carry.order = append(l.carry.order, traceID)
		}
		return nil
	}
	keys := phase1Keys(batch).Data()
	partIDs := phase1PartIDs(batch).Data()
	traceIDs := phase1TraceIDs(batch).Data()
	out := make([]uint16, 0, batch.ActiveLen())
	for _, rowIdx := range activeIndices(batch) {
		traceID := traceIDs[rowIdx]
		if _, alreadySeen := l.carry.keys[traceID]; alreadySeen {
			continue
		}
		partID := partIDs[rowIdx]
		l.carry.traceIDsByPart[partID] = append(l.carry.traceIDsByPart[partID], traceID)
		l.carry.keys[traceID] = keys[rowIdx]
		l.carry.order = append(l.carry.order, traceID)
		out = append(out, rowIdx)
		l.seen++
		if l.seen >= l.max {
			batch.Selection = out
			return vectorized.ErrLimitExhausted
		}
	}
	batch.Selection = out
	return nil
}

// Close is idempotent and closes the embedded carry operator.
func (l *LimitedDistinctTraceID) Close() error {
	if l.closed {
		return nil
	}
	l.closed = true
	return l.carry.Close()
}

// Carry returns the embedded carry-forward state.
func (l *LimitedDistinctTraceID) Carry() *DistinctTraceID {
	return l.carry
}
