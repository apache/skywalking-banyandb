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

// BatchLimit applies offset+limit windowing as a fusible in-place selection
// rewrite. State across batches is carried via a cumulative-seen counter.
//
// When the window closes (seen >= offset+limit), the current batch's selection
// is sliced to whatever portion of the window it contributed and Process
// returns vectorized.ErrLimitExhausted. The fused stage translates that
// sentinel into "emit current batch, then EOF on next pull".
type BatchLimit struct {
	schema *vectorized.BatchSchema
	offset uint32
	limit  uint32
	seen   uint32
}

// NewBatchLimit constructs a fusible limit operator.
func NewBatchLimit(schema *vectorized.BatchSchema, offset, limit uint32) *BatchLimit {
	return &BatchLimit{schema: schema, offset: offset, limit: limit}
}

// Init is a no-op. Limit has no per-pipeline setup.
func (l *BatchLimit) Init(_ context.Context) error { return nil }

// OutputSchema returns the unchanged input schema.
func (l *BatchLimit) OutputSchema() *vectorized.BatchSchema { return l.schema }

// Close is idempotent and a no-op.
func (l *BatchLimit) Close() error { return nil }

// Process rewrites b.Selection to keep only rows in [offset, offset+limit) of
// the cumulative active stream.
func (l *BatchLimit) Process(_ context.Context, b *vectorized.RecordBatch) error {
	active := activeIndices(b)
	out := make([]uint16, 0, len(active))
	end := l.offset + l.limit
	for _, idx := range active {
		if l.seen >= l.offset && l.seen < end {
			out = append(out, idx)
		}
		l.seen++
		if l.seen >= end {
			b.Selection = out
			return vectorized.ErrLimitExhausted
		}
	}
	b.Selection = out
	return nil
}

// activeIndices returns the row indices of b that are currently active. If
// Selection is nil it materializes [0, Len).
func activeIndices(b *vectorized.RecordBatch) []uint16 {
	if b.Selection != nil {
		return b.Selection
	}
	out := make([]uint16, b.Len)
	for i := range out {
		out[i] = uint16(i)
	}
	return out
}
