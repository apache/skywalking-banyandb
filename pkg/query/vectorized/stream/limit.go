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

// Limit applies the client-facing global offset+limit slice to the merged,
// deduplicated stream. It skips the first offset active rows, emits up to limit
// rows, then signals EOF via ErrLimitExhausted.
//
// These are the CLIENT offset/limit values, applied liaison-side as the final
// slice (mirrors distributedLimit.Execute in stream_plan_distributed.go). The
// per-node scan cap of limit+offset is applied UPSTREAM (M6), not here.
type Limit struct {
	schema  *vectorized.BatchSchema
	offset  uint32
	limit   uint32
	skipped uint32
	emitted uint32
	closed  bool
}

// NewLimit constructs a stream global offset+limit fusible.
func NewLimit(schema *vectorized.BatchSchema, offset, limit uint32) *Limit {
	return &Limit{schema: schema, offset: offset, limit: limit}
}

// Init is a no-op.
func (l *Limit) Init(context.Context) error {
	return nil
}

// OutputSchema returns the unchanged input schema.
func (l *Limit) OutputSchema() *vectorized.BatchSchema {
	return l.schema
}

// Process rewrites the selection to the client offset+limit window. A zero
// limit emits nothing and signals EOF immediately.
func (l *Limit) Process(_ context.Context, batch *vectorized.RecordBatch) error {
	active := activeIndices(batch)
	if l.limit == 0 {
		batch.Selection = []uint16{}
		return vectorized.ErrLimitExhausted
	}
	out := make([]uint16, 0, len(active))
	for _, rowIdx := range active {
		if l.skipped < l.offset {
			l.skipped++
			continue
		}
		if l.emitted >= l.limit {
			break
		}
		out = append(out, rowIdx)
		l.emitted++
		if l.emitted >= l.limit {
			batch.Selection = out
			return vectorized.ErrLimitExhausted
		}
	}
	batch.Selection = out
	return nil
}

// Close is idempotent and a no-op.
func (l *Limit) Close() error {
	if l.closed {
		return nil
	}
	l.closed = true
	return nil
}
