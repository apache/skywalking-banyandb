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

// Limit applies MaxTraceSize to the ordered Phase-1 stream.
type Limit struct {
	schema *vectorized.BatchSchema
	max    uint32
	seen   uint32
	closed bool
}

// NewLimit constructs a trace Phase-1 limit fusible.
func NewLimit(schema *vectorized.BatchSchema, maxRows uint32) *Limit {
	return &Limit{schema: schema, max: maxRows}
}

// Init is a no-op.
func (l *Limit) Init(context.Context) error {
	return nil
}

// OutputSchema returns the unchanged input schema.
func (l *Limit) OutputSchema() *vectorized.BatchSchema {
	return l.schema
}

// Process rewrites selection to keep only the first max active rows.
func (l *Limit) Process(_ context.Context, batch *vectorized.RecordBatch) error {
	if l.max == 0 {
		batch.Selection = activeIndices(batch)
		return nil
	}
	active := activeIndices(batch)
	out := make([]uint16, 0, len(active))
	for _, rowIdx := range active {
		if l.seen < l.max {
			out = append(out, rowIdx)
		}
		l.seen++
		if l.seen >= l.max {
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
