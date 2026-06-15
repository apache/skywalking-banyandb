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

// StaticTraceIDSource emits Phase-1 rows for direct trace-ID lookup mode.
type StaticTraceIDSource struct {
	schema    *vectorized.BatchSchema
	pool      *vectorized.BatchPool
	keys      map[string]int64
	traceIDs  []string
	batchSize int
	pos       int
	closed    bool
}

// NewStaticTraceIDSource constructs a static lookup source.
func NewStaticTraceIDSource(traceIDs []string, keys map[string]int64, batchSize int) *StaticTraceIDSource {
	return &StaticTraceIDSource{
		schema:    NewPhase1Schema(),
		traceIDs:  append([]string(nil), traceIDs...),
		keys:      keys,
		batchSize: batchSize,
	}
}

// Init initializes the output pool.
func (s *StaticTraceIDSource) Init(context.Context) error {
	if s.batchSize <= 0 {
		s.batchSize = vectorized.DefaultBatchSize
	}
	s.pool = vectorized.NewBatchPool(s.schema, s.batchSize)
	return nil
}

// OutputSchema returns the Phase-1 schema.
func (s *StaticTraceIDSource) OutputSchema() *vectorized.BatchSchema {
	return s.schema
}

// NextBatch emits the next static lookup batch.
func (s *StaticTraceIDSource) NextBatch(ctx context.Context) (*vectorized.RecordBatch, error) {
	if s.pos >= len(s.traceIDs) {
		return nil, nil
	}
	batch := s.pool.Get()
	for batch.Len < s.batchSize && s.pos < len(s.traceIDs) {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		traceID := s.traceIDs[s.pos]
		key := int64(0)
		if s.keys != nil {
			key = s.keys[traceID]
		}
		item := NewMergeItem(key, 0, 0, encodeTraceIDPayload(traceID))
		item.TraceID = traceID
		appendMergeItem(batch, item)
		s.pos++
	}
	return batch, nil
}

// Close is idempotent and a no-op.
func (s *StaticTraceIDSource) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	return nil
}

func encodeTraceIDPayload(traceID string) []byte {
	out := make([]byte, 1, len(traceID)+1)
	out[0] = idFormatV1
	out = append(out, traceID...)
	return out
}
