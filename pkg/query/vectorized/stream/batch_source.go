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

var _ vectorized.PullOperator = (*BatchSource)(nil)

// BatchSource is a PullOperator that replays pre-built batches in the order given.
// A multi-group query feeds the batches of every group through one, group after group,
// so the downstream merge sees ties in group order. Every batch must carry the schema
// the source was built with, because SortedMerge validates schemas by pointer.
type BatchSource struct {
	schema  *vectorized.BatchSchema
	batches []*vectorized.RecordBatch
	pos     int
}

// NewBatchSource returns a source that yields batches, then EOF.
func NewBatchSource(schema *vectorized.BatchSchema, batches []*vectorized.RecordBatch) *BatchSource {
	return &BatchSource{schema: schema, batches: batches}
}

// Init is a no-op.
func (s *BatchSource) Init(context.Context) error { return nil }

// OutputSchema returns the schema every batch carries.
func (s *BatchSource) OutputSchema() *vectorized.BatchSchema { return s.schema }

// NextBatch returns the next batch, or (nil, nil) once all were yielded.
func (s *BatchSource) NextBatch(ctx context.Context) (*vectorized.RecordBatch, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.pos >= len(s.batches) {
		return nil, nil
	}
	batch := s.batches[s.pos]
	s.batches[s.pos] = nil
	s.pos++
	return batch, nil
}

// Close drops the references to the batches not yet yielded.
func (s *BatchSource) Close() error {
	s.batches = nil
	return nil
}
