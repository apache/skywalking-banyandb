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

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// vectorizedMIterator adapts a vectorized Pipeline to the executor.MIterator
// interface so the existing gRPC handler can drive it without knowing it
// is internally columnar.
//
// Lifecycle: Next pulls a RecordBatch from the pipeline, serializes it via
// serializeBatchToProto, and exposes the result through Current. EOF and
// errors both terminate iteration; the latter is exposed via Err.
type vectorizedMIterator struct {
	ctx      context.Context
	pipeline *vectorized.Pipeline
	err      error
	current  []*measurev1.InternalDataPoint
	done     bool
}

// newVectorizedMIterator constructs an adapter bound to ctx.
func newVectorizedMIterator(ctx context.Context, p *vectorized.Pipeline) *vectorizedMIterator {
	return &vectorizedMIterator{ctx: ctx, pipeline: p}
}

// Next pulls one batch from the pipeline and serializes it. Returns true while
// data remains. After EOF or error, returns false and stays in the terminal state.
func (i *vectorizedMIterator) Next() bool {
	if i.done {
		return false
	}
	b, err := i.pipeline.Next(i.ctx)
	if err != nil {
		i.err = err
		i.done = true
		return false
	}
	if b == nil {
		i.done = true
		return false
	}
	i.current = serializeBatchToProto(b, i.current[:0])
	return true
}

// Current returns the most recently serialized batch.
func (i *vectorizedMIterator) Current() []*measurev1.InternalDataPoint {
	return i.current
}

// Err returns the storage error that terminated iteration, or nil.
func (i *vectorizedMIterator) Err() error {
	return i.err
}

// Close delegates to the underlying pipeline. Pipeline.Close is idempotent,
// so repeated Close calls on this adapter are safe.
func (i *vectorizedMIterator) Close() error {
	return i.pipeline.Close()
}
