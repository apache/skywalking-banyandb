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

package vectorized

import "sync"

// BatchPool reuses RecordBatches across pipeline iterations.
// All batches in a pool share the same schema and capacity.
//
// Caller contract: Put a batch only when it is consistent. Error paths must
// discard rather than Put — see fusedStage.NextBatch.
type BatchPool struct {
	schema   *BatchSchema
	pool     sync.Pool
	capacity int
}

// NewBatchPool returns a pool whose Get yields freshly Reset batches.
func NewBatchPool(schema *BatchSchema, capacity int) *BatchPool {
	p := &BatchPool{schema: schema, capacity: capacity}
	p.pool.New = func() any { return NewRecordBatch(schema, capacity) }
	return p
}

// Get returns a Reset batch. Caller may write rows up to capacity.
func (p *BatchPool) Get() *RecordBatch {
	b := p.pool.Get().(*RecordBatch)
	b.Reset()
	return b
}

// Put returns a batch to the pool. Nil batches and batches with a foreign
// schema are silently dropped.
func (p *BatchPool) Put(b *RecordBatch) {
	if b == nil || b.Schema != p.schema {
		return
	}
	p.pool.Put(b)
}
