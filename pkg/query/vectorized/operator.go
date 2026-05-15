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

import (
	"context"
	"errors"
)

// ErrLimitExhausted is returned by FusibleOperators that finished their work and
// want the fused stage to translate the signal into "emit current batch + EOF on next call".
var ErrLimitExhausted = errors.New("vectorized: limit exhausted")

// BatchOperator is the lifecycle base. All operators implement it.
//
// Close is idempotent and safe to call at any phase — after Init, mid-Consume,
// after Finalize, or after any error. It must release every MemoryTracker.Reserve
// the operator made during its lifetime.
type BatchOperator interface {
	Init(ctx context.Context) error
	OutputSchema() *BatchSchema
	Close() error
}

// PullOperator produces batches. It is the source of a pipeline.
//
// NextBatch contract:
//   - (non-nil, nil)  → valid batch with Len > 0
//   - (nil, nil)      → EOF; caller must not call NextBatch again
//   - (nil, non-nil)  → error; pipeline stops
//
// NextBatch may block — channel-backed implementations are explicitly supported
// for future distributed remote-scan operators.
type PullOperator interface {
	BatchOperator
	NextBatch(ctx context.Context) (*RecordBatch, error)
}

// FusibleOperator transforms a batch in place. No state across batches.
//
// Process must not retain references to the batch beyond the call. Returning
// ErrLimitExhausted signals "emit this batch then EOF on the next pull".
type FusibleOperator interface {
	BatchOperator
	Process(ctx context.Context, b *RecordBatch) error
}

// BreakerOperator buffers all input via Consume, then produces output via NextBatch
// after Finalize is called.
type BreakerOperator interface {
	BatchOperator
	Consume(ctx context.Context, b *RecordBatch) error
	Finalize(ctx context.Context) error
	NextBatch(ctx context.Context) (*RecordBatch, error)
}
