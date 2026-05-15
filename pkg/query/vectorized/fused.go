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

// fusedStage combines a PullOperator source with a list of FusibleOperators
// applied in order to each pulled batch. On any operator error the batch is
// discarded (R3: not returned to the caller's pool) to prevent corrupted
// batches from leaking into subsequent queries.
//
// When a fusible returns ErrLimitExhausted, the current batch is emitted and
// subsequent NextBatch calls return (nil, nil).
type fusedStage struct {
	source    PullOperator
	fused     []FusibleOperator
	limitDone bool
	closed    bool
}

func newFusedStage(source PullOperator, fused []FusibleOperator) *fusedStage {
	return &fusedStage{source: source, fused: fused}
}

// Init wires the source and every fused operator.
func (s *fusedStage) Init(ctx context.Context) error {
	if initErr := s.source.Init(ctx); initErr != nil {
		return initErr
	}
	for _, op := range s.fused {
		if initErr := op.Init(ctx); initErr != nil {
			return initErr
		}
	}
	return nil
}

// OutputSchema is the source's schema; fusible operators must not change column layout.
func (s *fusedStage) OutputSchema() *BatchSchema { return s.source.OutputSchema() }

// NextBatch pulls one batch and applies every fused operator in order.
//
// Error handling: if any fusible returns an error other than ErrLimitExhausted,
// the batch is dropped and (nil, err) is returned. The batch is NOT returned
// to the pool — see R3 in the design spec.
//
// Limit handling: if a fusible returns ErrLimitExhausted, the current batch
// is emitted (with whatever selection vector it has) and the next call
// returns (nil, nil) — EOF.
func (s *fusedStage) NextBatch(ctx context.Context) (*RecordBatch, error) {
	if s.limitDone {
		return nil, nil
	}
	b, pullErr := s.source.NextBatch(ctx)
	if pullErr != nil || b == nil {
		return b, pullErr
	}
	for _, op := range s.fused {
		processErr := op.Process(ctx, b)
		if processErr == nil {
			continue
		}
		if errors.Is(processErr, ErrLimitExhausted) {
			s.limitDone = true
			return b, nil
		}
		// R3: drop b to GC. Do NOT pool.Put(b).
		return nil, processErr
	}
	return b, nil
}

// Close closes the source and every underlying operator. Idempotent — repeat
// calls return without re-invoking children's Close so the BatchOperator
// contract holds for the stage itself, not just its leaves.
func (s *fusedStage) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	var firstErr error
	if closeErr := s.source.Close(); closeErr != nil {
		firstErr = closeErr
	}
	for _, op := range s.fused {
		if closeErr := op.Close(); closeErr != nil && firstErr == nil {
			firstErr = closeErr
		}
	}
	return firstErr
}
