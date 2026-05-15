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

package plan

import (
	"context"
	"fmt"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	measure "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure"
)

// Limit applies offset+limit windowing as a fusible operator on the
// pipeline. Schema-preserving: emits the same column layout as its child.
//
// Limit <= 0 means "no limit"; the analyzer normalises QueryRequest.Limit
// of zero to the row-path default (100) before constructing this node.
type Limit struct {
	Child  VecPlan
	Offset uint32
	N      uint32
}

// NewLimit constructs a Limit node wrapping child.
func NewLimit(child VecPlan, offset, n uint32) *Limit {
	return &Limit{Child: child, Offset: offset, N: n}
}

// Schema returns the child's schema (Limit is schema-preserving).
func (l *Limit) Schema() *vectorized.BatchSchema {
	if l.Child == nil {
		return nil
	}
	return l.Child.Schema()
}

// Children returns the single child.
func (l *Limit) Children() []VecPlan { return []VecPlan{l.Child} }

// Build recurses into the child first, then attaches a BatchLimit as a
// fusible operator. If N is zero, no operator is attached — the caller's
// downstream nodes still see the source's full output.
func (l *Limit) Build(ctx context.Context, bc *BuildContext) error {
	if l.Child == nil {
		return fmt.Errorf("plan.Limit.Build: Child is nil")
	}
	if buildErr := l.Child.Build(ctx, bc); buildErr != nil {
		return buildErr
	}
	if l.N == 0 {
		return nil
	}
	op := measure.NewBatchLimit(l.Schema(), l.Offset, l.N)
	bc.Builder.Apply(op)
	return nil
}

// String returns a single-line debug description.
func (l *Limit) String() string {
	return fmt.Sprintf("Limit(offset=%d, n=%d)", l.Offset, l.N)
}
