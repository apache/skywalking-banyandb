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

// Top selects the top-N (or bottom-N when Asc) rows by FieldName. Wraps
// `measure.BatchTop`, which uses a single global heap — the row path's
// per-timestamp TopN semantic is not yet reproduced here (BatchTop
// extension is tracked separately; see G6 plan).
//
// Schema-preserving.
type Top struct {
	Child     VecPlan
	FieldName string
	N         int
	Asc       bool
}

// NewTop constructs a Top node wrapping child.
func NewTop(child VecPlan, fieldName string, n int, asc bool) *Top {
	return &Top{Child: child, FieldName: fieldName, N: n, Asc: asc}
}

// Schema returns the child's schema (Top is schema-preserving).
func (t *Top) Schema() *vectorized.BatchSchema {
	if t.Child == nil {
		return nil
	}
	return t.Child.Schema()
}

// Children returns the single child.
func (t *Top) Children() []VecPlan { return []VecPlan{t.Child} }

// Build recurses into the child, locates FieldName in the propagated
// schema, then attaches a BatchTop as a breaker. The FieldName must
// reference a field column in the current schema.
func (t *Top) Build(ctx context.Context, bc *BuildContext) error {
	if t.Child == nil {
		return fmt.Errorf("plan.Top.Build: Child is nil")
	}
	if buildErr := t.Child.Build(ctx, bc); buildErr != nil {
		return buildErr
	}
	schema := t.Schema()
	fieldIdx := -1
	for i, def := range schema.Columns {
		if def.Role == vectorized.RoleField && def.Name == t.FieldName {
			fieldIdx = i
			break
		}
	}
	if fieldIdx < 0 {
		return fmt.Errorf("plan.Top.Build: field %q not present in schema", t.FieldName)
	}
	op := measure.NewBatchTop(schema, fieldIdx, t.N, t.Asc, bc.Config.BatchSize)
	bc.Builder.Break(op)
	return nil
}

// String returns a single-line debug description.
func (t *Top) String() string {
	dir := "desc"
	if t.Asc {
		dir = "asc"
	}
	return fmt.Sprintf("Top(field=%s, n=%d, %s)", t.FieldName, t.N, dir)
}
