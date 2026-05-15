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
	"strings"

	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	vmeasure "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure"
)

// GroupByAgg is the vec aggregation/grouping node. It fuses GroupBy and a
// single Aggregation into one BatchAggregation operator (see G7d
// planner), and also covers the two single-sided shapes:
//
//   - GroupBy + Agg → BatchAggregation: key columns + one agg result
//     column (schema-rewriting; timestamp dropped, per G7 decision D2).
//   - Agg without GroupBy → scalar reduce: BatchAggregation with no key
//     columns, a single output row (first-seen tags + agg result).
//   - GroupBy without Agg → raw GroupBy: a first-seen-row-per-group
//     BatchGroupBy, schema-preserving.
//
// The concrete operator (and therefore the output schema) is chosen by
// vmeasure.BuildOperators from which of GroupBy/Agg is set.
type GroupByAgg struct {
	Child       VecPlan
	GroupBy     *model.MeasureGroupBy
	Agg         *model.MeasureAgg
	outputCache *vectorized.BatchSchema
}

// NewGroupByAgg constructs a GroupByAgg node wrapping child. At least one
// of groupBy/agg must be set (BuildOperators routes on which); child must
// not be nil.
func NewGroupByAgg(child VecPlan, groupBy *model.MeasureGroupBy, agg *model.MeasureAgg) (*GroupByAgg, error) {
	if groupBy == nil && agg == nil {
		return nil, fmt.Errorf("plan.GroupByAgg: GroupBy and Agg must not both be nil")
	}
	if child == nil {
		return nil, fmt.Errorf("plan.GroupByAgg: Child must not be nil")
	}
	return &GroupByAgg{Child: child, GroupBy: groupBy, Agg: agg}, nil
}

// Schema returns the aggregation output schema. The schema is computed
// lazily on first call by running BuildOperators against the child
// schema; subsequent calls return the cached value.
func (g *GroupByAgg) Schema() *vectorized.BatchSchema {
	if g.outputCache != nil {
		return g.outputCache
	}
	if g.Child == nil {
		return nil
	}
	inputSchema := g.Child.Schema()
	if inputSchema == nil {
		return nil
	}
	// Synthesize a transient MeasureQueryOptions to drive the planner.
	opts := model.MeasureQueryOptions{GroupBy: g.GroupBy, Agg: g.Agg}
	// A throwaway tracker; we only need the resulting operator's
	// OutputSchema, not its bookkeeping.
	tracker := vectorized.NewMemoryTracker(1 << 30)
	ops, err := vmeasure.BuildOperators(opts, inputSchema, tracker, 1024)
	if err != nil || len(ops) != 1 {
		return nil
	}
	g.outputCache = ops[0].OutputSchema()
	return g.outputCache
}

// Children returns the single child.
func (g *GroupByAgg) Children() []VecPlan { return []VecPlan{g.Child} }

// Build recurses into child, then constructs the BatchAggregation via
// BuildOperators and attaches it as a breaker. The pipeline-shared
// MemoryTracker from bc threads through.
func (g *GroupByAgg) Build(ctx context.Context, bc *BuildContext) error {
	if buildErr := g.Child.Build(ctx, bc); buildErr != nil {
		return buildErr
	}
	inputSchema := g.Child.Schema()
	opts := model.MeasureQueryOptions{GroupBy: g.GroupBy, Agg: g.Agg}
	ops, opsErr := vmeasure.BuildOperators(opts, inputSchema, bc.Tracker, bc.Config.BatchSize)
	if opsErr != nil {
		return fmt.Errorf("plan.GroupByAgg.Build: %w", opsErr)
	}
	if len(ops) != 1 {
		return fmt.Errorf("plan.GroupByAgg.Build: expected 1 operator, got %d", len(ops))
	}
	bc.Builder.Break(ops[0])
	// Cache for subsequent Schema() queries — saves re-running the planner.
	g.outputCache = ops[0].OutputSchema()
	return nil
}

// String returns a single-line debug description.
func (g *GroupByAgg) String() string {
	tagNames := ""
	if g.GroupBy != nil {
		tagNames = strings.Join(g.GroupBy.TagNames, ",")
	}
	if g.Agg == nil {
		return fmt.Sprintf("GroupByAgg(keys=%s, raw)", tagNames)
	}
	return fmt.Sprintf("GroupByAgg(keys=%s, fn=%v, field=%s)", tagNames, g.Agg.Func, g.Agg.FieldName)
}
