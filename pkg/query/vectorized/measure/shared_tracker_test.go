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
	"strings"
	"testing"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// TestSharedTracker_GroupByAggregation_StacksAgainstSingleBudget pins the G7a
// invariant: when BatchGroupBy and BatchAggregation are constructed with the
// same MemoryTracker, their per-group reservations stack against one budget.
// Without sharing, each operator would independently fit under the limit and
// the pipeline would consume 2x the intended ceiling.
func TestSharedTracker_GroupByAggregation_StacksAgainstSingleBudget(t *testing.T) {
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "g", Type: vectorized.ColumnTypeString},
		{Role: vectorized.RoleField, Name: "v", Type: vectorized.ColumnTypeInt64},
	})

	// Tight budget: only enough for GroupBy's 2 groups (2*100=200), no
	// headroom for Aggregation to reserve for the same groups.
	const budget = 250
	tracker := vectorized.NewMemoryTracker(budget)

	gbPool := vectorized.NewBatchPool(schema, 8)
	gb := NewBatchGroupBy(schema, []int{0}, gbPool, 8, tracker, 100, 0)
	if initErr := gb.Init(context.Background()); initErr != nil {
		t.Fatalf("groupby init: %v", initErr)
	}
	defer gb.Close()

	agg := NewBatchAggregation(schema, []int{0},
		[]AggSpec{{Func: AggSum, InputCol: 1, Output: "sum_v"}},
		AggModeAll, 8, tracker, 100)
	if initErr := agg.Init(context.Background()); initErr != nil {
		t.Fatalf("agg init: %v", initErr)
	}
	defer agg.Close()

	// Two distinct groups, one row each. GroupBy reserves 2*100=200 (used=200).
	// Aggregation then sees the GroupBy output and tries to reserve 100 for
	// its first new group: 200+100=300 > 250 → budget exhausted.
	in := vectorized.NewRecordBatch(schema, 2)
	in.Columns[0].(*vectorized.TypedColumn[string]).Append("a")
	in.Columns[1].(*vectorized.TypedColumn[int64]).Append(1)
	in.Columns[0].(*vectorized.TypedColumn[string]).Append("b")
	in.Columns[1].(*vectorized.TypedColumn[int64]).Append(2)
	in.Len = 2

	if consumeErr := gb.Consume(context.Background(), in); consumeErr != nil {
		t.Fatalf("groupby consume should fit under budget: %v", consumeErr)
	}
	if got := tracker.Used(); got != 200 {
		t.Fatalf("after groupby consume: tracker used = %d, want 200", got)
	}
	if finalizeErr := gb.Finalize(context.Background()); finalizeErr != nil {
		t.Fatalf("groupby finalize: %v", finalizeErr)
	}

	// Drain GroupBy and feed each batch into Aggregation. Expect a single
	// budget-exhausted error.
	var aggErr error
	for {
		out, pullErr := gb.NextBatch(context.Background())
		if pullErr != nil {
			t.Fatalf("groupby NextBatch: %v", pullErr)
		}
		if out == nil {
			break
		}
		aggErr = agg.Consume(context.Background(), out)
		if aggErr != nil {
			break
		}
	}
	if aggErr == nil {
		t.Fatal("aggregation consume should overflow shared budget but did not")
	}
	if !strings.Contains(aggErr.Error(), "memory budget exceeded") {
		t.Fatalf("aggregation error must surface budget exhaustion, got: %v", aggErr)
	}
}

// TestSharedTracker_ReleaseOnClose pins the lifecycle: Close releases the
// outstanding reservation, returning the budget to fully unused. Important
// for queries reusing trackers across stages or pipelines.
func TestSharedTracker_ReleaseOnClose(t *testing.T) {
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "g", Type: vectorized.ColumnTypeString},
		{Role: vectorized.RoleField, Name: "v", Type: vectorized.ColumnTypeInt64},
	})
	tracker := vectorized.NewMemoryTracker(1 << 20)

	agg := NewBatchAggregation(schema, []int{0},
		[]AggSpec{{Func: AggSum, InputCol: 1, Output: "sum_v"}},
		AggModeAll, 8, tracker, 128)
	_ = agg.Init(context.Background())

	in := vectorized.NewRecordBatch(schema, 3)
	in.Columns[0].(*vectorized.TypedColumn[string]).Append("a")
	in.Columns[1].(*vectorized.TypedColumn[int64]).Append(1)
	in.Columns[0].(*vectorized.TypedColumn[string]).Append("b")
	in.Columns[1].(*vectorized.TypedColumn[int64]).Append(2)
	in.Columns[0].(*vectorized.TypedColumn[string]).Append("c")
	in.Columns[1].(*vectorized.TypedColumn[int64]).Append(3)
	in.Len = 3

	if consumeErr := agg.Consume(context.Background(), in); consumeErr != nil {
		t.Fatalf("agg consume: %v", consumeErr)
	}
	if got := tracker.Used(); got != 3*128 {
		t.Fatalf("after consume: tracker used = %d, want %d", got, 3*128)
	}
	if closeErr := agg.Close(); closeErr != nil {
		t.Fatalf("agg close: %v", closeErr)
	}
	if got := tracker.Used(); got != 0 {
		t.Fatalf("after close: tracker used = %d, want 0 (reservation must be refunded)", got)
	}
}
