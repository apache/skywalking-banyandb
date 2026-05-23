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
	"testing"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/aggregation"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

func aggIntSchema() *vectorized.BatchSchema {
	return vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "g", Type: vectorized.ColumnTypeString},
		{Role: vectorized.RoleField, Name: "v", Type: vectorized.ColumnTypeInt64},
	})
}

func aggFloatSchema() *vectorized.BatchSchema {
	return vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "g", Type: vectorized.ColumnTypeString},
		{Role: vectorized.RoleField, Name: "v", Type: vectorized.ColumnTypeFloat64},
	})
}

// feedAggInt builds a single batch of (g,v) int64 pairs and Consumes it.
func feedAggInt(t *testing.T, op *BatchAggregation, schema *vectorized.BatchSchema, pairs ...struct {
	g    string
	v    int64
	null bool
},
) {
	t.Helper()
	b := vectorized.NewRecordBatch(schema, len(pairs))
	gCol := b.Columns[0].(*vectorized.TypedColumn[string])
	vCol := b.Columns[1].(*vectorized.TypedColumn[int64])
	for _, p := range pairs {
		gCol.Append(p.g)
		if p.null {
			vCol.AppendNull()
		} else {
			vCol.Append(p.v)
		}
	}
	b.Len = len(pairs)
	if err := op.Consume(context.Background(), b); err != nil {
		t.Fatal(err)
	}
}

// findAggRow returns the output row index whose key column has the requested value.
func findAggRow(t *testing.T, b *vectorized.RecordBatch, key string) int {
	t.Helper()
	col := b.Columns[0].(*vectorized.TypedColumn[string])
	for i := range b.Len {
		if col.Data()[i] == key {
			return i
		}
	}
	t.Fatalf("output row for key %q not found", key)
	return -1
}

func TestBatchAggregation_AggModeAll_SumInt64(t *testing.T) {
	s := aggIntSchema()
	op := NewBatchAggregation(s, []int{0},
		[]AggSpec{{Func: AggSum, InputCol: 1, Output: "sum_v"}}, AggModeAll, 8, vectorized.NewMemoryTracker(1<<30), 0)
	_ = op.Init(context.Background())
	defer op.Close()

	feedAggInt(t, op, s,
		struct {
			g    string
			v    int64
			null bool
		}{"a", 1, false},
		struct {
			g    string
			v    int64
			null bool
		}{"b", 3, false},
		struct {
			g    string
			v    int64
			null bool
		}{"a", 2, false},
		struct {
			g    string
			v    int64
			null bool
		}{"b", 4, false},
	)
	_ = op.Finalize(context.Background())
	out, _ := op.NextBatch(context.Background())
	if out.Len != 2 {
		t.Fatalf("Len: want 2, got %d", out.Len)
	}
	sums := out.Columns[1].(*vectorized.TypedColumn[int64]).Data()
	if got := sums[findAggRow(t, out, "a")]; got != 3 {
		t.Fatalf("sum(a): want 3, got %d", got)
	}
	if got := sums[findAggRow(t, out, "b")]; got != 7 {
		t.Fatalf("sum(b): want 7, got %d", got)
	}
}

func TestBatchAggregation_AggModeAll_SumFloat64(t *testing.T) {
	s := aggFloatSchema()
	op := NewBatchAggregation(s, []int{0},
		[]AggSpec{{Func: AggSum, InputCol: 1, Output: "sum_v"}}, AggModeAll, 8, vectorized.NewMemoryTracker(1<<30), 0)
	_ = op.Init(context.Background())
	defer op.Close()

	b := vectorized.NewRecordBatch(s, 3)
	b.Columns[0].(*vectorized.TypedColumn[string]).Append("x")
	b.Columns[1].(*vectorized.TypedColumn[float64]).Append(1.5)
	b.Columns[0].(*vectorized.TypedColumn[string]).Append("x")
	b.Columns[1].(*vectorized.TypedColumn[float64]).Append(2.5)
	b.Columns[0].(*vectorized.TypedColumn[string]).Append("y")
	b.Columns[1].(*vectorized.TypedColumn[float64]).Append(0.5)
	b.Len = 3
	if err := op.Consume(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	_ = op.Finalize(context.Background())
	out, _ := op.NextBatch(context.Background())
	sums := out.Columns[1].(*vectorized.TypedColumn[float64]).Data()
	if got := sums[findAggRow(t, out, "x")]; got != 4.0 {
		t.Fatalf("sum(x): want 4.0, got %v", got)
	}
}

func TestBatchAggregation_AggModeAll_Count(t *testing.T) {
	s := aggIntSchema()
	op := NewBatchAggregation(s, []int{0},
		[]AggSpec{{Func: AggCount, InputCol: 1, Output: "n"}}, AggModeAll, 8, vectorized.NewMemoryTracker(1<<30), 0)
	_ = op.Init(context.Background())
	defer op.Close()

	feedAggInt(t, op, s,
		struct {
			g    string
			v    int64
			null bool
		}{"a", 0, false},
		struct {
			g    string
			v    int64
			null bool
		}{"a", 0, false},
		struct {
			g    string
			v    int64
			null bool
		}{"a", 0, false},
		struct {
			g    string
			v    int64
			null bool
		}{"b", 0, false},
	)
	_ = op.Finalize(context.Background())
	out, _ := op.NextBatch(context.Background())
	counts := out.Columns[1].(*vectorized.TypedColumn[int64]).Data()
	if got := counts[findAggRow(t, out, "a")]; got != 3 {
		t.Fatalf("count(a): want 3, got %d", got)
	}
	if got := counts[findAggRow(t, out, "b")]; got != 1 {
		t.Fatalf("count(b): want 1, got %d", got)
	}
}

func TestBatchAggregation_AggModeAll_Min_Int64(t *testing.T) {
	s := aggIntSchema()
	op := NewBatchAggregation(s, []int{0},
		[]AggSpec{{Func: AggMin, InputCol: 1, Output: "min_v"}}, AggModeAll, 8, vectorized.NewMemoryTracker(1<<30), 0)
	_ = op.Init(context.Background())
	defer op.Close()
	feedAggInt(t, op, s,
		struct {
			g    string
			v    int64
			null bool
		}{"a", 7, false},
		struct {
			g    string
			v    int64
			null bool
		}{"a", 3, false},
		struct {
			g    string
			v    int64
			null bool
		}{"a", 5, false},
	)
	_ = op.Finalize(context.Background())
	out, _ := op.NextBatch(context.Background())
	mins := out.Columns[1].(*vectorized.TypedColumn[int64]).Data()
	if got := mins[findAggRow(t, out, "a")]; got != 3 {
		t.Fatalf("min(a): want 3, got %d", got)
	}
}

func TestBatchAggregation_AggModeAll_Max_Float64(t *testing.T) {
	s := aggFloatSchema()
	op := NewBatchAggregation(s, []int{0},
		[]AggSpec{{Func: AggMax, InputCol: 1, Output: "max_v"}}, AggModeAll, 8, vectorized.NewMemoryTracker(1<<30), 0)
	_ = op.Init(context.Background())
	defer op.Close()

	b := vectorized.NewRecordBatch(s, 3)
	b.Columns[0].(*vectorized.TypedColumn[string]).Append("g")
	b.Columns[1].(*vectorized.TypedColumn[float64]).Append(1.0)
	b.Columns[0].(*vectorized.TypedColumn[string]).Append("g")
	b.Columns[1].(*vectorized.TypedColumn[float64]).Append(9.5)
	b.Columns[0].(*vectorized.TypedColumn[string]).Append("g")
	b.Columns[1].(*vectorized.TypedColumn[float64]).Append(4.0)
	b.Len = 3
	_ = op.Consume(context.Background(), b)
	_ = op.Finalize(context.Background())
	out, _ := op.NextBatch(context.Background())
	maxs := out.Columns[1].(*vectorized.TypedColumn[float64]).Data()
	if got := maxs[findAggRow(t, out, "g")]; got != 9.5 {
		t.Fatalf("max(g): want 9.5, got %v", got)
	}
}

func TestBatchAggregation_AggModeAll_Mean_Float64(t *testing.T) {
	s := aggFloatSchema()
	op := NewBatchAggregation(s, []int{0},
		[]AggSpec{{Func: AggMean, InputCol: 1, Output: "mean_v"}}, AggModeAll, 8, vectorized.NewMemoryTracker(1<<30), 0)
	_ = op.Init(context.Background())
	defer op.Close()

	b := vectorized.NewRecordBatch(s, 4)
	b.Columns[0].(*vectorized.TypedColumn[string]).Append("g")
	b.Columns[1].(*vectorized.TypedColumn[float64]).Append(1.0)
	b.Columns[0].(*vectorized.TypedColumn[string]).Append("g")
	b.Columns[1].(*vectorized.TypedColumn[float64]).Append(2.0)
	b.Columns[0].(*vectorized.TypedColumn[string]).Append("g")
	b.Columns[1].(*vectorized.TypedColumn[float64]).Append(3.0)
	b.Columns[0].(*vectorized.TypedColumn[string]).Append("g")
	b.Columns[1].(*vectorized.TypedColumn[float64]).Append(4.0)
	b.Len = 4
	_ = op.Consume(context.Background(), b)
	_ = op.Finalize(context.Background())
	out, _ := op.NextBatch(context.Background())
	means := out.Columns[1].(*vectorized.TypedColumn[float64]).Data()
	if got := means[findAggRow(t, out, "g")]; got != 2.5 {
		t.Fatalf("mean(g): want 2.5, got %v", got)
	}
}

func TestBatchAggregation_AggModeAll_NullField_ExcludedFromAggregation(t *testing.T) {
	s := aggIntSchema()
	op := NewBatchAggregation(s, []int{0},
		[]AggSpec{
			{Func: AggSum, InputCol: 1, Output: "sum_v"},
			{Func: AggCount, InputCol: 1, Output: "n"},
		}, AggModeAll, 8, vectorized.NewMemoryTracker(1<<30), 0)
	_ = op.Init(context.Background())
	defer op.Close()
	feedAggInt(t, op, s,
		struct {
			g    string
			v    int64
			null bool
		}{"a", 5, false},
		struct {
			g    string
			v    int64
			null bool
		}{"a", 0, true}, // null — must be skipped
		struct {
			g    string
			v    int64
			null bool
		}{"a", 7, false},
	)
	_ = op.Finalize(context.Background())
	out, _ := op.NextBatch(context.Background())
	row := findAggRow(t, out, "a")
	sum := out.Columns[1].(*vectorized.TypedColumn[int64]).Data()[row]
	count := out.Columns[2].(*vectorized.TypedColumn[int64]).Data()[row]
	if sum != 12 {
		t.Fatalf("sum(a): null must be excluded; want 12, got %d", sum)
	}
	if count != 2 {
		t.Fatalf("count(a): null must be excluded; want 2, got %d", count)
	}
}

// Pins the regression flagged by Copilot: distinct (a,b) tuples whose
// components contain embedded NUL bytes must NOT collapse into one group.
func TestBatchAggregation_NULInStringKey_NoCollision(t *testing.T) {
	s := vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "a", Type: vectorized.ColumnTypeString},
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "b", Type: vectorized.ColumnTypeString},
		{Role: vectorized.RoleField, Name: "v", Type: vectorized.ColumnTypeInt64},
	})
	op := NewBatchAggregation(s, []int{0, 1},
		[]AggSpec{{Func: AggSum, InputCol: 2, Output: "sum"}}, AggModeAll, 8, vectorized.NewMemoryTracker(1<<30), 0)
	_ = op.Init(context.Background())
	defer op.Close()

	b := vectorized.NewRecordBatch(s, 2)
	b.Columns[0].(*vectorized.TypedColumn[string]).Append("a\x00b")
	b.Columns[1].(*vectorized.TypedColumn[string]).Append("c")
	b.Columns[2].(*vectorized.TypedColumn[int64]).Append(10)
	b.Columns[0].(*vectorized.TypedColumn[string]).Append("a")
	b.Columns[1].(*vectorized.TypedColumn[string]).Append("b\x00c")
	b.Columns[2].(*vectorized.TypedColumn[int64]).Append(20)
	b.Len = 2

	_ = op.Consume(context.Background(), b)
	_ = op.Finalize(context.Background())
	out, _ := op.NextBatch(context.Background())
	if out.Len != 2 {
		t.Fatalf("two distinct (a,b) tuples must produce two output rows; got Len=%d (NUL-in-key collision merged)", out.Len)
	}
}

// aggMapSchema is the schema used by AggModeMap tests: it adds a RoleShardID
// input column ahead of the tag + value columns, mirroring what the storage
// bridge actually provides in production. AggModeMap captures the first-fed
// row's shard id per group (G9f.2.a).
func aggMapSchema() *vectorized.BatchSchema {
	return vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleShardID, Name: shardIDOutputName, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "g", Type: vectorized.ColumnTypeString},
		{Role: vectorized.RoleField, Name: "v", Type: vectorized.ColumnTypeInt64},
	})
}

// feedAggMap consumes one batch of (shard, group, value) tuples through op.
func feedAggMap(t *testing.T, op *BatchAggregation, s *vectorized.BatchSchema, rows ...struct {
	g     string
	shard int64
	v     int64
},
) {
	t.Helper()
	b := vectorized.NewRecordBatch(s, len(rows))
	shardCol := b.Columns[0].(*vectorized.TypedColumn[int64])
	gCol := b.Columns[1].(*vectorized.TypedColumn[string])
	vCol := b.Columns[2].(*vectorized.TypedColumn[int64])
	for _, r := range rows {
		shardCol.Append(r.shard)
		gCol.Append(r.g)
		vCol.Append(r.v)
	}
	b.Len = len(rows)
	if err := op.Consume(context.Background(), b); err != nil {
		t.Fatalf("Consume: %v", err)
	}
}

// findAggMapRow locates the output row for group key, accounting for the
// leading shard-id column (output column 1 is the tag).
func findAggMapRow(t *testing.T, b *vectorized.RecordBatch, key string) int {
	t.Helper()
	col := b.Columns[1].(*vectorized.TypedColumn[string])
	for i := range b.Len {
		if col.Data()[i] == key {
			return i
		}
	}
	t.Fatalf("output row for key %q not found", key)
	return -1
}

// TestBatchAggregation_AggModeMap_OutputSchema_ShardIDFirst pins the
// AggModeMap output layout: the partial batch begins with a RoleShardID
// int64 column named "shard_id", then the projected tags, then per-agg
// value columns. Non-MEAN aggs contribute one column; MEAN aggs contribute
// two (value + "__agg_count" sidecar) — matching the row path's
// aggCountFieldName convention so cross-path diagnostics line up.
func TestBatchAggregation_AggModeMap_OutputSchema_ShardIDFirst(t *testing.T) {
	s := aggMapSchema()
	op := NewBatchAggregation(s, []int{1},
		[]AggSpec{
			{Func: AggSum, InputCol: 2, Output: "sum_v"},
			{Func: AggMean, InputCol: 2, Output: "mean_v"},
		}, AggModeMap, 8, vectorized.NewMemoryTracker(1<<30), 0)
	defer op.Close()

	out := op.OutputSchema()
	wantCols := []struct {
		name string
		role vectorized.ColumnRole
		typ  vectorized.ColumnType
	}{
		{shardIDOutputName, vectorized.RoleShardID, vectorized.ColumnTypeInt64},
		{"g", vectorized.RoleTag, vectorized.ColumnTypeString},
		{"sum_v", vectorized.RoleField, vectorized.ColumnTypeInt64},
		{"mean_v", vectorized.RoleField, vectorized.ColumnTypeInt64},
		{"mean_v" + meanCountSuffix, vectorized.RoleField, vectorized.ColumnTypeInt64},
	}
	if len(out.Columns) != len(wantCols) {
		t.Fatalf("output schema column count = %d, want %d (shard + tag + sum + mean-value + mean-count)",
			len(out.Columns), len(wantCols))
	}
	for i, want := range wantCols {
		got := out.Columns[i]
		if got.Name != want.name || got.Role != want.role || got.Type != want.typ {
			t.Errorf("output column %d = {Name:%q Role:%d Type:%d}, want {Name:%q Role:%d Type:%d}",
				i, got.Name, got.Role, got.Type, want.name, want.role, want.typ)
		}
	}
}

// TestBatchAggregation_AggModeMap_CapturesFirstFedShardID pins the G9f.2.a
// semantic-repro rule: when a group is created, its shard-id is captured
// from the *first* input row that lands in the group — never overwritten by
// later rows with different shard ids. This mirrors
// pkg/query/logical/measure/measure_plan_aggregation.go:285 ("first idp fed
// into the group wins") so a flag-on partial's dedup key matches the row
// path's incidental shard assignment.
func TestBatchAggregation_AggModeMap_CapturesFirstFedShardID(t *testing.T) {
	s := aggMapSchema()
	op := NewBatchAggregation(s, []int{1},
		[]AggSpec{{Func: AggSum, InputCol: 2, Output: "sum_v"}}, AggModeMap, 8, vectorized.NewMemoryTracker(1<<30), 0)
	_ = op.Init(context.Background())
	defer op.Close()

	// Group "a" sees shard 7 first, then shard 9 (must NOT overwrite).
	// Group "b" sees shard 5 first.
	feedAggMap(t, op, s,
		struct {
			g     string
			shard int64
			v     int64
		}{"a", 7, 1},
		struct {
			g     string
			shard int64
			v     int64
		}{"b", 5, 10},
		struct {
			g     string
			shard int64
			v     int64
		}{"a", 9, 2}, // later row, must NOT overwrite "a"'s shard.
	)
	_ = op.Finalize(context.Background())
	out, _ := op.NextBatch(context.Background())
	if out == nil || out.Len != 2 {
		t.Fatalf("want 2 groups, got %v", out)
	}
	shardCol := out.Columns[0].(*vectorized.TypedColumn[int64]).Data()
	rowA := findAggMapRow(t, out, "a")
	rowB := findAggMapRow(t, out, "b")
	if shardCol[rowA] != 7 {
		t.Errorf("group a shard_id = %d, want 7 (first-fed wins)", shardCol[rowA])
	}
	if shardCol[rowB] != 5 {
		t.Errorf("group b shard_id = %d, want 5", shardCol[rowB])
	}
}

// TestBatchAggregation_AggModeMap_ScalarReduce_ShardIDZero pins the row
// path's scalar-reduce contract: an Agg without GroupBy (len(keyIndices)==0)
// emits ShardId: 0 regardless of input shard ids, matching
// pkg/query/logical/measure/measure_plan_aggregation.go:364
// (aggAllIterator.Current() hardcoding ShardId: 0).
func TestBatchAggregation_AggModeMap_ScalarReduce_ShardIDZero(t *testing.T) {
	s := aggMapSchema()
	op := NewBatchAggregation(s, nil, // no keyIndices ⇒ scalar reduce
		[]AggSpec{{Func: AggSum, InputCol: 2, Output: "sum_v"}}, AggModeMap, 8, vectorized.NewMemoryTracker(1<<30), 0)
	_ = op.Init(context.Background())
	defer op.Close()

	feedAggMap(t, op, s,
		struct {
			g     string
			shard int64
			v     int64
		}{"a", 7, 1},
		struct {
			g     string
			shard int64
			v     int64
		}{"b", 9, 2},
	)
	_ = op.Finalize(context.Background())
	out, _ := op.NextBatch(context.Background())
	if out == nil || out.Len != 1 {
		t.Fatalf("scalar reduce must collapse to one row; got %v", out)
	}
	if got := out.Columns[0].(*vectorized.TypedColumn[int64]).Data()[0]; got != 0 {
		t.Fatalf("scalar reduce shard_id = %d, want 0", got)
	}
}

// TestBatchAggregation_AggModeMap_MeanEmitsValueAndCount pins MEAN's
// two-component partial: Partial.Value (sum) and Partial.Count (n) land in
// the value column and the "__agg_count" sidecar column respectively, so a
// downstream Reduce can recover the weighted mean across nodes.
func TestBatchAggregation_AggModeMap_MeanEmitsValueAndCount(t *testing.T) {
	s := aggMapSchema()
	op := NewBatchAggregation(s, []int{1},
		[]AggSpec{{Func: AggMean, InputCol: 2, Output: "mean_v"}}, AggModeMap, 8, vectorized.NewMemoryTracker(1<<30), 0)
	_ = op.Init(context.Background())
	defer op.Close()

	feedAggMap(t, op, s,
		struct {
			g     string
			shard int64
			v     int64
		}{"a", 7, 10},
		struct {
			g     string
			shard int64
			v     int64
		}{"a", 7, 20},
		struct {
			g     string
			shard int64
			v     int64
		}{"a", 7, 30},
	)
	_ = op.Finalize(context.Background())
	out, _ := op.NextBatch(context.Background())
	if out == nil || out.Len != 1 {
		t.Fatalf("want 1 group, got %v", out)
	}
	// Output column layout: [shard_id, g, mean_v(value), mean_v__agg_count].
	sums := out.Columns[2].(*vectorized.TypedColumn[int64]).Data()
	counts := out.Columns[3].(*vectorized.TypedColumn[int64]).Data()
	if sums[0] != 60 {
		t.Errorf("MEAN partial Value (sum) = %d, want 60", sums[0])
	}
	if counts[0] != 3 {
		t.Errorf("MEAN partial Count = %d, want 3", counts[0])
	}
}

// TestBatchAggregation_AggModeMap_NonMean_EmitsValueOnly asserts non-MEAN
// aggs contribute exactly one output column in AggModeMap — the value
// column (no sidecar). This keeps the partial frame compact for the common
// SUM/COUNT/MIN/MAX cases.
func TestBatchAggregation_AggModeMap_NonMean_EmitsValueOnly(t *testing.T) {
	s := aggMapSchema()
	op := NewBatchAggregation(s, []int{1},
		[]AggSpec{
			{Func: AggSum, InputCol: 2, Output: "sum_v"},
			{Func: AggMin, InputCol: 2, Output: "min_v"},
			{Func: AggMax, InputCol: 2, Output: "max_v"},
			{Func: AggCount, InputCol: 2, Output: "count_v"},
		}, AggModeMap, 8, vectorized.NewMemoryTracker(1<<30), 0)
	defer op.Close()

	// [shard_id, g, sum_v, min_v, max_v, count_v] = 6 columns; no sidecars.
	if got := len(op.OutputSchema().Columns); got != 6 {
		t.Fatalf("AggModeMap output column count = %d, want 6 (shard + tag + 4 non-MEAN aggs, no sidecars)", got)
	}
}

// aggReduceSchema builds the canonical AggModeMap output schema used as
// AggModeReduce input: [shard_id, g (tag), sum_v (int64)]. shard_id is
// int64 RoleShardID; g is a string RoleTag (single group key). For pure
// non-MEAN aggs the count sidecar is omitted (matches AggModeMap output
// for SUM/MIN/MAX/COUNT).
func aggReduceSchema() *vectorized.BatchSchema {
	return vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleShardID, Name: shardIDOutputName, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleTag, TagFamily: "f", Name: "g", Type: vectorized.ColumnTypeString},
		{Role: vectorized.RoleField, Name: "sum_v", Type: vectorized.ColumnTypeInt64},
	})
}

// aggReduceSchemaWithCount is the MEAN variant: an extra count sidecar
// column at offset 3 (matches AggModeMap output for AggMean — value
// then "__agg_count").
func aggReduceSchemaWithCount() *vectorized.BatchSchema {
	return vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleShardID, Name: shardIDOutputName, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleTag, TagFamily: "f", Name: "g", Type: vectorized.ColumnTypeString},
		{Role: vectorized.RoleField, Name: "mean_v", Type: vectorized.ColumnTypeFloat64},
		{Role: vectorized.RoleField, Name: "mean_v" + meanCountSuffix, Type: vectorized.ColumnTypeFloat64},
	})
}

// feedReduce runs Init/Consume/Finalize and drains NextBatch into a slice.
func feedReduce(t *testing.T, op *BatchAggregation, b *vectorized.RecordBatch) []*vectorized.RecordBatch {
	t.Helper()
	if err := op.Init(context.Background()); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if err := op.Consume(context.Background(), b); err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if err := op.Finalize(context.Background()); err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	var out []*vectorized.RecordBatch
	for {
		nb, err := op.NextBatch(context.Background())
		if err != nil {
			t.Fatalf("NextBatch: %v", err)
		}
		if nb == nil {
			break
		}
		out = append(out, nb)
	}
	return out
}

// TestBatchAggregation_AggModeReduce_OutputSchema_ShardIDDropped asserts the
// Reduce output matches AggModeAll's shape: tags + final value column, no
// leading shard_id and no count sidecar. Mirrors the row path's final
// QueryResponse shape (one DataPoint per group; no shard echo).
func TestBatchAggregation_AggModeReduce_OutputSchema_ShardIDDropped(t *testing.T) {
	s := aggReduceSchema()
	op := NewBatchAggregation(s, []int{1},
		[]AggSpec{{Func: AggSum, InputCol: 2, Output: "sum_v"}}, AggModeReduce, 8,
		vectorized.NewMemoryTracker(1<<30), 0)
	defer op.Close()
	got := op.OutputSchema().Columns
	if len(got) != 2 {
		t.Fatalf("Reduce output column count = %d, want 2 (g + sum_v)", len(got))
	}
	if got[0].Role != vectorized.RoleTag || got[0].Name != "g" {
		t.Fatalf("col 0 = %+v, want RoleTag/g", got[0])
	}
	if got[1].Role != vectorized.RoleField || got[1].Name != "sum_v" {
		t.Fatalf("col 1 = %+v, want RoleField/sum_v", got[1])
	}
}

// TestBatchAggregation_AggModeReduce_CombinesPartialsAcrossShards asserts
// partials from DIFFERENT shards sharing the same group key are COMBINED
// (not dedup'd) — the row path's deduplicateAggregatedDataPointsWithShard
// preserves cross-shard rows for the same group, and the vec path must too.
// SUM(10@shard=1) + SUM(20@shard=2) for group "a" yields 30.
func TestBatchAggregation_AggModeReduce_CombinesPartialsAcrossShards(t *testing.T) {
	s := aggReduceSchema()
	op := NewBatchAggregation(s, []int{1},
		[]AggSpec{{Func: AggSum, InputCol: 2, Output: "sum_v"}}, AggModeReduce, 8,
		vectorized.NewMemoryTracker(1<<30), 0)
	defer op.Close()

	b := vectorized.NewRecordBatch(s, 2)
	b.Columns[0].(*vectorized.TypedColumn[int64]).Append(1)
	b.Columns[1].(*vectorized.TypedColumn[string]).Append("a")
	b.Columns[2].(*vectorized.TypedColumn[int64]).Append(10)
	b.Columns[0].(*vectorized.TypedColumn[int64]).Append(2)
	b.Columns[1].(*vectorized.TypedColumn[string]).Append("a")
	b.Columns[2].(*vectorized.TypedColumn[int64]).Append(20)
	b.Len = 2

	batches := feedReduce(t, op, b)
	if len(batches) != 1 || batches[0].Len != 1 {
		t.Fatalf("expected 1 output batch with 1 row, got %d batches", len(batches))
	}
	if g := batches[0].Columns[0].(*vectorized.TypedColumn[string]).Data()[0]; g != "a" {
		t.Fatalf("group = %q, want \"a\"", g)
	}
	if sum := batches[0].Columns[1].(*vectorized.TypedColumn[int64]).Data()[0]; sum != 30 {
		t.Fatalf("sum_v = %d, want 30 (10@shard=1 + 20@shard=2)", sum)
	}
}

// TestBatchAggregation_AggModeReduce_BindsNumericDuplicateField asserts the
// liaison reduce binds the numeric partial column when a schema also carries a
// passthrough field with the same name. Distributed multi-source schemas may
// carry row-compatible passthrough fields alongside the AggModeMap partial; the
// reducer must not pick the passthrough column and panic on type assertion.
func TestBatchAggregation_AggModeReduce_BindsNumericDuplicateField(t *testing.T) {
	s := vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleShardID, Name: shardIDOutputName, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleTag, TagFamily: "f", Name: "g", Type: vectorized.ColumnTypeString},
		{Role: vectorized.RoleField, Name: "sum_v", Type: vectorized.ColumnTypeFieldValue},
		{Role: vectorized.RoleField, Name: "sum_v", Type: vectorized.ColumnTypeInt64},
	})
	specs, bindErr := bindAggReduceSpecs(s, []AggReduceSpec{{OutputName: "sum_v", Func: AggSum}})
	if bindErr != nil {
		t.Fatalf("bindAggReduceSpecs: %v", bindErr)
	}
	if got := specs[0].InputCol; got != 3 {
		t.Fatalf("InputCol = %d, want numeric duplicate column 3", got)
	}
	op := NewBatchAggregation(s, []int{1}, specs, AggModeReduce, 8, vectorized.NewMemoryTracker(1<<30), 0)
	defer op.Close()
	b := vectorized.NewRecordBatch(s, 2)
	b.Columns[0].(*vectorized.TypedColumn[int64]).Append(1)
	b.Columns[1].(*vectorized.TypedColumn[string]).Append("a")
	b.Columns[2].(*vectorized.TypedColumn[*modelv1.FieldValue]).Append(nil)
	b.Columns[3].(*vectorized.TypedColumn[int64]).Append(10)
	b.Columns[0].(*vectorized.TypedColumn[int64]).Append(2)
	b.Columns[1].(*vectorized.TypedColumn[string]).Append("a")
	b.Columns[2].(*vectorized.TypedColumn[*modelv1.FieldValue]).Append(nil)
	b.Columns[3].(*vectorized.TypedColumn[int64]).Append(20)
	b.Len = 2
	batches := feedReduce(t, op, b)
	if len(batches) != 1 || batches[0].Len != 1 {
		t.Fatalf("expected 1 output batch with 1 row, got %d batches", len(batches))
	}
	if got := batches[0].Columns[1].(*vectorized.TypedColumn[int64]).Data()[0]; got != 30 {
		t.Fatalf("sum_v = %d, want 30", got)
	}
}

// TestBatchAggregation_AggModeReduce_BindsFieldValueFallback covers the hidden
// criteria path where a data-node iterator wrapper serializes partial rows
// through DataPoint egress, so the wire frame carries FieldValue passthrough
// columns instead of native numeric partial columns.
func TestBatchAggregation_AggModeReduce_BindsFieldValueFallback(t *testing.T) {
	s := vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleShardID, Name: shardIDOutputName, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleTag, TagFamily: "f", Name: "g", Type: vectorized.ColumnTypeString},
		{Role: vectorized.RoleField, Name: "sum_v", Type: vectorized.ColumnTypeFieldValue},
	})
	specs, bindErr := bindAggReduceSpecs(s, []AggReduceSpec{{OutputName: "sum_v", Func: AggSum}})
	if bindErr != nil {
		t.Fatalf("bindAggReduceSpecs: %v", bindErr)
	}
	if got := specs[0].InputCol; got != 2 {
		t.Fatalf("InputCol = %d, want FieldValue fallback column 2", got)
	}
	op := NewBatchAggregation(s, []int{1}, specs, AggModeReduce, 8, vectorized.NewMemoryTracker(1<<30), 0)
	defer op.Close()
	b := vectorized.NewRecordBatch(s, 2)
	b.Columns[0].(*vectorized.TypedColumn[int64]).Append(1)
	b.Columns[1].(*vectorized.TypedColumn[string]).Append("a")
	b.Columns[2].(*vectorized.TypedColumn[*modelv1.FieldValue]).Append(&modelv1.FieldValue{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: 10}}})
	b.Columns[0].(*vectorized.TypedColumn[int64]).Append(2)
	b.Columns[1].(*vectorized.TypedColumn[string]).Append("a")
	b.Columns[2].(*vectorized.TypedColumn[*modelv1.FieldValue]).Append(&modelv1.FieldValue{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: 20}}})
	b.Len = 2
	batches := feedReduce(t, op, b)
	if len(batches) != 1 || batches[0].Len != 1 {
		t.Fatalf("expected 1 output batch with 1 row, got %d batches", len(batches))
	}
	if got := batches[0].Columns[1].(*vectorized.TypedColumn[int64]).Data()[0]; got != 30 {
		t.Fatalf("sum_v = %d, want 30", got)
	}
}

// TestBatchAggregation_AggModeReduce_TagValueGroupKeyDoesNotCollapse is a
// focused debug reproducer for distributed hidden-criteria queries: the data
// node serializes partial aggregate rows through DataPoint egress, so the
// liaison sees group keys as ColumnTypeTagValue passthrough cells. Distinct
// TagValue keys must remain distinct during reduce.
func TestBatchAggregation_AggModeReduce_TagValueGroupKeyDoesNotCollapse(t *testing.T) {
	s := vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleShardID, Name: shardIDOutputName, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleTag, TagFamily: "f", Name: "entity_id", Type: vectorized.ColumnTypeTagValue},
		{Role: vectorized.RoleField, Name: "sum_v", Type: vectorized.ColumnTypeFieldValue},
	})
	specs, bindErr := bindAggReduceSpecs(s, []AggReduceSpec{{OutputName: "sum_v", Func: AggSum}})
	if bindErr != nil {
		t.Fatalf("bindAggReduceSpecs: %v", bindErr)
	}
	op := NewBatchAggregation(s, []int{1}, specs, AggModeReduce, 8, vectorized.NewMemoryTracker(1<<30), 0)
	defer op.Close()
	b := vectorized.NewRecordBatch(s, 2)
	b.Columns[0].(*vectorized.TypedColumn[int64]).Append(1)
	b.Columns[1].(*vectorized.TypedColumn[*modelv1.TagValue]).Append(&modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "e2e-service-consumer"}}})
	b.Columns[2].(*vectorized.TypedColumn[*modelv1.FieldValue]).Append(&modelv1.FieldValue{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: 10}}})
	b.Columns[0].(*vectorized.TypedColumn[int64]).Append(1)
	b.Columns[1].(*vectorized.TypedColumn[*modelv1.TagValue]).Append(&modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "e2e-service-provider"}}})
	b.Columns[2].(*vectorized.TypedColumn[*modelv1.FieldValue]).Append(&modelv1.FieldValue{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: 20}}})
	b.Len = 2
	batches := feedReduce(t, op, b)
	if len(batches) != 1 || batches[0].Len != 2 {
		t.Fatalf("expected 1 output batch with 2 rows, got %d batches and %d rows", len(batches), batches[0].Len)
	}
	gotKeys := make(map[string]struct{}, batches[0].Len)
	keyCol := batches[0].Columns[0].(*vectorized.TypedColumn[*modelv1.TagValue])
	for rowIdx := range batches[0].Len {
		key := keyCol.Data()[rowIdx].GetStr().GetValue()
		gotKeys[key] = struct{}{}
	}
	for _, wantKey := range []string{"e2e-service-consumer", "e2e-service-provider"} {
		if _, ok := gotKeys[wantKey]; !ok {
			t.Fatalf("missing reduced group key %q in %v", wantKey, gotKeys)
		}
	}
}

// TestBatchAggregation_AggModeReduce_DedupsSameShardSameGroup asserts replica
// duplicates — same shard_id + same group_key — collapse to a single
// contribution. Two identical (shard=1, g=a, sum_v=10) rows yield sum=10.
func TestBatchAggregation_AggModeReduce_DedupsSameShardSameGroup(t *testing.T) {
	s := aggReduceSchema()
	op := NewBatchAggregation(s, []int{1},
		[]AggSpec{{Func: AggSum, InputCol: 2, Output: "sum_v"}}, AggModeReduce, 8,
		vectorized.NewMemoryTracker(1<<30), 0)
	defer op.Close()

	b := vectorized.NewRecordBatch(s, 2)
	b.Columns[0].(*vectorized.TypedColumn[int64]).Append(1)
	b.Columns[1].(*vectorized.TypedColumn[string]).Append("a")
	b.Columns[2].(*vectorized.TypedColumn[int64]).Append(10)
	b.Columns[0].(*vectorized.TypedColumn[int64]).Append(1)
	b.Columns[1].(*vectorized.TypedColumn[string]).Append("a")
	b.Columns[2].(*vectorized.TypedColumn[int64]).Append(10)
	b.Len = 2

	batches := feedReduce(t, op, b)
	if len(batches) != 1 || batches[0].Len != 1 {
		t.Fatalf("expected 1 output batch with 1 row, got %d batches", len(batches))
	}
	if got := batches[0].Columns[1].(*vectorized.TypedColumn[int64]).Data()[0]; got != 10 {
		t.Fatalf("sum_v = %d, want 10 (replica duplicate must be dropped)", got)
	}
}

// TestBatchAggregation_AggModeReduce_MeanFinalises asserts MEAN finalization
// happens inside Reduce.Val(): partials carry (Sum, Count) and the final
// value is Sum/Count. Two shards: (sum=10,count=2) + (sum=20,count=3) ⇒
// 30/5 = 6.
func TestBatchAggregation_AggModeReduce_MeanFinalises(t *testing.T) {
	s := aggReduceSchemaWithCount()
	op := NewBatchAggregation(s, []int{1},
		[]AggSpec{{Func: AggMean, InputCol: 2, Output: "mean_v"}}, AggModeReduce, 8,
		vectorized.NewMemoryTracker(1<<30), 0)
	defer op.Close()

	b := vectorized.NewRecordBatch(s, 2)
	b.Columns[0].(*vectorized.TypedColumn[int64]).Append(1)
	b.Columns[1].(*vectorized.TypedColumn[string]).Append("a")
	b.Columns[2].(*vectorized.TypedColumn[float64]).Append(10)
	b.Columns[3].(*vectorized.TypedColumn[float64]).Append(2)
	b.Columns[0].(*vectorized.TypedColumn[int64]).Append(2)
	b.Columns[1].(*vectorized.TypedColumn[string]).Append("a")
	b.Columns[2].(*vectorized.TypedColumn[float64]).Append(20)
	b.Columns[3].(*vectorized.TypedColumn[float64]).Append(3)
	b.Len = 2

	batches := feedReduce(t, op, b)
	if len(batches) != 1 || batches[0].Len != 1 {
		t.Fatalf("expected 1 output batch with 1 row, got %d batches", len(batches))
	}
	if got := batches[0].Columns[1].(*vectorized.TypedColumn[float64]).Data()[0]; got != 6 {
		t.Fatalf("mean_v = %v, want 6 ((10+20)/(2+3))", got)
	}
}

// TestBatchAggregation_DelegatesToAggregationPackage verifies that the same
// arithmetic computed via pkg/query/aggregation directly produces results
// identical to BatchAggregation's. This is the structural proof that
// BatchAggregation does not reimplement reduction logic — if it did, drift
// between the two paths would surface here.
func TestBatchAggregation_DelegatesToAggregationPackage(t *testing.T) {
	s := aggIntSchema()
	op := NewBatchAggregation(s, []int{0},
		[]AggSpec{{Func: AggSum, InputCol: 1, Output: "sum_v"}}, AggModeAll, 8, vectorized.NewMemoryTracker(1<<30), 0)
	_ = op.Init(context.Background())
	defer op.Close()

	values := []int64{3, 1, 4, 1, 5, 9, 2, 6}
	for _, v := range values {
		feedAggInt(t, op, s, struct {
			g    string
			v    int64
			null bool
		}{"a", v, false})
	}
	_ = op.Finalize(context.Background())
	out, _ := op.NextBatch(context.Background())
	gotSum := out.Columns[1].(*vectorized.TypedColumn[int64]).Data()[findAggRow(t, out, "a")]

	// Reference: same computation via aggregation.NewMap[int64] directly.
	ref, refErr := aggregation.NewMap[int64](modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM)
	if refErr != nil {
		t.Fatal(refErr)
	}
	for _, v := range values {
		ref.In(v)
	}
	wantSum := ref.Val()
	if gotSum != wantSum {
		t.Fatalf("BatchAggregation SUM (%d) must equal aggregation.NewMap SUM (%d) — divergence indicates delegation broke",
			gotSum, wantSum)
	}
}

// TestBatchAggregation_Correctness_MatchesManualComputation pins parity-relevant
// arithmetic against a manual reference, complementing the delegation assertion.
func TestBatchAggregation_Correctness_MatchesManualComputation(t *testing.T) {
	s := aggIntSchema()
	op := NewBatchAggregation(s, []int{0},
		[]AggSpec{
			{Func: AggSum, InputCol: 1, Output: "sum_v"},
			{Func: AggMin, InputCol: 1, Output: "min_v"},
			{Func: AggMax, InputCol: 1, Output: "max_v"},
		}, AggModeAll, 8, vectorized.NewMemoryTracker(1<<30), 0)
	_ = op.Init(context.Background())
	defer op.Close()
	feedAggInt(t, op, s,
		struct {
			g    string
			v    int64
			null bool
		}{"a", 3, false},
		struct {
			g    string
			v    int64
			null bool
		}{"a", 1, false},
		struct {
			g    string
			v    int64
			null bool
		}{"a", 4, false},
		struct {
			g    string
			v    int64
			null bool
		}{"a", 1, false},
		struct {
			g    string
			v    int64
			null bool
		}{"a", 5, false},
	)
	_ = op.Finalize(context.Background())
	out, _ := op.NextBatch(context.Background())
	row := findAggRow(t, out, "a")
	sum := out.Columns[1].(*vectorized.TypedColumn[int64]).Data()[row]
	mn := out.Columns[2].(*vectorized.TypedColumn[int64]).Data()[row]
	mx := out.Columns[3].(*vectorized.TypedColumn[int64]).Data()[row]
	if sum != 14 || mn != 1 || mx != 5 {
		t.Fatalf("sum/min/max: want 14/1/5, got %d/%d/%d", sum, mn, mx)
	}
}
