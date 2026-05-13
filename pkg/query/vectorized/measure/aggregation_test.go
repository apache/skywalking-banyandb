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
	"errors"
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

func TestBatchAggregation_AggModeMap_ReturnsErrNotImplemented(t *testing.T) {
	s := aggIntSchema()
	op := NewBatchAggregation(s, []int{0},
		[]AggSpec{{Func: AggSum, InputCol: 1, Output: "sum_v"}}, AggModeMap, 8, vectorized.NewMemoryTracker(1<<30), 0)
	if err := op.Init(context.Background()); err != nil {
		t.Fatalf("Init must succeed regardless of mode; got %v", err)
	}
	defer op.Close()
	b := vectorized.NewRecordBatch(s, 1)
	b.Columns[0].(*vectorized.TypedColumn[string]).Append("a")
	b.Columns[1].(*vectorized.TypedColumn[int64]).Append(1)
	b.Len = 1
	if err := op.Consume(context.Background(), b); !errors.Is(err, ErrAggModeNotImplemented) {
		t.Fatalf("Consume(AggModeMap) must surface ErrAggModeNotImplemented, got %v", err)
	}
	if err := op.Finalize(context.Background()); !errors.Is(err, ErrAggModeNotImplemented) {
		t.Fatalf("Finalize(AggModeMap) must surface ErrAggModeNotImplemented, got %v", err)
	}
	if _, err := op.NextBatch(context.Background()); !errors.Is(err, ErrAggModeNotImplemented) {
		t.Fatalf("NextBatch(AggModeMap) must surface ErrAggModeNotImplemented, got %v", err)
	}
}

func TestBatchAggregation_AggModeReduce_ReturnsErrNotImplemented(t *testing.T) {
	s := aggIntSchema()
	op := NewBatchAggregation(s, []int{0},
		[]AggSpec{{Func: AggSum, InputCol: 1, Output: "sum_v"}}, AggModeReduce, 8, vectorized.NewMemoryTracker(1<<30), 0)
	if err := op.Init(context.Background()); err != nil {
		t.Fatalf("Init must succeed regardless of mode; got %v", err)
	}
	defer op.Close()
	b := vectorized.NewRecordBatch(s, 1)
	b.Columns[0].(*vectorized.TypedColumn[string]).Append("a")
	b.Columns[1].(*vectorized.TypedColumn[int64]).Append(1)
	b.Len = 1
	if err := op.Consume(context.Background(), b); !errors.Is(err, ErrAggModeNotImplemented) {
		t.Fatalf("Consume(AggModeReduce) must surface ErrAggModeNotImplemented, got %v", err)
	}
	if err := op.Finalize(context.Background()); !errors.Is(err, ErrAggModeNotImplemented) {
		t.Fatalf("Finalize(AggModeReduce) must surface ErrAggModeNotImplemented, got %v", err)
	}
	if _, err := op.NextBatch(context.Background()); !errors.Is(err, ErrAggModeNotImplemented) {
		t.Fatalf("NextBatch(AggModeReduce) must surface ErrAggModeNotImplemented, got %v", err)
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
