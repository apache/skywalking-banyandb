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
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

package ql

import (
	"context"
	"fmt"
	"math"
	"runtime"
	"strconv"
	"testing"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

type expressionBenchSource struct{ batch *vectorized.RecordBatch }

func (s *expressionBenchSource) Init(context.Context) error            { return nil }
func (s *expressionBenchSource) OutputSchema() *vectorized.BatchSchema { return s.batch.Schema }
func (s *expressionBenchSource) NextBatch(context.Context) (*vectorized.RecordBatch, error) {
	return s.batch, nil
}
func (s *expressionBenchSource) Close() error { return nil }

type expressionBenchFixture struct {
	expression *Expression
	batch      *vectorized.RecordBatch
	kind       string
	resultType ExpressionType
}

var expressionBenchSink vectorized.Column

// BenchmarkExpressions compares compiled typed kernels with an independent,
// allocation-free row AST interpreter. Setup, complete value checks, and structural
// checks are outside the clock. Both paths reuse their value buffers and construct
// the same typed result descriptors and null bitmaps on every batch. Constants can
// remain scalars in the compiled path; the interpreter materializes row results.
func BenchmarkExpressions(b *testing.B) {
	for _, size := range []int{1, 1024, 65536} {
		for _, kind := range []string{
			"constant", "arithmetic", "arithmetic_selected", "parameter_selected_nulls",
			"coalesce_nulls", "case_hits0", "case_hits50", "case_hits100", "nested_selected_nulls",
		} {
			fixture := makeExpressionBenchFixture(size, kind)
			name := fmt.Sprintf("rows=%d/%s", size, kind)
			b.Run(name+"/vector", func(b *testing.B) {
				ctx := vectorized.WithQueryMemoryTracker(b.Context(), 256<<20)
				source := &expressionBenchSource{batch: fixture.batch}
				operator, err := WrapExpressions(ctx, source, &ExpressionPlan{Outputs: []ExpressionOutput{{
					Expr: fixture.expression, Column: vectorized.ColumnDef{Name: "result", Role: vectorized.RoleField},
				}}})
				if err != nil {
					b.Fatal(err)
				}
				if err = operator.Init(ctx); err != nil {
					b.Fatal(err)
				}
				defer func() {
					if closeErr := operator.Close(); closeErr != nil {
						b.Fatal(closeErr)
					}
					if vectorized.QueryMemoryTracker(ctx, 0).Used() != 0 {
						b.Fatal("expression reservation leaked")
					}
				}()
				batch, err := operator.NextBatch(ctx)
				if err != nil {
					b.Fatal(err)
				}
				validateExpressionBench(b, fixture, batch.Columns[0])
				stats, _ := ExpressionStatistics(operator)
				if kind == "constant" && stats.FullColumnAllocations != 0 {
					b.Fatal("constant allocated a full vector")
				}
				if kind == "case_hits100" && stats.LaterArmRows != 0 {
					b.Fatal("unreachable arm executed")
				}
				runtime.GC()
				b.ReportAllocs()
				b.ResetTimer()
				for iteration := 0; iteration < b.N; iteration++ {
					batch, err = operator.NextBatch(ctx)
					if err != nil {
						b.Fatal(err)
					}
					expressionBenchSink = batch.Columns[0]
				}
				b.StopTimer()
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*fixture.batch.ActiveLen()), "ns/active-row")
				b.ReportMetric(float64(fixture.batch.ActiveLen()), "active-rows/op")
				b.ReportMetric(float64(stats.LaterArmRows), "later-arm-rows/op")
			})
			b.Run(name+"/scalar", func(b *testing.B) {
				result := newExpressionBenchResult(fixture.resultType, size)
				column := runExpressionBenchScalar(fixture, result)
				validateExpressionBench(b, fixture, column)
				runtime.GC()
				b.ReportAllocs()
				b.ResetTimer()
				for iteration := 0; iteration < b.N; iteration++ {
					expressionBenchSink = runExpressionBenchScalar(fixture, result)
				}
				b.StopTimer()
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*fixture.batch.ActiveLen()), "ns/active-row")
				b.ReportMetric(float64(fixture.batch.ActiveLen()), "active-rows/op")
			})
		}
	}
}

func makeExpressionBenchFixture(size int, kind string) expressionBenchFixture {
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{{Name: "v", Role: vectorized.RoleField, Type: vectorized.ColumnTypeInt64}})
	batch := vectorized.NewRecordBatch(schema, size)
	column := batch.Columns[0].(*vectorized.TypedColumn[int64])
	nullable := kind == "parameter_selected_nulls" || kind == "coalesce_nulls" || kind == "nested_selected_nulls"
	for row := 0; row < size; row++ {
		value := int64(row%6) * 2
		if kind == "case_hits0" {
			value = 2
		}
		if kind == "case_hits50" {
			value = int64(row%2) * 2
		}
		if kind == "case_hits100" {
			value = 0
		}
		if nullable && row%4 == 2 {
			column.AppendNull()
		} else {
			column.Append(value)
		}
	}
	batch.Len = size
	if kind == "arithmetic_selected" || kind == "parameter_selected_nulls" || kind == "nested_selected_nulls" {
		batch.Selection = make([]uint16, 0, (size+1)/2)
		for row := 0; row < size; row += 2 {
			batch.Selection = append(batch.Selection, uint16(row))
		}
	}
	v := expressionValueColumn()
	var expression *Expression
	condition := &Expression{Op: "=", Args: []*Expression{v, expressionInt(0)}}
	ratio := &Expression{Op: "case", Args: []*Expression{condition, expressionInt(0), {Op: "/", Args: []*Expression{expressionInt(8), v}}}}
	switch kind {
	case "constant":
		expression = &Expression{Op: "cast", Type: ExpressionFloat, Args: []*Expression{{Op: "literal", Type: ExpressionString, Literal: ExpressionScalar{String: "1.5"}}}}
	case "arithmetic", "arithmetic_selected":
		expression = &Expression{Op: "+", Args: []*Expression{v, expressionInt(2)}}
	case "parameter_selected_nulls":
		expression = &Expression{Op: "+", Args: []*Expression{
			v,
			{Op: "cast", Type: ExpressionFloat, Args: []*Expression{
				{Op: "literal", Type: ExpressionString, Literal: ExpressionScalar{String: "1.5"}},
			}},
		}}
	case "coalesce_nulls":
		expression = &Expression{Op: "coalesce", Args: []*Expression{v, expressionInt(-1)}}
	case "nested_selected_nulls":
		expression = &Expression{Op: "coalesce", Args: []*Expression{ratio, expressionInt(-1)}}
	default:
		expression = ratio
	}
	typ := ExpressionInt
	if kind == "constant" || kind == "parameter_selected_nulls" {
		typ = ExpressionFloat
	}
	return expressionBenchFixture{expression: expression, batch: batch, kind: kind, resultType: typ}
}

func validateExpressionBench(tb testing.TB, fixture expressionBenchFixture, column vectorized.Column) {
	tb.Helper()
	values := fixture.batch.Columns[0].(*vectorized.TypedColumn[int64])
	constant, isConstant := vectorized.AsConst(column)
	for pos := 0; pos < fixture.batch.ActiveLen(); pos++ {
		row := pos
		if fixture.batch.Selection != nil {
			row = int(fixture.batch.Selection[pos])
		}
		value := values.Data()[row]
		null := values.IsNull(row)
		var expected float64
		switch fixture.kind {
		case "constant":
			expected = 1.5
			null = false
		case "arithmetic", "arithmetic_selected":
			expected = float64(value + 2)
		case "parameter_selected_nulls":
			expected = float64(value) + 1.5
		case "coalesce_nulls":
			expected = float64(value)
			if null {
				expected = -1
				null = false
			}
		default:
			if value != 0 {
				expected = float64(8 / value)
			}
			if fixture.kind == "nested_selected_nulls" && null {
				expected = -1
				null = false
			}
		}
		if column.IsNull(row) != null {
			tb.Fatalf("row %d null=%t expected %t", row, column.IsNull(row), null)
		}
		if null {
			continue
		}
		var actual float64
		switch {
		case isConstant && fixture.resultType == ExpressionFloat:
			actual = constant.Float64()
		case isConstant:
			actual = float64(constant.Int64())
		case fixture.resultType == ExpressionFloat:
			actual = column.(*vectorized.TypedColumn[float64]).Data()[row]
		default:
			actual = float64(column.(*vectorized.TypedColumn[int64]).Data()[row])
		}
		if actual != expected {
			tb.Fatalf("row %d got %g expected %g", row, actual, expected)
		}
	}
}

// expressionBenchCell and the interpreter below deliberately do not use compiled
// readers, type resolution, kernels, masks or scalar conversions from production.
type expressionBenchCell struct {
	text     string
	integer  int64
	floating float64
	boolean  bool
	null     bool
	typ      ExpressionType
}
type expressionBenchResult struct {
	integers []int64
	floating []float64
	nulls    []bool
	typ      ExpressionType
}

func newExpressionBenchResult(typ ExpressionType, size int) *expressionBenchResult {
	return &expressionBenchResult{integers: make([]int64, size), floating: make([]float64, size), nulls: make([]bool, size), typ: typ}
}

func runExpressionBenchScalar(fixture expressionBenchFixture, result *expressionBenchResult) vectorized.Column {
	clear(result.nulls)
	for pos := 0; pos < fixture.batch.ActiveLen(); pos++ {
		row := pos
		if fixture.batch.Selection != nil {
			row = int(fixture.batch.Selection[pos])
		}
		cell := evalExpressionBenchScalar(fixture.expression, fixture.batch, row)
		result.nulls[row] = cell.null
		if result.typ == ExpressionFloat {
			result.floating[row] = cell.floating
		} else {
			result.integers[row] = cell.integer
		}
	}
	var column vectorized.Column
	if result.typ == ExpressionFloat {
		column = vectorized.NewColumnFromSlice(vectorized.ColumnTypeFloat64, result.floating)
	} else {
		column = vectorized.NewColumnFromSlice(vectorized.ColumnTypeInt64, result.integers)
	}
	for row, null := range result.nulls {
		if null {
			column.MarkNullAt(row)
		}
	}
	return column
}

func evalExpressionBenchScalar(expr *Expression, batch *vectorized.RecordBatch, row int) expressionBenchCell {
	switch expr.Op {
	case "literal":
		return expressionBenchCell{integer: expr.Literal.Int64, floating: expr.Literal.Float64, text: expr.Literal.String, null: expr.Literal.Null, typ: expr.Type}
	case "column":
		value := batch.Columns[0].(*vectorized.TypedColumn[int64])
		return expressionBenchCell{integer: value.Data()[row], null: value.IsNull(row), typ: ExpressionInt}
	case "cast":
		cell := evalExpressionBenchScalar(expr.Args[0], batch, row)
		if cell.null {
			return expressionBenchCell{null: true, typ: expr.Type}
		}
		if cell.typ == ExpressionString {
			value, err := strconv.ParseFloat(cell.text, 64)
			if err != nil {
				panic(err)
			}
			return expressionBenchCell{floating: value, typ: ExpressionFloat}
		}
		return expressionBenchCell{floating: float64(cell.integer), typ: ExpressionFloat}
	case "coalesce":
		for _, arg := range expr.Args {
			cell := evalExpressionBenchScalar(arg, batch, row)
			if !cell.null {
				return cell
			}
		}
		return expressionBenchCell{null: true}
	case "case":
		for index := 0; index < len(expr.Args)-1; index += 2 {
			condition := evalExpressionBenchScalar(expr.Args[index], batch, row)
			if !condition.null && condition.boolean {
				return evalExpressionBenchScalar(expr.Args[index+1], batch, row)
			}
		}
		return evalExpressionBenchScalar(expr.Args[len(expr.Args)-1], batch, row)
	default:
		return evalExpressionBenchBinary(expr, batch, row)
	}
}

func evalExpressionBenchBinary(expr *Expression, batch *vectorized.RecordBatch, row int) expressionBenchCell {
	left := evalExpressionBenchScalar(expr.Args[0], batch, row)
	right := evalExpressionBenchScalar(expr.Args[1], batch, row)
	if left.null || right.null {
		return expressionBenchCell{null: true}
	}
	switch expr.Op {
	case "=":
		return expressionBenchCell{boolean: left.integer == right.integer, typ: ExpressionBool}
	case "+":
		if left.typ == ExpressionFloat || right.typ == ExpressionFloat {
			a, b := left.floating, right.floating
			if left.typ != ExpressionFloat {
				a = float64(left.integer)
			}
			if right.typ != ExpressionFloat {
				b = float64(right.integer)
			}
			return expressionBenchCell{floating: a + b, typ: ExpressionFloat}
		}
		value := left.integer + right.integer
		if (right.integer > 0 && value < left.integer) || (right.integer < 0 && value > left.integer) {
			panic("integer overflow")
		}
		return expressionBenchCell{integer: value, typ: ExpressionInt}
	case "/":
		if right.integer == 0 {
			panic("division by zero")
		}
		if left.integer == math.MinInt64 && right.integer == -1 {
			panic("integer overflow")
		}
		return expressionBenchCell{integer: left.integer / right.integer, typ: ExpressionInt}
	default:
		panic("unsupported reference expression")
	}
}
