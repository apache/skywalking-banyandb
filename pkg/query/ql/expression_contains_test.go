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
	"testing"

	"github.com/stretchr/testify/require"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

func containsFixture() (*rawSource, vectorized.ColumnDef) {
	def := vectorized.ColumnDef{Name: "labels", TagFamily: "default", Role: vectorized.RoleTag, Type: vectorized.ColumnTypeStrArray}
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{def})
	batch := vectorized.NewRecordBatch(schema, 4)
	column := batch.Columns[0].(*vectorized.TypedColumn[[]string])
	column.AppendSlice([][]string{{"red", "blue"}, {"blue"}, {}, nil})
	column.MarkNullAt(3)
	batch.Len = 4
	batch.Selection = []uint16{3, 2, 1, 0}
	return &rawSource{schema: schema, batches: []*vectorized.RecordBatch{batch}}, def
}

func TestContainsKeepsLegacyArrayMembershipAndBorrowsValues(t *testing.T) {
	source, def := containsFixture()
	original := source.batches[0]
	ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
	operator, err := WrapFilter(ctx, source, &Expression{Op: "contains", Args: []*Expression{
		{Op: "column", Source: def}, {Op: "literal", Type: ExpressionString, Literal: ExpressionScalar{String: "red"}},
	}})
	require.NoError(t, err)
	require.NoError(t, operator.Init(ctx))
	batch, err := operator.NextBatch(ctx)
	require.NoError(t, err)
	require.Equal(t, []uint16{0}, batch.Selection)
	require.Same(t, original.Schema, batch.Schema)
	require.Same(t, original.Columns[0], batch.Columns[0])
	require.Equal(t, []uint16{3, 2, 1, 0}, original.Selection)
	require.NoError(t, operator.Close())
	require.Equal(t, 1, source.closes)
	require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
}

func TestContainsPreservesScalarListNullAndEmptyLegacySemantics(t *testing.T) {
	for _, integer := range []bool{false, true} {
		for _, array := range []bool{false, true} {
			for _, protobuf := range []bool{false, true} {
				for _, test := range []struct {
					name         string
					values       []string
					expected     []uint16
					scalar, null bool
				}{
					{name: "scalar", values: []string{"red"}, scalar: true, expected: []uint16{0}},
					{name: "one-list", values: []string{"red"}, expected: []uint16{0}},
					{name: "all-required", values: []string{"red", "blue"}, expected: []uint16{0}},
					{name: "shared", values: []string{"blue"}, expected: []uint16{0, 1}},
					{name: "duplicates", values: []string{"blue", "blue"}, expected: []uint16{0, 1}},
					{name: "empty"},
					{name: "null", null: true},
					{name: "absent", values: []string{"missing"}},
				} {
					for _, negate := range []bool{false, true} {
						source, def := containsMatrixFixture(integer, array, protobuf)
						expected := test.expected
						if !array && len(test.values) != 1 && test.name != "duplicates" {
							expected = nil
						} else if !array && len(test.values) > 0 && test.values[0] == "blue" {
							expected = []uint16{1}
						}
						if negate {
							complement := []uint16{}
							for row := uint16(0); row < 4; row++ {
								found := false
								for _, matched := range expected {
									found = found || matched == row
								}
								if !found {
									complement = append(complement, row)
								}
							}
							expected = complement
						}
						rhs := &Expression{Op: "literal", Type: ExpressionStringArray, Literal: ExpressionScalar{Strings: test.values}}
						if integer {
							rhs.Type = ExpressionIntArray
							for _, value := range test.values {
								mapped := int64(9)
								if value == "red" {
									mapped = 1
								}
								if value == "blue" {
									mapped = 2
								}
								rhs.Literal.Ints = append(rhs.Literal.Ints, mapped)
							}
						}
						if test.scalar {
							if integer {
								rhs.Type, rhs.Literal.Int64 = ExpressionInt, 1
							} else {
								rhs.Type, rhs.Literal.String = ExpressionString, "red"
							}
						}
						if test.null {
							rhs = &Expression{Op: "literal", Type: ExpressionNull, Literal: ExpressionScalar{Null: true}}
						}
						op := "contains"
						if negate {
							op = "not_contains"
						}
						ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
						operator, err := WrapFilter(ctx, source, &Expression{Op: op, Args: []*Expression{{Op: "column", Source: def}, rhs}})
						require.NoError(t, err, test.name)
						batch, err := operator.NextBatch(ctx)
						require.NoError(t, err, test.name)
						actual := []uint16{}
						if batch != nil {
							for pos := range batch.ActiveLen() {
								row := uint16(pos)
								if batch.Selection != nil {
									row = batch.Selection[pos]
								}
								actual = append(actual, row)
							}
						}
						require.ElementsMatch(t, expected, actual, "case=%s int=%v array=%v protobuf=%v negate=%v", test.name, integer, array, protobuf, negate)
						require.NoError(t, operator.Close())
						require.Equal(t, 1, source.closes)
						require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
					}
				}
			}
		}
	}
}

func containsMatrixFixture(integer, array, protobuf bool) (*rawSource, vectorized.ColumnDef) {
	typ := vectorized.ColumnTypeString
	if array {
		typ = vectorized.ColumnTypeStrArray
	}
	if integer {
		typ = vectorized.ColumnTypeInt64
		if array {
			typ = vectorized.ColumnTypeInt64Array
		}
	}
	def := vectorized.ColumnDef{Name: "labels", TagFamily: "default", Role: vectorized.RoleTag, Type: typ}
	physical := def
	if protobuf {
		physical.Type = vectorized.ColumnTypeTagValue
	}
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{physical})
	batch := vectorized.NewRecordBatch(schema, 4)
	if protobuf {
		column := batch.Columns[0].(*vectorized.TypedColumn[*modelv1.TagValue])
		for row := range 4 {
			cell := &modelv1.TagValue{Value: &modelv1.TagValue_Null{}}
			if row < 3 {
				switch typ {
				case vectorized.ColumnTypeString:
					cell.Value = &modelv1.TagValue_Str{Str: &modelv1.Str{Value: []string{"red", "blue", ""}[row]}}
				case vectorized.ColumnTypeInt64:
					cell.Value = &modelv1.TagValue_Int{Int: &modelv1.Int{Value: []int64{1, 2, 0}[row]}}
				case vectorized.ColumnTypeStrArray:
					cell.Value = &modelv1.TagValue_StrArray{StrArray: &modelv1.StrArray{Value: [][]string{{"red", "blue"}, {"blue"}, {}}[row]}}
				case vectorized.ColumnTypeInt64Array:
					cell.Value = &modelv1.TagValue_IntArray{IntArray: &modelv1.IntArray{Value: [][]int64{{1, 2}, {2}, {}}[row]}}
				default:
				}
			}
			column.Append(cell)
		}
	} else {
		switch typ {
		case vectorized.ColumnTypeString:
			batch.Columns[0].(*vectorized.TypedColumn[string]).AppendSlice([]string{"red", "blue", "", ""})
		case vectorized.ColumnTypeInt64:
			batch.Columns[0].(*vectorized.TypedColumn[int64]).AppendSlice([]int64{1, 2, 0, 0})
		case vectorized.ColumnTypeStrArray:
			batch.Columns[0].(*vectorized.TypedColumn[[]string]).AppendSlice([][]string{{"red", "blue"}, {"blue"}, {}, nil})
		case vectorized.ColumnTypeInt64Array:
			batch.Columns[0].(*vectorized.TypedColumn[[]int64]).AppendSlice([][]int64{{1, 2}, {2}, {}, nil})
		default:
		}
		batch.Columns[0].MarkNullAt(3)
	}
	batch.Len = 4
	return &rawSource{schema: schema, batches: []*vectorized.RecordBatch{batch}}, def
}

func TestContainsComposesWithShortCircuitMasks(t *testing.T) {
	source, labels := containsFixture()
	batch := source.batches[0]
	divisor := aggregateField("divisor", vectorized.ColumnTypeInt64)
	defs := append(append([]vectorized.ColumnDef(nil), source.schema.Columns...), divisor)
	source.schema = vectorized.NewBatchSchema(defs)
	batch.Schema = source.schema
	values := vectorized.NewRecordBatch(vectorized.NewBatchSchema([]vectorized.ColumnDef{divisor}), 4).Columns[0]
	values.(*vectorized.TypedColumn[int64]).AppendSlice([]int64{0, 1, 1, 1})
	batch.Columns = append(batch.Columns, values)
	predicate := &Expression{Op: "or", Args: []*Expression{
		{Op: "contains", Args: []*Expression{{Op: "column", Source: labels}, {Op: "literal", Type: ExpressionString, Literal: ExpressionScalar{String: "red"}}}},
		{Op: ">", Args: []*Expression{{Op: "/", Args: []*Expression{expressionInt(1), {Op: "column", Source: divisor}}}, expressionInt(0)}},
	}}
	ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
	operator, err := WrapFilter(ctx, source, predicate)
	require.NoError(t, err)
	output, err := operator.NextBatch(ctx)
	require.NoError(t, err)
	require.Equal(t, []uint16{3, 2, 1, 0}, output.Selection)
	require.NoError(t, operator.Close())
	require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
}
