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
	"time"

	"github.com/stretchr/testify/require"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

func TestExpressionQueryFiltersBeforeEvaluationAndPaginatesAfter(t *testing.T) {
	value := vectorized.ColumnDef{Name: "v", Role: vectorized.RoleField, Type: vectorized.ColumnTypeInt64}
	defs := []vectorized.ColumnDef{
		{Name: "time", Role: vectorized.RoleTimestamp, Type: vectorized.ColumnTypeInt64},
		{Name: "sid", Role: vectorized.RoleSeriesID, Type: vectorized.ColumnTypeInt64},
		{Name: "version", Role: vectorized.RoleVersion, Type: vectorized.ColumnTypeInt64},
		value,
	}
	schema := vectorized.NewBatchSchema(defs)
	batch := vectorized.NewRecordBatch(schema, 4)
	for row, input := range []int64{0, 2, 4, 8} {
		batch.Columns[0].(*vectorized.TypedColumn[int64]).Append(int64(row) * int64(time.Second))
		batch.Columns[1].(*vectorized.TypedColumn[int64]).Append(7)
		batch.Columns[2].(*vectorized.TypedColumn[int64]).Append(9)
		batch.Columns[3].(*vectorized.TypedColumn[int64]).Append(input)
	}
	batch.Len = 4
	source := &rawSource{schema: schema, batches: []*vectorized.RecordBatch{batch}}
	limit := uint32(1)
	ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
	op, err := WrapRaw(ctx, source, &RawPlan{
		Slots: []Slot{{Source: value, Output: value}},
		Expressions: &ExpressionPlan{Outputs: []ExpressionOutput{{
			Expr: &Expression{Op: "/", Args: []*Expression{
				{Op: "literal", Type: ExpressionInt, Literal: ExpressionScalar{Int64: 8}},
				{Op: "column", Source: value},
			}},
			Column: vectorized.ColumnDef{Name: "ratio", Role: vectorized.RoleField, Type: vectorized.ColumnTypeInt64},
		}}},
		Ranges:   []MillisRange{{Begin: 1000, End: 1001}, {Begin: 2000, End: 2001}},
		Residual: true, Limit: &limit, Offset: 1,
	})
	require.NoError(t, err)
	iterator, err := RawIterator(ctx, op)
	require.NoError(t, err)
	require.True(t, iterator.Next())
	point := iterator.Current()[0].DataPoint
	require.Equal(t, int64(2), point.Fields[0].Value.GetInt().Value)
	require.Equal(t, "ratio", point.Fields[0].Name)
	require.Equal(t, time.Unix(2, 0).UTC(), point.Timestamp.AsTime())
	require.EqualValues(t, 7, point.Sid)
	require.EqualValues(t, 9, point.Version)
	require.False(t, iterator.Next())
	if failure, ok := iterator.(interface{ Err() error }); ok {
		require.NoError(t, failure.Err())
	}
	require.NoError(t, iterator.Close())
	require.Equal(t, 1, source.closes)
	require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
}

func TestExpressionConstantTimestampUsesExistingTagEncoding(t *testing.T) {
	source, slots := rawFixture()
	stamp := time.Date(2026, time.January, 2, 3, 4, 5, 123456789, time.UTC)
	output := vectorized.ColumnDef{Name: "clock", TagFamily: "__ql", Role: vectorized.RoleTag, Type: vectorized.ColumnTypeInt64}
	absent := output
	absent.Name = "absent"
	ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
	op, err := WrapRaw(ctx, source, &RawPlan{
		Slots: slots[:1],
		Expressions: &ExpressionPlan{Outputs: []ExpressionOutput{
			{Expr: &Expression{Op: "literal", Type: ExpressionTimestamp, Literal: ExpressionScalar{Int64: stamp.UnixNano()}}, Column: output},
			{Expr: &Expression{Op: "literal", Type: ExpressionTimestamp, Literal: ExpressionScalar{Null: true}}, Column: absent},
		}},
	})
	require.NoError(t, err)
	iterator, err := RawIterator(ctx, op)
	require.NoError(t, err)
	rows := 0
	for iterator.Next() {
		point := iterator.Current()[0].DataPoint
		require.Len(t, point.TagFamilies, 1)
		require.Equal(t, "__ql", point.TagFamilies[0].Name)
		tags := point.TagFamilies[0].Tags
		require.Equal(t, stamp, tags[0].Value.GetTimestamp().AsTime())
		_, isNull := tags[1].Value.Value.(*modelv1.TagValue_Null)
		require.True(t, isNull)
		rows++
	}
	if failure, ok := iterator.(interface{ Err() error }); ok {
		require.NoError(t, failure.Err())
	}
	require.Equal(t, 4, rows)
	require.NoError(t, iterator.Close())
	require.Equal(t, 1, source.closes)
	require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
}
