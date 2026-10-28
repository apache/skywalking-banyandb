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
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

func filterRows(rows int) *rawSource {
	def := vectorized.ColumnDef{Name: "v", Role: vectorized.RoleField, Type: vectorized.ColumnTypeInt64}
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{def})
	batch := vectorized.NewRecordBatch(schema, rows)
	column := batch.Columns[0].(*vectorized.TypedColumn[int64])
	for row := range rows {
		column.Append(int64(row))
	}
	batch.Len = rows
	return &rawSource{schema: schema, batches: []*vectorized.RecordBatch{batch}}
}

func TestFilterSupportsLastPhysicalRowAndRejectsOversizedBatches(t *testing.T) {
	source := filterRows(65536)
	source.batches[0].Selection = []uint16{65535, 0}
	ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
	operator, err := WrapFilter(ctx, source, &Expression{Op: ">", Args: []*Expression{expressionValueColumn(), expressionInt(65534)}})
	require.NoError(t, err)
	batch, err := operator.NextBatch(ctx)
	require.NoError(t, err)
	require.Equal(t, []uint16{65535}, batch.Selection)
	require.Equal(t, 65536, batch.Len)
	require.NoError(t, operator.Close())
	require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
	for _, selection := range [][]uint16{nil, {}} {
		source = filterRows(65537)
		source.batches[0].Selection = selection
		operator, err = WrapFilter(ctx, source, &Expression{Op: "literal", Type: ExpressionBool, Literal: ExpressionScalar{Bool: true}})
		require.NoError(t, err)
		batch, err = operator.NextBatch(ctx)
		require.ErrorIs(t, err, ErrInvalidExpression)
		require.Nil(t, batch)
		require.Equal(t, 1, source.closes)
		require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
	}
}

func TestFilterFailuresReleaseCompiledStateAndSelection(t *testing.T) {
	for _, test := range []struct {
		name      string
		predicate *Expression
		selection []uint16
	}{
		{name: "non boolean", predicate: expressionInt(1)},
		{name: "missing input", predicate: &Expression{Op: ">", Args: []*Expression{
			{Op: "column", Source: vectorized.ColumnDef{Name: "absent", Role: vectorized.RoleField, Type: vectorized.ColumnTypeInt64}}, expressionInt(1),
		}}},
		{name: "invalid selected row", selection: []uint16{4}, predicate: &Expression{Op: ">", Args: []*Expression{expressionValueColumn(), expressionInt(1)}}},
		{name: "arithmetic failure", predicate: &Expression{Op: ">", Args: []*Expression{
			{Op: "/", Args: []*Expression{expressionInt(1), expressionValueColumn()}}, expressionInt(0),
		}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			source, _ := rawFixture()
			source.batches[0].Selection = test.selection
			ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
			operator, err := WrapFilter(ctx, source, test.predicate)
			if err == nil {
				_, err = operator.NextBatch(ctx)
			}
			require.ErrorIs(t, err, ErrInvalidExpression)
			if operator != nil {
				require.NoError(t, operator.Close())
			}
			require.Equal(t, 1, source.closes)
			require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
		})
	}
	source, _ := rawFixture()
	ctx := vectorized.WithQueryMemoryTracker(t.Context(), 16<<10)
	predicate := &Expression{Op: "=", Args: []*Expression{
		{Op: "literal", Type: ExpressionBytes, Literal: ExpressionScalar{Bytes: make([]byte, 64<<10)}},
		{Op: "literal", Type: ExpressionBytes},
	}}
	operator, err := WrapFilter(ctx, source, predicate)
	require.ErrorIs(t, err, vectorized.ErrMemoryBudgetExceeded)
	require.Nil(t, operator)
	require.Equal(t, 1, source.closes)
	require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
}

func TestFilterBudgetFailureOnBatchGrowthReturnsAllReservations(t *testing.T) {
	source := filterRows(4)
	larger := filterRows(65536)
	source.batches = append(source.batches, larger.batches[0])
	ctx := vectorized.WithQueryMemoryTracker(t.Context(), 32<<10)
	operator, err := WrapFilter(ctx, source, &Expression{Op: ">", Args: []*Expression{expressionValueColumn(), expressionInt(0)}})
	require.NoError(t, err)
	batch, err := operator.NextBatch(ctx)
	require.NoError(t, err)
	require.Equal(t, []uint16{1, 2, 3}, batch.Selection)
	batch, err = operator.NextBatch(ctx)
	require.ErrorIs(t, err, vectorized.ErrMemoryBudgetExceeded)
	require.Nil(t, batch)
	require.Equal(t, 1, source.closes)
	stats, ok := FilterStatistics(operator)
	require.True(t, ok)
	require.Zero(t, stats.LiveBytes)
	require.Equal(t, stats.ReservedBytes, stats.ReleasedBytes)
	require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
}

type filterErrorSource struct {
	*rawSource
	err error
}

func (s *filterErrorSource) NextBatch(context.Context) (*vectorized.RecordBatch, error) {
	return nil, s.err
}

func TestFilterPreservesSourceFailuresAndOwnsCancellation(t *testing.T) {
	failed := errors.New("source failed")
	predicate := &Expression{Op: "literal", Type: ExpressionBool, Literal: ExpressionScalar{Bool: true}}
	for _, phase := range []string{"constructor", "init", "pull", "source init", "source pull"} {
		t.Run(phase, func(t *testing.T) {
			source, _ := rawFixture()
			canceled, cancel := context.WithCancel(t.Context())
			defer cancel()
			ctx := vectorized.WithQueryMemoryTracker(canceled, 1<<20)
			expected := context.Canceled
			if phase == "constructor" {
				cancel()
			}
			var input vectorized.PullOperator = source
			switch phase {
			case "source init":
				source.initErr = failed
				expected = failed
			case "source pull":
				input = &filterErrorSource{rawSource: source, err: failed}
				expected = failed
			}
			operator, err := WrapFilter(ctx, input, predicate)
			if phase == "init" || phase == "pull" {
				cancel()
			}
			if err == nil {
				if phase == "init" || phase == "source init" {
					err = operator.Init(ctx)
				} else {
					_, err = operator.NextBatch(ctx)
				}
			}
			require.ErrorIs(t, err, expected)
			if operator != nil {
				require.NoError(t, operator.Close())
			}
			require.Equal(t, 1, source.closes)
			require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
		})
	}
}
