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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

func TestFilterKeepsOnlyTrueRowsAndBorrowsPhysicalColumns(t *testing.T) {
	source, _ := rawFixture()
	original := source.batches[0]
	original.Selection = []uint16{3, 2, 0}
	original.Hints = vectorized.BatchHints{SeriesKnown: true, TimeOrder: vectorized.TimeOrderDesc}
	ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
	predicate := &Expression{Op: ">", Args: []*Expression{expressionValueColumn(), expressionInt(0)}}
	operator, err := WrapFilter(ctx, source, predicate)
	require.NoError(t, err)
	require.NoError(t, operator.Init(ctx))
	batch, err := operator.NextBatch(ctx)
	require.NoError(t, err)
	require.Equal(t, []uint16{3}, batch.Selection)
	require.Equal(t, 4, batch.Len)
	require.Same(t, original.Schema, batch.Schema)
	require.Equal(t, original.Hints, batch.Hints)
	for index := range original.Columns {
		require.Same(t, original.Columns[index], batch.Columns[index])
	}
	require.Equal(t, []uint16{3, 2, 0}, original.Selection)
	stats, ok := FilterStatistics(operator)
	require.True(t, ok)
	require.Equal(t, uint64(3), stats.InputRows)
	require.Equal(t, uint64(1), stats.TrueRows)
	require.Equal(t, uint64(1), stats.FalseRows)
	require.Equal(t, uint64(1), stats.UnknownRows)
	require.Zero(t, stats.CopiedBytes)
	require.NoError(t, operator.Close())
	require.NoError(t, operator.Close())
	require.Equal(t, 1, source.closes)
	require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
}

type filterCancelSource struct {
	*rawSource
	cancel context.CancelFunc
}

func (s *filterCancelSource) NextBatch(ctx context.Context) (*vectorized.RecordBatch, error) {
	batch, err := s.rawSource.NextBatch(ctx)
	s.cancel()
	return batch, err
}

func TestFilterCancellationDuringSourcePullReleasesTheQuery(t *testing.T) {
	source, _ := rawFixture()
	canceled, cancel := context.WithCancel(t.Context())
	defer cancel()
	ctx := vectorized.WithQueryMemoryTracker(canceled, 1<<20)
	operator, err := WrapFilter(ctx, &filterCancelSource{rawSource: source, cancel: cancel},
		&Expression{Op: "literal", Type: ExpressionBool, Literal: ExpressionScalar{Bool: true}})
	require.NoError(t, err)
	batch, err := operator.NextBatch(ctx)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, batch)
	require.Equal(t, 1, source.closes)
	require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
}

func TestFilterConstantsPreserveSelectionsAndSkipEmptyInput(t *testing.T) {
	for _, test := range []struct {
		name       string
		value      bool
		null       bool
		contextual bool
		output     bool
		unknown    uint64
	}{
		{name: "true", value: true, output: true},
		{name: "false"},
		{name: "unknown", null: true, unknown: 2},
		{name: "unknown_context", null: true, contextual: true, unknown: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			source, _ := rawFixture()
			original := source.batches[0]
			original.Selection = []uint16{3, 0}
			ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
			predicate := &Expression{Op: "literal", Type: ExpressionBool, Literal: ExpressionScalar{Bool: test.value, Null: test.null}}
			if test.contextual {
				predicate.Type = ExpressionNull
			}
			operator, err := WrapFilter(ctx, source, predicate)
			require.NoError(t, err)
			batch, err := operator.NextBatch(ctx)
			require.NoError(t, err)
			if test.output {
				require.Equal(t, []uint16{3, 0}, batch.Selection)
				batch, err = operator.NextBatch(ctx)
				require.NoError(t, err)
			}
			require.Nil(t, batch)
			stats, ok := FilterStatistics(operator)
			require.True(t, ok)
			require.Equal(t, test.unknown, stats.UnknownRows)
			require.Zero(t, stats.SelectionBytes)
			require.Zero(t, stats.LiveBytes)
			require.Equal(t, stats.ReservedBytes, stats.ReleasedBytes)
			require.Equal(t, 1, source.closes)
			require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
		})
	}
	source, _ := rawFixture()
	source.batches[0].Selection = []uint16{}
	ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
	operator, err := WrapFilter(ctx, source, &Expression{Op: "=", Args: []*Expression{
		{Op: "/", Args: []*Expression{expressionInt(1), expressionInt(0)}}, expressionInt(0),
	}})
	require.NoError(t, err)
	batch, err := operator.NextBatch(ctx)
	require.NoError(t, err)
	require.Nil(t, batch)
	stats, ok := FilterStatistics(operator)
	require.True(t, ok)
	require.Zero(t, stats.Executions)
	require.Zero(t, stats.InputRows)
	require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
}

func TestFilterReusesMaskedBooleanKernels(t *testing.T) {
	for _, test := range []struct {
		name      string
		predicate *Expression
		expected  []uint16
	}{
		{
			name: "and skips zero divisor",
			predicate: &Expression{Op: "and", Args: []*Expression{
				{Op: "!=", Args: []*Expression{expressionValueColumn(), expressionInt(0)}},
				{Op: ">", Args: []*Expression{{Op: "/", Args: []*Expression{expressionInt(8), expressionValueColumn()}}, expressionInt(2)}},
			}},
			expected: []uint16{1},
		},
		{
			name: "or skips zero divisor",
			predicate: &Expression{Op: "or", Args: []*Expression{
				{Op: "=", Args: []*Expression{expressionValueColumn(), expressionInt(0)}},
				{Op: ">", Args: []*Expression{{Op: "/", Args: []*Expression{expressionInt(8), expressionValueColumn()}}, expressionInt(2)}},
			}},
			expected: []uint16{0, 1},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			source, _ := rawFixture()
			ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
			operator, err := WrapFilter(ctx, source, test.predicate)
			require.NoError(t, err)
			batch, err := operator.NextBatch(ctx)
			require.NoError(t, err)
			require.Equal(t, test.expected, batch.Selection)
			require.NoError(t, operator.Close())
			require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
		})
	}
}

func TestFilterLendsSourceBytesAndAllowsExplicitRetention(t *testing.T) {
	source, _ := rawFixture()
	ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
	operator, err := WrapFilter(ctx, source, &Expression{Op: ">", Args: []*Expression{expressionValueColumn(), expressionInt(0)}})
	require.NoError(t, err)
	batch, err := operator.NextBatch(ctx)
	require.NoError(t, err)
	borrowed := batch.Columns[3].(*vectorized.TypedColumn[[]byte]).Data()[1]
	retained, err := RetainBatchView(ctx, batch, []int{3})
	require.NoError(t, err)
	require.NoError(t, operator.Close())
	require.Equal(t, []byte("xxxxx"), borrowed)
	require.Equal(t, []byte("first"), retained.Columns[0].(*vectorized.TypedColumn[[]byte]).Data()[1])
	retained.ReleaseOwners()
	retained.ReleaseOwners()
	require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
}
