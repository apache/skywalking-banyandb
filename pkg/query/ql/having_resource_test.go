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

func TestHavingRequiresExplicitVisibleOutputsAndOwnsSource(t *testing.T) {
	source, slots := rawFixture()
	ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
	operator, err := WrapRaw(ctx, source, &RawPlan{
		Slots:  slots,
		Having: &HavingPlan{Predicate: &Expression{Op: "literal", Type: ExpressionBool, Literal: ExpressionScalar{Bool: true}}},
	})
	if operator != nil {
		require.NoError(t, operator.Close())
	}
	require.ErrorIs(t, err, ErrInvalidExpression)
	require.Nil(t, operator)
	require.Equal(t, 1, source.closes)
	require.Zero(t, source.pulls)
	require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
}

func TestHavingFailuresOwnSourceAndReturnEveryReservation(t *testing.T) {
	failed := errors.New("completed groups failed")
	for _, phase := range []string{"constructor", "init", "pull", "source init", "source pull", "predicate", "projection", "budget"} {
		t.Run(phase, func(t *testing.T) {
			source, slots := rawFixture()
			canceled, cancel := context.WithCancel(t.Context())
			defer cancel()
			budget := int64(1 << 20)
			if phase == "budget" {
				budget = 16 << 10
			}
			ctx := vectorized.WithQueryMemoryTracker(canceled, budget)
			predicate := &Expression{Op: "literal", Type: ExpressionBool, Literal: ExpressionScalar{Bool: true}}
			expected := context.Canceled
			switch phase {
			case "constructor":
				cancel()
			case "source init":
				source.initErr = failed
				expected = failed
			case "source pull":
				expected = failed
			case "predicate":
				predicate = &Expression{Op: ">", Args: []*Expression{
					{Op: "/", Args: []*Expression{expressionInt(1), expressionValueColumn()}}, expressionInt(0),
				}}
				expected = ErrInvalidExpression
			case "projection":
				slots[0].Source.Name = "missing"
				expected = nil
			case "budget":
				predicate = &Expression{Op: "contains", Args: []*Expression{
					expressionValueColumn(), {Op: "literal", Type: ExpressionIntArray, Literal: ExpressionScalar{Ints: make([]int64, 16384)}},
				}}
				expected = vectorized.ErrMemoryBudgetExceeded
			default:
			}
			var input vectorized.PullOperator = source
			if phase == "source pull" {
				input = &filterErrorSource{rawSource: source, err: failed}
			}
			operator, err := WrapRaw(ctx, input, &RawPlan{Slots: slots, Having: &HavingPlan{Predicate: predicate, Outputs: slots}})
			if phase == "init" {
				cancel()
			}
			if err == nil {
				err = operator.Init(ctx)
			}
			if phase == "pull" {
				cancel()
			}
			if err == nil {
				_, err = operator.NextBatch(ctx)
			}
			if expected != nil {
				require.ErrorIs(t, err, expected)
			} else {
				require.Error(t, err)
			}
			if operator != nil {
				require.NoError(t, operator.Close())
			}
			require.Equal(t, 1, source.closes)
			require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
		})
	}
}

func TestHavingPredicateFailureAfterLocalAggregationReleasesGroups(t *testing.T) {
	source, plan, sum := havingFixture()
	denominator := &Expression{Op: "-", Args: []*Expression{{Op: "column", Source: sum}, expressionInt(2)}}
	quotient := &Expression{Op: "/", Args: []*Expression{expressionInt(1), denominator}}
	plan.Having = &HavingPlan{
		Predicate: &Expression{Op: ">", Args: []*Expression{quotient, expressionInt(0)}}, Outputs: []Slot{{Source: sum, Output: sum}},
	}
	ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
	operator, err := WrapRaw(ctx, source, plan)
	require.NoError(t, err)
	batch, err := operator.NextBatch(ctx)
	require.ErrorIs(t, err, ErrInvalidExpression)
	require.Nil(t, batch)
	require.NoError(t, operator.Close())
	require.Equal(t, 1, source.closes)
	require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
}

func TestHavingBorrowedGroupValuesAndEmptyProjection(t *testing.T) {
	for _, empty := range []bool{false, true} {
		source, slots := rawFixture()
		original := source.batches[0]
		original.Selection = []uint16{3, 1}
		original.Hints = vectorized.BatchHints{TimeOrder: vectorized.TimeOrderDesc}
		outputs := slots[:1]
		if empty {
			outputs = []Slot{}
		}
		ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
		operator, err := WrapRaw(ctx, source, &RawPlan{Having: &HavingPlan{
			Predicate: &Expression{Op: "literal", Type: ExpressionBool, Literal: ExpressionScalar{Bool: true}}, Outputs: outputs,
		}})
		require.NoError(t, err)
		batch, err := operator.NextBatch(ctx)
		require.NoError(t, err)
		require.Equal(t, []uint16{3, 1}, batch.Selection)
		require.Equal(t, original.Hints, batch.Hints)
		if !empty {
			require.Same(t, original.Columns[3], batch.Columns[0])
		} else {
			require.Empty(t, batch.Schema.FieldColumns)
			require.Len(t, batch.Columns, 3)
		}
		require.NoError(t, operator.Close())
		require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
	}
}
