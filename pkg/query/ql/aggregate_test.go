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
	"testing"

	"github.com/stretchr/testify/require"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

func aggregateField(name string, typ vectorized.ColumnType) vectorized.ColumnDef {
	return vectorized.ColumnDef{Name: name, Role: vectorized.RoleField, Type: typ}
}

func aggregateFixture(passthrough bool) (*rawSource, []vectorized.ColumnDef) {
	defs := []vectorized.ColumnDef{
		{
			Name:      "zone",
			TagFamily: "default",
			Role:      vectorized.RoleTag,
			Type:      vectorized.ColumnTypeString,
		},
		aggregateField("v", vectorized.ColumnTypeInt64),
		aggregateField("f", vectorized.ColumnTypeFloat64),
	}
	actual := append([]vectorized.ColumnDef(nil), defs...)
	if passthrough {
		actual[0].Type = vectorized.ColumnTypeTagValue
		actual[1].Type = vectorized.ColumnTypeFieldValue
		actual[2].Type = vectorized.ColumnTypeFieldValue
	}
	schema := vectorized.NewBatchSchema(actual)
	batch := vectorized.NewRecordBatch(schema, 6)
	zones := []string{"x", "x", "y", "y", "", ""}
	ints := []int64{0, 2, 4, 0, 6, 0}
	floats := []float64{.25, .75, 1.5, 0, 2.5, 4}
	for i := range 6 {
		if passthrough {
			z := &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: zones[i]}}}
			if i == 4 {
				z = nil
			}
			batch.Columns[0].(*vectorized.TypedColumn[*modelv1.TagValue]).Append(z)
			var v *modelv1.FieldValue
			if i != 3 && i != 5 {
				v = &modelv1.FieldValue{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: ints[i]}}}
			}
			batch.Columns[1].(*vectorized.TypedColumn[*modelv1.FieldValue]).Append(v)
			var f *modelv1.FieldValue
			if i != 3 {
				f = &modelv1.FieldValue{Value: &modelv1.FieldValue_Float{Float: &modelv1.Float{Value: floats[i]}}}
			}
			batch.Columns[2].(*vectorized.TypedColumn[*modelv1.FieldValue]).Append(f)
		} else {
			if i == 4 {
				batch.Columns[0].AppendNull()
			} else {
				batch.Columns[0].(*vectorized.TypedColumn[string]).Append(zones[i])
			}
			if i == 3 || i == 5 {
				batch.Columns[1].AppendNull()
			} else {
				batch.Columns[1].(*vectorized.TypedColumn[int64]).Append(ints[i])
			}
			if i == 3 {
				batch.Columns[2].AppendNull()
			} else {
				batch.Columns[2].(*vectorized.TypedColumn[float64]).Append(floats[i])
			}
		}
	}
	batch.Len = 6
	return &rawSource{schema: schema, batches: []*vectorized.RecordBatch{batch}}, defs
}

func aggregateCallsPlan(defs []vectorized.ColumnDef) *AggregatePlan {
	calls := []AggregateCall{
		{Source: defs[1], Function: AggregateCount},
		{Source: defs[1], Function: AggregateSum},
		{Source: defs[1], Function: AggregateMin},
		{Source: defs[1], Function: AggregateMax},
		{Source: defs[1], Function: AggregateAvg},
		{Source: defs[2], Function: AggregateAvg},
		{Source: defs[2], Function: AggregateSum},
		{Source: defs[2], Function: AggregateCount},
	}
	outputs := make([]AggregateOutput, len(calls))
	for i, call := range calls {
		typ := call.Source.Type
		if call.Function == AggregateCount {
			typ = vectorized.ColumnTypeInt64
		}
		if call.Function == AggregateAvg {
			typ = vectorized.ColumnTypeFloat64
		}
		outputs[i] = AggregateOutput{Column: aggregateField("result_"+string(rune('a'+i)), typ), Index: i}
	}
	return &AggregatePlan{Calls: calls, Outputs: outputs}
}

func TestAggregateScalarSQLNullsAndSharedState(t *testing.T) {
	for _, passthrough := range []bool{false, true} {
		t.Run(map[bool]string{false: "typed", true: "native-passthrough"}[passthrough], func(t *testing.T) {
			source, defs := aggregateFixture(passthrough)
			ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
			op, err := WrapAggregate(ctx, source, aggregateCallsPlan(defs))
			require.NoError(t, err)
			require.NoError(t, op.Init(ctx))
			batch, err := op.NextBatch(ctx)
			require.NoError(t, err)
			require.Equal(t, 1, batch.Len)
			require.Equal(t, -1, batch.Schema.TimestampIndex())
			require.Equal(t, int64(4), batch.Columns[0].(*vectorized.TypedColumn[int64]).Data()[0])
			require.Equal(t, int64(12), batch.Columns[1].(*vectorized.TypedColumn[int64]).Data()[0])
			require.Equal(t, int64(0), batch.Columns[2].(*vectorized.TypedColumn[int64]).Data()[0])
			require.Equal(t, int64(6), batch.Columns[3].(*vectorized.TypedColumn[int64]).Data()[0])
			require.Equal(t, 3., batch.Columns[4].(*vectorized.TypedColumn[float64]).Data()[0])
			require.InDelta(t, 1.8, batch.Columns[5].(*vectorized.TypedColumn[float64]).Data()[0], 1e-12)
			require.Equal(t, 9., batch.Columns[6].(*vectorized.TypedColumn[float64]).Data()[0])
			require.Equal(t, int64(5), batch.Columns[7].(*vectorized.TypedColumn[int64]).Data()[0])
			stats, ok := AggregateStatistics(op)
			require.True(t, ok)
			require.EqualValues(t, 6, stats.InputRows)
			require.EqualValues(t, 1, stats.Groups)
			require.EqualValues(t, 7, stats.StateLanes)
			require.NoError(t, op.Close())
			require.NoError(t, op.Close())
			require.Equal(t, 1, source.closes)
			require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
		})
	}
}

func TestAggregateGroupingKeepsNullAndEmptyApart(t *testing.T) {
	source, defs := aggregateFixture(false)
	plan := aggregateCallsPlan(defs)
	plan.GroupBy = []vectorized.ColumnDef{defs[0]}
	plan.Outputs = append([]AggregateOutput{{Column: defs[0], Index: 0, IsGroup: true}}, plan.Outputs...)
	op, err := WrapAggregate(t.Context(), source, plan)
	require.NoError(t, err)
	require.NoError(t, op.Init(t.Context()))
	batch, err := op.NextBatch(t.Context())
	require.NoError(t, err)
	require.Equal(t, 4, batch.Len)
	zones := batch.Columns[0].(*vectorized.TypedColumn[string])
	require.Equal(t, []string{"x", "y", "", ""}, zones.Data())
	require.True(t, zones.IsNull(2))
	require.False(t, zones.IsNull(3))
	require.True(t, batch.Columns[2].IsNull(3))
	require.Equal(t, []int64{2, 1, 1, 0}, batch.Columns[1].(*vectorized.TypedColumn[int64]).Data())
	require.NoError(t, op.Close())
}

func TestAggregateScalarAndGroupedEmpty(t *testing.T) {
	for _, grouped := range []bool{false, true} {
		source, defs := aggregateFixture(false)
		source.batches = nil
		plan := aggregateCallsPlan(defs)
		if grouped {
			plan.GroupBy = []vectorized.ColumnDef{defs[0]}
		}
		op, err := WrapAggregate(t.Context(), source, plan)
		require.NoError(t, err)
		require.NoError(t, op.Init(t.Context()))
		batch, err := op.NextBatch(t.Context())
		require.NoError(t, err)
		if grouped {
			require.Nil(t, batch)
		} else {
			require.Equal(t, 1, batch.Len)
			require.False(t, batch.Columns[0].IsNull(0))
			require.Equal(t, int64(0), batch.Columns[0].(*vectorized.TypedColumn[int64]).Data()[0])
			for _, i := range []int{1, 2, 3, 4, 5, 6} {
				require.True(t, batch.Columns[i].IsNull(0))
			}
		}
		require.NoError(t, op.Close())
		require.Equal(t, 1, source.closes)
	}
}

func TestAggregateGroupFloatCanonicalizationAndTypedBoundaries(t *testing.T) {
	defs := []vectorized.ColumnDef{aggregateField("key", vectorized.ColumnTypeFloat64), aggregateField("v", vectorized.ColumnTypeInt64)}
	schema := vectorized.NewBatchSchema(defs)
	batch := vectorized.NewRecordBatch(schema, 5)
	batch.Columns[0].(*vectorized.TypedColumn[float64]).AppendSlice([]float64{0, math.Copysign(0, -1), math.NaN(), math.Float64frombits(0x7ff8000000000011), 1})
	batch.Columns[1].(*vectorized.TypedColumn[int64]).AppendSlice([]int64{1, 1, 1, 1, 1})
	batch.Len = 5
	source := &rawSource{schema: schema, batches: []*vectorized.RecordBatch{batch}}
	plan := aggregateCountPlan(defs[:1], defs[1])
	op, err := WrapAggregate(t.Context(), source, plan)
	require.NoError(t, err)
	require.NoError(t, op.Init(t.Context()))
	out, err := op.NextBatch(t.Context())
	require.NoError(t, err)
	require.Equal(t, 3, out.Len)
	require.Equal(t, []int64{2, 2, 1}, out.Columns[1].(*vectorized.TypedColumn[int64]).Data())
	require.False(t, math.Signbit(out.Columns[0].(*vectorized.TypedColumn[float64]).Data()[0]))
	require.True(t, math.IsNaN(out.Columns[0].(*vectorized.TypedColumn[float64]).Data()[1]))
	require.NoError(t, op.Close())
}

func TestAggregateCancellationAndBudgetFailureRelease(t *testing.T) {
	source, defs := aggregateFixture(false)
	ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
	op, err := WrapAggregate(ctx, source, aggregateCallsPlan(defs))
	require.NoError(t, err)
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = op.NextBatch(canceled)
	require.ErrorIs(t, err, context.Canceled)
	require.NoError(t, op.Close())
	require.Equal(t, 1, source.closes)
	require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
	source, defs = aggregateFixture(false)
	ctx = vectorized.WithQueryMemoryTracker(t.Context(), 1)
	_, err = WrapAggregate(ctx, source, aggregateCallsPlan(defs))
	require.ErrorIs(t, err, vectorized.ErrMemoryBudgetExceeded)
	require.Equal(t, 1, source.closes)
	require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
}

func TestAggregateSpanningSelectionBoundaryMatchesIndependentScalar(t *testing.T) {
	const rows, groupCount = 131073, 513
	defs := []vectorized.ColumnDef{aggregateField("key", vectorized.ColumnTypeInt64), aggregateField("f", vectorized.ColumnTypeFloat64)}
	schema := vectorized.NewBatchSchema(defs)
	source := &rawSource{schema: schema}
	type scalar struct {
		count         int64
		sum, min, max float64
	}
	expected := make(map[int64]scalar)
	for begin := 0; begin < rows; begin += 65536 {
		end := min(begin+65536, rows)
		batch := vectorized.NewRecordBatch(schema, end-begin)
		for row := begin; row < end; row++ {
			key := int64(row % groupCount)
			batch.Columns[0].(*vectorized.TypedColumn[int64]).Append(key)
			if row%7 == 0 {
				batch.Columns[1].AppendNull()
				if _, ok := expected[key]; !ok {
					expected[key] = scalar{}
				}
				continue
			}
			value := float64(row%23-11) / 4
			batch.Columns[1].(*vectorized.TypedColumn[float64]).Append(value)
			state := expected[key]
			if state.count == 0 || value < state.min {
				state.min = value
			}
			if state.count == 0 || value > state.max {
				state.max = value
			}
			state.count++
			state.sum += value
			expected[key] = state
		}
		batch.Len = end - begin
		source.batches = append(source.batches, batch)
	}
	calls := []AggregateCall{
		{Source: defs[1], Function: AggregateCount},
		{Source: defs[1], Function: AggregateSum},
		{Source: defs[1], Function: AggregateMin},
		{Source: defs[1], Function: AggregateMax},
		{Source: defs[1], Function: AggregateAvg},
	}
	outputs := []AggregateOutput{{Column: defs[0], IsGroup: true}}
	for i, call := range calls {
		outputs = append(outputs, AggregateOutput{Column: aggregateField("result_"+string(rune('a'+i)), aggregateOutputType(call)), Index: i})
	}
	ctx := vectorized.WithQueryMemoryTracker(t.Context(), 4<<20)
	op, err := WrapAggregate(ctx, source, &AggregatePlan{GroupBy: defs[:1], Calls: calls, Outputs: outputs})
	require.NoError(t, err)
	require.NoError(t, op.Init(ctx))
	seen := 0
	for {
		batch, pullErr := op.NextBatch(ctx)
		require.NoError(t, pullErr)
		if batch == nil {
			break
		}
		for row, key := range batch.Columns[0].(*vectorized.TypedColumn[int64]).Data() {
			want := expected[key]
			require.Equal(t, want.count, batch.Columns[1].(*vectorized.TypedColumn[int64]).Data()[row])
			for index, wantValue := range []float64{want.sum, want.min, want.max, want.sum / float64(want.count)} {
				require.Equal(t, wantValue, batch.Columns[index+2].(*vectorized.TypedColumn[float64]).Data()[row])
			}
			seen++
		}
	}
	require.Equal(t, groupCount, seen)
	stats, ok := AggregateStatistics(op)
	require.True(t, ok)
	require.EqualValues(t, rows, stats.InputRows)
	require.EqualValues(t, groupCount, stats.Groups)
	require.EqualValues(t, 1024*4*8, stats.StateCapacity)
	require.EqualValues(t, 4, stats.StateLanes)
	require.EqualValues(t, groupCount*4*8, stats.StateBytes)
	require.NoError(t, op.Close())
	require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
}

func TestAggregateCountPrecisionAtStateBoundary(t *testing.T) {
	source, defs := aggregateFixture(false)
	op, err := WrapAggregate(t.Context(), source, aggregateCountPlan(nil, defs[1]))
	require.NoError(t, err)
	require.NoError(t, op.Init(t.Context()))
	_, err = op.NextBatch(t.Context())
	require.NoError(t, err)
	// Exercise finalized state directly instead of ingesting quadrillions of rows.
	aggregate := op.(*aggregateOperator)
	aggregate.states[0].count[0] = (1 << 53) + 1
	aggregate.position = 0
	batch, err := op.NextBatch(t.Context())
	require.NoError(t, err)
	require.Equal(t, int64((1<<53)+1), batch.Columns[0].(*vectorized.TypedColumn[int64]).Data()[0])
	require.NoError(t, op.Close())
}

type aggregateErrorSource struct {
	*rawSource
	nextErr, closeErr error
}

func (s *aggregateErrorSource) NextBatch(ctx context.Context) (*vectorized.RecordBatch, error) {
	if s.nextErr != nil {
		return nil, s.nextErr
	}
	return s.rawSource.NextBatch(ctx)
}
func (s *aggregateErrorSource) Close() error { _ = s.rawSource.Close(); return s.closeErr }

func TestAggregateFailurePathsReturnReservations(t *testing.T) {
	for _, stage := range []string{"init", "pull", "close", "growth"} {
		t.Run(stage, func(t *testing.T) {
			source, defs := aggregateFixture(false)
			failure := fmt.Errorf("source %s failed", stage)
			wrapped := &aggregateErrorSource{rawSource: source}
			switch stage {
			case "init":
				source.initErr = failure
			case "pull":
				wrapped.nextErr = failure
			case "close":
				wrapped.closeErr = failure
			}
			budget := int64(1 << 20)
			plan := aggregateCallsPlan(defs)
			if stage == "growth" {
				plan.GroupBy = defs[:1]
				metadata, err := rawDescriptorBound(len(plan.GroupBy)+len(plan.Calls)+len(plan.Outputs), 0, 0)
				require.NoError(t, err)
				budget = metadata + 400
			}
			ctx := vectorized.WithQueryMemoryTracker(t.Context(), budget)
			op, err := WrapAggregate(ctx, wrapped, plan)
			require.NoError(t, err)
			err = op.Init(ctx)
			if err == nil {
				_, err = op.NextBatch(ctx)
			}
			if stage == "growth" {
				require.ErrorIs(t, err, vectorized.ErrMemoryBudgetExceeded)
			} else {
				require.ErrorIs(t, err, failure)
			}
			require.NoError(t, op.Close())
			require.Equal(t, 1, source.closes)
			require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
		})
	}
}

func TestAggregateSelectionAndTypedKeyBoundaries(t *testing.T) {
	defs := []vectorized.ColumnDef{
		{
			Name:      "a",
			TagFamily: "f",
			Role:      vectorized.RoleTag,
			Type:      vectorized.ColumnTypeString,
		},
		{
			Name:      "b",
			TagFamily: "f",
			Role:      vectorized.RoleTag,
			Type:      vectorized.ColumnTypeString,
		},
		aggregateField("v", vectorized.ColumnTypeInt64),
	}
	schema := vectorized.NewBatchSchema(defs)
	batch := vectorized.NewRecordBatch(schema, 3)
	batch.Columns[0].(*vectorized.TypedColumn[string]).AppendSlice([]string{"a", "ab", "a"})
	batch.Columns[1].(*vectorized.TypedColumn[string]).AppendSlice([]string{"bc", "c", "bc"})
	batch.Columns[2] = vectorized.NewConstInt64(1, 3)
	batch.Len = 3
	batch.Selection = []uint16{2, 1}
	source := &rawSource{schema: schema, batches: []*vectorized.RecordBatch{batch}}
	op, err := WrapAggregate(t.Context(), source, aggregateCountPlan(defs[:2], defs[2]))
	require.NoError(t, err)
	require.NoError(t, op.Init(t.Context()))
	out, err := op.NextBatch(t.Context())
	require.NoError(t, err)
	require.Equal(t, 2, out.Len)
	require.Equal(t, []string{"a", "ab"}, out.Columns[0].(*vectorized.TypedColumn[string]).Data())
	require.Equal(t, []int64{1, 1}, out.Columns[2].(*vectorized.TypedColumn[int64]).Data())
	require.NoError(t, op.Close())
}

func TestAggregateRetainsKeysBeforeNativeSourcePoison(t *testing.T) {
	for _, typ := range []vectorized.ColumnType{vectorized.ColumnTypeString, vectorized.ColumnTypeBytes} {
		t.Run(typ.String(), func(t *testing.T) {
			payload := []byte("first")
			defs := []vectorized.ColumnDef{
				{Name: "key", TagFamily: "f", Role: vectorized.RoleTag, Type: typ},
				aggregateField("v", vectorized.ColumnTypeInt64),
			}
			schema := vectorized.NewBatchSchema(defs)
			batch := vectorized.NewRecordBatch(schema, 1)
			if typ == vectorized.ColumnTypeBytes {
				batch.Columns[0].(*vectorized.TypedColumn[[]byte]).Append(payload)
			} else {
				// Native decoders lend strings over mutable buffers. The copied key must
				// survive the source overwriting the buffer at its next pull and Close.
				batch.Columns[0].(*vectorized.TypedColumn[string]).Append(convert.BytesToString(payload))
			}
			batch.Columns[1].(*vectorized.TypedColumn[int64]).Append(1)
			batch.Len = 1
			source := &rawSource{schema: schema, batches: []*vectorized.RecordBatch{batch}, poison: payload}
			op, err := WrapAggregate(t.Context(), source, aggregateCountPlan(defs[:1], defs[1]))
			require.NoError(t, err)
			require.NoError(t, op.Init(t.Context()))
			out, err := op.NextBatch(t.Context())
			require.NoError(t, err)
			require.Equal(t, "xxxxx", string(payload))
			if typ == vectorized.ColumnTypeBytes {
				require.Equal(t, []byte("first"), out.Columns[0].(*vectorized.TypedColumn[[]byte]).Data()[0])
			} else {
				require.Equal(t, "first", out.Columns[0].(*vectorized.TypedColumn[string]).Data()[0])
			}
			aggregate := op.(*aggregateOperator)
			require.Nil(t, aggregate.groupReaders[0].column)
			require.Nil(t, aggregate.stateReaders[0].column)
			require.NoError(t, op.Close())
		})
	}
}

func aggregateCountPlan(groups []vectorized.ColumnDef, source vectorized.ColumnDef) *AggregatePlan {
	outputs := make([]AggregateOutput, 0, len(groups)+1)
	for index, def := range groups {
		outputs = append(outputs, AggregateOutput{Column: def, Index: index, IsGroup: true})
	}
	outputs = append(outputs, AggregateOutput{Column: aggregateField("n", vectorized.ColumnTypeInt64)})
	return &AggregatePlan{GroupBy: groups, Calls: []AggregateCall{{Source: source, Function: AggregateCount}}, Outputs: outputs}
}
