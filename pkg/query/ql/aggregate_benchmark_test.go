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
	"runtime"
	"testing"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

const aggregateKernelRows = 65537

type aggregateKernelInput struct {
	key   int64
	value float64
	null  bool
}

type aggregateKernelCase struct {
	name      string
	functions []AggregateFunction
}

var aggregateKernelSink float64

// BenchmarkAggregateKernel compares the real batch operator with an independent
// scalar map over the same nullable rows, including the uint16 batch boundary.
func BenchmarkAggregateKernel(b *testing.B) {
	for _, groups := range []int{1, 1024, aggregateKernelRows} {
		inputs, batches, defs := aggregateKernelFixture(aggregateKernelRows, groups)
		cases := []aggregateKernelCase{
			{name: "sum", functions: []AggregateFunction{AggregateSum}},
			{name: "multiple", functions: []AggregateFunction{AggregateCount, AggregateSum, AggregateMin, AggregateMax}},
			{name: "avg", functions: []AggregateFunction{AggregateAvg}},
			{name: "sum-count-avg", functions: []AggregateFunction{AggregateSum, AggregateCount, AggregateAvg}},
		}
		for _, testCase := range cases {
			benchmarkAggregateKernelCase(b, inputs, batches, defs, testCase, groups)
		}
	}
}

// BenchmarkAggregateMillionGroups verifies the local operator at one million
// typed rows and groups. It does not exercise native RPC framing or admission.
func BenchmarkAggregateMillionGroups(b *testing.B) {
	const rows = 1_000_000
	inputs, batches, defs := aggregateKernelFixture(rows, rows)
	for _, testCase := range []aggregateKernelCase{
		{name: "avg", functions: []AggregateFunction{AggregateAvg}},
		{name: "sum-count-avg", functions: []AggregateFunction{AggregateSum, AggregateCount, AggregateAvg}},
	} {
		benchmarkAggregateKernelCase(b, inputs, batches, defs, testCase, rows)
	}
}

func benchmarkAggregateKernelCase(b *testing.B, inputs []aggregateKernelInput, batches []*vectorized.RecordBatch,
	defs []vectorized.ColumnDef, testCase aggregateKernelCase, groups int,
) {
	const budget = 128 << 20
	plan := aggregateKernelPlan(testCase, defs)
	b.Run(fmt.Sprintf("groups=%d/%s/dense", groups, testCase.name), func(b *testing.B) {
		stats := validateAggregateKernel(b, inputs, batches, plan, testCase.functions, budget)
		// Exclude reclamation of the independent validation maps from measured execution.
		runtime.GC()
		b.ReportAllocs()
		b.SetBytes(int64(len(inputs)) * 16)
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			source := &rawSource{schema: batches[0].Schema, batches: append([]*vectorized.RecordBatch(nil), batches...)}
			ctx := vectorized.WithQueryMemoryTracker(context.Background(), budget)
			op, err := WrapAggregate(ctx, source, plan)
			if err != nil {
				b.Fatal(err)
			}
			if err = op.Init(ctx); err != nil {
				b.Fatal(err)
			}
			for {
				batch, pullErr := op.NextBatch(ctx)
				if pullErr != nil {
					b.Fatal(pullErr)
				}
				if batch == nil {
					break
				}
				aggregateKernelSink += float64(batch.Len)
			}
			if err = op.Close(); err != nil {
				b.Fatal(err)
			}
		}
		b.ReportMetric(float64(stats.InputRows), "input-rows/op")
		b.ReportMetric(float64(stats.Groups), "groups/op")
		b.ReportMetric(float64(stats.StateBytes), "state-B/op")
		b.ReportMetric(float64(stats.StateCapacity), "state-cap-B/op")
		b.ReportMetric(float64(stats.KeyCapacity), "key-B/op")
		b.ReportMetric(float64(stats.StateLanes), "lanes/op")
		b.ReportMetric(budget, "budget-B")
	})
	b.Run(fmt.Sprintf("groups=%d/%s/scalar", groups, testCase.name), func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(inputs)) * 16)
		for iteration := 0; iteration < b.N; iteration++ {
			result := aggregateKernelReference(inputs, testCase.functions)
			aggregateKernelSink += float64(len(result))
		}
	})
}

func validateAggregateKernel(b *testing.B, inputs []aggregateKernelInput, batches []*vectorized.RecordBatch,
	plan *AggregatePlan, functions []AggregateFunction, budget int64,
) AggregateStats {
	b.Helper()
	expected := aggregateKernelReference(inputs, functions)
	expectedGroups := uint64(len(expected))
	source := &rawSource{schema: batches[0].Schema, batches: append([]*vectorized.RecordBatch(nil), batches...)}
	ctx := vectorized.WithQueryMemoryTracker(context.Background(), budget)
	op, err := WrapAggregate(ctx, source, plan)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		if closeErr := op.Close(); closeErr != nil {
			b.Fatal(closeErr)
		}
	}()
	if err = op.Init(ctx); err != nil {
		b.Fatal(err)
	}
	for {
		batch, pullErr := op.NextBatch(ctx)
		if pullErr != nil {
			b.Fatal(pullErr)
		}
		if batch == nil {
			break
		}
		keys := batch.Columns[0].(*vectorized.TypedColumn[int64]).Data()
		for row, key := range keys {
			want, exists := expected[key]
			if !exists {
				b.Fatal("aggregate emitted an unexpected or repeated group")
			}
			for index, value := range want {
				column := batch.Columns[index+1]
				if column.IsNull(row) != value.null {
					b.Fatal("aggregate null differs from scalar reference")
				}
				if value.null {
					continue
				}
				if functions[index] == AggregateCount {
					if float64(column.(*vectorized.TypedColumn[int64]).Data()[row]) != value.value {
						b.Fatal("count differs from scalar reference")
					}
				} else if column.(*vectorized.TypedColumn[float64]).Data()[row] != value.value {
					b.Fatal("aggregate differs from scalar reference")
				}
			}
			delete(expected, key)
		}
	}
	if len(expected) != 0 {
		b.Fatal("aggregate omitted expected groups")
	}
	stats, ok := AggregateStatistics(op)
	if !ok || stats.InputRows != uint64(len(inputs)) {
		b.Fatal("aggregate did not consume the complete typed input")
	}
	if stats.Groups != expectedGroups {
		b.Fatal("aggregate group-state cardinality differs from the independent reference")
	}
	if err = op.Close(); err != nil {
		b.Fatal(err)
	}
	if vectorized.QueryMemoryTracker(ctx, 0).Used() != 0 {
		b.Fatal("aggregate retained memory after Close")
	}
	return stats
}

func aggregateKernelFixture(rows, groups int) ([]aggregateKernelInput, []*vectorized.RecordBatch, []vectorized.ColumnDef) {
	defs := []vectorized.ColumnDef{aggregateField("key", vectorized.ColumnTypeInt64), aggregateField("f", vectorized.ColumnTypeFloat64)}
	schema := vectorized.NewBatchSchema(defs)
	inputs := make([]aggregateKernelInput, rows)
	var batches []*vectorized.RecordBatch
	for begin := 0; begin < rows; begin += 65536 {
		end := min(begin+65536, rows)
		batch := vectorized.NewRecordBatch(schema, end-begin)
		for row := begin; row < end; row++ {
			input := aggregateKernelInput{key: int64(row % groups), value: float64(row%23-11) / 4, null: row%5 == 0}
			inputs[row] = input
			batch.Columns[0].(*vectorized.TypedColumn[int64]).Append(input.key)
			if input.null {
				batch.Columns[1].AppendNull()
			} else {
				batch.Columns[1].(*vectorized.TypedColumn[float64]).Append(input.value)
			}
		}
		batch.Len = end - begin
		batches = append(batches, batch)
	}
	return inputs, batches, defs
}

func aggregateKernelPlan(testCase aggregateKernelCase, defs []vectorized.ColumnDef) *AggregatePlan {
	plan := &AggregatePlan{GroupBy: defs[:1], Outputs: []AggregateOutput{{Column: defs[0], IsGroup: true}}}
	for index, function := range testCase.functions {
		call := AggregateCall{Source: defs[1], Function: function}
		plan.Calls = append(plan.Calls, call)
		plan.Outputs = append(plan.Outputs, AggregateOutput{Column: aggregateField(fmt.Sprintf("output_%d", index), aggregateOutputType(call)), Index: index})
	}
	return plan
}

// The oracle deliberately uses ordinary Go values and a map keyed by the input
// integer, without the production key codec, state arrays or finalization helper.
func aggregateKernelReference(inputs []aggregateKernelInput, functions []AggregateFunction) map[int64][]aggregateKernelExpected {
	type state struct {
		count         int64
		sum, min, max float64
	}
	states := make(map[int64]state)
	var needsSum, needsMin, needsMax bool
	for _, function := range functions {
		needsSum = needsSum || function == AggregateSum || function == AggregateAvg
		needsMin = needsMin || function == AggregateMin
		needsMax = needsMax || function == AggregateMax
	}
	for _, input := range inputs {
		current := states[input.key]
		if !input.null {
			if needsMin && (current.count == 0 || input.value < current.min) {
				current.min = input.value
			}
			if needsMax && (current.count == 0 || input.value > current.max) {
				current.max = input.value
			}
			current.count++
			if needsSum {
				current.sum += input.value
			}
		}
		states[input.key] = current
	}
	results := make(map[int64][]aggregateKernelExpected, len(states))
	for key, current := range states {
		values := make([]aggregateKernelExpected, len(functions))
		for index, function := range functions {
			values[index].null = current.count == 0 && function != AggregateCount
			switch function {
			case AggregateCount:
				values[index].value = float64(current.count)
			case AggregateSum:
				values[index].value = current.sum
			case AggregateMin:
				values[index].value = current.min
			case AggregateMax:
				values[index].value = current.max
			case AggregateAvg:
				if current.count != 0 {
					values[index].value = current.sum / float64(current.count)
				}
			}
		}
		results[key] = values
	}
	return results
}

type aggregateKernelExpected struct {
	value float64
	null  bool
}
