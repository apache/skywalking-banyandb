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

type nativeMergeKernelKey struct{ high, low int64 }

type nativeMergeKernelCell struct {
	key   nativeMergeKernelKey
	value float64
}

type nativeMergeKernelResult struct {
	count int64
	sum   float64
}

type nativeMergeKernelFixture struct {
	plan    *NativeAggregatePlan
	sums    []nativeMergeKernelCell
	counts  []nativeMergeKernelCell
	batches [2][]*vectorized.RecordBatch
}

var nativeMergeKernelSink float64

// BenchmarkNativeAggregateMerge compares the public keyed column merge with an
// independent scalar reference. Only group count and native branch order vary.
func BenchmarkNativeAggregateMerge(b *testing.B) {
	for _, groups := range []int{1, 1024, 65537} {
		for _, permuted := range []bool{false, true} {
			fixture := makeNativeMergeKernelFixture(groups, permuted)
			name := fmt.Sprintf("groups=%d/permuted=%t", groups, permuted)
			b.Run(name+"/column", func(b *testing.B) {
				stats := validateNativeMergeKernel(b, fixture)
				runtime.GC()
				b.ReportAllocs()
				b.ResetTimer()
				for iteration := 0; iteration < b.N; iteration++ {
					_, total := runNativeMergeKernel(b, fixture, nil)
					nativeMergeKernelSink += total
				}
				b.ReportMetric(float64(stats.Branches), "branches/op")
				b.ReportMetric(float64(stats.InputRows), "input-groups/op")
				b.ReportMetric(float64(stats.Groups), "groups/op")
				b.ReportMetric(float64(stats.StateBytes), "state-B/op")
				b.ReportMetric(float64(stats.StateCapacity), "state-cap-B/op")
				b.ReportMetric(float64(stats.KeyCapacity), "key-cap-B/op")
				b.ReportMetric(float64(stats.MergeVisits), "merge-visits/op")
			})
			b.Run(name+"/scalar", func(b *testing.B) {
				b.ReportAllocs()
				for iteration := 0; iteration < b.N; iteration++ {
					result := nativeMergeKernelReference(fixture)
					var total float64
					for _, value := range result {
						total += float64(value.count) + value.sum
						if value.count != 0 {
							total += value.sum / float64(value.count)
						}
					}
					nativeMergeKernelSink += total
				}
			})
		}
	}
}

func makeNativeMergeKernelFixture(groups int, permuted bool) nativeMergeKernelFixture {
	defs := []vectorized.ColumnDef{aggregateField("high", vectorized.ColumnTypeInt64), aggregateField("low", vectorized.ColumnTypeInt64)}
	value := aggregateField("v", vectorized.ColumnTypeFloat64)
	fixture := nativeMergeKernelFixture{plan: nativeMergePlan(defs, value, AggregateSum, AggregateCount, AggregateAvg)}
	fixture.plan.MaxCount = 3
	fixture.plan.Leaves = []NativeAggregateLeaf{{Source: value, Function: AggregateSum}, {Source: value, Function: AggregateCount}}
	if permuted {
		fixture.plan.Leaves[0], fixture.plan.Leaves[1] = fixture.plan.Leaves[1], fixture.plan.Leaves[0]
	}
	for row := range groups {
		group := row
		if permuted {
			group = groups - row - 1
		}
		key := nativeMergeKernelKey{high: int64(group / 16), low: int64(group % 16)}
		sum := float64(group%23+1) / 4
		if group != 0 && group%11 == 0 {
			sum = 0
		}
		fixture.sums = append(fixture.sums, nativeMergeKernelCell{key: key, value: sum})
		fixture.counts = append(fixture.counts, nativeMergeKernelCell{
			key: nativeMergeKernelKey{high: int64(row / 16), low: int64(row % 16)}, value: float64(1 + row%3),
		})
		if row != 0 && row%11 == 0 {
			fixture.counts[row].value = 0
		}
	}
	schema := vectorized.NewBatchSchema(append(defs, value))
	for branch, cells := range [][]nativeMergeKernelCell{fixture.sums, fixture.counts} {
		for begin := 0; begin < groups; begin += 65536 {
			end := min(begin+65536, groups)
			batch := vectorized.NewRecordBatch(schema, end-begin)
			for _, cell := range cells[begin:end] {
				batch.Columns[0].(*vectorized.TypedColumn[int64]).Append(cell.key.high)
				batch.Columns[1].(*vectorized.TypedColumn[int64]).Append(cell.key.low)
				batch.Columns[2].(*vectorized.TypedColumn[float64]).Append(cell.value)
			}
			batch.Len = end - begin
			fixture.batches[branch] = append(fixture.batches[branch], batch)
		}
	}
	return fixture
}

func nativeMergeKernelReference(fixture nativeMergeKernelFixture) map[nativeMergeKernelKey]nativeMergeKernelResult {
	result := make(map[nativeMergeKernelKey]nativeMergeKernelResult)
	for _, cell := range fixture.sums {
		result[cell.key] = nativeMergeKernelResult{sum: cell.value}
	}
	for _, cell := range fixture.counts {
		value := result[cell.key]
		value.count = int64(cell.value)
		result[cell.key] = value
	}
	return result
}

func validateNativeMergeKernel(tb testing.TB, fixture nativeMergeKernelFixture) NativeAggregateStats {
	tb.Helper()
	expected := nativeMergeKernelReference(fixture)
	stats, _ := runNativeMergeKernel(tb, fixture, expected)
	if len(expected) != 0 {
		tb.Fatal("native merge omitted expected groups")
	}
	if stats.Groups != uint64(len(fixture.sums)) || stats.Branches != 2 || stats.InputRows != stats.Groups*2 || stats.MergeVisits != stats.InputRows+stats.Groups {
		tb.Fatalf("unexpected native merge structure: %+v", stats)
	}
	return stats
}

func runNativeMergeKernel(tb testing.TB, fixture nativeMergeKernelFixture,
	expected map[nativeMergeKernelKey]nativeMergeKernelResult,
) (NativeAggregateStats, float64) {
	tb.Helper()
	ctx := vectorized.WithQueryMemoryTracker(context.Background(), 128<<20)
	op, err := MergeNativeAggregate(ctx, fixture.plan, func(_ context.Context, leaf NativeAggregateLeaf) (vectorized.PullOperator, error) {
		branch := 0
		if leaf.Function == AggregateCount {
			branch = 1
		}
		batches := fixture.batches[branch]
		return &rawSource{schema: batches[0].Schema, batches: append([]*vectorized.RecordBatch(nil), batches...)}, nil
	})
	if err != nil {
		tb.Fatal(err)
	}
	if err = op.Init(ctx); err != nil {
		tb.Fatal(err)
	}
	var total float64
	for {
		batch, pullErr := op.NextBatch(ctx)
		if pullErr != nil {
			tb.Fatal(pullErr)
		}
		if batch == nil {
			break
		}
		for row := range batch.Len {
			key := nativeMergeKernelKey{
				high: batch.Columns[0].(*vectorized.TypedColumn[int64]).Data()[row], low: batch.Columns[1].(*vectorized.TypedColumn[int64]).Data()[row],
			}
			count := batch.Columns[3].(*vectorized.TypedColumn[int64]).Data()[row]
			sum := batch.Columns[2].(*vectorized.TypedColumn[float64]).Data()[row]
			avg := batch.Columns[4].(*vectorized.TypedColumn[float64]).Data()[row]
			total += float64(count) + sum + avg
			if expected != nil {
				want, exists := expected[key]
				if !exists || count != want.count || (count != 0 && (sum != want.sum || avg != want.sum/float64(count))) ||
					batch.Columns[2].IsNull(row) != (count == 0) || batch.Columns[4].IsNull(row) != (count == 0) {
					tb.Fatalf("native merge differs from independent reference for %+v", key)
				}
				delete(expected, key)
			}
		}
	}
	stats, _ := NativeAggregateStatistics(op)
	if err = op.Close(); err != nil {
		tb.Fatal(err)
	}
	if vectorized.QueryMemoryTracker(ctx, 0).Used() != 0 {
		tb.Fatal("native merge retained its query budget after Close")
	}
	return stats, total
}
