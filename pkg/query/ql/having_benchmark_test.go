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
	"fmt"
	"testing"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// BenchmarkHavingT14 measures only a completed group batch and final borrowed
// projection. The baseline has the same columns and projection without HAVING;
// no scan, aggregation or protobuf result encoding is included in this kernel.
func BenchmarkHavingT14(b *testing.B) {
	for _, rows := range []int{1, 1024, 65536} {
		for _, threshold := range []int{-1, 0, 1, 50, 100} {
			mode := "baseline"
			if threshold >= 0 {
				mode = fmt.Sprintf("threshold%d", threshold)
			}
			b.Run(fmt.Sprintf("groups%d/%s", rows, mode), func(b *testing.B) {
				effective := threshold
				if threshold < 0 {
					effective = 100
				}
				batch, expected, _ := filterBenchmarkFixture(rows, effective, false)
				value := batch.Schema.Columns[0]
				plan := &RawPlan{Slots: []Slot{{Source: value, Output: value}}}
				if threshold >= 0 {
					plan.Having = &HavingPlan{Predicate: &Expression{Op: "<", Args: []*Expression{
						{Op: "column", Source: value}, expressionInt(int64(threshold)),
					}}, Outputs: plan.Slots}
				}
				benchmarkHavingT14(b, batch, plan, expected)
			})
		}
	}
	// Collection input length is a separate, bounded kernel probe. Half the
	// rows contain zero first; the other half scan the entire nonmatching array.
	for _, length := range []int{0, 1, 16, 256} {
		b.Run(fmt.Sprintf("membership/rows1024/array%d", length), func(b *testing.B) {
			labels := vectorized.ColumnDef{Name: "labels", TagFamily: "default", Role: vectorized.RoleTag, Type: vectorized.ColumnTypeInt64Array}
			schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{labels})
			batch := vectorized.NewRecordBatch(schema, 1024)
			column := batch.Columns[0].(*vectorized.TypedColumn[[]int64])
			expected := []uint16{}
			for row := range 1024 {
				values := make([]int64, length)
				for index := range values {
					values[index] = int64(index + row%2)
				}
				column.Append(values)
				if length > 0 && row%2 == 0 {
					expected = append(expected, uint16(row))
				}
			}
			batch.Len = 1024
			plan := &RawPlan{
				Slots: []Slot{{Source: labels, Output: labels}},
				Having: &HavingPlan{Predicate: &Expression{Op: "contains", Args: []*Expression{
					{Op: "column", Source: labels}, expressionInt(0),
				}}, Outputs: []Slot{{Source: labels, Output: labels}}},
			}
			benchmarkHavingT14(b, batch, plan, expected)
			b.ReportMetric(float64(1024*length), "input_array_elements/op")
		})
	}
}

func benchmarkHavingT14(b *testing.B, batch *vectorized.RecordBatch, plan *RawPlan, expected []uint16) {
	ctx := vectorized.WithQueryMemoryTracker(b.Context(), 16<<20)
	tracker := vectorized.QueryMemoryTracker(ctx, 0)
	source := &filterBenchmarkSource{batch: batch, ready: true}
	// Expected row identities come from the fixture's arithmetic before the
	// timed loop, and borrowed column identity independently checks zero-copy.
	warm, err := WrapRaw(ctx, source, plan)
	if err != nil {
		b.Fatal(err)
	}
	if err = warm.Init(ctx); err != nil {
		b.Fatal(err)
	}
	output, err := warm.NextBatch(ctx)
	if err != nil {
		b.Fatal(err)
	}
	if len(expected) == 0 {
		if output != nil {
			b.Fatal("zero-match HAVING produced a batch")
		}
	} else {
		if output == nil || output.ActiveLen() != len(expected) || output.Columns[0] != batch.Columns[0] {
			b.Fatal("HAVING changed values or row count")
		}
		for pos, row := range expected {
			actual := pos
			if output.Selection != nil {
				actual = int(output.Selection[pos])
			}
			if actual != int(row) {
				b.Fatalf("row %d: got physical row %d, want %d", pos, actual, row)
			}
		}
		output, err = warm.NextBatch(ctx)
		if err != nil || output != nil {
			b.Fatalf("HAVING did not finish: %v", err)
		}
	}
	if err = warm.Close(); err != nil || tracker.Used() != 0 {
		b.Fatalf("warm HAVING retained memory: %v, %d", err, tracker.Used())
	}
	b.ReportAllocs()
	var final vectorized.PullOperator
	for b.Loop() {
		source.ready = true
		operator, wrapErr := WrapRaw(ctx, source, plan)
		if wrapErr != nil {
			b.Fatal(wrapErr)
		}
		if initErr := operator.Init(ctx); initErr != nil {
			b.Fatal(initErr)
		}
		passed := 0
		for {
			result, pullErr := operator.NextBatch(ctx)
			if pullErr != nil {
				b.Fatal(pullErr)
			}
			if result == nil {
				break
			}
			passed += result.ActiveLen()
		}
		if passed != len(expected) {
			b.Fatalf("got %d rows, want %d", passed, len(expected))
		}
		if closeErr := operator.Close(); closeErr != nil {
			b.Fatal(closeErr)
		}
		final = operator
	}
	var finalStats FilterStats
	if plan.Having != nil {
		var ok bool
		finalStats, ok = HavingStatistics(final)
		if !ok {
			b.Fatal("missing HAVING statistics")
		}
	}
	if tracker.Used() != 0 || finalStats.LiveBytes != 0 || finalStats.ReservedBytes != finalStats.ReleasedBytes || finalStats.CopiedBytes != 0 {
		b.Fatalf("HAVING lifecycle leaked: %+v, tracker=%d", finalStats, tracker.Used())
	}
	b.ReportMetric(float64(batch.ActiveLen()), "completed_groups/op")
	b.ReportMetric(float64(finalStats.InputRows), "having_input_groups/op")
	b.ReportMetric(float64(len(expected)), "passed_groups/op")
	b.ReportMetric(float64(finalStats.SelectionBytes), "selection_bytes/op")
	b.ReportMetric(float64(finalStats.ReservedBytes), "filter_reserved_bytes/op")
	b.ReportMetric(float64(finalStats.CopiedBytes), "copied_bytes/op")
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(batch.ActiveLen()), "ns/completed-group")
}
