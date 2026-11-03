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
	"testing"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

type filterBenchmarkSource struct {
	batch *vectorized.RecordBatch
	ready bool
}

func (*filterBenchmarkSource) Init(context.Context) error              { return nil }
func (s *filterBenchmarkSource) OutputSchema() *vectorized.BatchSchema { return s.batch.Schema }
func (*filterBenchmarkSource) Close() error                            { return nil }
func (s *filterBenchmarkSource) NextBatch(context.Context) (*vectorized.RecordBatch, error) {
	if !s.ready {
		return nil, nil
	}
	s.ready = false
	return s.batch, nil
}

func filterBenchmarkFixture(rows, threshold int, selected bool) (*vectorized.RecordBatch, []uint16, int) {
	def := vectorized.ColumnDef{Name: "v", Role: vectorized.RoleField, Type: vectorized.ColumnTypeInt64}
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{def})
	batch := vectorized.NewRecordBatch(schema, rows)
	column := batch.Columns[0].(*vectorized.TypedColumn[int64])
	expected := make([]uint16, 0, rows)
	active := 0
	for row := range rows {
		value := int64(row % 100)
		null := false
		if selected {
			value = int64((row / 2) % 100)
			null = row%2 == 0 && (row/2)%5 == 4
		}
		column.Append(value)
		if null {
			column.MarkNullAt(row)
		}
		if selected && row%2 != 0 {
			continue
		}
		if selected {
			batch.Selection = append(batch.Selection, uint16(row))
		}
		active++
		if !null && value < int64(threshold) {
			expected = append(expected, uint16(row))
		}
	}
	batch.Len = rows
	return batch, expected, active
}

// BenchmarkFilterT12 times the complete public one-batch filter lifecycle,
// including expression compilation, Init, draining through EOF, and Close.
// NULL rows in selected_nulls are UNKNOWN, so actual pass counts are reported
// alongside the predicate's threshold percentage; tiny batches may also round.
func BenchmarkFilterT12(b *testing.B) {
	for _, rows := range []int{1, 1024, 65536} {
		for _, threshold := range []int{0, 1, 50, 100} {
			for _, selected := range []bool{false, true} {
				mode := "dense"
				if selected {
					mode = "selected_nulls"
				}
				b.Run(fmt.Sprintf("rows%d/threshold%d/%s", rows, threshold, mode), func(b *testing.B) {
					benchmarkFilterT12(b, rows, threshold, selected)
				})
			}
		}
	}
}

func benchmarkFilterT12(b *testing.B, rows, threshold int, selected bool) {
	batch, expected, active := filterBenchmarkFixture(rows, threshold, selected)
	ctx := vectorized.WithQueryMemoryTracker(b.Context(), 16<<20)
	tracker := vectorized.QueryMemoryTracker(ctx, 0)
	source := &filterBenchmarkSource{batch: batch, ready: true}
	predicate := &Expression{Op: "<", Args: []*Expression{expressionValueColumn(), expressionInt(int64(threshold))}}
	warm, err := WrapFilter(ctx, source, predicate)
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
			b.Fatal("zero-match filter produced a batch")
		}
	} else {
		if output == nil || output.Len != rows || output.ActiveLen() != len(expected) || output.Columns[0] != batch.Columns[0] {
			b.Fatal("filter changed physical values or result size")
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
			b.Fatalf("filter did not finish: %v", err)
		}
	}
	if err = warm.Close(); err != nil || tracker.Used() != 0 {
		b.Fatalf("warm filter retained memory: %v, %d", err, tracker.Used())
	}
	b.ReportAllocs()
	var final vectorized.PullOperator
	for b.Loop() {
		source.ready = true
		operator, wrapErr := WrapFilter(ctx, source, predicate)
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
	stats, ok := FilterStatistics(final)
	if !ok || stats.LiveBytes != 0 || stats.ReservedBytes != stats.ReleasedBytes || tracker.Used() != 0 || stats.CopiedBytes != 0 {
		b.Fatalf("filter lifecycle leaked or copied values: %+v", stats)
	}
	b.ReportMetric(float64(active), "examined_rows/op")
	b.ReportMetric(float64(b.N)*float64(active)/b.Elapsed().Seconds(), "filtered_rows/s")
	b.ReportMetric(float64(b.N)*float64(len(expected))/b.Elapsed().Seconds(), "passed_rows/s")
	b.ReportMetric(float64(len(expected)), "passed_rows/op")
	b.ReportMetric(float64(stats.UnknownRows), "unknown_rows/op")
	b.ReportMetric(float64(stats.SelectionBytes), "selection_bytes/op")
	b.ReportMetric(float64(stats.ReservedBytes), "reserved_bytes/op")
	b.ReportMetric(float64(stats.CopiedBytes), "copied_bytes/op")
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(active), "ns/examined-row")
}
