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
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package ql

import (
	"fmt"
	"testing"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

var rawBenchmarkResult *vectorized.RecordBatch

// BenchmarkRawProjection compares borrowed projection descriptors with an
// independent scalar value-copy reference over the same immutable input.
func BenchmarkRawProjection(b *testing.B) {
	const rows = 1024
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Name: "time", Role: vectorized.RoleTimestamp, Type: vectorized.ColumnTypeInt64},
		{Name: "v", Role: vectorized.RoleField, Type: vectorized.ColumnTypeInt64},
	})
	batch := vectorized.NewRecordBatch(schema, rows)
	for row := range rows {
		batch.Columns[0].(*vectorized.TypedColumn[int64]).Append(int64(row) * 1000000)
		batch.Columns[1].(*vectorized.TypedColumn[int64]).Append(int64(row))
	}
	batch.Len = rows
	for _, width := range []int{1, 32} {
		slots := make([]Slot, width)
		for index := range slots {
			output := schema.Columns[1]
			output.Name = fmt.Sprintf("value_%d", index)
			slots[index] = Slot{ID: SlotID(index), Source: schema.Columns[1], Output: output}
		}
		for _, residual := range []bool{false, true} {
			b.Run(fmt.Sprintf("batch/columns_%d/residual_%v", width, residual), func(b *testing.B) {
				ctx := vectorized.WithQueryMemoryTracker(b.Context(), 256<<20)
				b.ReportAllocs()
				for b.Loop() {
					source := &scanSource{schema: schema, batch: batch}
					op, err := WrapRaw(ctx, source, &RawPlan{Slots: slots, Residual: residual, Ranges: []MillisRange{{Begin: 0, End: 256}, {Begin: 768, End: 1024}}})
					if err != nil {
						b.Fatal(err)
					}
					if err = op.Init(ctx); err != nil {
						b.Fatal(err)
					}
					rawBenchmarkResult, err = op.NextBatch(ctx)
					if err != nil {
						b.Fatal(err)
					}
					if err = op.Close(); err != nil {
						b.Fatal(err)
					}
				}
				b.ReportMetric(0, "value-B/op")
			})
		}
		b.Run(fmt.Sprintf("scalar_copy/columns_%d", width), func(b *testing.B) {
			defs := make([]vectorized.ColumnDef, width)
			for index := range slots {
				defs[index] = slots[index].Output
			}
			outputSchema := vectorized.NewBatchSchema(defs)
			values := batch.Columns[1].(*vectorized.TypedColumn[int64]).Data()
			b.ReportAllocs()
			for b.Loop() {
				output := vectorized.NewRecordBatch(outputSchema, rows)
				for row := range rows {
					for _, column := range output.Columns {
						column.(*vectorized.TypedColumn[int64]).Append(values[row])
					}
				}
				output.Len = rows
				rawBenchmarkResult = output
			}
			b.ReportMetric(float64(rows*width*8), "value-B/op")
		})
	}
}
