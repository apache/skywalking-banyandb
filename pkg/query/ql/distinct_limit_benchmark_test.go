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
	"strings"
	"testing"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// BenchmarkDistinctPaginationKernel compares complete consumption with early
// termination over the same predetermined ascending input. Ordered is only a
// physical control here; it does not add support for SQL ORDER BY expressions.
func BenchmarkDistinctPaginationKernel(b *testing.B) {
	schema, batch, slots, expected := distinctPaginationKernelFixture()
	for _, control := range []struct {
		name      string
		ordered   bool
		inputRows uint64
	}{
		{name: "full_input", ordered: true, inputRows: 1000},
		{name: "early_stop", inputRows: 15},
	} {
		b.Run(control.name, func(b *testing.B) {
			limit := uint32(10)
			plan := &RawPlan{Distinct: true, Ordered: control.ordered, Slots: slots, Limit: &limit, Offset: 5}
			stats, err := distinctPaginationKernelRun(b.Context(), schema, batch, plan, control.inputRows, expected)
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				if _, err = distinctPaginationKernelRun(b.Context(), schema, batch, plan, control.inputRows, nil); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(stats.InputRows), "input-rows/op")
			b.ReportMetric(float64(stats.Groups), "tuples/op")
			b.ReportMetric(float64(stats.KeyBytes), "key-bytes/op")
			b.ReportMetric(float64(stats.KeyCapacity), "key-capacity/op")
		})
	}
}

func distinctPaginationKernelFixture() (*vectorized.BatchSchema, *vectorized.RecordBatch, []Slot, map[[2]string]struct{}) {
	defs := []vectorized.ColumnDef{
		{Name: "a", Role: vectorized.RoleField, Type: vectorized.ColumnTypeString},
		{Name: "b", Role: vectorized.RoleField, Type: vectorized.ColumnTypeString},
	}
	schema := vectorized.NewBatchSchema(defs)
	batch := vectorized.NewRecordBatch(schema, 1000)
	leftPadding, rightPadding := strings.Repeat("a", 256), strings.Repeat("b", 256)
	for row := 0; row < 1000; row++ {
		id := fmt.Sprintf("%06d", row)
		batch.Columns[0].(*vectorized.TypedColumn[string]).Append(id + leftPadding)
		batch.Columns[1].(*vectorized.TypedColumn[string]).Append(id + rightPadding)
	}
	batch.Len = 1000
	// The oracle names the ten visible tuples independently of the QL key codec
	// and pagination implementation.
	expected := make(map[[2]string]struct{}, 10)
	for _, id := range []string{"000005", "000006", "000007", "000008", "000009", "000010", "000011", "000012", "000013", "000014"} {
		expected[[2]string{id + leftPadding, id + rightPadding}] = struct{}{}
	}
	slots := []Slot{{Source: defs[0], Output: defs[0]}, {Source: defs[1], Output: defs[1]}}
	return schema, batch, slots, expected
}

func distinctPaginationKernelRun(ctx context.Context, schema *vectorized.BatchSchema, batch *vectorized.RecordBatch,
	plan *RawPlan, inputRows uint64, expected map[[2]string]struct{},
) (AggregateStats, error) {
	ctx = vectorized.WithQueryMemoryTracker(ctx, 16<<20)
	source := &rawSource{schema: schema, batches: []*vectorized.RecordBatch{batch}}
	operator, err := WrapRaw(ctx, source, plan)
	if err != nil {
		return AggregateStats{}, err
	}
	defer func() { _ = operator.Close() }()
	if err = operator.Init(ctx); err != nil {
		return AggregateStats{}, err
	}
	var actual map[[2]string]struct{}
	if expected != nil {
		actual = make(map[[2]string]struct{}, 10)
	}
	outputRows := 0
	for {
		output, nextErr := operator.NextBatch(ctx)
		if nextErr != nil {
			return AggregateStats{}, nextErr
		}
		if output == nil {
			break
		}
		outputRows += output.ActiveLen()
		if expected != nil {
			if err = validateDistinctPaginationKernelOutput(output, expected, actual); err != nil {
				return AggregateStats{}, err
			}
		}
	}
	if outputRows != 10 || expected != nil && len(actual) != 10 {
		return AggregateStats{}, fmt.Errorf("pagination returned %d rows and %d reference tuples; want 10", outputRows, len(actual))
	}
	stats, ok := DistinctStatistics(operator)
	if !ok || stats.InputRows != inputRows || stats.Groups != inputRows || stats.StateLanes != 0 || stats.StateBytes != 0 || stats.StateCapacity != 0 {
		return stats, fmt.Errorf("invalid DISTINCT pagination state: %+v; want %d examined rows and groups, zero state", stats, inputRows)
	}
	if err = operator.Close(); err != nil {
		return stats, err
	}
	if source.closes != 1 || vectorized.QueryMemoryTracker(ctx, 0).Used() != 0 {
		return stats, fmt.Errorf("DISTINCT pagination did not close source and release all reservations")
	}
	return stats, nil
}

func validateDistinctPaginationKernelOutput(batch *vectorized.RecordBatch, expected, actual map[[2]string]struct{}) error {
	left := batch.Columns[0].(*vectorized.TypedColumn[string]).Data()
	right := batch.Columns[1].(*vectorized.TypedColumn[string]).Data()
	for position := 0; position < batch.ActiveLen(); position++ {
		row := position
		if batch.Selection != nil {
			row = int(batch.Selection[position])
		}
		tuple := [2]string{left[row], right[row]}
		if _, ok := expected[tuple]; !ok {
			return fmt.Errorf("DISTINCT pagination returned unexpected tuple")
		}
		if _, ok := actual[tuple]; ok {
			return fmt.Errorf("DISTINCT pagination returned repeated tuple")
		}
		actual[tuple] = struct{}{}
	}
	return nil
}
