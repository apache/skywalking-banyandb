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

// BenchmarkDistinctTupleKernel exercises the public deduplication boundary. The
// reference uses Go tuple equality, independently of the encoded QL key codec.
func BenchmarkDistinctTupleKernel(b *testing.B) {
	for _, width := range []int{8, 256} {
		for _, percent := range []int{0, 1, 50, 100} {
			name := fmt.Sprintf("width%d/unique%d", width, percent)
			b.Run(name, func(b *testing.B) {
				const rows = 1000
				count := rows
				unique := rows * percent / 100
				if percent == 0 {
					count = 0
				}
				defs := []vectorized.ColumnDef{
					{Name: "a", Role: vectorized.RoleField, Type: vectorized.ColumnTypeString},
					{Name: "b", Role: vectorized.RoleField, Type: vectorized.ColumnTypeString},
				}
				schema := vectorized.NewBatchSchema(defs)
				batch := vectorized.NewRecordBatch(schema, count)
				expected := make(map[[2]string]struct{})
				for row := 0; row < count; row++ {
					id := row % unique
					tuple := [2]string{fmt.Sprintf("%06d", id) + strings.Repeat("a", width), fmt.Sprintf("%06d", id) + strings.Repeat("b", width)}
					batch.Columns[0].(*vectorized.TypedColumn[string]).Append(tuple[0])
					batch.Columns[1].(*vectorized.TypedColumn[string]).Append(tuple[1])
					expected[tuple] = struct{}{}
				}
				batch.Len = count
				plan := &RawPlan{Distinct: true, Slots: []Slot{{Source: defs[0], Output: defs[0]}, {Source: defs[1], Output: defs[1]}}}
				execute := func(validate bool) (AggregateStats, error) {
					ctx := vectorized.WithQueryMemoryTracker(context.Background(), 16<<20)
					source := &rawSource{schema: schema, batches: []*vectorized.RecordBatch{batch}}
					operator, err := WrapRaw(ctx, source, plan)
					if err != nil {
						return AggregateStats{}, err
					}
					defer func() { _ = operator.Close() }()
					if err = operator.Init(ctx); err != nil {
						return AggregateStats{}, err
					}
					actual := make(map[[2]string]struct{})
					outputRows := 0
					for {
						output, nextErr := operator.NextBatch(ctx)
						if nextErr != nil {
							return AggregateStats{}, nextErr
						}
						if output == nil {
							break
						}
						outputRows += output.Len
						if validate {
							left := output.Columns[0].(*vectorized.TypedColumn[string]).Data()
							right := output.Columns[1].(*vectorized.TypedColumn[string]).Data()
							for i := range left {
								actual[[2]string{left[i], right[i]}] = struct{}{}
							}
						}
					}
					if outputRows != len(expected) {
						return AggregateStats{}, fmt.Errorf("unique tuples: got %d want %d", outputRows, len(expected))
					}
					if validate {
						for tuple := range expected {
							if _, ok := actual[tuple]; !ok {
								return AggregateStats{}, fmt.Errorf("missing reference tuple")
							}
						}
					}
					stats, _ := DistinctStatistics(operator)
					if stats.StateLanes != 0 || stats.InputRows != uint64(count) {
						return stats, fmt.Errorf("invalid distinct structural state")
					}
					if err = operator.Close(); err != nil {
						return stats, err
					}
					if vectorized.QueryMemoryTracker(ctx, 0).Used() != 0 {
						return stats, fmt.Errorf("distinct retained reservations after Close")
					}
					return stats, nil
				}
				stats, err := execute(true)
				if err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, err = execute(false); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				b.ReportMetric(float64(stats.KeyBytes), "key-bytes/op")
				b.ReportMetric(float64(stats.Groups), "tuples/op")
				b.ReportMetric(float64(stats.StateLanes), "state-lanes/op")
			})
		}
	}
}
