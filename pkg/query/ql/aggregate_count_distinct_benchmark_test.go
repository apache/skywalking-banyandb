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

// BenchmarkAggregateCountDistinct isolates exact-set overhead and cardinality
// against ordinary COUNT on identical scalar inputs, without native scan costs.
func BenchmarkAggregateCountDistinct(b *testing.B) {
	const rows = 65536
	def := aggregateField("value", vectorized.ColumnTypeInt64)
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{def})
	for _, cardinality := range []int{64, rows} {
		values := make([]int64, rows)
		for i := range values {
			values[i] = int64(i % cardinality)
		}
		batch := vectorized.NewRecordBatch(schema, 0)
		batch.Columns = []vectorized.Column{vectorized.NewColumnFromSlice(vectorized.ColumnTypeInt64, values)}
		batch.Len = rows
		for _, distinct := range []bool{false, true} {
			b.Run(fmt.Sprintf("cardinality=%d/distinct=%t", cardinality, distinct), func(b *testing.B) {
				plan := countDistinctPlan(def)
				expected := int64(cardinality)
				if !distinct {
					plan.Calls[0].Function = AggregateCount
					expected = rows
				}
				b.ReportAllocs()
				var stats AggregateStats
				for i := 0; i < b.N; i++ {
					ctx := vectorized.WithQueryMemoryTracker(context.Background(), 16<<20)
					source := &rawSource{schema: schema, batches: []*vectorized.RecordBatch{batch}}
					op, err := WrapAggregate(ctx, source, plan)
					if err != nil {
						b.Fatal(err)
					}
					if err = op.Init(ctx); err != nil {
						b.Fatal(err)
					}
					result, err := op.NextBatch(ctx)
					if err != nil {
						b.Fatal(err)
					}
					if got := result.Columns[0].(*vectorized.TypedColumn[int64]).Data()[0]; got != expected {
						b.Fatalf("count: got %d, want %d", got, expected)
					}
					stats, _ = AggregateStatistics(op)
					if err = op.Close(); err != nil {
						b.Fatal(err)
					}
					if vectorized.QueryMemoryTracker(ctx, 0).Used() != 0 {
						b.Fatal("unreleased query memory")
					}
				}
				b.ReportMetric(float64(stats.DistinctKeys), "distinct-keys/op")
				b.ReportMetric(float64(stats.DistinctKeyCapacity), "distinct-reserved-B/op")
			})
		}
	}
}
