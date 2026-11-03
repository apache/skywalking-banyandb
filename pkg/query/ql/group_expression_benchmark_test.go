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
	"slices"
	"testing"
	"time"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

type groupExpressionBenchSource struct {
	schema  *vectorized.BatchSchema
	batches []*vectorized.RecordBatch
	next    int
}

func (s *groupExpressionBenchSource) Init(context.Context) error            { return nil }
func (s *groupExpressionBenchSource) OutputSchema() *vectorized.BatchSchema { return s.schema }
func (s *groupExpressionBenchSource) NextBatch(context.Context) (*vectorized.RecordBatch, error) {
	if s.next == len(s.batches) {
		return nil, nil
	}
	batch := s.batches[s.next]
	s.next++
	return batch, nil
}
func (s *groupExpressionBenchSource) Close() error { return nil }

type groupExpressionBenchFixture struct {
	schema           *vectorized.BatchSchema
	plan             *AggregatePlan
	batches          []*vectorized.RecordBatch
	rows, groups     int
	computed, bucket bool
}

var groupExpressionBenchSink int

// BenchmarkGroupExpressions compares equivalent precomputed and computed integer
// keys through the public aggregate operator. Both return the same duplicated
// key and nullable SUM, using the same input batches and group table. Ordinal
// resolution is a planning concern covered by the real RPC controls.
func BenchmarkGroupExpressions(b *testing.B) {
	for _, shape := range []struct {
		rows, groups int
		bucket       bool
	}{{1024, 8, false}, {1024, 512, false}, {32768, 8, false}, {32768, 16384, false}, {1024, 512, true}} {
		for _, computed := range []bool{false, true} {
			kind := "precomputed"
			if computed {
				kind = "computed"
			}
			fixture := makeGroupExpressionBenchFixture(shape.rows, shape.groups, computed)
			if shape.bucket {
				fixture = makeGroupBucketBenchFixture(computed)
				kind = "bucket2s_" + kind
			}
			b.Run(fmt.Sprintf("rows=%d/groups=%d/%s", shape.rows, shape.groups, kind), func(b *testing.B) {
				benchmarkGroupExpressionFixture(b, fixture)
			})
		}
	}
}

func benchmarkGroupExpressionFixture(b *testing.B, fixture groupExpressionBenchFixture) {
	stats, work := validateGroupExpressionBench(b, fixture)
	runtime.GC()
	b.ReportAllocs()
	b.SetBytes(int64(fixture.rows) * 16)
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		ctx, operator := startGroupExpressionBench(b, fixture)
		for {
			batch, err := operator.NextBatch(ctx)
			if err != nil {
				_ = operator.Close()
				b.Fatal(err)
			}
			if batch == nil {
				break
			}
			groupExpressionBenchSink = batch.Len
		}
		if err := operator.Close(); err != nil {
			b.Fatal(err)
		}
		if vectorized.QueryMemoryTracker(ctx, 0).Used() != 0 {
			b.Fatal("group key kernel retained reservations after Close")
		}
	}
	b.StopTimer()
	validateGroupExpressionBench(b, fixture)
	b.ReportMetric(float64(stats.InputRows), "input-rows/op")
	b.ReportMetric(float64(stats.Groups), "groups/op")
	b.ReportMetric(float64(stats.KeyBytes), "key-B/op")
	b.ReportMetric(float64(stats.StateBytes), "state-B/op")
	b.ReportMetric(float64(stats.StateLanes), "lanes/op")
	if work.KeyRootCalls > 0 {
		b.ReportMetric(float64(work.KeyEvaluations), "key-applications/op")
		b.ReportMetric(float64(work.KeyRootCalls), "key-root-calls/op")
		b.ReportMetric(float64(work.VectorRows), "vector-rows/op")
		b.ReportMetric(float64(work.ScalarEvaluations), "scalar-evaluations/op")
	}
}

func TestGroupExpressionBenchmarkFixtures(t *testing.T) {
	for _, groups := range []int{8, 512} {
		for _, computed := range []bool{false, true} {
			validateGroupExpressionBench(t, makeGroupExpressionBenchFixture(1024, groups, computed))
		}
	}
	for _, computed := range []bool{false, true} {
		validateGroupExpressionBench(t, makeGroupBucketBenchFixture(computed))
	}
}

func makeGroupExpressionBenchFixture(rows, groups int, computed bool) groupExpressionBenchFixture {
	defs := []vectorized.ColumnDef{
		{Name: "precomputed", Role: vectorized.RoleField, Type: vectorized.ColumnTypeInt64},
		{Name: "raw", Role: vectorized.RoleField, Type: vectorized.ColumnTypeInt64},
		{Name: "v", Role: vectorized.RoleField, Type: vectorized.ColumnTypeInt64},
	}
	fixture := groupExpressionBenchFixture{schema: vectorized.NewBatchSchema(defs), rows: rows, groups: groups, computed: computed}
	for first := 0; first < rows; first += 1024 {
		end := min(first+1024, rows)
		batch := vectorized.NewRecordBatch(fixture.schema, end-first)
		for row := first; row < end; row++ {
			key := int64(row % groups)
			batch.Columns[0].(*vectorized.TypedColumn[int64]).Append(key)
			batch.Columns[1].(*vectorized.TypedColumn[int64]).Append(key - 1)
			if row%5 == 0 {
				batch.Columns[2].AppendNull()
			} else {
				batch.Columns[2].(*vectorized.TypedColumn[int64]).Append(int64(row%97 - 48))
			}
		}
		batch.Len = end - first
		fixture.batches = append(fixture.batches, batch)
	}
	group := defs[0]
	fixture.plan = &AggregatePlan{Calls: []AggregateCall{{Source: defs[2], Function: AggregateSum}}}
	if computed {
		group = vectorized.ColumnDef{Name: "__ql_group_0", Role: vectorized.RoleField, Type: vectorized.ColumnTypeInt64}
		column := &Expression{Op: "column", Source: defs[1]}
		literal := func(value int64) *Expression {
			return &Expression{Op: "literal", Type: ExpressionInt, Literal: ExpressionScalar{Int64: value}}
		}
		key := &Expression{Op: "case", Args: []*Expression{
			{Op: ">=", Args: []*Expression{column, literal(0)}},
			{Op: "+", Args: []*Expression{column, literal(1)}},
			literal(0),
		}}
		fixture.plan.GroupExpressions = &ExpressionPlan{Outputs: []ExpressionOutput{{Expr: key, Column: group}}}
	}
	fixture.plan.GroupBy = []vectorized.ColumnDef{group}
	fixture.plan.Outputs = []AggregateOutput{
		{Column: vectorized.ColumnDef{Name: "key", Role: vectorized.RoleField, Type: vectorized.ColumnTypeInt64}, IsGroup: true},
		{Column: vectorized.ColumnDef{Name: "key_again", Role: vectorized.RoleField, Type: vectorized.ColumnTypeInt64}, IsGroup: true},
		{Column: vectorized.ColumnDef{Name: "total", Role: vectorized.RoleField, Type: vectorized.ColumnTypeInt64}},
	}
	return fixture
}

func makeGroupBucketBenchFixture(computed bool) groupExpressionBenchFixture {
	fixture := makeGroupExpressionBenchFixture(1024, 512, false)
	fixture.bucket, fixture.computed = true, computed
	fixture.plan.TimeBucketWidth, fixture.plan.TimeBucketIndex = "2s", 0
	timestamp := vectorized.ColumnDef{Name: "time", Role: vectorized.RoleTimestamp, Type: vectorized.ColumnTypeInt64}
	fixture.schema = vectorized.NewBatchSchema(append(fixture.schema.Columns, timestamp))
	for _, batch := range fixture.batches {
		batch.Schema = fixture.schema
		times := vectorized.NewInt64Column(batch.Len)
		for row := 0; row < batch.Len; row++ {
			times.Append(int64(row) * int64(time.Second))
			batch.Columns[0].(*vectorized.TypedColumn[int64]).Data()[row] = int64(row/2) * int64(2*time.Second)
		}
		batch.Columns = append(batch.Columns, times)
	}
	if computed {
		group := vectorized.ColumnDef{Name: "__ql_group_0", Role: vectorized.RoleField, Type: vectorized.ColumnTypeInt64}
		fixture.plan.GroupBy = []vectorized.ColumnDef{group}
		fixture.plan.GroupExpressions = &ExpressionPlan{Outputs: []ExpressionOutput{{
			Expr: &Expression{
				Op: "time_bucket", Type: ExpressionTimestamp, Literal: ExpressionScalar{Int64: int64(2 * time.Second)},
				Args: []*Expression{{Op: "column", Source: timestamp}},
			}, Column: group,
		}}}
	}
	return fixture
}

func startGroupExpressionBench(b testing.TB, fixture groupExpressionBenchFixture) (context.Context, vectorized.PullOperator) {
	b.Helper()
	ctx := vectorized.WithQueryMemoryTracker(b.Context(), 256<<20)
	source := &groupExpressionBenchSource{schema: fixture.schema, batches: fixture.batches}
	operator, err := WrapAggregate(ctx, source, fixture.plan)
	if err != nil {
		b.Fatal(err)
	}
	if err = operator.Init(ctx); err != nil {
		_ = operator.Close()
		b.Fatal(err)
	}
	return ctx, operator
}

func validateGroupExpressionBench(b testing.TB, fixture groupExpressionBenchFixture) (AggregateStats, ExpressionStats) {
	b.Helper()
	before := GroupExpressionExecutionStats()
	ctx, operator := startGroupExpressionBench(b, fixture)
	defer func() {
		if err := operator.Close(); err != nil {
			b.Fatal(err)
		}
	}()
	validateGroupExpressionBenchSchema(b, fixture, operator.OutputSchema())
	seen := make([]bool, fixture.groups)
	for {
		batch, err := operator.NextBatch(ctx)
		if err != nil {
			b.Fatal(err)
		}
		if batch == nil {
			break
		}
		validateGroupExpressionBenchSchema(b, fixture, batch.Schema)
		if len(batch.Columns) != len(batch.Schema.Columns) {
			b.Fatal("group key kernel returned a different batch column layout")
		}
		keys := batch.Columns[0].(*vectorized.TypedColumn[int64]).Data()
		validateGroupExpressionBenchTime(b, fixture, batch, keys)
		duplicates := batch.Columns[1].(*vectorized.TypedColumn[int64]).Data()
		totals := batch.Columns[2].(*vectorized.TypedColumn[int64]).Data()
		for row, key := range keys {
			identity := key
			if fixture.bucket {
				if key%int64(2*time.Second) != 0 {
					b.Fatal("time bucket kernel returned an unaligned key")
				}
				identity = key / int64(2*time.Second)
			}
			if identity < 0 || identity >= int64(fixture.groups) || seen[identity] || duplicates[row] != key {
				b.Fatal("group key kernel returned an unknown, duplicate or inconsistent key")
			}
			seen[identity] = true
			total, valid := groupExpressionBenchExpected(fixture, identity)
			if batch.Columns[2].IsNull(row) == valid || valid && totals[row] != total {
				b.Fatal("group key kernel returned a wrong nullable SUM")
			}
		}
	}
	for _, present := range seen {
		if !present {
			b.Fatal("group key kernel omitted a group")
		}
	}
	stats, ok := AggregateStatistics(operator)
	if !ok || stats.InputRows != uint64(fixture.rows) || stats.Groups != uint64(fixture.groups) || stats.StateLanes != 2 ||
		stats.StateBytes != uint64(fixture.groups)*16 || stats.KeyBytes != uint64(fixture.groups)*10 {
		b.Fatal("group key kernel did not consume exactly one input with bounded shared state")
	}
	if err := operator.Close(); err != nil {
		b.Fatal(err)
	}
	if vectorized.QueryMemoryTracker(ctx, 0).Used() != 0 {
		b.Fatal("group key kernel leaked query reservations")
	}
	work := validateGroupExpressionBenchWork(b, fixture, before, GroupExpressionExecutionStats())
	return stats, work
}

func validateGroupExpressionBenchWork(b testing.TB, fixture groupExpressionBenchFixture, before, after ExpressionStats) ExpressionStats {
	b.Helper()
	work := ExpressionStats{
		Executions: after.Executions - before.Executions, Batches: after.Batches - before.Batches,
		KeyRootCalls: after.KeyRootCalls - before.KeyRootCalls, KeyEvaluations: after.KeyEvaluations - before.KeyEvaluations,
		VectorRows: after.VectorRows - before.VectorRows, ScalarEvaluations: after.ScalarEvaluations - before.ScalarEvaluations,
		ArmRows: after.ArmRows - before.ArmRows, LaterArmRows: after.LaterArmRows - before.LaterArmRows,
	}
	if !fixture.computed {
		if work != (ExpressionStats{}) {
			b.Fatal("precomputed grouping unexpectedly evaluated computed keys")
		}
		return work
	}
	if work.Executions == 0 {
		return work // Process-wide observations are disabled in normal builds.
	}
	rows := uint64(fixture.rows)
	if work.KeyRootCalls != uint64(len(fixture.batches)) || work.KeyEvaluations != rows || work.ScalarEvaluations != 0 {
		b.Fatal("computed group roots or scalar work differ from independently counted batches and rows")
	}
	if fixture.bucket {
		if work.VectorRows != rows || work.ArmRows != 0 || work.LaterArmRows != 0 {
			b.Fatal("time bucket kernel work differs from its independent row count")
		}
		return work
	}
	zeroKeys := uint64((fixture.rows + fixture.groups - 1) / fixture.groups)
	if work.VectorRows != 3*rows-zeroKeys || work.ArmRows != rows || work.LaterArmRows != zeroKeys {
		b.Fatal("CASE kernel work differs from independent fixture arithmetic")
	}
	return work
}

func validateGroupExpressionBenchSchema(b testing.TB, fixture groupExpressionBenchFixture, schema *vectorized.BatchSchema) {
	b.Helper()
	expected := []vectorized.ColumnDef{
		{Name: "key", Role: vectorized.RoleField, Type: vectorized.ColumnTypeInt64},
		{Name: "key_again", Role: vectorized.RoleField, Type: vectorized.ColumnTypeInt64},
		{Name: "total", Role: vectorized.RoleField, Type: vectorized.ColumnTypeInt64},
	}
	if fixture.bucket {
		expected = append(expected, vectorized.ColumnDef{Name: "time", Role: vectorized.RoleTimestamp, Type: vectorized.ColumnTypeInt64})
	}
	if schema == nil || !slices.Equal(schema.Columns, expected) {
		b.Fatalf("group key kernel output schema differs: got %v, want %v", schema, expected)
	}
}

func validateGroupExpressionBenchTime(b testing.TB, fixture groupExpressionBenchFixture, batch *vectorized.RecordBatch, keys []int64) {
	b.Helper()
	if !fixture.bucket {
		return
	}
	timestamps, ok := batch.Columns[3].(*vectorized.TypedColumn[int64])
	if !ok {
		b.Fatal("time bucket kernel returned a different timestamp type")
	}
	for row, key := range keys {
		if timestamps.IsNull(row) || timestamps.Data()[row] != key {
			b.Fatal("time bucket kernel metadata differs from the expected bucket key")
		}
	}
}

// The oracle walks fixture rows without evaluating a compiled expression,
// encoding group keys or using the production aggregate state.
func groupExpressionBenchExpected(fixture groupExpressionBenchFixture, identity int64) (total int64, valid bool) {
	first, end, step := int(identity), fixture.rows, fixture.groups
	if fixture.bucket {
		first, end, step = 2*int(identity), 2*int(identity)+2, 1
	}
	for input := first; input < end; input += step {
		if input%5 != 0 {
			total += int64(input%97 - 48)
			valid = true
		}
	}
	return total, valid
}
