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
	"bytes"
	"context"
	"fmt"
	"math/bits"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/apache/skywalking-banyandb/pkg/pool"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

type sortBenchShape struct {
	order                     string
	rows, keys, limit, offset int
	width                     int
}

// The cases vary one concern at a time around the 100k random control. They
// deliberately avoid a Cartesian matrix; only the first two own the <=0.80 gate.
func sortBenchShapes() []sortBenchShape {
	return []sortBenchShape{
		{order: "random", rows: 100000, keys: 3, limit: 10, width: 16},
		{order: "random", rows: 100000, keys: 3, limit: 100, width: 16},
		{order: "ordered", rows: 1024, keys: 1, limit: 10, width: 16},
		{order: "reverse", rows: 32768, keys: 3, limit: 100, width: 16},
		{order: "tied", rows: 32768, keys: 8, limit: 1000, width: 16},
		{order: "random", rows: 32768, keys: 8, limit: 100, width: 16},
		{order: "random", rows: 32768, keys: 1, limit: 100, width: 1024},
		{order: "reverse", rows: 32768, keys: 3, limit: 1000, width: 1024},
		{order: "random", rows: 100000, keys: 3, limit: 100, offset: 90000, width: 16},
	}
}

func (s sortBenchShape) name() string {
	return fmt.Sprintf("rows=%d/keys=%d/%s/width=%d/limit=%d/offset=%d", s.rows, s.keys, s.order, s.width, s.limit, s.offset)
}

type sortBenchTuple struct {
	payload string
	keys    [8]int64
	nulls   [8]bool
	rid     int64
}

type sortBenchFixture struct {
	schema  *vectorized.BatchSchema
	plan    SortPlan
	input   []sortBenchTuple
	want    []sortBenchTuple
	batches []*vectorized.RecordBatch
	shape   sortBenchShape
}

type sortBenchSource struct {
	schema  *vectorized.BatchSchema
	batches []*vectorized.RecordBatch
	next    int
	closes  int
}

func (s *sortBenchSource) Init(context.Context) error            { return nil }
func (s *sortBenchSource) OutputSchema() *vectorized.BatchSchema { return s.schema }
func (s *sortBenchSource) NextBatch(context.Context) (*vectorized.RecordBatch, error) {
	if s.next == len(s.batches) {
		return nil, nil
	}
	batch := s.batches[s.next]
	s.next++
	return batch, nil
}
func (s *sortBenchSource) Close() error { s.closes++; return nil }

var sortBenchSink int64

// BenchmarkSortT17 measures the complete public operator lifecycle. Full sort
// receives the same finite page as top-k, so both copy and emit identical rows.
// The tuple baseline uses only standard-library sorting and fixture arithmetic.
func BenchmarkSortT17(b *testing.B) {
	for _, shape := range sortBenchShapes() {
		fixture := makeSortBenchFixture(shape)
		b.Run(shape.name(), func(b *testing.B) {
			b.Run("independent_tuple", func(b *testing.B) { benchmarkSortTuple(b, fixture) })
			for _, full := range []bool{true, false} {
				name := "topk"
				if full {
					name = "fullsort_slice"
				}
				b.Run(name, func(b *testing.B) { benchmarkSortOperator(b, fixture, full) })
			}
		})
	}
}

func benchmarkSortTuple(b *testing.B, fixture sortBenchFixture) {
	validateSortBench(b, fixture, true)
	b.ReportAllocs()
	b.SetBytes(int64(fixture.shape.rows) * int64(fixture.shape.width+8*fixture.shape.keys))
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		tuples := slices.Clone(fixture.input)
		slices.SortFunc(tuples, func(left, right sortBenchTuple) int { return compareSortBenchTuples(left, right, fixture.shape.keys) })
		page := tuples[fixture.shape.offset:min(fixture.shape.offset+fixture.shape.limit, len(tuples))]
		for _, tuple := range page {
			sortBenchSink = tuple.rid + int64(len(tuple.payload))
		}
	}
}

func benchmarkSortOperator(b *testing.B, fixture sortBenchFixture, full bool) {
	stats := validateSortBench(b, fixture, full)
	runtime.GC()
	b.ReportAllocs()
	b.SetBytes(int64(fixture.shape.rows) * int64(fixture.shape.width+8*fixture.shape.keys))
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		ctx, operator, _ := startSortBench(b, fixture, full)
		for {
			batch, err := operator.NextBatch(ctx)
			if err != nil {
				_ = operator.Close()
				b.Fatal(err)
			}
			if batch == nil {
				break
			}
			sortBenchSink = int64(batch.Len)
		}
		if err := operator.Close(); err != nil {
			b.Fatal(err)
		}
		if vectorized.QueryMemoryTracker(ctx, 0).Used() != 0 {
			b.Fatal("sort retained reservations after Close")
		}
	}
	b.StopTimer()
	validateSortBench(b, fixture, full)
	b.ReportMetric(float64(stats.InputRows), "input-rows/op")
	b.ReportMetric(float64(stats.OutputRows), "output-rows/op")
	b.ReportMetric(float64(stats.Comparisons), "comparisons/op")
	b.ReportMetric(float64(stats.KeyRootCalls), "key-root-calls/op")
	b.ReportMetric(float64(stats.KeyEvaluations), "key-applications/op")
	b.ReportMetric(float64(stats.KeyKernelRows), "key-kernel-rows/op")
	b.ReportMetric(float64(stats.PeakCandidates), "peak-candidates/op")
	b.ReportMetric(float64(stats.PeakPayloadBytes), "peak-payload-B/op")
	b.ReportMetric(float64(stats.DescriptorBytes), "descriptor-B/op")
	b.ReportMetric(float64(stats.PeakBytes), "peak-retained-B/op")
}

func makeSortBenchFixture(shape sortBenchShape) sortBenchFixture {
	fixture := sortBenchFixture{shape: shape}
	for row := 0; row < shape.rows; row++ {
		tuple := sortBenchTuple{rid: int64(row), payload: fmt.Sprintf("row_%010d_", row) + strings.Repeat("x", max(0, shape.width-15))}
		for key := 0; key < shape.keys-1; key++ {
			// SplitMix64 gives a fixed, reproducible distribution independent of
			// production sort/evaluation code and source iteration order.
			value := sortBenchMix(uint64(row) + uint64(key+1)*0x9e3779b97f4a7c15)
			tuple.keys[key] = int64(value % uint64(max(1, shape.rows/8)))
			tuple.nulls[key] = (row+key*3)%19 == 0
			if shape.order == "tied" {
				tuple.keys[key] = int64(row % 4)
			}
		}
		tuple.keys[shape.keys-1] = tuple.rid
		fixture.input = append(fixture.input, tuple)
	}
	ordered := slices.Clone(fixture.input)
	slices.SortFunc(ordered, func(left, right sortBenchTuple) int { return compareSortBenchTuples(left, right, shape.keys) })
	fixture.want = slices.Clone(ordered[shape.offset:min(shape.offset+shape.limit, len(ordered))])
	if shape.order == "random" {
		// Permute physical source order independently of key generation. In
		// particular, the one-key control uses RID and must not scan it in
		// ascending order while claiming random input.
		for row := len(fixture.input) - 1; row > 0; row-- {
			other := int(sortBenchMix(uint64(row)^0x8c3c010cb4754c9d) % uint64(row+1))
			fixture.input[row], fixture.input[other] = fixture.input[other], fixture.input[row]
		}
	}
	if shape.order == "ordered" || shape.order == "reverse" {
		fixture.input = ordered
		if shape.order == "reverse" {
			slices.Reverse(fixture.input)
		}
	}
	defs := []vectorized.ColumnDef{
		{Name: "rid", Role: vectorized.RoleField, Type: vectorized.ColumnTypeInt64},
		{Name: "payload", Role: vectorized.RoleField, Type: vectorized.ColumnTypeString},
	}
	for key := 0; key < shape.keys-1; key++ {
		defs = append(defs, vectorized.ColumnDef{Name: fmt.Sprintf("key_%d", key), Role: vectorized.RoleField, Type: vectorized.ColumnTypeInt64})
	}
	fixture.schema = vectorized.NewBatchSchema(defs)
	limit := uint32(shape.limit)
	fixture.plan = SortPlan{Limit: &limit, Offset: uint32(shape.offset), Outputs: []Slot{{Source: defs[0], Output: defs[0]}, {Source: defs[1], Output: defs[1]}}}
	for key := 0; key < shape.keys; key++ {
		column := defs[0]
		if key < shape.keys-1 {
			column = defs[key+2]
		}
		expr := &Expression{Op: "column", Source: column}
		if key == 0 && shape.keys > 1 {
			expr = &Expression{Op: "+", Args: []*Expression{expr, {Op: "literal", Type: ExpressionInt, Literal: ExpressionScalar{Int64: 1}}}}
		}
		fixture.plan.Keys = append(fixture.plan.Keys, SortKey{Expr: expr, Descending: key%2 == 0, NullsFirst: key%2 == 1})
	}
	for first := 0; first < shape.rows; first += 1024 {
		end := min(first+1024, shape.rows)
		batch := vectorized.NewRecordBatch(fixture.schema, end-first)
		for _, tuple := range fixture.input[first:end] {
			batch.Columns[0].(*vectorized.TypedColumn[int64]).Append(tuple.rid)
			batch.Columns[1].(*vectorized.TypedColumn[string]).Append(tuple.payload)
			for key := 0; key < shape.keys-1; key++ {
				if tuple.nulls[key] {
					batch.Columns[key+2].AppendNull()
				} else {
					batch.Columns[key+2].(*vectorized.TypedColumn[int64]).Append(tuple.keys[key])
				}
			}
		}
		batch.Len = end - first
		fixture.batches = append(fixture.batches, batch)
	}
	return fixture
}

func sortBenchMix(value uint64) uint64 {
	value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9
	value = (value ^ (value >> 27)) * 0x94d049bb133111eb
	return value ^ (value >> 31)
}

func compareSortBenchTuples(left, right sortBenchTuple, keys int) int {
	for key := 0; key < keys; key++ {
		if left.nulls[key] != right.nulls[key] {
			if left.nulls[key] == (key%2 == 1) {
				return -1
			}
			return 1
		}
		if left.nulls[key] || left.keys[key] == right.keys[key] {
			continue
		}
		if (left.keys[key] < right.keys[key]) != (key%2 == 0) {
			return -1
		}
		return 1
	}
	return 0
}

func startSortBench(t testing.TB, fixture sortBenchFixture, full bool) (context.Context, vectorized.PullOperator, *sortBenchSource) {
	t.Helper()
	ctx := vectorized.WithQueryMemoryTracker(t.Context(), 256<<20)
	source := &sortBenchSource{schema: fixture.schema, batches: fixture.batches}
	plan := fixture.plan
	plan.FullSort = full
	operator, err := WrapSort(ctx, source, &plan)
	if err != nil {
		t.Fatal(err)
	}
	if err = operator.Init(ctx); err != nil {
		_ = operator.Close()
		t.Fatal(err)
	}
	return ctx, operator, source
}

func validateSortBench(t testing.TB, fixture sortBenchFixture, full bool) SortStats {
	t.Helper()
	owners := vectorized.OutstandingOwnerPins()
	flatten := pool.AllRefsCount()["native-query-codec-flatten"]
	memory := vectorized.QueryMemoryStats().LiveReservedBytes
	ctx, operator, source := startSortBench(t, fixture, full)
	defer func() { _ = operator.Close() }()
	position := 0
	for {
		batch, err := operator.NextBatch(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if batch == nil {
			break
		}
		if len(batch.Columns) != 2 || batch.Schema.Columns[0].Name != "rid" || batch.Schema.Columns[1].Name != "payload" {
			t.Fatal("sort returned hidden keys or wrong projection")
		}
		for row := 0; row < batch.Len; row++ {
			if position == len(fixture.want) {
				t.Fatal("sort returned extra rows")
			}
			want := fixture.want[position]
			if batch.Columns[0].IsNull(row) || batch.Columns[1].IsNull(row) ||
				batch.Columns[0].(*vectorized.TypedColumn[int64]).Data()[row] != want.rid || batch.Columns[1].(*vectorized.TypedColumn[string]).Data()[row] != want.payload {
				t.Fatalf("sort row %d differs from independent tuple page", position)
			}
			position++
		}
	}
	if position != len(fixture.want) {
		t.Fatalf("sort returned %d rows, want %d", position, len(fixture.want))
	}
	stats, ok := SortStatistics(operator)
	if !ok {
		t.Fatal("sort statistics unavailable")
	}
	validateSortBenchWork(t, fixture, stats, full)
	if err := operator.Close(); err != nil {
		t.Fatal(err)
	}
	closed, _ := SortStatistics(operator)
	if vectorized.QueryMemoryTracker(ctx, 0).Used() != 0 || vectorized.OutstandingOwnerPins() != owners || source.closes != 1 || closed.LiveBytes != 0 ||
		pool.AllRefsCount()["native-query-codec-flatten"] != flatten || vectorized.QueryMemoryStats().LiveReservedBytes != memory {
		t.Fatal("sort did not restore tracker/source/owner baselines")
	}
	return stats
}

func validateSortBenchWork(t testing.TB, fixture sortBenchFixture, stats SortStats, full bool) {
	t.Helper()
	shape := fixture.shape
	if stats.InputRows != uint64(shape.rows) || stats.OutputRows != uint64(len(fixture.want)) || stats.Batches != uint64(len(fixture.batches)) ||
		stats.KeyEvaluations != uint64(shape.rows*shape.keys) || stats.KeyRootCalls != uint64(shape.keys*len(fixture.batches)) || stats.KeyScalarEvaluations != 0 {
		t.Fatalf("sort actual root work differs from fixture rows/batches: %+v", stats)
	}
	wantKernelRows := uint64(0)
	if shape.keys > 1 {
		// Only the first key has an arithmetic node. Columns and literals are
		// leaves, so the independent expectation is exactly one kernel per row.
		wantKernelRows = uint64(shape.rows)
	}
	if stats.KeyKernelRows != wantKernelRows || stats.Comparisons == 0 || stats.SourcePins != 0 || stats.FullSort != full {
		t.Fatalf("sort actual kernels/comparator/source ownership differs: %+v", stats)
	}
	if full && shape.rows >= 1024 && stats.Comparisons > uint64(4*shape.rows*bits.Len(uint(shape.rows-1))) {
		t.Fatal("full sort exceeded comparator work bound")
	}
	if !full && (stats.PeakCandidates > uint64(shape.limit+shape.offset) || stats.Candidates > uint64(shape.limit+shape.offset)) {
		t.Fatal("top-k exceeded checked page candidate bound")
	}
	if stats.PayloadCapacity > 2*stats.PayloadBytes || stats.CandidateCapacity < stats.Candidates || stats.PeakBytes == 0 || stats.DescriptorBytes == 0 {
		t.Fatalf("sort candidate payload/capacity bounds differ: %+v", stats)
	}
}

func TestSortT17BenchmarkFixtures(t *testing.T) {
	for _, shape := range sortBenchShapes() {
		t.Run(shape.name(), func(t *testing.T) {
			fixture := makeSortBenchFixture(shape)
			for _, full := range []bool{true, false} {
				validateSortBench(t, fixture, full)
			}
		})
	}
}

type sortBenchPoisonOwner struct {
	data []byte
	refs int
}

func (o *sortBenchPoisonOwner) Retain() { o.refs++ }
func (o *sortBenchPoisonOwner) Release() {
	o.refs--
	if o.refs == 0 {
		clear(o.data)
	}
}

type sortBenchLargeSource struct {
	schema  *vectorized.BatchSchema
	current *vectorized.RecordBatch
	owners  []*sortBenchPoisonOwner
	next    int
}

func (s *sortBenchLargeSource) Init(context.Context) error            { return nil }
func (s *sortBenchLargeSource) OutputSchema() *vectorized.BatchSchema { return s.schema }
func (s *sortBenchLargeSource) NextBatch(context.Context) (*vectorized.RecordBatch, error) {
	if s.current != nil {
		s.current.ReleaseOwners()
		s.current = nil
	}
	if s.next == 32 {
		return nil, nil
	}
	owner := &sortBenchPoisonOwner{data: bytes.Repeat([]byte{'x'}, 1<<20)}
	batch := vectorized.NewRecordBatch(s.schema, 128)
	batch.Owners.Pin(owner)
	for row := 0; row < 128; row++ {
		key := int64(1000000 + s.next*128 + row)
		payload := owner.data[row*8192 : (row+1)*8192]
		if row == 0 {
			key = int64(s.next)
			payload = payload[:16]
		}
		batch.Columns[0].(*vectorized.TypedColumn[int64]).Append(key)
		batch.Columns[1].(*vectorized.TypedColumn[[]byte]).Append(payload)
	}
	batch.Len, s.current = 128, batch
	s.owners = append(s.owners, owner)
	s.next++
	return batch, nil
}

func (s *sortBenchLargeSource) Close() error {
	if s.current != nil {
		s.current.ReleaseOwners()
		s.current = nil
	}
	return nil
}

// TestSortT17NarrowCandidatesReleaseMiBSources poisons each 1 MiB input on the
// next pull. Exactly one narrow row from every batch survives the 32-row heap;
// checking payloads after every source was released detects borrowed payloads
// even when no reference-counted batch owner was explicitly pinned.
func TestSortT17NarrowCandidatesReleaseMiBSources(t *testing.T) {
	owners := vectorized.OutstandingOwnerPins()
	flatten := pool.AllRefsCount()["native-query-codec-flatten"]
	memory := vectorized.QueryMemoryStats().LiveReservedBytes
	defs := []vectorized.ColumnDef{
		{Name: "key", Role: vectorized.RoleField, Type: vectorized.ColumnTypeInt64},
		{Name: "payload", Role: vectorized.RoleField, Type: vectorized.ColumnTypeBytes},
	}
	source := &sortBenchLargeSource{schema: vectorized.NewBatchSchema(defs)}
	limit := uint32(32)
	plan := &SortPlan{
		Keys: []SortKey{{Expr: &Expression{Op: "column", Source: defs[0]}}}, Limit: &limit,
		Outputs: []Slot{{Source: defs[0], Output: defs[0]}, {Source: defs[1], Output: defs[1]}},
	}
	ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
	operator, err := WrapSort(ctx, source, plan)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = operator.Close() }()
	if err = operator.Init(ctx); err != nil {
		t.Fatal(err)
	}
	position := 0
	for {
		batch, pullErr := operator.NextBatch(ctx)
		if pullErr != nil {
			t.Fatal(pullErr)
		}
		if batch == nil {
			break
		}
		for row := 0; row < batch.Len; row++ {
			key := batch.Columns[0].(*vectorized.TypedColumn[int64]).Data()[row]
			payload := batch.Columns[1].(*vectorized.TypedColumn[[]byte]).Data()[row]
			if key != int64(position) || !bytes.Equal(payload, bytes.Repeat([]byte{'x'}, 16)) {
				t.Fatal("retained candidate differs after poisoning its MiB source")
			}
			position++
		}
	}
	stats, ok := SortStatistics(operator)
	if !ok || position != 32 || stats.InputRows != 32*128 || stats.PeakCandidates != 32 || stats.SourcePins != 0 ||
		stats.PayloadBytes != 32*16 || stats.PayloadCapacity > 2*stats.PayloadBytes || stats.CandidateCapacity > 64 {
		t.Fatalf("narrow candidates exceeded compact retained-state bounds: %+v", stats)
	}
	for _, owner := range source.owners {
		if owner.refs != 0 || !bytes.Equal(owner.data, make([]byte, 1<<20)) {
			t.Fatal("top-k kept a source owner pinned after copying candidates")
		}
	}
	if err = operator.Close(); err != nil {
		t.Fatal(err)
	}
	if vectorized.QueryMemoryTracker(ctx, 0).Used() != 0 || vectorized.OutstandingOwnerPins() != owners ||
		pool.AllRefsCount()["native-query-codec-flatten"] != flatten || vectorized.QueryMemoryStats().LiveReservedBytes != memory {
		t.Fatal("MiB source benchmark retained reservations or owners after Close")
	}
}
