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

package measure

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"slices"
	"sort"
	"testing"

	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure/frame"
)

// ordSumName is the output name of the sum aggregation the tests bind.
const ordSumName = "sum_v"

// Column positions of ordFixtureSchema.
const (
	ordColTS = iota
	ordColG
	ordColH
	ordColV
	ordColK
)

// ordSizes are the batch sizes every operator is exercised at: below, just above
// and far above the 1024-row pipeline batch size, and the largest lent batch.
var ordSizes = []int{7, 1025, 8192, 65536}

// ordCase is one way a batch addresses its rows together with the row order a
// selection-aware operator must observe.
type ordCase struct {
	name      string
	selection []uint16
	order     []int
}

// ordCases returns the selections a lent scan batch can carry: none, the
// descending scan's [n-1..0], a shuffled permutation and a reversed subset.
func ordCases(n int) []ordCase {
	identity := make([]int, n)
	reversedSel := make([]uint16, n)
	reversedOrder := make([]int, n)
	for i := range n {
		identity[i] = i
		reversedSel[i] = uint16(n - 1 - i)
		reversedOrder[i] = n - 1 - i
	}
	perm := rand.New(rand.NewSource(int64(n))).Perm(n) //nolint:gosec // deterministic test data
	shuffledSel := make([]uint16, n)
	for i, row := range perm {
		shuffledSel[i] = uint16(row)
	}
	var subsetSel []uint16
	var subsetOrder []int
	for i := n - 1; i >= 0; i-- {
		if i%2 == 1 {
			subsetSel = append(subsetSel, uint16(i))
			subsetOrder = append(subsetOrder, i)
		}
	}
	if subsetSel == nil {
		subsetSel = []uint16{}
	}
	return []ordCase{
		{name: "nil", order: identity},
		{name: "reversed", selection: reversedSel, order: reversedOrder},
		{name: "shuffled", selection: shuffledSel, order: perm},
		{name: "reversed-subset", selection: subsetSel, order: subsetOrder},
	}
}

// ordFixtureSchema is "timestamp, tag g (group key), tag h (unique), field v
// (storage row index), field k (a value with many ties)".
func ordFixtureSchema() *vectorized.BatchSchema {
	return vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleTimestamp, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "g", Type: vectorized.ColumnTypeString},
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "h", Type: vectorized.ColumnTypeString},
		{Role: vectorized.RoleField, Name: "v", Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleField, Name: "k", Type: vectorized.ColumnTypeInt64},
	})
}

func ordGroup(row, n int) string { return fmt.Sprintf("g%d", row%(n/3+1)) }

func ordKey(row int) int64 { return int64(row * 7919 % 251) }

// ordBatch builds a batch of n rows in storage order: ts=row*10, v=row.
func ordBatch(schema *vectorized.BatchSchema, n int, selection []uint16) *vectorized.RecordBatch {
	b := vectorized.NewRecordBatch(schema, n)
	for row := range n {
		b.Columns[ordColTS].(*vectorized.TypedColumn[int64]).Append(int64(row) * 10)
		b.Columns[ordColG].(*vectorized.TypedColumn[string]).Append(ordGroup(row, n))
		b.Columns[ordColH].(*vectorized.TypedColumn[string]).Append(fmt.Sprintf("h%d", row))
		b.Columns[ordColV].(*vectorized.TypedColumn[int64]).Append(int64(row))
		b.Columns[ordColK].(*vectorized.TypedColumn[int64]).Append(ordKey(row))
	}
	b.Len = n
	if selection != nil {
		b.Selection = slices.Clone(selection)
	}
	return b
}

// ordRows reads the v column of b in row order.
func ordRows(b *vectorized.RecordBatch) []int {
	col := b.Columns[ordColV].(*vectorized.TypedColumn[int64]).Data()
	out := make([]int, b.Len)
	for i := range out {
		out[i] = int(col[i])
	}
	return out
}

// ordDrain pulls every batch from next and concatenates their v columns.
func ordDrain(t *testing.T, next func(context.Context) (*vectorized.RecordBatch, error)) []int {
	t.Helper()
	var out []int
	for {
		b, err := next(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if b == nil {
			return out
		}
		out = append(out, ordRows(b)...)
	}
}

func ordCompare(t *testing.T, got, want []int) {
	t.Helper()
	if slices.Equal(got, want) {
		return
	}
	for i := range min(len(got), len(want)) {
		if got[i] != want[i] {
			t.Fatalf("row %d: got storage row %d, want %d (got %d rows, want %d)", i, got[i], want[i], len(got), len(want))
		}
	}
	t.Fatalf("got %d rows, want %d", len(got), len(want))
}

func forEachOrdCase(t *testing.T, run func(t *testing.T, tc ordCase, n int)) {
	t.Helper()
	for _, n := range ordSizes {
		for _, tc := range ordCases(n) {
			t.Run(fmt.Sprintf("%s/%d", tc.name, n), func(t *testing.T) { run(t, tc, n) })
		}
	}
}

// TestBatchLimit_KeepsSelectionOrder pins that offset/limit cut the batch by
// position in selection order, not by storage row index.
func TestBatchLimit_KeepsSelectionOrder(t *testing.T) {
	schema := ordFixtureSchema()
	forEachOrdCase(t, func(t *testing.T, tc ordCase, n int) {
		const offset, limit = 3, 1000
		b := ordBatch(schema, n, tc.selection)
		op := NewBatchLimit(schema, offset, limit)
		if err := op.Process(context.Background(), b); err != nil && !errors.Is(err, vectorized.ErrLimitExhausted) {
			t.Fatal(err)
		}
		want := tc.order[min(offset, len(tc.order)):min(offset+limit, len(tc.order))]
		got := make([]int, 0, len(b.Selection))
		for _, row := range b.Selection {
			got = append(got, int(row))
		}
		ordCompare(t, got, want)
	})
}

// TestBatchGroupBy_KeepsSelectionOrder pins that rows are bucketed in selection
// order: groups appear in first-seen order and rows keep their order within one.
func TestBatchGroupBy_KeepsSelectionOrder(t *testing.T) {
	schema := ordFixtureSchema()
	forEachOrdCase(t, func(t *testing.T, tc ordCase, n int) {
		var groups []string
		members := make(map[string][]int)
		var first []int
		for _, row := range tc.order {
			g := ordGroup(row, n)
			if _, ok := members[g]; !ok {
				groups = append(groups, g)
				first = append(first, row)
			}
			members[g] = append(members[g], row)
		}
		var all []int
		for _, g := range groups {
			all = append(all, members[g]...)
		}
		for _, firstOnly := range []bool{false, true} {
			pool := vectorized.NewBatchPool(schema, vectorized.DefaultBatchSize)
			tracker := vectorized.NewMemoryTracker(1 << 40)
			op := NewBatchGroupBy(schema, []int{ordColG}, pool, vectorized.DefaultBatchSize, tracker, 64, 0)
			want := all
			if firstOnly {
				op = NewBatchGroupByFirst(schema, []int{ordColG}, pool, vectorized.DefaultBatchSize, tracker, 64)
				want = first
			}
			if err := op.Init(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := op.Consume(context.Background(), ordBatch(schema, n, tc.selection)); err != nil {
				t.Fatal(err)
			}
			if err := op.Finalize(context.Background()); err != nil {
				t.Fatal(err)
			}
			ordCompare(t, ordDrain(t, op.NextBatch), want)
			if err := op.Close(); err != nil {
				t.Fatal(err)
			}
		}
	})
}

// TestBatchAggregation_KeepsSelectionOrder pins that groups are emitted in the
// order their first row appears in the selection, that the carried-forward
// non-key tag is that first row's, and that the folded values are right.
func TestBatchAggregation_KeepsSelectionOrder(t *testing.T) {
	schema := ordFixtureSchema()
	forEachOrdCase(t, func(t *testing.T, tc ordCase, n int) {
		var groups []string
		sum := make(map[string]int64)
		count := make(map[string]int64)
		firstH := make(map[string]string)
		for _, row := range tc.order {
			g := ordGroup(row, n)
			if _, ok := sum[g]; !ok {
				groups = append(groups, g)
				firstH[g] = fmt.Sprintf("h%d", row)
			}
			sum[g] += int64(row)
			count[g]++
		}
		op := NewBatchAggregation(schema, []int{ordColG}, []AggSpec{
			{Func: AggSum, InputCol: ordColV, Output: ordSumName},
			{Func: AggCount, InputCol: ordColV, Output: "cnt_v"},
		}, AggModeAll, vectorized.DefaultBatchSize, vectorized.NewMemoryTracker(1<<40), 0)
		if err := op.Init(context.Background()); err != nil {
			t.Fatal(err)
		}
		defer op.Close()
		if err := op.Consume(context.Background(), ordBatch(schema, n, tc.selection)); err != nil {
			t.Fatal(err)
		}
		if err := op.Finalize(context.Background()); err != nil {
			t.Fatal(err)
		}
		out := op.OutputSchema()
		idx := func(name string) int {
			for i, def := range out.Columns {
				if def.Name == name {
					return i
				}
			}
			t.Fatalf("output has no column %q", name)
			return -1
		}
		gIdx, hIdx, sumIdx, cntIdx := idx("g"), idx("h"), idx(ordSumName), idx("cnt_v")
		row := 0
		for {
			b, err := op.NextBatch(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if b == nil {
				break
			}
			for i := range b.Len {
				if row >= len(groups) {
					t.Fatalf("more output rows than the %d expected groups", len(groups))
				}
				g := groups[row]
				if got := b.Columns[gIdx].(*vectorized.TypedColumn[string]).Data()[i]; got != g {
					t.Fatalf("group %d: got %q, want %q (first-seen order)", row, got, g)
				}
				if got := b.Columns[hIdx].(*vectorized.TypedColumn[string]).Data()[i]; got != firstH[g] {
					t.Fatalf("group %q: carried tag %q, want first-seen %q", g, got, firstH[g])
				}
				if got := b.Columns[sumIdx].(*vectorized.TypedColumn[int64]).Data()[i]; got != sum[g] {
					t.Fatalf("group %q: sum %d, want %d", g, got, sum[g])
				}
				if got := b.Columns[cntIdx].(*vectorized.TypedColumn[int64]).Data()[i]; got != count[g] {
					t.Fatalf("group %q: count %d, want %d", g, got, count[g])
				}
				row++
			}
		}
		if row != len(groups) {
			t.Fatalf("got %d groups, want %d", row, len(groups))
		}
	})
}

// TestBatchTop_KeepsSelectionOrder pins that ties on the sort key are resolved
// by position in the selection, so the earlier row of the selection wins.
func TestBatchTop_KeepsSelectionOrder(t *testing.T) {
	schema := ordFixtureSchema()
	forEachOrdCase(t, func(t *testing.T, tc ordCase, n int) {
		for _, asc := range []bool{true, false} {
			const topN = 10
			ranked := slices.Clone(tc.order)
			sort.SliceStable(ranked, func(i, j int) bool {
				if asc {
					return ordKey(ranked[i]) < ordKey(ranked[j])
				}
				return ordKey(ranked[i]) > ordKey(ranked[j])
			})
			want := ranked[:min(topN, len(ranked))]
			op := NewBatchTop(schema, ordColK, topN, asc, vectorized.DefaultBatchSize)
			if err := op.Init(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := op.Consume(context.Background(), ordBatch(schema, n, tc.selection)); err != nil {
				t.Fatal(err)
			}
			if err := op.Finalize(context.Background()); err != nil {
				t.Fatal(err)
			}
			ordCompare(t, ordDrain(t, op.NextBatch), want)
			if err := op.Close(); err != nil {
				t.Fatal(err)
			}
		}
	})
}

// TestSerializeAndFrame_KeepSelectionOrder pins that every egress path (proto
// rows, frame encode/decode, the pipeline drain) emits the active rows in
// selection order.
func TestSerializeAndFrame_KeepSelectionOrder(t *testing.T) {
	schema := ordFixtureSchema()
	forEachOrdCase(t, func(t *testing.T, tc ordCase, n int) {
		src := ordBatch(schema, n, tc.selection)

		points := serializeBatchToProto(src, nil)
		gotTS := make([]int, len(points))
		for i, point := range points {
			gotTS[i] = int(point.GetDataPoint().GetTimestamp().AsTime().UnixNano() / 10)
		}
		ordCompare(t, gotTS, tc.order)

		encoded, encodeErr := frame.Encode(src)
		if encodeErr != nil {
			t.Fatal(encodeErr)
		}
		decoded, decodeErr := frame.Decode(encoded)
		if decodeErr != nil {
			t.Fatal(decodeErr)
		}
		defer decoded.ReleaseOwners()
		ordCompare(t, ordRows(decoded), tc.order)

		flat := vectorized.NewRecordBatch(schema, 0)
		if appendErr := appendActive(flat, src); appendErr != nil {
			t.Fatal(appendErr)
		}
		ordCompare(t, ordRows(flat), tc.order)

		pipeline, buildErr := vectorized.NewPipelineBuilder().From(&fakeBucketUpstream{schema: schema, batches: []*vectorized.RecordBatch{
			ordBatch(schema, n, tc.selection),
		}}).Build()
		if buildErr != nil {
			t.Fatal(buildErr)
		}
		if initErr := pipeline.Init(context.Background()); initErr != nil {
			t.Fatal(initErr)
		}
		body, drainErr := DrainPipelineToFrame(context.Background(), pipeline, schema)
		if drainErr != nil {
			t.Fatal(drainErr)
		}
		if closeErr := pipeline.Close(); closeErr != nil {
			t.Fatal(closeErr)
		}
		if len(tc.order) == 0 {
			return
		}
		drained, decodeErr := frame.Decode(body)
		if decodeErr != nil {
			t.Fatal(decodeErr)
		}
		defer drained.ReleaseOwners()
		ordCompare(t, ordRows(drained), tc.order)
	})
}

// ordBucketSum is the reference: the sum of v per (bucket, g) over rows.
func ordBucketSum(rows []int, n int, width int64) map[string]int64 {
	out := make(map[string]int64)
	for _, row := range rows {
		out[fmt.Sprintf("%d|%s", int64(row)*10/width*width, ordGroup(row, n))] += int64(row)
	}
	return out
}

func ordDrainBucket(t *testing.T, bt *BatchTimeBucket) (map[string]int64, []int64) {
	t.Helper()
	got := make(map[string]int64)
	var bucketOrder []int64
	for {
		b, err := bt.NextBatch(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if b == nil {
			return got, bucketOrder
		}
		cols := b.Schema.Columns
		var tsIdx, gIdx, sumIdx int
		for i, def := range cols {
			switch def.Name {
			case "g":
				gIdx = i
			case ordSumName:
				sumIdx = i
			}
			if def.Role == vectorized.RoleTimestamp {
				tsIdx = i
			}
		}
		for i := range b.Len {
			ts := b.Columns[tsIdx].(*vectorized.TypedColumn[int64]).Data()[i]
			key := fmt.Sprintf("%d|%s", ts, b.Columns[gIdx].(*vectorized.TypedColumn[string]).Data()[i])
			if _, dup := got[key]; dup {
				t.Fatalf("group %s emitted twice", key)
			}
			got[key] = b.Columns[sumIdx].(*vectorized.TypedColumn[int64]).Data()[i]
			bucketOrder = append(bucketOrder, ts)
		}
	}
}

func ordNewBucket(schema *vectorized.BatchSchema, b *vectorized.RecordBatch, width int64, streaming bool) *BatchTimeBucket {
	spec := []AggSpec{{Func: AggSum, InputCol: ordColV, Output: ordSumName}}
	upstream := &fakeBucketUpstream{schema: schema, batches: []*vectorized.RecordBatch{b}}
	return NewBatchTimeBucket(upstream, schema, []int{ordColG}, ordColTS, width, spec, AggModeAll,
		vectorized.DefaultBatchSize, vectorized.NewMemoryTracker(1<<40), 0, streaming)
}

// TestBatchTimeBucket_MapModeKeepsSelectionOrder pins that the index-mode map
// path buckets exactly the selected rows whatever their order, and still emits
// buckets ascending.
func TestBatchTimeBucket_MapModeKeepsSelectionOrder(t *testing.T) {
	schema := ordFixtureSchema()
	forEachOrdCase(t, func(t *testing.T, tc ordCase, n int) {
		const width = 100
		bt := ordNewBucket(schema, ordBatch(schema, n, tc.selection), width, false)
		if err := bt.Init(context.Background()); err != nil {
			t.Fatal(err)
		}
		defer bt.Close()
		got, order := ordDrainBucket(t, bt)
		want := ordBucketSum(tc.order, n, width)
		if len(got) != len(want) {
			t.Fatalf("got %d groups, want %d", len(got), len(want))
		}
		for key, sum := range want {
			if got[key] != sum {
				t.Fatalf("group %s: sum %d, want %d", key, got[key], sum)
			}
		}
		if !slices.IsSorted(order) {
			t.Fatal("map mode must emit buckets ascending")
		}
	})
}

// TestBatchTimeBucket_StreamingTimeDirection pins how the streaming path treats
// the direction of the selection: it needs ascending time, so a descending
// selection over ascending storage fails loudly instead of reopening a closed
// bucket, while a reversed selection over descending storage (ascending rows)
// is bucketed correctly, at every batch size.
func TestBatchTimeBucket_StreamingTimeDirection(t *testing.T) {
	schema := ordFixtureSchema()
	for _, n := range ordSizes {
		t.Run(fmt.Sprintf("%d", n), func(t *testing.T) {
			const width = 10 // one bucket per row, so a descending row always regresses
			ascending := make([]int, n)
			for i := range ascending {
				ascending[i] = i
			}

			bt := ordNewBucket(schema, ordBatch(schema, n, nil), width, true)
			if err := bt.Init(context.Background()); err != nil {
				t.Fatal(err)
			}
			got, order := ordDrainBucket(t, bt)
			want := ordBucketSum(ascending, n, width)
			for key, sum := range want {
				if got[key] != sum {
					t.Fatalf("ascending: group %s: sum %d, want %d", key, got[key], sum)
				}
			}
			if len(got) != len(want) || !slices.IsSorted(order) {
				t.Fatalf("ascending: got %d groups (sorted=%v), want %d", len(got), slices.IsSorted(order), len(want))
			}
			if err := bt.Close(); err != nil {
				t.Fatal(err)
			}

			descending := ordCases(n)[1]
			bt = ordNewBucket(schema, ordBatch(schema, n, descending.selection), width, true)
			if err := bt.Init(context.Background()); err != nil {
				t.Fatal(err)
			}
			defer bt.Close()
			var pullErr error
			for pullErr == nil {
				var b *vectorized.RecordBatch
				if b, pullErr = bt.NextBatch(context.Background()); b == nil && pullErr == nil {
					t.Fatal("a descending selection must not stream silently")
				}
			}
		})
	}
}

// TestReduceAndTopOverSelectedPartials pins that the liaison-side reduce and
// top-N follow a Selection (reversed, shuffled, subset) on a partial, both when
// the active rows fit one batchSize chunk of a much larger batch and when they
// span several chunks.
func TestReduceAndTopOverSelectedPartials(t *testing.T) {
	const numGroups = 5
	for _, n := range []int{1025, 8192, 65536} {
		rows := make([]scalarPartialRow, n)
		for i := range rows {
			rows[i] = scalarPartialRow{shard: int64(i), g: fmt.Sprintf("g%d", i%numGroups), v: int64(i) + 1}
		}
		selections := ordCases(n)[1:]
		// A short selection over a huge batch takes ReducePartialBatches' no-chunk path.
		short := make([]uint16, 500)
		for i := range short {
			short[i] = uint16(n - 1 - 2*i%n)
		}
		selections = append(selections, ordCase{name: "short-reversed", selection: short, order: func() []int {
			order := make([]int, len(short))
			for i, row := range short {
				order[i] = int(row)
			}
			return order
		}()})
		for _, tc := range selections {
			t.Run(fmt.Sprintf("%s/%d", tc.name, n), func(t *testing.T) {
				want := make(map[string]int64)
				for _, row := range tc.order {
					want[rows[row].g] += rows[row].v
				}
				partial := scalarPartialBatch(rows)
				partial.Selection = slices.Clone(tc.selection)
				batches, _, err := ReducePartialBatches([]*vectorized.RecordBatch{partial}, "default", []string{"g"}, false,
					[]AggReduceSpec{{OutputName: "out", Func: AggSum}}, vectorized.DefaultBatchSize, vectorized.NewMemoryTracker(1<<30))
				if err != nil {
					t.Fatal(err)
				}
				got := sumReducedByGroup(t, batches)
				if len(got) != len(want) {
					t.Fatalf("got %d groups, want %d: %v", len(got), len(want), got)
				}
				for g, sum := range want {
					if got[g] != sum {
						t.Fatalf("sum[%s] = %d, want %d", g, got[g], sum)
					}
				}

				// One row per storage row, so the values rank the rows uniquely.
				groups := make([]string, n)
				values := make([]int64, n)
				for i := range groups {
					groups[i] = fmt.Sprintf("g%d", i)
					values[i] = int64(i)
				}
				reduced := reducedTestBatch(groups, values)
				reduced.Selection = slices.Clone(tc.selection)
				topped, topErr := ApplyTopToReduce([]*vectorized.RecordBatch{reduced}, ReduceTopSpec{FieldName: "out", N: 3, Asc: false},
					vectorized.DefaultBatchSize)
				if topErr != nil {
					t.Fatal(topErr)
				}
				ranked := slices.Clone(tc.order)
				sort.Slice(ranked, func(i, j int) bool { return ranked[i] > ranked[j] })
				wantTop := make([]int64, 0, 3)
				for _, row := range ranked[:min(3, len(ranked))] {
					wantTop = append(wantTop, int64(row))
				}
				if gotTop := collectReducedValues(topped); !slices.Equal(gotTop, wantTop) {
					t.Fatalf("top-3: got %v, want %v", gotTop, wantTop)
				}
			})
		}
	}
}

// TestBatchSourceLendsLargeBatchesInSelectionOrder pins that a block-sized
// storage batch (up to the 65,536-row lend limit) reaches the pipeline with its
// selection intact and serializes in selection order.
func TestBatchSourceLendsLargeBatchesInSelectionOrder(t *testing.T) {
	schema := lendSchema()
	for _, n := range []int{1025, 8192, 65536} {
		for _, tc := range ordCases(n)[:3] {
			t.Run(fmt.Sprintf("%s/%d", tc.name, n), func(t *testing.T) {
				pool := vectorized.NewBatchPool(schema, vectorized.DefaultBatchSize)
				mb, _ := nativeBatch(schema, 1, 0, n, nil)
				mb.Selection = slices.Clone(tc.selection)
				src := NewBatchSourceFromBatchResult(&fakeBatchResult{seq: []*model.MeasureBatch{mb}}, schema, pool, vectorized.DefaultBatchSize)
				defer src.Close()
				batches := drain(t, src)
				if len(batches) != 1 || !batches[0].Lent || batches[0].Len != n {
					t.Fatalf("want one lent batch of %d rows, got %d batches", n, len(batches))
				}
				defer batches[0].ReleaseOwners()
				points := serializeBatchToProto(batches[0], nil)
				got := make([]int, len(points))
				for i, point := range points {
					got[i] = int(point.GetDataPoint().GetTimestamp().AsTime().UnixNano())
				}
				ordCompare(t, got, tc.order)
			})
		}
	}
}
