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
	"fmt"
	"math"
	"math/rand"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// The tests in this file prove that the ConstColumn / runs fast paths give the result
// of the per-row path bit for bit. Each scenario is built twice from the same rows:
// the fast batches carry entity tags as ConstColumns where a batch is one series and
// the hints a window scan sets, the plain batches carry only typed columns and no
// hints. The fast batches run with the fast paths on, the plain ones with them off,
// and every emitted row is compared, floats by their bits.

const (
	runsColTS = iota
	runsColShard
	runsColSvc
	runsColInst
	runsColNote
	runsColVI
	runsColVF
	runsColName
)

func runsSchema() *vectorized.BatchSchema {
	return vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleTimestamp, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleShardID, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "svc", Type: vectorized.ColumnTypeString, Entity: true},
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "inst", Type: vectorized.ColumnTypeInt64, Entity: true},
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "note", Type: vectorized.ColumnTypeString},
		{Role: vectorized.RoleField, Name: "vi", Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleField, Name: "vf", Type: vectorized.ColumnTypeFloat64},
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "name", Type: vectorized.ColumnTypeString},
	})
}

type runsRow struct {
	svc    string
	note   string
	name   string
	ts     int64
	inst   int64
	vi     int64
	vf     float64
	viNull bool
	vfNull bool
}

// runsScenario is a sequence of batches, each a list of rows in storage order, plus
// the active order's direction and an optional selection of storage rows.
type runsBatch struct {
	rows []runsRow
	// selection lists storage rows in active order; nil means every row in order.
	selection []uint16
	order     vectorized.TimeOrder
}

func (b *runsBatch) series(i int) string {
	return fmt.Sprintf("%s/%d", b.rows[i].svc, b.rows[i].inst)
}

func (b *runsBatch) singleSeries() bool {
	for i := 1; i < len(b.rows); i++ {
		if b.series(i) != b.series(0) {
			return false
		}
	}
	return true
}

func (b *runsBatch) seriesEnds() []int {
	var ends []int
	for i := 1; i < len(b.rows); i++ {
		if b.series(i) != b.series(i-1) {
			ends = append(ends, i)
		}
	}
	if len(ends) == 0 {
		return nil
	}
	return append(ends, len(b.rows))
}

// build returns the batch. With fast set it carries the scan's hints and, for a single
// series, its entity tags as ConstColumns.
func (b *runsBatch) build(schema *vectorized.BatchSchema, fast bool) *vectorized.RecordBatch {
	n := len(b.rows)
	out := vectorized.NewRecordBatch(schema, n)
	ts := out.Columns[runsColTS].(*vectorized.TypedColumn[int64])
	shard := out.Columns[runsColShard].(*vectorized.TypedColumn[int64])
	note := out.Columns[runsColNote].(*vectorized.TypedColumn[string])
	vi := out.Columns[runsColVI].(*vectorized.TypedColumn[int64])
	vf := out.Columns[runsColVF].(*vectorized.TypedColumn[float64])
	name := out.Columns[runsColName].(*vectorized.TypedColumn[string])
	svc := out.Columns[runsColSvc].(*vectorized.TypedColumn[string])
	inst := out.Columns[runsColInst].(*vectorized.TypedColumn[int64])
	for _, r := range b.rows {
		ts.Append(r.ts)
		shard.Append(r.inst % 3)
		note.Append(r.note)
		svc.Append(r.svc)
		inst.Append(r.inst)
		if r.viNull {
			vi.AppendNull()
		} else {
			vi.Append(r.vi)
		}
		if r.vfNull {
			vf.AppendNull()
		} else {
			vf.Append(r.vf)
		}
		if r.name == "" {
			name.AppendNull()
		} else {
			name.Append(r.name)
		}
	}
	out.Len = n
	out.Selection = b.selection
	if !fast {
		return out
	}
	if b.singleSeries() {
		out.Columns[runsColSvc] = vectorized.NewConstString(b.rows[0].svc, n)
		out.Columns[runsColInst] = vectorized.NewConstInt64(b.rows[0].inst, n)
	}
	out.Hints = vectorized.BatchHints{SeriesKnown: true, SeriesEnds: b.seriesEnds(), TimeOrder: b.order}
	return out
}

// runsTimeline generates rows in time order across several series, cut into batches.
// Series change in bursts, so batches are a mix of single-series blocks and
// interleaved pages. desc reverses the timeline.
func runsTimeline(rng *rand.Rand, nRows int, desc bool, stepMax int64) []runsBatch {
	rows := make([]runsRow, 0, nRows)
	ts := int64(10_000)
	series := rng.Intn(4)
	for range nRows {
		if rng.Intn(10) < 3 {
			series = rng.Intn(4)
		}
		ts += rng.Int63n(stepMax)
		r := runsRow{
			ts:   ts,
			svc:  fmt.Sprintf("svc-%d", series%2),
			inst: int64(series / 2),
			note: fmt.Sprintf("n%d", rng.Intn(3)),
			vi:   rng.Int63n(2000) - 1000,
			// Fractions that do not sum exactly, so a reordered float sum would differ.
			vf:     rng.Float64()*1e3 + 1e-7,
			viNull: rng.Intn(12) == 0,
			vfNull: rng.Intn(12) == 0,
		}
		if rng.Intn(5) != 0 {
			r.name = fmt.Sprintf("name-%d", rng.Intn(4))
		}
		rows = append(rows, r)
	}
	if desc {
		for i, j := 0, len(rows)-1; i < j; i, j = i+1, j-1 {
			rows[i], rows[j] = rows[j], rows[i]
		}
	}
	order := vectorized.TimeOrderAsc
	if desc {
		order = vectorized.TimeOrderDesc
	}
	var batches []runsBatch
	for start := 0; start < len(rows); {
		size := 1 + rng.Intn(40)
		if rng.Intn(3) == 0 {
			// A longer stretch of one series, as a lent block is.
			size = 60 + rng.Intn(60)
		}
		end := min(start+size, len(rows))
		batch := runsBatch{rows: append([]runsRow(nil), rows[start:end]...), order: order}
		start = end
		batches = append(batches, batch)
	}
	return batches
}

// runsRegroup makes a block-shaped single-series batch where the rows allow it, by
// splitting every batch at series changes.
func runsRegroup(batches []runsBatch) []runsBatch {
	var out []runsBatch
	for _, b := range batches {
		start := 0
		for i := 1; i <= len(b.rows); i++ {
			if i < len(b.rows) && b.series(i) == b.series(start) {
				continue
			}
			out = append(out, runsBatch{rows: b.rows[start:i], order: b.order})
			start = i
		}
	}
	return out
}

// runsLentDesc turns every single-series descending batch into the shape a lent
// block of a descending scan has: ascending storage and a reversed selection.
func runsLentDesc(batches []runsBatch) []runsBatch {
	out := make([]runsBatch, 0, len(batches))
	for _, b := range batches {
		if b.order != vectorized.TimeOrderDesc || !b.singleSeries() {
			out = append(out, b)
			continue
		}
		n := len(b.rows)
		rows := make([]runsRow, n)
		sel := make([]uint16, n)
		for i := range b.rows {
			rows[n-1-i] = b.rows[i]
			sel[i] = uint16(n - 1 - i)
		}
		out = append(out, runsBatch{rows: rows, selection: sel, order: vectorized.TimeOrderDesc})
	}
	return out
}

// runsFilter drops a random subset of the active rows through a selection.
func runsFilter(rng *rand.Rand, batches []runsBatch) []runsBatch {
	out := make([]runsBatch, 0, len(batches))
	for _, b := range batches {
		var active []uint16
		if b.selection != nil {
			active = b.selection
		} else {
			for i := range b.rows {
				active = append(active, uint16(i))
			}
		}
		if rng.Intn(2) == 0 {
			var kept []uint16
			for _, r := range active {
				if rng.Intn(3) != 0 {
					kept = append(kept, r)
				}
			}
			if kept == nil {
				kept = []uint16{}
			}
			b.selection = kept
		}
		out = append(out, b)
	}
	return out
}

// dumpBatch renders every active... column of every row of out, in storage order.
func dumpBatch(out *vectorized.RecordBatch) string {
	var sb strings.Builder
	for row := range out.Len {
		for c, col := range out.Columns {
			if c > 0 {
				sb.WriteByte('|')
			}
			sb.WriteString(dumpCell(col, row))
		}
		sb.WriteByte('\n')
	}
	return sb.String()
}

func dumpCell(col vectorized.Column, row int) string {
	if col.IsNull(row) {
		return "<null>"
	}
	switch c := col.(type) {
	case *vectorized.TypedColumn[int64]:
		return fmt.Sprintf("%d", c.Data()[row])
	case *vectorized.TypedColumn[float64]:
		return fmt.Sprintf("f%016x", math.Float64bits(c.Data()[row]))
	case *vectorized.TypedColumn[string]:
		return c.Data()[row]
	case *vectorized.TypedColumn[[]byte]:
		return fmt.Sprintf("%x", c.Data()[row])
	default:
		return fmt.Sprintf("?%T", col)
	}
}

func runsAggSpecs() []AggSpec {
	return []AggSpec{
		{Func: AggSum, InputCol: runsColVI, Output: "sum_i"},
		{Func: AggCount, InputCol: runsColVI, Output: "cnt_i"},
		{Func: AggMin, InputCol: runsColVI, Output: "min_i"},
		{Func: AggMax, InputCol: runsColVI, Output: "max_i"},
		{Func: AggMean, InputCol: runsColVI, Output: "mean_i"},
		{Func: AggSum, InputCol: runsColVF, Output: "sum_f"},
		{Func: AggCount, InputCol: runsColVF, Output: "cnt_f"},
		{Func: AggMin, InputCol: runsColVF, Output: "min_f"},
		{Func: AggMax, InputCol: runsColVF, Output: "max_f"},
		{Func: AggMean, InputCol: runsColVF, Output: "mean_f"},
		{Func: AggCountDistinct, InputCol: runsColVI, Output: "dist_i"},
		{Func: AggCount, InputCol: runsColName, Output: "cnt_name"},
		// An aggregate over an entity tag: a constant input in a single-series batch.
		{Func: AggSum, InputCol: runsColInst, Output: "sum_inst"},
		{Func: AggMean, InputCol: runsColInst, Output: "mean_inst"},
	}
}

// runAggregation feeds batches to a BatchAggregation and returns every emitted row.
func runAggregation(t *testing.T, schema *vectorized.BatchSchema, keys []int, mode AggMode,
	batches []runsBatch, fast bool,
) (string, int64) {
	t.Helper()
	defer SetRunsDisabled(SetRunsDisabled(!fast))
	op := NewBatchAggregation(schema, keys, runsAggSpecs(), mode, 16, vectorized.NewMemoryTracker(1<<30), 64)
	require.NoError(t, op.Init(context.Background()))
	defer op.Close()
	for i := range batches {
		b := batches[i].build(schema, fast)
		require.NoError(t, op.Consume(context.Background(), b), "consume batch %d", i)
	}
	require.NoError(t, op.Finalize(context.Background()))
	var sb strings.Builder
	for {
		out, err := op.NextBatch(context.Background())
		require.NoError(t, err)
		if out == nil {
			break
		}
		sb.WriteString(dumpBatch(out))
	}
	return sb.String(), op.runsFolded
}

func TestBatchAggregationRunsMatchPerRow(t *testing.T) {
	defer SetRunsVerification(SetRunsVerification(true))
	schema := runsSchema()
	keySets := map[string]struct {
		keys     []int
		wantRuns bool
	}{
		"scalar":   {keys: nil, wantRuns: true},
		"svc":      {keys: []int{runsColSvc}, wantRuns: true},
		"svc,inst": {keys: []int{runsColSvc, runsColInst}, wantRuns: true},
		"inst,svc": {keys: []int{runsColInst, runsColSvc}, wantRuns: true},
		"note":     {keys: []int{runsColNote}, wantRuns: false},
		"svc,note": {keys: []int{runsColSvc, runsColNote}, wantRuns: false},
	}
	shapes := map[string]func(rng *rand.Rand) []runsBatch{
		"asc pages": func(rng *rand.Rand) []runsBatch { return runsTimeline(rng, 600, false, 300) },
		"asc blocks": func(rng *rand.Rand) []runsBatch {
			return runsRegroup(runsTimeline(rng, 600, false, 300))
		},
		"desc pages": func(rng *rand.Rand) []runsBatch { return runsTimeline(rng, 600, true, 300) },
		"desc lent blocks": func(rng *rand.Rand) []runsBatch {
			return runsLentDesc(runsRegroup(runsTimeline(rng, 600, true, 300)))
		},
		"filtered blocks": func(rng *rand.Rand) []runsBatch {
			return runsFilter(rng, runsRegroup(runsTimeline(rng, 600, false, 300)))
		},
		"filtered desc lent blocks": func(rng *rand.Rand) []runsBatch {
			return runsFilter(rng, runsLentDesc(runsRegroup(runsTimeline(rng, 600, true, 300))))
		},
		"filtered pages": func(rng *rand.Rand) []runsBatch {
			return runsFilter(rng, runsTimeline(rng, 600, false, 300))
		},
	}
	for shapeName, shape := range shapes {
		for keyName, ks := range keySets {
			for _, mode := range []AggMode{AggModeAll, AggModeMap} {
				for seed := int64(1); seed <= 4; seed++ {
					t.Run(fmt.Sprintf("%s/%s/mode%d/seed%d", shapeName, keyName, mode, seed), func(t *testing.T) {
						batches := shape(seededRand(seed))
						want, wantRuns := runAggregation(t, schema, ks.keys, mode, batches, false)
						got, gotRuns := runAggregation(t, schema, ks.keys, mode, batches, true)
						require.Zero(t, wantRuns, "the per-row run must not use runs")
						require.NotEmpty(t, want)
						require.Equal(t, want, got)
						if ks.wantRuns {
							require.Positive(t, gotRuns, "the fast path must have been taken")
						} else {
							require.Zero(t, gotRuns, "keys that are not constant per run must fall back")
						}
					})
				}
			}
		}
	}
}

// runsSource is a PullOperator over prepared batches.
type runsSource struct {
	schema  *vectorized.BatchSchema
	batches []*vectorized.RecordBatch
}

func (s *runsSource) Init(context.Context) error            { return nil }
func (s *runsSource) OutputSchema() *vectorized.BatchSchema { return s.schema }
func (s *runsSource) Close() error                          { return nil }
func (s *runsSource) NextBatch(context.Context) (*vectorized.RecordBatch, error) {
	if len(s.batches) == 0 {
		return nil, nil
	}
	b := s.batches[0]
	s.batches = s.batches[1:]
	return b, nil
}

func runTimeBucket(t *testing.T, schema *vectorized.BatchSchema, keys []int, mode AggMode, streaming bool,
	batches []runsBatch, fast bool,
) (rows string, errText string, runBatches int64) {
	t.Helper()
	defer SetRunsDisabled(SetRunsDisabled(!fast))
	src := &runsSource{schema: schema}
	for i := range batches {
		src.batches = append(src.batches, batches[i].build(schema, fast))
	}
	bt := NewBatchTimeBucket(src, schema, keys, runsColTS, 500, runsAggSpecs(), mode, 16, vectorized.NewMemoryTracker(1<<30), 64, streaming)
	require.NoError(t, bt.Init(context.Background()))
	defer bt.Close()
	var sb strings.Builder
	for {
		out, err := bt.NextBatch(context.Background())
		if err != nil {
			return sb.String(), err.Error(), bt.runBatches
		}
		if out == nil {
			return sb.String(), "", bt.runBatches
		}
		sb.WriteString(dumpBatch(out))
	}
}

func TestBatchTimeBucketRunsMatchPerRow(t *testing.T) {
	defer SetRunsVerification(SetRunsVerification(true))
	schema := runsSchema()
	keySets := map[string][]int{
		"bucket only": nil,
		"svc":         {runsColSvc},
		"svc,inst":    {runsColSvc, runsColInst},
	}
	shapes := map[string]struct {
		build func(rng *rand.Rand) []runsBatch
		// streaming needs a time-ascending input; a descending one must fail the same way.
		streaming bool
	}{
		"asc pages":  {func(rng *rand.Rand) []runsBatch { return runsTimeline(rng, 500, false, 300) }, true},
		"asc blocks": {func(rng *rand.Rand) []runsBatch { return runsRegroup(runsTimeline(rng, 500, false, 300)) }, true},
		"asc filtered": {func(rng *rand.Rand) []runsBatch {
			return runsFilter(rng, runsRegroup(runsTimeline(rng, 500, false, 300)))
		}, true},
		"asc sparse":       {func(rng *rand.Rand) []runsBatch { return runsRegroup(runsTimeline(rng, 500, false, 4000)) }, true},
		"asc dense":        {func(rng *rand.Rand) []runsBatch { return runsRegroup(runsTimeline(rng, 500, false, 4)) }, true},
		"desc pages":       {func(rng *rand.Rand) []runsBatch { return runsTimeline(rng, 500, true, 300) }, false},
		"desc lent blocks": {func(rng *rand.Rand) []runsBatch { return runsLentDesc(runsRegroup(runsTimeline(rng, 500, true, 300))) }, false},
		"desc filtered": {func(rng *rand.Rand) []runsBatch {
			return runsFilter(rng, runsLentDesc(runsRegroup(runsTimeline(rng, 500, true, 300))))
		}, false},
	}
	for shapeName, shape := range shapes {
		for keyName, keys := range keySets {
			for _, mode := range []AggMode{AggModeAll, AggModeMap} {
				for _, streaming := range []bool{true, false} {
					for seed := int64(1); seed <= 3; seed++ {
						name := fmt.Sprintf("%s/%s/mode%d/streaming=%v/seed%d", shapeName, keyName, mode, streaming, seed)
						t.Run(name, func(t *testing.T) {
							batches := shape.build(seededRand(seed))
							wantRows, wantErr, wantRuns := runTimeBucket(t, schema, keys, mode, streaming, batches, false)
							gotRows, gotErr, gotRuns := runTimeBucket(t, schema, keys, mode, streaming, batches, true)
							require.Zero(t, wantRuns)
							require.Equal(t, wantErr, gotErr)
							require.Equal(t, wantRows, gotRows)
							if wantErr == "" {
								require.NotEmpty(t, wantRows)
								require.Positive(t, gotRuns, "the fast path must have been taken")
							}
						})
					}
				}
			}
		}
	}
}

func TestBatchTimeBucketRunsFallBack(t *testing.T) {
	schema := runsSchema()
	rng := seededRand(7)
	batches := runsRegroup(runsTimeline(rng, 200, false, 300))
	cases := map[string]func(b *vectorized.RecordBatch){
		"no time order":  func(b *vectorized.RecordBatch) { b.Hints.TimeOrder = vectorized.TimeOrderUnknown },
		"series unknown": func(b *vectorized.RecordBatch) { b.Hints.SeriesKnown = false },
		"null timestamp": func(b *vectorized.RecordBatch) { b.Columns[runsColTS].MarkNullAt(0) },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			src := &runsSource{schema: schema}
			for i := range batches {
				b := batches[i].build(schema, true)
				mutate(b)
				src.batches = append(src.batches, b)
			}
			bt := NewBatchTimeBucket(src, schema, []int{runsColSvc}, runsColTS, 500, runsAggSpecs(), AggModeAll, 16,
				vectorized.NewMemoryTracker(1<<30), 64, false)
			require.NoError(t, bt.Init(context.Background()))
			defer bt.Close()
			for {
				out, err := bt.NextBatch(context.Background())
				require.NoError(t, err)
				if out == nil {
					break
				}
			}
			require.Zero(t, bt.runBatches, "the hint is missing or unusable, so rows must be visited one by one")
		})
	}
}

func TestBucketRunEndMatchesLinearScan(t *testing.T) {
	rng := seededRand(3)
	for range 300 {
		n := 1 + rng.Intn(200)
		ts := make([]int64, n)
		cur := rng.Int63n(1000)
		for i := range ts {
			cur += rng.Int63n(120)
			ts[i] = cur
		}
		desc := rng.Intn(2) == 0
		if desc {
			for i, j := 0, n-1; i < j; i, j = i+1, j-1 {
				ts[i], ts[j] = ts[j], ts[i]
			}
		}
		var sel []uint16
		if rng.Intn(2) == 0 {
			sel = make([]uint16, 0, n)
			for i := range ts {
				if rng.Intn(3) != 0 {
					sel = append(sel, uint16(i))
				}
			}
		}
		active := n
		if sel != nil {
			active = len(sel)
		}
		if active == 0 {
			continue
		}
		from := rng.Intn(active)
		bucket := bucketStart(ts[rowAt(sel, from)], 100)
		want := from
		for want < active && bucketStart(ts[rowAt(sel, want)], 100) == bucket {
			want++
		}
		require.Equal(t, want, bucketRunEnd(ts, sel, from, active, bucket, 100))
	}
}

// TestRunsFallBackWhenSeriesInterleaveRowByRow checks that a batch whose series change
// every row or two is folded row by row: a run would cost as much as a row there.
func TestRunsFallBackWhenSeriesInterleaveRowByRow(t *testing.T) {
	schema := runsSchema()
	rows := make([]runsRow, 0, 60)
	for i := range 60 {
		rows = append(rows, runsRow{ts: int64(1000 + 10*i), svc: fmt.Sprintf("svc-%d", i%3), inst: int64(i % 2), vi: int64(i), vf: float64(i) / 3})
	}
	batches := []runsBatch{{rows: rows, order: vectorized.TimeOrderAsc}}
	want, _ := runAggregation(t, schema, []int{runsColSvc}, AggModeAll, batches, false)
	got, runs := runAggregation(t, schema, []int{runsColSvc}, AggModeAll, batches, true)
	require.Equal(t, want, got)
	require.Zero(t, runs, "a batch that changes series every row is folded row by row")

	wantRows, wantErr, _ := runTimeBucket(t, schema, []int{runsColSvc}, AggModeAll, true, batches, false)
	gotRows, gotErr, runBatches := runTimeBucket(t, schema, []int{runsColSvc}, AggModeAll, true, batches, true)
	require.Equal(t, wantErr, gotErr)
	require.Equal(t, wantRows, gotRows)
	require.Zero(t, runBatches)
}

// seededRand returns a generator with a fixed seed, so a generated scenario is reproducible.
func seededRand(seed int64) *rand.Rand {
	return rand.New(rand.NewSource(seed)) //nolint:gosec // a fixed seed makes the generated rows reproducible
}
