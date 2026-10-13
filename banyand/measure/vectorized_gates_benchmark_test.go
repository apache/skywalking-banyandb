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
	"os"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	vmeasure "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

// Storage-backed G9e workloads (W1..W8).
//
// Every workload starts from a real flushed measure part and runs the
// production read path inside the timer: tstIter block scan -> blockCursor
// decode (loadData + copyAllToBatch / mergeBatch through a real queryResult,
// the same PullBatch the data node serves) -> vectorized pipeline -> egress
// (NewIteratorFromPipeline). Fixture generation (writing and flushing the part)
// happens once, outside the timer, from closed-form deterministic data, so no
// random seed is involved and every run decodes identical bytes.
//
// The older prebuilt-proto harness (pkg/query/vectorized/measure
// BenchmarkRowPath_W*/BenchmarkVectorizedPath_W* and TestBenchGates_PerWorkload)
// is retired: it handed both sides pre-built *modelv1.TagValue wrappers, so
// storage decode was free and native columns were charged the egress wrapper
// rebuild while the decode cost they remove was hidden. Its "row path" half was
// also a test-local replica of a production path that no longer exists.
//
//	go test ./banyand/measure -run '^$' -bench BenchmarkVectorizedStorage -benchmem -count=6
//	BYDB_BENCH_LEGACY_SCHEMA=1 go test ... (previous passthrough column selection)
//	RUN_BENCH_GATES=1 go test ./banyand/measure -run '^TestVectorizedStorageGates$' -count=1 -v

const (
	gateBatchSize = 1024
	gateGroupCard = 16
	gateTopN      = 50
	gateLimitN    = 100
	gateTSTag     = "svc"
	gateTagEnv    = "env_id"
	gateTagCrit   = "criteria"
	gateFieldInt  = "v_int"
	gateFieldFlt  = "v_float"
	gateFamily    = "default"
)

type gateShape int

const (
	gateShapeScan gateShape = iota
	gateShapeTop
	gateShapeReduce // scalar reduce, raw group-by, group-by + agg
	gateShapeHidden
)

// gateLayout is the stored data shape; workloads sharing a layout share one
// flushed part.
type gateLayout struct {
	name    string
	series  int
	rowsPer int
}

var (
	gateLayoutSingle = gateLayout{name: "single_10k", series: 1, rowsPer: 10000}
	gateLayoutMulti  = gateLayout{name: "multi_100x1k", series: 100, rowsPer: 1000}
	gateLayoutGroups = gateLayout{name: "groups_16x6250", series: gateGroupCard, rowsPer: 6250}
)

type gateWorkload struct {
	id       string
	topField string
	hidden   string
	layout   gateLayout
	opts     model.MeasureQueryOptions
	topN     int
	limitN   int
	wantRows int
	shape    gateShape
	topAsc   bool
}

func gateProj(tags ...string) []model.TagProjection {
	if len(tags) == 0 {
		return nil
	}
	return []model.TagProjection{{Family: gateFamily, Names: tags}}
}

var (
	gateGroupBy = &model.MeasureGroupBy{TagFamily: gateFamily, TagNames: []string{gateTSTag}}
	gateSum     = &model.MeasureAgg{FieldName: gateFieldInt, Func: modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM}
	gateCount   = &model.MeasureAgg{FieldName: gateFieldFlt, Func: modelv1.AggregationFunction_AGGREGATION_FUNCTION_COUNT}
)

// gateWorkloads keeps the W1..W8 shapes of the retired harness: W1 single-series
// scan, W2 multi-tag multi-series scan, W3 top-N over a scan, W4 scalar reduce,
// W5 raw group-by, W6 group-by + SUM, W7 hidden criteria tag stripped at egress,
// W8 group-by + COUNT over a float field.
func gateWorkloads() []gateWorkload {
	return []gateWorkload{
		{
			id: "W1", layout: gateLayoutSingle, shape: gateShapeScan, wantRows: 10000,
			opts: model.MeasureQueryOptions{FieldProjection: []string{gateFieldInt}},
		},
		{
			id: "W2", layout: gateLayoutMulti, shape: gateShapeScan, wantRows: 100000,
			opts: model.MeasureQueryOptions{
				TagProjection: gateProj(gateTSTag, gateTagEnv), FieldProjection: []string{gateFieldInt, gateFieldFlt},
			},
		},
		{
			id: "W3", layout: gateLayoutMulti, shape: gateShapeTop, wantRows: gateTopN,
			topField: gateFieldInt, topN: gateTopN, limitN: gateLimitN,
			opts: model.MeasureQueryOptions{TagProjection: gateProj(gateTSTag), FieldProjection: []string{gateFieldInt}},
		},
		{
			id: "W4", layout: gateLayoutMulti, shape: gateShapeReduce, wantRows: 1, limitN: gateLimitN,
			opts: model.MeasureQueryOptions{
				TagProjection: gateProj(gateTSTag), FieldProjection: []string{gateFieldInt}, Agg: gateSum,
			},
		},
		{
			id: "W5", layout: gateLayoutGroups, shape: gateShapeReduce, wantRows: gateGroupCard, limitN: gateLimitN,
			opts: model.MeasureQueryOptions{
				TagProjection: gateProj(gateTSTag), FieldProjection: []string{gateFieldInt}, GroupBy: gateGroupBy,
			},
		},
		{
			id: "W6", layout: gateLayoutGroups, shape: gateShapeReduce, wantRows: gateGroupCard, limitN: gateLimitN,
			opts: model.MeasureQueryOptions{
				TagProjection: gateProj(gateTSTag), FieldProjection: []string{gateFieldInt}, GroupBy: gateGroupBy, Agg: gateSum,
			},
		},
		{
			id: "W7", layout: gateLayoutMulti, shape: gateShapeHidden, wantRows: 100000, hidden: gateTagCrit,
			opts: model.MeasureQueryOptions{
				TagProjection: gateProj(gateTSTag, gateTagCrit), FieldProjection: []string{gateFieldInt},
			},
		},
		{
			id: "W8", layout: gateLayoutGroups, shape: gateShapeReduce, wantRows: gateGroupCard, limitN: gateLimitN,
			opts: model.MeasureQueryOptions{
				TagProjection: gateProj(gateTSTag), FieldProjection: []string{gateFieldFlt}, GroupBy: gateGroupBy, Agg: gateCount,
			},
		},
	}
}

func gateMeasure() *databasev1.Measure {
	return &databasev1.Measure{
		TagFamilies: []*databasev1.TagFamilySpec{{Name: gateFamily, Tags: []*databasev1.TagSpec{
			{Name: gateTSTag, Type: databasev1.TagType_TAG_TYPE_STRING},
			{Name: gateTagEnv, Type: databasev1.TagType_TAG_TYPE_INT},
			{Name: gateTagCrit, Type: databasev1.TagType_TAG_TYPE_STRING},
		}}},
		Fields: []*databasev1.FieldSpec{
			{Name: gateFieldInt, FieldType: databasev1.FieldType_FIELD_TYPE_INT},
			{Name: gateFieldFlt, FieldType: databasev1.FieldType_FIELD_TYPE_FLOAT},
		},
	}
}

var gateTagTypes = map[string]pbv1.ValueType{
	gateTSTag: pbv1.ValueTypeStr, gateTagEnv: pbv1.ValueTypeInt64, gateTagCrit: pbv1.ValueTypeStr,
}

// gateHarness is one flushed part holding a gateLayout.
type gateHarness struct {
	parts  []*part
	sids   []common.SeriesID
	layout gateLayout
}

// setupGateHarness writes the layout through the real write path and waits for
// the part to be flushed. Values are closed-form in (series, row), so the
// fixture is byte-identical on every run.
func setupGateHarness(tb testing.TB, layout gateLayout) *gateHarness {
	tb.Helper()
	tmpPath, defFn := test.Space(require.New(tb))
	tst, err := newTSTable(
		fs.NewLocalFileSystem(),
		tmpPath, common.Position{},
		logger.GetLogger("bench"),
		timestamp.TimeRange{},
		option{flushTimeout: 0, mergePolicy: newDefaultMergePolicyForTesting(), protector: protector.Nop{}},
		nil,
	)
	require.NoError(tb, err)
	total := layout.series * layout.rowsPer
	points := &dataPoints{
		seriesIDs:   make([]common.SeriesID, total),
		timestamps:  make([]int64, total),
		versions:    make([]int64, total),
		tagFamilies: make([][]nameValues, total),
		fields:      make([]nameValues, total),
	}
	for s := range layout.series {
		for r := range layout.rowsPer {
			i := s*layout.rowsPer + r
			points.seriesIDs[i] = common.SeriesID(s + 1)
			points.timestamps[i] = int64(r + 1)
			points.versions[i] = 1
			points.tagFamilies[i] = []nameValues{{name: gateFamily, values: []*nameValue{
				{name: gateTSTag, valueType: pbv1.ValueTypeStr, value: []byte(fmt.Sprintf("svc-%d", s%gateGroupCard))},
				{name: gateTagEnv, valueType: pbv1.ValueTypeInt64, value: convert.Int64ToBytes(int64(s + 1))},
				{name: gateTagCrit, valueType: pbv1.ValueTypeStr, value: []byte(fmt.Sprintf("crit-%d", (r+s)%26))},
			}}}
			points.fields[i] = nameValues{name: "skipped", values: []*nameValue{
				{name: gateFieldInt, valueType: pbv1.ValueTypeInt64, value: convert.Int64ToBytes(int64(r*7 + s))},
				{name: gateFieldFlt, valueType: pbv1.ValueTypeFloat64, value: convert.Float64ToBytes(float64(r) * 0.25)},
			}}
		}
	}
	tst.mustAddDataPoints(points)
	time.Sleep(300 * time.Millisecond)
	snap := tst.currentSnapshot()
	require.NotNil(tb, snap)
	parts, _ := snap.getParts(nil, storage.NewShardCache("gate-group", 0, 0), 0, int64(layout.rowsPer)+1)
	require.NotEmpty(tb, parts)
	sids := make([]common.SeriesID, layout.series)
	for i := range sids {
		sids[i] = common.SeriesID(i + 1)
	}
	tb.Cleanup(func() {
		snap.decRef()
		tst.Close()
		defFn()
	})
	return &gateHarness{parts: parts, sids: sids, layout: layout}
}

// newQueryResult builds a real queryResult over every block of the part, the way
// measure.searchBlocks does, so PullBatch runs the production load / merge /
// copy code.
func (h *gateHarness) newQueryResult(schema *vectorized.BatchSchema, opts model.MeasureQueryOptions) *queryResult {
	qo := queryOptions{
		schemaTagTypes: gateTagTypes,
		minTimestamp:   0,
		maxTimestamp:   int64(h.layout.rowsPer) + 1,
	}
	qo.MeasureQueryOptions.TagProjection = opts.TagProjection
	qo.MeasureQueryOptions.FieldProjection = opts.FieldProjection
	qr := &queryResult{ctx: context.Background(), batchSchema: schema, tagProjection: opts.TagProjection}
	queryResultTracker.Acquire(qr)
	ti := generateTstIter()
	defer releaseTstIter(ti)
	// releaseTstIter clears the slice it was handed, so give it a per-query copy
	// (searchBlocks builds a fresh parts slice per query as well).
	ti.init(slices.Clone(h.parts), h.sids, qo.minTimestamp, qo.maxTimestamp)
	for ti.nextBlock() {
		bc := generateBlockCursor()
		p := ti.piHeap[0]
		bc.init(p.p, p.curBlock, qo)
		qr.data = append(qr.data, bc)
	}
	if ti.Error() != nil {
		panic(ti.Error())
	}
	return qr
}

func gateSchema(legacy bool, opts model.MeasureQueryOptions) *vectorized.BatchSchema {
	schema, err := vmeasure.BuildBatchSchema(gateMeasure(), opts)
	if err != nil {
		panic(err)
	}
	if legacy {
		return passthroughSchema(schema, opts)
	}
	return schema
}

// runGateWorkload runs one full query over the stored part and returns the
// number of data points the egress adapter emitted.
func runGateWorkload(h *gateHarness, w gateWorkload, legacy bool) int {
	schema := gateSchema(legacy, w.opts)
	qr := h.newQueryResult(schema, w.opts)
	pool := vectorized.NewBatchPool(schema, gateBatchSize)
	source := vmeasure.NewBatchSourceFromBatchResult(qr, schema, pool, gateBatchSize)
	tracker := vectorized.NewMemoryTracker(64 * 1024 * 1024)
	builder := vectorized.NewPipelineBuilder().WithMemoryTracker(tracker).From(source)
	terminal := schema
	switch w.shape {
	case gateShapeScan, gateShapeHidden:
	case gateShapeTop:
		idx, ok := schema.FieldIndex(w.topField)
		if !ok {
			panic("gate: top field not in schema")
		}
		builder.Break(vmeasure.NewBatchTop(schema, idx, w.topN, w.topAsc, gateBatchSize))
	case gateShapeReduce:
		ops, err := vmeasure.BuildOperators(w.opts, schema, tracker, gateBatchSize, vmeasure.AggModeAll)
		if err != nil {
			panic(err)
		}
		builder.Break(ops[0])
		terminal = ops[0].OutputSchema()
	}
	if w.limitN > 0 {
		builder.Apply(vmeasure.NewBatchLimit(terminal, 0, uint32(w.limitN)))
	}
	pipeline, err := builder.Build()
	if err != nil {
		panic(err)
	}
	if err = pipeline.Init(context.Background()); err != nil {
		panic(err)
	}
	it := vmeasure.NewIteratorFromPipeline(context.Background(), pipeline, terminal, vectorized.NewBatchPool(terminal, gateBatchSize))
	rows := 0
	var hidden logical.HiddenTagSet
	if w.hidden != "" {
		hidden = logical.NewHiddenTagSet()
		hidden.Add(w.hidden)
	}
	for it.Next() {
		for _, dp := range it.Current() {
			if hidden != nil {
				dp.DataPoint.TagFamilies = hidden.StripHiddenTags(dp.DataPoint.TagFamilies)
			}
			rows++
		}
	}
	if err = it.Close(); err != nil {
		panic(err)
	}
	return rows
}

var gateSink int

func benchGateWorkload(b *testing.B, h *gateHarness, w gateWorkload, legacy bool) {
	b.ReportAllocs()
	b.ResetTimer()
	sink := 0
	for b.Loop() {
		sink += runGateWorkload(h, w, legacy)
	}
	gateSink = sink
}

// gateHarnesses lazily builds one harness per layout and shares it across
// workloads; the owner test or benchmark tears them down via tb.Cleanup.
type gateHarnesses struct {
	tb testing.TB
	by map[string]*gateHarness
	mu sync.Mutex
}

func newGateHarnesses(tb testing.TB) *gateHarnesses {
	return &gateHarnesses{tb: tb, by: map[string]*gateHarness{}}
}

func (g *gateHarnesses) get(layout gateLayout) *gateHarness {
	g.mu.Lock()
	defer g.mu.Unlock()
	if h, ok := g.by[layout.name]; ok {
		return h
	}
	h := setupGateHarness(g.tb, layout)
	g.by[layout.name] = h
	return h
}

// BenchmarkVectorizedStorage runs W1..W8 over real stored parts with the column
// selection chosen by BYDB_BENCH_LEGACY_SCHEMA (unset: native default; 1: the
// previous passthrough selection).
func BenchmarkVectorizedStorage(b *testing.B) {
	legacy := os.Getenv("BYDB_BENCH_LEGACY_SCHEMA") == "1"
	hs := newGateHarnesses(b)
	for _, w := range gateWorkloads() {
		b.Run(w.id, func(b *testing.B) { benchGateWorkload(b, hs.get(w.layout), w, legacy) })
	}
}

// gate describes what one workload must satisfy. The baseline is the same
// workload run in the same process with the previous column selection
// (passthrough columns for everything but GroupBy keys and the Agg target, the
// BYDB_BENCH_LEGACY_SCHEMA=1 selection), over the same stored part:
//
//   - maxNsRatio: native ns/op <= legacy ns/op * ratio. Wall time is noisy
//     (shared hosts, GC, goroutine scheduling in the parallel block loader), so
//     each side is the minimum of gateRounds interleaved runs and the ratio only
//     has to catch a real slowdown, not a 1% wobble.
//   - maxAllocRatio / maxBytesRatio: native allocs/op and B/op <= legacy * ratio.
//     Allocation counts are deterministic, so these are tight. For workloads where
//     native decode removes per-cell wrappers (W1-W5, W7) the ratio sits just above
//     the measured win; a regression back to passthrough (or a lost zero-copy
//     borrow) lands at ratio ~1.0 and trips it. W6 / W8 already run native
//     GroupBy/Agg columns in both selections, so for them the ratio only pins
//     "no worse".
//   - allocBudget: checked-in absolute allocs/op of the native path, with
//     gateAbsTolerance. This is what catches a regression that hits both
//     selections (for example an extra per-row allocation in egress or the
//     operators), which a ratio against the same code cannot see. Re-baseline it
//     deliberately when a change legitimately moves the number.
type gate struct {
	maxNsRatio    float64
	maxAllocRatio float64
	maxBytesRatio float64
	allocBudget   int64
}

const (
	gateRounds       = 3
	gateAbsTolerance = 1.05
)

// Measured on the reference run recorded with the change that introduced the
// gates (AMD EPYC 7B13, go1.26.9): native / legacy allocs
// W1 0.75, W2 0.64, W3 0.05, W4 0.01, W5 0.01, W6 1.00, W7 0.70, W8 1.00.
var gates = map[string]gate{
	// Scans and top-N: native decode removes per-cell wrappers; ns wins too.
	"W1": {maxNsRatio: 1.10, maxAllocRatio: 0.80, maxBytesRatio: 1.05, allocBudget: 60200},
	"W2": {maxNsRatio: 1.10, maxAllocRatio: 0.68, maxBytesRatio: 1.05, allocBudget: 1405700},
	"W3": {maxNsRatio: 1.10, maxAllocRatio: 0.06, maxBytesRatio: 0.33, allocBudget: 31800},
	// Scalar reduce and raw group-by: decode wrappers vanish (allocs ~0.01x); time is
	// decode/IO bound so ns is a tie.
	"W4": {maxNsRatio: 1.15, maxAllocRatio: 0.03, maxBytesRatio: 0.55, allocBudget: 4400},
	"W5": {maxNsRatio: 1.10, maxAllocRatio: 0.03, maxBytesRatio: 0.55, allocBudget: 3000},
	// GroupBy+Agg already used native columns in both selections: pin "no worse".
	"W6": {maxNsRatio: 1.15, maxAllocRatio: 1.02, maxBytesRatio: 1.15, allocBudget: 2800},
	"W7": {maxNsRatio: 1.10, maxAllocRatio: 0.75, maxBytesRatio: 1.05, allocBudget: 1406500},
	"W8": {maxNsRatio: 1.15, maxAllocRatio: 1.02, maxBytesRatio: 1.15, allocBudget: 2800},
}

type gateStats struct {
	ns     int64
	bytes  int64
	allocs int64
}

// measureGate returns the best (minimum) of gateRounds interleaved native and
// legacy runs of one workload.
func measureGate(h *gateHarness, w gateWorkload) (native, legacy gateStats) {
	best := func(cur gateStats, r testing.BenchmarkResult) gateStats {
		got := gateStats{ns: r.NsPerOp(), bytes: r.AllocedBytesPerOp(), allocs: r.AllocsPerOp()}
		if cur.ns == 0 || got.ns < cur.ns {
			cur.ns = got.ns
		}
		if cur.bytes == 0 || got.bytes < cur.bytes {
			cur.bytes = got.bytes
		}
		if cur.allocs == 0 || got.allocs < cur.allocs {
			cur.allocs = got.allocs
		}
		return cur
	}
	for range gateRounds {
		native = best(native, testing.Benchmark(func(b *testing.B) { benchGateWorkload(b, h, w, false) }))
		legacy = best(legacy, testing.Benchmark(func(b *testing.B) { benchGateWorkload(b, h, w, true) }))
	}
	return native, legacy
}

// TestVectorizedStorageGates enforces the gates above over real stored parts. It
// is skipped unless RUN_BENCH_GATES=1 (about a minute of wall time).
func TestVectorizedStorageGates(t *testing.T) {
	if os.Getenv("RUN_BENCH_GATES") != "1" {
		t.Skip("set RUN_BENCH_GATES=1 to run the G9e storage-backed bench gates")
	}
	if testing.Short() {
		t.Skip("skipping bench gates in -short mode")
	}
	hs := newGateHarnesses(t)
	for _, w := range gateWorkloads() {
		t.Run(w.id, func(t *testing.T) {
			g, ok := gates[w.id]
			require.True(t, ok, "no gate defined for %s", w.id)
			h := hs.get(w.layout)
			// Both selections must return the same number of data points: a gate
			// that passes because the query silently returned less is worthless.
			require.Equal(t, w.wantRows, runGateWorkload(h, w, false), "native rows")
			require.Equal(t, w.wantRows, runGateWorkload(h, w, true), "legacy rows")

			nat, leg := measureGate(h, w)
			t.Logf("%s native: %d ns/op, %d B/op, %d allocs/op", w.id, nat.ns, nat.bytes, nat.allocs)
			t.Logf("%s legacy: %d ns/op, %d B/op, %d allocs/op", w.id, leg.ns, leg.bytes, leg.allocs)
			t.Logf("%s ratios: ns %.3f, allocs %.3f, bytes %.3f", w.id,
				float64(nat.ns)/float64(leg.ns), float64(nat.allocs)/float64(leg.allocs), float64(nat.bytes)/float64(leg.bytes))

			if !ratioLE(nat.ns, leg.ns, g.maxNsRatio) {
				t.Errorf("%s ns/op gate: native %d > legacy %d x %.2f", w.id, nat.ns, leg.ns, g.maxNsRatio)
			}
			if !ratioLE(nat.allocs, leg.allocs, g.maxAllocRatio) {
				t.Errorf("%s allocs/op gate: native %d > legacy %d x %.3f", w.id, nat.allocs, leg.allocs, g.maxAllocRatio)
			}
			if !ratioLE(nat.bytes, leg.bytes, g.maxBytesRatio) {
				t.Errorf("%s B/op gate: native %d > legacy %d x %.3f", w.id, nat.bytes, leg.bytes, g.maxBytesRatio)
			}
			if !ratioLE(nat.allocs, g.allocBudget, gateAbsTolerance) {
				t.Errorf("%s absolute allocs/op budget: native %d > %d x %.2f", w.id, nat.allocs, g.allocBudget, gateAbsTolerance)
			}
		})
	}
}

// ratioLE reports whether got <= baseline * ratio; a non-positive baseline
// cannot form a ratio and passes.
func ratioLE(got, baseline int64, ratio float64) bool {
	if baseline <= 0 {
		return true
	}
	return float64(got) <= float64(baseline)*ratio
}
