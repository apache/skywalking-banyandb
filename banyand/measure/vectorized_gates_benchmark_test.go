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
	"runtime"
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
//	BYDB_BENCH_LEGACY_SCAN=1 go test ...   (decode every block, then merge row by row)
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

// gateParts says how a layout's rows are spread over parts.
type gateParts int

const (
	// gatePartsOne flushes every row through the write path into one part.
	gatePartsOne gateParts = iota
	// gatePartsDisjoint writes gatePartCount memory parts, each holding a
	// disjoint slice of every series' time range: one block per part and series,
	// none overlapping.
	gatePartsDisjoint
	// gatePartsOverlap writes two memory parts that overlap in the middle 40% of
	// every series' time range; the later part carries a higher version.
	gatePartsOverlap
)

const gatePartCount = 4

// gateLayout is the stored data shape; workloads sharing a layout share one
// stored data set.
type gateLayout struct {
	name    string
	parts   gateParts
	series  int
	rowsPer int
}

var (
	gateLayoutSingle   = gateLayout{name: "single_10k", series: 1, rowsPer: 10000}
	gateLayoutMulti    = gateLayout{name: "multi_100x1k", series: 100, rowsPer: 1000}
	gateLayoutGroups   = gateLayout{name: "groups_16x6250", series: gateGroupCard, rowsPer: 6250}
	gateLayoutDisjoint = gateLayout{name: "multi_100x1k_4parts_disjoint", parts: gatePartsDisjoint, series: 100, rowsPer: 1000}
	gateLayoutOverlap  = gateLayout{name: "multi_100x1k_2parts_overlap", parts: gatePartsOverlap, series: 100, rowsPer: 1000}
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
	// timeOrdered runs the query without ORDER BY: rows ordered by time across series.
	timeOrdered bool
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
		// W9 and W10 read the W2 projection over several parts per series: W9 where
		// every series' blocks are time-disjoint (the zero-copy segments), W10 where
		// they overlap and every series takes the row merge.
		{
			id: "W9", layout: gateLayoutDisjoint, shape: gateShapeScan, wantRows: 100000,
			opts: model.MeasureQueryOptions{
				TagProjection: gateProj(gateTSTag, gateTagEnv), FieldProjection: []string{gateFieldInt, gateFieldFlt},
			},
		},
		{
			id: "W10", layout: gateLayoutOverlap, shape: gateShapeScan, wantRows: 100000,
			opts: model.MeasureQueryOptions{
				TagProjection: gateProj(gateTSTag, gateTagEnv), FieldProjection: []string{gateFieldInt, gateFieldFlt},
			},
		},
		// W11 and W12 are W10 and W9 as OAP issues them: no ORDER BY, so the rows of
		// the 100 series that share one time range interleave by timestamp, through
		// the time-ordered merge (blocks overlapping in time, and time-disjoint).
		{
			id: "W11", layout: gateLayoutOverlap, shape: gateShapeScan, wantRows: 100000, timeOrdered: true,
			opts: model.MeasureQueryOptions{
				TagProjection: gateProj(gateTSTag, gateTagEnv), FieldProjection: []string{gateFieldInt, gateFieldFlt},
			},
		},
		{
			id: "W12", layout: gateLayoutDisjoint, shape: gateShapeScan, wantRows: 100000, timeOrdered: true,
			opts: model.MeasureQueryOptions{
				TagProjection: gateProj(gateTSTag, gateTagEnv), FieldProjection: []string{gateFieldInt, gateFieldFlt},
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
	parts  []partRef
	sids   []common.SeriesID
	layout gateLayout
}

// gateDataPoints builds rows [rowLo, rowHi) of every series of the layout at the
// given version. Values are closed-form in (series, row), so a fixture is
// byte-identical on every run; the version only differs in overlapping layouts.
func gateDataPoints(layout gateLayout, rowLo, rowHi int, version int64) *dataPoints {
	total := layout.series * (rowHi - rowLo)
	points := &dataPoints{
		seriesIDs:   make([]common.SeriesID, 0, total),
		timestamps:  make([]int64, 0, total),
		versions:    make([]int64, 0, total),
		tagFamilies: make([][]nameValues, 0, total),
		fields:      make([]nameValues, 0, total),
	}
	for s := range layout.series {
		for r := rowLo; r < rowHi; r++ {
			points.seriesIDs = append(points.seriesIDs, common.SeriesID(s+1))
			points.timestamps = append(points.timestamps, int64(r+1))
			points.versions = append(points.versions, version)
			points.tagFamilies = append(points.tagFamilies, []nameValues{{name: gateFamily, values: []*nameValue{
				{name: gateTSTag, valueType: pbv1.ValueTypeStr, value: []byte(fmt.Sprintf("svc-%d", s%gateGroupCard))},
				{name: gateTagEnv, valueType: pbv1.ValueTypeInt64, value: convert.Int64ToBytes(int64(s + 1))},
				{name: gateTagCrit, valueType: pbv1.ValueTypeStr, value: []byte(fmt.Sprintf("crit-%d", (r+s)%26))},
			}}})
			points.fields = append(points.fields, nameValues{name: "skipped", values: []*nameValue{
				{name: gateFieldInt, valueType: pbv1.ValueTypeInt64, value: convert.Int64ToBytes(int64(r*7 + s))},
				{name: gateFieldFlt, valueType: pbv1.ValueTypeFloat64, value: convert.Float64ToBytes(float64(r) * 0.25)},
			}})
		}
	}
	return points
}

// setupGateHarness stores the layout: through the real write path and a flush for
// a single part, or as memory parts written by the part writer for the
// multi-part layouts (whose point is how many parts a series spans, not the
// flush).
func setupGateHarness(tb testing.TB, layout gateLayout) *gateHarness {
	tb.Helper()
	sids := make([]common.SeriesID, layout.series)
	for i := range sids {
		sids[i] = common.SeriesID(i + 1)
	}
	if layout.parts != gatePartsOne {
		return &gateHarness{parts: gateMemParts(layout), sids: sids, layout: layout}
	}
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
	tst.mustAddDataPoints(gateDataPoints(layout, 0, layout.rowsPer, 1))
	time.Sleep(300 * time.Millisecond)
	snap := tst.currentSnapshot()
	require.NotNil(tb, snap)
	parts, _ := snap.getParts(nil, storage.NewShardCache("gate-group", 0, 0), 0, 0, int64(layout.rowsPer)+1)
	require.NotEmpty(tb, parts)
	tb.Cleanup(func() {
		snap.decRef()
		tst.Close()
		defFn()
	})
	return &gateHarness{parts: parts, sids: sids, layout: layout}
}

// gateMemParts writes a multi-part layout as memory parts, oldest first.
func gateMemParts(layout gateLayout) []partRef {
	var spans []struct {
		lo, hi  int
		version int64
	}
	switch layout.parts {
	case gatePartsDisjoint:
		step := layout.rowsPer / gatePartCount
		for k := range gatePartCount {
			spans = append(spans, struct {
				lo, hi  int
				version int64
			}{k * step, (k + 1) * step, 1})
		}
	case gatePartsOverlap:
		spans = append(spans,
			struct {
				lo, hi  int
				version int64
			}{0, layout.rowsPer * 6 / 10, 1},
			struct {
				lo, hi  int
				version int64
			}{layout.rowsPer * 4 / 10, layout.rowsPer, 2},
		)
	case gatePartsOne:
	}
	parts := make([]partRef, 0, len(spans))
	for i, sl := range spans {
		mp := generateMemPart()
		mp.mustInitFromDataPoints(gateDataPoints(layout, sl.lo, sl.hi, sl.version))
		p := openMemPart(mp)
		p.partMetadata.ID = uint64(i + 1)
		parts = append(parts, partRef{p: p, cache: storage.NewShardCache("gate-group", 0, 0)})
	}
	return parts
}

// newQueryResult builds a real queryResult over every block of the part, the way
// measure.searchBlocks does, so PullBatch runs the production load / merge /
// copy code. The query is ordered by series (the order the sids were asked in)
// unless timeOrdered is set: that is the default of a query without ORDER BY, whose
// rows interleave the series by timestamp (the OAP shape) and take the merge.
func (h *gateHarness) newQueryResult(schema *vectorized.BatchSchema, opts model.MeasureQueryOptions, timeOrdered bool) *queryResult {
	qo := queryOptions{
		schemaTagTypes: gateTagTypes,
		minTimestamp:   0,
		maxTimestamp:   int64(h.layout.rowsPer) + 1,
	}
	qo.MeasureQueryOptions.TagProjection = opts.TagProjection
	qo.MeasureQueryOptions.FieldProjection = opts.FieldProjection
	qr := &queryResult{
		ctx: context.Background(), batchSchema: schema, tagProjection: opts.TagProjection, dp: gateDecodePool(),
		sidToIndex: make(map[common.SeriesID]int, len(h.sids)), orderByTS: timeOrdered, ascTS: timeOrdered,
	}
	for i, sid := range h.sids {
		qr.sidToIndex[sid] = i
	}
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

var (
	gatePool     *decodePool
	gatePoolOnce sync.Once
)

// gateDecodePool is the node-wide decode pool the gate queries share, sized like a
// data node's (GOMAXPROCS), as the harness builds queryResults without a service.
func gateDecodePool() *decodePool {
	gatePoolOnce.Do(func() { gatePool = newDecodePool(runtime.GOMAXPROCS(0), nil) })
	return gatePool
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
	qr := h.newQueryResult(schema, w.opts, w.timeOrdered)
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
	if os.Getenv("BYDB_BENCH_LEGACY_SCAN") == "1" {
		defer setLegacyBatchScan(setLegacyBatchScan(true))
	}
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

// Absolute budgets of W3-W6 and W8 were lowered with the series-window scan (step 4a
// of the scan path v2 design): the harness now orders the query by series like
// production does (sidToIndex was nil, which made the legacy merge interleave every
// series by timestamp), and a query ordered by series is served by whole-series
// windows. W3 31800 -> 5500, W4 4400 -> 2700, W5 3000 -> 1000, W6 and W8 2800 -> 850;
// measured 5436, 2644, 980, 826 and 827. W9 and W10 are new (several parts per
// series, disjoint and overlapping).
//
// Measured on the reference run recorded with the change that introduced the
// gates (AMD EPYC 7B13, go1.26.9): native / legacy allocs
// W1 0.75, W2 0.64, W3 0.05, W4 0.01, W5 0.01, W6 1.00, W7 0.70, W8 1.00.
var gates = map[string]gate{
	// Scans and top-N: native decode removes per-cell wrappers; ns wins too.
	"W1": {maxNsRatio: 1.10, maxAllocRatio: 0.80, maxBytesRatio: 1.05, allocBudget: 60200},
	"W2": {maxNsRatio: 1.10, maxAllocRatio: 0.68, maxBytesRatio: 1.05, allocBudget: 1405700},
	"W3": {maxNsRatio: 1.10, maxAllocRatio: 0.06, maxBytesRatio: 0.33, allocBudget: 5500},
	// Scalar reduce and raw group-by: decode wrappers vanish (allocs ~0.01x); time is
	// decode/IO bound so ns is a tie.
	"W4": {maxNsRatio: 1.15, maxAllocRatio: 0.03, maxBytesRatio: 0.55, allocBudget: 2700},
	"W5": {maxNsRatio: 1.10, maxAllocRatio: 0.03, maxBytesRatio: 0.55, allocBudget: 1000},
	// GroupBy+Agg already used native columns in both selections: pin "no worse".
	"W6": {maxNsRatio: 1.15, maxAllocRatio: 1.02, maxBytesRatio: 1.15, allocBudget: 850},
	"W7": {maxNsRatio: 1.10, maxAllocRatio: 0.75, maxBytesRatio: 1.05, allocBudget: 1406500},
	"W8": {maxNsRatio: 1.15, maxAllocRatio: 1.02, maxBytesRatio: 1.15, allocBudget: 850},
	// Several parts per series (W2 projection): time-disjoint blocks and overlapping ones.
	"W9":  {maxNsRatio: 1.10, maxAllocRatio: 0.68, maxBytesRatio: 1.05, allocBudget: 1414000},
	"W10": {maxNsRatio: 1.10, maxAllocRatio: 0.68, maxBytesRatio: 1.05, allocBudget: 1408000},
	// The OAP shape: the same 100 series without ORDER BY, so their rows interleave
	// through the time-ordered merge. Allocations are the egress wrappers; the merge adds
	// a few thousand per query. Time is gated against the passthrough baseline like the rest.
	"W11": {maxNsRatio: 1.10, maxAllocRatio: 0.68, maxBytesRatio: 1.05, allocBudget: 1407000},
	"W12": {maxNsRatio: 1.10, maxAllocRatio: 0.68, maxBytesRatio: 1.05, allocBudget: 1412000},
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
