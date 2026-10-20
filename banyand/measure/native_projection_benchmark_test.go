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
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	vmeasure "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

// End-to-end projection benchmark: real stored blocks are decoded by the
// blockCursor straight into the batch the schema declares, drained through the
// vectorized pipeline and serialized at egress. It compares the pre-native column
// selection (every non-GroupBy/Agg projected column passthrough) against the
// native default, so the decode savings and the egress wrapper cost are both
// inside the measured loop.
//
//	go test ./banyand/measure -run '^$' -bench BenchmarkNativeProjection -benchmem -count=6
//	BYDB_BENCH_LEGACY_SCHEMA=1 go test ... (the baseline)

const (
	nativeBenchSeries  = 100
	nativeBenchRowsPer = 1000
	nativeBenchBatch   = 1024
	nativeBenchRows    = nativeBenchSeries * nativeBenchRowsPer
)

var (
	nativeBenchTagProj = []model.TagProjection{
		{Family: "default", Names: []string{"svc", "endpoint", "env_id", "blob"}},
	}
	nativeBenchFieldProj = []string{"v_int", "v_float"}
	nativeBenchTagTypes  = map[string]pbv1.ValueType{
		"svc": pbv1.ValueTypeStr, "endpoint": pbv1.ValueTypeStr,
		"env_id": pbv1.ValueTypeInt64, "blob": pbv1.ValueTypeBinaryData,
	}
)

func nativeBenchMeasure() *databasev1.Measure {
	return &databasev1.Measure{
		TagFamilies: []*databasev1.TagFamilySpec{{Name: "default", Tags: []*databasev1.TagSpec{
			{Name: "svc", Type: databasev1.TagType_TAG_TYPE_STRING},
			{Name: "endpoint", Type: databasev1.TagType_TAG_TYPE_STRING},
			{Name: "env_id", Type: databasev1.TagType_TAG_TYPE_INT},
			{Name: "blob", Type: databasev1.TagType_TAG_TYPE_DATA_BINARY},
		}}},
		Fields: []*databasev1.FieldSpec{
			{Name: "v_int", FieldType: databasev1.FieldType_FIELD_TYPE_INT},
			{Name: "v_float", FieldType: databasev1.FieldType_FIELD_TYPE_FLOAT},
		},
	}
}

func nativeBenchSchema(legacy bool, opts model.MeasureQueryOptions) *vectorized.BatchSchema {
	schema, err := vmeasure.BuildBatchSchema(nativeBenchMeasure(), opts)
	if err != nil {
		panic(err)
	}
	if !legacy {
		return schema
	}
	return passthroughSchema(schema, opts)
}

// passthroughSchema reproduces the column selection used before projected
// tags and fields became native by default: only GroupBy keys and the Agg
// target stay native, every other projected tag/field is a passthrough column.
func passthroughSchema(schema *vectorized.BatchSchema, opts model.MeasureQueryOptions) *vectorized.BatchSchema {
	cols := append([]vectorized.ColumnDef(nil), schema.Columns...)
	for i := range cols {
		switch cols[i].Role {
		case vectorized.RoleTag:
			isKey := opts.GroupBy != nil && opts.GroupBy.TagFamily == cols[i].TagFamily && containsString(opts.GroupBy.TagNames, cols[i].Name)
			if !isKey {
				cols[i].Type = vectorized.ColumnTypeTagValue
			}
		case vectorized.RoleField:
			if opts.Agg == nil || opts.Agg.FieldName != cols[i].Name {
				cols[i].Type = vectorized.ColumnTypeFieldValue
			}
		case vectorized.RoleTimestamp, vectorized.RoleVersion, vectorized.RoleSeriesID, vectorized.RoleShardID,
			vectorized.RoleElementID, vectorized.RoleOrderKey:
		}
	}
	return vectorized.NewBatchSchema(cols)
}

func containsString(list []string, s string) bool {
	for _, e := range list {
		if e == s {
			return true
		}
	}
	return false
}

type nativeBenchHarness struct {
	tst   *tsTable
	snap  *snapshot
	parts []partRef
	sids  []common.SeriesID
}

func setupNativeBenchHarness(b testing.TB) *nativeBenchHarness {
	b.Helper()
	tmpPath, defFn := test.Space(require.New(b))
	tst, err := newTSTable(
		fs.NewLocalFileSystem(),
		tmpPath, common.Position{},
		logger.GetLogger("bench"),
		timestamp.TimeRange{},
		option{flushTimeout: 0, mergePolicy: newDefaultMergePolicyForTesting(), protector: protector.Nop{}},
		nil,
	)
	require.NoError(b, err)
	total := nativeBenchRows
	points := &dataPoints{
		seriesIDs:   make([]common.SeriesID, total),
		timestamps:  make([]int64, total),
		versions:    make([]int64, total),
		tagFamilies: make([][]nameValues, total),
		fields:      make([]nameValues, total),
	}
	for s := range nativeBenchSeries {
		for r := range nativeBenchRowsPer {
			i := s*nativeBenchRowsPer + r
			points.seriesIDs[i] = common.SeriesID(s + 1)
			points.timestamps[i] = int64(r + 1)
			points.versions[i] = 1
			points.tagFamilies[i] = []nameValues{{name: "default", values: []*nameValue{
				{name: "svc", valueType: pbv1.ValueTypeStr, value: []byte(fmt.Sprintf("service-%d", s%8))},
				{name: "endpoint", valueType: pbv1.ValueTypeStr, value: []byte(fmt.Sprintf("/api/v1/resource/%d/item/%d", s, r))},
				{name: "env_id", valueType: pbv1.ValueTypeInt64, value: convert.Int64ToBytes(int64(s + 1))},
				{name: "blob", valueType: pbv1.ValueTypeBinaryData, value: []byte{byte(r), byte(s), 1, 2, 3, 4}},
			}}}
			points.fields[i] = nameValues{name: "skipped", values: []*nameValue{
				{name: "v_int", valueType: pbv1.ValueTypeInt64, value: convert.Int64ToBytes(int64(r*7 + s))},
				{name: "v_float", valueType: pbv1.ValueTypeFloat64, value: convert.Float64ToBytes(float64(r) * 0.25)},
			}}
		}
	}
	tst.mustAddDataPoints(points)
	time.Sleep(300 * time.Millisecond)
	snap := tst.currentSnapshot()
	require.NotNil(b, snap)
	shardCache := storage.NewShardCache("bench-group", 0, 0)
	parts, _ := snap.getParts(nil, shardCache, 0, 0, int64(nativeBenchRowsPer)+1)
	require.NotEmpty(b, parts)
	sids := make([]common.SeriesID, nativeBenchSeries)
	for i := range sids {
		sids[i] = common.SeriesID(i + 1)
	}
	b.Cleanup(func() {
		snap.decRef()
		tst.Close()
		defFn()
	})
	return &nativeBenchHarness{tst: tst, snap: snap, parts: parts, sids: sids}
}

// cursorBatchResult serves one MeasureBatch per stored block through PullBatch,
// decoding with the same copyAllToBatch the production queryResult uses.
type cursorBatchResult struct {
	schema *vectorized.BatchSchema
	ti     *tstIter
	opts   queryOptions
}

func (h *nativeBenchHarness) newResult(schema *vectorized.BatchSchema, opts model.MeasureQueryOptions) *cursorBatchResult {
	qo := queryOptions{
		schemaTagTypes: nativeBenchTagTypes,
		minTimestamp:   0,
		maxTimestamp:   int64(nativeBenchRowsPer) + 1,
	}
	qo.MeasureQueryOptions.TagProjection = opts.TagProjection
	qo.MeasureQueryOptions.FieldProjection = opts.FieldProjection
	ti := &tstIter{}
	ti.init(h.parts, h.sids, qo.minTimestamp, qo.maxTimestamp)
	return &cursorBatchResult{schema: schema, ti: ti, opts: qo}
}

func (r *cursorBatchResult) PullBatch(context.Context) (*model.MeasureBatch, error) {
	for r.ti.nextBlock() {
		bc := generateBlockCursor()
		p := r.ti.piHeap[0]
		bc.init(p.p, p.curBlock, r.opts)
		tmpBlock := generateBlock()
		loaded := bc.loadData(tmpBlock, true)
		releaseBlock(tmpBlock)
		if !loaded {
			releaseBlockCursor(bc)
			continue
		}
		mb := newMeasureBatchForSchema(r.schema, len(bc.timestamps))
		bc.copyAllToBatch(mb, r.schema, nil, false)
		releaseBlockCursor(bc)
		return mb, nil
	}
	return nil, nil
}

func (r *cursorBatchResult) Release() {}

// nativeBenchStages is what a scenario adds on top of the scan source.
type nativeBenchStages struct {
	out      *vectorized.BatchSchema
	breakers []vectorized.BreakerOperator
	fusible  []vectorized.FusibleOperator
}

type nativeBenchBuild func(schema *vectorized.BatchSchema, tracker *vectorized.MemoryTracker) nativeBenchStages

type nativeBenchScenario struct {
	build nativeBenchBuild
	name  string
	opts  model.MeasureQueryOptions
	frame bool
}

func groupByStages(opts model.MeasureQueryOptions) nativeBenchBuild {
	return func(s *vectorized.BatchSchema, tracker *vectorized.MemoryTracker) nativeBenchStages {
		ops, err := vmeasure.BuildOperators(opts, s, tracker, nativeBenchBatch, vmeasure.AggModeAll)
		if err != nil {
			panic(err)
		}
		return nativeBenchStages{out: ops[0].OutputSchema(), breakers: []vectorized.BreakerOperator{ops[0]}}
	}
}

func nativeBenchScenarios() []nativeBenchScenario {
	plain := model.MeasureQueryOptions{TagProjection: nativeBenchTagProj, FieldProjection: nativeBenchFieldProj}
	limit := func(n uint32) nativeBenchBuild {
		return func(s *vectorized.BatchSchema, _ *vectorized.MemoryTracker) nativeBenchStages {
			return nativeBenchStages{out: s, fusible: []vectorized.FusibleOperator{vmeasure.NewBatchLimit(s, 0, n)}}
		}
	}
	top := func(n int) nativeBenchBuild {
		return func(s *vectorized.BatchSchema, _ *vectorized.MemoryTracker) nativeBenchStages {
			idx, _ := s.FieldIndex("v_int")
			return nativeBenchStages{out: s, breakers: []vectorized.BreakerOperator{vmeasure.NewBatchTop(s, idx, n, false, nativeBenchBatch)}}
		}
	}
	none := func(s *vectorized.BatchSchema, _ *vectorized.MemoryTracker) nativeBenchStages {
		return nativeBenchStages{out: s}
	}
	groupBy := model.MeasureQueryOptions{
		TagProjection:   []model.TagProjection{{Family: "default", Names: []string{"svc"}}},
		FieldProjection: []string{"v_int"},
		GroupBy:         &model.MeasureGroupBy{TagFamily: "default", TagNames: []string{"svc"}},
		Agg:             &model.MeasureAgg{FieldName: "v_int", Func: modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM},
	}
	return []nativeBenchScenario{
		{name: "scan100", opts: plain, build: none},
		{name: "limit10", opts: plain, build: limit(nativeBenchRows / 10)},
		{name: "limit1", opts: plain, build: limit(nativeBenchRows / 100)},
		{name: "top10", opts: plain, build: top(nativeBenchRows / 10)},
		{name: "top1", opts: plain, build: top(nativeBenchRows / 100)},
		{name: "groupby", opts: groupBy, build: groupByStages(groupBy)},
		{name: "frame_scan100", opts: plain, build: none, frame: true},
		{name: "frame_top10", opts: plain, build: top(nativeBenchRows / 10), frame: true},
	}
}

func runNativeBenchScenario(b *testing.B, h *nativeBenchHarness, sc nativeBenchScenario, legacy bool) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()
	var sink int
	for b.Loop() {
		schema := nativeBenchSchema(legacy, sc.opts)
		res := h.newResult(schema, sc.opts)
		pool := vectorized.NewBatchPool(schema, nativeBenchBatch)
		source := vmeasure.NewBatchSourceFromBatchResult(res, schema, pool, nativeBenchBatch)
		tracker := vectorized.NewMemoryTracker(256 * 1024 * 1024)
		builder := vectorized.NewPipelineBuilder().WithMemoryTracker(tracker).From(source)
		stages := sc.build(schema, tracker)
		outSchema := stages.out
		for _, br := range stages.breakers {
			builder.Break(br)
		}
		for _, f := range stages.fusible {
			builder.Apply(f)
		}
		pipeline, err := builder.Build()
		if err != nil {
			b.Fatal(err)
		}
		if err = pipeline.Init(context.Background()); err != nil {
			b.Fatal(err)
		}
		if sc.frame {
			body, frameErr := vmeasure.DrainPipelineToFrame(context.Background(), pipeline, outSchema)
			if frameErr != nil {
				b.Fatal(frameErr)
			}
			sink += len(body)
			_ = pipeline.Close()
			continue
		}
		it := vmeasure.NewIteratorFromPipeline(context.Background(), pipeline, outSchema, vectorized.NewBatchPool(outSchema, nativeBenchBatch))
		for it.Next() {
			sink += len(it.Current())
		}
		if err = it.Close(); err != nil {
			b.Fatal(err)
		}
	}
	nativeBenchSink = sink
}

var nativeBenchSink int

// BenchmarkNativeProjection runs every scenario once with the column selection
// chosen by BYDB_BENCH_LEGACY_SCHEMA (unset: native default; 1: legacy).
func BenchmarkNativeProjection(b *testing.B) {
	legacy := os.Getenv("BYDB_BENCH_LEGACY_SCHEMA") == "1"
	h := setupNativeBenchHarness(b)
	for _, sc := range nativeBenchScenarios() {
		b.Run(sc.name, func(b *testing.B) { runNativeBenchScenario(b, h, sc, legacy) })
	}
}
