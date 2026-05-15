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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

// G5b storage-level bench: measures the cost of block_cursor decode at
// the raw-bytes-to-cell boundary, where native column types can skip
// the *modelv1.TagValue wrapper construction (3 allocs/cell) that the
// row path and passthrough column types pay through mustDecodeTagValue.
//
// The package-level bench in pkg/query/vectorized/measure cannot model
// this — its fixture builds *modelv1.TagValue at the model.MeasureResult
// boundary, so both passthrough and native end up reading pre-built
// wrappers. Here, we build a real tsTable, snapshot it, and time three
// storage emit paths over identical raw stored bytes:
//
//   - row:         copyAllTo (legacy row path) → *MeasureResult
//   - passthrough: copyAllToBatch with ColumnTypeTagValue/FieldValue
//   - native:      copyAllToBatch with native primitives
//
// Run via:
//
//	go test ./banyand/measure -bench=BenchmarkStorageDecode -benchmem -run=^$ -count=3 -benchtime=2s

const (
	benchStorageSeries  = 100
	benchStorageRowsPer = 1000
)

var benchStorageProj = []model.TagProjection{
	{Family: "default", Names: []string{"svc", "env_id"}},
}

var benchStorageFieldProj = []string{"v_int"}

var benchStorageSchemaTagTypes = map[string]pbv1.ValueType{
	"svc":    pbv1.ValueTypeStr,
	"env_id": pbv1.ValueTypeInt64,
}

// benchPassthroughSchema mirrors what BuildBatchSchema currently produces
// in production: tag/field cells stay as *modelv1.TagValue / FieldValue.
func benchPassthroughSchema() *vectorized.BatchSchema {
	return vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleTimestamp, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleVersion, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleSeriesID, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleShardID, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "svc", Type: vectorized.ColumnTypeTagValue},
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "env_id", Type: vectorized.ColumnTypeTagValue},
		{Role: vectorized.RoleField, Name: "v_int", Type: vectorized.ColumnTypeFieldValue},
	})
}

// benchNativeSchema declares the same columns with native primitive types,
// so block_cursor's appendDecodedTagBytesAsTyped path is exercised.
func benchNativeSchema() *vectorized.BatchSchema {
	return vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleTimestamp, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleVersion, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleSeriesID, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleShardID, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "svc", Type: vectorized.ColumnTypeString},
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "env_id", Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleField, Name: "v_int", Type: vectorized.ColumnTypeInt64},
	})
}

func buildBenchStorageDataPoints(seriesCount, rowsPerSeries int) *dataPoints {
	total := seriesCount * rowsPerSeries
	dps := &dataPoints{
		seriesIDs:   make([]common.SeriesID, total),
		timestamps:  make([]int64, total),
		versions:    make([]int64, total),
		tagFamilies: make([][]nameValues, total),
		fields:      make([]nameValues, total),
	}
	for s := range seriesCount {
		for r := range rowsPerSeries {
			i := s*rowsPerSeries + r
			dps.seriesIDs[i] = common.SeriesID(s + 1)
			dps.timestamps[i] = int64(r + 1)
			dps.versions[i] = 1
			dps.tagFamilies[i] = []nameValues{
				{
					name: "default", values: []*nameValue{
						{name: "svc", valueType: pbv1.ValueTypeStr, value: []byte("alpha")},
						{name: "env_id", valueType: pbv1.ValueTypeInt64, value: convert.Int64ToBytes(int64(s + 1))},
					},
				},
			}
			dps.fields[i] = nameValues{
				name: "skipped", values: []*nameValue{
					{name: "v_int", valueType: pbv1.ValueTypeInt64, value: convert.Int64ToBytes(42)},
				},
			}
		}
	}
	return dps
}

// benchStorageHarness owns the tsTable + snapshot + part list used by all
// three storage decode benchmarks. Built once per benchmark function via
// b.Helper / b.Cleanup so the per-iteration cost focuses on cursor decode.
type benchStorageHarness struct {
	tst       *tsTable
	snap      *snapshot
	parts     []*part
	sids      []common.SeriesID
	queryOpts queryOptions
}

func setupBenchStorageHarness(b *testing.B) *benchStorageHarness {
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
	dps := buildBenchStorageDataPoints(benchStorageSeries, benchStorageRowsPer)
	tst.mustAddDataPoints(dps)
	time.Sleep(200 * time.Millisecond)

	snap := tst.currentSnapshot()
	require.NotNil(b, snap)

	shardCache := storage.NewShardCache("bench-group", 0, 0)
	parts, _ := snap.getParts(nil, shardCache, 0, int64(benchStorageRowsPer)+1)
	require.NotEmpty(b, parts)

	sids := make([]common.SeriesID, benchStorageSeries)
	for i := range sids {
		sids[i] = common.SeriesID(i + 1)
	}

	queryOpts := queryOptions{
		schemaTagTypes: benchStorageSchemaTagTypes,
		minTimestamp:   0,
		maxTimestamp:   int64(benchStorageRowsPer) + 1,
	}
	queryOpts.MeasureQueryOptions.TagProjection = benchStorageProj
	queryOpts.MeasureQueryOptions.FieldProjection = benchStorageFieldProj

	b.Cleanup(func() {
		snap.decRef()
		tst.Close()
		defFn()
	})
	return &benchStorageHarness{tst: tst, snap: snap, parts: parts, sids: sids, queryOpts: queryOpts}
}

// drainCursors walks every block in the harness's parts and invokes f
// against each loaded cursor. Mirrors loadCursorsForBatch's per-block
// step but inline so the bench stays single-goroutine and deterministic.
func (h *benchStorageHarness) drainCursors(f func(bc *blockCursor)) {
	ti := &tstIter{}
	ti.init(h.parts, h.sids, h.queryOpts.minTimestamp, h.queryOpts.maxTimestamp)
	for ti.nextBlock() {
		bc := generateBlockCursor()
		p := ti.piHeap[0]
		opts := h.queryOpts
		opts.TagProjection = benchStorageProj
		opts.FieldProjection = benchStorageFieldProj
		bc.init(p.p, p.curBlock, opts)
		tmpBlock := generateBlock()
		if bc.loadData(tmpBlock) {
			f(bc)
		}
		releaseBlock(tmpBlock)
		releaseBlockCursor(bc)
	}
}

func BenchmarkStorageDecode_Row(b *testing.B) {
	h := setupBenchStorageHarness(b)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		h.drainCursors(func(bc *blockCursor) {
			r := &model.MeasureResult{}
			bc.copyAllTo(r, nil, benchStorageProj, false)
		})
	}
}

func BenchmarkStorageDecode_Passthrough(b *testing.B) {
	h := setupBenchStorageHarness(b)
	schema := benchPassthroughSchema()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		h.drainCursors(func(bc *blockCursor) {
			mb := newMeasureBatchForSchema(schema, len(bc.timestamps))
			bc.copyAllToBatch(mb, schema, nil, false)
		})
	}
}

func BenchmarkStorageDecode_Native(b *testing.B) {
	h := setupBenchStorageHarness(b)
	schema := benchNativeSchema()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		h.drainCursors(func(bc *blockCursor) {
			mb := newMeasureBatchForSchema(schema, len(bc.timestamps))
			bc.copyAllToBatch(mb, schema, nil, false)
		})
	}
}
