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
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/apache/skywalking-banyandb/api/common"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	vmeasure "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure"
)

// Window-scan fixtures: memory parts built through the real memPart writer (so
// blocks split, dedupe and encode exactly as flushed parts do) whose rows are a
// closed-form function of (series, timestamp, version). A row's values change
// with its version, so a query that kept the wrong version of a duplicated
// timestamp reads visibly different tags and fields.

const (
	fxFamily = "default"
	fxTagSvc = "svc"   // string, few distinct values (dictionary encoded)
	fxTagURL = "url"   // string, unique per row (plain encoded)
	fxTagN   = "n"     // int64 with nulls (bytes form)
	fxTagN2  = "n2"    // int64 without nulls (typed numeric form)
	fxTagArr = "arr"   // string array
	fxTagIAr = "iarr"  // int array
	fxTagBin = "bin"   // binary
	fxTagHid = "hid"   // string, substituted from the index in some tests
	fxTagGho = "ghost" // not in the measure schema: passthrough column, always null
	fxFldInt = "fi"    // int64 field
	fxFldFlt = "ff"    // float64 field
	fxFldNul = "fnull" // int64 field with nulls
	fxFldStr = "fstr"  // string field
	fxFldBin = "fbin"  // binary field
)

var fxTagTypes = map[string]pbv1.ValueType{
	fxTagSvc: pbv1.ValueTypeStr, fxTagURL: pbv1.ValueTypeStr, fxTagN: pbv1.ValueTypeInt64, fxTagN2: pbv1.ValueTypeInt64,
	fxTagArr: pbv1.ValueTypeStrArr, fxTagIAr: pbv1.ValueTypeInt64Arr, fxTagBin: pbv1.ValueTypeBinaryData, fxTagHid: pbv1.ValueTypeStr,
}

func fxMeasure() *databasev1.Measure {
	return &databasev1.Measure{
		TagFamilies: []*databasev1.TagFamilySpec{{Name: fxFamily, Tags: []*databasev1.TagSpec{
			{Name: fxTagSvc, Type: databasev1.TagType_TAG_TYPE_STRING},
			{Name: fxTagURL, Type: databasev1.TagType_TAG_TYPE_STRING},
			{Name: fxTagN, Type: databasev1.TagType_TAG_TYPE_INT},
			{Name: fxTagN2, Type: databasev1.TagType_TAG_TYPE_INT},
			{Name: fxTagArr, Type: databasev1.TagType_TAG_TYPE_STRING_ARRAY},
			{Name: fxTagIAr, Type: databasev1.TagType_TAG_TYPE_INT_ARRAY},
			{Name: fxTagBin, Type: databasev1.TagType_TAG_TYPE_DATA_BINARY},
			{Name: fxTagHid, Type: databasev1.TagType_TAG_TYPE_STRING},
		}}},
		Fields: []*databasev1.FieldSpec{
			{Name: fxFldInt, FieldType: databasev1.FieldType_FIELD_TYPE_INT},
			{Name: fxFldFlt, FieldType: databasev1.FieldType_FIELD_TYPE_FLOAT},
			{Name: fxFldNul, FieldType: databasev1.FieldType_FIELD_TYPE_INT},
			{Name: fxFldStr, FieldType: databasev1.FieldType_FIELD_TYPE_STRING},
			{Name: fxFldBin, FieldType: databasev1.FieldType_FIELD_TYPE_DATA_BINARY},
		},
	}
}

// fxRange is rows [from, to] of one series at one version.
type fxRange struct {
	sid     common.SeriesID
	from    int64
	to      int64
	version int64
}

// fxPart is one memory part: the ranges written into it.
type fxPart []fxRange

type fxFixture struct {
	written []fxPart
	parts   []*part
	sids    []common.SeriesID // sorted
}

func fxValue(name string, valueType pbv1.ValueType, value []byte) *nameValue {
	return &nameValue{name: name, valueType: valueType, value: value}
}

func fxPoints(ranges fxPart) *dataPoints {
	dps := &dataPoints{}
	for _, r := range ranges {
		for ts := r.from; ts <= r.to; ts++ {
			sid, v := int64(r.sid), r.version
			n := fxValue(fxTagN, pbv1.ValueTypeInt64, convert.Int64ToBytes(ts*100+v))
			if ts%13 == 0 {
				n.value = nil
			}
			arr := &nameValue{name: fxTagArr, valueType: pbv1.ValueTypeStrArr, valueArr: [][]byte{
				[]byte(fmt.Sprintf("a%d", ts%3)), []byte(fmt.Sprintf("has|pipe-%d", v)), []byte(`back\slash`),
			}}
			if ts%17 == 0 {
				arr.valueArr = nil
				arr.value = nil
			}
			iarr := &nameValue{name: fxTagIAr, valueType: pbv1.ValueTypeInt64Arr, valueArr: [][]byte{
				convert.Int64ToBytes(ts), convert.Int64ToBytes(v),
			}}
			bin := fxValue(fxTagBin, pbv1.ValueTypeBinaryData, []byte(fmt.Sprintf("bin-%d-%d-%d", sid, ts, v)))
			if ts%19 == 0 {
				bin.value = nil
			}
			fnull := fxValue(fxFldNul, pbv1.ValueTypeInt64, convert.Int64ToBytes(ts-v))
			if ts%7 == 0 {
				fnull.value = nil
			}
			fstr := fxValue(fxFldStr, pbv1.ValueTypeStr, []byte(fmt.Sprintf("fs-%d-%d", ts, v)))
			if ts%11 == 0 {
				fstr.value = nil
			}
			dps.seriesIDs = append(dps.seriesIDs, r.sid)
			dps.timestamps = append(dps.timestamps, ts)
			dps.versions = append(dps.versions, v)
			dps.tagFamilies = append(dps.tagFamilies, []nameValues{{name: fxFamily, values: []*nameValue{
				fxValue(fxTagSvc, pbv1.ValueTypeStr, []byte(fmt.Sprintf("svc-%d", sid%4))),
				fxValue(fxTagURL, pbv1.ValueTypeStr, []byte(fmt.Sprintf("/u/%d/%d/%d", sid, ts, v))),
				n,
				fxValue(fxTagN2, pbv1.ValueTypeInt64, convert.Int64ToBytes(sid*1000000+ts*10+v)),
				arr,
				iarr,
				bin,
			}}})
			dps.fields = append(dps.fields, nameValues{name: "f", values: []*nameValue{
				fxValue(fxFldInt, pbv1.ValueTypeInt64, convert.Int64ToBytes(sid*1000+ts*3+v)),
				fxValue(fxFldFlt, pbv1.ValueTypeFloat64, convert.Float64ToBytes(float64(ts)*0.5+float64(v))),
				fnull,
				fstr,
				fxValue(fxFldBin, pbv1.ValueTypeBinaryData, []byte(fmt.Sprintf("fb-%d-%d", ts, v))),
			}})
		}
	}
	return dps
}

// newFixture writes each part through memPart.mustInitFromDataPoints and opens it.
// Parts are listed oldest first, the order a snapshot would hand them out in.
func newFixture(t testing.TB, parts ...fxPart) *fxFixture {
	t.Helper()
	fx := &fxFixture{written: parts}
	seen := map[common.SeriesID]struct{}{}
	for i, ranges := range parts {
		mp := generateMemPart()
		mp.mustInitFromDataPoints(fxPoints(ranges))
		p := openMemPart(mp)
		p.cache = storage.NewShardCache("scan-window-test", 0, 0)
		p.partMetadata.ID = uint64(i + 1)
		fx.parts = append(fx.parts, p)
		for _, r := range ranges {
			if _, ok := seen[r.sid]; !ok {
				seen[r.sid] = struct{}{}
				fx.sids = append(fx.sids, r.sid)
			}
		}
	}
	slices.Sort(fx.sids)
	return fx
}

// fxQuery is one query over a fixture.
type fxQuery struct {
	groupBy     *model.MeasureGroupBy
	agg         *model.MeasureAgg
	stored      map[common.SeriesID]map[string]*modelv1.TagValue
	order       []common.SeriesID // the order sids are requested in; nil means ascending
	tags        []string
	fields      []string
	minTS       int64
	maxTS       int64
	offset      uint32
	limit       uint32
	orderByTS   bool
	ascTS       bool
	passthrough bool
}

func (q *fxQuery) options() model.MeasureQueryOptions {
	var proj []model.TagProjection
	if len(q.tags) > 0 {
		proj = []model.TagProjection{{Family: fxFamily, Names: q.tags}}
	}
	return model.MeasureQueryOptions{TagProjection: proj, FieldProjection: q.fields, GroupBy: q.groupBy, Agg: q.agg}
}

func (q *fxQuery) schema(t testing.TB) *vectorized.BatchSchema {
	t.Helper()
	opts := q.options()
	schema, err := vmeasure.BuildBatchSchema(fxMeasure(), opts)
	require.NoError(t, err)
	if q.passthrough {
		return passthroughSchema(schema, opts)
	}
	return schema
}

// newResult builds a queryResult over the fixture the way searchBlocks does.
func (fx *fxFixture) newResult(t testing.TB, q *fxQuery, schema *vectorized.BatchSchema) *queryResult {
	t.Helper()
	opts := q.options()
	qo := queryOptions{schemaTagTypes: fxTagTypes, minTimestamp: q.minTS, maxTimestamp: q.maxTS}
	qo.MeasureQueryOptions.TagProjection = opts.TagProjection
	qo.MeasureQueryOptions.FieldProjection = opts.FieldProjection
	order := q.order
	if order == nil {
		order = fx.sids
	}
	qr := &queryResult{
		ctx:              context.Background(),
		batchSchema:      schema,
		tagProjection:    opts.TagProjection,
		storedIndexValue: q.stored,
		orderByTS:        q.orderByTS,
		ascTS:            q.ascTS,
		sidToIndex:       map[common.SeriesID]int{},
	}
	for i, sid := range order {
		qr.sidToIndex[sid] = i
	}
	sorted := slices.Clone(order)
	slices.Sort(sorted)
	ti := generateTstIter()
	defer releaseTstIter(ti)
	ti.init(slices.Clone(fx.parts), sorted, qo.minTimestamp, qo.maxTimestamp)
	for ti.nextBlock() {
		bc := generateBlockCursor()
		p := ti.piHeap[0]
		bc.init(p.p, p.curBlock, qo)
		qr.data = append(qr.data, bc)
	}
	require.NoError(t, ti.Error())
	return qr
}

// fxResult is everything one execution of a query produced.
type fxResult struct {
	iterErr  error
	frameErr error
	scan     *scanStats
	frame    []byte
	points   []*measurev1.InternalDataPoint
}

// run executes q over fx through the production pipeline — PullBatch, the scan
// source, a limit, the iterator and the raw frame drain — with the legacy scan on
// or off.
func (fx *fxFixture) run(t testing.TB, q *fxQuery, legacy bool) fxResult {
	t.Helper()
	prev := setLegacyBatchScan(legacy)
	defer setLegacyBatchScan(prev)
	var res fxResult
	schema := q.schema(t)
	terminal := schema
	build := func() (*vectorized.Pipeline, *queryResult) {
		qr := fx.newResult(t, q, schema)
		pool := vectorized.NewBatchPool(schema, 1024)
		tracker := vectorized.NewMemoryTracker(64 * 1024 * 1024)
		builder := vectorized.NewPipelineBuilder().WithMemoryTracker(tracker).From(vmeasure.NewBatchSourceFromBatchResult(qr, schema, pool, 1024))
		terminal = schema
		if q.groupBy != nil || q.agg != nil {
			ops, err := vmeasure.BuildOperators(q.options(), schema, tracker, 1024, vmeasure.AggModeAll)
			require.NoError(t, err)
			builder.Break(ops[0])
			terminal = ops[0].OutputSchema()
		}
		if q.limit > 0 || q.offset > 0 {
			limit := q.limit
			if limit == 0 {
				limit = 1 << 30
			}
			builder.Apply(vmeasure.NewBatchLimit(terminal, q.offset, limit))
		}
		pipeline, err := builder.Build()
		require.NoError(t, err)
		require.NoError(t, pipeline.Init(context.Background()))
		return pipeline, qr
	}
	pipeline, qr := build()
	it := vmeasure.NewIteratorFromPipeline(context.Background(), pipeline, terminal, vectorized.NewBatchPool(terminal, 1024))
	for it.Next() {
		res.points = append(res.points, it.Current()...)
	}
	res.iterErr = it.Err()
	if qr.scan != nil {
		stats := qr.scan.stats
		res.scan = &stats
	}
	require.NoError(t, it.Close())

	pipeline, _ = build()
	res.frame, res.frameErr = vmeasure.DrainPipelineToFrame(context.Background(), pipeline, terminal)
	require.NoError(t, pipeline.Close())
	return res
}

// requireSameResult fails unless got reproduces want row for row and byte for byte.
func requireSameResult(t testing.TB, want, got fxResult, what string) {
	t.Helper()
	require.Equal(t, fmt.Sprint(want.iterErr), fmt.Sprint(got.iterErr), what)
	require.Equal(t, fmt.Sprint(want.frameErr), fmt.Sprint(got.frameErr), what)
	require.Len(t, got.points, len(want.points), "%s: row count", what)
	for i := range want.points {
		if !proto.Equal(want.points[i], got.points[i]) {
			t.Fatalf("%s: row %d differs\nwant %v\n got %v", what, i, want.points[i], got.points[i])
		}
	}
	require.Equal(t, want.frame, got.frame, "%s: raw frame bytes", what)
}
