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
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/apache/skywalking-banyandb/api/common"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// legacyBuildBatchSchema reproduces the column selection BuildBatchSchema had
// before projected tags and fields became native by default: only GroupBy keys
// and the Agg target are native, everything else is a passthrough column. It is
// the baseline the equivalence tests compare against.
func legacyBuildBatchSchema(measureSchema *databasev1.Measure, opts model.MeasureQueryOptions) (*vectorized.BatchSchema, error) {
	native, err := BuildBatchSchema(measureSchema, opts)
	if err != nil {
		return nil, err
	}
	nativeTags := map[string]struct{}{}
	if opts.GroupBy != nil && opts.GroupBy.TagFamily != "" {
		for _, name := range opts.GroupBy.TagNames {
			nativeTags[opts.GroupBy.TagFamily+"\x00"+name] = struct{}{}
		}
	}
	nativeFields := map[string]struct{}{}
	if opts.Agg != nil {
		if opts.Agg.TagName != "" {
			nativeTags[opts.Agg.TagFamily+"\x00"+opts.Agg.TagName] = struct{}{}
		}
		if opts.Agg.FieldName != "" {
			nativeFields[opts.Agg.FieldName] = struct{}{}
		}
	}
	cols := append([]vectorized.ColumnDef(nil), native.Columns...)
	for i := range cols {
		switch cols[i].Role {
		case vectorized.RoleTag:
			if _, keep := nativeTags[cols[i].TagFamily+"\x00"+cols[i].Name]; !keep {
				cols[i].Type = vectorized.ColumnTypeTagValue
			}
		case vectorized.RoleField:
			if _, keep := nativeFields[cols[i].Name]; !keep {
				cols[i].Type = vectorized.ColumnTypeFieldValue
			}
		case vectorized.RoleTimestamp, vectorized.RoleVersion, vectorized.RoleSeriesID, vectorized.RoleShardID,
			vectorized.RoleElementID, vectorized.RoleOrderKey:
		}
	}
	return vectorized.NewBatchSchema(cols), nil
}

func nativeTestMeasure() *databasev1.Measure {
	return &databasev1.Measure{
		TagFamilies: []*databasev1.TagFamilySpec{
			{Name: "default", Tags: []*databasev1.TagSpec{
				{Name: "t_int", Type: databasev1.TagType_TAG_TYPE_INT},
				{Name: "t_str", Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: "t_bin", Type: databasev1.TagType_TAG_TYPE_DATA_BINARY},
				{Name: "t_iarr", Type: databasev1.TagType_TAG_TYPE_INT_ARRAY},
				{Name: "t_sarr", Type: databasev1.TagType_TAG_TYPE_STRING_ARRAY},
			}},
		},
		Fields: []*databasev1.FieldSpec{
			{Name: "f_int", FieldType: databasev1.FieldType_FIELD_TYPE_INT},
			{Name: "f_float", FieldType: databasev1.FieldType_FIELD_TYPE_FLOAT},
			{Name: "f_str", FieldType: databasev1.FieldType_FIELD_TYPE_STRING},
			{Name: "f_bin", FieldType: databasev1.FieldType_FIELD_TYPE_DATA_BINARY},
		},
	}
}

func nativeTestOpts() model.MeasureQueryOptions {
	return model.MeasureQueryOptions{
		TagProjection: []model.TagProjection{
			{Family: "default", Names: []string{"t_int", "t_str", "t_bin", "t_iarr", "t_sarr", "t_unknown"}},
			{Family: "no_such_family", Names: []string{"t_x"}},
		},
		FieldProjection: []string{"f_int", "f_float", "f_str", "f_bin", "f_unknown"},
	}
}

func TestBuildBatchSchemaNativeByDefault(t *testing.T) {
	schema, err := BuildBatchSchema(nativeTestMeasure(), nativeTestOpts())
	require.NoError(t, err)
	type colKey struct{ family, name string }
	wantTags := map[colKey]vectorized.ColumnType{
		{"default", "t_int"}:      vectorized.ColumnTypeInt64,
		{"default", "t_str"}:      vectorized.ColumnTypeString,
		{"default", "t_bin"}:      vectorized.ColumnTypeBytes,
		{"default", "t_iarr"}:     vectorized.ColumnTypeInt64Array,
		{"default", "t_sarr"}:     vectorized.ColumnTypeStrArray,
		{"default", "t_unknown"}:  vectorized.ColumnTypeTagValue,
		{"no_such_family", "t_x"}: vectorized.ColumnTypeTagValue,
	}
	wantFields := map[string]vectorized.ColumnType{
		"f_int":     vectorized.ColumnTypeInt64,
		"f_float":   vectorized.ColumnTypeFloat64,
		"f_str":     vectorized.ColumnTypeString,
		"f_bin":     vectorized.ColumnTypeBytes,
		"f_unknown": vectorized.ColumnTypeFieldValue,
	}
	seenTags, seenFields := 0, 0
	for _, def := range schema.Columns {
		switch def.Role {
		case vectorized.RoleTag:
			want, ok := wantTags[colKey{def.TagFamily, def.Name}]
			require.True(t, ok, "unexpected tag %s.%s", def.TagFamily, def.Name)
			require.Equal(t, want, def.Type, "tag %s.%s", def.TagFamily, def.Name)
			seenTags++
		case vectorized.RoleField:
			want, ok := wantFields[def.Name]
			require.True(t, ok, "unexpected field %s", def.Name)
			require.Equal(t, want, def.Type, "field %s", def.Name)
			seenFields++
		case vectorized.RoleTimestamp, vectorized.RoleVersion, vectorized.RoleSeriesID, vectorized.RoleShardID,
			vectorized.RoleElementID, vectorized.RoleOrderKey:
			require.Equal(t, vectorized.ColumnTypeInt64, def.Type)
		}
	}
	require.Equal(t, len(wantTags), seenTags)
	require.Equal(t, len(wantFields), seenFields)
}

func TestBuildBatchSchemaGroupByAggStaysNative(t *testing.T) {
	opts := nativeTestOpts()
	opts.GroupBy = &model.MeasureGroupBy{TagFamily: "default", TagNames: []string{"t_str"}}
	opts.Agg = &model.MeasureAgg{FieldName: "f_int"}
	native, err := BuildBatchSchema(nativeTestMeasure(), opts)
	require.NoError(t, err)
	legacy, err := legacyBuildBatchSchema(nativeTestMeasure(), opts)
	require.NoError(t, err)
	for i, def := range native.Columns {
		if def.Name == "t_str" || def.Name == "f_int" {
			require.Equal(t, legacy.Columns[i].Type, def.Type, "%s keeps its native type", def.Name)
		}
	}
	// Every other mapped column is native in the new schema and passthrough in the legacy one.
	idx, ok := native.TagIndex("default", "t_int")
	require.True(t, ok)
	require.Equal(t, vectorized.ColumnTypeTagValue, legacy.Columns[idx].Type)
	require.Equal(t, vectorized.ColumnTypeInt64, native.Columns[idx].Type)
}

// resultBatchSource serves pre-built MeasureResults through PullBatch, building
// each batch for the schema under test the way the TopN row-merge fallback does.
type resultBatchSource struct {
	schema  *vectorized.BatchSchema
	results []*model.MeasureResult
	idx     int
}

func (f *resultBatchSource) PullBatch(context.Context) (*model.MeasureBatch, error) {
	if f.idx >= len(f.results) {
		return nil, nil
	}
	r := f.results[f.idx]
	f.idx++
	return BuildMeasureBatchFromResult(r, f.schema)
}

func (f *resultBatchSource) Release() {}

// nativeTestResults builds the fixture. withNil also emits bare nil cells; the
// storage decoder never produces those (it emits the Null singletons), so the
// frame comparison leaves them out.
func nativeTestResults(withNil bool) []*model.MeasureResult {
	const n = 9
	mk := func(sid common.SeriesID, base int64) *model.MeasureResult {
		r := &model.MeasureResult{SID: sid}
		for i := range n {
			r.Timestamps = append(r.Timestamps, base+int64(i))
			r.Versions = append(r.Versions, int64(i%3))
			r.ShardIDs = append(r.ShardIDs, common.ShardID(sid))
		}
		tv := func(f func(i int) *modelv1.TagValue) []*modelv1.TagValue {
			out := make([]*modelv1.TagValue, n)
			for i := range out {
				switch i % 5 {
				case 3:
					out[i] = pbv1.NullTagValue
				case 4:
					if withNil {
						out[i] = nil
					} else {
						out[i] = pbv1.NullTagValue
					}
				default:
					out[i] = f(i)
				}
			}
			return out
		}
		fv := func(f func(i int) *modelv1.FieldValue) []*modelv1.FieldValue {
			out := make([]*modelv1.FieldValue, n)
			for i := range out {
				switch i % 5 {
				case 3:
					out[i] = pbv1.NullFieldValue
				case 4:
					if withNil {
						out[i] = nil
					} else {
						out[i] = pbv1.NullFieldValue
					}
				default:
					out[i] = f(i)
				}
			}
			return out
		}
		r.TagFamilies = []model.TagFamily{{Name: "default", Tags: []model.Tag{
			{Name: "t_int", Values: tv(func(i int) *modelv1.TagValue {
				return &modelv1.TagValue{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: int64(i) - 4}}}
			})},
			{Name: "t_str", Values: tv(func(i int) *modelv1.TagValue {
				return &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: fmt.Sprintf("s-%d", i%2)}}}
			})},
			{Name: "t_bin", Values: tv(func(i int) *modelv1.TagValue {
				return &modelv1.TagValue{Value: &modelv1.TagValue_BinaryData{BinaryData: []byte{byte(i), 0xff}}}
			})},
			{Name: "t_iarr", Values: tv(func(i int) *modelv1.TagValue {
				return &modelv1.TagValue{Value: &modelv1.TagValue_IntArray{IntArray: &modelv1.IntArray{Value: []int64{int64(i), -1}}}}
			})},
			{Name: "t_sarr", Values: tv(func(i int) *modelv1.TagValue {
				return &modelv1.TagValue{Value: &modelv1.TagValue_StrArray{StrArray: &modelv1.StrArray{Value: []string{"a", fmt.Sprint(i)}}}}
			})},
		}}}
		r.Fields = []model.Field{
			{Name: "f_int", Values: fv(func(i int) *modelv1.FieldValue {
				return &modelv1.FieldValue{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: int64(i * 10)}}}
			})},
			{Name: "f_float", Values: fv(func(i int) *modelv1.FieldValue {
				return &modelv1.FieldValue{Value: &modelv1.FieldValue_Float{Float: &modelv1.Float{Value: float64(i) / 4}}}
			})},
			{Name: "f_str", Values: fv(func(i int) *modelv1.FieldValue {
				return &modelv1.FieldValue{Value: &modelv1.FieldValue_Str{Str: &modelv1.Str{Value: fmt.Sprintf("f%d", i)}}}
			})},
			{Name: "f_bin", Values: fv(func(i int) *modelv1.FieldValue {
				return &modelv1.FieldValue{Value: &modelv1.FieldValue_BinaryData{BinaryData: []byte{0, byte(i)}}}
			})},
		}
		return r
	}
	return []*model.MeasureResult{mk(1, 100), mk(2, 200)}
}

func nativeTestPipeline(t *testing.T, schema *vectorized.BatchSchema, results []*model.MeasureResult) (*vectorized.Pipeline, *vectorized.BatchPool) {
	t.Helper()
	const batchSize = 4
	pool := vectorized.NewBatchPool(schema, batchSize)
	source := NewBatchSourceFromBatchResult(&resultBatchSource{schema: schema, results: results}, schema, pool, batchSize)
	pipeline, err := vectorized.NewPipelineBuilder().From(source).Build()
	require.NoError(t, err)
	require.NoError(t, pipeline.Init(context.Background()))
	return pipeline, pool
}

func nativeTestMIterator(t *testing.T, schema *vectorized.BatchSchema, results []*model.MeasureResult) []*measurev1.InternalDataPoint {
	t.Helper()
	pipeline, pool := nativeTestPipeline(t, schema, results)
	it := NewIteratorFromPipeline(context.Background(), pipeline, schema, pool)
	var out []*measurev1.InternalDataPoint
	for it.Next() {
		out = append(out, it.Current()...)
	}
	require.NoError(t, it.Err())
	require.NoError(t, it.Close())
	return out
}

func requireSameDataPoints(t *testing.T, want, got []*measurev1.InternalDataPoint) {
	t.Helper()
	require.Len(t, got, len(want))
	for i := range want {
		require.True(t, proto.Equal(want[i], got[i]), "row %d:\nwant %v\ngot  %v", i, want[i], got[i])
		wantBytes, wantErr := proto.MarshalOptions{Deterministic: true}.Marshal(want[i])
		require.NoError(t, wantErr)
		gotBytes, gotErr := proto.MarshalOptions{Deterministic: true}.Marshal(got[i])
		require.NoError(t, gotErr)
		require.Equal(t, wantBytes, gotBytes, "row %d wire bytes", i)
	}
}

// TestEgressNativeMatchesPassthrough proves a native projection and a
// passthrough projection of the same cells produce identical egress output:
// null, absent (nil) and explicit-null cells, ints, strings, binary, int and
// string arrays, floats, and unknown tags/fields/families — over the standalone
// proto path and the distributed frame path.
func TestEgressNativeMatchesPassthrough(t *testing.T) {
	opts := nativeTestOpts()
	native, err := BuildBatchSchema(nativeTestMeasure(), opts)
	require.NoError(t, err)
	legacy, err := legacyBuildBatchSchema(nativeTestMeasure(), opts)
	require.NoError(t, err)
	require.NotEqual(t, native.Columns, legacy.Columns, "the two schemas must differ for the comparison to mean anything")

	want := nativeTestMIterator(t, legacy, nativeTestResults(true))
	got := nativeTestMIterator(t, native, nativeTestResults(true))
	require.Len(t, want, 18)
	requireSameDataPoints(t, want, got)

	t.Run("frame", func(t *testing.T) {
		encode := func(schema *vectorized.BatchSchema) []*measurev1.InternalDataPoint {
			pipeline, _ := nativeTestPipeline(t, schema, nativeTestResults(false))
			body, encErr := DrainPipelineToFrame(context.Background(), pipeline, schema)
			require.NoError(t, encErr)
			require.NoError(t, pipeline.Close())
			idps, decErr := DecodeFramesToInternalDataPoints([][]byte{body})
			require.NoError(t, decErr)
			return idps
		}
		requireSameDataPoints(t, encode(legacy), encode(native))
	})
}
