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
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/index"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

func TestIndexedArrayProjection(t *testing.T) {
	db := openTestTSDBForRefTest(t, t.TempDir(), 1, nil)
	defer db.Close()
	segment, err := db.CreateSegmentIfNotExist(time.Now())
	require.NoError(t, err)
	defer segment.DecRef()
	cases := []struct {
		labels, nums *modelv1.TagValue
		name         string
	}{
		{name: "multiple", labels: strArrTagValue([]string{"red", "blue"}), nums: int64ArrTagValue([]int64{2, 1})},
		{
			name: "duplicates_and_escaping", labels: strArrTagValue([]string{"z|a", "", `a\b`, "z|a", "é"}),
			nums: int64ArrTagValue([]int64{math.MaxInt64, -1, math.MinInt64, 0, -1}),
		},
		{name: "single_empty_string", labels: strArrTagValue([]string{""}), nums: int64ArrTagValue([]int64{0})},
		{name: "null", labels: pbv1.NullTagValue, nums: pbv1.NullTagValue},
		// Empty indexed arrays emit no stored fields, as null arrays do. This
		// existing persisted representation cannot distinguish those two inputs.
		{name: "empty", labels: strArrTagValue(nil), nums: int64ArrTagValue(nil)},
	}
	schema := &databasev1.Measure{
		Metadata: &commonv1.Metadata{Name: "indexed_array"},
		Entity:   &databasev1.Entity{TagNames: []string{"rid"}},
		TagFamilies: []*databasev1.TagFamilySpec{{Name: "default", Tags: []*databasev1.TagSpec{
			{Name: "rid", Type: databasev1.TagType_TAG_TYPE_INT},
			{Name: "labels", Type: databasev1.TagType_TAG_TYPE_STRING_ARRAY},
			{Name: "nums", Type: databasev1.TagType_TAG_TYPE_INT_ARRAY},
			{Name: "host", Type: databasev1.TagType_TAG_TYPE_STRING},
		}}},
	}
	is := indexSchema{indexRules: []*databasev1.IndexRule{
		{Metadata: &commonv1.Metadata{Id: 1}, Tags: []string{"labels"}, Type: databasev1.IndexRule_TYPE_INVERTED, Analyzer: "keyword", NoSort: true},
		{Metadata: &commonv1.Metadata{Id: 2}, Tags: []string{"nums"}, Type: databasev1.IndexRule_TYPE_INVERTED, NoSort: true},
		{Metadata: &commonv1.Metadata{Id: 3}, Tags: []string{"host"}, Type: databasev1.IndexRule_TYPE_INVERTED, Analyzer: "keyword"},
	}}
	is.parse(schema)
	m := &measure{schema: schema}
	m.indexSchema.Store(is)
	for caseIndex, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rid := int64TagValue(int64(caseIndex + 1))
			req := &measurev1.WriteRequest{DataPoint: &measurev1.DataPointValue{
				TagFamilies: []*modelv1.TagFamilyForWrite{{Tags: []*modelv1.TagValue{rid, tc.labels, tc.nums, strTagValue("last")}}},
			}}
			families, fields := handleTagFamily(schema, req, is.indexRuleLocators, nil)
			for i := range families {
				families[i].reset()
			}
			series := &pbv1.Series{Subject: schema.Metadata.Name, EntityValues: []*modelv1.TagValue{rid}}
			require.NoError(t, series.Marshal())
			require.NoError(t, segment.IndexDB().Insert(index.Documents{{DocID: uint64(series.ID), EntityValues: series.Buffer, Fields: fields}}))
			wants := map[string]*modelv1.TagValue{"labels": tc.labels, "nums": tc.nums, "host": strTagValue("last")}
			if tc.name == "empty" {
				wants["labels"], wants["nums"] = pbv1.NullTagValue, pbv1.NullTagValue
			}
			for _, order := range []*index.OrderBy{nil, {Type: index.OrderByTypeIndex, Index: is.indexRules[2]}} {
				var got map[string]*modelv1.TagValue
				require.NotPanics(t, func() {
					_, _, _, _, stored, _, queryErr := m.searchSeriesList(context.Background(), []*pbv1.Series{series},
						model.MeasureQueryOptions{TagProjection: []model.TagProjection{{Family: "default", Names: []string{"labels", "nums", "host"}}}, Order: order},
						[]storage.Segment[*tsTable, option]{segment})
					require.NoError(t, queryErr)
					got = stored[series.ID]
				})
				for name, want := range wants {
					value := got[name]
					if value == nil {
						value = pbv1.NullTagValue
					}
					require.True(t, proto.Equal(want, value), "%s want %v, got %v", name, want, value)
				}
				// Exercise index-mode row and typed batch consumers with the same real
				// native projection, including absent arrays and singleton empty values.
				projection := []index.FieldKey{{IndexRuleID: 1}, {IndexRuleID: 2}, {IndexRuleID: 3}}
				multi := map[string]struct{}{projection[0].Marshal(): {}, projection[1].Marshal(): {}}
				sd, _, searchErr := segment.IndexDB().Search(context.Background(), []*pbv1.Series{series},
					storage.IndexSearchOpts{Projection: projection, MultiValueProjection: multi, Order: order})
				require.NoError(t, searchErr)
				require.Len(t, sd.SeriesList, 1)
				if tc.name == "multiple" && order == nil {
					search := func(capacity int64, full bool) (int64, error) {
						tracker := vectorized.NewMemoryTracker(capacity)
						ctx, scope := query.WithAllocationScope(context.Background(), tracker.Reserve, tracker.Release)
						opts := storage.IndexSearchOpts{Projection: projection}
						if full {
							opts.MultiValueProjection = multi
						}
						_, _, queryErr := segment.IndexDB().Search(ctx, []*pbv1.Series{series}, opts)
						used := tracker.Used()
						scope.Close()
						require.Zero(t, tracker.Used(), "all index reservations released on success or failure")
						return used, queryErr
					}
					scalarBytes, scalarErr := search(1<<30, false)
					require.NoError(t, scalarErr)
					arrayBytes, arrayErr := search(1<<30, true)
					require.NoError(t, arrayErr)
					require.Greater(t, arrayBytes, scalarBytes, "complete arrays must reserve more than the scalar projection")
					_, rejectErr := search(arrayBytes-1, true)
					require.Error(t, rejectErr, "reject before retaining the complete array when its reservation does not fit")
					_, recoveryErr := search(1<<30, true)
					require.NoError(t, recoveryErr)
				}
				sr := &segResult{SeriesData: sd}
				tfl := tagFamilyLocation{name: "default", tagNames: []string{"labels", "nums", "host"}, fieldToValueType: map[string]tagNameWithType{
					"labels": {fieldName: projection[0].Marshal(), typ: pbv1.ValueTypeStrArr},
					"nums":   {fieldName: projection[1].Marshal(), typ: pbv1.ValueTypeInt64Arr},
					"host":   {fieldName: projection[2].Marshal(), typ: pbv1.ValueTypeStr},
				}}
				iqr := &indexSortResult{tfl: []tagFamilyLocation{tfl}}
				row := iqr.copyTo(sr)
				require.Len(t, row.TagFamilies, 1)
				for _, tag := range row.TagFamilies[0].Tags {
					require.True(t, proto.Equal(wants[tag.Name], tag.Values[0]), "%s row", tag.Name)
				}
				for _, def := range []vectorized.ColumnDef{
					{Role: vectorized.RoleTag, TagFamily: "default", Name: "labels", Type: vectorized.ColumnTypeStrArray},
					{Role: vectorized.RoleTag, TagFamily: "default", Name: "nums", Type: vectorized.ColumnTypeInt64Array},
				} {
					batch := model.AcquireMeasureBatch(vectorized.NewBatchSchema([]vectorized.ColumnDef{def}), 1)
					iqr.fillSegTagCell(batch.Tags[0], def, sr, 0, sd.Fields[0])
					require.True(t, proto.Equal(wants[def.Name], nativeCellAsTagValue(batch.Tags[0], 0)), "%s batch", def.Name)
					batch.Release()
				}
			}
		})
	}
}
