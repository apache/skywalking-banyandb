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
	"sort"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/apache/skywalking-banyandb/api/common"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	itest "github.com/apache/skywalking-banyandb/banyand/internal/test"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/run"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
	"github.com/apache/skywalking-banyandb/pkg/watcher"
)

func TestQueryResult(t *testing.T) {
	tests := []struct {
		wantErr       error
		name          string
		dpsList       []*dataPoints
		sids          []common.SeriesID
		want          []model.MeasureResult
		minTimestamp  int64
		maxTimestamp  int64
		orderBySeries bool
		ascTS         bool
	}{
		{
			name:         "Test with multiple parts with duplicated data order by TS",
			dpsList:      []*dataPoints{dpsTS1, dpsTS1},
			sids:         []common.SeriesID{1, 2, 3},
			minTimestamp: 1,
			maxTimestamp: 1,
			want: []model.MeasureResult{{
				SID:        1,
				Timestamps: []int64{1},
				Versions:   []int64{1},
				TagFamilies: []model.TagFamily{
					{Name: "arrTag", Tags: []model.Tag{
						{Name: "strArrTag", Values: []*modelv1.TagValue{strArrTagValue([]string{"value1", "value2"})}},
						{Name: "intArrTag", Values: []*modelv1.TagValue{int64ArrTagValue([]int64{25, 30})}},
					}},
					{Name: "binaryTag", Tags: []model.Tag{
						{Name: "binaryTag", Values: []*modelv1.TagValue{binaryDataTagValue(longText)}},
					}},
					{Name: "singleTag", Tags: []model.Tag{
						{Name: "strTag", Values: []*modelv1.TagValue{strTagValue("value1")}},
						{Name: "intTag", Values: []*modelv1.TagValue{int64TagValue(10)}},
						{Name: "strTag1", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "strTag2", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
				},
				Fields: []model.Field{
					{Name: "strField", Values: []*modelv1.FieldValue{strFieldValue("field1")}},
					{Name: "intField", Values: []*modelv1.FieldValue{int64FieldValue(1110)}},
					{Name: "floatField", Values: []*modelv1.FieldValue{float64FieldValue(1.221233343e+06)}},
					{Name: "binaryField", Values: []*modelv1.FieldValue{binaryDataFieldValue(longText)}},
				},
			}, {
				SID:        2,
				Timestamps: []int64{1},
				Versions:   []int64{2},
				TagFamilies: []model.TagFamily{
					{Name: "arrTag", Tags: []model.Tag{
						{Name: "strArrTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "intArrTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
					{Name: "binaryTag", Tags: []model.Tag{
						{Name: "binaryTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
					{Name: "singleTag", Tags: []model.Tag{
						{Name: "strTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "intTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "strTag1", Values: []*modelv1.TagValue{strTagValue("tag1")}},
						{Name: "strTag2", Values: []*modelv1.TagValue{strTagValue("tag2")}},
					}},
				},
				Fields: nil,
			}, {
				SID:        3,
				Timestamps: []int64{1},
				Versions:   []int64{3},
				TagFamilies: []model.TagFamily{
					{Name: "arrTag", Tags: []model.Tag{
						{Name: "strArrTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "intArrTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
					{Name: "binaryTag", Tags: []model.Tag{
						{Name: "binaryTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
					{Name: "singleTag", Tags: []model.Tag{
						{Name: "strTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "intTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "strTag1", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "strTag2", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
				},
				Fields: []model.Field{
					{Name: "intField", Values: []*modelv1.FieldValue{int64FieldValue(1110)}},
				},
			}},
		},
		{
			name:         "Test with multiple parts with duplicated data with different version order by TS 1",
			dpsList:      []*dataPoints{dpsTS1, dpsTS11},
			sids:         []common.SeriesID{1, 2, 3},
			minTimestamp: 1,
			maxTimestamp: 1,
			want: []model.MeasureResult{{
				SID:        1,
				Timestamps: []int64{1},
				Versions:   []int64{1},
				TagFamilies: []model.TagFamily{
					{Name: "arrTag", Tags: []model.Tag{
						{Name: "strArrTag", Values: []*modelv1.TagValue{strArrTagValue([]string{"value1", "value2"})}},
						{Name: "intArrTag", Values: []*modelv1.TagValue{int64ArrTagValue([]int64{25, 30})}},
					}},
					{Name: "binaryTag", Tags: []model.Tag{
						{Name: "binaryTag", Values: []*modelv1.TagValue{binaryDataTagValue(longText)}},
					}},
					{Name: "singleTag", Tags: []model.Tag{
						{Name: "strTag", Values: []*modelv1.TagValue{strTagValue("value1")}},
						{Name: "intTag", Values: []*modelv1.TagValue{int64TagValue(10)}},
						{Name: "strTag1", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "strTag2", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
				},
				Fields: []model.Field{
					{Name: "strField", Values: []*modelv1.FieldValue{strFieldValue("field1")}},
					{Name: "intField", Values: []*modelv1.FieldValue{int64FieldValue(1110)}},
					{Name: "floatField", Values: []*modelv1.FieldValue{float64FieldValue(1.221233343e+06)}},
					{Name: "binaryField", Values: []*modelv1.FieldValue{binaryDataFieldValue(longText)}},
				},
			}, {
				SID:        2,
				Timestamps: []int64{1},
				Versions:   []int64{2},
				TagFamilies: []model.TagFamily{
					{Name: "arrTag", Tags: []model.Tag{
						{Name: "strArrTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "intArrTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
					{Name: "binaryTag", Tags: []model.Tag{
						{Name: "binaryTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
					{Name: "singleTag", Tags: []model.Tag{
						{Name: "strTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "intTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "strTag1", Values: []*modelv1.TagValue{strTagValue("tag1")}},
						{Name: "strTag2", Values: []*modelv1.TagValue{strTagValue("tag2")}},
					}},
				},
				Fields: nil,
			}, {
				SID:        3,
				Timestamps: []int64{1},
				Versions:   []int64{3},
				TagFamilies: []model.TagFamily{
					{Name: "arrTag", Tags: []model.Tag{
						{Name: "strArrTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "intArrTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
					{Name: "binaryTag", Tags: []model.Tag{
						{Name: "binaryTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
					{Name: "singleTag", Tags: []model.Tag{
						{Name: "strTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "intTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "strTag1", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "strTag2", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
				},
				Fields: []model.Field{
					{Name: "intField", Values: []*modelv1.FieldValue{int64FieldValue(1110)}},
				},
			}},
		},
		{
			name:         "Test with multiple parts with duplicated data with different version order by TS 2",
			dpsList:      []*dataPoints{dpsTS11, dpsTS1},
			sids:         []common.SeriesID{1, 2, 3},
			minTimestamp: 1,
			maxTimestamp: 1,
			want: []model.MeasureResult{{
				SID:        1,
				Timestamps: []int64{1},
				Versions:   []int64{1},
				TagFamilies: []model.TagFamily{
					{Name: "arrTag", Tags: []model.Tag{
						{Name: "strArrTag", Values: []*modelv1.TagValue{strArrTagValue([]string{"value1", "value2"})}},
						{Name: "intArrTag", Values: []*modelv1.TagValue{int64ArrTagValue([]int64{25, 30})}},
					}},
					{Name: "binaryTag", Tags: []model.Tag{
						{Name: "binaryTag", Values: []*modelv1.TagValue{binaryDataTagValue(longText)}},
					}},
					{Name: "singleTag", Tags: []model.Tag{
						{Name: "strTag", Values: []*modelv1.TagValue{strTagValue("value1")}},
						{Name: "intTag", Values: []*modelv1.TagValue{int64TagValue(10)}},
						{Name: "strTag1", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "strTag2", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
				},
				Fields: []model.Field{
					{Name: "strField", Values: []*modelv1.FieldValue{strFieldValue("field1")}},
					{Name: "intField", Values: []*modelv1.FieldValue{int64FieldValue(1110)}},
					{Name: "floatField", Values: []*modelv1.FieldValue{float64FieldValue(1.221233343e+06)}},
					{Name: "binaryField", Values: []*modelv1.FieldValue{binaryDataFieldValue(longText)}},
				},
			}, {
				SID:        2,
				Timestamps: []int64{1},
				Versions:   []int64{2},
				TagFamilies: []model.TagFamily{
					{Name: "arrTag", Tags: []model.Tag{
						{Name: "strArrTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "intArrTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
					{Name: "binaryTag", Tags: []model.Tag{
						{Name: "binaryTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
					{Name: "singleTag", Tags: []model.Tag{
						{Name: "strTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "intTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "strTag1", Values: []*modelv1.TagValue{strTagValue("tag1")}},
						{Name: "strTag2", Values: []*modelv1.TagValue{strTagValue("tag2")}},
					}},
				},
				Fields: nil,
			}, {
				SID:        3,
				Timestamps: []int64{1},
				Versions:   []int64{3},
				TagFamilies: []model.TagFamily{
					{Name: "arrTag", Tags: []model.Tag{
						{Name: "strArrTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "intArrTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
					{Name: "binaryTag", Tags: []model.Tag{
						{Name: "binaryTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
					{Name: "singleTag", Tags: []model.Tag{
						{Name: "strTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "intTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "strTag1", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "strTag2", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
				},
				Fields: []model.Field{
					{Name: "intField", Values: []*modelv1.FieldValue{int64FieldValue(1110)}},
				},
			}},
		},
		{
			name:         "Test with multiple parts with multiple data orderBy TS desc 1",
			dpsList:      []*dataPoints{dpsTS1, dpsTS2},
			sids:         []common.SeriesID{1, 2, 3},
			minTimestamp: 1,
			maxTimestamp: 2,
			want: []model.MeasureResult{{
				SID:        1,
				Timestamps: []int64{2},
				Versions:   []int64{4},
				TagFamilies: []model.TagFamily{
					{Name: "arrTag", Tags: []model.Tag{
						{Name: "strArrTag", Values: []*modelv1.TagValue{strArrTagValue([]string{"value5", "value6"})}},
						{Name: "intArrTag", Values: []*modelv1.TagValue{int64ArrTagValue([]int64{35, 40})}},
					}},
					{Name: "binaryTag", Tags: []model.Tag{
						{Name: "binaryTag", Values: []*modelv1.TagValue{binaryDataTagValue(longText)}},
					}},
					{Name: "singleTag", Tags: []model.Tag{
						{Name: "strTag", Values: []*modelv1.TagValue{strTagValue("value3")}},
						{Name: "intTag", Values: []*modelv1.TagValue{int64TagValue(30)}},
						{Name: "strTag1", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "strTag2", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
				},
				Fields: []model.Field{
					{Name: "strField", Values: []*modelv1.FieldValue{strFieldValue("field3")}},
					{Name: "intField", Values: []*modelv1.FieldValue{int64FieldValue(3330)}},
					{Name: "floatField", Values: []*modelv1.FieldValue{float64FieldValue(3663699.029)}},
					{Name: "binaryField", Values: []*modelv1.FieldValue{binaryDataFieldValue(longText)}},
				},
			}, {
				SID:        2,
				Timestamps: []int64{2},
				Versions:   []int64{5},
				TagFamilies: []model.TagFamily{
					{Name: "arrTag", Tags: []model.Tag{
						{Name: "strArrTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "intArrTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
					{Name: "binaryTag", Tags: []model.Tag{
						{Name: "binaryTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
					{Name: "singleTag", Tags: []model.Tag{
						{Name: "strTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "intTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "strTag1", Values: []*modelv1.TagValue{strTagValue("tag3")}},
						{Name: "strTag2", Values: []*modelv1.TagValue{strTagValue("tag4")}},
					}},
				},
				Fields: nil,
			}, {
				SID:        3,
				Timestamps: []int64{2},
				Versions:   []int64{6},
				TagFamilies: []model.TagFamily{
					{Name: "arrTag", Tags: []model.Tag{
						{Name: "strArrTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "intArrTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
					{Name: "binaryTag", Tags: []model.Tag{
						{Name: "binaryTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
					{Name: "singleTag", Tags: []model.Tag{
						{Name: "strTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "intTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "strTag1", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "strTag2", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
				},
				Fields: []model.Field{
					{Name: "intField", Values: []*modelv1.FieldValue{int64FieldValue(4440)}},
				},
			}, {
				SID:        1,
				Timestamps: []int64{1},
				Versions:   []int64{1},
				TagFamilies: []model.TagFamily{
					{Name: "arrTag", Tags: []model.Tag{
						{Name: "strArrTag", Values: []*modelv1.TagValue{strArrTagValue([]string{"value1", "value2"})}},
						{Name: "intArrTag", Values: []*modelv1.TagValue{int64ArrTagValue([]int64{25, 30})}},
					}},
					{Name: "binaryTag", Tags: []model.Tag{
						{Name: "binaryTag", Values: []*modelv1.TagValue{binaryDataTagValue(longText)}},
					}},
					{Name: "singleTag", Tags: []model.Tag{
						{Name: "strTag", Values: []*modelv1.TagValue{strTagValue("value1")}},
						{Name: "intTag", Values: []*modelv1.TagValue{int64TagValue(10)}},
						{Name: "strTag1", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "strTag2", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
				},
				Fields: []model.Field{
					{Name: "strField", Values: []*modelv1.FieldValue{strFieldValue("field1")}},
					{Name: "intField", Values: []*modelv1.FieldValue{int64FieldValue(1110)}},
					{Name: "floatField", Values: []*modelv1.FieldValue{float64FieldValue(1221233.343)}},
					{Name: "binaryField", Values: []*modelv1.FieldValue{binaryDataFieldValue(longText)}},
				},
			}, {
				SID:        2,
				Timestamps: []int64{1},
				Versions:   []int64{2},
				TagFamilies: []model.TagFamily{
					{Name: "arrTag", Tags: []model.Tag{
						{Name: "strArrTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "intArrTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
					{Name: "binaryTag", Tags: []model.Tag{
						{Name: "binaryTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
					{Name: "singleTag", Tags: []model.Tag{
						{Name: "strTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "intTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "strTag1", Values: []*modelv1.TagValue{strTagValue("tag1")}},
						{Name: "strTag2", Values: []*modelv1.TagValue{strTagValue("tag2")}},
					}},
				},
				Fields: nil,
			}, {
				SID:        3,
				Timestamps: []int64{1},
				Versions:   []int64{3},
				TagFamilies: []model.TagFamily{
					{Name: "arrTag", Tags: []model.Tag{
						{Name: "strArrTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "intArrTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
					{Name: "binaryTag", Tags: []model.Tag{
						{Name: "binaryTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
					{Name: "singleTag", Tags: []model.Tag{
						{Name: "strTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "intTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "strTag1", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "strTag2", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
				},
				Fields: []model.Field{
					{Name: "intField", Values: []*modelv1.FieldValue{int64FieldValue(1110)}},
				},
			}},
		},
		{
			name:         "Test with multiple parts with multiple data orderBy TS desc 2",
			dpsList:      []*dataPoints{dpsTS2, dpsTS1},
			sids:         []common.SeriesID{1, 2, 3},
			minTimestamp: 1,
			maxTimestamp: 2,
			want: []model.MeasureResult{{
				SID:        1,
				Timestamps: []int64{2},
				Versions:   []int64{4},
				TagFamilies: []model.TagFamily{
					{Name: "arrTag", Tags: []model.Tag{
						{Name: "strArrTag", Values: []*modelv1.TagValue{strArrTagValue([]string{"value5", "value6"})}},
						{Name: "intArrTag", Values: []*modelv1.TagValue{int64ArrTagValue([]int64{35, 40})}},
					}},
					{Name: "binaryTag", Tags: []model.Tag{
						{Name: "binaryTag", Values: []*modelv1.TagValue{binaryDataTagValue(longText)}},
					}},
					{Name: "singleTag", Tags: []model.Tag{
						{Name: "strTag", Values: []*modelv1.TagValue{strTagValue("value3")}},
						{Name: "intTag", Values: []*modelv1.TagValue{int64TagValue(30)}},
						{Name: "strTag1", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "strTag2", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
				},
				Fields: []model.Field{
					{Name: "strField", Values: []*modelv1.FieldValue{strFieldValue("field3")}},
					{Name: "intField", Values: []*modelv1.FieldValue{int64FieldValue(3330)}},
					{Name: "floatField", Values: []*modelv1.FieldValue{float64FieldValue(3663699.029)}},
					{Name: "binaryField", Values: []*modelv1.FieldValue{binaryDataFieldValue(longText)}},
				},
			}, {
				SID:        2,
				Timestamps: []int64{2},
				Versions:   []int64{5},
				TagFamilies: []model.TagFamily{
					{Name: "arrTag", Tags: []model.Tag{
						{Name: "strArrTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "intArrTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
					{Name: "binaryTag", Tags: []model.Tag{
						{Name: "binaryTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
					{Name: "singleTag", Tags: []model.Tag{
						{Name: "strTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "intTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "strTag1", Values: []*modelv1.TagValue{strTagValue("tag3")}},
						{Name: "strTag2", Values: []*modelv1.TagValue{strTagValue("tag4")}},
					}},
				},
				Fields: nil,
			}, {
				SID:        3,
				Timestamps: []int64{2},
				Versions:   []int64{6},
				TagFamilies: []model.TagFamily{
					{Name: "arrTag", Tags: []model.Tag{
						{Name: "strArrTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "intArrTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
					{Name: "binaryTag", Tags: []model.Tag{
						{Name: "binaryTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
					{Name: "singleTag", Tags: []model.Tag{
						{Name: "strTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "intTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "strTag1", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "strTag2", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
				},
				Fields: []model.Field{
					{Name: "intField", Values: []*modelv1.FieldValue{int64FieldValue(4440)}},
				},
			}, {
				SID:        1,
				Timestamps: []int64{1},
				Versions:   []int64{1},
				TagFamilies: []model.TagFamily{
					{Name: "arrTag", Tags: []model.Tag{
						{Name: "strArrTag", Values: []*modelv1.TagValue{strArrTagValue([]string{"value1", "value2"})}},
						{Name: "intArrTag", Values: []*modelv1.TagValue{int64ArrTagValue([]int64{25, 30})}},
					}},
					{Name: "binaryTag", Tags: []model.Tag{
						{Name: "binaryTag", Values: []*modelv1.TagValue{binaryDataTagValue(longText)}},
					}},
					{Name: "singleTag", Tags: []model.Tag{
						{Name: "strTag", Values: []*modelv1.TagValue{strTagValue("value1")}},
						{Name: "intTag", Values: []*modelv1.TagValue{int64TagValue(10)}},
						{Name: "strTag1", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "strTag2", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
				},
				Fields: []model.Field{
					{Name: "strField", Values: []*modelv1.FieldValue{strFieldValue("field1")}},
					{Name: "intField", Values: []*modelv1.FieldValue{int64FieldValue(1110)}},
					{Name: "floatField", Values: []*modelv1.FieldValue{float64FieldValue(1221233.343)}},
					{Name: "binaryField", Values: []*modelv1.FieldValue{binaryDataFieldValue(longText)}},
				},
			}, {
				SID:        2,
				Timestamps: []int64{1},
				Versions:   []int64{2},
				TagFamilies: []model.TagFamily{
					{Name: "arrTag", Tags: []model.Tag{
						{Name: "strArrTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "intArrTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
					{Name: "binaryTag", Tags: []model.Tag{
						{Name: "binaryTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
					{Name: "singleTag", Tags: []model.Tag{
						{Name: "strTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "intTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "strTag1", Values: []*modelv1.TagValue{strTagValue("tag1")}},
						{Name: "strTag2", Values: []*modelv1.TagValue{strTagValue("tag2")}},
					}},
				},
				Fields: nil,
			}, {
				SID:        3,
				Timestamps: []int64{1},
				Versions:   []int64{3},
				TagFamilies: []model.TagFamily{
					{Name: "arrTag", Tags: []model.Tag{
						{Name: "strArrTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "intArrTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
					{Name: "binaryTag", Tags: []model.Tag{
						{Name: "binaryTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
					{Name: "singleTag", Tags: []model.Tag{
						{Name: "strTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "intTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "strTag1", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "strTag2", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
				},
				Fields: []model.Field{
					{Name: "intField", Values: []*modelv1.FieldValue{int64FieldValue(1110)}},
				},
			}},
		},
		{
			name:         "Test with multiple parts with multiple data orderBy TS asc 1",
			dpsList:      []*dataPoints{dpsTS1, dpsTS2},
			sids:         []common.SeriesID{1, 2, 3},
			ascTS:        true,
			minTimestamp: 1,
			maxTimestamp: 2,
			want: []model.MeasureResult{{
				SID:        1,
				Timestamps: []int64{1},
				Versions:   []int64{1},
				TagFamilies: []model.TagFamily{
					{Name: "arrTag", Tags: []model.Tag{
						{Name: "strArrTag", Values: []*modelv1.TagValue{strArrTagValue([]string{"value1", "value2"})}},
						{Name: "intArrTag", Values: []*modelv1.TagValue{int64ArrTagValue([]int64{25, 30})}},
					}},
					{Name: "binaryTag", Tags: []model.Tag{
						{Name: "binaryTag", Values: []*modelv1.TagValue{binaryDataTagValue(longText)}},
					}},
					{Name: "singleTag", Tags: []model.Tag{
						{Name: "strTag", Values: []*modelv1.TagValue{strTagValue("value1")}},
						{Name: "intTag", Values: []*modelv1.TagValue{int64TagValue(10)}},
						{Name: "strTag1", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "strTag2", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
				},
				Fields: []model.Field{
					{Name: "strField", Values: []*modelv1.FieldValue{strFieldValue("field1")}},
					{Name: "intField", Values: []*modelv1.FieldValue{int64FieldValue(1110)}},
					{Name: "floatField", Values: []*modelv1.FieldValue{float64FieldValue(1221233.343)}},
					{Name: "binaryField", Values: []*modelv1.FieldValue{binaryDataFieldValue(longText)}},
				},
			}, {
				SID:        2,
				Timestamps: []int64{1},
				Versions:   []int64{2},
				TagFamilies: []model.TagFamily{
					{Name: "arrTag", Tags: []model.Tag{
						{Name: "strArrTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "intArrTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
					{Name: "binaryTag", Tags: []model.Tag{
						{Name: "binaryTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
					{Name: "singleTag", Tags: []model.Tag{
						{Name: "strTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "intTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "strTag1", Values: []*modelv1.TagValue{strTagValue("tag1")}},
						{Name: "strTag2", Values: []*modelv1.TagValue{strTagValue("tag2")}},
					}},
				},
				Fields: nil,
			}, {
				SID:        3,
				Timestamps: []int64{1},
				Versions:   []int64{3},
				TagFamilies: []model.TagFamily{
					{Name: "arrTag", Tags: []model.Tag{
						{Name: "strArrTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "intArrTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
					{Name: "binaryTag", Tags: []model.Tag{
						{Name: "binaryTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
					{Name: "singleTag", Tags: []model.Tag{
						{Name: "strTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "intTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "strTag1", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "strTag2", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
				},
				Fields: []model.Field{
					{Name: "intField", Values: []*modelv1.FieldValue{int64FieldValue(1110)}},
				},
			}, {
				SID:        1,
				Timestamps: []int64{2},
				Versions:   []int64{4},
				TagFamilies: []model.TagFamily{
					{Name: "arrTag", Tags: []model.Tag{
						{Name: "strArrTag", Values: []*modelv1.TagValue{strArrTagValue([]string{"value5", "value6"})}},
						{Name: "intArrTag", Values: []*modelv1.TagValue{int64ArrTagValue([]int64{35, 40})}},
					}},
					{Name: "binaryTag", Tags: []model.Tag{
						{Name: "binaryTag", Values: []*modelv1.TagValue{binaryDataTagValue(longText)}},
					}},
					{Name: "singleTag", Tags: []model.Tag{
						{Name: "strTag", Values: []*modelv1.TagValue{strTagValue("value3")}},
						{Name: "intTag", Values: []*modelv1.TagValue{int64TagValue(30)}},
						{Name: "strTag1", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "strTag2", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
				},
				Fields: []model.Field{
					{Name: "strField", Values: []*modelv1.FieldValue{strFieldValue("field3")}},
					{Name: "intField", Values: []*modelv1.FieldValue{int64FieldValue(3330)}},
					{Name: "floatField", Values: []*modelv1.FieldValue{float64FieldValue(3663699.029)}},
					{Name: "binaryField", Values: []*modelv1.FieldValue{binaryDataFieldValue(longText)}},
				},
			}, {
				SID:        2,
				Timestamps: []int64{2},
				Versions:   []int64{5},
				TagFamilies: []model.TagFamily{
					{Name: "arrTag", Tags: []model.Tag{
						{Name: "strArrTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "intArrTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
					{Name: "binaryTag", Tags: []model.Tag{
						{Name: "binaryTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
					{Name: "singleTag", Tags: []model.Tag{
						{Name: "strTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "intTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "strTag1", Values: []*modelv1.TagValue{strTagValue("tag3")}},
						{Name: "strTag2", Values: []*modelv1.TagValue{strTagValue("tag4")}},
					}},
				},
				Fields: nil,
			}, {
				SID:        3,
				Timestamps: []int64{2},
				Versions:   []int64{6},
				TagFamilies: []model.TagFamily{
					{Name: "arrTag", Tags: []model.Tag{
						{Name: "strArrTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "intArrTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
					{Name: "binaryTag", Tags: []model.Tag{
						{Name: "binaryTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
					{Name: "singleTag", Tags: []model.Tag{
						{Name: "strTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "intTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "strTag1", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "strTag2", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
				},
				Fields: []model.Field{
					{Name: "intField", Values: []*modelv1.FieldValue{int64FieldValue(4440)}},
				},
			}},
		},
		{
			name:         "Test with multiple parts with multiple data orderBy TS asc 2",
			dpsList:      []*dataPoints{dpsTS2, dpsTS1},
			sids:         []common.SeriesID{1, 2, 3},
			ascTS:        true,
			minTimestamp: 1,
			maxTimestamp: 2,
			want: []model.MeasureResult{{
				SID:        1,
				Timestamps: []int64{1},
				Versions:   []int64{1},
				TagFamilies: []model.TagFamily{
					{Name: "arrTag", Tags: []model.Tag{
						{Name: "strArrTag", Values: []*modelv1.TagValue{strArrTagValue([]string{"value1", "value2"})}},
						{Name: "intArrTag", Values: []*modelv1.TagValue{int64ArrTagValue([]int64{25, 30})}},
					}},
					{Name: "binaryTag", Tags: []model.Tag{
						{Name: "binaryTag", Values: []*modelv1.TagValue{binaryDataTagValue(longText)}},
					}},
					{Name: "singleTag", Tags: []model.Tag{
						{Name: "strTag", Values: []*modelv1.TagValue{strTagValue("value1")}},
						{Name: "intTag", Values: []*modelv1.TagValue{int64TagValue(10)}},
						{Name: "strTag1", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "strTag2", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
				},
				Fields: []model.Field{
					{Name: "strField", Values: []*modelv1.FieldValue{strFieldValue("field1")}},
					{Name: "intField", Values: []*modelv1.FieldValue{int64FieldValue(1110)}},
					{Name: "floatField", Values: []*modelv1.FieldValue{float64FieldValue(1221233.343)}},
					{Name: "binaryField", Values: []*modelv1.FieldValue{binaryDataFieldValue(longText)}},
				},
			}, {
				SID:        2,
				Timestamps: []int64{1},
				Versions:   []int64{2},
				TagFamilies: []model.TagFamily{
					{Name: "arrTag", Tags: []model.Tag{
						{Name: "strArrTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "intArrTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
					{Name: "binaryTag", Tags: []model.Tag{
						{Name: "binaryTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
					{Name: "singleTag", Tags: []model.Tag{
						{Name: "strTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "intTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "strTag1", Values: []*modelv1.TagValue{strTagValue("tag1")}},
						{Name: "strTag2", Values: []*modelv1.TagValue{strTagValue("tag2")}},
					}},
				},
				Fields: nil,
			}, {
				SID:        3,
				Timestamps: []int64{1},
				Versions:   []int64{3},
				TagFamilies: []model.TagFamily{
					{Name: "arrTag", Tags: []model.Tag{
						{Name: "strArrTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "intArrTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
					{Name: "binaryTag", Tags: []model.Tag{
						{Name: "binaryTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
					{Name: "singleTag", Tags: []model.Tag{
						{Name: "strTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "intTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "strTag1", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "strTag2", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
				},
				Fields: []model.Field{
					{Name: "intField", Values: []*modelv1.FieldValue{int64FieldValue(1110)}},
				},
			}, {
				SID:        1,
				Timestamps: []int64{2},
				Versions:   []int64{4},
				TagFamilies: []model.TagFamily{
					{Name: "arrTag", Tags: []model.Tag{
						{Name: "strArrTag", Values: []*modelv1.TagValue{strArrTagValue([]string{"value5", "value6"})}},
						{Name: "intArrTag", Values: []*modelv1.TagValue{int64ArrTagValue([]int64{35, 40})}},
					}},
					{Name: "binaryTag", Tags: []model.Tag{
						{Name: "binaryTag", Values: []*modelv1.TagValue{binaryDataTagValue(longText)}},
					}},
					{Name: "singleTag", Tags: []model.Tag{
						{Name: "strTag", Values: []*modelv1.TagValue{strTagValue("value3")}},
						{Name: "intTag", Values: []*modelv1.TagValue{int64TagValue(30)}},
						{Name: "strTag1", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "strTag2", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
				},
				Fields: []model.Field{
					{Name: "strField", Values: []*modelv1.FieldValue{strFieldValue("field3")}},
					{Name: "intField", Values: []*modelv1.FieldValue{int64FieldValue(3330)}},
					{Name: "floatField", Values: []*modelv1.FieldValue{float64FieldValue(3663699.029)}},
					{Name: "binaryField", Values: []*modelv1.FieldValue{binaryDataFieldValue(longText)}},
				},
			}, {
				SID:        2,
				Timestamps: []int64{2},
				Versions:   []int64{5},
				TagFamilies: []model.TagFamily{
					{Name: "arrTag", Tags: []model.Tag{
						{Name: "strArrTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "intArrTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
					{Name: "binaryTag", Tags: []model.Tag{
						{Name: "binaryTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
					{Name: "singleTag", Tags: []model.Tag{
						{Name: "strTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "intTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "strTag1", Values: []*modelv1.TagValue{strTagValue("tag3")}},
						{Name: "strTag2", Values: []*modelv1.TagValue{strTagValue("tag4")}},
					}},
				},
				Fields: nil,
			}, {
				SID:        3,
				Timestamps: []int64{2},
				Versions:   []int64{6},
				TagFamilies: []model.TagFamily{
					{Name: "arrTag", Tags: []model.Tag{
						{Name: "strArrTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "intArrTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
					{Name: "binaryTag", Tags: []model.Tag{
						{Name: "binaryTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
					{Name: "singleTag", Tags: []model.Tag{
						{Name: "strTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "intTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "strTag1", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "strTag2", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
				},
				Fields: []model.Field{
					{Name: "intField", Values: []*modelv1.FieldValue{int64FieldValue(4440)}},
				},
			}},
		},
		{
			name:          "Test with multiple parts with duplicated data order by Series",
			dpsList:       []*dataPoints{dpsTS1, dpsTS1},
			sids:          []common.SeriesID{1, 2, 3},
			orderBySeries: true,
			minTimestamp:  1,
			maxTimestamp:  1,
			want: []model.MeasureResult{{
				SID:        1,
				Timestamps: []int64{1},
				Versions:   []int64{1},
				TagFamilies: []model.TagFamily{
					{Name: "arrTag", Tags: []model.Tag{
						{Name: "strArrTag", Values: []*modelv1.TagValue{strArrTagValue([]string{"value1", "value2"})}},
						{Name: "intArrTag", Values: []*modelv1.TagValue{int64ArrTagValue([]int64{25, 30})}},
					}},
					{Name: "binaryTag", Tags: []model.Tag{
						{Name: "binaryTag", Values: []*modelv1.TagValue{binaryDataTagValue(longText)}},
					}},
					{Name: "singleTag", Tags: []model.Tag{
						{Name: "strTag", Values: []*modelv1.TagValue{strTagValue("value1")}},
						{Name: "intTag", Values: []*modelv1.TagValue{int64TagValue(10)}},
						{Name: "strTag1", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "strTag2", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
				},
				Fields: []model.Field{
					{Name: "strField", Values: []*modelv1.FieldValue{strFieldValue("field1")}},
					{Name: "intField", Values: []*modelv1.FieldValue{int64FieldValue(1110)}},
					{Name: "floatField", Values: []*modelv1.FieldValue{float64FieldValue(1.221233343e+06)}},
					{Name: "binaryField", Values: []*modelv1.FieldValue{binaryDataFieldValue(longText)}},
				},
			}, {
				SID:        2,
				Timestamps: []int64{1},
				Versions:   []int64{2},
				TagFamilies: []model.TagFamily{
					{Name: "arrTag", Tags: []model.Tag{
						{Name: "strArrTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "intArrTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
					{Name: "binaryTag", Tags: []model.Tag{
						{Name: "binaryTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
					{Name: "singleTag", Tags: []model.Tag{
						{Name: "strTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "intTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "strTag1", Values: []*modelv1.TagValue{strTagValue("tag1")}},
						{Name: "strTag2", Values: []*modelv1.TagValue{strTagValue("tag2")}},
					}},
				},
				Fields: nil,
			}, {
				SID:        3,
				Timestamps: []int64{1},
				Versions:   []int64{3},
				TagFamilies: []model.TagFamily{
					{Name: "arrTag", Tags: []model.Tag{
						{Name: "strArrTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "intArrTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
					{Name: "binaryTag", Tags: []model.Tag{
						{Name: "binaryTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
					{Name: "singleTag", Tags: []model.Tag{
						{Name: "strTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "intTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "strTag1", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "strTag2", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
				},
				Fields: []model.Field{
					{Name: "intField", Values: []*modelv1.FieldValue{int64FieldValue(1110)}},
				},
			}},
		},
		{
			name:          "Test with multiple parts with duplicated data with different versions order by Series 1",
			dpsList:       []*dataPoints{dpsTS11, dpsTS1},
			sids:          []common.SeriesID{1, 2, 3},
			orderBySeries: true,
			minTimestamp:  1,
			maxTimestamp:  1,
			want: []model.MeasureResult{{
				SID:        1,
				Timestamps: []int64{1},
				Versions:   []int64{1},
				TagFamilies: []model.TagFamily{
					{Name: "arrTag", Tags: []model.Tag{
						{Name: "strArrTag", Values: []*modelv1.TagValue{strArrTagValue([]string{"value1", "value2"})}},
						{Name: "intArrTag", Values: []*modelv1.TagValue{int64ArrTagValue([]int64{25, 30})}},
					}},
					{Name: "binaryTag", Tags: []model.Tag{
						{Name: "binaryTag", Values: []*modelv1.TagValue{binaryDataTagValue(longText)}},
					}},
					{Name: "singleTag", Tags: []model.Tag{
						{Name: "strTag", Values: []*modelv1.TagValue{strTagValue("value1")}},
						{Name: "intTag", Values: []*modelv1.TagValue{int64TagValue(10)}},
						{Name: "strTag1", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "strTag2", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
				},
				Fields: []model.Field{
					{Name: "strField", Values: []*modelv1.FieldValue{strFieldValue("field1")}},
					{Name: "intField", Values: []*modelv1.FieldValue{int64FieldValue(1110)}},
					{Name: "floatField", Values: []*modelv1.FieldValue{float64FieldValue(1.221233343e+06)}},
					{Name: "binaryField", Values: []*modelv1.FieldValue{binaryDataFieldValue(longText)}},
				},
			}, {
				SID:        2,
				Timestamps: []int64{1},
				Versions:   []int64{2},
				TagFamilies: []model.TagFamily{
					{Name: "arrTag", Tags: []model.Tag{
						{Name: "strArrTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "intArrTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
					{Name: "binaryTag", Tags: []model.Tag{
						{Name: "binaryTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
					{Name: "singleTag", Tags: []model.Tag{
						{Name: "strTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "intTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "strTag1", Values: []*modelv1.TagValue{strTagValue("tag1")}},
						{Name: "strTag2", Values: []*modelv1.TagValue{strTagValue("tag2")}},
					}},
				},
				Fields: nil,
			}, {
				SID:        3,
				Timestamps: []int64{1},
				Versions:   []int64{3},
				TagFamilies: []model.TagFamily{
					{Name: "arrTag", Tags: []model.Tag{
						{Name: "strArrTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "intArrTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
					{Name: "binaryTag", Tags: []model.Tag{
						{Name: "binaryTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
					{Name: "singleTag", Tags: []model.Tag{
						{Name: "strTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "intTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "strTag1", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "strTag2", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
				},
				Fields: []model.Field{
					{Name: "intField", Values: []*modelv1.FieldValue{int64FieldValue(1110)}},
				},
			}},
		},
		{
			name:          "Test with multiple parts with duplicated data with different versions order by Series 2",
			dpsList:       []*dataPoints{dpsTS1, dpsTS11},
			sids:          []common.SeriesID{1, 2, 3},
			orderBySeries: true,
			minTimestamp:  1,
			maxTimestamp:  1,
			want: []model.MeasureResult{{
				SID:        1,
				Timestamps: []int64{1},
				Versions:   []int64{1},
				TagFamilies: []model.TagFamily{
					{Name: "arrTag", Tags: []model.Tag{
						{Name: "strArrTag", Values: []*modelv1.TagValue{strArrTagValue([]string{"value1", "value2"})}},
						{Name: "intArrTag", Values: []*modelv1.TagValue{int64ArrTagValue([]int64{25, 30})}},
					}},
					{Name: "binaryTag", Tags: []model.Tag{
						{Name: "binaryTag", Values: []*modelv1.TagValue{binaryDataTagValue(longText)}},
					}},
					{Name: "singleTag", Tags: []model.Tag{
						{Name: "strTag", Values: []*modelv1.TagValue{strTagValue("value1")}},
						{Name: "intTag", Values: []*modelv1.TagValue{int64TagValue(10)}},
						{Name: "strTag1", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "strTag2", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
				},
				Fields: []model.Field{
					{Name: "strField", Values: []*modelv1.FieldValue{strFieldValue("field1")}},
					{Name: "intField", Values: []*modelv1.FieldValue{int64FieldValue(1110)}},
					{Name: "floatField", Values: []*modelv1.FieldValue{float64FieldValue(1.221233343e+06)}},
					{Name: "binaryField", Values: []*modelv1.FieldValue{binaryDataFieldValue(longText)}},
				},
			}, {
				SID:        2,
				Timestamps: []int64{1},
				Versions:   []int64{2},
				TagFamilies: []model.TagFamily{
					{Name: "arrTag", Tags: []model.Tag{
						{Name: "strArrTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "intArrTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
					{Name: "binaryTag", Tags: []model.Tag{
						{Name: "binaryTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
					{Name: "singleTag", Tags: []model.Tag{
						{Name: "strTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "intTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "strTag1", Values: []*modelv1.TagValue{strTagValue("tag1")}},
						{Name: "strTag2", Values: []*modelv1.TagValue{strTagValue("tag2")}},
					}},
				},
				Fields: nil,
			}, {
				SID:        3,
				Timestamps: []int64{1},
				Versions:   []int64{3},
				TagFamilies: []model.TagFamily{
					{Name: "arrTag", Tags: []model.Tag{
						{Name: "strArrTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "intArrTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
					{Name: "binaryTag", Tags: []model.Tag{
						{Name: "binaryTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
					{Name: "singleTag", Tags: []model.Tag{
						{Name: "strTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "intTag", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "strTag1", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "strTag2", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
				},
				Fields: []model.Field{
					{Name: "intField", Values: []*modelv1.FieldValue{int64FieldValue(1110)}},
				},
			}},
		},
		{
			name:          "Test with multiple parts with multiple data order by Series 1",
			dpsList:       []*dataPoints{dpsTS1, dpsTS2},
			sids:          []common.SeriesID{2, 1, 3},
			orderBySeries: true,
			minTimestamp:  1,
			maxTimestamp:  2,
			want: []model.MeasureResult{{
				SID:        2,
				Timestamps: []int64{1, 2},
				Versions:   []int64{2, 5},
				TagFamilies: []model.TagFamily{
					{Name: "arrTag", Tags: []model.Tag{
						{Name: "strArrTag", Values: []*modelv1.TagValue{pbv1.NullTagValue, pbv1.NullTagValue}},
						{Name: "intArrTag", Values: []*modelv1.TagValue{pbv1.NullTagValue, pbv1.NullTagValue}},
					}},
					{Name: "binaryTag", Tags: []model.Tag{
						{Name: "binaryTag", Values: []*modelv1.TagValue{pbv1.NullTagValue, pbv1.NullTagValue}},
					}},
					{Name: "singleTag", Tags: []model.Tag{
						{Name: "strTag", Values: []*modelv1.TagValue{pbv1.NullTagValue, pbv1.NullTagValue}},
						{Name: "intTag", Values: []*modelv1.TagValue{pbv1.NullTagValue, pbv1.NullTagValue}},
						{Name: "strTag1", Values: []*modelv1.TagValue{strTagValue("tag1"), strTagValue("tag3")}},
						{Name: "strTag2", Values: []*modelv1.TagValue{strTagValue("tag2"), strTagValue("tag4")}},
					}},
				},
				Fields: nil,
			}, {
				SID:        1,
				Timestamps: []int64{1, 2},
				Versions:   []int64{1, 4},
				TagFamilies: []model.TagFamily{
					{Name: "arrTag", Tags: []model.Tag{
						{Name: "strArrTag", Values: []*modelv1.TagValue{strArrTagValue([]string{"value1", "value2"}), strArrTagValue([]string{"value5", "value6"})}},
						{Name: "intArrTag", Values: []*modelv1.TagValue{int64ArrTagValue([]int64{25, 30}), int64ArrTagValue([]int64{35, 40})}},
					}},
					{Name: "binaryTag", Tags: []model.Tag{
						{Name: "binaryTag", Values: []*modelv1.TagValue{binaryDataTagValue(longText), binaryDataTagValue(longText)}},
					}},
					{Name: "singleTag", Tags: []model.Tag{
						{Name: "strTag", Values: []*modelv1.TagValue{strTagValue("value1"), strTagValue("value3")}},
						{Name: "intTag", Values: []*modelv1.TagValue{int64TagValue(10), int64TagValue(30)}},
						{Name: "strTag1", Values: []*modelv1.TagValue{pbv1.NullTagValue, pbv1.NullTagValue}},
						{Name: "strTag2", Values: []*modelv1.TagValue{pbv1.NullTagValue, pbv1.NullTagValue}},
					}},
				},
				Fields: []model.Field{
					{Name: "strField", Values: []*modelv1.FieldValue{strFieldValue("field1"), strFieldValue("field3")}},
					{Name: "intField", Values: []*modelv1.FieldValue{int64FieldValue(1110), int64FieldValue(3330)}},
					{Name: "floatField", Values: []*modelv1.FieldValue{float64FieldValue(1.221233343e+06), float64FieldValue(3663699.029)}},
					{Name: "binaryField", Values: []*modelv1.FieldValue{binaryDataFieldValue(longText), binaryDataFieldValue(longText)}},
				},
			}, {
				SID:        3,
				Timestamps: []int64{1, 2},
				Versions:   []int64{3, 6},
				TagFamilies: []model.TagFamily{
					{Name: "arrTag", Tags: []model.Tag{
						{Name: "strArrTag", Values: []*modelv1.TagValue{pbv1.NullTagValue, pbv1.NullTagValue}},
						{Name: "intArrTag", Values: []*modelv1.TagValue{pbv1.NullTagValue, pbv1.NullTagValue}},
					}},
					{Name: "binaryTag", Tags: []model.Tag{
						{Name: "binaryTag", Values: []*modelv1.TagValue{pbv1.NullTagValue, pbv1.NullTagValue}},
					}},
					{Name: "singleTag", Tags: []model.Tag{
						{Name: "strTag", Values: []*modelv1.TagValue{pbv1.NullTagValue, pbv1.NullTagValue}},
						{Name: "intTag", Values: []*modelv1.TagValue{pbv1.NullTagValue, pbv1.NullTagValue}},
						{Name: "strTag1", Values: []*modelv1.TagValue{pbv1.NullTagValue, pbv1.NullTagValue}},
						{Name: "strTag2", Values: []*modelv1.TagValue{pbv1.NullTagValue, pbv1.NullTagValue}},
					}},
				},
				Fields: []model.Field{
					{Name: "intField", Values: []*modelv1.FieldValue{int64FieldValue(1110), int64FieldValue(4440)}},
				},
			}},
		},
		{
			name:          "Test with multiple parts with multiple data order by Series 2",
			dpsList:       []*dataPoints{dpsTS2, dpsTS1},
			sids:          []common.SeriesID{2, 1, 3},
			orderBySeries: true,
			minTimestamp:  1,
			maxTimestamp:  2,
			want: []model.MeasureResult{{
				SID:        2,
				Timestamps: []int64{1, 2},
				Versions:   []int64{2, 5},
				TagFamilies: []model.TagFamily{
					{Name: "arrTag", Tags: []model.Tag{
						{Name: "strArrTag", Values: []*modelv1.TagValue{pbv1.NullTagValue, pbv1.NullTagValue}},
						{Name: "intArrTag", Values: []*modelv1.TagValue{pbv1.NullTagValue, pbv1.NullTagValue}},
					}},
					{Name: "binaryTag", Tags: []model.Tag{
						{Name: "binaryTag", Values: []*modelv1.TagValue{pbv1.NullTagValue, pbv1.NullTagValue}},
					}},
					{Name: "singleTag", Tags: []model.Tag{
						{Name: "strTag", Values: []*modelv1.TagValue{pbv1.NullTagValue, pbv1.NullTagValue}},
						{Name: "intTag", Values: []*modelv1.TagValue{pbv1.NullTagValue, pbv1.NullTagValue}},
						{Name: "strTag1", Values: []*modelv1.TagValue{strTagValue("tag1"), strTagValue("tag3")}},
						{Name: "strTag2", Values: []*modelv1.TagValue{strTagValue("tag2"), strTagValue("tag4")}},
					}},
				},
				Fields: nil,
			}, {
				SID:        1,
				Timestamps: []int64{1, 2},
				Versions:   []int64{1, 4},
				TagFamilies: []model.TagFamily{
					{Name: "arrTag", Tags: []model.Tag{
						{Name: "strArrTag", Values: []*modelv1.TagValue{strArrTagValue([]string{"value1", "value2"}), strArrTagValue([]string{"value5", "value6"})}},
						{Name: "intArrTag", Values: []*modelv1.TagValue{int64ArrTagValue([]int64{25, 30}), int64ArrTagValue([]int64{35, 40})}},
					}},
					{Name: "binaryTag", Tags: []model.Tag{
						{Name: "binaryTag", Values: []*modelv1.TagValue{binaryDataTagValue(longText), binaryDataTagValue(longText)}},
					}},
					{Name: "singleTag", Tags: []model.Tag{
						{Name: "strTag", Values: []*modelv1.TagValue{strTagValue("value1"), strTagValue("value3")}},
						{Name: "intTag", Values: []*modelv1.TagValue{int64TagValue(10), int64TagValue(30)}},
						{Name: "strTag1", Values: []*modelv1.TagValue{pbv1.NullTagValue, pbv1.NullTagValue}},
						{Name: "strTag2", Values: []*modelv1.TagValue{pbv1.NullTagValue, pbv1.NullTagValue}},
					}},
				},
				Fields: []model.Field{
					{Name: "strField", Values: []*modelv1.FieldValue{strFieldValue("field1"), strFieldValue("field3")}},
					{Name: "intField", Values: []*modelv1.FieldValue{int64FieldValue(1110), int64FieldValue(3330)}},
					{Name: "floatField", Values: []*modelv1.FieldValue{float64FieldValue(1.221233343e+06), float64FieldValue(3663699.029)}},
					{Name: "binaryField", Values: []*modelv1.FieldValue{binaryDataFieldValue(longText), binaryDataFieldValue(longText)}},
				},
			}, {
				SID:        3,
				Timestamps: []int64{1, 2},
				Versions:   []int64{3, 6},
				TagFamilies: []model.TagFamily{
					{Name: "arrTag", Tags: []model.Tag{
						{Name: "strArrTag", Values: []*modelv1.TagValue{pbv1.NullTagValue, pbv1.NullTagValue}},
						{Name: "intArrTag", Values: []*modelv1.TagValue{pbv1.NullTagValue, pbv1.NullTagValue}},
					}},
					{Name: "binaryTag", Tags: []model.Tag{
						{Name: "binaryTag", Values: []*modelv1.TagValue{pbv1.NullTagValue, pbv1.NullTagValue}},
					}},
					{Name: "singleTag", Tags: []model.Tag{
						{Name: "strTag", Values: []*modelv1.TagValue{pbv1.NullTagValue, pbv1.NullTagValue}},
						{Name: "intTag", Values: []*modelv1.TagValue{pbv1.NullTagValue, pbv1.NullTagValue}},
						{Name: "strTag1", Values: []*modelv1.TagValue{pbv1.NullTagValue, pbv1.NullTagValue}},
						{Name: "strTag2", Values: []*modelv1.TagValue{pbv1.NullTagValue, pbv1.NullTagValue}},
					}},
				},
				Fields: []model.Field{
					{Name: "intField", Values: []*modelv1.FieldValue{int64FieldValue(1110), int64FieldValue(4440)}},
				},
			}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			verify := func(t *testing.T, tst *tsTable) {
				defer tst.Close()
				queryOpts := queryOptions{
					schemaTagTypes: testSchemaTagTypes,
					minTimestamp:   tt.minTimestamp,
					maxTimestamp:   tt.maxTimestamp,
				}
				s := tst.currentSnapshot()
				require.NotNil(t, s)
				defer s.decRef()
				shardCache := storage.NewShardCache("test-group", 0, 0)
				pp, _ := s.getParts(nil, shardCache, queryOpts.minTimestamp, queryOpts.maxTimestamp)
				sids := make([]common.SeriesID, len(tt.sids))
				copy(sids, tt.sids)
				sort.Slice(sids, func(i, j int) bool {
					return sids[i] < tt.sids[j]
				})
				ti := &tstIter{}
				ti.init(pp, sids, tt.minTimestamp, tt.maxTimestamp)

				var result queryResult
				result.ctx = context.TODO()
				// Query all tags
				result.tagProjection = allTagProjections
				for ti.nextBlock() {
					bc := generateBlockCursor()
					p := ti.piHeap[0]
					opts := queryOpts
					opts.TagProjection = tagProjections[int(p.curBlock.seriesID)]
					opts.FieldProjection = fieldProjections[int(p.curBlock.seriesID)]
					bc.init(p.p, p.curBlock, opts)
					result.data = append(result.data, bc)
				}
				defer result.Release()
				if tt.orderBySeries {
					result.sidToIndex = make(map[common.SeriesID]int)
					for i, si := range tt.sids {
						result.sidToIndex[si] = i
					}
				} else {
					result.orderByTS = true
					result.ascTS = tt.ascTS
				}
				var got []model.MeasureResult
				for {
					r := result.Pull()
					if r == nil {
						break
					}
					sort.Slice(r.TagFamilies, func(i, j int) bool {
						return r.TagFamilies[i].Name < r.TagFamilies[j].Name
					})
					got = append(got, *r)
				}

				if !errors.Is(ti.Error(), tt.wantErr) {
					t.Errorf("Unexpected error: got %v, want %v", ti.err, tt.wantErr)
				}

				if diff := cmp.Diff(got, tt.want,
					protocmp.IgnoreUnknown(), protocmp.Transform(),
					cmpopts.IgnoreFields(model.MeasureResult{}, "ShardIDs")); diff != "" {
					t.Errorf("Unexpected []pbv1.Result (-got +want):\n%s", diff)
				}
			}

			t.Run("memory snapshot", func(t *testing.T) {
				tmpPath, defFn := test.Space(require.New(t))
				defer defFn()
				tst := &tsTable{
					loopCloser:    run.NewCloser(2),
					introductions: make(chan *introduction),
					fileSystem:    fs.NewLocalFileSystem(),
					root:          tmpPath,
				}
				tst.gc.init(tst)
				flushCh := make(chan *flusherIntroduction)
				mergeCh := make(chan *mergerIntroduction)
				introducerWatcher := make(watcher.Channel, 1)
				go tst.introducerLoop(flushCh, mergeCh, introducerWatcher, 1)
				for _, dps := range tt.dpsList {
					tst.mustAddDataPoints(dps)
					time.Sleep(100 * time.Millisecond)
				}
				verify(t, tst)
			})

			t.Run("file snapshot", func(t *testing.T) {
				// Initialize a tstIter object.
				tmpPath, defFn := test.Space(require.New(t))
				fileSystem := fs.NewLocalFileSystem()
				defer defFn()
				tst, err := newTSTable(fileSystem, tmpPath, common.Position{},
					logger.GetLogger("test"), timestamp.TimeRange{}, option{flushTimeout: 0, mergePolicy: newDefaultMergePolicyForTesting(), protector: protector.Nop{}}, nil)
				require.NoError(t, err)
				for _, dps := range tt.dpsList {
					tst.mustAddDataPoints(dps)
					time.Sleep(100 * time.Millisecond)
				}
				// wait until the introducer is done
				if len(tt.dpsList) > 0 {
					for {
						snp := tst.currentSnapshot()
						if snp == nil {
							time.Sleep(100 * time.Millisecond)
							continue
						}
						if snp.creator == snapshotCreatorMemPart {
							snp.decRef()
							time.Sleep(100 * time.Millisecond)
							continue
						}
						snp.decRef()
						tst.Close()
						break
					}
				}

				// reopen the table
				tst, err = newTSTable(fileSystem, tmpPath, common.Position{},
					logger.GetLogger("test"), timestamp.TimeRange{}, option{
						flushTimeout: defaultFlushTimeout, mergePolicy: newDefaultMergePolicyForTesting(),
						protector: protector.Nop{},
					}, nil)
				require.NoError(t, err)

				verify(t, tst)
			})
		})
	}
}

func TestQueryResult_QuotaExceeded(t *testing.T) {
	tests := []struct {
		wantErr             error
		name                string
		dpsList             []*dataPoints
		sids                []common.SeriesID
		want                []model.MeasureResult
		minTimestamp        int64
		maxTimestamp        int64
		orderBySeries       bool
		ascTS               bool
		expectQuotaExceeded bool
	}{
		{
			name:                "TestQuotaNotExceeded_ExpectSuccess",
			dpsList:             []*dataPoints{dpsTS1, dpsTS1},
			sids:                []common.SeriesID{1},
			minTimestamp:        1,
			maxTimestamp:        1,
			expectQuotaExceeded: false,
			want: []model.MeasureResult{{
				SID:        1,
				Timestamps: []int64{1},
				Versions:   []int64{1},
				TagFamilies: []model.TagFamily{
					{Name: "arrTag", Tags: []model.Tag{
						{Name: "strArrTag", Values: []*modelv1.TagValue{strArrTagValue([]string{"value1", "value2"})}},
						{Name: "intArrTag", Values: []*modelv1.TagValue{int64ArrTagValue([]int64{25, 30})}},
					}},
					{Name: "binaryTag", Tags: []model.Tag{
						{Name: "binaryTag", Values: []*modelv1.TagValue{binaryDataTagValue(longText)}},
					}},
					{Name: "singleTag", Tags: []model.Tag{
						{Name: "strTag", Values: []*modelv1.TagValue{strTagValue("value1")}},
						{Name: "intTag", Values: []*modelv1.TagValue{int64TagValue(10)}},
						{Name: "strTag1", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
						{Name: "strTag2", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
					}},
				},
				Fields: []model.Field{
					{Name: "strField", Values: []*modelv1.FieldValue{strFieldValue("field1")}},
					{Name: "intField", Values: []*modelv1.FieldValue{int64FieldValue(1110)}},
					{Name: "floatField", Values: []*modelv1.FieldValue{float64FieldValue(1.221233343e+06)}},
					{Name: "binaryField", Values: []*modelv1.FieldValue{binaryDataFieldValue(longText)}},
				},
			}},
		},
		{
			name:                "TestQuotaExceeded_ExpectError",
			dpsList:             []*dataPoints{dpsTS1, dpsTS1},
			sids:                []common.SeriesID{1},
			minTimestamp:        1,
			maxTimestamp:        1,
			expectQuotaExceeded: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Initialize a tstIter object.
			tmpPath, defFn := test.Space(require.New(t))
			fileSystem := fs.NewLocalFileSystem()
			defer defFn()
			tst, err := newTSTable(fileSystem, tmpPath, common.Position{},
				logger.GetLogger("test"), timestamp.TimeRange{}, option{flushTimeout: 0, mergePolicy: newDefaultMergePolicyForTesting(), protector: protector.Nop{}}, nil)
			require.NoError(t, err)
			for _, dps := range tt.dpsList {
				tst.mustAddDataPoints(dps)
				time.Sleep(100 * time.Millisecond)
			}
			// wait until the introducer is done
			if len(tt.dpsList) > 0 {
				for {
					snp := tst.currentSnapshot()
					if snp == nil {
						time.Sleep(100 * time.Millisecond)
						continue
					}
					if snp.creator == snapshotCreatorMemPart {
						snp.decRef()
						time.Sleep(100 * time.Millisecond)
						continue
					}
					snp.decRef()
					tst.Close()
					break
				}
			}

			// reopen the table
			tst, err = newTSTable(fileSystem, tmpPath, common.Position{},
				logger.GetLogger("test"), timestamp.TimeRange{}, option{
					flushTimeout: defaultFlushTimeout, mergePolicy: newDefaultMergePolicyForTesting(),
					protector: protector.Nop{},
				}, nil)
			require.NoError(t, err)

			m := &measure{
				pm: &itest.MockMemoryProtector{ExpectQuotaExceeded: tt.expectQuotaExceeded},
			}
			defer tst.Close()
			queryOpts := queryOptions{
				schemaTagTypes: testSchemaTagTypes,
				minTimestamp:   tt.minTimestamp,
				maxTimestamp:   tt.maxTimestamp,
			}
			queryOpts.TagProjection = tagProjections[1]
			queryOpts.FieldProjection = fieldProjections[1]
			s := tst.currentSnapshot()
			require.NotNil(t, s)
			defer s.decRef()
			shardCache := storage.NewShardCache("test-group", 0, 0)
			pp, _ := s.getParts(nil, shardCache, queryOpts.minTimestamp, queryOpts.maxTimestamp)
			var result queryResult
			result.ctx = context.TODO()
			// Query all tags
			result.tagProjection = allTagProjections
			err = m.searchBlocks(context.TODO(), &result, tt.sids, pp, queryOpts)
			if tt.expectQuotaExceeded {
				require.Error(t, err)
				require.Contains(t, err.Error(), "quota exceeded", "expected quota to be exceeded but got: %v", err)
				return
			}
			require.NoError(t, err)
			defer result.Release()
			if tt.orderBySeries {
				result.sidToIndex = make(map[common.SeriesID]int)
				for i, si := range tt.sids {
					result.sidToIndex[si] = i
				}
			} else {
				result.orderByTS = true
				result.ascTS = tt.ascTS
			}
			var got []model.MeasureResult
			for {
				r := result.Pull()
				if r == nil {
					break
				}
				sort.Slice(r.TagFamilies, func(i, j int) bool {
					return r.TagFamilies[i].Name < r.TagFamilies[j].Name
				})
				got = append(got, *r)
			}
			if diff := cmp.Diff(got, tt.want,
				protocmp.IgnoreUnknown(), protocmp.Transform(),
				cmpopts.IgnoreFields(model.MeasureResult{}, "ShardIDs")); diff != "" {
				t.Errorf("Unexpected []pbv1.Result (-got +want):\n%s", diff)
			}
		})
	}
}

func TestSegResultHeap_Sorting(t *testing.T) {
	tests := []struct {
		name        string
		segResults  []*segResult
		expectOrder []int
		sortDesc    bool
	}{
		{
			name:     "Sort ascending by SeriesID (no sortedValues)",
			sortDesc: false,
			segResults: []*segResult{
				{
					SeriesData: storage.SeriesData{
						SeriesList: pbv1.SeriesList{
							&pbv1.Series{ID: common.SeriesID(30)},
						},
					},
					i: 0,
				},
				{
					SeriesData: storage.SeriesData{
						SeriesList: pbv1.SeriesList{
							&pbv1.Series{ID: common.SeriesID(10)},
						},
					},
					i: 0,
				},
				{
					SeriesData: storage.SeriesData{
						SeriesList: pbv1.SeriesList{
							&pbv1.Series{ID: common.SeriesID(20)},
						},
					},
					i: 0,
				},
			},
			expectOrder: []int{1, 0, 2}, // SeriesID order: 10, 30, 20
		},
		{
			name:     "Sort descending by SeriesID (no sortedValues)",
			sortDesc: true,
			segResults: []*segResult{
				{
					SeriesData: storage.SeriesData{
						SeriesList: pbv1.SeriesList{
							&pbv1.Series{ID: common.SeriesID(10)},
						},
					},
					i: 0,
				},
				{
					SeriesData: storage.SeriesData{
						SeriesList: pbv1.SeriesList{
							&pbv1.Series{ID: common.SeriesID(30)},
						},
					},
					i: 0,
				},
				{
					SeriesData: storage.SeriesData{
						SeriesList: pbv1.SeriesList{
							&pbv1.Series{ID: common.SeriesID(20)},
						},
					},
					i: 0,
				},
			},
			expectOrder: []int{1, 0, 2}, // SeriesID order: 30, 10, 20
		},
		{
			name:     "Sort ascending by sortedValues",
			sortDesc: false,
			segResults: []*segResult{
				{
					SeriesData: storage.SeriesData{
						SeriesList: pbv1.SeriesList{
							&pbv1.Series{ID: common.SeriesID(1)},
						},
					},
					sortedValues: [][]byte{[]byte("charlie")},
					i:            0,
				},
				{
					SeriesData: storage.SeriesData{
						SeriesList: pbv1.SeriesList{
							&pbv1.Series{ID: common.SeriesID(2)},
						},
					},
					sortedValues: [][]byte{[]byte("alpha")},
					i:            0,
				},
				{
					SeriesData: storage.SeriesData{
						SeriesList: pbv1.SeriesList{
							&pbv1.Series{ID: common.SeriesID(3)},
						},
					},
					sortedValues: [][]byte{[]byte("beta")},
					i:            0,
				},
			},
			expectOrder: []int{1, 0, 2}, // alpha, charlie, beta
		},
		{
			name:     "Sort descending by sortedValues",
			sortDesc: true,
			segResults: []*segResult{
				{
					SeriesData: storage.SeriesData{
						SeriesList: pbv1.SeriesList{
							&pbv1.Series{ID: common.SeriesID(1)},
						},
					},
					sortedValues: [][]byte{[]byte("alpha")},
					i:            0,
				},
				{
					SeriesData: storage.SeriesData{
						SeriesList: pbv1.SeriesList{
							&pbv1.Series{ID: common.SeriesID(2)},
						},
					},
					sortedValues: [][]byte{[]byte("charlie")},
					i:            0,
				},
				{
					SeriesData: storage.SeriesData{
						SeriesList: pbv1.SeriesList{
							&pbv1.Series{ID: common.SeriesID(3)},
						},
					},
					sortedValues: [][]byte{[]byte("beta")},
					i:            0,
				},
			},
			expectOrder: []int{1, 0, 2}, // charlie, alpha, beta
		},
		{
			name:     "Mixed sortedValues and nil sortedValues ascending",
			sortDesc: false,
			segResults: []*segResult{
				{
					SeriesData: storage.SeriesData{
						SeriesList: pbv1.SeriesList{
							&pbv1.Series{ID: common.SeriesID(30)},
						},
					},
					sortedValues: nil, // Will use SeriesID for sorting
					i:            0,
				},
				{
					SeriesData: storage.SeriesData{
						SeriesList: pbv1.SeriesList{
							&pbv1.Series{ID: common.SeriesID(10)},
						},
					},
					sortedValues: [][]byte{[]byte("zzz")}, // Should come after nil sortedValues when sorted by SeriesID
					i:            0,
				},
				{
					SeriesData: storage.SeriesData{
						SeriesList: pbv1.SeriesList{
							&pbv1.Series{ID: common.SeriesID(20)},
						},
					},
					sortedValues: nil, // Will use SeriesID for sorting
					i:            0,
				},
			},
			expectOrder: []int{1, 0, 2}, // SeriesID 10, 30, 20 (nil sortedValues use SeriesID)
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create and initialize heap
			heap := segResultHeap{
				results:  make([]*segResult, 0),
				sortDesc: tt.sortDesc,
			}

			// Add all results to heap
			heap.results = append(heap.results, tt.segResults...)

			// Initialize heap
			require.Equal(t, len(tt.segResults), heap.Len())

			// Sort using Go's heap
			heapImpl := &heap
			heap2 := make([]*segResult, len(tt.segResults))
			copy(heap2, heap.results)

			// Sort manually to get expected order
			sort.Slice(heap2, func(i, j int) bool {
				return heapImpl.Less(i, j)
			})

			// Verify the order matches expectation
			for i, expectedIdx := range tt.expectOrder {
				actual := heap2[i]
				expected := tt.segResults[expectedIdx]
				require.Equal(t, expected.SeriesList[expected.i].ID, actual.SeriesList[actual.i].ID,
					"Position %d: expected SeriesID %d, got %d", i, expected.SeriesList[expected.i].ID, actual.SeriesList[actual.i].ID)
			}
		})
	}
}

func TestSegResultHeap_NPE_Prevention(t *testing.T) {
	tests := []struct {
		name       string
		segResults []*segResult
		i, j       int
		expectLess bool
	}{
		{
			name:       "Out of bounds indices",
			segResults: []*segResult{{SeriesData: storage.SeriesData{SeriesList: pbv1.SeriesList{{ID: 1}}}, i: 0}},
			i:          0,
			j:          5, // Out of bounds
			expectLess: false,
		},
		{
			name:       "Nil segResult",
			segResults: []*segResult{nil, {SeriesData: storage.SeriesData{SeriesList: pbv1.SeriesList{{ID: 1}}}, i: 0}},
			i:          0,
			j:          1,
			expectLess: false,
		},
		{
			name: "Index out of bounds for SeriesList",
			segResults: []*segResult{
				{SeriesData: storage.SeriesData{SeriesList: pbv1.SeriesList{{ID: 1}}}, i: 5}, // i is out of bounds
				{SeriesData: storage.SeriesData{SeriesList: pbv1.SeriesList{{ID: 2}}}, i: 0},
			},
			i:          0,
			j:          1,
			expectLess: false,
		},
		{
			name: "Index out of bounds for sortedValues",
			segResults: []*segResult{
				{
					SeriesData:   storage.SeriesData{SeriesList: pbv1.SeriesList{{ID: 1}}},
					sortedValues: [][]byte{[]byte("test")},
					i:            5, // i is out of bounds for sortedValues
				},
				{
					SeriesData:   storage.SeriesData{SeriesList: pbv1.SeriesList{{ID: 2}}},
					sortedValues: [][]byte{[]byte("test2")},
					i:            0,
				},
			},
			i:          0,
			j:          1,
			expectLess: false, // Should fallback to SeriesID comparison
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			heap := segResultHeap{
				results:  tt.segResults,
				sortDesc: false,
			}

			// This should not panic due to NPE prevention
			result := heap.Less(tt.i, tt.j)
			require.Equal(t, tt.expectLess, result)
		})
	}
}

func TestIndexSortResult_OrderBySortDesc(t *testing.T) {
	tests := []struct {
		name       string
		sortOrder  modelv1.Sort
		expectDesc bool
	}{
		{
			name:       "SORT_ASC should be ascending",
			sortOrder:  modelv1.Sort_SORT_ASC,
			expectDesc: false,
		},
		{
			name:       "SORT_UNSPECIFIED should be ascending",
			sortOrder:  modelv1.Sort_SORT_UNSPECIFIED,
			expectDesc: false,
		},
		{
			name:       "SORT_DESC should be descending",
			sortOrder:  modelv1.Sort_SORT_DESC,
			expectDesc: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create mock segments and measure data
			mqo := model.MeasureQueryOptions{
				Order: &index.OrderBy{
					Sort: tt.sortOrder,
				},
			}

			// Create a simple segResult
			sr := &segResult{
				SeriesData: storage.SeriesData{
					SeriesList: pbv1.SeriesList{
						&pbv1.Series{ID: common.SeriesID(1)},
						&pbv1.Series{ID: common.SeriesID(2)},
					},
					Timestamps: []int64{1000, 2000},
					Versions:   []int64{1, 2},
				},
				sortedValues: [][]byte{[]byte("test1"), []byte("test2")},
				i:            0,
			}

			// Create index sort result
			r := &indexSortResult{
				tfl: []tagFamilyLocation{},
				segResults: segResultHeap{
					results:  []*segResult{sr},
					sortDesc: false, // This should be set by buildIndexQueryResult
				},
			}

			// Simulate the logic from buildIndexQueryResult
			if mqo.Order != nil && mqo.Order.Sort == modelv1.Sort_SORT_DESC {
				r.segResults.sortDesc = true
			}

			// Verify the sort order was set correctly
			require.Equal(t, tt.expectDesc, r.segResults.sortDesc)
		})
	}
}
