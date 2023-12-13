// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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

	"github.com/stretchr/testify/assert"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

func Test_memPart_mustInitFromDataPoints(t *testing.T) {
	tests := []struct {
		name string
		dps  *dataPoints
		want partMetadata
	}{
		{
			name: "Test with empty dataPoints",
			dps: &dataPoints{
				timestamps:  []int64{},
				seriesIDs:   []common.SeriesID{},
				tagFamilies: make([][]nameValues, 0),
				fields:      make([]nameValues, 0),
			},
			want: partMetadata{},
		},
		{
			name: "Test with one item in dataPoints",
			dps: &dataPoints{
				timestamps: []int64{1},
				seriesIDs:  []common.SeriesID{1},
				tagFamilies: [][]nameValues{{{
					"arrTag", []*nameValue{
						{"strArrTag", pbv1.ValueTypeStrArr, nil, [][]byte{[]byte("value1"), []byte("value2")}},
						{"intArrTag", pbv1.ValueTypeInt64Arr, nil, [][]byte{convert.Int64ToBytes(25), convert.Int64ToBytes(30)}},
					},
				}}},
				fields: []nameValues{{
					"skipped", []*nameValue{
						{"intField", pbv1.ValueTypeInt64, convert.Int64ToBytes(1110), nil},
					},
				}},
			},
			want: partMetadata{
				BlocksCount:  1,
				MinTimestamp: 1,
				MaxTimestamp: 1,
				TotalCount:   1,
			},
		},
		{
			name: "Test with multiple items in dataPoints",
			dps:  dps,
			want: partMetadata{
				BlocksCount:  3,
				MinTimestamp: 1,
				MaxTimestamp: 220,
				TotalCount:   6,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mp := &memPart{}
			mp.mustInitFromDataPoints(tt.dps)
			assert.Equal(t, tt.want.BlocksCount, mp.partMetadata.BlocksCount)
			assert.Equal(t, tt.want.MinTimestamp, mp.partMetadata.MinTimestamp)
			assert.Equal(t, tt.want.MaxTimestamp, mp.partMetadata.MaxTimestamp)
			assert.Equal(t, tt.want.TotalCount, mp.partMetadata.TotalCount)
		})
	}
}

var dps = &dataPoints{
	seriesIDs:  []common.SeriesID{1, 1, 2, 2, 3, 3},
	timestamps: []int64{1, 2, 8, 10, 100, 220},
	tagFamilies: [][]nameValues{
		{
			{
				"arrTag", []*nameValue{
					{"strArrTag", pbv1.ValueTypeStrArr, nil, [][]byte{[]byte("value1"), []byte("value2")}},
					{"intArrTag", pbv1.ValueTypeInt64Arr, nil, [][]byte{convert.Int64ToBytes(25), convert.Int64ToBytes(30)}},
				},
			}, {
				"binaryTag", []*nameValue{
					{"binaryTag", pbv1.ValueTypeBinaryData, []byte(longText), nil},
				},
			}, {
				"singleTag", []*nameValue{
					{"strTag", pbv1.ValueTypeStr, []byte("value1"), nil},
					{"intTag", pbv1.ValueTypeInt64, convert.Int64ToBytes(10), nil},
					{"floatTag", pbv1.ValueTypeFloat64, convert.Float64ToBytes(12233.343), nil},
				},
			},
		},
		{
			{
				"arrTag", []*nameValue{
					{"strArrTag", pbv1.ValueTypeStrArr, nil, [][]byte{[]byte("value3"), []byte("value4")}},
					{"intArrTag", pbv1.ValueTypeInt64Arr, nil, [][]byte{convert.Int64ToBytes(50), convert.Int64ToBytes(60)}},
				},
			}, {
				"binaryTag", []*nameValue{
					{"binaryTag", pbv1.ValueTypeBinaryData, []byte(longText), nil},
				},
			}, {
				"singleTag", []*nameValue{
					{"strTag", pbv1.ValueTypeStr, []byte("value2"), nil},
					{"intTag", pbv1.ValueTypeInt64, convert.Int64ToBytes(20), nil},
					{"floatTag", pbv1.ValueTypeFloat64, convert.Float64ToBytes(24466.686), nil},
				},
			},
		},
		{
			{
				"singleTag", []*nameValue{
					{"strTag", pbv1.ValueTypeStr, []byte("tag1"), nil},
					{"strTag", pbv1.ValueTypeInt64, []byte("tag2"), nil},
				},
			},
		},
		{
			{
				"singleTag", []*nameValue{
					{"strTag", pbv1.ValueTypeStr, []byte("tag11"), nil},
					{"strTag", pbv1.ValueTypeInt64, []byte("tag22"), nil},
				},
			},
		},
		{},
		{}, // empty tagFamilies for seriesID 3
	},
	fields: []nameValues{
		{
			"skipped", []*nameValue{
				{"strField", pbv1.ValueTypeStr, []byte("field1"), nil},
				{"intField", pbv1.ValueTypeInt64, convert.Int64ToBytes(1110), nil},
				{"floatField", pbv1.ValueTypeFloat64, convert.Float64ToBytes(1221233.343), nil},
				{"binaryField", pbv1.ValueTypeBinaryData, []byte(longText), nil},
			},
		},
		{
			"skipped", []*nameValue{
				{"strField", pbv1.ValueTypeStr, []byte("field2"), nil},
				{"intField", pbv1.ValueTypeInt64, convert.Int64ToBytes(2220), nil},
				{"floatField", pbv1.ValueTypeFloat64, convert.Float64ToBytes(2442466.686), nil},
				{"binaryField", pbv1.ValueTypeBinaryData, []byte(longText), nil},
			},
		},
		{},
		{}, // empty fields for seriesID 2
		{
			"onlyFields", []*nameValue{
				{"intField", pbv1.ValueTypeInt64, convert.Int64ToBytes(1110), nil},
			},
		},
		{
			"onlyFields", []*nameValue{
				{"intField", pbv1.ValueTypeInt64, convert.Int64ToBytes(2220), nil},
			},
		},
	},
}
