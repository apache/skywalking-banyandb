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

package stream

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

func Test_memPart_mustInitFromDataPoints(t *testing.T) {
	tests := []struct {
		dps  *dataPoints
		name string
		want partMetadata
	}{
		{
			name: "Test with empty dataPoints",
			dps: &dataPoints{
				timestamps:  []int64{},
				elementIDs:  []string{},
				seriesIDs:   []common.SeriesID{},
				tagFamilies: make([][]nameValues, 0),
			},
			want: partMetadata{},
		},
		{
			name: "Test with one item in dataPoints",
			dps: &dataPoints{
				timestamps: []int64{1},
				elementIDs: []string{"0"},
				seriesIDs:  []common.SeriesID{1},
				tagFamilies: [][]nameValues{
					{
						{
							"arrTag", []*nameValue{
								{name: "strArrTag", valueType: pbv1.ValueTypeStrArr, value: nil, valueArr: [][]byte{[]byte("value1"), []byte("value2")}},
								{name: "intArrTag", valueType: pbv1.ValueTypeInt64Arr, value: nil, valueArr: [][]byte{convert.Int64ToBytes(25), convert.Int64ToBytes(30)}},
							},
						},
					},
				},
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
	elementIDs: []string{"0", "1", "2", "3", "4", "5"},
	tagFamilies: [][]nameValues{
		{
			{
				name: "arrTag", values: []*nameValue{
					{name: "strArrTag", valueType: pbv1.ValueTypeStrArr, value: nil, valueArr: [][]byte{[]byte("value1"), []byte("value2")}},
					{name: "intArrTag", valueType: pbv1.ValueTypeInt64Arr, value: nil, valueArr: [][]byte{convert.Int64ToBytes(25), convert.Int64ToBytes(30)}},
				},
			},
			{
				name: "binaryTag", values: []*nameValue{
					{name: "binaryTag", valueType: pbv1.ValueTypeBinaryData, value: longText, valueArr: nil},
				},
			},
			{
				name: "singleTag", values: []*nameValue{
					{name: "strTag", valueType: pbv1.ValueTypeStr, value: []byte("value1"), valueArr: nil},
					{name: "intTag", valueType: pbv1.ValueTypeInt64, value: convert.Int64ToBytes(10), valueArr: nil},
				},
			},
		},
		{
			{
				name: "arrTag", values: []*nameValue{
					{name: "strArrTag", valueType: pbv1.ValueTypeStrArr, value: nil, valueArr: [][]byte{[]byte("value3"), []byte("value4")}},
					{name: "intArrTag", valueType: pbv1.ValueTypeInt64Arr, value: nil, valueArr: [][]byte{convert.Int64ToBytes(50), convert.Int64ToBytes(60)}},
				},
			},
			{
				name: "binaryTag", values: []*nameValue{
					{name: "binaryTag", valueType: pbv1.ValueTypeBinaryData, value: longText, valueArr: nil},
				},
			},
			{
				name: "singleTag", values: []*nameValue{
					{name: "strTag", valueType: pbv1.ValueTypeStr, value: []byte("value2"), valueArr: nil},
					{name: "intTag", valueType: pbv1.ValueTypeInt64, value: convert.Int64ToBytes(20), valueArr: nil},
				},
			},
		},
		{
			{
				name: "singleTag", values: []*nameValue{
					{name: "strTag", valueType: pbv1.ValueTypeStr, value: []byte("tag1"), valueArr: nil},
					{name: "strTag", valueType: pbv1.ValueTypeInt64, value: []byte("tag2"), valueArr: nil},
				},
			},
		},
		{
			{
				name: "singleTag", values: []*nameValue{
					{name: "strTag", valueType: pbv1.ValueTypeStr, value: []byte("tag11"), valueArr: nil},
					{name: "strTag", valueType: pbv1.ValueTypeInt64, value: []byte("tag22"), valueArr: nil},
				},
			},
		},
		{},
		{}, // empty tagFamilies for seriesID 3
	},
}
