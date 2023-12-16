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

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

func Test_tsTable_mustAddDataPoints(t *testing.T) {
	tests := []struct {
		name    string
		dpsList []*dataPoints
		want    int
	}{
		{
			name: "Test with empty dataPoints",
			dpsList: []*dataPoints{
				{
					timestamps:  []int64{},
					seriesIDs:   []common.SeriesID{},
					tagFamilies: make([][]nameValues, 0),
					fields:      make([]nameValues, 0),
				},
			},
			want: 0,
		},
		{
			name: "Test with one item in dataPoints",
			dpsList: []*dataPoints{
				{
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
			},
			want: 1,
		},
		{
			name: "Test with multiple calls to mustAddDataPoints",
			dpsList: []*dataPoints{
				dps_ts1,
				dps_ts2,
			},
			want: 2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tst := &tsTable{}
			defer tst.Close()
			for _, dps := range tt.dpsList {
				tst.mustAddDataPoints(dps)
				time.Sleep(100 * time.Millisecond)
			}
			assert.Equal(t, tt.want, len(tst.memParts))
			var lastVersion int64
			for _, pw := range tst.memParts {
				if lastVersion == 0 {
					lastVersion = pw.p.partMetadata.Version
				} else {
					require.Less(t, lastVersion, pw.p.partMetadata.Version)
				}
			}
		})
	}
}

func Test_tstIter(t *testing.T) {
	tests := []struct {
		name         string
		dpsList      []*dataPoints
		sids         []common.SeriesID
		minTimestamp int64
		maxTimestamp int64
		want         []blockMetadata
		wantErr      error
	}{
		{
			name:         "Test with no data points",
			dpsList:      []*dataPoints{},
			sids:         []common.SeriesID{1, 2, 3},
			minTimestamp: 1,
			maxTimestamp: 1,
		},
		{
			name:         "Test with single part",
			dpsList:      []*dataPoints{dps_ts1},
			sids:         []common.SeriesID{1, 2, 3},
			minTimestamp: 1,
			maxTimestamp: 1,
			want: []blockMetadata{
				{seriesID: 1, count: 2, uncompressedSizeBytes: 3426},
				{seriesID: 2, count: 2, uncompressedSizeBytes: 106},
				{seriesID: 3, count: 2, uncompressedSizeBytes: 48},
			},
		},
		{
			name:         "Test with multiple parts with different ts",
			dpsList:      []*dataPoints{dps_ts1, dps_ts2},
			sids:         []common.SeriesID{1, 2, 3},
			minTimestamp: 1,
			maxTimestamp: 2,
			want: []blockMetadata{
				{seriesID: 1, count: 2, uncompressedSizeBytes: 3426},
				{seriesID: 1, count: 2, uncompressedSizeBytes: 3426},
				{seriesID: 2, count: 2, uncompressedSizeBytes: 106},
				{seriesID: 2, count: 2, uncompressedSizeBytes: 106},
				{seriesID: 3, count: 2, uncompressedSizeBytes: 48},
				{seriesID: 3, count: 2, uncompressedSizeBytes: 48},
			},
		},
		{
			name:         "Test with multiple parts with same ts",
			dpsList:      []*dataPoints{dps_ts1, dps_ts1},
			sids:         []common.SeriesID{1, 2, 3},
			minTimestamp: 1,
			maxTimestamp: 2,
			want: []blockMetadata{
				{seriesID: 1, count: 2, uncompressedSizeBytes: 3426},
				{seriesID: 1, count: 2, uncompressedSizeBytes: 3426},
				{seriesID: 2, count: 2, uncompressedSizeBytes: 106},
				{seriesID: 2, count: 2, uncompressedSizeBytes: 106},
				{seriesID: 3, count: 2, uncompressedSizeBytes: 48},
				{seriesID: 3, count: 2, uncompressedSizeBytes: 48},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tst := &tsTable{}
			defer tst.Close()
			for _, dps := range tt.dpsList {
				tst.mustAddDataPoints(dps)
				time.Sleep(100 * time.Millisecond)
			}
			pws, pp := tst.getParts(nil, nil, &QueryOptions{
				minTimestamp: tt.minTimestamp,
				maxTimestamp: tt.maxTimestamp,
			})
			defer func() {
				for _, pw := range pws {
					pw.decRef()
				}
			}()
			ti := &tstIter{}
			ti.init(pp, tt.sids, tt.minTimestamp, tt.maxTimestamp)
			var got []blockMetadata
			for ti.nextBlock() {
				if ti.piHeap[0].curBlock.seriesID == 0 {
					t.Errorf("Expected curBlock to be initialized, but it was nil")
				}
				got = append(got, ti.piHeap[0].curBlock)
			}

			if ti.Error() != tt.wantErr {
				t.Errorf("Unexpected error: got %v, want %v", ti.err, tt.wantErr)
			}

			if diff := cmp.Diff(got, tt.want,
				cmpopts.IgnoreFields(blockMetadata{}, "timestamps"),
				cmpopts.IgnoreFields(blockMetadata{}, "field"),
				cmpopts.IgnoreFields(blockMetadata{}, "tagFamilies"),
				cmp.AllowUnexported(blockMetadata{}),
			); diff != "" {
				t.Errorf("Unexpected blockMetadata (-got +want):\n%s", diff)
			}
		})
	}
}

var dps_ts1 = &dataPoints{
	seriesIDs:  []common.SeriesID{1, 2, 3},
	timestamps: []int64{1, 1, 1},
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
				},
			},
		},
		{
			{
				"singleTag", []*nameValue{
					{"strTag1", pbv1.ValueTypeStr, []byte("tag1"), nil},
					{"strTag2", pbv1.ValueTypeStr, []byte("tag2"), nil},
				},
			},
		},
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
		{}, // empty fields for seriesID 2
		{
			"onlyFields", []*nameValue{
				{"intField", pbv1.ValueTypeInt64, convert.Int64ToBytes(1110), nil},
			},
		},
	},
}

var dps_ts2 = &dataPoints{
	seriesIDs:  []common.SeriesID{1, 2, 3},
	timestamps: []int64{2, 2, 2},
	tagFamilies: [][]nameValues{
		{
			{
				"arrTag", []*nameValue{
					{"strArrTag", pbv1.ValueTypeStrArr, nil, [][]byte{[]byte("value5"), []byte("value6")}},
					{"intArrTag", pbv1.ValueTypeInt64Arr, nil, [][]byte{convert.Int64ToBytes(35), convert.Int64ToBytes(40)}},
				},
			}, {
				"binaryTag", []*nameValue{
					{"binaryTag", pbv1.ValueTypeBinaryData, []byte(longText), nil},
				},
			}, {
				"singleTag", []*nameValue{
					{"strTag", pbv1.ValueTypeStr, []byte("value3"), nil},
					{"intTag", pbv1.ValueTypeInt64, convert.Int64ToBytes(30), nil},
				},
			},
		},
		{
			{
				"singleTag", []*nameValue{
					{"strTag1", pbv1.ValueTypeStr, []byte("tag3"), nil},
					{"strTag2", pbv1.ValueTypeStr, []byte("tag4"), nil},
				},
			},
		},
		{}, // empty tagFamilies for seriesID 6
	},
	fields: []nameValues{
		{
			"skipped", []*nameValue{
				{"strField", pbv1.ValueTypeStr, []byte("field3"), nil},
				{"intField", pbv1.ValueTypeInt64, convert.Int64ToBytes(3330), nil},
				{"floatField", pbv1.ValueTypeFloat64, convert.Float64ToBytes(3663699.029), nil},
				{"binaryField", pbv1.ValueTypeBinaryData, []byte(longText), nil},
			},
		},
		{}, // empty fields for seriesID 5
		{
			"onlyFields", []*nameValue{
				{"intField", pbv1.ValueTypeInt64, convert.Int64ToBytes(4440), nil},
			},
		},
	},
}
