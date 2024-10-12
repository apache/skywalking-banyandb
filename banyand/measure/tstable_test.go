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
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/run"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/watcher"
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
					versions:    []int64{},
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
					versions:   []int64{1},
					seriesIDs:  []common.SeriesID{1},
					tagFamilies: [][]nameValues{
						{
							{
								name: "arrTag", values: []*nameValue{
									{name: "strArrTag", valueType: pbv1.ValueTypeStrArr, value: nil, valueArr: [][]byte{[]byte("value1"), []byte("value2")}},
									{name: "intArrTag", valueType: pbv1.ValueTypeInt64Arr, value: nil, valueArr: [][]byte{convert.Int64ToBytes(25), convert.Int64ToBytes(30)}},
								},
							},
						},
					},
					fields: []nameValues{
						{
							name: "skipped", values: []*nameValue{
								{name: "intField", valueType: pbv1.ValueTypeInt64, value: convert.Int64ToBytes(1110), valueArr: nil},
							},
						},
					},
				},
			},
			want: 1,
		},
		{
			name: "Test with multiple calls to mustAddDataPoints",
			dpsList: []*dataPoints{
				dpsTS1,
				dpsTS2,
			},
			want: 2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tst := &tsTable{
				loopCloser:    run.NewCloser(2),
				introductions: make(chan *introduction),
			}
			flushCh := make(chan *flusherIntroduction)
			mergeCh := make(chan *mergerIntroduction)
			introducerWatcher := make(watcher.Channel, 1)
			go tst.introducerLoop(flushCh, mergeCh, introducerWatcher, 1)
			defer tst.Close()
			for _, dps := range tt.dpsList {
				tst.mustAddDataPoints(dps)
				time.Sleep(100 * time.Millisecond)
			}
			s := tst.currentSnapshot()
			if s == nil {
				s = new(snapshot)
			}
			defer s.decRef()
			assert.Equal(t, tt.want, len(s.parts))
			var lastVersion uint64
			for _, pw := range s.parts {
				require.Greater(t, pw.ID(), uint64(0))
				if lastVersion == 0 {
					lastVersion = pw.ID()
				} else {
					require.Less(t, lastVersion, pw.ID())
				}
			}
		})
	}
}

func Test_tstIter(t *testing.T) {
	type testCtx struct {
		wantErr      error
		name         string
		dpsList      []*dataPoints
		sids         []common.SeriesID
		want         []blockMetadata
		minTimestamp int64
		maxTimestamp int64
	}
	bma := generateBlockMetadataArray()
	defer releaseBlockMetadataArray(bma)

	verify := func(t *testing.T, tt testCtx, tst *tsTable) uint64 {
		defer tst.Close()
		s := tst.currentSnapshot()
		if s == nil {
			s = new(snapshot)
		}
		defer s.decRef()
		pp, n := s.getParts(nil, tt.minTimestamp, tt.maxTimestamp)
		require.Equal(t, len(s.parts), n)
		ti := &tstIter{}
		ti.init(bma, pp, tt.sids, tt.minTimestamp, tt.maxTimestamp)
		var got []blockMetadata
		for ti.nextBlock() {
			if ti.piHeap[0].curBlock.seriesID == 0 {
				t.Errorf("Expected curBlock to be initialized, but it was nil")
			}
			var bm blockMetadata
			bm.copyFrom(ti.piHeap[0].curBlock)
			got = append(got, bm)
		}

		if !errors.Is(ti.Error(), tt.wantErr) {
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
		return s.epoch
	}

	t.Run("memory snapshot", func(t *testing.T) {
		tests := []testCtx{
			{
				name:         "Test with no data points",
				dpsList:      []*dataPoints{},
				sids:         []common.SeriesID{1, 2, 3},
				minTimestamp: 1,
				maxTimestamp: 1,
			},
			{
				name:         "Test with single part",
				dpsList:      []*dataPoints{dpsTS1},
				sids:         []common.SeriesID{1, 2, 3},
				minTimestamp: 1,
				maxTimestamp: 1,
				want: []blockMetadata{
					{seriesID: 1, count: 1, uncompressedSizeBytes: 1684},
					{seriesID: 2, count: 1, uncompressedSizeBytes: 63},
					{seriesID: 3, count: 1, uncompressedSizeBytes: 32},
				},
			},
			{
				name:         "Test with multiple parts with different ts",
				dpsList:      []*dataPoints{dpsTS1, dpsTS2},
				sids:         []common.SeriesID{1, 2, 3},
				minTimestamp: 1,
				maxTimestamp: 2,
				want: []blockMetadata{
					{seriesID: 1, count: 1, uncompressedSizeBytes: 1684},
					{seriesID: 1, count: 1, uncompressedSizeBytes: 1684},
					{seriesID: 2, count: 1, uncompressedSizeBytes: 63},
					{seriesID: 2, count: 1, uncompressedSizeBytes: 63},
					{seriesID: 3, count: 1, uncompressedSizeBytes: 32},
					{seriesID: 3, count: 1, uncompressedSizeBytes: 32},
				},
			},
			{
				name:         "Test with a single part with same ts",
				dpsList:      []*dataPoints{duplicatedDps1},
				sids:         []common.SeriesID{1, 2, 3},
				minTimestamp: 1,
				maxTimestamp: 1,
				want: []blockMetadata{
					{seriesID: 1, count: 1, uncompressedSizeBytes: 16},
					{seriesID: 2, count: 1, uncompressedSizeBytes: 16},
					{seriesID: 3, count: 1, uncompressedSizeBytes: 16},
				},
			},
			{
				name:         "Test with multiple parts with same ts",
				dpsList:      []*dataPoints{dpsTS1, dpsTS1},
				sids:         []common.SeriesID{1, 2, 3},
				minTimestamp: 1,
				maxTimestamp: 2,
				want: []blockMetadata{
					{seriesID: 1, count: 1, uncompressedSizeBytes: 1684},
					{seriesID: 1, count: 1, uncompressedSizeBytes: 1684},
					{seriesID: 2, count: 1, uncompressedSizeBytes: 63},
					{seriesID: 2, count: 1, uncompressedSizeBytes: 63},
					{seriesID: 3, count: 1, uncompressedSizeBytes: 32},
					{seriesID: 3, count: 1, uncompressedSizeBytes: 32},
				},
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
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
				verify(t, tt, tst)
			})
		}
	})
}

var tagProjections = map[int][]model.TagProjection{
	1: {
		{Family: "arrTag", Names: []string{"strArrTag", "intArrTag"}},
		{Family: "binaryTag", Names: []string{"binaryTag"}},
		{Family: "singleTag", Names: []string{"strTag", "intTag"}},
	},
	2: {
		{Family: "singleTag", Names: []string{"strTag1", "strTag2"}},
	},
}

var allTagProjections = []model.TagProjection{
	{Family: "arrTag", Names: []string{"strArrTag", "intArrTag"}},
	{Family: "binaryTag", Names: []string{"binaryTag"}},
	{Family: "singleTag", Names: []string{"strTag", "intTag", "strTag1", "strTag2"}},
}

var fieldProjections = map[int][]string{
	1: {"strField", "intField", "floatField", "binaryField"},
	3: {"intField"},
}

var dpsTS1 = &dataPoints{
	seriesIDs:  []common.SeriesID{1, 2, 3},
	timestamps: []int64{1, 1, 1},
	versions:   []int64{1, 2, 3},
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
				name: "singleTag", values: []*nameValue{
					{name: "strTag1", valueType: pbv1.ValueTypeStr, value: []byte("tag1"), valueArr: nil},
					{name: "strTag2", valueType: pbv1.ValueTypeStr, value: []byte("tag2"), valueArr: nil},
				},
			},
		},
		{}, // empty tagFamilies for seriesID 3
	},
	fields: []nameValues{
		{
			name: "skipped", values: []*nameValue{
				{name: "strField", valueType: pbv1.ValueTypeStr, value: []byte("field1"), valueArr: nil},
				{name: "intField", valueType: pbv1.ValueTypeInt64, value: convert.Int64ToBytes(1110), valueArr: nil},
				{name: "floatField", valueType: pbv1.ValueTypeFloat64, value: convert.Float64ToBytes(1221233.343), valueArr: nil},
				{name: "binaryField", valueType: pbv1.ValueTypeBinaryData, value: longText, valueArr: nil},
			},
		},
		{}, // empty fields for seriesID 2
		{
			name: "onlyFields", values: []*nameValue{
				{name: "intField", valueType: pbv1.ValueTypeInt64, value: convert.Int64ToBytes(1110), valueArr: nil},
			},
		},
	},
}

var dpsTS11 = &dataPoints{
	seriesIDs:  []common.SeriesID{1, 2, 3},
	timestamps: []int64{1, 1, 1},
	versions:   []int64{0, 1, 2},
	tagFamilies: [][]nameValues{
		{
			{
				name: "arrTag", values: []*nameValue{
					{name: "strArrTag", valueType: pbv1.ValueTypeStrArr, value: nil, valueArr: [][]byte{[]byte("value5"), []byte("value6")}},
					{name: "intArrTag", valueType: pbv1.ValueTypeInt64Arr, value: nil, valueArr: [][]byte{convert.Int64ToBytes(35), convert.Int64ToBytes(40)}},
				},
			},
			{
				name: "binaryTag", values: []*nameValue{
					{name: "binaryTag", valueType: pbv1.ValueTypeBinaryData, value: longText, valueArr: nil},
				},
			},
			{
				name: "singleTag", values: []*nameValue{
					{name: "strTag", valueType: pbv1.ValueTypeStr, value: []byte("value3"), valueArr: nil},
					{name: "intTag", valueType: pbv1.ValueTypeInt64, value: convert.Int64ToBytes(30), valueArr: nil},
				},
			},
		},
		{
			{
				name: "singleTag", values: []*nameValue{
					{name: "strTag1", valueType: pbv1.ValueTypeStr, value: []byte("tag3"), valueArr: nil},
					{name: "strTag2", valueType: pbv1.ValueTypeStr, value: []byte("tag4"), valueArr: nil},
				},
			},
		},
		{}, // empty tagFamilies for seriesID 6
	},
	fields: []nameValues{
		{
			name: "skipped", values: []*nameValue{
				{name: "strField", valueType: pbv1.ValueTypeStr, value: []byte("field3"), valueArr: nil},
				{name: "intField", valueType: pbv1.ValueTypeInt64, value: convert.Int64ToBytes(3330), valueArr: nil},
				{name: "floatField", valueType: pbv1.ValueTypeFloat64, value: convert.Float64ToBytes(3663699.029), valueArr: nil},
				{name: "binaryField", valueType: pbv1.ValueTypeBinaryData, value: longText, valueArr: nil},
			},
		},
		{}, // empty fields for seriesID 5
		{
			name: "onlyFields", values: []*nameValue{
				{name: "intField", valueType: pbv1.ValueTypeInt64, value: convert.Int64ToBytes(4440), valueArr: nil},
			},
		},
	},
}

var dpsTS2 = &dataPoints{
	seriesIDs:  []common.SeriesID{1, 2, 3},
	timestamps: []int64{2, 2, 2},
	versions:   []int64{4, 5, 6},
	tagFamilies: [][]nameValues{
		{
			{
				name: "arrTag", values: []*nameValue{
					{name: "strArrTag", valueType: pbv1.ValueTypeStrArr, value: nil, valueArr: [][]byte{[]byte("value5"), []byte("value6")}},
					{name: "intArrTag", valueType: pbv1.ValueTypeInt64Arr, value: nil, valueArr: [][]byte{convert.Int64ToBytes(35), convert.Int64ToBytes(40)}},
				},
			},
			{
				name: "binaryTag", values: []*nameValue{
					{name: "binaryTag", valueType: pbv1.ValueTypeBinaryData, value: longText, valueArr: nil},
				},
			},
			{
				name: "singleTag", values: []*nameValue{
					{name: "strTag", valueType: pbv1.ValueTypeStr, value: []byte("value3"), valueArr: nil},
					{name: "intTag", valueType: pbv1.ValueTypeInt64, value: convert.Int64ToBytes(30), valueArr: nil},
				},
			},
		},
		{
			{
				name: "singleTag", values: []*nameValue{
					{name: "strTag1", valueType: pbv1.ValueTypeStr, value: []byte("tag3"), valueArr: nil},
					{name: "strTag2", valueType: pbv1.ValueTypeStr, value: []byte("tag4"), valueArr: nil},
				},
			},
		},
		{}, // empty tagFamilies for seriesID 6
	},
	fields: []nameValues{
		{
			name: "skipped", values: []*nameValue{
				{name: "strField", valueType: pbv1.ValueTypeStr, value: []byte("field3"), valueArr: nil},
				{name: "intField", valueType: pbv1.ValueTypeInt64, value: convert.Int64ToBytes(3330), valueArr: nil},
				{name: "floatField", valueType: pbv1.ValueTypeFloat64, value: convert.Float64ToBytes(3663699.029), valueArr: nil},
				{name: "binaryField", valueType: pbv1.ValueTypeBinaryData, value: longText, valueArr: nil},
			},
		},
		{}, // empty fields for seriesID 5
		{
			name: "onlyFields", values: []*nameValue{
				{name: "intField", valueType: pbv1.ValueTypeInt64, value: convert.Int64ToBytes(4440), valueArr: nil},
			},
		},
	},
}

var duplicatedDps = &dataPoints{
	seriesIDs:  []common.SeriesID{1, 1, 1},
	timestamps: []int64{1, 1, 1},
	versions:   []int64{1, 2, 3},
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
				name: "singleTag", values: []*nameValue{
					{name: "strTag1", valueType: pbv1.ValueTypeStr, value: []byte("tag1"), valueArr: nil},
					{name: "strTag2", valueType: pbv1.ValueTypeStr, value: []byte("tag2"), valueArr: nil},
				},
			},
		},
		{}, // empty tagFamilies for seriesID 3
	},
	fields: []nameValues{
		{
			name: "skipped", values: []*nameValue{
				{name: "strField", valueType: pbv1.ValueTypeStr, value: []byte("field1"), valueArr: nil},
				{name: "intField", valueType: pbv1.ValueTypeInt64, value: convert.Int64ToBytes(1110), valueArr: nil},
				{name: "floatField", valueType: pbv1.ValueTypeFloat64, value: convert.Float64ToBytes(1221233.343), valueArr: nil},
				{name: "binaryField", valueType: pbv1.ValueTypeBinaryData, value: longText, valueArr: nil},
			},
		},
		{}, // empty fields for seriesID 2
		{
			name: "onlyFields", values: []*nameValue{
				{name: "intField", valueType: pbv1.ValueTypeInt64, value: convert.Int64ToBytes(1110), valueArr: nil},
			},
		},
	},
}

var duplicatedDps1 = &dataPoints{
	seriesIDs:  []common.SeriesID{2, 2, 2, 1, 1, 1, 3, 3, 3},
	timestamps: []int64{1, 1, 1, 1, 1, 1, 1, 1, 1},
	versions:   []int64{1, 2, 3, 3, 2, 1, 2, 1, 3},
	tagFamilies: [][]nameValues{
		{},
		{},
		{},
		{},
		{},
		{},
		{},
		{},
		{},
	},
	fields: []nameValues{
		{},
		{},
		{},
		{},
		{},
		{},
		{},
		{},
		{},
	},
}

func generateHugeDps(startTimestamp, endTimestamp, timestamp int64) *dataPoints {
	hugeDps := &dataPoints{
		seriesIDs:   []common.SeriesID{},
		timestamps:  []int64{},
		versions:    []int64{},
		tagFamilies: [][]nameValues{},
		fields:      []nameValues{},
	}
	now := time.Now().UnixNano()
	for i := startTimestamp; i <= endTimestamp; i++ {
		hugeDps.seriesIDs = append(hugeDps.seriesIDs, 1)
		hugeDps.timestamps = append(hugeDps.timestamps, i)
		hugeDps.versions = append(hugeDps.versions, now+i)
		hugeDps.tagFamilies = append(hugeDps.tagFamilies, []nameValues{
			{
				name: "arrTag", values: []*nameValue{
					{name: "strArrTag", valueType: pbv1.ValueTypeStrArr, value: nil, valueArr: [][]byte{[]byte("value5"), []byte("value6")}},
					{name: "intArrTag", valueType: pbv1.ValueTypeInt64Arr, value: nil, valueArr: [][]byte{convert.Int64ToBytes(35), convert.Int64ToBytes(40)}},
				},
			},
			{
				name: "binaryTag", values: []*nameValue{
					{name: "binaryTag", valueType: pbv1.ValueTypeBinaryData, value: longText, valueArr: nil},
				},
			},
			{
				name: "singleTag", values: []*nameValue{
					{name: "strTag", valueType: pbv1.ValueTypeStr, value: []byte("value3"), valueArr: nil},
					{name: "intTag", valueType: pbv1.ValueTypeInt64, value: convert.Int64ToBytes(30), valueArr: nil},
				},
			},
		})
		hugeDps.fields = append(hugeDps.fields, nameValues{
			name: "skipped", values: []*nameValue{
				{name: "strField", valueType: pbv1.ValueTypeStr, value: []byte("field3"), valueArr: nil},
				{name: "intField", valueType: pbv1.ValueTypeInt64, value: convert.Int64ToBytes(3330), valueArr: nil},
				{name: "floatField", valueType: pbv1.ValueTypeFloat64, value: convert.Float64ToBytes(3663699.029), valueArr: nil},
				{name: "binaryField", valueType: pbv1.ValueTypeBinaryData, value: longText, valueArr: nil},
			},
		})
	}
	hugeDps.seriesIDs = append(hugeDps.seriesIDs, []common.SeriesID{2, 3}...)
	hugeDps.timestamps = append(hugeDps.timestamps, []int64{timestamp, timestamp}...)
	hugeDps.versions = append(hugeDps.versions, []int64{now + timestamp, now + timestamp}...)
	hugeDps.tagFamilies = append(hugeDps.tagFamilies, [][]nameValues{{
		{
			name: "singleTag", values: []*nameValue{
				{name: "strTag1", valueType: pbv1.ValueTypeStr, value: []byte("tag3"), valueArr: nil},
				{name: "strTag2", valueType: pbv1.ValueTypeStr, value: []byte("tag4"), valueArr: nil},
			},
		},
	}, {}}...)
	hugeDps.fields = append(hugeDps.fields, []nameValues{{}, {
		name: "onlyFields", values: []*nameValue{
			{name: "intField", valueType: pbv1.ValueTypeInt64, value: convert.Int64ToBytes(4440), valueArr: nil},
		},
	}}...)
	return hugeDps
}

func generateHugeSmallDps(startTimestamp, endTimestamp, timestamp int64) *dataPoints {
	hugeDps := &dataPoints{
		seriesIDs:   []common.SeriesID{},
		timestamps:  []int64{},
		versions:    []int64{},
		tagFamilies: [][]nameValues{},
		fields:      []nameValues{},
	}
	now := time.Now().UnixNano()
	for i := startTimestamp; i <= endTimestamp; i++ {
		hugeDps.seriesIDs = append(hugeDps.seriesIDs, 1)
		hugeDps.timestamps = append(hugeDps.timestamps, i)
		hugeDps.versions = append(hugeDps.versions, now+i)
		hugeDps.tagFamilies = append(hugeDps.tagFamilies, []nameValues{})
		hugeDps.fields = append(hugeDps.fields, nameValues{
			name: "skipped", values: []*nameValue{
				{name: "intField", valueType: pbv1.ValueTypeInt64, value: convert.Int64ToBytes(3330), valueArr: nil},
			},
		})
	}
	hugeDps.seriesIDs = append(hugeDps.seriesIDs, []common.SeriesID{2, 3}...)
	hugeDps.timestamps = append(hugeDps.timestamps, []int64{timestamp, timestamp}...)
	hugeDps.versions = append(hugeDps.versions, []int64{now + timestamp, now + timestamp}...)
	hugeDps.tagFamilies = append(hugeDps.tagFamilies, [][]nameValues{{}, {}}...)
	hugeDps.fields = append(hugeDps.fields, []nameValues{{}, {
		name: "onlyFields", values: []*nameValue{
			{name: "intField", valueType: pbv1.ValueTypeInt64, value: convert.Int64ToBytes(4440), valueArr: nil},
		},
	}}...)
	return hugeDps
}
