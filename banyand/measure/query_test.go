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
	"sort"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/apache/skywalking-banyandb/api/common"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

func TestQueryResult(t *testing.T) {
	tests := []struct {
		wantErr       error
		name          string
		dpsList       []*dataPoints
		sids          []common.SeriesID
		want          []pbv1.Result
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
			want: []pbv1.Result{{
				SID:        1,
				Timestamps: []int64{1},
				TagFamilies: []pbv1.TagFamily{
					{Name: "arrTag", Tags: []pbv1.Tag{
						{Name: "strArrTag", Values: []*modelv1.TagValue{strArrTagValue([]string{"value1", "value2"})}},
						{Name: "intArrTag", Values: []*modelv1.TagValue{int64ArrTagValue([]int64{25, 30})}},
					}},
					{Name: "binaryTag", Tags: []pbv1.Tag{
						{Name: "binaryTag", Values: []*modelv1.TagValue{binaryDataTagValue(longText)}},
					}},
					{Name: "singleTag", Tags: []pbv1.Tag{
						{Name: "strTag", Values: []*modelv1.TagValue{strTagValue("value1")}},
						{Name: "intTag", Values: []*modelv1.TagValue{int64TagValue(10)}},
					}},
				},
				Fields: []pbv1.Field{
					{Name: "strField", Values: []*modelv1.FieldValue{strFieldValue("field1")}},
					{Name: "intField", Values: []*modelv1.FieldValue{int64FieldValue(1110)}},
					{Name: "floatField", Values: []*modelv1.FieldValue{float64FieldValue(1.221233343e+06)}},
					{Name: "binaryField", Values: []*modelv1.FieldValue{binaryDataFieldValue(longText)}},
				},
			}, {
				SID:        2,
				Timestamps: []int64{1},
				TagFamilies: []pbv1.TagFamily{
					{Name: "singleTag", Tags: []pbv1.Tag{
						{Name: "strTag1", Values: []*modelv1.TagValue{strTagValue("tag1")}},
						{Name: "strTag2", Values: []*modelv1.TagValue{strTagValue("tag2")}},
					}},
				},
				Fields: nil,
			}, {
				SID:         3,
				Timestamps:  []int64{1},
				TagFamilies: nil,
				Fields: []pbv1.Field{
					{Name: "intField", Values: []*modelv1.FieldValue{int64FieldValue(1110)}},
				},
			}},
		},
		{
			name:         "Test with multiple parts with multiple data orderBy TS desc",
			dpsList:      []*dataPoints{dpsTS1, dpsTS2},
			sids:         []common.SeriesID{1, 2, 3},
			minTimestamp: 1,
			maxTimestamp: 2,
			want: []pbv1.Result{{
				SID:        1,
				Timestamps: []int64{2},
				TagFamilies: []pbv1.TagFamily{
					{Name: "arrTag", Tags: []pbv1.Tag{
						{Name: "strArrTag", Values: []*modelv1.TagValue{strArrTagValue([]string{"value5", "value6"})}},
						{Name: "intArrTag", Values: []*modelv1.TagValue{int64ArrTagValue([]int64{35, 40})}},
					}},
					{Name: "binaryTag", Tags: []pbv1.Tag{
						{Name: "binaryTag", Values: []*modelv1.TagValue{binaryDataTagValue(longText)}},
					}},
					{Name: "singleTag", Tags: []pbv1.Tag{
						{Name: "strTag", Values: []*modelv1.TagValue{strTagValue("value3")}},
						{Name: "intTag", Values: []*modelv1.TagValue{int64TagValue(30)}},
					}},
				},
				Fields: []pbv1.Field{
					{Name: "strField", Values: []*modelv1.FieldValue{strFieldValue("field3")}},
					{Name: "intField", Values: []*modelv1.FieldValue{int64FieldValue(3330)}},
					{Name: "floatField", Values: []*modelv1.FieldValue{float64FieldValue(3663699.029)}},
					{Name: "binaryField", Values: []*modelv1.FieldValue{binaryDataFieldValue(longText)}},
				},
			}, {
				SID:        2,
				Timestamps: []int64{2},
				TagFamilies: []pbv1.TagFamily{
					{Name: "singleTag", Tags: []pbv1.Tag{
						{Name: "strTag1", Values: []*modelv1.TagValue{strTagValue("tag3")}},
						{Name: "strTag2", Values: []*modelv1.TagValue{strTagValue("tag4")}},
					}},
				},
				Fields: nil,
			}, {
				SID:         3,
				Timestamps:  []int64{2},
				TagFamilies: nil,
				Fields: []pbv1.Field{
					{Name: "intField", Values: []*modelv1.FieldValue{int64FieldValue(4440)}},
				},
			}, {
				SID:        1,
				Timestamps: []int64{1},
				TagFamilies: []pbv1.TagFamily{
					{Name: "arrTag", Tags: []pbv1.Tag{
						{Name: "strArrTag", Values: []*modelv1.TagValue{strArrTagValue([]string{"value1", "value2"})}},
						{Name: "intArrTag", Values: []*modelv1.TagValue{int64ArrTagValue([]int64{25, 30})}},
					}},
					{Name: "binaryTag", Tags: []pbv1.Tag{
						{Name: "binaryTag", Values: []*modelv1.TagValue{binaryDataTagValue(longText)}},
					}},
					{Name: "singleTag", Tags: []pbv1.Tag{
						{Name: "strTag", Values: []*modelv1.TagValue{strTagValue("value1")}},
						{Name: "intTag", Values: []*modelv1.TagValue{int64TagValue(10)}},
					}},
				},
				Fields: []pbv1.Field{
					{Name: "strField", Values: []*modelv1.FieldValue{strFieldValue("field1")}},
					{Name: "intField", Values: []*modelv1.FieldValue{int64FieldValue(1110)}},
					{Name: "floatField", Values: []*modelv1.FieldValue{float64FieldValue(1221233.343)}},
					{Name: "binaryField", Values: []*modelv1.FieldValue{binaryDataFieldValue(longText)}},
				},
			}, {
				SID:        2,
				Timestamps: []int64{1},
				TagFamilies: []pbv1.TagFamily{
					{Name: "singleTag", Tags: []pbv1.Tag{
						{Name: "strTag1", Values: []*modelv1.TagValue{strTagValue("tag1")}},
						{Name: "strTag2", Values: []*modelv1.TagValue{strTagValue("tag2")}},
					}},
				},
				Fields: nil,
			}, {
				SID:         3,
				Timestamps:  []int64{1},
				TagFamilies: nil,
				Fields: []pbv1.Field{
					{Name: "intField", Values: []*modelv1.FieldValue{int64FieldValue(1110)}},
				},
			}},
		},
		{
			name:         "Test with multiple parts with multiple data orderBy TS asc",
			dpsList:      []*dataPoints{dpsTS1, dpsTS2},
			sids:         []common.SeriesID{1, 2, 3},
			ascTS:        true,
			minTimestamp: 1,
			maxTimestamp: 2,
			want: []pbv1.Result{{
				SID:        1,
				Timestamps: []int64{1},
				TagFamilies: []pbv1.TagFamily{
					{Name: "arrTag", Tags: []pbv1.Tag{
						{Name: "strArrTag", Values: []*modelv1.TagValue{strArrTagValue([]string{"value1", "value2"})}},
						{Name: "intArrTag", Values: []*modelv1.TagValue{int64ArrTagValue([]int64{25, 30})}},
					}},
					{Name: "binaryTag", Tags: []pbv1.Tag{
						{Name: "binaryTag", Values: []*modelv1.TagValue{binaryDataTagValue(longText)}},
					}},
					{Name: "singleTag", Tags: []pbv1.Tag{
						{Name: "strTag", Values: []*modelv1.TagValue{strTagValue("value1")}},
						{Name: "intTag", Values: []*modelv1.TagValue{int64TagValue(10)}},
					}},
				},
				Fields: []pbv1.Field{
					{Name: "strField", Values: []*modelv1.FieldValue{strFieldValue("field1")}},
					{Name: "intField", Values: []*modelv1.FieldValue{int64FieldValue(1110)}},
					{Name: "floatField", Values: []*modelv1.FieldValue{float64FieldValue(1221233.343)}},
					{Name: "binaryField", Values: []*modelv1.FieldValue{binaryDataFieldValue(longText)}},
				},
			}, {
				SID:        2,
				Timestamps: []int64{1},
				TagFamilies: []pbv1.TagFamily{
					{Name: "singleTag", Tags: []pbv1.Tag{
						{Name: "strTag1", Values: []*modelv1.TagValue{strTagValue("tag1")}},
						{Name: "strTag2", Values: []*modelv1.TagValue{strTagValue("tag2")}},
					}},
				},
				Fields: nil,
			}, {
				SID:         3,
				Timestamps:  []int64{1},
				TagFamilies: nil,
				Fields: []pbv1.Field{
					{Name: "intField", Values: []*modelv1.FieldValue{int64FieldValue(1110)}},
				},
			}, {
				SID:        1,
				Timestamps: []int64{2},
				TagFamilies: []pbv1.TagFamily{
					{Name: "arrTag", Tags: []pbv1.Tag{
						{Name: "strArrTag", Values: []*modelv1.TagValue{strArrTagValue([]string{"value5", "value6"})}},
						{Name: "intArrTag", Values: []*modelv1.TagValue{int64ArrTagValue([]int64{35, 40})}},
					}},
					{Name: "binaryTag", Tags: []pbv1.Tag{
						{Name: "binaryTag", Values: []*modelv1.TagValue{binaryDataTagValue(longText)}},
					}},
					{Name: "singleTag", Tags: []pbv1.Tag{
						{Name: "strTag", Values: []*modelv1.TagValue{strTagValue("value3")}},
						{Name: "intTag", Values: []*modelv1.TagValue{int64TagValue(30)}},
					}},
				},
				Fields: []pbv1.Field{
					{Name: "strField", Values: []*modelv1.FieldValue{strFieldValue("field3")}},
					{Name: "intField", Values: []*modelv1.FieldValue{int64FieldValue(3330)}},
					{Name: "floatField", Values: []*modelv1.FieldValue{float64FieldValue(3663699.029)}},
					{Name: "binaryField", Values: []*modelv1.FieldValue{binaryDataFieldValue(longText)}},
				},
			}, {
				SID:        2,
				Timestamps: []int64{2},
				TagFamilies: []pbv1.TagFamily{
					{Name: "singleTag", Tags: []pbv1.Tag{
						{Name: "strTag1", Values: []*modelv1.TagValue{strTagValue("tag3")}},
						{Name: "strTag2", Values: []*modelv1.TagValue{strTagValue("tag4")}},
					}},
				},
				Fields: nil,
			}, {
				SID:         3,
				Timestamps:  []int64{2},
				TagFamilies: nil,
				Fields: []pbv1.Field{
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
			want: []pbv1.Result{{
				SID:        1,
				Timestamps: []int64{1},
				TagFamilies: []pbv1.TagFamily{
					{Name: "arrTag", Tags: []pbv1.Tag{
						{Name: "strArrTag", Values: []*modelv1.TagValue{strArrTagValue([]string{"value1", "value2"})}},
						{Name: "intArrTag", Values: []*modelv1.TagValue{int64ArrTagValue([]int64{25, 30})}},
					}},
					{Name: "binaryTag", Tags: []pbv1.Tag{
						{Name: "binaryTag", Values: []*modelv1.TagValue{binaryDataTagValue(longText)}},
					}},
					{Name: "singleTag", Tags: []pbv1.Tag{
						{Name: "strTag", Values: []*modelv1.TagValue{strTagValue("value1")}},
						{Name: "intTag", Values: []*modelv1.TagValue{int64TagValue(10)}},
					}},
				},
				Fields: []pbv1.Field{
					{Name: "strField", Values: []*modelv1.FieldValue{strFieldValue("field1")}},
					{Name: "intField", Values: []*modelv1.FieldValue{int64FieldValue(1110)}},
					{Name: "floatField", Values: []*modelv1.FieldValue{float64FieldValue(1.221233343e+06)}},
					{Name: "binaryField", Values: []*modelv1.FieldValue{binaryDataFieldValue(longText)}},
				},
			}, {
				SID:        2,
				Timestamps: []int64{1},
				TagFamilies: []pbv1.TagFamily{
					{Name: "singleTag", Tags: []pbv1.Tag{
						{Name: "strTag1", Values: []*modelv1.TagValue{strTagValue("tag1")}},
						{Name: "strTag2", Values: []*modelv1.TagValue{strTagValue("tag2")}},
					}},
				},
				Fields: nil,
			}, {
				SID:         3,
				Timestamps:  []int64{1},
				TagFamilies: nil,
				Fields: []pbv1.Field{
					{Name: "intField", Values: []*modelv1.FieldValue{int64FieldValue(1110)}},
				},
			}},
		},
		{
			name:          "Test with multiple parts with multiple data order by Series",
			dpsList:       []*dataPoints{dpsTS1, dpsTS2},
			sids:          []common.SeriesID{2, 1, 3},
			orderBySeries: true,
			minTimestamp:  1,
			maxTimestamp:  2,
			want: []pbv1.Result{{
				SID:        2,
				Timestamps: []int64{1, 2},
				TagFamilies: []pbv1.TagFamily{
					{Name: "singleTag", Tags: []pbv1.Tag{
						{Name: "strTag1", Values: []*modelv1.TagValue{strTagValue("tag1"), strTagValue("tag3")}},
						{Name: "strTag2", Values: []*modelv1.TagValue{strTagValue("tag2"), strTagValue("tag4")}},
					}},
				},
				Fields: nil,
			}, {
				SID:        1,
				Timestamps: []int64{1, 2},
				TagFamilies: []pbv1.TagFamily{
					{Name: "arrTag", Tags: []pbv1.Tag{
						{Name: "strArrTag", Values: []*modelv1.TagValue{strArrTagValue([]string{"value1", "value2"}), strArrTagValue([]string{"value5", "value6"})}},
						{Name: "intArrTag", Values: []*modelv1.TagValue{int64ArrTagValue([]int64{25, 30}), int64ArrTagValue([]int64{35, 40})}},
					}},
					{Name: "binaryTag", Tags: []pbv1.Tag{
						{Name: "binaryTag", Values: []*modelv1.TagValue{binaryDataTagValue(longText), binaryDataTagValue(longText)}},
					}},
					{Name: "singleTag", Tags: []pbv1.Tag{
						{Name: "strTag", Values: []*modelv1.TagValue{strTagValue("value1"), strTagValue("value3")}},
						{Name: "intTag", Values: []*modelv1.TagValue{int64TagValue(10), int64TagValue(30)}},
					}},
				},
				Fields: []pbv1.Field{
					{Name: "strField", Values: []*modelv1.FieldValue{strFieldValue("field1"), strFieldValue("field3")}},
					{Name: "intField", Values: []*modelv1.FieldValue{int64FieldValue(1110), int64FieldValue(3330)}},
					{Name: "floatField", Values: []*modelv1.FieldValue{float64FieldValue(1.221233343e+06), float64FieldValue(3663699.029)}},
					{Name: "binaryField", Values: []*modelv1.FieldValue{binaryDataFieldValue(longText), binaryDataFieldValue(longText)}},
				},
			}, {
				SID:         3,
				Timestamps:  []int64{1, 2},
				TagFamilies: nil,
				Fields: []pbv1.Field{
					{Name: "intField", Values: []*modelv1.FieldValue{int64FieldValue(1110), int64FieldValue(4440)}},
				},
			}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Initialize a tstIter object.
			tst := &tsTable{}
			defer tst.Close()
			for _, dps := range tt.dpsList {
				tst.mustAddDataPoints(dps)
				time.Sleep(100 * time.Millisecond)
			}
			queryOpts := queryOptions{
				minTimestamp: tt.minTimestamp,
				maxTimestamp: tt.maxTimestamp,
			}
			pws, pp := tst.getParts(nil, nil, queryOpts)
			defer func() {
				for _, pw := range pws {
					pw.decRef()
				}
			}()
			sids := make([]common.SeriesID, len(tt.sids))
			copy(sids, tt.sids)
			sort.Slice(sids, func(i, j int) bool {
				return sids[i] < tt.sids[j]
			})
			ti := &tstIter{}
			ti.init(pp, sids, tt.minTimestamp, tt.maxTimestamp)

			var result queryResult
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
			var got []pbv1.Result
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
				protocmp.IgnoreUnknown(), protocmp.Transform()); diff != "" {
				t.Errorf("Unexpected []pbv1.Result (-got +want):\n%s", diff)
			}
		})
	}
}
