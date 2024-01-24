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

package stream

import (
	"errors"
	"sort"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/apache/skywalking-banyandb/api/common"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/run"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
	"github.com/apache/skywalking-banyandb/pkg/watcher"
)

func TestQueryResult(t *testing.T) {
	tests := []struct {
		wantErr       error
		name          string
		esList        []*elements
		sids          []common.SeriesID
		want          []pbv1.StreamResult
		minTimestamp  int64
		maxTimestamp  int64
		orderBySeries bool
		ascTS         bool
	}{
		{
			name:         "Test with multiple parts with duplicated data order by TS",
			esList:       []*elements{esTS1, esTS1},
			sids:         []common.SeriesID{1, 2, 3},
			minTimestamp: 1,
			maxTimestamp: 1,
			want: []pbv1.StreamResult{{
				SID:        1,
				Timestamps: []int64{1},
				ElementIDs: []string{"11"},
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
			}, {
				SID:        2,
				Timestamps: []int64{1},
				ElementIDs: []string{"21"},
				TagFamilies: []pbv1.TagFamily{
					{Name: "singleTag", Tags: []pbv1.Tag{
						{Name: "strTag1", Values: []*modelv1.TagValue{strTagValue("tag1")}},
						{Name: "strTag2", Values: []*modelv1.TagValue{strTagValue("tag2")}},
					}},
				},
			}, {
				SID:         3,
				Timestamps:  []int64{1},
				TagFamilies: nil,
			}},
		},
		{
			name:         "Test with multiple parts with multiple data orderBy TS desc",
			esList:       []*elements{esTS1, esTS2},
			sids:         []common.SeriesID{1, 2, 3},
			minTimestamp: 1,
			maxTimestamp: 2,
			want: []pbv1.StreamResult{{
				SID:        1,
				Timestamps: []int64{2},
				ElementIDs: []string{"12"},
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
			}, {
				SID:        2,
				Timestamps: []int64{2},
				ElementIDs: []string{"22"},
				TagFamilies: []pbv1.TagFamily{
					{Name: "singleTag", Tags: []pbv1.Tag{
						{Name: "strTag1", Values: []*modelv1.TagValue{strTagValue("tag3")}},
						{Name: "strTag2", Values: []*modelv1.TagValue{strTagValue("tag4")}},
					}},
				},
			}, {
				SID:         3,
				Timestamps:  []int64{2},
				ElementIDs:  []string{"32"},
				TagFamilies: nil,
			}, {
				SID:        1,
				Timestamps: []int64{1},
				ElementIDs: []string{"11"},
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
			}, {
				SID:        2,
				Timestamps: []int64{1},
				ElementIDs: []string{"21"},
				TagFamilies: []pbv1.TagFamily{
					{Name: "singleTag", Tags: []pbv1.Tag{
						{Name: "strTag1", Values: []*modelv1.TagValue{strTagValue("tag1")}},
						{Name: "strTag2", Values: []*modelv1.TagValue{strTagValue("tag2")}},
					}},
				},
			}, {
				SID:         3,
				Timestamps:  []int64{1},
				ElementIDs:  []string{"31"},
				TagFamilies: nil,
			}},
		},
		{
			name:         "Test with multiple parts with multiple data orderBy TS asc",
			esList:       []*elements{esTS1, esTS2},
			sids:         []common.SeriesID{1, 2, 3},
			ascTS:        true,
			minTimestamp: 1,
			maxTimestamp: 2,
			want: []pbv1.StreamResult{{
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
			}, {
				SID:        2,
				Timestamps: []int64{1},
				ElementIDs: []string{"21"},
				TagFamilies: []pbv1.TagFamily{
					{Name: "singleTag", Tags: []pbv1.Tag{
						{Name: "strTag1", Values: []*modelv1.TagValue{strTagValue("tag1")}},
						{Name: "strTag2", Values: []*modelv1.TagValue{strTagValue("tag2")}},
					}},
				},
			}, {
				SID:         3,
				Timestamps:  []int64{1},
				ElementIDs:  []string{"31"},
				TagFamilies: nil,
			}, {
				SID:        1,
				Timestamps: []int64{2},
				ElementIDs: []string{"12"},
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
			}, {
				SID:        2,
				Timestamps: []int64{2},
				ElementIDs: []string{"22"},
				TagFamilies: []pbv1.TagFamily{
					{Name: "singleTag", Tags: []pbv1.Tag{
						{Name: "strTag1", Values: []*modelv1.TagValue{strTagValue("tag3")}},
						{Name: "strTag2", Values: []*modelv1.TagValue{strTagValue("tag4")}},
					}},
				},
			}, {
				SID:         3,
				Timestamps:  []int64{2},
				ElementIDs:  []string{"32"},
				TagFamilies: nil,
			}},
		},
		{
			name:          "Test with multiple parts with duplicated data order by Series",
			esList:        []*elements{esTS1, esTS1},
			sids:          []common.SeriesID{1, 2, 3},
			orderBySeries: true,
			minTimestamp:  1,
			maxTimestamp:  1,
			want: []pbv1.StreamResult{{
				SID:        1,
				Timestamps: []int64{1},
				ElementIDs: []string{"11"},
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
			}, {
				SID:        2,
				Timestamps: []int64{1},
				ElementIDs: []string{"21"},
				TagFamilies: []pbv1.TagFamily{
					{Name: "singleTag", Tags: []pbv1.Tag{
						{Name: "strTag1", Values: []*modelv1.TagValue{strTagValue("tag1")}},
						{Name: "strTag2", Values: []*modelv1.TagValue{strTagValue("tag2")}},
					}},
				},
			}, {
				SID:         3,
				Timestamps:  []int64{1},
				ElementIDs:  []string{"31"},
				TagFamilies: nil,
			}},
		},
		{
			name:          "Test with multiple parts with multiple data order by Series",
			esList:        []*elements{esTS1, esTS2},
			sids:          []common.SeriesID{2, 1, 3},
			orderBySeries: true,
			minTimestamp:  1,
			maxTimestamp:  2,
			want: []pbv1.StreamResult{{
				SID:        2,
				Timestamps: []int64{1, 2},
				ElementIDs: []string{"21", "22"},
				TagFamilies: []pbv1.TagFamily{
					{Name: "singleTag", Tags: []pbv1.Tag{
						{Name: "strTag1", Values: []*modelv1.TagValue{strTagValue("tag1"), strTagValue("tag3")}},
						{Name: "strTag2", Values: []*modelv1.TagValue{strTagValue("tag2"), strTagValue("tag4")}},
					}},
				},
			}, {
				SID:        1,
				Timestamps: []int64{1, 2},
				ElementIDs: []string{"11", "12"},
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
			}, {
				SID:         3,
				Timestamps:  []int64{1, 2},
				ElementIDs:  []string{"31", "32"},
				TagFamilies: nil,
			}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			verify := func(t *testing.T, tst *tsTable) {
				defer tst.Close()
				queryOpts := queryOptions{
					minTimestamp: tt.minTimestamp,
					maxTimestamp: tt.maxTimestamp,
				}
				s := tst.currentSnapshot()
				require.NotNil(t, s)
				defer s.decRef()
				pp, _ := s.getParts(nil, queryOpts.minTimestamp, queryOpts.maxTimestamp)
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
				var got []pbv1.StreamResult
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
				for _, dps := range tt.esList {
					tst.mustAddElements(dps)
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
					logger.GetLogger("test"), timestamp.TimeRange{}, option{flushTimeout: defaultFlushTimeout, mergePolicy: newDefaultMergePolicyForTesting()})
				require.NoError(t, err)
				for _, dps := range tt.esList {
					tst.mustAddElements(dps)
					time.Sleep(100 * time.Millisecond)
				}
				// wait until the introducer is done
				if len(tt.esList) > 0 {
					for {
						snp := tst.currentSnapshot()
						if snp == nil {
							time.Sleep(100 * time.Millisecond)
							continue
						}
						if len(snp.parts) == len(tt.esList) {
							snp.decRef()
							tst.Close()
							break
						}
						snp.decRef()
						time.Sleep(100 * time.Millisecond)
					}
				}

				// reopen the table
				tst, err = newTSTable(fileSystem, tmpPath, common.Position{},
					logger.GetLogger("test"), timestamp.TimeRange{}, option{flushTimeout: defaultFlushTimeout, mergePolicy: newDefaultMergePolicyForTesting()})
				require.NoError(t, err)

				verify(t, tst)
			})
		})
	}
}
