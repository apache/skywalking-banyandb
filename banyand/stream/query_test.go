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
	"context"
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
		esList        []*elements
		sids          []common.SeriesID
		want          []model.StreamResult
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
			want: []model.StreamResult{{
				SIDs:       []common.SeriesID{1},
				Timestamps: []int64{1},
				ElementIDs: []uint64{11},
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
			}, {
				SIDs:        []common.SeriesID{3, 3},
				Timestamps:  []int64{1, 1},
				ElementIDs:  []uint64{31, 31},
				TagFamilies: emptyTagFamilies(2),
			}, {
				SIDs:       []common.SeriesID{2, 2},
				Timestamps: []int64{1, 1},
				ElementIDs: []uint64{21, 21},
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
						{Name: "strTag1", Values: []*modelv1.TagValue{strTagValue("tag1"), strTagValue("tag1")}},
						{Name: "strTag2", Values: []*modelv1.TagValue{strTagValue("tag2"), strTagValue("tag2")}},
					}},
				},
			}, {
				SIDs:       []common.SeriesID{1},
				Timestamps: []int64{1},
				ElementIDs: []uint64{11},
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
			}},
		},
		{
			name:         "Test with multiple parts with multiple data orderBy TS desc",
			esList:       []*elements{esTS1, esTS2},
			sids:         []common.SeriesID{1, 2, 3},
			minTimestamp: 1,
			maxTimestamp: 2,
			want: []model.StreamResult{{
				SIDs:       []common.SeriesID{1},
				Timestamps: []int64{2},
				ElementIDs: []uint64{12},
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
			}, {
				SIDs:       []common.SeriesID{2},
				Timestamps: []int64{2},
				ElementIDs: []uint64{22},
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
			}, {
				SIDs:        []common.SeriesID{3},
				Timestamps:  []int64{2},
				ElementIDs:  []uint64{32},
				TagFamilies: emptyTagFamilies(1),
			}, {
				SIDs:       []common.SeriesID{1},
				Timestamps: []int64{1},
				ElementIDs: []uint64{11},
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
			}, {
				SIDs:        []common.SeriesID{3},
				Timestamps:  []int64{1},
				ElementIDs:  []uint64{31},
				TagFamilies: emptyTagFamilies(1),
			}, {
				SIDs:       []common.SeriesID{2},
				Timestamps: []int64{1},
				ElementIDs: []uint64{21},
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
			}},
		},
		{
			name:         "Test with multiple parts with multiple data orderBy TS asc",
			esList:       []*elements{esTS1, esTS2},
			sids:         []common.SeriesID{1, 2, 3},
			ascTS:        true,
			minTimestamp: 1,
			maxTimestamp: 2,
			want: []model.StreamResult{{
				SIDs:       []common.SeriesID{1},
				Timestamps: []int64{1},
				ElementIDs: []uint64{11},
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
			}, {
				SIDs:        []common.SeriesID{3},
				Timestamps:  []int64{1},
				ElementIDs:  []uint64{31},
				TagFamilies: emptyTagFamilies(1),
			}, {
				SIDs:       []common.SeriesID{2, 2},
				Timestamps: []int64{1, 2},
				ElementIDs: []uint64{21, 22},
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
			}, {
				SIDs:       []common.SeriesID{1},
				Timestamps: []int64{2},
				ElementIDs: []uint64{12},
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
			}, {
				SIDs:        []common.SeriesID{3},
				Timestamps:  []int64{2},
				ElementIDs:  []uint64{32},
				TagFamilies: emptyTagFamilies(1),
			}},
		},
	}
	bma := generateBlockMetadataArray()
	defer releaseBlockMetadataArray(bma)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			verify := func(t *testing.T, tst *tsTable) {
				var result queryResult

				result.qo = queryOptions{
					minTimestamp: tt.minTimestamp,
					maxTimestamp: tt.maxTimestamp,
					sortedSids:   tt.sids,
					StreamQueryOptions: model.StreamQueryOptions{
						TagProjection: tagProjectionAll,
					},
				}
				result.tabs = []*tsTable{tst}
				defer result.Release()
				if !tt.orderBySeries {
					result.orderByTS = true
					result.asc = tt.ascTS
				}
				var got []model.StreamResult
				ctx := context.Background()
				for {
					r := result.Pull(ctx)
					if r == nil {
						break
					}
					if !errors.Is(r.Error, tt.wantErr) {
						t.Errorf("Unexpected error: got %v, want %v", r.Error, tt.wantErr)
					}
					sort.Slice(r.TagFamilies, func(i, j int) bool {
						return r.TagFamilies[i].Name < r.TagFamilies[j].Name
					})
					got = append(got, *r)
				}

				if diff := cmp.Diff(got, tt.want,
					protocmp.IgnoreUnknown(), protocmp.Transform()); diff != "" {
					t.Errorf("Unexpected []pbv1.Result (-got +want):\n%s", diff)
				}
			}

			t.Run("memory snapshot", func(t *testing.T) {
				tmpPath, defFn := test.Space(require.New(t))
				defer defFn()
				index, _ := newElementIndex(context.TODO(), tmpPath, 0)
				tst := &tsTable{
					index:         index,
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
				for _, es := range tt.esList {
					tst.mustAddElements(es)
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
					// Since Stream deduplicate data in merging process, we need to disable the merging in the test.
					logger.GetLogger("test"), timestamp.TimeRange{}, option{flushTimeout: 0, mergePolicy: newDisabledMergePolicyForTesting()})
				require.NoError(t, err)
				for _, es := range tt.esList {
					tst.mustAddElements(es)
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
						if snp.creator != snapshotCreatorMemPart && len(snp.parts) == len(tt.esList) {
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

func emptyTagFamilies(size int) []model.TagFamily {
	var values []*modelv1.TagValue
	for i := 0; i < size; i++ {
		values = append(values, pbv1.NullTagValue)
	}
	return []model.TagFamily{
		{Name: "arrTag", Tags: []model.Tag{
			{Name: "strArrTag", Values: values},
			{Name: "intArrTag", Values: values},
		}},
		{Name: "binaryTag", Tags: []model.Tag{
			{Name: "binaryTag", Values: values},
		}},
		{Name: "singleTag", Tags: []model.Tag{
			{Name: "strTag", Values: values},
			{Name: "intTag", Values: values},
			{Name: "strTag1", Values: values},
			{Name: "strTag2", Values: values},
		}},
	}
}
