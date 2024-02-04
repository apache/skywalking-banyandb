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
	"fmt"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/run"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
	"github.com/apache/skywalking-banyandb/pkg/watcher"
)

func Test_tsTable_mustAddDataPoints(t *testing.T) {
	tests := []struct {
		name   string
		esList []*elements
		want   int
	}{
		{
			name: "Test with empty elements",
			esList: []*elements{
				{
					timestamps:  []int64{},
					elementIDs:  []string{},
					seriesIDs:   []common.SeriesID{},
					tagFamilies: make([][]tagValues, 0),
				},
			},
			want: 0,
		},
		{
			name: "Test with one item in elements",
			esList: []*elements{
				{
					timestamps: []int64{1},
					elementIDs: []string{"0"},
					seriesIDs:  []common.SeriesID{1},
					tagFamilies: [][]tagValues{
						{
							{
								tag: "arrTag", values: []*tagValue{
									{tag: "strArrTag", valueType: pbv1.ValueTypeStrArr, value: nil, valueArr: [][]byte{[]byte("value1"), []byte("value2")}},
									{tag: "intArrTag", valueType: pbv1.ValueTypeInt64Arr, value: nil, valueArr: [][]byte{convert.Int64ToBytes(25), convert.Int64ToBytes(30)}},
								},
							},
						},
					},
				},
			},
			want: 1,
		},
		{
			name: "Test with multiple calls to mustAddDataPoints",
			esList: []*elements{
				esTS1,
				esTS2,
			},
			want: 2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpPath, _ := test.Space(require.New(t))
			index, _ := newElementIndex(context.TODO(), tmpPath, 0)
			tst := &tsTable{
				index:         index,
				loopCloser:    run.NewCloser(2),
				introductions: make(chan *introduction),
			}
			flushCh := make(chan *flusherIntroduction)
			mergeCh := make(chan *mergerIntroduction)
			introducerWatcher := make(watcher.Channel, 1)
			go tst.introducerLoop(flushCh, mergeCh, introducerWatcher, 1)
			defer tst.Close()
			for _, es := range tt.esList {
				tst.mustAddElements(es)
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
		esList       []*elements
		sids         []common.SeriesID
		want         []blockMetadata
		minTimestamp int64
		maxTimestamp int64
	}

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
		ti.init(pp, tt.sids, tt.minTimestamp, tt.maxTimestamp)
		var got []blockMetadata
		for ti.nextBlock() {
			if ti.piHeap[0].curBlock.seriesID == 0 {
				t.Errorf("Expected curBlock to be initialized, but it was nil")
			}
			got = append(got, ti.piHeap[0].curBlock)
		}

		if !errors.Is(ti.Error(), tt.wantErr) {
			t.Errorf("Unexpected error: got %v, want %v", ti.err, tt.wantErr)
		}

		if diff := cmp.Diff(got, tt.want,
			cmpopts.IgnoreFields(blockMetadata{}, "timestamps"),
			cmpopts.IgnoreFields(blockMetadata{}, "elementIDs"),
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
				esList:       []*elements{},
				sids:         []common.SeriesID{1, 2, 3},
				minTimestamp: 1,
				maxTimestamp: 1,
			},
			{
				name:         "Test with single part",
				esList:       []*elements{esTS1},
				sids:         []common.SeriesID{1, 2, 3},
				minTimestamp: 1,
				maxTimestamp: 1,
				want: []blockMetadata{
					{seriesID: 1, count: 1, uncompressedSizeBytes: 881},
					{seriesID: 2, count: 1, uncompressedSizeBytes: 55},
					{seriesID: 3, count: 1, uncompressedSizeBytes: 8},
				},
			},
			{
				name:         "Test with multiple parts with different ts",
				esList:       []*elements{esTS1, esTS2},
				sids:         []common.SeriesID{1, 2, 3},
				minTimestamp: 1,
				maxTimestamp: 2,
				want: []blockMetadata{
					{seriesID: 1, count: 1, uncompressedSizeBytes: 881},
					{seriesID: 1, count: 1, uncompressedSizeBytes: 881},
					{seriesID: 2, count: 1, uncompressedSizeBytes: 55},
					{seriesID: 2, count: 1, uncompressedSizeBytes: 55},
					{seriesID: 3, count: 1, uncompressedSizeBytes: 8},
					{seriesID: 3, count: 1, uncompressedSizeBytes: 8},
				},
			},
			{
				name:         "Test with multiple parts with same ts",
				esList:       []*elements{esTS1, esTS1},
				sids:         []common.SeriesID{1, 2, 3},
				minTimestamp: 1,
				maxTimestamp: 2,
				want: []blockMetadata{
					{seriesID: 1, count: 1, uncompressedSizeBytes: 881},
					{seriesID: 1, count: 1, uncompressedSizeBytes: 881},
					{seriesID: 2, count: 1, uncompressedSizeBytes: 55},
					{seriesID: 2, count: 1, uncompressedSizeBytes: 55},
					{seriesID: 3, count: 1, uncompressedSizeBytes: 8},
					{seriesID: 3, count: 1, uncompressedSizeBytes: 8},
				},
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				tmpPath, defFn := test.Space(require.New(t))
				index, _ := newElementIndex(context.TODO(), tmpPath, 0)
				defer defFn()
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
				verify(t, tt, tst)
			})
		}
	})

	t.Run("file snapshot", func(t *testing.T) {
		tests := []testCtx{
			{
				name:         "Test with no data points",
				esList:       []*elements{},
				sids:         []common.SeriesID{1, 2, 3},
				minTimestamp: 1,
				maxTimestamp: 1,
			},
			{
				name:         "Test with single part",
				esList:       []*elements{esTS1},
				sids:         []common.SeriesID{1, 2, 3},
				minTimestamp: 1,
				maxTimestamp: 1,
				want: []blockMetadata{
					{seriesID: 1, count: 1, uncompressedSizeBytes: 881},
					{seriesID: 2, count: 1, uncompressedSizeBytes: 55},
					{seriesID: 3, count: 1, uncompressedSizeBytes: 8},
				},
			},
			{
				name:         "Test with multiple parts with different ts, the block will be merged",
				esList:       []*elements{esTS1, esTS2, esTS2},
				sids:         []common.SeriesID{1, 2, 3},
				minTimestamp: 1,
				maxTimestamp: 2,
				want: []blockMetadata{
					{seriesID: 1, count: 3, uncompressedSizeBytes: 2643},
					{seriesID: 2, count: 3, uncompressedSizeBytes: 165},
					{seriesID: 3, count: 3, uncompressedSizeBytes: 24},
				},
			},
			{
				name:         "Test with multiple parts with same ts, duplicated blocks will be merged",
				esList:       []*elements{esTS1, esTS1},
				sids:         []common.SeriesID{1, 2, 3},
				minTimestamp: 1,
				maxTimestamp: 2,
				want: []blockMetadata{
					{seriesID: 1, count: 2, uncompressedSizeBytes: 1762},
					{seriesID: 2, count: 2, uncompressedSizeBytes: 110},
					{seriesID: 3, count: 2, uncompressedSizeBytes: 16},
				},
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Run("merging on the fly", func(t *testing.T) {
					tmpPath, defFn := test.Space(require.New(t))
					fileSystem := fs.NewLocalFileSystem()
					defer defFn()

					tst, err := newTSTable(fileSystem, tmpPath, common.Position{},
						logger.GetLogger("test"), timestamp.TimeRange{}, option{flushTimeout: 0, elementIndexFlushTimeout: 0, mergePolicy: newDefaultMergePolicyForTesting()})
					require.NoError(t, err)
					for i, es := range tt.esList {
						tst.mustAddElements(es)
						for {
							snp := tst.currentSnapshot()
							if snp == nil {
								t.Logf("waiting for snapshot %d to be introduced", i)
								time.Sleep(100 * time.Millisecond)
								continue
							}
							if snp.creator != snapshotCreatorMemPart {
								snp.decRef()
								break
							}
							t.Logf("waiting for snapshot %d to be flushed or merged: current creator:%d, parts: %+v",
								i, snp.creator, snp.parts)
							snp.decRef()
							time.Sleep(100 * time.Millisecond)
						}
					}
					// wait until some parts are merged
					if len(tt.esList) > 0 {
						for {
							snp := tst.currentSnapshot()
							if snp == nil {
								time.Sleep(100 * time.Millisecond)
								continue
							}
							if len(snp.parts) == 1 || len(snp.parts) < len(tt.esList) {
								snp.decRef()
								break
							}
							t.Logf("waiting for snapshot to be merged: current creator:%d, parts: %+v", snp.creator, snp.parts)
							snp.decRef()
							time.Sleep(100 * time.Millisecond)
						}
					}
					verify(t, tt, tst)
				})

				t.Run("merging on close", func(t *testing.T) {
					t.Skip("the test is flaky due to unpredictable merge loop schedule.")
					tmpPath, defFn := test.Space(require.New(t))
					fileSystem := fs.NewLocalFileSystem()
					defer defFn()

					tst, err := newTSTable(fileSystem, tmpPath, common.Position{},
						logger.GetLogger("test"), timestamp.TimeRange{},
						option{
							flushTimeout:             defaultFlushTimeout,
							elementIndexFlushTimeout: defaultFlushTimeout,
							mergePolicy:              newDefaultMergePolicyForTesting(),
						})
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
							if len(snp.parts) == len(tt.esList) {
								snp.decRef()
								tst.Close()
								break
							}
							snp.decRef()
							time.Sleep(100 * time.Millisecond)
						}
					} else {
						tst.Close()
					}
					// reopen the table
					tst, err = newTSTable(fileSystem, tmpPath, common.Position{},
						logger.GetLogger("test"), timestamp.TimeRange{},
						option{
							flushTimeout:             defaultFlushTimeout,
							elementIndexFlushTimeout: defaultFlushTimeout,
							mergePolicy:              newDefaultMergePolicyForTesting(),
						})
					require.NoError(t, err)
					verify(t, tt, tst)
				})
			})
		}
	})
}

// nolint: unused
var tagProjections = map[int][]pbv1.TagProjection{
	1: {
		{Family: "arrTag", Names: []string{"strArrTag", "intArrTag"}},
		{Family: "binaryTag", Names: []string{"binaryTag"}},
		{Family: "singleTag", Names: []string{"strTag", "intTag"}},
	},
	2: {
		{Family: "singleTag", Names: []string{"strTag1", "strTag2"}},
	},
}

var esTS1 = &elements{
	seriesIDs:  []common.SeriesID{1, 2, 3},
	timestamps: []int64{1, 1, 1},
	elementIDs: []string{"11", "21", "31"},
	tagFamilies: [][]tagValues{
		{
			{
				tag: "arrTag", values: []*tagValue{
					{tag: "strArrTag", valueType: pbv1.ValueTypeStrArr, value: nil, valueArr: [][]byte{[]byte("value1"), []byte("value2")}},
					{tag: "intArrTag", valueType: pbv1.ValueTypeInt64Arr, value: nil, valueArr: [][]byte{convert.Int64ToBytes(25), convert.Int64ToBytes(30)}},
				},
			},
			{
				tag: "binaryTag", values: []*tagValue{
					{tag: "binaryTag", valueType: pbv1.ValueTypeBinaryData, value: longText, valueArr: nil},
				},
			},
			{
				tag: "singleTag", values: []*tagValue{
					{tag: "strTag", valueType: pbv1.ValueTypeStr, value: []byte("value1"), valueArr: nil},
					{tag: "intTag", valueType: pbv1.ValueTypeInt64, value: convert.Int64ToBytes(10), valueArr: nil},
				},
			},
		},
		{
			{
				tag: "singleTag", values: []*tagValue{
					{tag: "strTag1", valueType: pbv1.ValueTypeStr, value: []byte("tag1"), valueArr: nil},
					{tag: "strTag2", valueType: pbv1.ValueTypeStr, value: []byte("tag2"), valueArr: nil},
				},
			},
		},
		{}, // empty tagFamilies for seriesID 3
	},
}

var esTS2 = &elements{
	seriesIDs:  []common.SeriesID{1, 2, 3},
	timestamps: []int64{2, 2, 2},
	elementIDs: []string{"12", "22", "32"},
	tagFamilies: [][]tagValues{
		{
			{
				tag: "arrTag", values: []*tagValue{
					{tag: "strArrTag", valueType: pbv1.ValueTypeStrArr, value: nil, valueArr: [][]byte{[]byte("value5"), []byte("value6")}},
					{tag: "intArrTag", valueType: pbv1.ValueTypeInt64Arr, value: nil, valueArr: [][]byte{convert.Int64ToBytes(35), convert.Int64ToBytes(40)}},
				},
			},
			{
				tag: "binaryTag", values: []*tagValue{
					{tag: "binaryTag", valueType: pbv1.ValueTypeBinaryData, value: longText, valueArr: nil},
				},
			},
			{
				tag: "singleTag", values: []*tagValue{
					{tag: "strTag", valueType: pbv1.ValueTypeStr, value: []byte("value3"), valueArr: nil},
					{tag: "intTag", valueType: pbv1.ValueTypeInt64, value: convert.Int64ToBytes(30), valueArr: nil},
				},
			},
		},
		{
			{
				tag: "singleTag", values: []*tagValue{
					{tag: "strTag1", valueType: pbv1.ValueTypeStr, value: []byte("tag3"), valueArr: nil},
					{tag: "strTag2", valueType: pbv1.ValueTypeStr, value: []byte("tag4"), valueArr: nil},
				},
			},
		},
		{}, // empty tagFamilies for seriesID 6
	},
}

func generateHugeEs(startTimestamp, endTimestamp, timestamp int64) *elements {
	hugeEs := &elements{
		seriesIDs:   []common.SeriesID{},
		timestamps:  []int64{},
		elementIDs:  []string{},
		tagFamilies: [][]tagValues{},
	}
	for i := startTimestamp; i <= endTimestamp; i++ {
		hugeEs.seriesIDs = append(hugeEs.seriesIDs, 1)
		hugeEs.timestamps = append(hugeEs.timestamps, i)
		hugeEs.elementIDs = append(hugeEs.elementIDs, "1"+fmt.Sprint(i))
		hugeEs.tagFamilies = append(hugeEs.tagFamilies, []tagValues{
			{
				tag: "arrTag", values: []*tagValue{
					{tag: "strArrTag", valueType: pbv1.ValueTypeStrArr, value: nil, valueArr: [][]byte{[]byte("value5"), []byte("value6")}},
					{tag: "intArrTag", valueType: pbv1.ValueTypeInt64Arr, value: nil, valueArr: [][]byte{convert.Int64ToBytes(35), convert.Int64ToBytes(40)}},
				},
			},
			{
				tag: "binaryTag", values: []*tagValue{
					{tag: "binaryTag", valueType: pbv1.ValueTypeBinaryData, value: longText, valueArr: nil},
				},
			},
			{
				tag: "singleTag", values: []*tagValue{
					{tag: "strTag", valueType: pbv1.ValueTypeStr, value: []byte("value3"), valueArr: nil},
					{tag: "intTag", valueType: pbv1.ValueTypeInt64, value: convert.Int64ToBytes(30), valueArr: nil},
				},
			},
		})
	}
	hugeEs.seriesIDs = append(hugeEs.seriesIDs, []common.SeriesID{2, 3}...)
	hugeEs.timestamps = append(hugeEs.timestamps, []int64{timestamp, timestamp}...)
	hugeEs.elementIDs = append(hugeEs.elementIDs, []string{"2" + fmt.Sprint(timestamp), "3" + fmt.Sprint(timestamp)}...)
	hugeEs.tagFamilies = append(hugeEs.tagFamilies, [][]tagValues{{
		{
			tag: "singleTag", values: []*tagValue{
				{tag: "strTag1", valueType: pbv1.ValueTypeStr, value: []byte("tag3"), valueArr: nil},
				{tag: "strTag2", valueType: pbv1.ValueTypeStr, value: []byte("tag4"), valueArr: nil},
			},
		},
	}, {}}...)
	return hugeEs
}
