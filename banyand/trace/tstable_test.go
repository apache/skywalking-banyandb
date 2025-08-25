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

package trace

import (
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/run"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/watcher"
)

func Test_tsTable_mustAddTraces(t *testing.T) {
	tests := []struct {
		name   string
		tsList []*traces
		want   int
	}{
		{
			name: "Test with empty traces",
			tsList: []*traces{
				{
					traceIDs:   []string{},
					timestamps: []int64{},
					tags:       [][]*tagValue{},
					spans:      [][]byte{},
				},
			},
			want: 0,
		},
		{
			name: "Test with one item in traces",
			tsList: []*traces{
				{
					traceIDs:   []string{"trace1"},
					timestamps: []int64{1},
					tags: [][]*tagValue{
						{
							{tag: "strArrTag", valueType: pbv1.ValueTypeStrArr, value: nil, valueArr: [][]byte{[]byte("value1"), []byte("value2")}},
							{tag: "intArrTag", valueType: pbv1.ValueTypeInt64Arr, value: nil, valueArr: [][]byte{convert.Int64ToBytes(25), convert.Int64ToBytes(30)}},
						},
					},
					spans: [][]byte{[]byte("span1")},
				},
			},
			want: 1,
		},
		{
			name: "Test with multiple calls to mustAddTraces",
			tsList: []*traces{
				tsTS1,
				tsTS2,
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
			for _, ts := range tt.tsList {
				tst.mustAddTraces(ts)
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
		tsList       []*traces
		wantErr      error
		name         string
		tid          string
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
		ti.init(bma, pp, tt.tid)
		var got []blockMetadata
		for ti.nextBlock() {
			if ti.piPool[ti.idx].curBlock.traceID == "" {
				t.Errorf("Expected curBlock to be initialized, but it was nil")
			}
			var bm blockMetadata
			bm.copyFrom(ti.piPool[ti.idx].curBlock)
			got = append(got, bm)
		}

		if !errors.Is(ti.Error(), tt.wantErr) {
			t.Errorf("Unexpected error: got %v, want %v", ti.err, tt.wantErr)
		}

		if diff := cmp.Diff(got, tt.want,
			cmpopts.IgnoreFields(blockMetadata{}, "timestamps"),
			cmpopts.IgnoreFields(blockMetadata{}, "spans"),
			cmpopts.IgnoreFields(blockMetadata{}, "tags"),
			cmpopts.IgnoreFields(blockMetadata{}, "tagType"),
			cmp.AllowUnexported(blockMetadata{}),
		); diff != "" {
			t.Errorf("Unexpected blockMetadata (-got +want):\n%s", diff)
		}
		return s.epoch
	}

	t.Run("memory snapshot", func(t *testing.T) {
		tests := []testCtx{
			{
				name:   "Test with no traces",
				tsList: []*traces{},
			},
			{
				name:         "Test with single part",
				tsList:       []*traces{tsTS1},
				tid:          "trace1",
				minTimestamp: 1,
				maxTimestamp: 2,
				want: []blockMetadata{
					{traceID: "trace1", count: 1, uncompressedSpanSizeBytes: 5},
				},
			},
			{
				name:         "Test with multiple parts",
				tsList:       []*traces{tsTS1, tsTS2},
				tid:          "trace1",
				minTimestamp: 1,
				maxTimestamp: 2,
				want: []blockMetadata{
					{traceID: "trace1", count: 1, uncompressedSpanSizeBytes: 5},
					{traceID: "trace1", count: 1, uncompressedSpanSizeBytes: 5},
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
				for _, ts := range tt.tsList {
					tst.mustAddTraces(ts)
					time.Sleep(100 * time.Millisecond)
				}
				verify(t, tt, tst)
			})
		}
	})
}

var allTagProjections = &model.TagProjection{
	Names: []string{"strArrTag", "strTag", "intTag"},
}

var tsTS1 = &traces{
	traceIDs:   []string{"trace1", "trace2", "trace3"},
	timestamps: []int64{1, 1, 1},
	tags: [][]*tagValue{
		{
			{tag: "strArrTag", valueType: pbv1.ValueTypeStrArr, value: nil, valueArr: [][]byte{[]byte("value1"), []byte("value2")}},
			{tag: "strTag", valueType: pbv1.ValueTypeStr, value: []byte("value1"), valueArr: nil},
			{tag: "intTag", valueType: pbv1.ValueTypeInt64, value: convert.Int64ToBytes(10), valueArr: nil},
		},
		{
			{tag: "strArrTag", valueType: pbv1.ValueTypeStrArr, value: nil, valueArr: [][]byte{[]byte("value3"), []byte("value4")}},
			{tag: "strTag", valueType: pbv1.ValueTypeStr, value: []byte("value2"), valueArr: nil},
			{tag: "intTag", valueType: pbv1.ValueTypeInt64, value: convert.Int64ToBytes(20), valueArr: nil},
		},
		{
			{tag: "strArrTag", valueType: pbv1.ValueTypeStrArr, value: nil, valueArr: [][]byte{[]byte("value5"), []byte("value6")}},
			{tag: "strTag", valueType: pbv1.ValueTypeStr, value: []byte("value3"), valueArr: nil},
			{tag: "intTag", valueType: pbv1.ValueTypeInt64, value: convert.Int64ToBytes(30), valueArr: nil},
		},
	},
	spans: [][]byte{[]byte("span1"), []byte("span2"), []byte("span3")},
}

var tsTS2 = &traces{
	traceIDs:   []string{"trace1", "trace2", "trace3"},
	timestamps: []int64{2, 2, 2},
	tags: [][]*tagValue{
		{
			{tag: "strArrTag", valueType: pbv1.ValueTypeStrArr, value: nil, valueArr: [][]byte{[]byte("value5"), []byte("value6")}},
			{tag: "strTag", valueType: pbv1.ValueTypeStr, value: []byte("value4"), valueArr: nil},
			{tag: "intTag", valueType: pbv1.ValueTypeInt64, value: convert.Int64ToBytes(40), valueArr: nil},
		},
		{
			{tag: "strArrTag", valueType: pbv1.ValueTypeStrArr, value: nil, valueArr: [][]byte{[]byte("value7"), []byte("value8")}},
			{tag: "strTag", valueType: pbv1.ValueTypeStr, value: []byte("value5"), valueArr: nil},
			{tag: "intTag", valueType: pbv1.ValueTypeInt64, value: convert.Int64ToBytes(50), valueArr: nil},
		},
		{
			{tag: "strArrTag", valueType: pbv1.ValueTypeStrArr, value: nil, valueArr: [][]byte{[]byte("value9"), []byte("value10")}},
			{tag: "strTag", valueType: pbv1.ValueTypeStr, value: []byte("value6"), valueArr: nil},
			{tag: "intTag", valueType: pbv1.ValueTypeInt64, value: convert.Int64ToBytes(60), valueArr: nil},
		},
	},
	spans: [][]byte{[]byte("span4"), []byte("span5"), []byte("span6")},
}

func generateHugeTraces(num int) *traces {
	traces := &traces{
		traceIDs:   []string{},
		timestamps: []int64{},
		tags:       [][]*tagValue{},
		spans:      [][]byte{},
	}
	for i := 1; i <= num; i++ {
		traces.traceIDs = append(traces.traceIDs, "trace1")
		traces.timestamps = append(traces.timestamps, int64(i))
		traces.tags = append(traces.tags, []*tagValue{
			{tag: "strArrTag", valueType: pbv1.ValueTypeStrArr, value: nil, valueArr: [][]byte{[]byte("value5"), []byte("value6")}},
			{tag: "intArrTag", valueType: pbv1.ValueTypeInt64Arr, value: nil, valueArr: [][]byte{convert.Int64ToBytes(35), convert.Int64ToBytes(40)}},
			{tag: "binaryTag", valueType: pbv1.ValueTypeBinaryData, value: longText, valueArr: nil},
			{tag: "strTag", valueType: pbv1.ValueTypeStr, value: []byte("value3"), valueArr: nil},
			{tag: "intTag", valueType: pbv1.ValueTypeInt64, value: convert.Int64ToBytes(30), valueArr: nil},
		})
		traces.spans = append(traces.spans, []byte("span1"))
	}
	traces.traceIDs = append(traces.traceIDs, []string{"trace2", "trace3"}...)
	traces.timestamps = append(traces.timestamps, []int64{int64(num + 1), int64(num + 2)}...)
	traces.tags = append(traces.tags, [][]*tagValue{
		{
			{tag: "strTag1", valueType: pbv1.ValueTypeStr, value: []byte("tag3"), valueArr: nil},
			{tag: "strTag2", valueType: pbv1.ValueTypeStr, value: []byte("tag4"), valueArr: nil},
		},
		{}, // empty tags
	}...)
	traces.spans = append(traces.spans, [][]byte{[]byte("span2"), []byte("span3")}...)
	return traces
}
