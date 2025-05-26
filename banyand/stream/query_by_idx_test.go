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
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/run"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/watcher"
)

type fakeMemory struct {
	availableBytes      uint64
	limit               uint64
	acquireErr          error
	expectQuotaExceeded bool
}

func (f *fakeMemory) AvailableBytes() int64 {
	if f.expectQuotaExceeded {
		return 10
	}
	return 10000
}

func (f *fakeMemory) GetLimit() uint64 {
	return f.limit
}

func (f *fakeMemory) AcquireResource(ctx context.Context, size uint64) error {
	return f.acquireErr
}

func (f *fakeMemory) Name() string {
	return "fake-memory"
}

func (f *fakeMemory) FlagSet() *run.FlagSet {
	return run.NewFlagSet("fake-memory")
}

func (f *fakeMemory) Validate() error {
	return nil
}

func (f *fakeMemory) PreRun(ctx context.Context) error {
	return nil
}

func (f *fakeMemory) GracefulStop() {
	// no-op for test
}

func (f *fakeMemory) Serve() run.StopNotify {
	ch := make(chan struct{})
	close(ch)
	return ch
}

func TestQueryResult_QuotaExceeded(t *testing.T) {
	type testCtx struct {
		wantErr             error
		name                string
		esList              []*elements
		sids                []common.SeriesID
		want                []blockMetadata
		minTimestamp        int64
		maxTimestamp        int64
		expectQuotaExceeded bool
	}

	verify := func(t *testing.T, tt testCtx, tst *tsTable) {
		defer tst.Close()
		qr := &idxResult{
			pm:   &fakeMemory{expectQuotaExceeded: tt.expectQuotaExceeded},
			tabs: []*tsTable{tst},
		}
		qo := queryOptions{
			minTimestamp: tt.minTimestamp,
			maxTimestamp: tt.maxTimestamp,
			sortedSids:   tt.sids,
		}
		err := qr.scanParts(context.TODO(), qo)
		if tt.expectQuotaExceeded {
			require.Error(t, err)
			require.Contains(t, err.Error(), "quota exceeded", "expected quota to be exceeded but got: %v", err)
			return
		} else {
			require.NoError(t, err)
		}
		var got []blockMetadata
		for _, data := range qr.data {
			got = append(got, data.bm)
		}
		if diff := cmp.Diff(got, tt.want,
			cmpopts.IgnoreFields(blockMetadata{}, "timestamps"),
			cmpopts.IgnoreFields(blockMetadata{}, "elementIDs"),
			cmpopts.IgnoreFields(blockMetadata{}, "tagFamilies"),
			cmp.AllowUnexported(blockMetadata{}),
		); diff != "" {
			t.Errorf("Unexpected blockMetadata (-got +want):\n%s", diff)
		}
	}

	t.Run("memory snapshot", func(t *testing.T) {
		tests := []testCtx{
			{
				name:                "TestQuotaNotExceeded_ExpectSuccess",
				esList:              []*elements{esTS1},
				sids:                []common.SeriesID{1, 2, 3},
				minTimestamp:        1,
				maxTimestamp:        1,
				expectQuotaExceeded: false,
				want: []blockMetadata{
					{seriesID: 1, count: 1, uncompressedSizeBytes: 889},
					{seriesID: 2, count: 1, uncompressedSizeBytes: 63},
					{seriesID: 3, count: 1, uncompressedSizeBytes: 16},
				},
			},
			{
				name:                "TestQuotaExceeded_ExpectError",
				esList:              []*elements{esTS1},
				sids:                []common.SeriesID{1, 2, 3},
				minTimestamp:        1,
				maxTimestamp:        1,
				expectQuotaExceeded: true,
				want: []blockMetadata{
					{seriesID: 1, count: 1, uncompressedSizeBytes: 889},
					{seriesID: 2, count: 1, uncompressedSizeBytes: 63},
					{seriesID: 3, count: 1, uncompressedSizeBytes: 16},
				},
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				tmpPath, defFn := test.Space(require.New(t))
				index, _ := newElementIndex(context.TODO(), tmpPath, 0, nil)
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
}
