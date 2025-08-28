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
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/apache/skywalking-banyandb/api/common"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/run"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
	"github.com/apache/skywalking-banyandb/pkg/watcher"
)

func TestQueryResult(t *testing.T) {
	// TODO: please fix this test @ButterBright
	t.Skip("skipping query test")
	tests := []struct {
		wantErr      error
		name         string
		tracesList   []*traces
		traceID      string
		want         []model.TraceResult
		minTimestamp int64
		maxTimestamp int64
	}{
		{
			name:         "Test with single trace data from tsTS1",
			tracesList:   []*traces{tsTS1},
			traceID:      "trace1",
			minTimestamp: 1,
			maxTimestamp: 1,
			want: []model.TraceResult{{
				Error: nil,
				TID:   "trace1",
				Tags: []model.Tag{
					{Name: "strArrTag", Values: []*modelv1.TagValue{strArrTagValue([]string{"value1", "value2"})}},
					{Name: "strTag", Values: []*modelv1.TagValue{strTagValue("value1")}},
					{Name: "intTag", Values: []*modelv1.TagValue{int64TagValue(10)}},
				},
				Spans: [][]byte{[]byte("span1")},
			}},
		},
		{
			name:         "Test with multiple trace data from tsTS1 and tsTS2",
			tracesList:   []*traces{tsTS1, tsTS2},
			traceID:      "trace1",
			minTimestamp: 1,
			maxTimestamp: 2,
			want: []model.TraceResult{{
				Error: nil,
				TID:   "trace1",
				Tags: []model.Tag{
					{Name: "strArrTag", Values: []*modelv1.TagValue{strArrTagValue([]string{"value1", "value2"}), strArrTagValue([]string{"value5", "value6"})}},
					{Name: "strTag", Values: []*modelv1.TagValue{strTagValue("value1"), strTagValue("value4")}},
					{Name: "intTag", Values: []*modelv1.TagValue{int64TagValue(10), int64TagValue(40)}},
				},
				Spans: [][]byte{[]byte("span1"), []byte("span4")},
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
				bma := generateBlockMetadataArray()
				defer releaseBlockMetadataArray(bma)
				ti := &tstIter{}
				ti.init(bma, pp, tt.traceID)

				var result queryResult
				result.ctx = context.TODO()
				// Query all tags
				result.tagProjection = allTagProjections
				for ti.nextBlock() {
					bc := generateBlockCursor()
					p := ti.piPool[ti.idx]
					opts := queryOpts
					opts.TagProjection = allTagProjections
					bc.init(p.p, p.curBlock, opts)
					result.data = append(result.data, bc)
				}
				defer result.Release()

				var got []model.TraceResult
				for {
					r := result.Pull()
					if r == nil {
						break
					}
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
				for _, traces := range tt.tracesList {
					tst.mustAddTraces(traces)
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
					logger.GetLogger("test"), timestamp.TimeRange{}, option{flushTimeout: 0, mergePolicy: newDefaultMergePolicyForTesting(), protector: protector.Nop{}}, nil)
				require.NoError(t, err)
				for _, traces := range tt.tracesList {
					tst.mustAddTraces(traces)
					time.Sleep(100 * time.Millisecond)
				}
				// wait until the introducer is done
				if len(tt.tracesList) > 0 {
					for {
						snp := tst.currentSnapshot()
						if snp == nil {
							time.Sleep(100 * time.Millisecond)
							continue
						}
						if snp.creator == snapshotCreatorMemPart {
							snp.decRef()
							time.Sleep(100 * time.Millisecond)
							continue
						}
						snp.decRef()
						tst.Close()
						break
					}
				}

				// reopen the table
				tst, err = newTSTable(fileSystem, tmpPath, common.Position{},
					logger.GetLogger("test"), timestamp.TimeRange{}, option{
						flushTimeout: defaultFlushTimeout, mergePolicy: newDefaultMergePolicyForTesting(),
						protector: protector.Nop{},
					}, nil)
				require.NoError(t, err)

				verify(t, tst)
			})
		})
	}
}
