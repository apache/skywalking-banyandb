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
				Spans:   [][]byte{[]byte("span1")},
				SpanIDs: []string{"span1"},
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
				Spans:   [][]byte{[]byte("span1"), []byte("span4")},
				SpanIDs: []string{"span1", "span4"},
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
				pp, _ := s.getParts(nil, queryOpts.minTimestamp, queryOpts.maxTimestamp, []string{tt.traceID})
				bma := generateBlockMetadataArray()
				defer releaseBlockMetadataArray(bma)
				ti := &tstIter{}
				ti.init(bma, pp, []string{tt.traceID})

				var (
					result  queryResult
					cursors []*blockCursor
				)
				result.ctx = context.TODO()
				result.tagProjection = allTagProjections
				result.keys = map[string]int64{tt.traceID: 0}

				for ti.nextBlock() {
					bc := generateBlockCursor()
					p := ti.piPool[ti.idx]
					opts := queryOpts
					opts.TagProjection = allTagProjections
					bc.init(p.p, p.curBlock, opts)
					cursors = append(cursors, bc)
				}

				// Create cursor channel and send cursors through it
				cursorCh := make(chan scanCursorResult, len(cursors))
				for _, cursor := range cursors {
					cursorCh <- scanCursorResult{cursor: cursor}
				}
				close(cursorCh)

				cursorBatch := make(chan *scanBatch, 1)
				cursorBatch <- &scanBatch{
					traceBatch: traceBatch{
						traceIDs: []string{tt.traceID},
						keys:     map[string]int64{tt.traceID: 0},
					},
					cursorCh: cursorCh,
				}
				close(cursorBatch)

				// Create and close traceBatch channel (required for Release())
				result.cursorBatchCh = cursorBatch

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
					tst.mustAddTraces(traces, nil)
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
					tst.mustAddTraces(traces, nil)
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

func TestQueryResultMultipleBatches(t *testing.T) {
	tests := []struct {
		name         string
		tracesList   []*traces
		traceIDs     [][]string // Multiple batches of trace IDs
		want         []model.TraceResult
		minTimestamp int64
		maxTimestamp int64
	}{
		{
			name:         "Test with multiple batches from infinite sidx channel",
			tracesList:   []*traces{tsTS1, tsTS2},
			traceIDs:     [][]string{{"trace1"}, {"trace2"}, {"trace3"}},
			minTimestamp: 1,
			maxTimestamp: 2,
			want: []model.TraceResult{
				{
					Error: nil,
					TID:   "trace1",
					Key:   0, // Key from batch 0
					Tags: []model.Tag{
						{Name: "strArrTag", Values: []*modelv1.TagValue{strArrTagValue([]string{"value1", "value2"}), strArrTagValue([]string{"value5", "value6"})}},
						{Name: "strTag", Values: []*modelv1.TagValue{strTagValue("value1"), strTagValue("value4")}},
						{Name: "intTag", Values: []*modelv1.TagValue{int64TagValue(10), int64TagValue(40)}},
					},
					Spans:   [][]byte{[]byte("span1"), []byte("span4")},
					SpanIDs: []string{"span1", "span4"},
				},
				{
					Error: nil,
					TID:   "trace2",
					Key:   1, // Key from batch 1
					Tags: []model.Tag{
						{Name: "strArrTag", Values: []*modelv1.TagValue{strArrTagValue([]string{"value3", "value4"}), strArrTagValue([]string{"value7", "value8"})}},
						{Name: "strTag", Values: []*modelv1.TagValue{strTagValue("value2"), strTagValue("value5")}},
						{Name: "intTag", Values: []*modelv1.TagValue{int64TagValue(20), int64TagValue(50)}},
					},
					Spans:   [][]byte{[]byte("span2"), []byte("span5")},
					SpanIDs: []string{"span2", "span5"},
				},
				{
					Error: nil,
					TID:   "trace3",
					Key:   2, // Key from batch 2
					Tags: []model.Tag{
						{Name: "strArrTag", Values: []*modelv1.TagValue{strArrTagValue([]string{"value5", "value6"}), strArrTagValue([]string{"value9", "value10"})}},
						{Name: "strTag", Values: []*modelv1.TagValue{strTagValue("value3"), strTagValue("value6")}},
						{Name: "intTag", Values: []*modelv1.TagValue{int64TagValue(30), int64TagValue(60)}},
					},
					Spans:   [][]byte{[]byte("span3"), []byte("span6")},
					SpanIDs: []string{"span3", "span6"},
				},
			},
		},
		{
			name:         "Test with empty batches and batch exhaustion",
			tracesList:   []*traces{tsTS1, tsTS2},
			traceIDs:     [][]string{{"trace1"}, {}, {"trace2"}, {}, {"trace3"}}, // Empty batches in between
			minTimestamp: 1,
			maxTimestamp: 2,
			want: []model.TraceResult{
				{
					Error: nil,
					TID:   "trace1",
					Key:   0,
					Tags: []model.Tag{
						{Name: "strArrTag", Values: []*modelv1.TagValue{strArrTagValue([]string{"value1", "value2"}), strArrTagValue([]string{"value5", "value6"})}},
						{Name: "strTag", Values: []*modelv1.TagValue{strTagValue("value1"), strTagValue("value4")}},
						{Name: "intTag", Values: []*modelv1.TagValue{int64TagValue(10), int64TagValue(40)}},
					},
					Spans:   [][]byte{[]byte("span1"), []byte("span4")},
					SpanIDs: []string{"span1", "span4"},
				},
				{
					Error: nil,
					TID:   "trace2",
					Key:   2, // From batch index 2 (batch index 1 was empty)
					Tags: []model.Tag{
						{Name: "strArrTag", Values: []*modelv1.TagValue{strArrTagValue([]string{"value3", "value4"}), strArrTagValue([]string{"value7", "value8"})}},
						{Name: "strTag", Values: []*modelv1.TagValue{strTagValue("value2"), strTagValue("value5")}},
						{Name: "intTag", Values: []*modelv1.TagValue{int64TagValue(20), int64TagValue(50)}},
					},
					Spans:   [][]byte{[]byte("span2"), []byte("span5")},
					SpanIDs: []string{"span2", "span5"},
				},
				{
					Error: nil,
					TID:   "trace3",
					Key:   4, // From batch index 4 (batch index 3 was empty)
					Tags: []model.Tag{
						{Name: "strArrTag", Values: []*modelv1.TagValue{strArrTagValue([]string{"value5", "value6"}), strArrTagValue([]string{"value9", "value10"})}},
						{Name: "strTag", Values: []*modelv1.TagValue{strTagValue("value3"), strTagValue("value6")}},
						{Name: "intTag", Values: []*modelv1.TagValue{int64TagValue(30), int64TagValue(60)}},
					},
					Spans:   [][]byte{[]byte("span3"), []byte("span6")},
					SpanIDs: []string{"span3", "span6"},
				},
			},
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

				// Create multiple batches, each simulating a batch from sidx
				cursorBatch := make(chan *scanBatch, len(tt.traceIDs))

				for batchIdx, batchTraceIDs := range tt.traceIDs {
					bma := generateBlockMetadataArray()
					ti := &tstIter{}
					ti.init(bma, pp, batchTraceIDs)

					var cursors []*blockCursor
					for ti.nextBlock() {
						bc := generateBlockCursor()
						p := ti.piPool[ti.idx]
						opts := queryOpts
						opts.TagProjection = allTagProjections
						bc.init(p.p, p.curBlock, opts)
						cursors = append(cursors, bc)
					}
					releaseBlockMetadataArray(bma)

					// Create cursor channel for this batch
					cursorCh := make(chan scanCursorResult, len(cursors))
					for _, cursor := range cursors {
						cursorCh <- scanCursorResult{cursor: cursor}
					}
					close(cursorCh)

					// Create keys for this batch
					keys := make(map[string]int64, len(batchTraceIDs))
					for _, traceID := range batchTraceIDs {
						keys[traceID] = int64(batchIdx)
					}

					// Add batch to channel
					cursorBatch <- &scanBatch{
						traceBatch: traceBatch{
							traceIDs: batchTraceIDs,
							keys:     keys,
							seq:      batchIdx,
						},
						cursorCh: cursorCh,
					}
				}
				close(cursorBatch)

				// Set up query result
				var result queryResult
				result.ctx = context.TODO()
				result.tagProjection = allTagProjections
				result.cursorBatchCh = cursorBatch

				defer result.Release()

				var got []model.TraceResult
				for {
					r := result.Pull()
					if r == nil {
						break
					}
					got = append(got, *r)
				}

				if diff := cmp.Diff(got, tt.want,
					protocmp.IgnoreUnknown(), protocmp.Transform()); diff != "" {
					t.Errorf("Unexpected []pbv1.Result (-got +want):\n%s", diff)
				}
			}

			// Test with memory snapshot to validate multi-batch handling
			// Note: File snapshot test is skipped as it has non-deterministic ordering
			// which is unrelated to the multi-batch functionality being tested here.
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
					tst.mustAddTraces(traces, nil)
					time.Sleep(100 * time.Millisecond)
				}
				verify(t, tst)
			})
		})
	}
}
