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
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	itest "github.com/apache/skywalking-banyandb/banyand/internal/test"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/cgroups"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

func TestBlockScanner_QuotaExceeded(t *testing.T) {
	type testCtx struct {
		name                string
		esList              []*elements
		sids                []common.SeriesID
		want                []blockMetadata
		minTimestamp        int64
		maxTimestamp        int64
		expectQuotaExceeded bool
		asc                 bool
	}

	tests := []testCtx{
		{
			name:                "TestQuotaNotExceeded_ExpectSuccess",
			esList:              []*elements{esTS1},
			sids:                []common.SeriesID{1, 2, 3},
			minTimestamp:        1,
			maxTimestamp:        1,
			expectQuotaExceeded: false,
			want: []blockMetadata{
				{seriesID: 1, count: 1, uncompressedSizeBytes: 859},
				{seriesID: 2, count: 1, uncompressedSizeBytes: 47},
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
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Initialize a tstIter object.
			tmpPath, defFn := test.Space(require.New(t))
			fileSystem := fs.NewLocalFileSystem()
			defer defFn()
			lease := newTestRootLease(t, tmpPath)
			tst, err := newTSTableWithLease(fileSystem, tmpPath, common.Position{},
				logger.GetLogger("test"), timestamp.TimeRange{}, option{flushTimeout: 0, mergePolicy: newDefaultMergePolicyForTesting(), protector: protector.Nop{}}, nil, lease)
			require.NoError(t, err)
			for _, es := range tt.esList {
				tst.mustAddElements(es)
				time.Sleep(100 * time.Millisecond)
			}
			// wait until every introduced mem part has been flushed to disk;
			// inspecting only snapshot.creator races with concurrent introductions
			// (a flush of part N can land while part N+1 is still a mem part).
			if len(tt.esList) > 0 {
				require.Eventually(t, allPartsFlushed(tst), 30*time.Second, 100*time.Millisecond,
					"mem parts not flushed to disk in time")
				tst.Close()
			}

			// reopen the table
			tst, err = newTSTableWithLease(fileSystem, tmpPath, common.Position{},
				logger.GetLogger("test"), timestamp.TimeRange{}, option{
					flushTimeout: defaultFlushTimeout, mergePolicy: newDefaultMergePolicyForTesting(),
					protector: protector.Nop{},
				}, nil, lease)
			require.NoError(t, err)

			defer tst.Close()
			workerSize := cgroups.CPUs()
			var workerWg sync.WaitGroup
			batchCh := make(chan *blockScanResultBatch, workerSize)
			workerWg.Add(workerSize)

			qo := queryOptions{
				minTimestamp: tt.minTimestamp,
				maxTimestamp: tt.maxTimestamp,
				sortedSids:   tt.sids,
			}
			var parts []*part
			s := tst.currentSnapshot()
			require.NotNil(t, s)
			parts, _ = s.getParts(parts, qo.minTimestamp, qo.maxTimestamp)
			bsn := &blockScanner{
				parts: getDisjointParts(parts, tt.asc),
				qo:    qo,
				asc:   tt.asc,
				pm:    &itest.MockMemoryProtector{ExpectQuotaExceeded: tt.expectQuotaExceeded},
			}

			var (
				got     []blockMetadata
				mu      sync.Mutex
				errSeen atomic.Bool
			)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			for i := 0; i < workerSize; i++ {
				go func() {
					defer workerWg.Done()
					for {
						select {
						case <-ctx.Done():
							return
						case batch, ok := <-batchCh:
							if !ok {
								return
							}
							if batch.err != nil {
								if tt.expectQuotaExceeded {
									require.Error(t, batch.err)
									require.Contains(t, batch.err.Error(), "quota exceeded")
									errSeen.Store(true)
									releaseBlockScanResultBatch(batch)
									cancel() // stop all worker
									return
								}
								require.NoError(t, batch.err)
								releaseBlockScanResultBatch(batch)
								continue
							}
							mu.Lock()
							for _, bs := range batch.bss {
								got = append(got, bs.bm)
							}
							mu.Unlock()
							releaseBlockScanResultBatch(batch)
						}
					}
				}()
			}
			bsn.scan(ctx, batchCh)
			close(batchCh)
			workerWg.Wait()

			if tt.expectQuotaExceeded {
				if !errSeen.Load() {
					t.Errorf("Expected quota exceeded error, but none occurred")
				}
				return
			}

			sort.Slice(got, func(i, j int) bool {
				return got[i].seriesID < got[j].seriesID
			})

			if diff := cmp.Diff(got, tt.want,
				cmpopts.IgnoreFields(blockMetadata{}, "timestamps"),
				cmpopts.IgnoreFields(blockMetadata{}, "elementIDs"),
				cmpopts.IgnoreFields(blockMetadata{}, "tagFamilies"),
				cmpopts.IgnoreFields(blockMetadata{}, "tagType"),
				cmp.AllowUnexported(blockMetadata{}),
			); diff != "" {
				t.Errorf("Unexpected blockMetadata (-got +want):\n%s", diff)
			}
		})
	}
}

// allPartsFlushed returns a predicate that is true once tst's current
// snapshot exists and every part is file-backed (no mp != nil).
// It is used by file_snapshot tests to wait until every introduced mem
// part has been flushed before closing/reopening the table.
func allPartsFlushed(tst *tsTable) func() bool {
	return func() bool {
		snp := tst.currentSnapshot()
		if snp == nil {
			return false
		}
		defer snp.decRef()
		for _, pw := range snp.parts {
			if pw.mp != nil {
				return false
			}
		}
		return true
	}
}

// recordingProtector is a protector.Memory fake with a fixed available-bytes figure
// that records every AcquireResource request.
type recordingProtector struct {
	protector.Memory
	acquired  []uint64
	available int64
}

func (r *recordingProtector) AvailableBytes() int64 { return r.available }

func (r *recordingProtector) AcquireResource(_ context.Context, size uint64) error {
	r.acquired = append(r.acquired, size)
	return nil
}

// newUniformElements builds one element per series with identical tags, so every
// series yields one block of the same uncompressed size.
func newUniformElements(seriesCount int) *elements {
	es := &elements{}
	for i := 1; i <= seriesCount; i++ {
		es.seriesIDs = append(es.seriesIDs, common.SeriesID(i))
		es.timestamps = append(es.timestamps, 1)
		es.elementIDs = append(es.elementIDs, uint64(i))
		es.tagFamilies = append(es.tagFamilies, []tagValues{{
			tag: "singleTag", values: []*tagValue{
				{tag: "strTag", valueType: pbv1.ValueTypeStr, value: []byte("value-0123456789"), valueArr: nil},
			},
		}})
	}
	return es
}

// uniformSeriesCount spans two full batches and a partial third one.
const uniformSeriesCount = blockScannerBatchSize*2 + 8

type scanOutcome struct {
	err     error
	batches [][]uint64 // uncompressed block sizes per delivered batch.
}

func scanUniform(t *testing.T, pm protector.Memory) scanOutcome {
	t.Helper()
	tmpPath, defFn := test.Space(require.New(t))
	defer defFn()
	fileSystem := fs.NewLocalFileSystem()
	lease := newTestRootLease(t, tmpPath)
	tst, err := newTSTableWithLease(fileSystem, tmpPath, common.Position{},
		logger.GetLogger("test"), timestamp.TimeRange{}, option{flushTimeout: 0, mergePolicy: newDefaultMergePolicyForTesting(), protector: protector.Nop{}}, nil, lease)
	require.NoError(t, err)
	tst.mustAddElements(newUniformElements(uniformSeriesCount))
	require.Eventually(t, allPartsFlushed(tst), 30*time.Second, 100*time.Millisecond, "mem parts not flushed to disk in time")
	tst.Close()
	tst, err = newTSTableWithLease(fileSystem, tmpPath, common.Position{},
		logger.GetLogger("test"), timestamp.TimeRange{}, option{
			flushTimeout: defaultFlushTimeout, mergePolicy: newDefaultMergePolicyForTesting(), protector: protector.Nop{},
		}, nil, lease)
	require.NoError(t, err)
	defer tst.Close()

	sids := make([]common.SeriesID, 0, uniformSeriesCount)
	for i := 1; i <= uniformSeriesCount; i++ {
		sids = append(sids, common.SeriesID(i))
	}
	qo := queryOptions{minTimestamp: 1, maxTimestamp: 1, sortedSids: sids}
	var parts []*part
	s := tst.currentSnapshot()
	require.NotNil(t, s)
	parts, _ = s.getParts(parts, qo.minTimestamp, qo.maxTimestamp)
	bsn := &blockScanner{parts: getDisjointParts(parts, true), qo: qo, asc: true, pm: pm}

	var out scanOutcome
	batchCh := make(chan *blockScanResultBatch)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for batch := range batchCh {
			if batch.err != nil {
				out.err = batch.err
			}
			sizes := make([]uint64, 0, len(batch.bss))
			for i := range batch.bss {
				sizes = append(sizes, batch.bss[i].bm.uncompressedSizeBytes)
			}
			out.batches = append(out.batches, sizes)
			releaseBlockScanResultBatch(batch)
		}
	}()
	bsn.scan(context.Background(), batchCh)
	close(batchCh)
	<-done
	return out
}

func sumSizes(sizes []uint64) uint64 {
	var total uint64
	for _, v := range sizes {
		total += v
	}
	return total
}

func TestBlockScanner_QuotaCountsEachBlockOncePerBatch(t *testing.T) {
	// Learn the real block size with an unbounded quota.
	probe := scanUniform(t, &recordingProtector{available: 1 << 40})
	require.NoError(t, probe.err)
	require.Len(t, probe.batches, 3)
	blockBytes := probe.batches[0][0]
	require.NotZero(t, blockBytes)
	batchBytes := sumSizes(probe.batches[0])
	require.Equal(t, uint64(blockScannerBatchSize)*blockBytes, batchBytes)

	t.Run("quota just above one batch's bytes succeeds", func(t *testing.T) {
		// The old running total reached about b*k*(k+1)/2 for k blocks, far above this quota.
		pm := &recordingProtector{available: int64(batchBytes) + 1}
		out := scanUniform(t, pm)
		require.NoError(t, out.err)
		require.Len(t, out.batches, 3)
		require.Len(t, out.batches[0], blockScannerBatchSize)
	})

	t.Run("quota below one batch's bytes reports the batch's own figure", func(t *testing.T) {
		pm := &recordingProtector{available: int64(batchBytes) - 1}
		out := scanUniform(t, pm)
		require.Error(t, out.err)
		require.Contains(t, out.err.Error(), "block scan quota exceeded")
		require.Contains(t, out.err.Error(),
			fmt.Sprintf("used %d bytes, quota is %d bytes", batchBytes, batchBytes-1))
	})

	t.Run("totals do not accumulate across batches", func(t *testing.T) {
		pm := &recordingProtector{available: int64(batchBytes)}
		out := scanUniform(t, pm)
		require.NoError(t, out.err)
		require.Len(t, out.batches, 3)
		// Only full batches reach AcquireResource, and each asks for its own bytes.
		require.Equal(t, []uint64{batchBytes, batchBytes}, pm.acquired)
	})
}
