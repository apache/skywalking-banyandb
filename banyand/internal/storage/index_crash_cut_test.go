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

package storage

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/native"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

// TestSeriesIndex_CrashCut_AfterBatch_ReopenedRootIsComplete pins NIDX-03
// §12 item 6. The first batch is admitted and this test waits for it to
// become durable (a real on-disk generation); a second batch is then
// admitted and the owner is ABANDONED without ever calling Close -- no
// graceful shutdown, no final drain -- the same way a crashed process never
// runs one. Reopening the SAME directory with the real production
// constructor (newSeriesIndex, a fresh lease -- a crashed process's lease
// is gone with it) must see a complete generation (never a torn count
// between the two batch sizes) and the data durable before the cut must
// have survived.
func TestSeriesIndex_CrashCut_AfterBatch_ReopenedRootIsComplete(t *testing.T) {
	ctx := context.Background()
	dir, fn := setUp(require.New(t))
	defer fn()
	si, err := newSeriesIndex(ctx, dir, 10, 0, nil, &testRootLease{})
	require.NoError(t, err)

	const firstBatch = 5
	require.NoError(t, si.Insert(buildTestSeriesDocsNamed(t, "crash-cut-before", firstBatch)))

	indexPath := filepath.Join(dir, seriesIndexDirName)
	require.Eventually(t, func() bool {
		generation, openErr := native.OpenReadOnlyGeneration(indexPath)
		if openErr != nil {
			require.True(t, errors.Is(openErr, native.ErrNoSnapshot), "unexpected open error: %v", openErr)
			return false
		}
		defer func() { _ = generation.Close() }()
		count, countErr := generation.VisibleDocCount()
		require.NoError(t, countErr)
		return count == firstBatch
	}, 5*time.Second, time.Millisecond, "the first batch must become durable before the cut")

	// Admit a second batch and abandon si right here -- no Close, no final
	// drain -- simulating a crash in the window between admission and the
	// next scheduled asynchronous persist. si is deliberately never closed
	// again; its in-memory lease is simply discarded, the same way a real
	// crash releases a file-backed lease without any explicit Revoke/Close.
	const secondBatch = 3
	require.NoError(t, si.Insert(buildTestSeriesDocsNamed(t, "crash-cut-after", secondBatch)))

	si2, err := newSeriesIndex(ctx, dir, 10, 0, nil, &testRootLease{})
	require.NoError(t, err)
	defer func() { require.NoError(t, si2.Close()) }()

	generation, err := native.OpenReadOnlyGeneration(indexPath)
	require.NoError(t, err, "a directory abandoned without Close must still reopen as a complete generation")
	defer func() { _ = generation.Close() }()
	count, err := generation.VisibleDocCount()
	require.NoError(t, err)
	require.True(t, count == firstBatch || count == firstBatch+secondBatch,
		"reopened generation must be complete (%d or %d), got %d -- never a torn count", firstBatch, firstBatch+secondBatch, count)
	require.GreaterOrEqual(t, count, int64(firstBatch), "data durable before the cut must survive the abandonment")
}

// TestSeriesIndex_CrashCut_MidExternalReceive_ReopenedRootIsComplete pins
// NIDX-03 §7/§12 item 6: a receive that never reaches CompleteSegment (the
// window a crash mid external receive would cut through) must never become
// visible, and -- unlike a graceful Close, which runs abortExternalStreamers
// and deletes the staged file -- a real crash leaves the staged
// ".native-external-*" file on disk exactly where WriteChunk left it. This
// abandons the streamer without calling Close (si is never closed) so the
// staged file survives, then asserts it is still there, that data admitted
// and made durable before the cut survives reopen, and that the abandoned
// segment's data never becomes visible.
func TestSeriesIndex_CrashCut_MidExternalReceive_ReopenedRootIsComplete(t *testing.T) {
	ctx := context.Background()
	dir, fn := setUp(require.New(t))
	defer fn()
	si, err := newSeriesIndex(ctx, dir, 0, 0, nil, &testRootLease{})
	require.NoError(t, err)

	// Data durable before the cut: a synchronous (flushTimeoutSeconds == 0)
	// Insert blocks until this is on disk.
	survivorIdentity := externalSegIdentity(t, "survives-the-cut")
	require.NoError(t, si.Insert(index.Documents{{EntityValues: survivorIdentity, Timestamp: 1}}))

	abandonedIdentity := externalSegIdentity(t, "abandoned-mid-receive")
	seg := buildNativeExternalSegment(t, abandonedIdentity, "value")

	streamer, err := si.EnableExternalSegments()
	require.NoError(t, err)
	require.NoError(t, streamer.StartSegment())
	require.NoError(t, streamer.WriteChunk(seg))
	// No CompleteSegment, no Close: the receive and the owner are both
	// abandoned here, exactly as a crash would abandon them.

	indexPath := filepath.Join(dir, seriesIndexDirName)
	staged, globErr := filepath.Glob(filepath.Join(indexPath, ".native-external-*"))
	require.NoError(t, globErr)
	require.Len(t, staged, 1, "the abandoned receive must leave its staged file on disk, exactly as a crash would")
	_, statErr := os.Stat(staged[0])
	require.NoError(t, statErr, "the staged file must still exist")

	si2, err := newSeriesIndex(ctx, dir, 0, 0, nil, &testRootLease{})
	require.NoError(t, err)
	defer func() { require.NoError(t, si2.Close()) }()

	generation, err := native.OpenReadOnlyGeneration(indexPath)
	require.NoError(t, err, "a directory abandoned mid-receive must still reopen as a complete generation")
	defer func() { _ = generation.Close() }()
	count, err := generation.VisibleDocCount()
	require.NoError(t, err)
	require.Equal(t, int64(1), count, "only the data durable before the cut must be visible")

	var survivor, abandoned pbv1.Series
	require.NoError(t, survivor.Unmarshal(survivorIdentity))
	require.NoError(t, abandoned.Unmarshal(abandonedIdentity))
	sd, filterErr := si2.filter(ctx, []*pbv1.Series{&survivor})
	require.NoError(t, filterErr)
	require.Len(t, sd.SeriesList, 1, "data durable before the cut must survive")

	sd, filterErr = si2.filter(ctx, []*pbv1.Series{&abandoned})
	require.NoError(t, filterErr)
	require.Empty(t, sd.SeriesList, "an incomplete external receive must not become visible after reopen")
}
