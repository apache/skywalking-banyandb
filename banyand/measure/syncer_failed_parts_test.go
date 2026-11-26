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
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
)

type fakeSyncClient struct {
	syncFn func(call int32) (*queue.SyncResult, error)
	calls  *atomic.Int32
}

func (c *fakeSyncClient) SyncStreamingParts(_ context.Context, parts []queue.StreamingPartData) (*queue.SyncResult, error) {
	// Drain the readers like a real client would.
	for i := range parts {
		for _, f := range parts[i].Files {
			_, _ = io.Copy(io.Discard, f.Reader)
		}
	}
	return c.syncFn(c.calls.Add(1))
}

func (c *fakeSyncClient) Close() error { return nil }

type fakeTier2Client struct {
	queue.Client
	syncFn func(call int32) (*queue.SyncResult, error)
	calls  atomic.Int32
}

func (c *fakeTier2Client) NewChunkedSyncClient(string, uint32) (queue.ChunkedSyncClient, error) {
	return &fakeSyncClient{syncFn: c.syncFn, calls: &c.calls}, nil
}

// newSyncTestTable builds a tsTable whose snapshot holds partCount flushed file parts (IDs 1..partCount).
// No background loop runs; the test drives syncSnapshot directly.
func newSyncTestTable(t *testing.T, client queue.Client, partCount int) *tsTable {
	t.Helper()
	require.NoError(t, logger.Init(logger.Logging{Env: "dev", Level: flags.LogLevel}))
	fileSystem := fs.NewLocalFileSystem()
	root := t.TempDir()
	opt := testSnapshotOption()
	opt.tire2Client = client
	opt.syncInterval = time.Hour
	tst, _ := initTSTable(fileSystem, root, common.Position{}, logger.GetLogger("test"), opt, nil)
	tst.getNodes = func() []string { return []string{"data-1"} }
	tst.group = "test-group"
	tst.loopCloser = run.NewCloser(1)
	t.Cleanup(func() { _ = tst.Close() })

	parts := make([]*partWrapper, 0, partCount)
	for partID := uint64(1); partID <= uint64(partCount); partID++ {
		buildAndFlushMeasureMemPart(t, fileSystem, makeTestDataPoints(1_000_000_000), partPath(root, partID))
		p := mustOpenFilePart(partID, root, fileSystem)
		p.partMetadata.ID = partID
		require.Greater(t, p.partMetadata.TotalCount, uint64(0))
		parts = append(parts, newPartWrapper(nil, p))
	}
	snp := &snapshot{epoch: 1, ref: 1, parts: parts}
	tst.snapshot = snp
	tst.persistSnapshot(snp)
	return tst
}

// syncTestTimeout bounds one syncSnapshot call: failed parts are retried one after another, each backing off for 7s,
// so two failing parts take about 14s.
const syncTestTimeout = time.Minute

// runSyncSnapshot runs syncSnapshot and applies the resulting sync introduction like the introducer loop does.
// syncSnapshot returns only after the introduction is applied.
func runSyncSnapshot(t *testing.T, tst *tsTable) error {
	t.Helper()
	// The sync loop syncs the snapshot it took a reference of.
	snp := tst.currentSnapshot()
	require.NotNil(t, snp)
	defer snp.decRef()
	syncCh := make(chan *syncIntroduction)
	go func() {
		select {
		case si := <-syncCh:
			tst.introduceSync(si, snp.epoch+1)
		case <-tst.loopCloser.CloseNotify():
		}
	}()
	result := make(chan error, 1)
	go func() { result <- tst.syncSnapshot(snp, syncCh) }()
	select {
	case err := <-result:
		return err
	case <-time.After(syncTestTimeout):
		t.Fatalf("syncSnapshot did not return within %s", syncTestTimeout)
		return nil
	}
}

const syncFailedPartsMetric = "banyandb_measure_total_sync_failed_parts"

// installLiaisonMetrics gives tst the queue-side metrics backed by a gatherable Prometheus registry.
func installLiaisonMetrics(tst *tsTable) *promRegistry {
	r := newPromRegistry()
	qm, _ := (&queueSupplier{omr: r}).newMetrics(common.Position{Database: "test-group"})
	tst.metrics = qm.(*metrics)
	return r
}

// failedPartNames lists the copies under failed-parts; a missing directory yields none.
func failedPartNames(t *testing.T, tst *tsTable) []string {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(tst.root, storage.FailedPartsDirName))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	require.NoError(t, err)
	var names []string
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	return names
}

func requireSnapshotEmpty(t *testing.T, tst *tsTable) {
	t.Helper()
	cur := tst.currentSnapshot()
	require.NotNil(t, cur)
	defer cur.decRef()
	require.Empty(t, cur.parts, "a synced or permanently failed part must no longer be in the snapshot")
}

// Parts that fail on every attempt are retried, copied to failed-parts, counted once per part however many
// nodes failed them, and still removed from the snapshot.
func TestSyncSnapshot_AllNodesFail_PartsCountedCopiedAndRemoved(t *testing.T) {
	if testing.Short() {
		t.Skip("retry backoff is fixed at 1s+2s+4s per part")
	}
	nodes := []string{"data-1", "data-2", "data-3"}
	const partCount = 2
	client := &fakeTier2Client{syncFn: func(int32) (*queue.SyncResult, error) {
		return nil, errors.New("node unreachable")
	}}
	tst := newSyncTestTable(t, client, partCount)
	tst.getNodes = func() []string { return nodes }
	r := installLiaisonMetrics(tst)

	require.NoError(t, runSyncSnapshot(t, tst))
	// Every node gets one initial attempt and, for each part, DefaultMaxRetries retries.
	require.Equal(t, int32(len(nodes)*(1+partCount*storage.DefaultMaxRetries)), client.calls.Load())
	requireSnapshotEmpty(t, tst)
	var wantNames []string
	for partID := 1; partID <= partCount; partID++ {
		wantNames = append(wantNames, fmt.Sprintf("%016x_%s", partID, PartTypeCore))
	}
	require.Equal(t, wantNames, failedPartNames(t, tst))
	require.Equal(t, map[string]float64{"": partCount}, r.series(t, syncFailedPartsMetric))
}

// A part that fails once and then succeeds is neither counted nor copied to failed-parts.
func TestSyncSnapshot_RetrySucceeds_NotCounted(t *testing.T) {
	if testing.Short() {
		t.Skip("first retry waits 1s")
	}
	client := &fakeTier2Client{syncFn: func(call int32) (*queue.SyncResult, error) {
		if call == 1 {
			return nil, errors.New("transient")
		}
		return &queue.SyncResult{Success: true}, nil
	}}
	tst := newSyncTestTable(t, client, 1)
	r := installLiaisonMetrics(tst)

	require.NoError(t, runSyncSnapshot(t, tst))
	require.Equal(t, int32(2), client.calls.Load())
	require.Equal(t, map[string]float64{"": 0}, r.series(t, syncFailedPartsMetric))
	require.Empty(t, failedPartNames(t, tst))
	requireSnapshotEmpty(t, tst)
}

// A sync that returns errClosed because the table is closing does not count its part, and the part is not removed
// from the snapshot.
func TestSyncSnapshot_ClosedDuringRetry_NotCounted(t *testing.T) {
	var tst *tsTable
	client := &fakeTier2Client{syncFn: func(int32) (*queue.SyncResult, error) {
		// Close the loops like tsTable.Close does, then fail the call: the retry that follows sees a canceled context.
		go tst.loopCloser.CloseThenWait()
		<-tst.loopCloser.CloseNotify()
		return nil, errors.New("table closed")
	}}
	tst = newSyncTestTable(t, client, 1)
	r := installLiaisonMetrics(tst)

	require.ErrorIs(t, runSyncSnapshot(t, tst), errClosed)
	require.Equal(t, map[string]float64{"": 0}, r.series(t, syncFailedPartsMetric))
	cur := tst.currentSnapshot()
	require.NotNil(t, cur)
	defer cur.decRef()
	require.Len(t, cur.parts, 1, "the part is not removed from the snapshot")
}
