// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses this
// file to you under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package trace

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

// corruptSpansBin truncates partID's spans.bin by one byte so the sole block's declared
// size no longer matches the file: seqReader.mustReadFull's io.ReadFull hits a short read
// (io.ErrUnexpectedEOF, not io.EOF) and panics via logger.Panicf("cannot read data: ...")
// instead of surfacing a plain error. meta.bin, primary.bin and metadata.json are
// untouched, so mustOpenFilePart still succeeds; the panic fires only once the merge
// actually reads spans.bin, reproducing the outage's "panic inside the merge path" shape
// without a production test hook.
func corruptSpansBin(t *testing.T, root string, partID uint64) {
	t.Helper()
	spansPath := filepath.Join(partPath(root, partID), spansFilename)
	data, readErr := os.ReadFile(spansPath)
	require.NoError(t, readErr)
	require.Greater(t, len(data), 1, "spans.bin must have data to truncate meaningfully")
	require.NoError(t, os.WriteFile(spansPath, data[:len(data)-1], 0o600))
}

// singleTraceSet builds a minimal traces batch with one trace, so each test part has an
// identity distinct from every other part (avoids incidental traceID overlap across the
// parts merged in this test).
func singleTraceSet(traceID string, ts int64) *traces {
	return &traces{
		traceIDs:   []string{traceID},
		timestamps: []int64{ts},
		spanIDs:    []string{"span-" + traceID},
		spans:      [][]byte{[]byte("payload-" + traceID)},
		tags:       [][]*tagValue{{}},
	}
}

// waitForMergeControlChange blocks until condition() is true, waking only on
// tst.mergeControl's change notifications (never a bare sleep), and fails the test if
// deadline elapses first.
func waitForMergeControlChange(t *testing.T, tst *tsTable, deadline time.Duration, condition func() bool) {
	t.Helper()
	timeout := time.After(deadline)
	for {
		if condition() {
			return
		}
		state := tst.mergeControl.state()
		select {
		case <-state.changed:
		case <-timeout:
			t.Fatalf("timed out after %s waiting for merge control condition", deadline)
		}
	}
}

func consecutiveMergeFailures(tst *tsTable) int {
	tst.mergeControl.mu.Lock()
	defer tst.mergeControl.mu.Unlock()
	return tst.mergeControl.consecutiveFailures
}

// TestMergeWorkerSurvivesPanic reproduces the third defect behind the 2026-08-07 showcase
// outage: before Fix D, a panic inside the merge invocation (there: mkdir under inode
// exhaustion) killed the lane worker goroutine outright -- run.Go recovers the panic at
// the outermost layer and counts it, but the goroutine exits and is never respawned. With
// every fast-lane worker dead, the dispatcher blocks forever writing to the lane channel
// and the in-flight request's parts stay pinned in tst.inFlight.
//
// This drives the real mergeLoop/dispatcherLoop/mergeLaneWorker machinery started by
// newTSTable (via triggerMerge), not a direct mergeParts call, so the assertions exercise
// the worker's recover-to-error wrapping itself rather than just mergeParts' own panic-safe
// cleanup (already covered by TestMergePartsCleansOutputOnFailure).
func TestMergeWorkerSurvivesPanic(t *testing.T) {
	fileSystem := fs.NewLocalFileSystem()
	tmpPath, cleanup := test.Space(require.New(t))
	t.Cleanup(cleanup)

	tableRoot := filepath.Join(tmpPath, "table")
	fileSystem.MkdirPanicIfExist(tableRoot, 0o755)
	tst, tableErr := newTSTable(
		fileSystem,
		tableRoot,
		common.Position{Database: "merge-panic-survival"},
		logger.GetLogger("merge-panic-survival"),
		timestamp.TimeRange{},
		option{
			flushTimeout: 0,
			mergePolicy:  newDefaultMergePolicyForTesting(),
			protector:    protector.Nop{},
		},
		nil,
	)
	require.NoError(t, tableErr)
	t.Cleanup(func() { require.NoError(t, tst.Close()) })

	const badID, goodID = uint64(1), uint64(2)
	tst.observePartID(goodID)
	flushFilePart(t, fileSystem, tableRoot, badID, singleTraceSet("trace-bad", 1))
	corruptSpansBin(t, tableRoot, badID)
	flushFilePart(t, fileSystem, tableRoot, goodID, singleTraceSet("trace-good", 2))

	tst.mustAddFilePart(badID, nil)
	tst.mustAddFilePart(goodID, nil)

	require.NoError(t, tst.triggerMerge())

	// The panicking merge must be recovered into an ordinary failure: recordOutcome(false)
	// increments consecutiveFailures. A regression that lets the panic kill the worker
	// goroutine would never notify mergeControl again, so this wait times out instead of
	// hanging the whole suite.
	waitForMergeControlChange(t, tst, 10*time.Second, func() bool {
		return consecutiveMergeFailures(tst) >= 1
	})

	// The worker epilogue (releaseDispatchRequest) must still run unconditionally after a
	// recovered panic, unpinning both parts from tst.inFlight -- the exact pin leak the
	// outage hit when panics killed the goroutine before the epilogue could execute.
	waitForMergeControlChange(t, tst, 10*time.Second, tst.mergeInFlightEmpty)

	// Quarantine the poison part directly (whitebox, same package) instead of relying on
	// Fix B's attribution: a recovered panic is an *unattributed* failure by design (it
	// never flows through *unreadablePartError), so backoff -- not quarantine -- is its
	// normal safety net. Excluding badID here isolates the second trigger below to the
	// healthy parts, so a successful merge is unambiguous proof the lane worker survived.
	tst.quarantineMu.Lock()
	if tst.quarantineFails == nil {
		tst.quarantineFails = make(map[uint64]int)
	}
	tst.quarantineFails[badID] = quarantineThreshold
	tst.quarantineMu.Unlock()

	const extraID1, extraID2 = uint64(3), uint64(4)
	tst.observePartID(extraID2)
	flushFilePart(t, fileSystem, tableRoot, extraID1, singleTraceSet("trace-extra-1", 3))
	flushFilePart(t, fileSystem, tableRoot, extraID2, singleTraceSet("trace-extra-2", 4))
	tst.mustAddFilePart(extraID1, nil)
	tst.mustAddFilePart(extraID2, nil)

	require.NoError(t, tst.triggerMerge())
	waitCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	require.NoError(t, tst.waitForMergeIdle(waitCtx), "the lane worker must still be alive to serve this second merge")

	snap := tst.currentSnapshot()
	require.NotNil(t, snap)
	defer snap.decRef()

	var sawBadID bool
	mergedCount := 0
	for _, pw := range snap.parts {
		if pw.ID() == badID {
			sawBadID = true
			continue
		}
		mergedCount++
	}
	require.True(t, sawBadID, "the quarantined poison part must remain in the snapshot, unmerged")
	require.Equal(t, 1, mergedCount, "the three healthy parts (goodID + two extras) must have merged into one output part")

	tst.inFlightMu.RLock()
	require.Empty(t, tst.inFlight)
	tst.inFlightMu.RUnlock()

	// tst.metrics is nil for this lightweight table construction (no metrics supplier
	// wired), matching every other whitebox test in this file (e.g.
	// TestMergeQuarantinesUnreadablePart); incTotalMergePanicRecovered's nil guard makes
	// that safe, so the counter is not asserted numerically here. Its wiring (struct
	// field, DeleteAll, both newMetrics constructors) is exercised by the package build.
}
