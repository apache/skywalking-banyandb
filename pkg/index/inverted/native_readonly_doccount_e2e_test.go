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

package inverted

import (
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Committing a batch leaves the compatibility writer holding a superseded snapshot
// manifest, which it removes on its own schedule shortly afterwards -- measured
// at 44us to 2.7ms after the commit returns, across twenty runs. That removal
// is the writer's own housekeeping. A count measured across it would be
// credited with a change no reader made, so the immutability assertion waits
// for the directory to hold still first. The wait spans far more than the
// observed housekeeping latency; if it ever proved too short, the assertion it
// guards fails loudly rather than passing wrongly.
const (
	compatibilityWriterQuiesceReadings = 15
	compatibilityWriterQuiesceInterval = 10 * time.Millisecond
	compatibilityWriterQuiesceLimit    = 5 * time.Second
)

// TestE2EReadOnlyDocCount walks the production situation this milestone exists
// for. A segment's series index is owned by a live writer, and something else
// -- storage.SeriesIndexStats
// reporting a segment's document count -- has to count its documents without
// disturbing it. Here the writer stays open in this process while every count
// happens in another, which is the arrangement the guarantee is written for.
//
// The index is built through BanyanDB's compatibility writer boundary, the same
// path that produced existing series indexes, so this is also the
// old-bytes-still-read case: nothing in the milestone's native reader writes
// index bytes. Documents use the numeric IDs 11 and 12 named in issue #14008.
//
// Requirements proved here:
//
//	R5 -- an index directory a writer has opened but not yet flushed reports no
//	      committed generation and counts zero; callers may treat that as
//	      empty, and it is not classified as corruption.
//	R2 -- once document IDs 11 and 12 are committed, the count is 2 while the
//	      compatibility writer remains open, and it is still 2 after that writer
//	      closes and hands the same bytes over.
//	R1 -- no count changes any path, byte, size, hash, mtime, or directory
//	      entry.
//	R3 -- a damaged copy of those same live bytes reports the typed corruption
//	      sentinel rather than an untyped failure or a plausible wrong count.
func TestE2EReadOnlyDocCount(t *testing.T) {
	tester := require.New(t)
	dir := t.TempDir()

	writer, err := NewStore(StoreOpts{Path: dir})
	tester.NoError(err)
	writerClosed := false
	defer func() {
		if !writerClosed {
			_ = writer.Close()
		}
	}()

	// R5: the writer owns the directory and has committed nothing yet.
	beforeUnflushed := dirInventory(t, dir)
	unflushed := countInChildProcess(t, dir)
	tester.Zero(unflushed.Count, "an unflushed index counts as empty")
	tester.True(unflushed.NoCommitted, "want the absent-generation sentinel, got %q", unflushed.Err)
	tester.False(unflushed.Corrupt, "an index that was never flushed is not damaged")
	tester.Equal(beforeUnflushed, dirInventory(t, dir),
		"counting an unflushed directory must not add, remove, or rewrite an entry")

	tester.NoError(writer.Batch(nidx01aBatch()))

	// R2 + R1: count the committed generation from another process while this
	// one remains open, and prove the directory came through untouched.
	// The baseline is taken once the writer has finished its own post-commit
	// housekeeping, so that what follows measures the read-only call alone.
	beforeLive := waitForCompatibilityWriterQuiescence(t, dir)
	live := countInChildProcess(t, dir)
	tester.True(live.Succeeded, "want a count, got %q", live.Err)
	// 2 is the number of documents inserted above and the count declared for
	// this corpus by issue #14008.
	tester.Equal(nidx01aVisibleCount, live.Count)
	tester.Equal(beforeLive, dirInventory(t, dir),
		"counting a directory a writer owns must not add, remove, or rewrite a single entry")

	// R3: the same live bytes, damaged in a copy, are rejected as corruption.
	damaged := copyIndexDir(t, dir)
	setFieldsIndexOffset(t, newestSegmentFile(t, damaged), oversizeSectionOffset)
	rejected := countInChildProcess(t, damaged)
	tester.True(rejected.Corrupt, "want the corruption sentinel, got %q", rejected.Err)
	tester.Zero(rejected.Count)

	// R2: the writer hands the directory over, and the same bytes still count 2.
	tester.NoError(writer.Close())
	writerClosed = true

	beforeClosed := dirInventory(t, dir)
	closed := countInChildProcess(t, dir)
	tester.True(closed.Succeeded, "want a count, got %q", closed.Err)
	tester.Equal(nidx01aVisibleCount, closed.Count)
	tester.Equal(beforeClosed, dirInventory(t, dir),
		"counting a closed directory must not disturb it either")
}

// waitForCompatibilityWriterQuiescence blocks until the index directory at dir stops
// changing under the writer that owns it, and returns the inventory it settled
// on. Settling means compatibilityWriterQuiesceReadings consecutive inventories
// agree; a directory that never holds still fails the test rather than letting
// an immutability assertion be measured across a moving target.
func waitForCompatibilityWriterQuiescence(t *testing.T, dir string) []string {
	t.Helper()
	deadline := time.Now().Add(compatibilityWriterQuiesceLimit)
	settled := dirInventory(t, dir)
	agreed := 1
	for time.Now().Before(deadline) {
		time.Sleep(compatibilityWriterQuiesceInterval)
		current := dirInventory(t, dir)
		if !slices.Equal(settled, current) {
			settled = current
			agreed = 1
			continue
		}
		agreed++
		if agreed == compatibilityWriterQuiesceReadings {
			return settled
		}
	}
	t.Fatalf("the index directory at %s never stopped changing under its writer, so a read-only "+
		"call cannot be measured against it; last inventory was %v", dir, settled)
	return nil
}
