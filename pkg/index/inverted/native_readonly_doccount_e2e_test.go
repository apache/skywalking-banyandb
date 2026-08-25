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
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/blugelabs/bluge"
	"github.com/stretchr/testify/require"
)

// Committing a batch leaves the legacy writer holding a superseded snapshot
// manifest, which it removes on its own schedule shortly afterwards -- measured
// at 44us to 2.7ms after the commit returns, across twenty runs. That removal
// is the writer's own housekeeping. A count measured across it would be
// credited with a change no reader made, so the immutability assertion waits
// for the directory to hold still first. The wait spans far more than the
// observed housekeeping latency; if it ever proved too short, the assertion it
// guards fails loudly rather than passing wrongly.
const (
	legacyWriterQuiesceReadings = 15
	legacyWriterQuiesceInterval = 10 * time.Millisecond
	legacyWriterQuiesceLimit    = 5 * time.Second
)

// TestE2EReadOnlyDocCount walks the production situation this milestone exists
// for. A segment's series index is owned by a live writer that holds the
// exclusive directory lock, and something else -- storage.SeriesIndexStats
// reporting a segment's document count -- has to count its documents without
// disturbing it. Here the writer stays open in this process while every count
// happens in another, which is the arrangement the guarantee is written for.
//
// The index is built by the pinned legacy writer, the same writer that produced
// every series index already on disk in a deployed cluster, so this is also the
// old-bytes-still-read case: nothing in the milestone writes index bytes, and
// the directory it reads was written by the library being replaced. Documents
// are addressed by the string identities doc-11 and doc-12 named in issue
// #14008, which is why the writer is driven directly rather than through
// store.Batch.
//
// Requirements proved here:
//
//	R5 -- an index directory a writer has opened but not yet flushed reports no
//	      committed generation and counts zero; callers may treat that as
//	      empty, and it is not classified as corruption.
//	R2 -- once doc-11 and doc-12 are committed, the count is 2 while the legacy
//	      writer still owns the lock, and it is still 2 after that writer
//	      closes and hands the same bytes over.
//	R1 -- no count changes any path, byte, size, hash, mtime, or directory
//	      entry, the writer's own lock file included.
//	R3 -- a damaged copy of those same live bytes reports the typed corruption
//	      sentinel rather than an untyped failure or a plausible wrong count.
func TestE2EReadOnlyDocCount(t *testing.T) {
	tester := require.New(t)
	dir := t.TempDir()

	writer, err := bluge.OpenWriter(bluge.DefaultConfig(dir))
	tester.NoError(err)
	writerClosed := false
	defer func() {
		if !writerClosed {
			_ = writer.Close()
		}
	}()

	// R5: the writer owns the directory and has committed nothing yet.
	tester.FileExists(filepath.Join(dir, LockFilename), "the legacy writer must own the directory lock")
	unflushed := countInChildProcess(t, dir)
	tester.Zero(unflushed.Count, "an unflushed index counts as empty")
	tester.True(unflushed.NoCommitted, "want the absent-generation sentinel, got %q", unflushed.Err)
	tester.False(unflushed.Corrupt, "an index that was never flushed is not damaged")

	batch := bluge.NewBatch()
	for _, docID := range nidx01aDocIDs {
		doc := bluge.NewDocument(docID)
		doc.AddField(bluge.NewKeywordField("kind", "nidx01a").StoreValue())
		batch.Insert(doc)
	}
	tester.NoError(writer.Batch(batch))

	// R2 + R1: count the committed generation from another process while this
	// one still holds the lock, and prove the directory came through untouched.
	// The baseline is taken once the writer has finished its own post-commit
	// housekeeping, so that what follows measures the read-only call alone.
	beforeLive := waitForLegacyWriterQuiescence(t, dir)
	tester.FileExists(filepath.Join(dir, LockFilename))
	live := countInChildProcess(t, dir)
	tester.True(live.Succeeded, "want a count, got %q", live.Err)
	// 2 is the number of documents inserted above and the count declared for
	// this corpus by issue #14008.
	tester.Equal(nidx01aVisibleCount, live.Count)
	tester.Equal(beforeLive, dirInventory(t, dir),
		"counting a directory a writer owns must not add, remove, or rewrite a single entry")
	// The comparison above catches an entry a count modified, but not one an
	// earlier count in this test had already created: creating the same stray
	// file again leaves both sides of the delta equal. So also state absolutely
	// that nothing foreign is present. A writer's directory holds its lock
	// file, its segments and its manifests; a read-only call adds no directory
	// entry, so anything else here was put there by a reader that had no
	// business writing at all.
	tester.Empty(foreignDirEntries(t, dir),
		"a read-only count must leave the directory holding only what the writer wrote")

	// R3: the same live bytes, damaged in a copy, are rejected as corruption.
	damaged := copyIndexDir(t, dir)
	setFieldsIndexOffset(t, newestSegmentFile(t, damaged), oversizeSectionOffset)
	rejected := countInChildProcess(t, damaged)
	tester.True(rejected.Corrupt, "want the corruption sentinel, got %q", rejected.Err)
	tester.Zero(rejected.Count)

	// R2: the writer hands the directory over, and the same bytes still count 2.
	tester.NoError(writer.Close())
	writerClosed = true
	tester.NoFileExists(filepath.Join(dir, LockFilename), "closing the writer releases the lock")

	beforeClosed := dirInventory(t, dir)
	closed := countInChildProcess(t, dir)
	tester.True(closed.Succeeded, "want a count, got %q", closed.Err)
	tester.Equal(nidx01aVisibleCount, closed.Count)
	tester.Equal(beforeClosed, dirInventory(t, dir),
		"counting a closed directory must not disturb it either")
}

// foreignDirEntries returns the names in dir that belong to neither the on-disk
// grammar nor the writer that owns the directory: anything that is not a
// segment, a manifest, or the writer's lock file.
func foreignDirEntries(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	foreign := []string{}
	for _, entry := range entries {
		switch ext := filepath.Ext(entry.Name()); {
		case entry.Name() == LockFilename, ext == segExt, ext == snpExt:
		default:
			foreign = append(foreign, entry.Name())
		}
	}
	return foreign
}

// waitForLegacyWriterQuiescence blocks until the index directory at dir stops
// changing under the writer that owns it, and returns the inventory it settled
// on. Settling means legacyWriterQuiesceReadings consecutive inventories agree;
// a directory that never holds still fails the test rather than letting an
// immutability assertion be measured across a moving target.
func waitForLegacyWriterQuiescence(t *testing.T, dir string) []string {
	t.Helper()
	deadline := time.Now().Add(legacyWriterQuiesceLimit)
	settled := dirInventory(t, dir)
	agreed := 1
	for time.Now().Before(deadline) {
		time.Sleep(legacyWriterQuiesceInterval)
		current := dirInventory(t, dir)
		if !slices.Equal(settled, current) {
			settled = current
			agreed = 1
			continue
		}
		agreed++
		if agreed == legacyWriterQuiesceReadings {
			return settled
		}
	}
	t.Fatalf("the index directory at %s never stopped changing under its writer, so a read-only "+
		"call cannot be measured against it; last inventory was %v", dir, settled)
	return nil
}
