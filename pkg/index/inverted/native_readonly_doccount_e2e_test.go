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
	"path/filepath"
	"testing"

	"github.com/blugelabs/bluge"
	"github.com/stretchr/testify/require"
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
	beforeLive := dirInventory(t, dir)
	tester.FileExists(filepath.Join(dir, LockFilename))
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
	tester.NoFileExists(filepath.Join(dir, LockFilename), "closing the writer releases the lock")

	beforeClosed := dirInventory(t, dir)
	closed := countInChildProcess(t, dir)
	tester.True(closed.Succeeded, "want a count, got %q", closed.Err)
	tester.Equal(nidx01aVisibleCount, closed.Count)
	tester.Equal(beforeClosed, dirInventory(t, dir),
		"counting a closed directory must not disturb it either")
}
