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
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/convert"
)

const (
	// publicationCountLimit bounds how long the contract waits for a published
	// generation to become visible to a separate process. Each reading spawns a
	// process, so the budget is generous relative to the writer's own
	// millisecond-scale publication latency.
	publicationCountLimit = 30 * time.Second

	// publicationSettledReadings is how many counts must agree on the published
	// answer before the run is accepted, so a single lucky reading cannot stand
	// in for stable visibility.
	publicationSettledReadings = 3
)

// TestE2EReadOnlyCountDuringPublication walks the production situation this
// milestone exists for, end to end. A segment's series index is owned by a live
// compatibility writer that keeps committing, and something else --
// storage.SeriesIndexStats reporting a closed segment's document count -- has to
// count its documents from another process without disturbing it and without
// ever seeing a half-published generation.
//
// The index is built through BanyanDB's compatibility writer boundary, the same
// path that produced existing series indexes, so this is also the
// old-bytes-still-read case: nothing here writes index bytes. Documents use the
// numeric identifiers issue #14009 names -- doc-21 through doc-25 committed
// across two segments, doc-22 deleted, doc-26 published later.
//
// Requirements proved here:
//
//	R3 -- while a retained compatibility writer publishes, every count returns
//	      4 or 5 and nothing else. No count observes a partial generation,
//	      fails because the writer is open, or creates an index-local runtime
//	      file, and 5 eventually becomes and stays the answer.
//	R1 -- a manifest still being streamed into place is not the newest
//	      structurally complete generation, so the generation behind it is
//	      counted instead of being reported as damage.
//	R2 -- the deletion of doc-22 is honored on bytes the writer produced in
//	      this run, not only on the checked-in corpus.
func TestE2EReadOnlyCountDuringPublication(t *testing.T) {
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

	tester.NoError(writer.Batch(nidx01bBatch(nidx01bFirstBatch)))
	tester.NoError(writer.Batch(nidx01bBatch(nidx01bSecondBatch)))
	tester.NoError(writer.Delete([][]byte{convert.Uint64ToBytes(nidx01bDeletedDocID)}))

	// R2: five documents were committed and doc-22 was deleted, so four are
	// visible. The baseline is taken once the writer has finished its own
	// post-commit housekeeping, so what follows measures the read-only call
	// alone.
	beforeCommitted := waitForCompatibilityWriterQuiescence(t, dir)
	committed := countInChildProcess(t, dir)
	tester.True(committed.Succeeded, "want a count, got %q", committed.Err)
	tester.Equal(nidx01bVisibleCount, committed.Count)
	tester.Equal(beforeCommitted, dirInventory(t, dir),
		"counting a directory a writer owns must not add, remove, or rewrite a single entry")

	// R1 + R3, deterministically: the writer streams every manifest into its
	// final name in place, with no atomic rename, so a reader that arrives
	// mid-publication sees a newest manifest that is a prefix of the one being
	// written. That state must count the generation behind it, not report
	// damage, and it must never expose an intermediate answer.
	inFlight := copyIndexDir(t, dir)
	writeHalfPublishedGeneration(t, inFlight)
	partial := countInChildProcess(t, inFlight)
	tester.True(partial.Succeeded, "a manifest still being written is publication in flight, not damage; got %q", partial.Err)
	tester.Equal(nidx01bVisibleCount, partial.Count)

	// R3: publish doc-26 with the writer still open, and keep counting from
	// separate processes across the publication and the writer's own
	// housekeeping behind it.
	observations := []int64{committed.Count}
	tester.NoError(writer.Batch(nidx01bBatch([]uint64{nidx01bPublishedDocID})))
	deadline := time.Now().Add(publicationCountLimit)
	settled := 0
	for settled < publicationSettledReadings {
		observed := countInChildProcess(t, dir)
		tester.True(observed.Succeeded,
			"a count taken while the writer publishes must not fail; got %q", observed.Err)
		observations = append(observations, observed.Count)
		if observed.Count == nidx01bRestoredCount {
			settled++
		} else {
			settled = 0
		}
		if settled < publicationSettledReadings && !time.Now().Before(deadline) {
			t.Fatalf("the published document never became visible within %s; observed %v",
				publicationCountLimit, observations)
		}
	}

	// Four is the pre-publication answer and five the post-publication one.
	// Anything else is an intermediate the reader was never allowed to expose.
	for _, observed := range observations {
		tester.Contains([]int64{nidx01bVisibleCount, nidx01bRestoredCount}, observed,
			"observed counts must be a subset of {4,5}; saw %v", observations)
	}
	tester.Contains(observations, nidx01bRestoredCount)
	assertNoReaderRuntimeFiles(t, dir)

	// R3: the writer hands the directory over, and the same bytes still count 5.
	tester.NoError(writer.Close())
	writerClosed = true

	beforeClosed := dirInventory(t, dir)
	closed := countInChildProcess(t, dir)
	tester.True(closed.Succeeded, "want a count, got %q", closed.Err)
	tester.Equal(nidx01bRestoredCount, closed.Count)
	tester.Equal(beforeClosed, dirInventory(t, dir),
		"counting a closed directory must not disturb it either")
}

// writeHalfPublishedGeneration adds a manifest one generation newer than any
// the directory holds, containing a prefix of the newest committed manifest.
// That is byte for byte what a concurrent reader observes while the writer
// streams a manifest into its final name.
func writeHalfPublishedGeneration(t *testing.T, dir string) {
	t.Helper()
	manifests, globErr := filepath.Glob(filepath.Join(dir, "*"+snpExt))
	require.NoError(t, globErr)
	require.NotEmpty(t, manifests, "index directory %s holds no committed manifest", dir)
	slices.Sort(manifests)
	newest := manifests[len(manifests)-1]

	committed, readErr := os.ReadFile(newest)
	require.NoError(t, readErr)
	prefix := committed[:len(committed)/2]
	_, parseErr := parseCompatibilityManifest(prefix)
	require.Error(t, parseErr, "a half-written manifest must not decode as a committed generation")

	newestID, valid := generationIDOf(filepath.Base(newest))
	require.True(t, valid, "cannot read the generation identifier of %s", newest)
	require.NoError(t, os.WriteFile(filepath.Join(dir, generationFileName(newestID+1)), prefix, 0o600))
}

// generationIDOf reads the generation identifier out of a manifest file name.
func generationIDOf(name string) (uint64, bool) {
	id, err := strconv.ParseUint(strings.TrimSuffix(name, snpExt), 16, 64)
	return id, err == nil
}
