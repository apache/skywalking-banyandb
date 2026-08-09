// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package trace

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

// corruptPrimaryBin overwrites partID's primary.bin with same-length garbage so the
// reader's mustReadFull still succeeds and only zstd decompression fails, producing a
// plain read-side error rather than a panic (mirrors merger_failure_cleanup_test.go).
func corruptPrimaryBin(t *testing.T, root string, partID uint64) {
	t.Helper()
	primaryPath := filepath.Join(partPath(root, partID), primaryFilename)
	data, readErr := os.ReadFile(primaryPath)
	require.NoError(t, readErr)
	require.NotEmpty(t, data)
	garbage := make([]byte, len(data))
	for i := range garbage {
		garbage[i] = byte(0xAA ^ i)
	}
	require.NoError(t, os.WriteFile(primaryPath, garbage, 0o600))
}

// flushFilePart flushes tsData into a fresh, unopened file part at partID so a test can
// corrupt its files on disk before the part is ever opened.
func flushFilePart(t *testing.T, fileSystem fs.FileSystem, root string, partID uint64, tsData *traces) {
	t.Helper()
	mp := generateMemPart()
	mp.mustInitFromTraces(tsData)
	mp.mustFlush(fileSystem, partPath(root, partID))
	releaseMemPart(mp)
}

// mustCreateFilePart flushes tsData into a fresh file part at partID and opens it.
func mustCreateFilePart(t *testing.T, fileSystem fs.FileSystem, root string, partID uint64, tsData *traces) *part {
	t.Helper()
	flushFilePart(t, fileSystem, root, partID, tsData)
	p := mustOpenFilePart(partID, root, fileSystem)
	p.partMetadata.ID = partID
	return p
}

// TestUnreadablePartErrorAttribution reproduces a merge over one corrupt and one healthy
// file part and asserts the resulting error identifies the corrupt part via
// *unreadablePartError, recovered through errors.As across the block_reader/mergeBlocks
// wrapping chain.
func TestUnreadablePartErrorAttribution(t *testing.T) {
	tmpPath, defFn := test.Space(require.New(t))
	defer defFn()
	fileSystem := fs.NewLocalFileSystem()

	const badID, goodID = 1, 2
	flushFilePart(t, fileSystem, tmpPath, badID, ts)
	corruptPrimaryBin(t, tmpPath, badID)
	// Open after corruption: metadata/tag files are untouched, only primary.bin is
	// garbage, so mustOpenFilePart still succeeds and the failure surfaces on read.
	pBad := mustOpenFilePart(badID, tmpPath, fileSystem)
	pBad.partMetadata.ID = badID
	defer pBad.close()

	pGood := mustCreateFilePart(t, fileSystem, tmpPath, goodID, ts)
	defer pGood.close()

	tst := &tsTable{pm: protector.Nop{}, fileSystem: fileSystem, root: tmpPath, l: logger.GetLogger("test")}
	closeCh := make(chan struct{})
	defer close(closeCh)

	parts := []*partWrapper{newPartWrapper(nil, pBad), newPartWrapper(nil, pGood)}
	_, _, mergeErr := tst.mergeParts(fileSystem, closeCh, parts, 100, tmpPath, nil, nil)
	require.Error(t, mergeErr)

	var unreadableErr *unreadablePartError
	require.True(t, errors.As(mergeErr, &unreadableErr), "expected *unreadablePartError in the error chain, got: %v", mergeErr)
	require.Equal(t, uint64(badID), unreadableErr.partID)
	require.Equal(t, pBad.path, unreadableErr.partPath)
}

// TestMergeQuarantinesUnreadablePart simulates the retry loop hitting the same poison
// part three times, then asserts selection excludes it while still surfacing the
// healthy backlog for merging.
func TestMergeQuarantinesUnreadablePart(t *testing.T) {
	tmpPath, defFn := test.Space(require.New(t))
	defer defFn()
	fileSystem := fs.NewLocalFileSystem()

	const badID = uint64(500)
	flushFilePart(t, fileSystem, tmpPath, badID, tsTS1)
	corruptPrimaryBin(t, tmpPath, badID)
	pBad := mustOpenFilePart(badID, tmpPath, fileSystem)
	pBad.partMetadata.ID = badID
	// badPW's initial ref is later transferred to the snapshot built below; decRef is
	// not called here to avoid a double-release once the snapshot drops its ref.
	badPW := newPartWrapper(nil, pBad)

	var healthyPWs []*partWrapper
	for _, partID := range []uint64{501, 502, 503} {
		p := mustCreateFilePart(t, fileSystem, tmpPath, partID, tsTS1)
		healthyPWs = append(healthyPWs, newPartWrapper(nil, p))
	}

	tst := &tsTable{
		pm:         protector.Nop{},
		fileSystem: fileSystem,
		root:       tmpPath,
		l:          logger.GetLogger("test"),
		curPartID:  1000,
		option: option{
			mergePolicy: newDefaultMergePolicyForTesting(),
		},
	}
	closeCh := make(chan struct{})
	defer close(closeCh)

	// Produce the real attribution error once, then simulate three independent retry
	// cycles hitting the same poison part.
	_, _, mergeErr := tst.mergeParts(fileSystem, closeCh, []*partWrapper{badPW, healthyPWs[0]}, 9000, tmpPath, nil, nil)
	require.Error(t, mergeErr)
	var unreadableErr *unreadablePartError
	require.True(t, errors.As(mergeErr, &unreadableErr))
	require.Equal(t, badID, unreadableErr.partID)

	require.False(t, tst.isPartQuarantined(badID), "should not be quarantined before crossing the threshold")
	for attempt := 1; attempt <= quarantineThreshold; attempt++ {
		attributed := tst.recordUnreadablePart(mergeErr)
		require.True(t, attributed, "attempt %d: expected mergeErr to attribute to a part", attempt)
	}
	require.True(t, tst.isPartQuarantined(badID), "part should be quarantined after %d consecutive failures", quarantineThreshold)

	allParts := append([]*partWrapper{badPW}, healthyPWs...)
	snp := &snapshot{parts: allParts, epoch: 1}
	snp.incRef()
	defer snp.decRef()

	dst, toBeMerged := tst.getPartsToMergeUpTo(snp, uint64(1<<30), nil, 0)
	require.GreaterOrEqual(t, len(dst), 2, "healthy backlog should still be selectable")
	for _, pw := range dst {
		require.NotEqual(t, badID, pw.ID(), "quarantined part must be excluded from selection")
	}
	_, badSelected := toBeMerged[badID]
	require.False(t, badSelected, "quarantined part must not appear in toBeMerged")
}

// TestQuarantineSweep asserts sweepQuarantine drops registry entries for parts no
// longer present in the live set (merged away, TTL'd, or deleted).
func TestQuarantineSweep(t *testing.T) {
	tst := &tsTable{l: logger.GetLogger("test")}
	tst.quarantineFails = map[uint64]int{
		42: quarantineThreshold,
		43: quarantineThreshold,
	}
	require.True(t, tst.isPartQuarantined(42))
	require.True(t, tst.isPartQuarantined(43))

	tst.sweepQuarantine(map[uint64]struct{}{43: {}})

	require.False(t, tst.isPartQuarantined(42), "42 should be swept: absent from liveIDs")
	require.True(t, tst.isPartQuarantined(43), "43 should survive the sweep: present in liveIDs")
}
