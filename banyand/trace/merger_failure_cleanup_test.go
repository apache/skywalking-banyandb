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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

// countTopLevelEntries returns the number of top-level directory entries under
// root, so a test can assert that a failed merge left no new part directory.
func countTopLevelEntries(t *testing.T, root string) int {
	t.Helper()
	entries, err := os.ReadDir(root)
	require.NoError(t, err)
	return len(entries)
}

// TestMergePartsCleansOutputOnFailure reproduces the 2026-08-07 showcase outage
// mechanism: mergeParts creates the output part directory before mergeBlocks
// runs. Before Fix A, a corrupt input part left an orphan output directory on
// every failed merge attempt (~430k dirs/shard in production). This asserts
// the merge fails but leaves the shard root exactly as it was.
func TestMergePartsCleansOutputOnFailure(t *testing.T) {
	tmpPath, defFn := test.Space(require.New(t))
	defer defFn()
	fileSystem := fs.NewLocalFileSystem()

	mp1 := generateMemPart()
	mp1.mustInitFromTraces(ts)
	mp1.mustFlush(fileSystem, partPath(tmpPath, 1))
	releaseMemPart(mp1)

	mp2 := generateMemPart()
	mp2.mustInitFromTraces(ts)
	mp2.mustFlush(fileSystem, partPath(tmpPath, 2))
	releaseMemPart(mp2)

	// Corrupt part 1's primary.bin in place (same length, so the reader's
	// mustReadFull still succeeds and only zstd decompression fails), which
	// makes mergeBlocks return a plain error rather than panic.
	primaryPath := filepath.Join(partPath(tmpPath, 1), primaryFilename)
	data, readErr := os.ReadFile(primaryPath)
	require.NoError(t, readErr)
	require.NotEmpty(t, data)
	garbage := make([]byte, len(data))
	for i := range garbage {
		garbage[i] = byte(0xAA ^ i)
	}
	require.NoError(t, os.WriteFile(primaryPath, garbage, 0o600))

	p1 := mustOpenFilePart(1, tmpPath, fileSystem)
	p1.partMetadata.ID = 1
	p2 := mustOpenFilePart(2, tmpPath, fileSystem)
	p2.partMetadata.ID = 2
	defer p1.close()
	defer p2.close()

	tst := &tsTable{pm: protector.Nop{}, fileSystem: fileSystem, root: tmpPath}
	closeCh := make(chan struct{})
	defer close(closeCh)

	beforeCount := countTopLevelEntries(t, tmpPath)

	for attempt, partID := range []uint64{99, 100, 101} {
		parts := []*partWrapper{newPartWrapper(nil, p1), newPartWrapper(nil, p2)}
		_, _, mergeErr := tst.mergeParts(fileSystem, closeCh, parts, partID, tmpPath, nil, nil)
		require.Errorf(t, mergeErr, "attempt %d: expected merge to fail on corrupt primary.bin", attempt)

		afterCount := countTopLevelEntries(t, tmpPath)
		require.Equalf(t, beforeCount, afterCount,
			"attempt %d: shard root entry count changed (leaked output dir for partID %d)", attempt, partID)
	}
}
