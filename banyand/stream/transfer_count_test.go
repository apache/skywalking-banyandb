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
	"errors"
	"fmt"
	iofs "io/fs"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	exportsnapshot "github.com/apache/skywalking-banyandb/banyand/transfer/export/snapshot"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
)

// inventoryRows counts the rows of a shard directory the way the export inventory (statShard in
// banyand/transfer/export) does, in one pass: the parts the newest snapshot file names
// (LivePartIDs), each read through ParsePartMetadata, the reader the stream service hands the
// inventory (ReadPartMetadata), its TotalCount summed; a part gone since the listing
// (fs.ErrNotExist) is skipped. The listing is not repeated, so a merge that persists a snapshot
// during the pass shows as a different count, as it does in the inventory.
func inventoryRows(t *testing.T, fileSystem fs.FileSystem, shardDir string) uint64 {
	t.Helper()
	ids, err := exportsnapshot.LivePartIDs(shardDir)
	require.NoError(t, err)
	var rows uint64
	for _, id := range ids {
		pm, readErr := ParsePartMetadata(fileSystem, filepath.Join(shardDir, fmt.Sprintf("%016x", id)))
		if errors.Is(readErr, iofs.ErrNotExist) {
			continue
		}
		require.NoError(t, readErr)
		rows += pm.TotalCount
	}
	return rows
}

// livePartCount is how many parts the newest snapshot file of shardDir names.
func livePartCount(t *testing.T, shardDir string) int {
	t.Helper()
	ids, err := exportsnapshot.LivePartIDs(shardDir)
	require.NoError(t, err)
	return len(ids)
}

// TestFilePartIsCountedWithoutFlush checks a premise of the import's count reconciliation: a
// part introduced by the sync receive path (mustAddFilePart) is counted by the export
// inventory as soon as it is introduced, with no flush.
func TestFilePartIsCountedWithoutFlush(t *testing.T) {
	require.NoError(t, logger.Init(logger.Logging{Env: "dev", Level: flags.LogLevel}))
	fileSystem := fs.NewLocalFileSystem()
	tmpPath, deferFn := test.Space(require.New(t))
	defer deferFn()
	tabDir := filepath.Join(tmpPath, "tab")
	fileSystem.MkdirPanicIfExist(tabDir, 0o755)
	opt := streamSnapshotOption()
	opt.flushTimeout = 1 << 62 // no flush during the test
	tst, _, err := initTSTable(fileSystem, tabDir, common.Position{}, logger.GetLogger("test"), opt, nil, false)
	require.NoError(t, err)
	tst.startLoop(1)
	defer func() { require.NoError(t, tst.Close()) }()

	require.Zero(t, inventoryRows(t, fileSystem, tabDir))
	var want uint64
	for i, es := range []*elements{makeTestElements(1_000_000_000), esTS1} {
		id := uint64(i + 1)
		buildAndFlushStreamMemPart(t, fileSystem, es, partPath(tabDir, id))
		tst.mustAddFilePart(id)
		want += uint64(es.Len())
		require.Equal(t, want, inventoryRows(t, fileSystem, tabDir), "part %d is counted once mustAddFilePart returns", id)
	}
}

// TestMergeIntroductionKeepsInventoryRows checks the same premise end to end: once the merge loop
// of a running table merges introduced file parts and persists the snapshot that names the merged
// part, the inventory still counts every row, and at no point in between does it count another
// number.
func TestMergeIntroductionKeepsInventoryRows(t *testing.T) {
	require.NoError(t, logger.Init(logger.Logging{Env: "dev", Level: flags.LogLevel}))
	fileSystem := fs.NewLocalFileSystem()
	tmpPath, deferFn := test.Space(require.New(t))
	defer deferFn()
	tabDir := filepath.Join(tmpPath, "tab")
	fileSystem.MkdirPanicIfExist(tabDir, 0o755)
	tst, _, err := initTSTable(fileSystem, tabDir, common.Position{}, logger.GetLogger("test"), streamSnapshotOption(), nil, false)
	require.NoError(t, err)
	tst.startLoop(1)
	defer func() { require.NoError(t, tst.Close()) }()

	var want uint64
	const parts = 4
	for i, es := range []*elements{esTS1, esTS2, makeTestElements(1_000_000_000), esTS1} {
		// Far above the ids the table gives its merged parts.
		id := uint64(1000 + i)
		buildAndFlushStreamMemPart(t, fileSystem, es, partPath(tabDir, id))
		tst.mustAddFilePart(id)
		want += uint64(es.Len())
	}
	deadline := time.Now().Add(30 * time.Second)
	for livePartCount(t, tabDir) >= parts {
		require.Equal(t, want, inventoryRows(t, fileSystem, tabDir))
		require.True(t, time.Now().Before(deadline), "the merge loop merged none of the %d file parts", parts)
		time.Sleep(50 * time.Millisecond)
	}
	require.Equal(t, want, inventoryRows(t, fileSystem, tabDir), "the snapshot naming the merged part counts every row")
}

// TestMergeKeepsTotalCount checks a premise of the import's count reconciliation: merging
// stream parts keeps the sum of their total_count, duplicates included.
func TestMergeKeepsTotalCount(t *testing.T) {
	fileSystem := fs.NewLocalFileSystem()
	tmpPath, deferFn := test.Space(require.New(t))
	defer deferFn()
	var parts []*partWrapper
	defer func() {
		for _, pw := range parts {
			pw.decRef()
		}
	}()
	var want uint64
	for i, es := range []*elements{esTS1, esTS1, esTS2, generateHugeElements(1, 3000, 1)} {
		buildAndFlushStreamMemPart(t, fileSystem, es, partPath(tmpPath, uint64(i)))
		pw := newPartWrapper(nil, mustOpenFilePart(uint64(i), tmpPath, fileSystem))
		pw.p.partMetadata.ID = uint64(i)
		parts = append(parts, pw)
		want += pw.p.partMetadata.TotalCount
	}
	closeCh := make(chan struct{})
	defer close(closeCh)
	tst := &tsTable{pm: protector.Nop{}}
	const mergedID = 100
	merged, err := tst.mergeParts(fileSystem, closeCh, parts, mergedID, tmpPath)
	require.NoError(t, err)
	defer merged.decRef()
	require.Equal(t, want, merged.p.partMetadata.TotalCount)
	var onDisk partMetadata
	onDisk.mustReadMetadata(fileSystem, partPath(tmpPath, mergedID))
	require.Equal(t, want, onDisk.TotalCount, "the merged part's metadata.json, which the inventory reads, keeps every row")
}
