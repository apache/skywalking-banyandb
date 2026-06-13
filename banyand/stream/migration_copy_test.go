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
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/banyand/internal/migration"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/fs"
)

// TestAnalyzeGroupDiffWithTarget_PartialLossDetected verifies the
// len(tgtPaths) < len(srcPaths) branch in AnalyzeGroupDiffWithTarget.
// The source holds 3 rows: two identical-key rows (same entity → same
// seriesID, same Timestamp, same ElementID=99) and one distinct row
// (ElementID=88). The target holds only 2 rows: one copy of the
// duplicated key and the distinct row — simulating partial loss during
// migration. The diff must report SourceRows==3, TargetRows==2,
// MissingRows==1, MissingKeys==1, ExtraRows==0, and the single
// Missing entry must carry ElementID==99.
func TestAnalyzeGroupDiffWithTarget_PartialLossDetected(t *testing.T) {
	tmpDir := t.TempDir()

	const (
		group  = "sw_test"
		shardN = "shard-0"
	)

	fileSystem := fs.NewLocalFileSystem()
	noFsync := migration.NoFsyncFS{FileSystem: fileSystem}
	ir := storage.IntervalRule{Unit: storage.DAY, Num: 1}
	segStart := ir.Standard(time.Date(2026, 1, 10, 0, 0, 0, 0, time.UTC))
	segName := formatStreamDirectCopySegName(segStart, storage.DAY)

	// Source: group root → seg → shard → part.
	srcGroupRoot := filepath.Join(tmpDir, "source", group)
	srcShardDir := filepath.Join(srcGroupRoot, segName, shardN)
	require.NoError(t, os.MkdirAll(srcShardDir, storage.DirPerm))

	ts0 := segStart.Add(1 * time.Hour).UnixNano()
	ts1 := segStart.Add(2 * time.Hour).UnixNano()

	// Three rows: rows[0] and rows[1] share the same entity+ts+elementID
	// so they produce the same streamAnalyzeKey after flush.
	// rows[2] is distinct (different ts and elementID).
	srcRows := []DumpRow{
		{
			Entity: "entity-dup", Timestamp: ts0, ElementID: 99,
			Tags: []DumpTag{{Family: "default", Name: "svc", Value: "dup"}},
		},
		{
			Entity: "entity-dup", Timestamp: ts0, ElementID: 99,
			Tags: []DumpTag{{Family: "default", Name: "svc", Value: "dup"}},
		},
		{
			Entity: "entity-distinct", Timestamp: ts1, ElementID: 88,
			Tags: []DumpTag{{Family: "default", Name: "svc", Value: "other"}},
		},
	}
	_, _, srcCleanup := BuildPartForDump(srcShardDir, noFsync, uint64(1), srcRows)
	defer srcCleanup()

	// Target: only 2 rows — one copy of the duplicated key + the distinct row.
	tgtGroupRoot := filepath.Join(tmpDir, "target", group)
	tgtShardDir := filepath.Join(tgtGroupRoot, segName, shardN)
	require.NoError(t, os.MkdirAll(tgtShardDir, storage.DirPerm))

	tgtRows := []DumpRow{
		{
			Entity: "entity-dup", Timestamp: ts0, ElementID: 99,
			Tags: []DumpTag{{Family: "default", Name: "svc", Value: "dup"}},
		},
		{
			Entity: "entity-distinct", Timestamp: ts1, ElementID: 88,
			Tags: []DumpTag{{Family: "default", Name: "svc", Value: "other"}},
		},
	}
	_, _, tgtCleanup := BuildPartForDump(tgtShardDir, noFsync, uint64(1), tgtRows)
	defer tgtCleanup()

	diff, err := AnalyzeGroupDiffWithTarget([]string{srcGroupRoot}, tgtGroupRoot, fileSystem, 10)
	require.NoError(t, err)

	require.EqualValues(t, 3, diff.SourceRows, "source must report 3 rows")
	require.EqualValues(t, 2, diff.TargetRows, "target must report 2 rows")
	require.EqualValues(t, 1, diff.MissingRows, "one row must be detected missing")
	require.Equal(t, 1, diff.MissingKeys, "one key must be flagged")
	require.EqualValues(t, 0, diff.ExtraRows, "no extra rows expected")
	require.Equal(t, 0, diff.ExtraKeys, "no extra keys expected")
	require.Len(t, diff.Missing, 1, "exactly one Missing sample expected")
	require.EqualValues(t, 99, diff.Missing[0].ElementID, "missing entry must carry ElementID 99")
}

// TestSumGroupSourceRows_NoSnpFile verifies that SumGroupSourceRows counts
// part rows by pattern-scanning partID directories without requiring any
// .snp file in the shard directory.
func TestSumGroupSourceRows_NoSnpFile(t *testing.T) {
	tmpDir := t.TempDir()

	const (
		group  = "sw_test_nosnapfile"
		shardN = "shard-0"
	)

	fileSystem := fs.NewLocalFileSystem()
	noFsync := migration.NoFsyncFS{FileSystem: fileSystem}
	ir := storage.IntervalRule{Unit: storage.DAY, Num: 1}
	segStart := ir.Standard(time.Date(2026, 3, 5, 0, 0, 0, 0, time.UTC))
	segName := formatStreamDirectCopySegName(segStart, storage.DAY)

	srcGroupRoot := filepath.Join(tmpDir, "source", group)
	srcShardDir := filepath.Join(srcGroupRoot, segName, shardN)
	require.NoError(t, os.MkdirAll(srcShardDir, storage.DirPerm))

	ts0 := segStart.Add(1 * time.Hour).UnixNano()
	ts1 := segStart.Add(2 * time.Hour).UnixNano()
	ts2 := segStart.Add(3 * time.Hour).UnixNano()
	rows := []DumpRow{
		{
			Entity: "entity-A", Timestamp: ts0, ElementID: 1,
			Tags: []DumpTag{{Family: "default", Name: "svc", Value: "alpha"}},
		},
		{
			Entity: "entity-B", Timestamp: ts1, ElementID: 2,
			Tags: []DumpTag{{Family: "default", Name: "svc", Value: "beta"}},
		},
		{
			Entity: "entity-C", Timestamp: ts2, ElementID: 3,
			Tags: []DumpTag{{Family: "default", Name: "svc", Value: "gamma"}},
		},
	}
	_, _, cleanup := BuildPartForDump(srcShardDir, noFsync, uint64(1), rows)
	defer cleanup()

	// Confirm no .snp file exists.
	entries, err := os.ReadDir(srcShardDir)
	require.NoError(t, err)
	for _, e := range entries {
		require.False(t, strings.HasSuffix(e.Name(), directStreamCopySnpSuffix),
			"test precondition: no .snp file should exist under %s", srcShardDir)
	}

	rowCount, partCount, sumErr := SumGroupSourceRows([]string{srcGroupRoot}, fileSystem)
	require.NoError(t, sumErr)
	require.EqualValues(t, 3, rowCount, "must count all 3 rows without a .snp file")
	require.Equal(t, 1, partCount, "must count 1 part without a .snp file")
}

// TestMigrationFastPath builds a minimal source tree (one group, one segment,
// one shard, three elements) using the internal helpers, then exercises the
// fast-path copy (all rows align to one target segment) and asserts:
//   - srcRows == tgtRows (stream invariant: no dedup)
//   - target shard has exactly one .snp file
//   - target segment has a valid metadata file with endTime
//
// Note: the test does NOT build a real schema-property catalog or a real bluge
// element index (idx/) — creating those requires the full write-path with
// index rules. What's covered:
//   - part flush via BuildPartForDump + memPart (fast-path data copy)
//   - writeStreamDirectCopySnp / writeStreamDirectCopySegmentMetadata
//   - VerifyShardParts row count assertion
//   - streamSegStateRegistry registration + snapshot
//
// What's NOT covered (stubbed/unverified):
//   - Bluge element index rebuild on slow path (requires index rules + entity resolution)
//   - Series index union (requires real bluge sidx in source)
//   - End-to-end migration copy (requires full schema-property catalog)
func TestMigrationFastPath(t *testing.T) {
	tmpDir := t.TempDir()

	const (
		group  = "sw_segment"
		shardN = "shard-0"
	)

	ir := storage.IntervalRule{Unit: storage.DAY, Num: 1}
	segStart := ir.Standard(time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC))
	segName := formatStreamDirectCopySegName(segStart, storage.DAY)

	// Source layout: <tmpDir>/source/<group>/<segName>/<shardN>/<partID>/
	sourceGroupRoot := filepath.Join(tmpDir, "source", group)
	shardDir := filepath.Join(sourceGroupRoot, segName, shardN)
	require.NoError(t, os.MkdirAll(shardDir, storage.DirPerm))

	fileSystem := fs.NewLocalFileSystem()
	noFsync := migration.NoFsyncFS{FileSystem: fileSystem}

	// Write three elements into a single part under the shard dir.
	ts0 := segStart.Add(1 * time.Hour).UnixNano()
	ts1 := segStart.Add(2 * time.Hour).UnixNano()
	ts2 := segStart.Add(3 * time.Hour).UnixNano()
	rows := []DumpRow{
		{
			Entity: "entity-A", Timestamp: ts0, ElementID: 100,
			Tags: []DumpTag{{Family: "default", Name: "svc", Value: "alpha"}},
		},
		{
			Entity: "entity-A", Timestamp: ts1, ElementID: 101,
			Tags: []DumpTag{{Family: "default", Name: "svc", Value: "alpha"}},
		},
		{
			Entity: "entity-B", Timestamp: ts2, ElementID: 200,
			Tags: []DumpTag{{Family: "default", Name: "svc", Value: "beta"}},
		},
	}
	const srcPartID = uint64(1)
	_, rows, cleanup := BuildPartForDump(shardDir, noFsync, srcPartID, rows)
	defer cleanup()
	srcRows := uint64(len(rows))

	// Write .snp for the source shard (so VerifyShardParts works on src too).
	_, err := writeStreamDirectCopySnp(shardDir, []string{fmt.Sprintf("%016x", srcPartID)})
	require.NoError(t, err)

	// Write source segment metadata.
	_, err = writeStreamDirectCopySegmentMetadata(
		filepath.Join(sourceGroupRoot, segName),
		ir.NextTime(segStart),
	)
	require.NoError(t, err)

	// Verify source row count before copy.
	srcVerifiedRows, srcParts, srcVerErr := VerifyShardParts(shardDir, fileSystem)
	require.NoError(t, srcVerErr)
	require.EqualValues(t, srcRows, srcVerifiedRows, "source rows must match written count")
	require.Equal(t, 1, srcParts)

	// Read source part metadata to confirm single-segment alignment (fast path).
	partIDStr := fmt.Sprintf("%016x", srcPartID)
	partDir := filepath.Join(shardDir, partIDStr)
	var srcMeta partMetadata
	srcMeta.mustReadMetadata(noFsync, partDir)
	require.EqualValues(t, 3, srcMeta.TotalCount)

	alignedMin := ir.Standard(time.Unix(0, srcMeta.MinTimestamp))
	alignedMax := ir.Standard(time.Unix(0, srcMeta.MaxTimestamp))
	require.Equal(t, alignedMin, alignedMax,
		"all timestamps land in the same segment — fast path must apply")

	// Target layout.
	dstRoot := filepath.Join(tmpDir, "target", group)
	require.NoError(t, os.MkdirAll(dstRoot, storage.DirPerm))

	var partIDGen atomic.Uint64
	flushCh := make(chan streamFlushJob, 2)
	close(flushCh) // fast path never sends to flushCh
	segReg := newStreamSegStateRegistry()

	alignedSegName := formatStreamDirectCopySegName(alignedMin, storage.DAY)
	pr, copyErr := fastCopyOneStreamPart(
		streamProcessPartInput{
			ir:           ir,
			fileSystem:   noFsync,
			shardName:    shardN,
			shardDir:     shardDir,
			partIDStr:    partIDStr,
			dstGroupRoot: dstRoot,
			segStates:    segReg,
			partIDGen:    &partIDGen,
			flushCh:      flushCh,
		},
		partDir,
		alignedSegName,
		alignedMin,
		srcMeta.TotalCount,
	)
	require.NoError(t, copyErr)
	require.EqualValues(t, srcRows, pr.rows, "fast-path must preserve all rows")
	require.Equal(t, 1, pr.targetParts)

	// Finalize: write metadata + snp for every aligned segment in the registry.
	snap := segReg.snapshot()
	require.Len(t, snap, 1, "exactly one aligned target segment expected")

	for alignedSeg, ss := range snap {
		segDir := filepath.Join(dstRoot, alignedSeg)
		endTime := ir.NextTime(ss.alignedTime)

		_, err := writeStreamDirectCopySegmentMetadata(segDir, endTime)
		require.NoError(t, err)

		for shName, pids := range ss.shards {
			names := make([]string, len(pids))
			for i, pid := range pids {
				names[i] = fmt.Sprintf("%016x", pid)
			}
			_, snpErr := writeStreamDirectCopySnp(filepath.Join(segDir, shName), names)
			require.NoError(t, snpErr)
		}

		// Assert metadata file content.
		metaPath := filepath.Join(segDir, storage.SegmentMetadataFilename)
		require.FileExists(t, metaPath)
		raw, readErr := os.ReadFile(metaPath)
		require.NoError(t, readErr)
		var segMeta storage.SegmentMetadata
		require.NoError(t, json.Unmarshal(raw, &segMeta))
		require.NotEmpty(t, segMeta.EndTime, "segment metadata must carry endTime")

		// Assert exactly one .snp under each shard.
		for shName := range ss.shards {
			shardTarget := filepath.Join(segDir, shName)
			entries, rdErr := os.ReadDir(shardTarget)
			require.NoError(t, rdErr)
			snpCount := 0
			for _, e := range entries {
				if !e.IsDir() && strings.HasSuffix(e.Name(), directStreamCopySnpSuffix) {
					snpCount++
				}
			}
			require.Equal(t, 1, snpCount, "target shard must have exactly one .snp file")
		}

		// Assert target row count == source row count (stream no-dedup invariant).
		for shName := range ss.shards {
			shardTarget := filepath.Join(segDir, shName)
			tgtRows, tgtParts, vErr := VerifyShardParts(shardTarget, fileSystem)
			require.NoError(t, vErr)
			require.EqualValues(t, srcRows, tgtRows,
				"stream invariant violated: target rows must equal source rows")
			require.Equal(t, 1, tgtParts, "expected one target part")
		}
	}
}
