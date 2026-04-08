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
	"encoding/json"
	"path/filepath"
	"runtime"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/bytes"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

func TestSnapshotGetParts(t *testing.T) {
	tests := []struct {
		snapshot *snapshot
		name     string
		dst      []*part
		expected []*part
		opts     queryOptions
		count    int
	}{
		{
			name: "Test with empty snapshot",
			snapshot: &snapshot{
				parts: []*partWrapper{},
			},
			dst: []*part{},
			opts: queryOptions{
				minTimestamp: 0,
				maxTimestamp: 10,
			},
			expected: []*part{},
			count:    0,
		},
		{
			name: "Test with non-empty snapshot and no matching parts",
			snapshot: &snapshot{
				parts: []*partWrapper{
					{
						p: &part{partMetadata: partMetadata{
							MinTimestamp: 0,
							MaxTimestamp: 5,
						}},
					},
					{
						p: &part{partMetadata: partMetadata{
							MinTimestamp: 6,
							MaxTimestamp: 10,
						}},
					},
				},
			},
			dst: []*part{},
			opts: queryOptions{
				minTimestamp: 11,
				maxTimestamp: 15,
			},
			expected: []*part{},
			count:    0,
		},
		{
			name: "Test with non-empty snapshot and some matching parts",
			snapshot: &snapshot{
				parts: []*partWrapper{
					{
						p: &part{partMetadata: partMetadata{
							MinTimestamp: 0,
							MaxTimestamp: 5,
						}},
					},
					{
						p: &part{partMetadata: partMetadata{
							MinTimestamp: 6,
							MaxTimestamp: 10,
						}},
					},
				},
			},
			dst: []*part{},
			opts: queryOptions{
				minTimestamp: 5,
				maxTimestamp: 10,
			},
			expected: []*part{
				{partMetadata: partMetadata{
					MinTimestamp: 0,
					MaxTimestamp: 5,
				}},
				{partMetadata: partMetadata{
					MinTimestamp: 6,
					MaxTimestamp: 10,
				}},
			},
			count: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, count := tt.snapshot.getParts(tt.dst, nil, tt.opts.minTimestamp, tt.opts.maxTimestamp)
			assert.Equal(t, tt.expected, result)
			assert.Equal(t, tt.count, count)
		})
	}
}

func TestSnapshotCopyAllTo(t *testing.T) {
	tests := []struct {
		name      string
		snapshot  snapshot
		expected  snapshot
		nextEpoch uint64
		closePrev bool
	}{
		{
			name: "Test with empty snapshot",
			snapshot: snapshot{
				parts: []*partWrapper{},
			},
			nextEpoch: 1,
			expected: snapshot{
				epoch: 1,
				ref:   1,
				parts: nil,
			},
		},
		{
			name: "Test with non-empty snapshot",
			snapshot: snapshot{
				parts: []*partWrapper{
					{ref: 1},
					{ref: 2},
				},
			},
			nextEpoch: 1,
			expected: snapshot{
				epoch: 1,
				ref:   1,
				parts: []*partWrapper{
					{ref: 2},
					{ref: 3},
				},
			},
		},
		{
			name: "Test with closed previous snapshot",
			snapshot: snapshot{
				parts: []*partWrapper{
					{ref: 1},
					{ref: 2},
				},
			},
			nextEpoch: 1,
			closePrev: true,
			expected: snapshot{
				epoch: 1,
				ref:   1,
				parts: []*partWrapper{
					{ref: 1},
					{ref: 2},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.snapshot.copyAllTo(tt.nextEpoch)
			if tt.closePrev {
				tt.snapshot.decRef()
			}
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSnapshotMerge(t *testing.T) {
	tests := []struct {
		snapshot  *snapshot
		nextParts map[uint64]*partWrapper
		name      string
		expected  snapshot
		nextEpoch uint64
		closePrev bool
	}{
		{
			name: "Test with empty snapshot and empty next parts",
			snapshot: &snapshot{
				parts: []*partWrapper{},
			},
			nextEpoch: 1,
			nextParts: map[uint64]*partWrapper{},
			expected: snapshot{
				epoch: 1,
				ref:   1,
				parts: nil,
			},
		},
		{
			name: "Test with non-empty snapshot and empty next parts",
			snapshot: &snapshot{
				parts: []*partWrapper{
					{p: &part{partMetadata: partMetadata{ID: 1}}, ref: 1},
					{p: &part{partMetadata: partMetadata{ID: 2}}, ref: 2},
				},
			},
			nextEpoch: 1,
			nextParts: map[uint64]*partWrapper{},
			expected: snapshot{
				epoch: 1,
				ref:   1,
				parts: []*partWrapper{
					{p: &part{partMetadata: partMetadata{ID: 1}}, ref: 2},
					{p: &part{partMetadata: partMetadata{ID: 2}}, ref: 3},
				},
			},
		},
		{
			name: "Test with non-empty snapshot and non-empty next parts",
			snapshot: &snapshot{
				parts: []*partWrapper{
					{p: &part{partMetadata: partMetadata{ID: 1}}, ref: 1},
					{p: &part{partMetadata: partMetadata{ID: 2}}, ref: 2},
				},
			},
			nextEpoch: 1,
			nextParts: map[uint64]*partWrapper{
				2: {p: &part{partMetadata: partMetadata{ID: 2}}, ref: 1},
				3: {p: &part{partMetadata: partMetadata{ID: 3}}, ref: 1},
			},
			expected: snapshot{
				epoch: 1,
				ref:   1,
				parts: []*partWrapper{
					{p: &part{partMetadata: partMetadata{ID: 1}}, ref: 2},
					{p: &part{partMetadata: partMetadata{ID: 2}}, ref: 1},
				},
			},
		},
		{
			name: "Test with closed previous snapshot",
			snapshot: &snapshot{
				parts: []*partWrapper{
					{p: &part{partMetadata: partMetadata{ID: 1}}, ref: 1},
					{p: &part{partMetadata: partMetadata{ID: 2}}, ref: 2},
				},
			},
			closePrev: true,
			nextEpoch: 1,
			nextParts: map[uint64]*partWrapper{
				2: {p: &part{partMetadata: partMetadata{ID: 2}}, ref: 1},
				3: {p: &part{partMetadata: partMetadata{ID: 3}}, ref: 1},
			},
			expected: snapshot{
				epoch: 1,
				ref:   1,
				parts: []*partWrapper{
					{p: &part{partMetadata: partMetadata{ID: 1}}, ref: 1},
					{p: &part{partMetadata: partMetadata{ID: 2}}, ref: 1},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.snapshot.merge(tt.nextEpoch, tt.nextParts)
			if tt.closePrev {
				tt.snapshot.decRef()
			}
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSnapshotRemove(t *testing.T) {
	tests := []struct {
		snapshot    *snapshot
		mergedParts map[uint64]struct{}
		name        string
		expected    snapshot
		nextEpoch   uint64
		closePrev   bool
	}{
		{
			name: "Test with empty snapshot and no parts to remove",
			snapshot: &snapshot{
				parts: []*partWrapper{},
			},
			nextEpoch:   1,
			mergedParts: map[uint64]struct{}{},
			expected: snapshot{
				epoch: 1,
				ref:   1,
				parts: nil,
			},
		},
		{
			name: "Test with non-empty snapshot and no parts to remove",
			snapshot: &snapshot{
				parts: []*partWrapper{
					{p: &part{partMetadata: partMetadata{ID: 1}}, ref: 1},
					{p: &part{partMetadata: partMetadata{ID: 2}}, ref: 2},
				},
			},
			nextEpoch:   1,
			mergedParts: map[uint64]struct{}{},
			expected: snapshot{
				epoch: 1,
				ref:   1,
				parts: []*partWrapper{
					{p: &part{partMetadata: partMetadata{ID: 1}}, ref: 2},
					{p: &part{partMetadata: partMetadata{ID: 2}}, ref: 3},
				},
			},
		},
		{
			name: "Test with non-empty snapshot and some parts to remove",
			snapshot: &snapshot{
				parts: []*partWrapper{
					{p: &part{partMetadata: partMetadata{ID: 1}}, ref: 1},
					{p: &part{partMetadata: partMetadata{ID: 2}}, ref: 2},
				},
			},
			nextEpoch: 1,
			mergedParts: map[uint64]struct{}{
				1: {},
			},
			expected: snapshot{
				epoch: 1,
				ref:   1,
				parts: []*partWrapper{
					{p: &part{partMetadata: partMetadata{ID: 2}}, ref: 3},
				},
			},
		},
		{
			name: "Test with empty snapshot, no parts to remove, and closePrev=true",
			snapshot: &snapshot{
				parts: []*partWrapper{},
			},
			nextEpoch:   1,
			mergedParts: map[uint64]struct{}{},
			expected: snapshot{
				epoch: 1,
				ref:   1,
				parts: nil,
			},
			closePrev: true,
		},
		{
			name: "Test with non-empty snapshot, no parts to remove, and closePrev=true",
			snapshot: &snapshot{
				parts: []*partWrapper{
					{p: &part{partMetadata: partMetadata{ID: 1}}, ref: 1},
					{p: &part{partMetadata: partMetadata{ID: 2}}, ref: 2},
				},
			},
			nextEpoch:   1,
			mergedParts: map[uint64]struct{}{},
			expected: snapshot{
				epoch: 1,
				ref:   1,
				parts: []*partWrapper{
					{p: &part{partMetadata: partMetadata{ID: 1}}, ref: 1},
					{p: &part{partMetadata: partMetadata{ID: 2}}, ref: 2},
				},
			},
			closePrev: true,
		},
		{
			name: "Test with non-empty snapshot, some parts to remove, and closePrev=true",
			snapshot: &snapshot{
				parts: []*partWrapper{
					{p: &part{
						partMetadata: partMetadata{ID: 1},
						timestamps:   &bytes.Buffer{},
						primary:      &bytes.Buffer{},
						fieldValues:  &bytes.Buffer{},
					}, ref: 1},
					{p: &part{partMetadata: partMetadata{ID: 2}}, ref: 2},
				},
			},
			nextEpoch: 1,
			mergedParts: map[uint64]struct{}{
				1: {},
			},
			expected: snapshot{
				epoch: 1,
				ref:   1,
				parts: []*partWrapper{
					{p: &part{partMetadata: partMetadata{ID: 2}}, ref: 2},
				},
			},
			closePrev: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.snapshot.remove(tt.nextEpoch, tt.mergedParts)
			if tt.closePrev {
				tt.snapshot.decRef()
			}
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSnapshotFunctionality(t *testing.T) {
	fileSystem := fs.NewLocalFileSystem()

	tmpPath, deferFn := test.Space(require.New(t))
	defer deferFn()

	tabDir := filepath.Join(tmpPath, "tab")
	fileSystem.MkdirPanicIfExist(tabDir, 0o755)

	tst, err := newTSTable(
		fileSystem,
		tabDir,
		common.Position{},
		logger.GetLogger("test"),
		timestamp.TimeRange{},
		option{
			flushTimeout: 0,
			mergePolicy:  newDefaultMergePolicyForTesting(),
			protector:    protector.Nop{},
		},
		nil,
	)
	if err != nil {
		t.Fatalf("failed to create newTSTable: %v", err)
	}
	defer tst.Close()

	tst.mustAddDataPoints(dpsTS1)
	tst.mustAddDataPoints(dpsTS2)
	time.Sleep(100 * time.Millisecond) // allow time for flushing

	require.Eventually(t, func() bool {
		dd := fileSystem.ReadDir(tabDir)
		partNum := 0
		for _, d := range dd {
			if d.IsDir() {
				partNum++
			}
		}
		return partNum >= 1
	}, flags.EventuallyTimeout, time.Millisecond, "wait for file parts to be created")

	snapshotPath := filepath.Join(tmpPath, "snapshot")
	fileSystem.MkdirIfNotExist(snapshotPath, 0o755)

	if _, err := tst.TakeFileSnapshot(snapshotPath); err != nil {
		t.Fatalf("TakeFileSnapshot failed: %v", err)
	}

	entries := fileSystem.ReadDir(snapshotPath)

	if len(entries) < 1 {
		t.Fatalf("expected 1 directory for flushed part, got %d", len(entries))
	}
	partDir := entries[0].Name()

	partFiles := fileSystem.ReadDir(filepath.Join(snapshotPath, partDir))

	// Check "primary.bin" and "meta.bin" existence
	hasPrimary := false
	hasMeta := false
	for _, pf := range partFiles {
		switch pf.Name() {
		case "primary.bin":
			hasPrimary = true
		case "meta.bin":
			hasMeta = true
		}
	}
	if !hasPrimary {
		t.Error("expected primary.bin in snapshot, but none found")
	}
	if !hasMeta {
		t.Error("expected meta.bin in snapshot, but none found")
	}

	// Verify hard links (Unix-only)
	if runtime.GOOS != "windows" {
		srcFile := filepath.Join(tabDir, partDir, "primary.bin")
		destFile := filepath.Join(snapshotPath, partDir, "primary.bin")

		err := fs.CompareINode(srcFile, destFile)
		if err != nil {
			t.Fatalf("expected hard linked files to share inode: %v", err)
		}
	}
}

func testSnapshotOption() option {
	return option{
		flushTimeout: 0,
		mergePolicy:  newDefaultMergePolicyForTesting(),
		protector:    protector.Nop{},
	}
}

func TestMustWriteSnapshotUsesTempThenRename(t *testing.T) {
	fileSystem := fs.NewLocalFileSystem()
	tmpPath, deferFn := test.Space(require.New(t))
	defer deferFn()
	tabDir := filepath.Join(tmpPath, "tab")
	fileSystem.MkdirPanicIfExist(tabDir, 0o755)
	tst := &tsTable{fileSystem: fileSystem, root: tabDir}
	const epoch = uint64(0x10)
	partNames := []string{partName(0x1)}
	tst.mustWriteSnapshot(epoch, partNames)
	snpPath := filepath.Join(tabDir, snapshotName(epoch))
	tmpPathFile := snpPath + ".tmp"
	require.False(t, fileSystem.IsExist(tmpPathFile), "temp file must be removed after rename")
	require.True(t, fileSystem.IsExist(snpPath), "final snapshot file must exist")
	data, err := fileSystem.Read(snpPath)
	require.NoError(t, err)
	var decoded []string
	require.NoError(t, json.Unmarshal(data, &decoded))
	require.Equal(t, partNames, decoded)
}

func TestMustWriteSnapshotWithMultipleParts(t *testing.T) {
	fileSystem := fs.NewLocalFileSystem()
	tmpPath, deferFn := test.Space(require.New(t))
	defer deferFn()
	tabDir := filepath.Join(tmpPath, "tab")
	fileSystem.MkdirPanicIfExist(tabDir, 0o755)
	tst := &tsTable{fileSystem: fileSystem, root: tabDir}
	partNames := []string{partName(0x1), partName(0x2), partName(0xff)}
	tst.mustWriteSnapshot(1, partNames)
	parts, readErr := tst.readSnapshot(1)
	require.NoError(t, readErr)
	require.Equal(t, []uint64{0x1, 0x2, 0xff}, parts)
}

func TestMustWriteSnapshotEmptyPartList(t *testing.T) {
	fileSystem := fs.NewLocalFileSystem()
	tmpPath, deferFn := test.Space(require.New(t))
	defer deferFn()
	tabDir := filepath.Join(tmpPath, "tab")
	fileSystem.MkdirPanicIfExist(tabDir, 0o755)
	tst := &tsTable{fileSystem: fileSystem, root: tabDir}
	tst.mustWriteSnapshot(1, []string{})
	parts, readErr := tst.readSnapshot(1)
	require.NoError(t, readErr)
	require.Empty(t, parts)
}

func TestTolerantLoaderFallbackToOlderSnapshot(t *testing.T) {
	fileSystem := fs.NewLocalFileSystem()
	tmpPath, deferFn := test.Space(require.New(t))
	defer deferFn()
	tabDir := filepath.Join(tmpPath, "tab")
	fileSystem.MkdirPanicIfExist(tabDir, 0o755)
	tst, err := newTSTable(fileSystem, tabDir, common.Position{}, logger.GetLogger("test"),
		timestamp.TimeRange{}, option{flushTimeout: 0, mergePolicy: newDefaultMergePolicyForTesting(), protector: protector.Nop{}}, nil)
	require.NoError(t, err)
	tst.mustAddDataPoints(dpsTS1)
	time.Sleep(100 * time.Millisecond)
	require.Eventually(t, func() bool {
		dd := fileSystem.ReadDir(tabDir)
		for _, d := range dd {
			if d.IsDir() && d.Name() != storage.FailedPartsDirName {
				return true
			}
		}
		return false
	}, flags.EventuallyTimeout, time.Millisecond, "wait for part")
	tst.Close()
	snapshots := make([]uint64, 0)
	for _, e := range fileSystem.ReadDir(tabDir) {
		if filepath.Ext(e.Name()) == snapshotSuffix {
			parsed, parseErr := parseSnapshot(e.Name())
			if parseErr == nil {
				snapshots = append(snapshots, parsed)
			}
		}
	}
	require.GreaterOrEqual(t, len(snapshots), 1)
	sort.Slice(snapshots, func(i, j int) bool { return snapshots[i] > snapshots[j] })
	validEpoch := snapshots[0]
	corruptEpoch := validEpoch + 1
	corruptPath := filepath.Join(tabDir, snapshotName(corruptEpoch))
	_, writeErr := fileSystem.Write([]byte{}, corruptPath, 0o600)
	require.NoError(t, writeErr)
	tst2, epoch2 := initTSTable(fileSystem, tabDir, common.Position{}, logger.GetLogger("test"), testSnapshotOption(), nil)
	require.NotNil(t, tst2)
	require.Equal(t, validEpoch, epoch2, "should load older valid snapshot when newest is corrupt")
	require.NotNil(t, tst2.snapshot)
	require.Equal(t, validEpoch, tst2.snapshot.epoch)
	require.False(t, fileSystem.IsExist(corruptPath), "corrupt newer snapshot must be deleted after fallback")
}

func TestMeasureInitTSTableDeletesMultipleFailedSnapshotsOnFallback(t *testing.T) {
	fileSystem := fs.NewLocalFileSystem()
	tmpPath, deferFn := test.Space(require.New(t))
	defer deferFn()
	tabDir := filepath.Join(tmpPath, "tab")
	fileSystem.MkdirPanicIfExist(tabDir, 0o755)
	tst, err := newTSTable(fileSystem, tabDir, common.Position{}, logger.GetLogger("test"),
		timestamp.TimeRange{}, testSnapshotOption(), nil)
	require.NoError(t, err)
	tst.mustAddDataPoints(dpsTS1)
	time.Sleep(100 * time.Millisecond)
	require.Eventually(t, func() bool {
		dd := fileSystem.ReadDir(tabDir)
		for _, d := range dd {
			if d.IsDir() && d.Name() != storage.FailedPartsDirName {
				return true
			}
		}
		return false
	}, flags.EventuallyTimeout, time.Millisecond, "wait for part")
	tst.Close()
	snapshots := make([]uint64, 0)
	for _, e := range fileSystem.ReadDir(tabDir) {
		if filepath.Ext(e.Name()) == snapshotSuffix {
			parsed, parseErr := parseSnapshot(e.Name())
			if parseErr == nil {
				snapshots = append(snapshots, parsed)
			}
		}
	}
	require.GreaterOrEqual(t, len(snapshots), 1)
	sort.Slice(snapshots, func(i, j int) bool { return snapshots[i] > snapshots[j] })
	validEpoch := snapshots[0]
	// Create two corrupt newer snapshots
	corruptEpoch1 := validEpoch + 1
	corruptEpoch2 := validEpoch + 2
	corruptPath1 := filepath.Join(tabDir, snapshotName(corruptEpoch1))
	corruptPath2 := filepath.Join(tabDir, snapshotName(corruptEpoch2))
	_, writeErr := fileSystem.Write([]byte{}, corruptPath1, 0o600)
	require.NoError(t, writeErr)
	_, writeErr = fileSystem.Write([]byte{}, corruptPath2, 0o600)
	require.NoError(t, writeErr)
	tst2, epoch2 := initTSTable(fileSystem, tabDir, common.Position{}, logger.GetLogger("test"), testSnapshotOption(), nil)
	require.NotNil(t, tst2)
	require.Equal(t, validEpoch, epoch2, "should load older valid snapshot when newer ones are corrupt")
	require.NotNil(t, tst2.snapshot)
	require.Equal(t, validEpoch, tst2.snapshot.epoch)
	require.False(t, fileSystem.IsExist(corruptPath1), "first corrupt snapshot must be deleted after fallback")
	require.False(t, fileSystem.IsExist(corruptPath2), "second corrupt snapshot must be deleted after fallback")
}

func TestReadSnapshotReturnsErrorOnEmptyFile(t *testing.T) {
	fileSystem := fs.NewLocalFileSystem()
	tmpPath, deferFn := test.Space(require.New(t))
	defer deferFn()
	tabDir := filepath.Join(tmpPath, "tab")
	fileSystem.MkdirPanicIfExist(tabDir, 0o755)
	snpPath := filepath.Join(tabDir, snapshotName(1))
	_, writeErr := fileSystem.Write([]byte{}, snpPath, 0o600)
	require.NoError(t, writeErr)
	tst := &tsTable{fileSystem: fileSystem, root: tabDir}
	_, readErr := tst.readSnapshot(1)
	require.Error(t, readErr)
	require.Contains(t, readErr.Error(), "cannot parse")
}

func TestReadSnapshotReturnsErrorOnInvalidJSON(t *testing.T) {
	fileSystem := fs.NewLocalFileSystem()
	tmpPath, deferFn := test.Space(require.New(t))
	defer deferFn()
	tabDir := filepath.Join(tmpPath, "tab")
	fileSystem.MkdirPanicIfExist(tabDir, 0o755)
	snpPath := filepath.Join(tabDir, snapshotName(1))
	_, writeErr := fileSystem.Write([]byte("{invalid}"), snpPath, 0o600)
	require.NoError(t, writeErr)
	tst := &tsTable{fileSystem: fileSystem, root: tabDir}
	_, readErr := tst.readSnapshot(1)
	require.Error(t, readErr)
	require.Contains(t, readErr.Error(), "cannot parse")
}

func TestReadSnapshotReturnsErrorOnInvalidPartName(t *testing.T) {
	fileSystem := fs.NewLocalFileSystem()
	tmpPath, deferFn := test.Space(require.New(t))
	defer deferFn()
	tabDir := filepath.Join(tmpPath, "tab")
	fileSystem.MkdirPanicIfExist(tabDir, 0o755)
	partNamesData, marshalErr := json.Marshal([]string{"not-hex"})
	require.NoError(t, marshalErr)
	snpPath := filepath.Join(tabDir, snapshotName(1))
	_, writeErr := fileSystem.Write(partNamesData, snpPath, 0o600)
	require.NoError(t, writeErr)
	tst := &tsTable{fileSystem: fileSystem, root: tabDir}
	_, readErr := tst.readSnapshot(1)
	require.Error(t, readErr)
}

func TestReadSnapshotSucceedsOnValidFile(t *testing.T) {
	fileSystem := fs.NewLocalFileSystem()
	tmpPath, deferFn := test.Space(require.New(t))
	defer deferFn()
	tabDir := filepath.Join(tmpPath, "tab")
	fileSystem.MkdirPanicIfExist(tabDir, 0o755)
	tst := &tsTable{fileSystem: fileSystem, root: tabDir}
	tst.mustWriteSnapshot(1, []string{partName(0xa), partName(0xb)})
	parts, readErr := tst.readSnapshot(1)
	require.NoError(t, readErr)
	require.Equal(t, []uint64{0xa, 0xb}, parts)
}

func TestMustReadSnapshotPanicsOnError(t *testing.T) {
	fileSystem := fs.NewLocalFileSystem()
	tmpPath, deferFn := test.Space(require.New(t))
	defer deferFn()
	tabDir := filepath.Join(tmpPath, "tab")
	fileSystem.MkdirPanicIfExist(tabDir, 0o755)
	snpPath := filepath.Join(tabDir, snapshotName(1))
	_, writeErr := fileSystem.Write([]byte{}, snpPath, 0o600)
	require.NoError(t, writeErr)
	tst := &tsTable{fileSystem: fileSystem, root: tabDir}
	require.Panics(t, func() { _ = tst.mustReadSnapshot(1) })
}

func TestInitTSTableEmptyDirectory(t *testing.T) {
	fileSystem := fs.NewLocalFileSystem()
	tmpPath, deferFn := test.Space(require.New(t))
	defer deferFn()
	tabDir := filepath.Join(tmpPath, "tab")
	fileSystem.MkdirPanicIfExist(tabDir, 0o755)
	tst, epoch := initTSTable(fileSystem, tabDir, common.Position{}, logger.GetLogger("test"), testSnapshotOption(), nil)
	require.NotNil(t, tst)
	require.Greater(t, epoch, uint64(0))
	require.Nil(t, tst.snapshot)
}

func TestInitTSTableEmptyTableWhenAllSnapshotsCorrupt(t *testing.T) {
	fileSystem := fs.NewLocalFileSystem()
	tmpPath, deferFn := test.Space(require.New(t))
	defer deferFn()
	tabDir := filepath.Join(tmpPath, "tab")
	fileSystem.MkdirPanicIfExist(tabDir, 0o755)
	snpPath := filepath.Join(tabDir, snapshotName(1))
	_, writeErr := fileSystem.Write([]byte{}, snpPath, 0o600)
	require.NoError(t, writeErr)
	tst, epoch := initTSTable(fileSystem, tabDir, common.Position{}, logger.GetLogger("test"), testSnapshotOption(), nil)
	require.NotNil(t, tst)
	require.Greater(t, epoch, uint64(0))
	require.Nil(t, tst.snapshot)
	require.False(t, fileSystem.IsExist(snpPath), "corrupt snapshot file must be deleted")
}

func TestInitTSTableLoadsNewestWhenMultipleValid(t *testing.T) {
	fileSystem := fs.NewLocalFileSystem()
	tmpPath, deferFn := test.Space(require.New(t))
	defer deferFn()
	tabDir := filepath.Join(tmpPath, "tab")
	fileSystem.MkdirPanicIfExist(tabDir, 0o755)
	tst, err := newTSTable(fileSystem, tabDir, common.Position{}, logger.GetLogger("test"),
		timestamp.TimeRange{}, testSnapshotOption(), nil)
	require.NoError(t, err)
	tst.mustAddDataPoints(dpsTS1)
	time.Sleep(100 * time.Millisecond)
	require.Eventually(t, func() bool {
		dd := fileSystem.ReadDir(tabDir)
		for _, d := range dd {
			if d.IsDir() && d.Name() != storage.FailedPartsDirName {
				return true
			}
		}
		return false
	}, flags.EventuallyTimeout, time.Millisecond, "wait for part")
	tst.Close()
	snapshots := make([]uint64, 0)
	for _, e := range fileSystem.ReadDir(tabDir) {
		if filepath.Ext(e.Name()) == snapshotSuffix {
			parsed, parseErr := parseSnapshot(e.Name())
			if parseErr == nil {
				snapshots = append(snapshots, parsed)
			}
		}
	}
	require.GreaterOrEqual(t, len(snapshots), 1)
	tst2, epoch2 := initTSTable(fileSystem, tabDir, common.Position{}, logger.GetLogger("test"), testSnapshotOption(), nil)
	require.NotNil(t, tst2)
	sort.Slice(snapshots, func(i, j int) bool { return snapshots[i] > snapshots[j] })
	require.Equal(t, snapshots[0], epoch2, "must load newest valid snapshot")
}

func TestTakeFileSnapshotEmptySegment(t *testing.T) {
	fileSystem := fs.NewLocalFileSystem()

	tmpPath, deferFn := test.Space(require.New(t))
	defer deferFn()

	tabDir := filepath.Join(tmpPath, "tab")
	fileSystem.MkdirPanicIfExist(tabDir, 0o755)

	tst, err := newTSTable(
		fileSystem,
		tabDir,
		common.Position{},
		logger.GetLogger("test"),
		timestamp.TimeRange{},
		option{
			flushTimeout: 0,
			mergePolicy:  newDefaultMergePolicy(),
			protector:    protector.Nop{},
		},
		nil,
	)
	require.NoError(t, err)
	defer tst.Close()

	tst.Lock()
	tst.snapshot = nil
	tst.Unlock()

	snapshotPath := filepath.Join(tmpPath, "snapshot")
	fileSystem.MkdirIfNotExist(snapshotPath, 0o755)

	created, err := tst.TakeFileSnapshot(snapshotPath)
	require.ErrorIs(t, err, storage.ErrNoCurrentSnapshot)
	assert.False(t, created)

	entries := fileSystem.ReadDir(snapshotPath)
	assert.Empty(t, entries, "no files or dirs should remain when snapshot is nil")
}
