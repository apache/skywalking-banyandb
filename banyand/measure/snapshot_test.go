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
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
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

	if err := tst.TakeFileSnapshot(snapshotPath); err != nil {
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
