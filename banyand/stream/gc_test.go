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
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

func TestGarbageCleanerCleanSnapshot(t *testing.T) {
	tests := []struct {
		name            string
		snapshotsOnDisk []uint64
		snapshotsInGC   []uint64
		wantOnDisk      []uint64
		wantInGC        []uint64
	}{
		{
			name: "Test with no snapshots on disk and no snapshots in GC",
		},
		{
			name:            "Test with some snapshots on disk and no snapshots in GC",
			snapshotsInGC:   nil,
			snapshotsOnDisk: []uint64{1, 2, 3},
			wantInGC:        nil,
			wantOnDisk:      []uint64{1, 2, 3},
		},
		{
			name:            "Test with no snapshots on disk and some snapshots in GC",
			snapshotsOnDisk: nil,
			snapshotsInGC:   []uint64{1, 2, 3},
			wantOnDisk:      nil,
			// gc won't fix the miss match between inmemory and disk
			wantInGC: []uint64{3},
		},
		{
			name:            "Test with some snapshots on disk and some snapshots in GC",
			snapshotsOnDisk: []uint64{1, 2, 3, 4, 5},
			snapshotsInGC:   []uint64{1, 3, 5},
			wantOnDisk:      []uint64{2, 4, 5},
			wantInGC:        []uint64{5},
		},
		{
			name:            "Test with unsorted",
			snapshotsOnDisk: []uint64{1, 3, 2, 5, 4},
			snapshotsInGC:   []uint64{5, 1, 3},
			wantOnDisk:      []uint64{2, 4, 5},
			wantInGC:        []uint64{5},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root, defFn := test.Space(require.New(t))
			defer defFn()
			parent := &tsTable{
				root:       root,
				l:          logger.GetLogger("test"),
				fileSystem: fs.NewLocalFileSystem(),
			}
			for i := range tt.snapshotsOnDisk {
				filePath := filepath.Join(parent.root, snapshotName(tt.snapshotsOnDisk[i]))
				file, err := parent.fileSystem.CreateFile(filePath, filePermission)
				require.NoError(t, err)
				require.NoError(t, file.Close())
			}
			g := &garbageCleaner{
				parent:    parent,
				snapshots: tt.snapshotsInGC,
			}
			g.cleanSnapshots()
			var got []uint64
			for _, de := range parent.fileSystem.ReadDir(parent.root) {
				s, err := parseSnapshot(de.Name())
				require.NoError(t, err, "failed to parse snapshot name:%s", de.Name())
				got = append(got, s)
			}
			sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
			assert.Equal(t, tt.wantOnDisk, got)
			assert.Equal(t, tt.wantInGC, g.snapshots)
		})
	}
}