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

package storage

import (
	"fmt"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

func TestDeleteStaleSnapshotsWithCount(t *testing.T) {
	fileSystem := fs.NewLocalFileSystem()
	tmpPath, deferFn := test.Space(require.New(t))
	defer deferFn()

	snapshotsRoot := filepath.Join(tmpPath, "snapshots")
	fileSystem.MkdirIfNotExist(snapshotsRoot, 0o755)

	snapshotNames := []string{
		"20201010101010-00000001",
		"20201010101010-00000002",
		"20201010101010-00000003",
		"20201010101010-00000004",
	}
	for _, name := range snapshotNames {
		dirPath := filepath.Join(snapshotsRoot, name)
		fileSystem.MkdirIfNotExist(dirPath, 0o755)
	}

	DeleteStaleSnapshots(snapshotsRoot, 2, 0, fileSystem)

	remaining := fileSystem.ReadDir(snapshotsRoot)
	require.Equal(t, 2, len(remaining))

	var names []string
	for _, info := range remaining {
		names = append(names, info.Name())
	}
	sort.Strings(names)
	assert.Equal(t, []string{"20201010101010-00000003", "20201010101010-00000004"}, names)
}

func TestParseSnapshotTimestamp(t *testing.T) {
	tests := []struct {
		expectTime  time.Time
		name        string
		snapshotDir string
		expectErr   bool
	}{
		{
			name:        "valid snapshot name",
			snapshotDir: "20201010101010-00000001",
			expectErr:   false,
			expectTime:  time.Date(2020, 10, 10, 10, 10, 10, 0, time.UTC),
		},
		{
			name:        "another valid snapshot",
			snapshotDir: "20231225153045-12345678",
			expectErr:   false,
			expectTime:  time.Date(2023, 12, 25, 15, 30, 45, 0, time.UTC),
		},
		{
			name:        "snapshot name too short",
			snapshotDir: "2020101010",
			expectErr:   true,
		},
		{
			name:        "invalid timestamp format",
			snapshotDir: "abcd1010101010-00000001",
			expectErr:   true,
		},
		{
			name:        "empty name",
			snapshotDir: "",
			expectErr:   true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsedTime, parseErr := ParseSnapshotTimestamp(tt.snapshotDir)
			if tt.expectErr {
				require.Error(t, parseErr)
			} else {
				require.NoError(t, parseErr)
				assert.Equal(t, tt.expectTime, parsedTime)
			}
		})
	}
}

func TestDeleteStaleSnapshotsWithMinAge(t *testing.T) {
	tests := []struct {
		name                  string
		snapshotAges          []time.Duration
		maxNum                int
		minAge                time.Duration
		expectedRemaining     int
		oldestDeletedAge      time.Duration
		validateOldestDeleted bool
	}{
		{
			name:              "all snapshots within age threshold - no deletion",
			maxNum:            2,
			minAge:            1 * time.Hour,
			snapshotAges:      []time.Duration{-30 * time.Minute, -45 * time.Minute, -50 * time.Minute},
			expectedRemaining: 3,
		},
		{
			name:                  "count exceeded and old snapshots exist - delete old only",
			maxNum:                2,
			minAge:                1 * time.Hour,
			snapshotAges:          []time.Duration{-3 * time.Hour, -2 * time.Hour, -30 * time.Minute},
			expectedRemaining:     2,
			validateOldestDeleted: true,
			oldestDeletedAge:      -3 * time.Hour,
		},
		{
			name:              "count not exceeded - no deletion despite old snapshots",
			maxNum:            5,
			minAge:            1 * time.Hour,
			snapshotAges:      []time.Duration{-3 * time.Hour, -2 * time.Hour, -30 * time.Minute},
			expectedRemaining: 3,
		},
		{
			name:                  "all snapshots old and count exceeded - delete to max",
			maxNum:                2,
			minAge:                1 * time.Hour,
			snapshotAges:          []time.Duration{-5 * time.Hour, -4 * time.Hour, -3 * time.Hour, -2 * time.Hour},
			expectedRemaining:     2,
			validateOldestDeleted: true,
			oldestDeletedAge:      -5 * time.Hour,
		},
		{
			name:              "mixed ages with high max - keep all",
			maxNum:            10,
			minAge:            1 * time.Hour,
			snapshotAges:      []time.Duration{-5 * time.Hour, -2 * time.Hour, -30 * time.Minute, -10 * time.Minute},
			expectedRemaining: 4,
		},
		{
			name:                  "boundary case - snapshot at threshold is deleted",
			maxNum:                1,
			minAge:                1 * time.Hour,
			snapshotAges:          []time.Duration{-1*time.Hour - 1*time.Minute, -59 * time.Minute},
			expectedRemaining:     1,
			validateOldestDeleted: true,
			oldestDeletedAge:      -1*time.Hour - 1*time.Minute,
		},
		{
			name:                  "large number of old snapshots",
			maxNum:                3,
			minAge:                30 * time.Minute,
			snapshotAges:          []time.Duration{-5 * time.Hour, -4 * time.Hour, -3 * time.Hour, -2 * time.Hour, -1 * time.Hour, -45 * time.Minute, -20 * time.Minute},
			expectedRemaining:     3,
			validateOldestDeleted: true,
			oldestDeletedAge:      -5 * time.Hour,
		},
		{
			name:              "partial deletion - some candidates too young",
			maxNum:            2,
			minAge:            2 * time.Hour,
			snapshotAges:      []time.Duration{-5 * time.Hour, -1*time.Hour - 30*time.Minute, -45 * time.Minute, -30 * time.Minute},
			expectedRemaining: 3,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fileSystem := fs.NewLocalFileSystem()
			tmpPath, deferFn := test.Space(require.New(t))
			defer deferFn()
			snapshotsRoot := filepath.Join(tmpPath, "snapshots")
			fileSystem.MkdirIfNotExist(snapshotsRoot, 0o755)
			now := time.Now().UTC()
			createdSnapshots := make(map[string]time.Time)
			for idx, age := range tt.snapshotAges {
				snapshotTime := now.Add(age)
				snapshotName := fmt.Sprintf("%s-%08d", snapshotTime.Format(SnapshotTimeFormat), idx+1)
				dirPath := filepath.Join(snapshotsRoot, snapshotName)
				fileSystem.MkdirIfNotExist(dirPath, 0o755)
				createdSnapshots[snapshotName] = snapshotTime
			}
			DeleteStaleSnapshots(snapshotsRoot, tt.maxNum, tt.minAge, fileSystem)
			remaining := fileSystem.ReadDir(snapshotsRoot)
			require.Equal(t, tt.expectedRemaining, len(remaining), "unexpected number of remaining snapshots")
			if tt.validateOldestDeleted {
				oldestDeletedTime := now.Add(tt.oldestDeletedAge)
				oldestDeletedName := fmt.Sprintf("%s-%08d", oldestDeletedTime.Format(SnapshotTimeFormat), 1)
				found := false
				for _, info := range remaining {
					if info.Name() == oldestDeletedName {
						found = true
						break
					}
				}
				require.False(t, found, "oldest snapshot %s should have been deleted", oldestDeletedName)
			}
		})
	}
}
