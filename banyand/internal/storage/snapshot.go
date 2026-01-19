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
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

const SnapshotTimeFormat = "20060102150405"

// ParseSnapshotTimestamp extracts the creation time from a snapshot directory name.
func ParseSnapshotTimestamp(name string) (time.Time, error) {
	if len(name) < 14 {
		return time.Time{}, fmt.Errorf("snapshot name too short: %s", name)
	}
	timestampStr := name[:14]
	parsedTime, parseErr := time.Parse(SnapshotTimeFormat, timestampStr)
	if parseErr != nil {
		return time.Time{}, fmt.Errorf("failed to parse timestamp from snapshot name %s: %w", name, parseErr)
	}
	return parsedTime, nil
}

// DeleteStaleSnapshots deletes the stale snapshots in the root directory.
func DeleteStaleSnapshots(root string, maxNum int, minAge time.Duration, lfs fs.FileSystem) {
	if maxNum <= 0 {
		return
	}
	lfs.MkdirIfNotExist(root, DirPerm)
	snapshots := lfs.ReadDir(root)
	if len(snapshots) <= maxNum {
		return
	}
	// sort by snapshot name whose format is "20060102150405-00000000"
	sort.Slice(snapshots, func(i, j int) bool {
		return snapshots[i].Name() < snapshots[j].Name()
	})
	now := time.Now()
	for i := 0; i < len(snapshots)-maxNum; i++ {
		snapshotName := snapshots[i].Name()
		// If the min age is not set, then only keep using the max num to delete
		if minAge == 0 {
			lfs.MustRMAll(filepath.Join(root, snapshotName))
			continue
		}
		snapshotTime, parseErr := ParseSnapshotTimestamp(snapshotName)
		if parseErr != nil {
			logger.GetLogger().Warn().Err(parseErr).Str("snapshot", snapshotName).Msg("failed to parse snapshot timestamp, skipping")
			continue
		}
		if now.Sub(snapshotTime) >= minAge {
			lfs.MustRMAll(filepath.Join(root, snapshotName))
		}
	}
}

// DeleteOldSnapshots deletes snapshots older than the specified maxAge duration.
// This function is used during forced cleanup to remove old snapshots regardless of count.
func DeleteOldSnapshots(root string, maxAge time.Duration, lfs fs.FileSystem) {
	if maxAge <= 0 {
		return
	}
	lfs.MkdirIfNotExist(root, DirPerm)
	snapshots := lfs.ReadDir(root)
	if len(snapshots) == 0 {
		return
	}

	cutoffTime := time.Now().Add(-maxAge)

	for _, snapshot := range snapshots {
		snapshotPath := filepath.Join(root, snapshot.Name())
		// Get file modification time using os.Stat since FileSystem interface doesn't provide Stat
		if info, err := os.Stat(snapshotPath); err == nil {
			if info.ModTime().Before(cutoffTime) {
				lfs.MustRMAll(snapshotPath)
			}
		}
	}
}
