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

package csv

import (
	"errors"
	"fmt"
	iofs "io/fs"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/apache/skywalking-banyandb/banyand/internal/dump"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/fs"
)

const snapshotSuffix = ".snp"

// LivePartIDs returns the sorted ids of the parts a table opened on shardDir would load:
// the parts named by the newest readable snapshot file that are on disk. A part
// directory no snapshot names is left over from a merge or flush the table had not
// cleaned up, and a shard without any snapshot file holds no live part. On a live
// directory a flush or merge can replace the snapshot files while they are read; the
// shard is then listed once more, and snapshot files vanishing again mean the directory
// itself is going away (fs.ErrNotExist). Snapshot files that exist but none of which can be
// read are an error, not an empty shard.
func LivePartIDs(shardDir string) ([]uint64, error) {
	ids, replaced, err := livePartIDs(shardDir)
	if replaced {
		ids, replaced, err = livePartIDs(shardDir)
	}
	if err == nil && replaced {
		err = fmt.Errorf("the snapshot files of %s vanished while being read: %w", shardDir, iofs.ErrNotExist)
	}
	return ids, err
}

// CountLiveParts adds up the live parts of the given shards of the segment at segDir. A
// shard that vanished means the session was preempted or released (FailedPrecondition).
func CountLiveParts(segDir string, shards []uint32) (uint32, error) {
	var parts uint32
	for _, shard := range shards {
		shardDir := filepath.Join(segDir, fmt.Sprintf("%s%d", shardDirPrefix, shard))
		ids, err := LivePartIDs(shardDir)
		if err != nil {
			return 0, readError("list parts of "+shardDir, err)
		}
		parts += uint32(len(ids))
	}
	return parts, nil
}

// livePartIDs lists shardDir once; replaced reports that no snapshot was readable because
// one vanished after the listing.
func livePartIDs(shardDir string) (ids []uint64, replaced bool, err error) {
	ids, err = dump.DiscoverPartIDs(shardDir)
	if err != nil || len(ids) == 0 {
		return nil, false, err
	}
	entries, err := os.ReadDir(shardDir)
	if err != nil {
		return nil, false, err
	}
	var snapshots []string
	for _, e := range entries {
		if name := e.Name(); !e.IsDir() && strings.HasSuffix(name, snapshotSuffix) {
			if _, parseErr := strconv.ParseUint(strings.TrimSuffix(name, snapshotSuffix), 16, 64); parseErr == nil {
				snapshots = append(snapshots, name)
			}
		}
	}
	// Snapshot names are fixed-width hex epochs, so the newest sorts last.
	slices.Sort(snapshots)
	lfs := fs.NewLocalFileSystem()
	var unreadable error
	for i := len(snapshots) - 1; i >= 0; i-- {
		names, readErr := storage.ReadSnapshotPartNames(lfs, filepath.Join(shardDir, snapshots[i]))
		if readErr != nil {
			if errors.Is(readErr, iofs.ErrNotExist) {
				replaced = true
			} else if unreadable == nil {
				unreadable = fmt.Errorf("read snapshot %s: %w", snapshots[i], readErr)
			}
			continue
		}
		listed := make(map[uint64]struct{}, len(names))
		for _, name := range names {
			if id, parseErr := strconv.ParseUint(name, 16, 64); parseErr == nil {
				listed[id] = struct{}{}
			}
		}
		live := ids[:0]
		for _, id := range ids {
			if _, ok := listed[id]; ok {
				live = append(live, id)
			}
		}
		return live, false, nil
	}
	if unreadable != nil && !replaced {
		return nil, false, fmt.Errorf("no readable snapshot in %s: %w", shardDir, unreadable)
	}
	return nil, replaced, nil
}
