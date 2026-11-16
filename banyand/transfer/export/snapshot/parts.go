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

// Package snapshot reads an export session's snapshot and holds the framing and rate
// contract every Export stream format shares.
package snapshot

import (
	"errors"
	"fmt"
	iofs "io/fs"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/nativefmt"
)

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

// SessionPartIDs returns the sorted ids of the parts an export session's snapshot holds in
// shardDir; see SessionParts.
func SessionPartIDs(shardDir string) ([]uint64, error) {
	ids, _, err := SessionParts(shardDir)
	return ids, err
}

// SessionParts returns the sorted ids of the parts an export session's snapshot holds in
// shardDir, the parts its newest snapshot file names that are on disk, and the name of that
// snapshot file. The snapshot file also names the in-memory parts of the table, which the
// snapshot does not hold, so a named part missing on disk is skipped. The session's snapshot
// does not change while the session holds it, and a unit names only shards holding parts, so a
// missing or unreadable snapshot file or a snapshot holding no part means the snapshot lost
// data: the error wraps fs.ErrNotExist for a missing snapshot file or part.
func SessionParts(shardDir string) (ids []uint64, newestSnapshot string, err error) {
	onDisk, snapshots, err := listShard(shardDir)
	if err != nil {
		return nil, "", err
	}
	if len(snapshots) == 0 {
		return nil, "", fmt.Errorf("no snapshot file in %s: %w", shardDir, iofs.ErrNotExist)
	}
	newestSnapshot = snapshots[len(snapshots)-1]
	newest := filepath.Join(shardDir, newestSnapshot)
	names, err := storage.ReadSnapshotPartNames(fs.NewLocalFileSystem(), newest)
	if err != nil {
		return nil, "", fmt.Errorf("read snapshot %s: %w", newest, err)
	}
	if ids = namedOnDisk(names, onDisk); len(ids) == 0 {
		return nil, "", fmt.Errorf("snapshot %s holds no part: %w", newest, iofs.ErrNotExist)
	}
	return ids, newestSnapshot, nil
}

// CountLiveParts adds up the parts of the given shards of the segment at segDir in an export
// session's snapshot. A shard that lost its snapshot or a part means the session was preempted
// or released (FailedPrecondition).
func CountLiveParts(segDir string, shards []uint32) (uint32, error) {
	var parts uint32
	for _, shard := range shards {
		shardDir := filepath.Join(segDir, nativefmt.ShardDir(shard))
		ids, err := SessionPartIDs(shardDir)
		if err != nil {
			return 0, ReadError("list parts of "+shardDir, err)
		}
		parts += uint32(len(ids))
	}
	return parts, nil
}

// livePartIDs lists shardDir once; replaced reports that no snapshot was readable because
// one vanished after the listing.
func livePartIDs(shardDir string) (ids []uint64, replaced bool, err error) {
	onDisk, snapshots, err := listShard(shardDir)
	if err != nil || len(onDisk) == 0 {
		return nil, false, err
	}
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
		return namedOnDisk(names, onDisk), false, nil
	}
	if unreadable != nil && !replaced {
		return nil, false, fmt.Errorf("no readable snapshot in %s: %w", shardDir, unreadable)
	}
	return nil, replaced, nil
}

// listShard reads shardDir once: the ids of its part directories, ascending, and the names of
// its snapshot files, oldest first (their names are fixed-width hex epochs, and os.ReadDir
// sorts by name).
func listShard(shardDir string) (partIDs []uint64, snapshots []string, err error) {
	entries, err := os.ReadDir(shardDir)
	if err != nil {
		return nil, nil, fmt.Errorf("read shard directory: %w", err)
	}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() {
			if id, parseErr := strconv.ParseUint(name, 16, 64); parseErr == nil {
				partIDs = append(partIDs, id)
			}
			continue
		}
		if stem, ok := strings.CutSuffix(name, nativefmt.SnapshotExt); ok {
			if _, parseErr := strconv.ParseUint(stem, 16, 64); parseErr == nil {
				snapshots = append(snapshots, name)
			}
		}
	}
	slices.Sort(partIDs)
	return partIDs, snapshots, nil
}

// namedOnDisk returns the ids of onDisk, which is sorted, that a snapshot file's part names
// list; names that are not hex part ids are ignored.
func namedOnDisk(names []string, onDisk []uint64) []uint64 {
	listed := make(map[uint64]struct{}, len(names))
	for _, name := range names {
		if id, parseErr := strconv.ParseUint(name, 16, 64); parseErr == nil {
			listed[id] = struct{}{}
		}
	}
	var ids []uint64
	for _, id := range onDisk {
		if _, ok := listed[id]; ok {
			ids = append(ids, id)
		}
	}
	return ids
}
