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
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
)

const partDiskMetadataFilename = "metadata.json" // per-part metadata file, shared by stream/measure/trace.

const snapshotFileSuffix = ".snp" // suffix of the per-shard snapshot manifest (a JSON array of part dir names).

// partDiskMetadata captures the fields shared by every data type's part
// metadata.json (stream/measure/trace use the same JSON keys for these).
type partDiskMetadata struct {
	CompressedSizeBytes uint64 `json:"compressedSizeBytes"`
	TotalCount          uint64 `json:"totalCount"`
}

// CollectClosedShardInfo reads per-shard stats for a closed segment directly
// from disk, WITHOUT opening any table or bluge index (which would reacquire
// the index's exclusive lock). Sub-index stats (inverted index / sidx) are
// reported empty; callers populate those only for open segments. It returns the
// shard list and the total on-disk data size.
func CollectClosedShardInfo(segLocation string) ([]*databasev1.ShardInfo, int64) {
	shardPrefix := shardPathPrefix + "-"
	// Use os directly (not the fs abstraction whose ReadDir panics on a missing
	// directory): a closed segment may be removed by retention concurrently, so
	// missing/unreadable paths must be skipped, not fatal. Segment data is
	// always on local disk.
	entries, err := os.ReadDir(segLocation)
	if err != nil {
		return nil, 0
	}
	var (
		infos     []*databasev1.ShardInfo
		totalSize int64
	)
	for _, entry := range entries {
		if !entry.IsDir() || !strings.HasPrefix(entry.Name(), shardPrefix) {
			continue
		}
		shardID, parseErr := strconv.ParseUint(strings.TrimPrefix(entry.Name(), shardPrefix), 10, 32)
		if parseErr != nil {
			continue
		}
		info := readShardInfoFromDisk(uint32(shardID), filepath.Join(segLocation, entry.Name()))
		infos = append(infos, info)
		totalSize += info.DataSizeBytes
	}
	return infos, totalSize
}

// readShardInfoFromDisk aggregates one shard's part statistics into a ShardInfo
// from the parts referenced by its latest snapshot manifest -- the same
// authoritative set the in-memory snapshot exposes for an open segment, so
// orphan parts not yet garbage-collected are excluded.
func readShardInfoFromDisk(shardID uint32, shardDir string) *databasev1.ShardInfo {
	info := &databasev1.ShardInfo{
		ShardId:           shardID,
		InvertedIndexInfo: &databasev1.InvertedIndexInfo{},
		SidxInfo:          &databasev1.SIDXInfo{},
	}
	partNames, ok := latestSnapshotParts(shardDir)
	if !ok {
		return info
	}
	for _, partName := range partNames {
		raw, err := os.ReadFile(filepath.Join(shardDir, partName, partDiskMetadataFilename))
		if err != nil {
			continue
		}
		var pm partDiskMetadata
		if json.Unmarshal(raw, &pm) != nil {
			continue
		}
		info.DataCount += int64(pm.TotalCount)
		info.DataSizeBytes += int64(pm.CompressedSizeBytes)
		info.PartCount++
	}
	info.FilePartCount = info.PartCount
	return info
}

// latestSnapshotParts returns the part directory names referenced by the
// highest-epoch ".snp" manifest in shardDir (false if none exists).
func latestSnapshotParts(shardDir string) ([]string, bool) {
	entries, err := os.ReadDir(shardDir)
	if err != nil {
		return nil, false
	}
	var (
		latestName  string
		latestEpoch uint64
	)
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, snapshotFileSuffix) {
			continue
		}
		epoch, parseErr := strconv.ParseUint(strings.TrimSuffix(name, snapshotFileSuffix), 16, 64)
		if parseErr != nil {
			continue
		}
		// A valid manifest name is never empty, so latestName doubles as the
		// "found one" flag.
		if latestName == "" || epoch > latestEpoch {
			latestEpoch, latestName = epoch, name
		}
	}
	if latestName == "" {
		return nil, false
	}
	// ReadSnapshotPartNames uses lfs.Read, which (unlike lfs.ReadDir) returns an
	// error rather than panicking on a missing file, so it is safe on the
	// concurrently-deletable closed path.
	partNames, err := ReadSnapshotPartNames(lfs, filepath.Join(shardDir, latestName))
	if err != nil {
		return nil, false
	}
	return partNames, true
}
