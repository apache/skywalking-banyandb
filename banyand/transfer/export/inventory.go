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

package export

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/dump"
	"github.com/apache/skywalking-banyandb/banyand/internal/sidx"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/index/inverted"
)

const (
	segDirPrefix   = "seg-"
	sidxDirName    = "sidx"
	shardDirPrefix = "shard-"
	// streamIdxDirName is the per-shard element index of a stream (stream/part.go).
	streamIdxDirName = "idx"
)

// segSuffixPattern is the day (YYYYMMDD) or hour (YYYYMMDDHH) suffix of a segment
// directory; anything else under a group (seg-bak, scratch copies) is not a unit.
var segSuffixPattern = regexp.MustCompile(`^[0-9]{8}$|^[0-9]{10}$`)

// partReadFunc reads one part's metadata.json; see PartReader.
type partReadFunc func(partDir string) (queue.StreamingPartData, error)

// statTSDBGroup lists <groupDir>/seg-*/shard-*/<partID>/metadata.json for stream,
// measure and trace groups. indexMode turns on the bluge document count of the
// segment-level series index, which is the only place index-mode measures store rows.
// The numbers describe the directory as it is: parts that are on disk but not yet
// published, or left behind by an interrupted merge, are counted too (design §2.3 calls
// the live-directory inventory an estimate). A segment that vanishes mid-walk, or whose
// metadata is still being written by a rollover, is skipped as a whole.
func statTSDBGroup(groupDir string, catalog commonv1.Catalog, group string, indexMode bool, read partReadFunc) ([]*transferv1.UnitInventory, error) {
	segEntries, err := readDirOrEmpty(groupDir)
	if err != nil {
		return nil, err
	}
	var units []*transferv1.UnitInventory
	for _, se := range segEntries {
		suffix, ok := segmentSuffixOf(se)
		if !ok {
			continue
		}
		unit, statErr := statSegment(filepath.Join(groupDir, se.Name()), suffix, catalog, group, indexMode, read)
		if statErr != nil {
			if errors.Is(statErr, fs.ErrNotExist) {
				continue // segment deleted by retention, or not yet fully created, between the two listings
			}
			return nil, statErr
		}
		units = append(units, unit)
	}
	sort.Slice(units, func(i, j int) bool {
		return units[i].GetSegment().GetUnit().GetSegmentSuffix() < units[j].GetSegment().GetUnit().GetSegmentSuffix()
	})
	return units, nil
}

// hasSegmentDir reports whether groupDir holds at least one segment directory.
func hasSegmentDir(groupDir string) (bool, error) {
	entries, err := readDirOrEmpty(groupDir)
	if err != nil {
		return false, err
	}
	return slices.ContainsFunc(entries, func(e os.DirEntry) bool {
		_, ok := segmentSuffixOf(e)
		return ok
	}), nil
}

// segmentSuffixOf returns the time suffix of a seg-<suffix> directory entry.
func segmentSuffixOf(e os.DirEntry) (string, bool) {
	if !e.IsDir() || !strings.HasPrefix(e.Name(), segDirPrefix) {
		return "", false
	}
	suffix := strings.TrimPrefix(e.Name(), segDirPrefix)
	return suffix, segSuffixPattern.MatchString(suffix)
}

// statSegment inventories one segment directory as a SegmentInventory unit.
func statSegment(segDir, suffix string, catalog commonv1.Catalog, group string, indexMode bool, read partReadFunc) (*transferv1.UnitInventory, error) {
	version, err := readSegmentVersion(segDir)
	if err != nil {
		return nil, err
	}
	sidxDir := filepath.Join(segDir, sidxDirName)
	sidxBytes, err := dirBytes(sidxDir)
	if err != nil {
		return nil, err
	}
	segLevel := &transferv1.SidxStat{EstimatedBytes: sidxBytes}
	if indexMode {
		if segLevel.DocCount, err = docCount(sidxDir); err != nil {
			return nil, err
		}
	}
	// ENOENT here is the segment itself going away; it must propagate so the caller drops
	// the segment instead of reporting a phantom unit without shards.
	entries, err := os.ReadDir(segDir)
	if err != nil {
		return nil, err
	}
	seg := &transferv1.SegmentInventory{
		Unit:           &transferv1.SegmentUnit{Catalog: catalog, Group: group, SegmentSuffix: suffix},
		SegmentVersion: version,
		SegmentLevel:   segLevel,
	}
	for _, e := range entries {
		id, ok := shardIDOf(e)
		if !ok {
			continue
		}
		stat, statErr := statShard(filepath.Join(segDir, e.Name()), id, catalog, read)
		if statErr != nil {
			if errors.Is(statErr, fs.ErrNotExist) {
				continue
			}
			return nil, statErr
		}
		if stat.PartsCount == 0 {
			continue // a shard directory without flushed parts holds nothing to export
		}
		seg.Unit.ShardIds = append(seg.Unit.ShardIds, id)
		seg.Shards = append(seg.Shards, stat)
	}
	sort.Slice(seg.Shards, func(i, j int) bool { return seg.Shards[i].ShardId < seg.Shards[j].ShardId })
	slices.Sort(seg.Unit.ShardIds)
	return &transferv1.UnitInventory{Kind: &transferv1.UnitInventory_Segment{Segment: seg}}, nil
}

// statShard aggregates the parts of one shard. The per-shard index that travels with the
// parts is added to both byte estimates: a stream's idx/ is copied as-is, so its size counts
// on both sides; a trace's sidx/<rule>/<part> carries its own manifest.json sizes.
func statShard(shardDir string, shardID uint32, catalog commonv1.Catalog, read partReadFunc) (*transferv1.ShardStat, error) {
	partIDs, err := dump.DiscoverPartIDs(shardDir)
	if err != nil {
		return nil, err
	}
	stat := &transferv1.ShardStat{ShardId: shardID}
	for _, id := range partIDs {
		pm, readErr := read(filepath.Join(shardDir, fmt.Sprintf("%016x", id)))
		if readErr != nil {
			if errors.Is(readErr, fs.ErrNotExist) {
				continue // part merged away between ReadDir and ReadFile
			}
			return nil, readErr
		}
		if stat.PartsCount == 0 || pm.MinTimestamp < stat.MinTimestamp {
			stat.MinTimestamp = pm.MinTimestamp
		}
		if pm.MaxTimestamp > stat.MaxTimestamp {
			stat.MaxTimestamp = pm.MaxTimestamp
		}
		stat.PartsCount++
		stat.TotalCount += pm.TotalCount
		stat.EstimatedCompressedBytes += pm.CompressedSizeBytes
		stat.EstimatedUncompressedBytes += pm.UncompressedSizeBytes
		stat.Parts = append(stat.Parts, &transferv1.PartStat{
			Id: id, MinTimestamp: pm.MinTimestamp, MaxTimestamp: pm.MaxTimestamp, TotalCount: pm.TotalCount,
		})
	}
	switch catalog {
	case commonv1.Catalog_CATALOG_STREAM:
		idxBytes, idxErr := dirBytes(filepath.Join(shardDir, streamIdxDirName))
		if idxErr != nil {
			return nil, idxErr
		}
		stat.EstimatedCompressedBytes += idxBytes
		stat.EstimatedUncompressedBytes += idxBytes
	case commonv1.Catalog_CATALOG_TRACE:
		compressed, uncompressed, sidxErr := sidxBytes(filepath.Join(shardDir, sidxDirName))
		if sidxErr != nil {
			return nil, sidxErr
		}
		stat.EstimatedCompressedBytes += compressed
		stat.EstimatedUncompressedBytes += uncompressed
	default: // measure parts travel without a per-shard index
	}
	return stat, nil
}

// sidxBytes sums the manifest.json sizes of every sidx part under <sidxDir>/<rule>/. A missing
// directory or a part merged away mid-walk counts as nothing.
func sidxBytes(sidxDir string) (uint64, uint64, error) {
	rules, err := readDirOrEmpty(sidxDir)
	if err != nil {
		return 0, 0, err
	}
	var compressed, uncompressed uint64
	for _, r := range rules {
		if !r.IsDir() {
			continue
		}
		ruleDir := filepath.Join(sidxDir, r.Name())
		partIDs, idErr := dump.DiscoverPartIDs(ruleDir)
		if idErr != nil {
			if errors.Is(idErr, fs.ErrNotExist) {
				continue
			}
			return 0, 0, idErr
		}
		for _, id := range partIDs {
			pm, readErr := sidx.ParsePartMetadata(localFS, filepath.Join(ruleDir, fmt.Sprintf("%016x", id)))
			if readErr != nil {
				if errors.Is(readErr, fs.ErrNotExist) {
					continue
				}
				return 0, 0, readErr
			}
			compressed += pm.CompressedSizeBytes
			uncompressed += pm.UncompressedSizeBytes
		}
	}
	return compressed, uncompressed, nil
}

// statPropertyGroup lists <groupDir>/shard-*/ bluge directories. Property has no segment
// layer, so it yields exactly one PropertyInventory unit per group, or nil when the group
// has no shard directory on this node. doc_count is the cheap bluge document count: it
// includes every version of an entity and its tombstones (design §3.1).
func statPropertyGroup(groupDir, group string) (*transferv1.UnitInventory, error) {
	entries, err := readDirOrEmpty(groupDir)
	if err != nil {
		return nil, err
	}
	prop := &transferv1.PropertyInventory{Group: group}
	for _, e := range entries {
		id, ok := shardIDOf(e)
		if !ok {
			continue
		}
		shardDir := filepath.Join(groupDir, e.Name())
		size, sizeErr := dirBytes(shardDir)
		if sizeErr != nil {
			return nil, sizeErr
		}
		count, countErr := docCount(shardDir)
		if countErr != nil {
			return nil, countErr
		}
		stat := &transferv1.PropertyShardStat{ShardId: id, EstimatedBytes: size, DocCount: count}
		prop.Shards = append(prop.Shards, stat)
	}
	if len(prop.Shards) == 0 {
		return nil, nil
	}
	sort.Slice(prop.Shards, func(i, j int) bool { return prop.Shards[i].ShardId < prop.Shards[j].ShardId })
	return &transferv1.UnitInventory{Kind: &transferv1.UnitInventory_Property{Property: prop}}, nil
}

// docCount is the committed document count of the bluge index in dir. An index that was
// never flushed (or is absent) holds no committed generation and counts 0; any other
// failure, a corrupt index among them, fails the plan rather than reporting 0 rows.
func docCount(dir string) (uint64, error) {
	count, err := inverted.ReadOnlyDocCount(dir)
	if err != nil {
		if errors.Is(err, inverted.ErrNoCommittedIndex) || errors.Is(err, fs.ErrNotExist) {
			return 0, nil
		}
		return 0, fmt.Errorf("count documents of index %s: %w", dir, err)
	}
	return uint64(count), nil
}

func shardIDOf(e os.DirEntry) (uint32, bool) {
	if !e.IsDir() || !strings.HasPrefix(e.Name(), shardDirPrefix) {
		return 0, false
	}
	id, err := strconv.ParseUint(strings.TrimPrefix(e.Name(), shardDirPrefix), 10, 32)
	if err != nil {
		return 0, false
	}
	return uint32(id), true
}

// readDirOrEmpty lists dir, treating a missing directory as empty. It deliberately uses
// os.ReadDir: pkg/fs panics on ENOENT, which a concurrent merge or retention can cause.
func readDirOrEmpty(dir string) ([]os.DirEntry, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	return entries, nil
}
