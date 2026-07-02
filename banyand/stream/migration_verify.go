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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/blugelabs/bluge"

	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/fs"
)

// SegmentReport summarizes one seg-* directory under a stream group
// root. Populated by EnumerateGroupTarget for the migration verify CLI.
//
// Aligned is true only when ALL of the following hold:
//   - the dir name parses cleanly into a start time;
//   - start = IntervalRule.Standard(start) (start is on the grid);
//   - <seg>/metadata exists, is well-formed JSON, and carries a non-empty endTime;
//   - end = IntervalRule.NextTime(start) (segment spans exactly one bucket);
//   - the inclusive last instant (end - 1ns) standardizes back to start.
type SegmentReport struct {
	StartTime    time.Time
	EndTime      time.Time
	Seg          string
	Shards       []ShardReport
	Rows         uint64
	SidxDocCount uint64
	Parts        int
	Aligned      bool
	SidxOpened   bool
}

// ShardReport summarizes one shard-N directory inside a segment,
// including the element index (idx/) bluge doc count (stream-only).
type ShardReport struct {
	Shard       string
	IdxDocCount uint64
	Rows        uint64
	Parts       int
	IdxOpened   bool
}

// EntryGroupReport aggregates source row count + target per-seg report
// for one (entry, group) pair. Both source and target are read-only.
type EntryGroupReport struct {
	Group       string
	EntryStage  string
	EntryTarget string
	TargetGroup string
	EntryNodes  []string
	SrcRoots    []string
	TargetSegs  []SegmentReport
	SrcRows     uint64
	SrcParts    int
}

// VerifyShardParts reads the newest .snp under shardDir, confirms every
// listed partID has an on-disk directory, opens each part, and returns the
// sum of partMetadata.TotalCount plus the part count.
func VerifyShardParts(shardDir string, fileSystem fs.FileSystem) (uint64, int, error) {
	entries, err := os.ReadDir(shardDir)
	if err != nil {
		return 0, 0, fmt.Errorf("read shard: %w", err)
	}
	var snpPath string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), directStreamCopySnpSuffix) {
			candidate := filepath.Join(shardDir, e.Name())
			if snpPath == "" || candidate > snpPath {
				snpPath = candidate
			}
		}
	}
	if snpPath == "" {
		return 0, 0, fmt.Errorf("no .snp file under %s", shardDir)
	}
	snpRaw, err := os.ReadFile(snpPath)
	if err != nil {
		return 0, 0, fmt.Errorf("read .snp: %w", err)
	}
	var partNames []string
	if err := json.Unmarshal(snpRaw, &partNames); err != nil {
		return 0, 0, fmt.Errorf("parse .snp: %w", err)
	}

	onDiskPartIDs := map[string]bool{}
	for _, e := range entries {
		if e.IsDir() && directStreamCopyPartDirPattern.MatchString(e.Name()) {
			onDiskPartIDs[e.Name()] = true
		}
	}
	for _, name := range partNames {
		if !onDiskPartIDs[name] {
			return 0, 0, fmt.Errorf("snp references missing partID %s", name)
		}
	}

	var rowsTotal uint64
	for _, name := range partNames {
		partID, parseErr := strconv.ParseUint(name, 16, 64)
		if parseErr != nil {
			return 0, 0, fmt.Errorf("parse partID %q: %w", name, parseErr)
		}
		rows, openErr := partRowCount(partID, name, shardDir, fileSystem)
		if openErr != nil {
			return 0, 0, openErr
		}
		rowsTotal += rows
	}
	return rowsTotal, len(partNames), nil
}

// partRowCount opens one part with panic recovery and returns its TotalCount.
func partRowCount(partID uint64, name, shardDir string, fileSystem fs.FileSystem) (rows uint64, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("open part %s panicked: %v", name, r)
		}
	}()
	p := mustOpenFilePart(partID, shardDir, fileSystem)
	defer p.close()
	return p.partMetadata.TotalCount, nil
}

// CountBlugeDocs opens the bluge index at path read-only and returns
// the total document count. Used by verify to spot-check idx/ and sidx/.
func CountBlugeDocs(path string) (uint64, error) {
	reader, err := bluge.OpenReader(bluge.DefaultConfig(path))
	if err != nil {
		return 0, err
	}
	defer func() { _ = reader.Close() }()
	return reader.Count()
}

// EnumerateGroupTarget walks <groupRoot>/seg-* and reports per-segment
// row total + sidx doc count + per-shard idx doc count + whether the segment's
// start time aligns to the supplied IntervalRule's standard grid.
// A missing groupRoot returns (nil, nil).
func EnumerateGroupTarget(groupRoot string, intervalRule storage.IntervalRule, fileSystem fs.FileSystem) ([]SegmentReport, error) {
	entries, err := os.ReadDir(groupRoot)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("read group root %s: %w", groupRoot, err)
	}
	var reports []SegmentReport
	for _, e := range entries {
		if !e.IsDir() || !strings.HasPrefix(e.Name(), directStreamCopySegPrefix) {
			continue
		}
		segDir := filepath.Join(groupRoot, e.Name())
		report := SegmentReport{Seg: e.Name()}

		var (
			startTime time.Time
			endTime   time.Time
			haveStart bool
			haveEnd   bool
		)
		if start, parseErr := parseStreamDirectCopySegStart(e.Name(), intervalRule.Unit); parseErr == nil {
			startTime = start
			haveStart = true
			report.StartTime = start
		}

		metaPath := filepath.Join(segDir, storage.SegmentMetadataFilename)
		if raw, readErr := os.ReadFile(metaPath); readErr == nil {
			var meta storage.SegmentMetadata
			if jsonErr := json.Unmarshal(raw, &meta); jsonErr == nil && meta.EndTime != "" {
				if parsed, parseErr := time.Parse(time.RFC3339Nano, meta.EndTime); parseErr == nil {
					endTime = parsed
					haveEnd = true
					report.EndTime = parsed
				}
			}
		}

		if haveStart && haveEnd {
			startOnGrid := intervalRule.Standard(startTime).Equal(startTime)
			endIsNext := intervalRule.NextTime(startTime).Equal(endTime)
			sameBucket := intervalRule.Standard(endTime.Add(-time.Nanosecond)).Equal(startTime)
			report.Aligned = startOnGrid && endIsNext && sameBucket
		}

		shardEntries, readErr := os.ReadDir(segDir)
		if readErr != nil {
			return nil, fmt.Errorf("read seg %s: %w", segDir, readErr)
		}
		for _, sh := range shardEntries {
			if !sh.IsDir() || !strings.HasPrefix(sh.Name(), directStreamCopyShardPrefix) {
				continue
			}
			shardDir := filepath.Join(segDir, sh.Name())
			shardReport := ShardReport{Shard: sh.Name()}

			rows, parts, partsErr := VerifyShardParts(shardDir, fileSystem)
			if partsErr != nil {
				return nil, fmt.Errorf("shard %s: %w", shardDir, partsErr)
			}
			shardReport.Rows = rows
			shardReport.Parts = parts

			// Stream-only: count element index docs in idx/.
			idxDir := filepath.Join(shardDir, elementIndexFilename)
			if info, statErr := os.Stat(idxDir); statErr == nil && info.IsDir() {
				count, idxErr := CountBlugeDocs(idxDir)
				if idxErr != nil {
					return nil, fmt.Errorf("shard %s idx open: %w", shardDir, idxErr)
				}
				shardReport.IdxOpened = true
				shardReport.IdxDocCount = count
			}

			report.Shards = append(report.Shards, shardReport)
			report.Rows += rows
			report.Parts += parts
		}

		sidxDir := filepath.Join(segDir, directStreamCopySidxDirName)
		if info, statErr := os.Stat(sidxDir); statErr == nil && info.IsDir() {
			count, sidxErr := CountBlugeDocs(sidxDir)
			if sidxErr != nil {
				return nil, fmt.Errorf("seg %s sidx open: %w", e.Name(), sidxErr)
			}
			report.SidxOpened = true
			report.SidxDocCount = count
		}
		reports = append(reports, report)
	}
	sort.Slice(reports, func(i, j int) bool { return reports[i].Seg < reports[j].Seg })
	return reports, nil
}

// SumGroupSourceRows walks every <root>/seg-*/shard-*/<partID>/ across
// the given source roots, opens each part read-only, and returns the total
// row count plus the part count.
func SumGroupSourceRows(srcRoots []string, fileSystem fs.FileSystem) (uint64, int, error) {
	var totalRows uint64
	var totalParts int
	for _, root := range srcRoots {
		segEntries, err := os.ReadDir(root)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return 0, 0, fmt.Errorf("read src root %s: %w", root, err)
		}
		for _, se := range segEntries {
			if !se.IsDir() || !strings.HasPrefix(se.Name(), directStreamCopySegPrefix) {
				continue
			}
			segDir := filepath.Join(root, se.Name())
			shardEntries, readErr := os.ReadDir(segDir)
			if readErr != nil {
				return 0, 0, fmt.Errorf("read src seg %s: %w", segDir, readErr)
			}
			for _, sh := range shardEntries {
				if !sh.IsDir() || !strings.HasPrefix(sh.Name(), directStreamCopyShardPrefix) {
					continue
				}
				shardDir := filepath.Join(segDir, sh.Name())
				rows, parts, partsErr := sumStreamShardPartsOnDisk(shardDir, fileSystem)
				if partsErr != nil {
					return 0, 0, fmt.Errorf("src shard %s: %w", shardDir, partsErr)
				}
				totalRows += rows
				totalParts += parts
			}
		}
	}
	return totalRows, totalParts, nil
}

// sumStreamShardPartsOnDisk pattern-scans <shardDir>/<partID> dirs — the same
// discovery the copy path uses; source shards may lack a migration-written
// .snp — and sums their TotalCount.
func sumStreamShardPartsOnDisk(shardDir string, fileSystem fs.FileSystem) (uint64, int, error) {
	entries, err := os.ReadDir(shardDir)
	if err != nil {
		return 0, 0, fmt.Errorf("read shard: %w", err)
	}
	var rowsTotal uint64
	parts := 0
	for _, e := range entries {
		if !e.IsDir() || !directStreamCopyPartDirPattern.MatchString(e.Name()) {
			continue
		}
		partID, parseErr := strconv.ParseUint(e.Name(), 16, 64)
		if parseErr != nil {
			return 0, 0, fmt.Errorf("parse partID %q: %w", e.Name(), parseErr)
		}
		rows, openErr := partRowCount(partID, e.Name(), shardDir, fileSystem)
		if openErr != nil {
			return 0, 0, openErr
		}
		rowsTotal += rows
		parts++
	}
	return rowsTotal, parts, nil
}
