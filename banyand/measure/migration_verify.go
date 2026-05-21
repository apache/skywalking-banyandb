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
	"context"
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

// SegmentReport summarizes one `seg-*` directory under a measure
// group root. Populated by EnumerateGroupTarget for the migration
// verify CLI.
//
// Aligned is true only when ALL of these hold:
//   - the dir name parses cleanly into a start time;
//   - start = IntervalRule.Standard(start) (start is on the grid);
//   - <seg>/metadata exists, is well-formed JSON, and carries a non-empty endTime;
//   - end = IntervalRule.NextTime(start) (the segment spans exactly one bucket);
//   - the inclusive last instant (end - 1ns) standardizes back to start (start
//     and end fall in the same IntervalRule bucket).
type SegmentReport struct {
	StartTime    time.Time
	EndTime      time.Time
	Seg          string
	Rows         uint64
	SidxDocCount uint64
	Shards       int
	Parts        int
	Aligned      bool
	SidxOpened   bool
}

// EntryGroupReport aggregates source row count + target per-seg report
// for one (entry, group) pair. Both source and target are read-only;
// the verify command never mutates the dataset.
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

// VerifyShardParts reads <shardDir>'s newest `.snp`, confirms every
// listed partID has an on-disk dir, opens each part, and returns the
// sum of partMetadata.TotalCount plus the part count.
//
// Errors (instead of t.Fatalf) so this helper is usable from a CLI.
func VerifyShardParts(shardDir string, fileSystem fs.FileSystem) (uint64, int, error) {
	entries, err := os.ReadDir(shardDir)
	if err != nil {
		return 0, 0, fmt.Errorf("read shard: %w", err)
	}
	var snpPath string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), directCopySnpSuffix) {
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
		if e.IsDir() && directCopyPartDirPattern.MatchString(e.Name()) {
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
		var rows uint64
		var openErr error
		func() {
			defer func() {
				if r := recover(); r != nil {
					openErr = fmt.Errorf("open part %s panicked: %v", name, r)
				}
			}()
			p := mustOpenFilePart(partID, shardDir, fileSystem)
			defer p.close()
			rows = p.partMetadata.TotalCount
		}()
		if openErr != nil {
			return 0, 0, openErr
		}
		rowsTotal += rows
	}
	return rowsTotal, len(partNames), nil
}

// CountBlugeDocs opens the bluge index at path read-only and returns the
// total document count. Used to spot-check the broadcast union sidx
// that `migration copy` writes into every aligned target segment.
func CountBlugeDocs(path string) (uint64, error) {
	reader, err := bluge.OpenReader(bluge.DefaultConfig(path))
	if err != nil {
		return 0, err
	}
	defer func() { _ = reader.Close() }()
	dmi, err := reader.Search(context.Background(),
		bluge.NewAllMatches(bluge.NewMatchAllQuery()))
	if err != nil {
		return 0, err
	}
	var count uint64
	for {
		next, nextErr := dmi.Next()
		if nextErr != nil {
			return count, nextErr
		}
		if next == nil {
			return count, nil
		}
		count++
	}
}

// EnumerateGroupTarget walks <groupRoot>/seg-* and reports per-segment
// row total + sidx doc count + whether the segment's start time aligns
// to the supplied IntervalRule's standard grid. A missing groupRoot
// returns (nil, nil) — the caller decides whether that is a no-op or
// an error (e.g. when the PVC didn't own the group, "no segs" is
// expected, not a fault).
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
		if !e.IsDir() || !strings.HasPrefix(e.Name(), directCopySegPrefix) {
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
		if start, parseErr := parseDirectCopySegStart(e.Name(), intervalRule.Unit); parseErr == nil {
			startTime = start
			haveStart = true
			report.StartTime = start
		}

		// Read <seg>/metadata: the migrated tree's runtime contract is that
		// every segment carries a JSON-encoded SegmentMetadata with a
		// non-empty endTime (RFC3339Nano) — both writer paths
		// (writeDirectCopySegmentMetadata and the live segmentController)
		// emit it. A missing / unparseable file or a missing endTime keeps
		// Aligned false so the verify CLI surfaces it loudly.
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
			if !sh.IsDir() || !strings.HasPrefix(sh.Name(), directCopyShardPrefix) {
				continue
			}
			report.Shards++
			shardDir := filepath.Join(segDir, sh.Name())
			rows, parts, partsErr := VerifyShardParts(shardDir, fileSystem)
			if partsErr != nil {
				return nil, fmt.Errorf("shard %s: %w", shardDir, partsErr)
			}
			report.Rows += rows
			report.Parts += parts
		}

		sidxDir := filepath.Join(segDir, directCopySidxDirName)
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
// the given source roots, opens each part read-only, and returns the
// total row count plus the part count. Missing roots are silently
// skipped — the caller has already filtered them via
// resolveEntrySrcRoots and only existing dirs end up here in practice.
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
			if !se.IsDir() || !strings.HasPrefix(se.Name(), directCopySegPrefix) {
				continue
			}
			segDir := filepath.Join(root, se.Name())
			shardEntries, readErr := os.ReadDir(segDir)
			if readErr != nil {
				return 0, 0, fmt.Errorf("read src seg %s: %w", segDir, readErr)
			}
			for _, sh := range shardEntries {
				if !sh.IsDir() || !strings.HasPrefix(sh.Name(), directCopyShardPrefix) {
					continue
				}
				shardDir := filepath.Join(segDir, sh.Name())
				rows, parts, partsErr := VerifyShardParts(shardDir, fileSystem)
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

// MigrationVerify drives a `migration verify` run end-to-end against a
// DirectCopyConfig. For every (entry, group) it:
//
//  1. Resolves the source roots via the same plan→cfg helper that
//     `copy` uses (so verify always sees what copy saw).
//  2. Sums source row counts by opening each `src/seg-*/shard-*/<partID>/`.
//  3. Enumerates `target/seg-*/`, opens every part + per-seg union sidx,
//     and flags whether each seg's start time is grid-aligned.
//  4. Builds an EntryGroupReport and hands it to `onReport` IMMEDIATELY
//     (callback fires inside the loop, not after) so the operator sees
//     numbers as they arrive — important on cold-tier scans where a
//     single (entry, group) can take minutes to finish.
//
// The function is read-only. Errors returned here mean "I could not
// read this PVC at all" (bad path, permission, broken FS); per-(entry,
// group) data failures are surfaced through the report itself, never
// suppress the callback for sibling pairs.
//
// Pairs with empty source AND empty target are skipped (no callback).
func MigrationVerify(ctx context.Context, cfg DirectCopyConfig, onReport func(EntryGroupReport)) error {
	if err := validateDirectCopyConfig(&cfg); err != nil {
		return err
	}
	//nolint:contextcheck // bluge reader.Search inside walkSchemaPropertyShard already uses its own context.
	resourceOpts, err := loadGroupResourceOptsFromSchema(cfg.BackupDir, cfg.SchemaPropertyPath, cfg.Groups)
	if err != nil {
		return fmt.Errorf("load group resource opts: %w", err)
	}

	fileSystem := fs.NewLocalFileSystem()
	for entryIdx, entry := range cfg.Entries {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		entryTag := fmt.Sprintf("entry [%d/%d]", entryIdx+1, len(cfg.Entries))
		for _, group := range cfg.Groups {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			opts, ok := resourceOpts[group]
			if !ok || opts == nil {
				return fmt.Errorf("%s group %s: ResourceOpts not found in backup schema-property catalog", entryTag, group)
			}
			ir, irErr := resolveStageInterval(opts, entry.Stage)
			if irErr != nil {
				return fmt.Errorf("%s stage=%s group %s: %w", entryTag, entry.Stage, group, irErr)
			}

			srcRoots := resolveEntrySrcRoots(cfg, entry, group)
			targetGroup := filepath.Join(entry.Target, group)
			if len(srcRoots) == 0 {
				// Confirm whether the target carries any parts so the
				// operator can still see them (rare but possible after
				// a partial copy). Empty src + empty tgt is silenced.
				//nolint:contextcheck // bluge reader.Search inside CountBlugeDocs already uses its own context.
				segs, segErr := EnumerateGroupTarget(targetGroup, ir, fileSystem)
				if segErr != nil {
					return fmt.Errorf("%s stage=%s group %s: target: %w", entryTag, entry.Stage, group, segErr)
				}
				if len(segs) == 0 {
					continue
				}
				onReport(EntryGroupReport{
					Group:       group,
					EntryStage:  entry.Stage,
					EntryTarget: entry.Target,
					EntryNodes:  entry.Nodes,
					SrcRoots:    nil,
					TargetGroup: targetGroup,
					TargetSegs:  segs,
				})
				continue
			}

			srcRows, srcParts, srcErr := SumGroupSourceRows(srcRoots, fileSystem)
			if srcErr != nil {
				return fmt.Errorf("%s stage=%s group %s: src: %w", entryTag, entry.Stage, group, srcErr)
			}

			//nolint:contextcheck // bluge reader.Search inside CountBlugeDocs already uses its own context.
			segs, segErr := EnumerateGroupTarget(targetGroup, ir, fileSystem)
			if segErr != nil {
				return fmt.Errorf("%s stage=%s group %s: target: %w", entryTag, entry.Stage, group, segErr)
			}

			onReport(EntryGroupReport{
				Group:       group,
				EntryStage:  entry.Stage,
				EntryTarget: entry.Target,
				EntryNodes:  entry.Nodes,
				SrcRoots:    srcRoots,
				SrcRows:     srcRows,
				SrcParts:    srcParts,
				TargetGroup: targetGroup,
				TargetSegs:  segs,
			})
		}
	}
	return nil
}
