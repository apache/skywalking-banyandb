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

	compat "github.com/blugelabs/bluge"

	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/index"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
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
//
// IndexMode-prefixed fields are populated only when the (entry, group) is an
// index-mode measure group: those groups carry their data in the segment sidx
// (one doc per series), so they are reconciled at the document level (doc count,
// distinct doc-id, per-(series,timestamp) value digest) instead of part rows.
type EntryGroupReport struct {
	Group             string
	EntryStage        string
	EntryTarget       string
	TargetGroup       string
	EntryNodes        []string
	SrcRoots          []string
	TargetSegs        []SegmentReport
	ValueMismatches   []IndexModeValueMismatch
	SegAligns         []IndexModeSegAlign
	SrcRows           uint64
	SrcDocs           uint64
	TgtDocs           uint64
	SrcDistinctIDs    uint64
	TgtDistinctIDs    uint64
	ExpectDistinctIDs uint64
	SrcParts          int
	IndexMode         bool
}

// IndexModeValueMismatch is one (seriesID, timestamp) key whose value digest did
// not reconcile between source and target during an index-mode verify. The
// reconciliation unit is (Segment, SeriesID) — the unit the runtime upserts
// index-mode data on. Kind is one of "missing" (in source, absent in target),
// "extra" (in target, absent in source) or "tampered" (present on both sides but
// the digests differ — a changed value or a dropped index-only field).
type IndexModeValueMismatch struct {
	Kind     string
	Segment  string
	SeriesID uint64
}

// IndexModeSegAlign reports one target segment's alignment during an index-mode
// verify: how many source segments fed it (SrcSegs), how many source docs routed
// in (SrcDocs) and how many target docs survived (TgtDocs). Aligned is true only
// when exactly one source segment fed the target and no docs collapsed (a 1:1
// byte-copy candidate); otherwise the segment was merged or split-into.
type IndexModeSegAlign struct {
	Segment string
	SrcSegs int
	SrcDocs uint64
	TgtDocs uint64
	Aligned bool
}

// VerifyShardParts reads <shardDir>'s newest `.snp`, confirms every
// listed partID has an on-disk dir, opens each part, and returns the
// sum of partMetadata.TotalCount plus the part count.
//
// Errors (instead of t.Fatalf) so this helper is usable from a CLI.
//
// fileSystem is passed through to mustOpenFilePart. Directory listing
// and .snp reads go through the os package directly: fs.FileSystem's
// ReadDir panics on missing directories, and verify is read-only, so
// the abstraction is intentionally bypassed for the enumeration path.
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

// CountBlugeDocs opens the bluge index at path read-only and returns the
// total document count. Used to spot-check the broadcast union sidx
// that `migration copy` writes into every aligned target segment.
func CountBlugeDocs(path string) (uint64, error) {
	reader, err := compat.OpenReader(compat.DefaultConfig(path))
	if err != nil {
		return 0, err
	}
	defer func() { _ = reader.Close() }()
	// Reader.Count sums per-segment doc counts in O(numSegments), not
	// O(numDocs) — important for `verify` because the union sidx is
	// broadcast into every aligned target seg of a group and we call
	// CountBlugeDocs once per seg. Iterating dmi.Next() instead would
	// scale with total docs × broadcast fan-out and dominate verify
	// runtime on production-sized datasets.
	return reader.Count()
}

type storedFieldDocument interface {
	VisitStoredFields(visit func(name string, value []byte) bool) error
}

type retainedStoredDocument struct {
	visitStoredFields func(func(name string, value []byte) bool) error
}

func (d retainedStoredDocument) VisitStoredFields(visit func(name string, value []byte) bool) error {
	return d.visitStoredFields(visit)
}

func readIndexModeDocs(ctx context.Context, sidxDir string, ruleByID map[uint32]indexRuleInfo,
	schemasBySubject map[string]*measureSchemaInfo,
) ([]index.Document, error) {
	reader, openErr := compat.OpenReader(compat.DefaultConfig(sidxDir))
	if openErr != nil {
		if strings.Contains(openErr.Error(), "unable to find a usable snapshot") {
			return nil, nil
		}
		return nil, fmt.Errorf("open sidx reader %s: %w", sidxDir, openErr)
	}
	defer func() { _ = reader.Close() }()
	matches, searchErr := reader.Search(ctx, compat.NewAllMatches(compat.NewMatchAllQuery()))
	if searchErr != nil {
		return nil, fmt.Errorf("search sidx %s: %w", sidxDir, searchErr)
	}
	var documents []index.Document
	var timeless []uint64
	tagNames := collectTagNames(schemasBySubject)
	missingRules := map[uint32]int{}
	for {
		match, nextErr := matches.Next()
		if nextErr != nil {
			return nil, fmt.Errorf("iterate sidx %s: %w", sidxDir, nextErr)
		}
		if match == nil {
			break
		}
		document, rebuildErr := rebuildOneDoc(retainedStoredDocument{visitStoredFields: func(visit func(name string, value []byte) bool) error {
			return match.VisitStoredFields(func(name string, value []byte) bool {
				return visit(name, value)
			})
		}}, ruleByID, schemasBySubject, tagNames, missingRules)
		if rebuildErr != nil {
			return nil, fmt.Errorf("rebuild doc in %s: %w", sidxDir, rebuildErr)
		}
		if document.Timestamp == 0 {
			timeless = append(timeless, document.DocID)
			continue
		}
		documents = append(documents, document)
	}
	if len(timeless) > 0 {
		sample := timeless
		if len(sample) > 10 {
			sample = sample[:10]
		}
		return nil, fmt.Errorf("sidx %s: %d index-mode doc(s) have a missing or undecodable _timestamp (ts==0), "+
			"which never occurs for valid data; the source is corrupt or unexpected — sample series IDs (first %d) %v",
			sidxDir, len(timeless), len(sample), sample)
	}
	warnMissingRules(sidxDir, missingRules)
	return documents, nil
}

func rebuildOneDoc(source storedFieldDocument, ruleByID map[uint32]indexRuleInfo,
	schemasBySubject map[string]*measureSchemaInfo, tagNames map[string]struct{}, missingRules map[uint32]int,
) (index.Document, error) {
	var entityValues []byte
	var timestamp, version int64
	var fields []index.Field
	visitErr := source.VisitStoredFields(func(name string, value []byte) bool {
		switch name {
		case imDocIDField:
			entityValues = append([]byte(nil), value...)
		case imTimestampField:
			if decodedTime, decodeErr := compat.DecodeDateTime(value); decodeErr == nil {
				timestamp = decodedTime.UnixNano()
			}
		case imVersionField:
			version = convert.BytesToInt64(value)
		default:
			key, indexed, missingRuleID := classifyStoredField(name, tagNames, ruleByID)
			field := index.NewBytesField(key, value)
			field.Store = true
			field.Index = indexed
			if rule, found := ruleByID[key.IndexRuleID]; indexed && found {
				field.NoSort = rule.NoSort
				field.Key.Analyzer = rule.Analyzer
			}
			if missingRuleID != 0 && missingRules != nil {
				missingRules[missingRuleID]++
			}
			fields = append(fields, field)
		}
		return true
	})
	if visitErr != nil {
		return index.Document{}, fmt.Errorf("visit stored fields: %w", visitErr)
	}
	var series pbv1.Series
	if unmarshalErr := series.Unmarshal(entityValues); unmarshalErr != nil {
		return index.Document{}, fmt.Errorf("unmarshal series from _id: %w", unmarshalErr)
	}
	fields = appendRegeneratedEntityFields(fields, &series, schemasBySubject[series.Subject])
	return index.Document{
		Fields:       fields,
		EntityValues: entityValues,
		Timestamp:    timestamp,
		DocID:        uint64(series.ID),
		Version:      version,
	}, nil
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
				rows, parts, partsErr := sumShardPartsOnDisk(shardDir, fileSystem)
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

// sumShardPartsOnDisk pattern-scans <shardDir>/<partID> dirs — the same
// discovery the copy path uses; source shards may lack a migration-written
// .snp — and sums their TotalCount.
func sumShardPartsOnDisk(shardDir string, fileSystem fs.FileSystem) (uint64, int, error) {
	entries, err := os.ReadDir(shardDir)
	if err != nil {
		return 0, 0, fmt.Errorf("read shard: %w", err)
	}
	var rowsTotal uint64
	parts := 0
	for _, e := range entries {
		if !e.IsDir() || !directCopyPartDirPattern.MatchString(e.Name()) {
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
