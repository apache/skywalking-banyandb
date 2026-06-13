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
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/bytes"
	"github.com/apache/skywalking-banyandb/pkg/compress/zstd"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/fs"
)

// AnalyzeGroupResult is the row-level summary `migration analyze`
// returns for one (entry, group): how many physical rows are on disk
// versus how many distinct (seriesID, timestamp) keys they describe.
//
// Two diff numbers matter:
//   - DuplicateRows is the GLOBAL diff (cross-part dups). These are
//     NOT dropped by migration copy because slow-path processes one
//     source part at a time and never cross-merges buckets.
//   - PerPartDupRows is the WITHIN-part diff summed over every source
//     part. This IS the count slow-path mustInitFromDataPoints would
//     drop on flush — it should match `verify`'s src-tgt diff.
type AnalyzeGroupResult struct {
	SamplesByVersion   []AnalyzeKeyMulti
	PerPartDups        []AnalyzePerPart
	TotalRows          uint64
	UniqueKeys         uint64
	DuplicateRows      uint64
	PerPartDupRows     uint64
	KeysWithDuplicates uint64 // count of (sid, ts) keys that appear in >1 physical rows
	PartsScanned       int
}

// AnalyzePerPart is one row in the per-part dup breakdown — non-zero
// PartialDupRows mean THIS specific source part has rows the slow
// path would dedup on its bucket flush. Boundaries lists the
// "block-N ends at ts=T, block-N+1 (same series) starts at ts=T"
// pairs WITHIN this part — these are exactly the rows the
// merger-fast-path bug (merger.go:306 `<=`) emits.
type AnalyzePerPart struct {
	PartID         string
	SegName        string
	ShardName      string
	SourceRoot     string
	Boundaries     []AnalyzeBoundaryPair
	Rows           uint64
	UniqueKeys     uint64
	PartialDupRows uint64
}

// AnalyzeBoundaryPair names a single boundary collision inside one
// part: block at BlockA ends at (SeriesID, Timestamp), block at
// BlockB (immediately following) begins at the same (SeriesID,
// Timestamp). Both copies survived because the merger fast-path
// wrote pendingBlock verbatim before doing row-level merge.
type AnalyzeBoundaryPair struct {
	SeriesID  common.SeriesID
	Timestamp int64
	VersionA  int64
	VersionB  int64
	BlockA    int
	BlockB    int
}

// AnalyzeKeyMulti reports one (sid, ts) pair that has more than one
// row on disk, along with every version banyandb stored for it.
// Useful operator-facing proof of the "newest version wins" dedup
// banyandb's merger applies.
type AnalyzeKeyMulti struct {
	Versions  []VersionWithPath
	SeriesID  common.SeriesID
	Timestamp int64
}

// VersionWithPath pairs a row's version field with the absolute part
// directory it physically lives in, so the operator can grep / cat
// the on-disk files directly to confirm duplicates.
type VersionWithPath struct {
	Path    string
	Version int64
}

type analyzeKey struct {
	sid common.SeriesID
	ts  int64
}

// pathTable interns part paths so we don't pay a string allocation
// per row in the (sid, ts) → []rowSrc map. Each unique path lands at
// a small uint32 index; per-row overhead drops from ~80B to 12B.
type pathTable struct {
	idx   map[string]uint32
	paths []string
}

func newPathTable() *pathTable {
	return &pathTable{idx: make(map[string]uint32, 256)}
}

func (t *pathTable) intern(p string) uint32 {
	if i, ok := t.idx[p]; ok {
		return i
	}
	i := uint32(len(t.paths))
	t.paths = append(t.paths, p)
	t.idx[p] = i
	return i
}

func (t *pathTable) get(i uint32) string {
	return t.paths[int(i)]
}

// analyzeRowSrc is a (version, partPathIdx) tuple stored in the
// (sid, ts) → []analyzeRowSrc map. partPathIdx is an index into the
// scan's pathTable.
type analyzeRowSrc struct {
	version int64
	pathIdx uint32
}

// AnalyzeMissingRow names one row that exists in src but not in tgt
// after a migration copy: (sid, ts) appears more times physically in
// source than in target, and MissingVersions lists the version values
// that didn't survive into the target (sorted ascending).
type AnalyzeMissingRow struct {
	MissingVersions []VersionWithPath
	SourceVersions  []VersionWithPath
	TargetVersions  []VersionWithPath
	SeriesID        common.SeriesID
	Timestamp       int64
}

// AnalyzeDiffResult is the source-vs-target multiset diff for one
// (entry, group). It is filled in by AnalyzeGroupDiffWithTarget and
// answers the question "which exact rows did `migration copy` drop?".
type AnalyzeDiffResult struct {
	Missing     []AnalyzeMissingRow
	SourceRows  uint64
	TargetRows  uint64
	MissingRows uint64
	MissingKeys int
}

// AnalyzeGroupRows walks every <root>/seg-*/shard-*/<partID>/ in
// srcRoots, opens each part read-only, decodes only block metadata +
// timestamps (NOT tag/field bodies — keeps the scan cheap), and
// aggregates (seriesID, timestamp) keys with their versions.
//
// Returns:
//   - TotalRows     : every physical row encountered on disk
//   - UniqueKeys    : distinct (sid, ts) pairs
//   - DuplicateRows : TotalRows - UniqueKeys
//   - SamplesByVersion[0:sampleCap]: first few (sid, ts) keys that had
//     >1 row on disk, with all version values they carried.
//
// Memory: roughly 32 bytes per unique key + (versions × 8B). Cold-tier
// groups with hundreds of millions of unique keys can be too large
// for a 20 GiB pod — call AnalyzeGroupRows on a small (entry, group)
// pair as a sanity probe, not on every shard at once.
func AnalyzeGroupRows(srcRoots []string, fileSystem fs.FileSystem, sampleCap int) (AnalyzeGroupResult, error) {
	var res AnalyzeGroupResult
	keys := make(map[analyzeKey][]analyzeRowSrc, 1<<20)
	paths := newPathTable()

	// Pre-pass: open each part's metadata only (cheap) to learn the
	// expected total row count, so the 10s ticker can report a real %.
	expectedTotal, err := sumGroupExpectedRows(srcRoots, fileSystem)
	if err != nil {
		return res, fmt.Errorf("count expected rows: %w", err)
	}
	progressDone := startScanProgressReporter(&res.TotalRows, uint64(expectedTotal))
	defer close(progressDone)

	var (
		compressed []byte
		raw        []byte
		bms        []blockMetadata
		timestamps []int64
		versions   []int64
	)

	for _, root := range srcRoots {
		segEntries, err := os.ReadDir(root)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return res, fmt.Errorf("read src root %s: %w", root, err)
		}
		for _, se := range segEntries {
			if !se.IsDir() || !strings.HasPrefix(se.Name(), directCopySegPrefix) {
				continue
			}
			segDir := filepath.Join(root, se.Name())
			shardEntries, shardReadErr := os.ReadDir(segDir)
			if shardReadErr != nil {
				return res, fmt.Errorf("read src seg %s: %w", segDir, shardReadErr)
			}
			for _, sh := range shardEntries {
				if !sh.IsDir() || !strings.HasPrefix(sh.Name(), directCopyShardPrefix) {
					continue
				}
				shardDir := filepath.Join(segDir, sh.Name())
				partEntries, partsReadErr := os.ReadDir(shardDir)
				if partsReadErr != nil {
					return res, fmt.Errorf("read src shard %s: %w", shardDir, partsReadErr)
				}
				for _, pe := range partEntries {
					if !pe.IsDir() || !directCopyPartDirPattern.MatchString(pe.Name()) {
						continue
					}
					partID, parseErr := strconv.ParseUint(pe.Name(), 16, 64)
					if parseErr != nil {
						return res, fmt.Errorf("parse partID %s: %w", pe.Name(), parseErr)
					}
					partDupKeys := make(map[analyzeKey]int, 1<<12)
					partPath := filepath.Join(shardDir, pe.Name())
					rowsThisPart, boundaries, scanErr := scanOnePartIntoMap(
						partID, shardDir, partPath, fileSystem,
						keys, paths, partDupKeys, &res,
						&compressed, &raw, &bms, &timestamps, &versions,
					)
					if scanErr != nil {
						return res, scanErr
					}
					var partDups uint64
					for _, c := range partDupKeys {
						if c > 1 {
							partDups += uint64(c - 1)
						}
					}
					res.PerPartDupRows += partDups
					if partDups > 0 {
						res.PerPartDups = append(res.PerPartDups, AnalyzePerPart{
							PartID:         pe.Name(),
							SegName:        se.Name(),
							ShardName:      sh.Name(),
							SourceRoot:     root,
							Boundaries:     boundaries,
							Rows:           rowsThisPart,
							UniqueKeys:     uint64(len(partDupKeys)),
							PartialDupRows: partDups,
						})
					}
				}
			}
		}
	}

	res.UniqueKeys = uint64(len(keys))
	res.DuplicateRows = res.TotalRows - res.UniqueKeys

	// Single pass to count every (sid, ts) key with >1 row AND collect
	// up to sampleCap samples. Counting always; sampling stops at cap.
	for k, vs := range keys {
		if len(vs) <= 1 {
			continue
		}
		res.KeysWithDuplicates++
		if sampleCap > 0 && len(res.SamplesByVersion) < sampleCap {
			res.SamplesByVersion = append(res.SamplesByVersion, AnalyzeKeyMulti{
				SeriesID:  k.sid,
				Timestamp: k.ts,
				Versions:  rowSrcsToVersionsWithPath(vs, paths),
			})
		}
	}
	return res, nil
}

// rowSrcsToVersionsWithPath turns the internal (version, pathIdx)
// slice into the operator-facing []VersionWithPath, sorted by version
// ascending. Used by every CLI emission so output stays consistent.
func rowSrcsToVersionsWithPath(rs []analyzeRowSrc, paths *pathTable) []VersionWithPath {
	out := make([]VersionWithPath, len(rs))
	for i, r := range rs {
		out[i] = VersionWithPath{Version: r.version, Path: paths.get(r.pathIdx)}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Version < out[j].Version })
	return out
}

// scanOnePartIntoMap opens one part, walks every block in on-disk
// order, and:
//
//   - merges every row's (version, partPath) under its (seriesID,
//     timestamp) key in keys (global, across all parts). The path is
//     interned via the shared pathTable so per-row overhead stays at
//     12B regardless of how long the path is.
//   - increments partDupKeys per-part so the caller derives the
//     within-part dedup count
//   - records every boundary collision (block N ends at ts=T,
//     block N+1 begins at ts=T, same series) — these are exactly
//     the rows the merger-fast-path bug at merger.go:306 emits
//
// Returns rowsInPart, boundaries, error.
func scanOnePartIntoMap(
	partID uint64,
	shardDir, partPath string,
	fileSystem fs.FileSystem,
	keys map[analyzeKey][]analyzeRowSrc,
	paths *pathTable,
	partDupKeys map[analyzeKey]int,
	res *AnalyzeGroupResult,
	compressed, raw *[]byte,
	bms *[]blockMetadata,
	timestamps, versions *[]int64,
) (partRows uint64, boundaries []AnalyzeBoundaryPair, err error) {
	var p *part
	defer func() {
		if p != nil {
			p.close()
		}
		if r := recover(); r != nil {
			err = fmt.Errorf("scan part %016x panicked: %v", partID, r)
		}
	}()
	p = mustOpenFilePart(partID, shardDir, fileSystem)
	res.PartsScanned++

	blockIdx := 0
	var prevSet bool
	var prevSid common.SeriesID
	var prevTS, prevVersion int64
	var prevBlockIdx int

	for i := range p.primaryBlockMetadata {
		pbm := &p.primaryBlockMetadata[i]
		*compressed = bytes.ResizeOver(*compressed, int(pbm.size))
		fs.MustReadData(p.primary, int64(pbm.offset), *compressed)
		var err error
		*raw, err = zstd.Decompress((*raw)[:0], *compressed)
		if err != nil {
			return partRows, boundaries, fmt.Errorf("decompress primary block: %w", err)
		}
		*bms, err = unmarshalBlockMetadata((*bms)[:0], *raw)
		if err != nil {
			return partRows, boundaries, fmt.Errorf("unmarshal block metadata: %w", err)
		}
		for j := range *bms {
			bm := &(*bms)[j]
			*timestamps, *versions = mustReadTimestampsFrom((*timestamps)[:0], (*versions)[:0], &bm.timestamps, int(bm.count), p.timestamps)
			n := int(bm.count)
			if n == 0 {
				blockIdx++
				continue
			}
			if prevSet && bm.seriesID == prevSid && (*timestamps)[0] == prevTS {
				boundaries = append(boundaries, AnalyzeBoundaryPair{
					SeriesID:  prevSid,
					Timestamp: prevTS,
					VersionA:  prevVersion,
					VersionB:  (*versions)[0],
					BlockA:    prevBlockIdx,
					BlockB:    blockIdx,
				})
			}
			partPathIdx := paths.intern(partPath)
			for k := 0; k < n; k++ {
				ak := analyzeKey{sid: bm.seriesID, ts: (*timestamps)[k]}
				keys[ak] = append(keys[ak], analyzeRowSrc{version: (*versions)[k], pathIdx: partPathIdx})
				partDupKeys[ak]++
				atomic.AddUint64(&res.TotalRows, 1)
				partRows++
			}
			prevSet = true
			prevSid = bm.seriesID
			prevTS = (*timestamps)[n-1]
			prevVersion = (*versions)[n-1]
			prevBlockIdx = blockIdx
			blockIdx++
		}
	}
	return partRows, boundaries, nil
}

// AnalyzeGroupDiffWithTarget walks both source roots and the target
// group root, builds a (sid, ts) → []version multiset for each side,
// and returns the rows that exist in src but NOT in target — i.e. the
// exact rows `migration copy` dropped at slow-path bucket-flush time.
//
// For each (sid, ts) where len(src.versions) > len(tgt.versions), the
// missing version values are computed via multiset subtraction (every
// version in tgt cancels one identical entry in src; anything left in
// src is reported as missing). MissingRows summed should equal
// `verify`'s src-tgt row diff for the same (entry, group).
//
// Memory: ~2x of AnalyzeGroupRows since both src and tgt maps are
// held concurrently. Use on small/medium entries.
func AnalyzeGroupDiffWithTarget(srcRoots []string, targetGroupRoot string, fileSystem fs.FileSystem, sampleCap int) (AnalyzeDiffResult, error) {
	var res AnalyzeDiffResult

	srcParts, srcPathByID, err := openAllPartsInRoots(srcRoots, fileSystem)
	if err != nil {
		return res, fmt.Errorf("open src parts: %w", err)
	}
	defer closeAllParts(srcParts)
	tgtParts, tgtPathByID, err := openAllPartsInRoots([]string{targetGroupRoot}, fileSystem)
	if err != nil {
		return res, fmt.Errorf("open tgt parts: %w", err)
	}
	defer closeAllParts(tgtParts)

	var srcTotal, tgtTotal int64
	for _, p := range srcParts {
		srcTotal += int64(p.partMetadata.TotalCount)
	}
	for _, p := range tgtParts {
		tgtTotal += int64(p.partMetadata.TotalCount)
	}

	srcStream, srcCleanup := newSidGroupStream(srcParts, srcPathByID)
	defer srcCleanup()
	tgtStream, tgtCleanup := newSidGroupStream(tgtParts, tgtPathByID)
	defer tgtCleanup()

	progressDone := startDiffProgressReporter(&srcStream.rowsRead, &tgtStream.rowsRead, srcTotal, tgtTotal)
	defer close(progressDone)

	// blockReader emits blocks in (seriesID asc, minTimestamp asc) order
	// but does NOT row-sort by ts WITHIN a seriesID when that series
	// spans overlapping ts ranges across parts. So we buffer one whole
	// seriesID's worth of rows from each side, sort by ts, then
	// row-merge-diff at ts granularity — same algorithm as before but
	// guaranteed sorted within each (sid, ts).
	srcGrp, srcOK := srcStream.nextGroup()
	tgtGrp, tgtOK := tgtStream.nextGroup()
	for srcOK || tgtOK {
		var cmpSid int
		switch {
		case !srcOK:
			cmpSid = +1
		case !tgtOK:
			cmpSid = -1
		case srcGrp.sid < tgtGrp.sid:
			cmpSid = -1
		case srcGrp.sid > tgtGrp.sid:
			cmpSid = +1
		default:
			cmpSid = 0
		}
		switch {
		case cmpSid < 0:
			absorbSrcOnlyGroup(srcGrp, sampleCap, &res)
			srcGrp, srcOK = srcStream.nextGroup()
		case cmpSid > 0:
			res.TargetRows += uint64(len(tgtGrp.rows))
			tgtGrp, tgtOK = tgtStream.nextGroup()
		default:
			diffSidGroup(srcGrp, tgtGrp, sampleCap, &res)
			srcGrp, srcOK = srcStream.nextGroup()
			tgtGrp, tgtOK = tgtStream.nextGroup()
		}
	}
	if err := srcStream.err(); err != nil {
		return res, fmt.Errorf("src block reader: %w", err)
	}
	if err := tgtStream.err(); err != nil {
		return res, fmt.Errorf("tgt block reader: %w", err)
	}
	return res, nil
}

// absorbSrcOnlyGroup records every src row of a series whose sid does
// not exist on the target side: each (sid, ts) bucket of src becomes
// a missing entry, with all its versions reported.
func absorbSrcOnlyGroup(g *sidGroup, sampleCap int, res *AnalyzeDiffResult) {
	res.SourceRows += uint64(len(g.rows))
	i := 0
	for i < len(g.rows) {
		ts := g.rows[i].ts
		j := i
		for j < len(g.rows) && g.rows[j].ts == ts {
			j++
		}
		srcVs := make([]VersionWithPath, 0, j-i)
		for k := i; k < j; k++ {
			srcVs = append(srcVs, VersionWithPath{Version: g.rows[k].version, Path: g.rows[k].partPath})
		}
		res.MissingKeys++
		res.MissingRows += uint64(len(srcVs))
		if sampleCap == 0 || len(res.Missing) < sampleCap {
			sortVersionsAsc(srcVs)
			missingCopy := append([]VersionWithPath(nil), srcVs...)
			res.Missing = append(res.Missing, AnalyzeMissingRow{
				SeriesID:        g.sid,
				Timestamp:       ts,
				SourceVersions:  srcVs,
				TargetVersions:  nil,
				MissingVersions: missingCopy,
			})
		}
		i = j
	}
}

// diffSidGroup runs the ts-by-ts multiset subtraction for one
// matching (sid). Both groups are already ts-sorted; walk them in
// merge fashion.
func diffSidGroup(srcGrp, tgtGrp *sidGroup, sampleCap int, res *AnalyzeDiffResult) {
	si, ti := 0, 0
	for si < len(srcGrp.rows) || ti < len(tgtGrp.rows) {
		var cmpTS int
		switch {
		case si >= len(srcGrp.rows):
			cmpTS = +1
		case ti >= len(tgtGrp.rows):
			cmpTS = -1
		case srcGrp.rows[si].ts < tgtGrp.rows[ti].ts:
			cmpTS = -1
		case srcGrp.rows[si].ts > tgtGrp.rows[ti].ts:
			cmpTS = +1
		default:
			cmpTS = 0
		}
		switch {
		case cmpTS < 0:
			ts := srcGrp.rows[si].ts
			start := si
			for si < len(srcGrp.rows) && srcGrp.rows[si].ts == ts {
				si++
			}
			srcVs := make([]VersionWithPath, 0, si-start)
			for k := start; k < si; k++ {
				srcVs = append(srcVs, VersionWithPath{Version: srcGrp.rows[k].version, Path: srcGrp.rows[k].partPath})
			}
			res.SourceRows += uint64(len(srcVs))
			res.MissingKeys++
			res.MissingRows += uint64(len(srcVs))
			if sampleCap == 0 || len(res.Missing) < sampleCap {
				sortVersionsAsc(srcVs)
				missingCopy := append([]VersionWithPath(nil), srcVs...)
				res.Missing = append(res.Missing, AnalyzeMissingRow{
					SeriesID:        srcGrp.sid,
					Timestamp:       ts,
					SourceVersions:  srcVs,
					TargetVersions:  nil,
					MissingVersions: missingCopy,
				})
			}
		case cmpTS > 0:
			ts := tgtGrp.rows[ti].ts
			for ti < len(tgtGrp.rows) && tgtGrp.rows[ti].ts == ts {
				res.TargetRows++
				ti++
			}
		default:
			ts := srcGrp.rows[si].ts
			startSrc := si
			for si < len(srcGrp.rows) && srcGrp.rows[si].ts == ts {
				si++
			}
			startTgt := ti
			for ti < len(tgtGrp.rows) && tgtGrp.rows[ti].ts == ts {
				ti++
			}
			srcVs := make([]VersionWithPath, 0, si-startSrc)
			for k := startSrc; k < si; k++ {
				srcVs = append(srcVs, VersionWithPath{Version: srcGrp.rows[k].version, Path: srcGrp.rows[k].partPath})
			}
			tgtVs := make([]VersionWithPath, 0, ti-startTgt)
			for k := startTgt; k < ti; k++ {
				tgtVs = append(tgtVs, VersionWithPath{Version: tgtGrp.rows[k].version, Path: tgtGrp.rows[k].partPath})
			}
			res.SourceRows += uint64(len(srcVs))
			res.TargetRows += uint64(len(tgtVs))
			if len(tgtVs) >= len(srcVs) {
				continue
			}
			tgtCount := make(map[int64]int, len(tgtVs))
			for _, t := range tgtVs {
				tgtCount[t.Version]++
			}
			var missing []VersionWithPath
			for _, s := range srcVs {
				if tgtCount[s.Version] > 0 {
					tgtCount[s.Version]--
					continue
				}
				missing = append(missing, s)
			}
			if len(missing) == 0 {
				continue
			}
			res.MissingKeys++
			res.MissingRows += uint64(len(missing))
			if sampleCap == 0 || len(res.Missing) < sampleCap {
				sortVersionsAsc(srcVs)
				sortVersionsAsc(tgtVs)
				sortVersionsAsc(missing)
				res.Missing = append(res.Missing, AnalyzeMissingRow{
					SeriesID:        srcGrp.sid,
					Timestamp:       ts,
					SourceVersions:  srcVs,
					TargetVersions:  tgtVs,
					MissingVersions: missing,
				})
			}
		}
	}
}

// sidGroup is one seriesID's full row set (across all parts), with
// rows sorted by timestamp ascending. Built by sidGroupStream.
type sidGroup struct {
	rows []sidRow
	sid  common.SeriesID
}

type sidRow struct {
	partPath string
	ts       int64
	version  int64
}

func sortVersionsAsc(vs []VersionWithPath) {
	sort.Slice(vs, func(i, j int) bool { return vs[i].Version < vs[j].Version })
}

// openAllPartsInRoots enumerates every part dir under the given roots,
// opens each via mustOpenFilePart, and returns the *part slice + a
// partID → full path map. Caller must closeAllParts when done.
//
// fileSystem is passed through to mustOpenFilePart. Directory walking
// goes through the os package directly: fs.FileSystem's ReadDir panics
// on missing directories, while this walker must skip roots that don't
// exist on every PVC (hash-shard sparsity). The abstraction is
// intentionally bypassed for the enumeration path.
func openAllPartsInRoots(roots []string, fileSystem fs.FileSystem) ([]*part, map[uint64]string, error) {
	var parts []*part
	pathByID := make(map[uint64]string, 64)
	for _, root := range roots {
		segEntries, err := os.ReadDir(root)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			closeAllParts(parts)
			return nil, nil, fmt.Errorf("read root %s: %w", root, err)
		}
		for _, se := range segEntries {
			if !se.IsDir() || !strings.HasPrefix(se.Name(), directCopySegPrefix) {
				continue
			}
			segDir := filepath.Join(root, se.Name())
			shardEntries, readErr := os.ReadDir(segDir)
			if readErr != nil {
				closeAllParts(parts)
				return nil, nil, fmt.Errorf("read seg %s: %w", segDir, readErr)
			}
			for _, sh := range shardEntries {
				if !sh.IsDir() || !strings.HasPrefix(sh.Name(), directCopyShardPrefix) {
					continue
				}
				shardDir := filepath.Join(segDir, sh.Name())
				partEntries, partsReadErr := os.ReadDir(shardDir)
				if partsReadErr != nil {
					closeAllParts(parts)
					return nil, nil, fmt.Errorf("read shard %s: %w", shardDir, partsReadErr)
				}
				for _, pe := range partEntries {
					if !pe.IsDir() || !directCopyPartDirPattern.MatchString(pe.Name()) {
						continue
					}
					partID, parseErr := strconv.ParseUint(pe.Name(), 16, 64)
					if parseErr != nil {
						closeAllParts(parts)
						return nil, nil, fmt.Errorf("parse partID %s: %w", pe.Name(), parseErr)
					}
					p := mustOpenFilePart(partID, shardDir, fileSystem)
					parts = append(parts, p)
					pathByID[p.partMetadata.ID] = filepath.Join(shardDir, pe.Name())
				}
			}
		}
	}
	return parts, pathByID, nil
}

func closeAllParts(parts []*part) {
	for _, p := range parts {
		if p != nil {
			p.close()
		}
	}
}

// sidGroupStream walks partMergeIter+blockReader on one side, but
// only exposes rows in (sid) groups: each NextGroup call drains every
// block that shares the current seriesID and returns them sorted by
// timestamp ascending. That's the level of sortedness diff needs to
// run multiset subtraction correctly (blockReader's order is
// (sid asc, block.minTs asc) — overlapping blocks within a sid are
// NOT row-ts-sorted, which is why the previous row-stream impl
// double-counted "missing" rows).
//
// rowsRead is atomic-summed across every row emitted so the
// progress reporter goroutine can report scan %.
type sidGroupStream struct {
	reader       *blockReader
	decoder      *encoding.BytesBlockDecoder
	pathByPartID map[uint64]string
	iters        []*partMergeIter

	rowsRead int64

	primed   bool // first nextBlockMetadata has been called
	finished bool
}

func newSidGroupStream(parts []*part, pathByPartID map[uint64]string) (*sidGroupStream, func()) {
	iters := make([]*partMergeIter, 0, len(parts))
	for _, p := range parts {
		pmi := generatePartMergeIter()
		pmi.mustInitFromPart(p)
		iters = append(iters, pmi)
	}
	decoder := generateColumnValuesDecoder()
	reader := generateBlockReader()
	reader.init(iters)
	s := &sidGroupStream{
		reader:       reader,
		iters:        iters,
		decoder:      decoder,
		pathByPartID: pathByPartID,
	}
	cleanup := func() {
		releaseBlockReader(reader)
		releaseColumnValuesDecoder(decoder)
		for _, it := range iters {
			releasePartMergeIter(it)
		}
	}
	return s, cleanup
}

func (s *sidGroupStream) err() error {
	return s.reader.error()
}

// nextGroup buffers every block whose seriesID matches the first
// pending block's sid, sorts the row collection by ts ascending, and
// returns it. Returns (nil, false) on EOF.
func (s *sidGroupStream) nextGroup() (*sidGroup, bool) {
	if s.finished {
		return nil, false
	}
	if !s.primed {
		if !s.reader.nextBlockMetadata() {
			s.finished = true
			return nil, false
		}
		s.primed = true
	}
	grp := &sidGroup{sid: s.reader.block.bm.seriesID}
	for {
		// reader.block is positioned at a block whose sid we may want
		// to consume. If sid changed, leave it for the next call.
		if s.reader.block.bm.seriesID != grp.sid {
			break
		}
		s.reader.loadBlockData(s.decoder)
		path := s.pathByPartID[s.reader.pih[0].partID]
		n := int(s.reader.block.bm.count)
		for k := 0; k < n; k++ {
			grp.rows = append(grp.rows, sidRow{
				ts:       s.reader.block.timestamps[k],
				version:  s.reader.block.versions[k],
				partPath: path,
			})
		}
		atomic.AddInt64(&s.rowsRead, int64(n))
		if !s.reader.nextBlockMetadata() {
			s.finished = true
			break
		}
	}
	sort.Slice(grp.rows, func(i, j int) bool { return grp.rows[i].ts < grp.rows[j].ts })
	return grp, true
}

// startDiffProgressReporter spawns a goroutine that, every 10
// seconds, prints how many rows each side has scanned + the
// percentage against the partMetadata-derived total. Returns a
// channel; close it (or the goroutine returns at total completion)
// to stop the reporter. Output goes to stderr so it doesn't mix
// with the structured stdout payload.
func startDiffProgressReporter(srcCounter, tgtCounter *int64, srcTotal, tgtTotal int64) chan struct{} {
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		start := time.Now()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				sc := atomic.LoadInt64(srcCounter)
				tc := atomic.LoadInt64(tgtCounter)
				srcPct := safePct(sc, srcTotal)
				tgtPct := safePct(tc, tgtTotal)
				fmt.Fprintf(os.Stderr,
					"[diff %5s] src %d/%d (%.1f%%)  tgt %d/%d (%.1f%%)\n",
					time.Since(start).Truncate(time.Second),
					sc, srcTotal, srcPct, tc, tgtTotal, tgtPct,
				)
			}
		}
	}()
	return done
}

func safePct(part, total int64) float64 {
	if total <= 0 {
		return 0
	}
	return 100.0 * float64(part) / float64(total)
}

func safePctU(part, total uint64) float64 {
	if total == 0 {
		return 0
	}
	return 100.0 * float64(part) / float64(total)
}

// startScanProgressReporter is the single-side variant of
// startDiffProgressReporter — used by AnalyzeGroupRows where only
// the source tree is scanned. Prints to stderr every 10s.
func startScanProgressReporter(counter *uint64, total uint64) chan struct{} {
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		start := time.Now()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				c := atomic.LoadUint64(counter)
				pct := safePctU(c, total)
				fmt.Fprintf(os.Stderr,
					"[scan %5s] %d/%d rows (%.1f%%)\n",
					time.Since(start).Truncate(time.Second),
					c, total, pct,
				)
			}
		}
	}()
	return done
}

// sumGroupExpectedRows walks every <root>/seg-*/shard-*/<partID>/
// directory under roots and sums partMetadata.TotalCount — used by
// AnalyzeGroupRows to size the progress-reporter's % calculation
// without holding all parts open. Each part metadata read is a small
// file read, so this pre-pass is cheap (~ms per part).
func sumGroupExpectedRows(roots []string, fileSystem fs.FileSystem) (int64, error) {
	var total int64
	for _, root := range roots {
		segEntries, err := os.ReadDir(root)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return 0, fmt.Errorf("read root %s: %w", root, err)
		}
		for _, se := range segEntries {
			if !se.IsDir() || !strings.HasPrefix(se.Name(), directCopySegPrefix) {
				continue
			}
			segDir := filepath.Join(root, se.Name())
			shardEntries, readErr := os.ReadDir(segDir)
			if readErr != nil {
				return 0, fmt.Errorf("read seg %s: %w", segDir, readErr)
			}
			for _, sh := range shardEntries {
				if !sh.IsDir() || !strings.HasPrefix(sh.Name(), directCopyShardPrefix) {
					continue
				}
				shardDir := filepath.Join(segDir, sh.Name())
				partEntries, partsReadErr := os.ReadDir(shardDir)
				if partsReadErr != nil {
					return 0, fmt.Errorf("read shard %s: %w", shardDir, partsReadErr)
				}
				for _, pe := range partEntries {
					if !pe.IsDir() || !directCopyPartDirPattern.MatchString(pe.Name()) {
						continue
					}
					partDir := filepath.Join(shardDir, pe.Name())
					var pm partMetadata
					pm.mustReadMetadata(fileSystem, partDir)
					total += int64(pm.TotalCount)
				}
			}
		}
	}
	return total, nil
}
