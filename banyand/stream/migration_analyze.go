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
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/bytes"
	"github.com/apache/skywalking-banyandb/pkg/compress/zstd"
	"github.com/apache/skywalking-banyandb/pkg/fs"
)

// AnalyzeDiffResult is the source-vs-target multiset diff for stream.
// Stream never deduplicates, so the diff should always be zero in a correct
// migration. Any non-zero MissingRows or ExtraRows indicates a true bug.
type AnalyzeDiffResult struct {
	Missing     []AnalyzeMissingRow
	Extra       []AnalyzeExtraRow
	SourceRows  uint64
	TargetRows  uint64
	MissingRows uint64
	MissingKeys int
	ExtraRows   uint64
	ExtraKeys   int
}

// AnalyzeMissingRow names one (seriesID, ts, elementID) tuple that
// exists in source but not in target after a migration copy.
type AnalyzeMissingRow struct {
	PartPath  string
	SeriesID  common.SeriesID
	Timestamp int64
	ElementID uint64
}

// AnalyzeExtraRow names one (seriesID, ts, elementID) tuple that
// appears in target more times than in source — indicating a duplicate
// incorrectly introduced by the copy.
type AnalyzeExtraRow struct {
	PartPath  string
	SeriesID  common.SeriesID
	Timestamp int64
	ElementID uint64
}

// streamAnalyzeKey is the per-row identity key for stream: no version field
// since stream never deduplicates — same (sid, ts) with different elementIDs
// are all distinct rows.
type streamAnalyzeKey struct {
	sid       common.SeriesID
	ts        int64
	elementID uint64
}

// AnalyzeGroupResult is the per-group output of AnalyzeGroupRows, which walks
// every source part under srcRoots and decodes only block metadata +
// timestamps + elementIDs (NOT tag bodies — cheap scan). For stream,
// TotalRows == UniqueKeys always (no dedup); any MissingRows in a src-vs-tgt
// diff is a real bug.
type AnalyzeGroupResult struct {
	PartsScanned uint64
	TotalRows    uint64
	UniqueKeys   uint64
}

// walkStreamGroupParts walks the 3-level seg-*/shard-*/<partID> directory tree
// under each root, applying the standard prefix/pattern guards, and invokes fn
// for every valid part. Iteration stops and the error is returned on the first
// fn failure or directory-read failure. Non-existent roots are silently skipped.
func walkStreamGroupParts(roots []string, fn func(shardDir, partPath string, partID uint64) error) error {
	for _, root := range roots {
		segEntries, err := os.ReadDir(root)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return fmt.Errorf("read root %s: %w", root, err)
		}
		for _, se := range segEntries {
			if !se.IsDir() || !strings.HasPrefix(se.Name(), directStreamCopySegPrefix) {
				continue
			}
			segDir := filepath.Join(root, se.Name())
			shardEntries, shardReadErr := os.ReadDir(segDir)
			if shardReadErr != nil {
				return fmt.Errorf("read seg %s: %w", segDir, shardReadErr)
			}
			for _, sh := range shardEntries {
				if !sh.IsDir() || !strings.HasPrefix(sh.Name(), directStreamCopyShardPrefix) {
					continue
				}
				shardDir := filepath.Join(segDir, sh.Name())
				partEntries, partsReadErr := os.ReadDir(shardDir)
				if partsReadErr != nil {
					return fmt.Errorf("read shard %s: %w", shardDir, partsReadErr)
				}
				for _, pe := range partEntries {
					if !pe.IsDir() || !directStreamCopyPartDirPattern.MatchString(pe.Name()) {
						continue
					}
					partID, parseErr := strconv.ParseUint(pe.Name(), 16, 64)
					if parseErr != nil {
						return fmt.Errorf("parse partID %s: %w", pe.Name(), parseErr)
					}
					partPath := filepath.Join(shardDir, pe.Name())
					if fnErr := fn(shardDir, partPath, partID); fnErr != nil {
						return fnErr
					}
				}
			}
		}
	}
	return nil
}

// AnalyzeGroupRows scans all source parts and counts rows.
func AnalyzeGroupRows(srcRoots []string, fileSystem fs.FileSystem) (AnalyzeGroupResult, error) {
	var res AnalyzeGroupResult

	var (
		compressed []byte
		raw        []byte
		bms        []blockMetadata
	)

	err := walkStreamGroupParts(srcRoots, func(shardDir, _ string, partID uint64) error {
		rows, scanErr := scanOneStreamPartRows(
			partID, shardDir, fileSystem,
			&compressed, &raw, &bms,
		)
		if scanErr != nil {
			return scanErr
		}
		atomic.AddUint64(&res.TotalRows, rows)
		atomic.AddUint64(&res.PartsScanned, 1)
		return nil
	})
	if err == nil {
		res.UniqueKeys = res.TotalRows // stream: every row is unique (no dedup)
	}
	return res, err
}

// scanOneStreamPartRows opens one part, walks primary block metadata, and
// returns the total row count.
func scanOneStreamPartRows(
	partID uint64,
	shardDir string,
	fileSystem fs.FileSystem,
	compressed, raw *[]byte,
	bms *[]blockMetadata,
) (partRows uint64, err error) {
	// The recover defer must be registered BEFORE p.close() is deferred so a
	// panic from close() itself is still caught.
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("scan part %016x panicked: %v", partID, r)
		}
	}()
	p := mustOpenFilePart(partID, shardDir, fileSystem)
	defer p.close()

	for i := range p.primaryBlockMetadata {
		pbm := &p.primaryBlockMetadata[i]
		*compressed = bytes.ResizeOver(*compressed, int(pbm.size))
		fs.MustReadData(p.primary, int64(pbm.offset), *compressed)
		var decErr error
		*raw, decErr = zstd.Decompress((*raw)[:0], *compressed)
		if decErr != nil {
			return partRows, fmt.Errorf("decompress primary block: %w", decErr)
		}
		*bms, decErr = unmarshalBlockMetadata((*bms)[:0], *raw)
		if decErr != nil {
			return partRows, fmt.Errorf("unmarshal block metadata: %w", decErr)
		}
		for j := range *bms {
			bm := &(*bms)[j]
			// We only need the count — no need to read timestamps for row counting.
			partRows += bm.count
		}
	}
	return partRows, nil
}

// AnalyzeGroupDiffWithTarget builds a (seriesID, ts, elementID) multiset
// for source and target, then computes the set diff. For a correct stream
// migration the diff is always zero. Any missing row is a true bug.
func AnalyzeGroupDiffWithTarget(
	srcRoots []string,
	targetGroupRoot string,
	fileSystem fs.FileSystem,
	sampleCap int,
) (AnalyzeDiffResult, error) {
	var res AnalyzeDiffResult
	srcKeys, srcErr := buildStreamKeyMultiset(srcRoots, fileSystem)
	if srcErr != nil {
		return res, fmt.Errorf("build src multiset: %w", srcErr)
	}
	tgtKeys, tgtErr := buildStreamKeyMultiset([]string{targetGroupRoot}, fileSystem)
	if tgtErr != nil {
		return res, fmt.Errorf("build tgt multiset: %w", tgtErr)
	}

	for k, srcPaths := range srcKeys {
		res.SourceRows += uint64(len(srcPaths))
		tgtPaths, inTgt := tgtKeys[k]
		switch {
		case !inTgt || len(tgtPaths) == 0:
			res.MissingRows += uint64(len(srcPaths))
			res.MissingKeys++
			if sampleCap == 0 || len(res.Missing) < sampleCap {
				res.Missing = append(res.Missing, AnalyzeMissingRow{
					SeriesID:  k.sid,
					Timestamp: k.ts,
					ElementID: k.elementID,
					PartPath:  srcPaths[0],
				})
			}
		case len(tgtPaths) > len(srcPaths):
			extra := uint64(len(tgtPaths) - len(srcPaths))
			res.ExtraRows += extra
			res.ExtraKeys++
			if sampleCap == 0 || len(res.Extra) < sampleCap {
				res.Extra = append(res.Extra, AnalyzeExtraRow{
					SeriesID:  k.sid,
					Timestamp: k.ts,
					ElementID: k.elementID,
					PartPath:  tgtPaths[0],
				})
			}
		case len(tgtPaths) < len(srcPaths):
			// Streams never deduplicate: a key occurring N times in the source
			// must occur N times in the target, so a lower count is row loss.
			res.MissingRows += uint64(len(srcPaths) - len(tgtPaths))
			res.MissingKeys++
			if sampleCap == 0 || len(res.Missing) < sampleCap {
				res.Missing = append(res.Missing, AnalyzeMissingRow{
					SeriesID:  k.sid,
					Timestamp: k.ts,
					ElementID: k.elementID,
					PartPath:  srcPaths[0],
				})
			}
		}
	}
	for k, tgtPaths := range tgtKeys {
		res.TargetRows += uint64(len(tgtPaths))
		// Detect keys that are only in target (not in source at all).
		if _, inSrc := srcKeys[k]; !inSrc {
			res.ExtraRows += uint64(len(tgtPaths))
			res.ExtraKeys++
			if sampleCap == 0 || len(res.Extra) < sampleCap {
				res.Extra = append(res.Extra, AnalyzeExtraRow{
					SeriesID:  k.sid,
					Timestamp: k.ts,
					ElementID: k.elementID,
					PartPath:  tgtPaths[0],
				})
			}
		}
	}
	return res, nil
}

// buildStreamKeyMultiset enumerates all parts under roots, reads
// timestamps + elementIDs per block, and returns a
// (seriesID, ts, elementID) → []partPath multiset.
func buildStreamKeyMultiset(
	roots []string,
	fileSystem fs.FileSystem,
) (map[streamAnalyzeKey][]string, error) {
	keys := make(map[streamAnalyzeKey][]string, 1<<16)

	var (
		compressed []byte
		raw        []byte
		bms        []blockMetadata
	)

	err := walkStreamGroupParts(roots, func(shardDir, partPath string, partID uint64) error {
		return scanOneStreamPartIntoMap(
			partID, shardDir, partPath, fileSystem,
			keys, &compressed, &raw, &bms,
		)
	})
	if err != nil {
		return nil, err
	}
	return keys, nil
}

// scanOneStreamPartIntoMap opens one part and merges every row's
// (seriesID, ts, elementID) into the provided map.
func scanOneStreamPartIntoMap(
	partID uint64,
	shardDir, partPath string,
	fileSystem fs.FileSystem,
	keys map[streamAnalyzeKey][]string,
	compressed, raw *[]byte,
	bms *[]blockMetadata,
) (err error) {
	// The recover defer must be registered BEFORE p.close() is deferred so a
	// panic from close() itself is still caught.
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("scan part %016x panicked: %v", partID, r)
		}
	}()
	p := mustOpenFilePart(partID, shardDir, fileSystem)
	defer p.close()

	for i := range p.primaryBlockMetadata {
		pbm := &p.primaryBlockMetadata[i]
		*compressed = bytes.ResizeOver(*compressed, int(pbm.size))
		fs.MustReadData(p.primary, int64(pbm.offset), *compressed)
		var decErr error
		*raw, decErr = zstd.Decompress((*raw)[:0], *compressed)
		if decErr != nil {
			return fmt.Errorf("decompress primary block: %w", decErr)
		}
		*bms, decErr = unmarshalBlockMetadata((*bms)[:0], *raw)
		if decErr != nil {
			return fmt.Errorf("unmarshal block metadata: %w", decErr)
		}
		for j := range *bms {
			bm := &(*bms)[j]
			// Read timestamps and elementIDs for this block.
			var ts []int64
			var elementIDs []uint64
			ts, elementIDs = mustReadTimestampsFrom(ts, elementIDs, &bm.timestamps, int(bm.count), p.timestamps)
			for k := range ts {
				ak := streamAnalyzeKey{sid: bm.seriesID, ts: ts[k], elementID: elementIDs[k]}
				keys[ak] = append(keys[ak], partPath)
			}
		}
	}
	return nil
}
