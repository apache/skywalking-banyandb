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
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/inverted"
)

func writeFile(t *testing.T, p string, body []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, body, 0o600); err != nil {
		t.Fatal(err)
	}
}

func partJSON(comp, uncomp, count uint64, minTS, maxTS int64) []byte {
	// Trace's reader takes the uncompressed size from uncompressedSpanSizeBytes, the others
	// from uncompressedSizeBytes; each ignores the other key.
	return []byte(`{"compressedSizeBytes":` + strconv.FormatUint(comp, 10) +
		`,"uncompressedSizeBytes":` + strconv.FormatUint(uncomp, 10) +
		`,"uncompressedSpanSizeBytes":` + strconv.FormatUint(uncomp, 10) +
		`,"totalCount":` + strconv.FormatUint(count, 10) +
		`,"blocksCount":1,"minTimestamp":` + strconv.FormatInt(minTS, 10) +
		`,"maxTimestamp":` + strconv.FormatInt(maxTS, 10) + `}`)
}

func TestStatTSDBGroup_AggregatesShardsAndSegments(t *testing.T) {
	group := filepath.Join(t.TempDir(), "sw_record")
	seg := filepath.Join(group, "seg-20260928")
	writeFile(t, filepath.Join(seg, "metadata"), []byte(`{"version":"1.5.0"}`))
	writeFile(t, filepath.Join(seg, "sidx", "000000.seg"), make([]byte, 100))
	writeFile(t, filepath.Join(seg, "shard-0", "0000000000000001", "metadata.json"), partJSON(10, 40, 7, 100, 200))
	writeFile(t, filepath.Join(seg, "shard-0", "0000000000000002", "metadata.json"), partJSON(20, 80, 3, 50, 300))
	writeFile(t, filepath.Join(seg, "shard-0", "idx", "x"), make([]byte, 16)) // stream element index: bytes, not a part
	writeFile(t, filepath.Join(seg, "shard-1", "0000000000000003", "metadata.json"), partJSON(1, 2, 1, 400, 500))
	// A segment without shards (only metadata) must still be reported.
	seg2 := filepath.Join(group, "seg-20260927")
	writeFile(t, filepath.Join(seg2, "metadata"), []byte("1.4.0"))
	writeFile(t, filepath.Join(group, "not-a-segment"), []byte("ignored"))
	// Only day and hour suffixes are segments; a backup copy or a rollover scratch is not.
	for _, name := range []string{"seg-bak", "seg-2026092", "seg-202609281", "seg-20260928.tmp"} {
		writeFile(t, filepath.Join(group, name, "metadata"), []byte(`{"version":"1.5.0"}`))
	}

	units, err := statTSDBGroup(group, commonv1.Catalog_CATALOG_STREAM, "sw_record", false, testPartReader(commonv1.Catalog_CATALOG_STREAM))
	if err != nil {
		t.Fatal(err)
	}
	if len(units) != 2 {
		t.Fatalf("want 2 segments, got %d", len(units))
	}
	if units[0].GetSegment().Unit.SegmentSuffix != "20260927" || units[1].GetSegment().Unit.SegmentSuffix != "20260928" {
		t.Fatalf("segments not sorted: %q %q", units[0].GetSegment().Unit.SegmentSuffix, units[1].GetSegment().Unit.SegmentSuffix)
	}
	u := units[1].GetSegment()
	if u.SegmentVersion != "1.5.0" || u.SegmentLevel.GetEstimatedBytes() != 100 || u.SegmentLevel.GetDocCount() != 0 {
		t.Fatalf("segment-level stats wrong: %+v", u.SegmentLevel)
	}
	if len(u.Unit.ShardIds) != 2 || u.Unit.ShardIds[0] != 0 || u.Unit.ShardIds[1] != 1 {
		t.Fatalf("shard ids = %v", u.Unit.ShardIds)
	}
	s0 := u.Shards[0]
	if s0.PartsCount != 2 || s0.TotalCount != 10 || s0.EstimatedCompressedBytes != 30+16 || s0.EstimatedUncompressedBytes != 120+16 ||
		s0.MinTimestamp != 50 || s0.MaxTimestamp != 300 || len(s0.Parts) != 2 || s0.Parts[0].Id != 1 || s0.Parts[1].Id != 2 {
		t.Fatalf("shard-0 stats must include the element index bytes: %+v", s0)
	}
	if units[0].GetSegment().SegmentVersion != "1.4.0" || len(units[0].GetSegment().Unit.ShardIds) != 0 {
		t.Fatalf("legacy segment wrong: %+v", units[0])
	}
}

func TestStatTSDBGroup_PartVanishedMidWalkIsSkipped(t *testing.T) {
	group := filepath.Join(t.TempDir(), "g")
	seg := filepath.Join(group, "seg-20260928")
	writeFile(t, filepath.Join(seg, "metadata"), []byte(`{"version":"1.5.0"}`))
	// A part directory without metadata.json simulates a part merged away between ReadDir and ReadFile.
	if err := os.MkdirAll(filepath.Join(seg, "shard-0", "0000000000000009"), 0o755); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(seg, "shard-0", "000000000000000a", "metadata.json"), partJSON(1, 1, 1, 1, 2))
	units, err := statTSDBGroup(group, commonv1.Catalog_CATALOG_MEASURE, "g", false, testPartReader(commonv1.Catalog_CATALOG_MEASURE))
	if err != nil {
		t.Fatal(err)
	}
	if units[0].GetSegment().Shards[0].PartsCount != 1 || units[0].GetSegment().Shards[0].Parts[0].Id != 10 {
		t.Fatalf("vanished part must be skipped, got %+v", units[0].GetSegment().Shards[0])
	}
}

func TestStatTSDBGroup_PartlessShardIsExcluded(t *testing.T) {
	group := filepath.Join(t.TempDir(), "g")
	seg := filepath.Join(group, "seg-20260928")
	writeFile(t, filepath.Join(seg, "metadata"), []byte(`{"version":"1.5.0"}`))
	if err := os.MkdirAll(filepath.Join(seg, "shard-0"), 0o755); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(seg, "shard-1", "0000000000000001", "metadata.json"), partJSON(1, 1, 1, 1, 2))
	units, err := statTSDBGroup(group, commonv1.Catalog_CATALOG_STREAM, "g", false, testPartReader(commonv1.Catalog_CATALOG_STREAM))
	if err != nil {
		t.Fatal(err)
	}
	if len(units) != 1 || len(units[0].GetSegment().Shards) != 1 || units[0].GetSegment().Shards[0].ShardId != 1 ||
		len(units[0].GetSegment().Unit.ShardIds) != 1 || units[0].GetSegment().Unit.ShardIds[0] != 1 {
		t.Fatalf("a shard directory without flushed parts must not appear in shards or shard_ids: %+v", units)
	}
}

func TestStatTSDBGroup_TraceShardCountsSidxBytes(t *testing.T) {
	group := filepath.Join(t.TempDir(), "g")
	seg := filepath.Join(group, "seg-2026092810")
	writeFile(t, filepath.Join(seg, "metadata"), []byte(`{"version":"1.5.0"}`))
	writeFile(t, filepath.Join(seg, "shard-0", "0000000000000001", "metadata.json"), partJSON(10, 40, 7, 100, 200))
	// sidx parts describe themselves in manifest.json (banyand/internal/sidx/part.go).
	writeFile(t, filepath.Join(seg, "shard-0", "sidx", "rule_a", "0000000000000001", "manifest.json"), partJSON(25, 60, 7, 100, 200))
	writeFile(t, filepath.Join(seg, "shard-0", "sidx", "rule_b", "0000000000000001", "manifest.json"), partJSON(5, 10, 7, 100, 200))
	writeFile(t, filepath.Join(seg, "shard-0", "sidx", "rule_b", "0000000000000001", "data"), make([]byte, 999)) // sizes come from the manifest, not files
	writeFile(t, filepath.Join(seg, "shard-0", "idx", "x"), make([]byte, 7))                                     // not a trace artifact; must not count
	units, err := statTSDBGroup(group, commonv1.Catalog_CATALOG_TRACE, "g", false, testPartReader(commonv1.Catalog_CATALOG_TRACE))
	if err != nil {
		t.Fatal(err)
	}
	if len(units) != 1 || units[0].GetSegment().Unit.SegmentSuffix != "2026092810" {
		t.Fatalf("hourly segment must be reported: %+v", units)
	}
	s0 := units[0].GetSegment().Shards[0]
	if s0.EstimatedCompressedBytes != 10+25+5 || s0.EstimatedUncompressedBytes != 40+60+10 || s0.PartsCount != 1 {
		t.Fatalf("trace shard bytes must include the sidx part metadata sizes: %+v", s0)
	}
	// A measure shard carries neither index directory.
	units, err = statTSDBGroup(group, commonv1.Catalog_CATALOG_MEASURE, "g", false, testPartReader(commonv1.Catalog_CATALOG_MEASURE))
	if err != nil || units[0].GetSegment().Shards[0].EstimatedCompressedBytes != 10 {
		t.Fatalf("measure shard bytes must be the parts only: %+v, %v", units[0].GetSegment().Shards[0], err)
	}
}

func TestStatTSDBGroup_SegmentBeingCreatedIsSkipped(t *testing.T) {
	group := filepath.Join(t.TempDir(), "g")
	// Storage creates the metadata file before writing it: an empty one is a rollover in flight.
	writeFile(t, filepath.Join(group, "seg-20260929", "metadata"), nil)
	writeFile(t, filepath.Join(group, "seg-20260929", "shard-0", "0000000000000001", "metadata.json"), partJSON(1, 1, 1, 1, 2))
	seg := filepath.Join(group, "seg-20260928")
	writeFile(t, filepath.Join(seg, "metadata"), []byte(`{"version":"1.5.0"}`))
	writeFile(t, filepath.Join(seg, "shard-0", "0000000000000001", "metadata.json"), partJSON(1, 1, 1, 1, 2))
	units, err := statTSDBGroup(group, commonv1.Catalog_CATALOG_STREAM, "g", false, testPartReader(commonv1.Catalog_CATALOG_STREAM))
	if err != nil {
		t.Fatal(err)
	}
	if len(units) != 1 || units[0].GetSegment().Unit.SegmentSuffix != "20260928" {
		t.Fatalf("the segment with empty metadata must be skipped, got %+v", units)
	}
}

func TestReadSegmentVersion_EmptyIsNotExist(t *testing.T) {
	seg := t.TempDir()
	writeFile(t, filepath.Join(seg, "metadata"), []byte(" \n"))
	_, err := readSegmentVersion(seg)
	if !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("empty metadata must read as not-yet-existing, got %v", err)
	}
}

func TestStatTSDBGroup_MissingGroupDirIsEmpty(t *testing.T) {
	units, err := statTSDBGroup(filepath.Join(t.TempDir(), "absent"), commonv1.Catalog_CATALOG_STREAM, "absent", false, testPartReader(commonv1.Catalog_CATALOG_STREAM))
	if err != nil || len(units) != 0 {
		t.Fatalf("want no units and no error, got %d, %v", len(units), err)
	}
}

func TestStatPropertyGroup(t *testing.T) {
	group := filepath.Join(t.TempDir(), "sw_prop")
	writeFile(t, filepath.Join(group, "shard-0", "index.bin"), make([]byte, 64))
	writeFile(t, filepath.Join(group, "shard-2", "index.bin"), make([]byte, 32))
	unit, err := statPropertyGroup(group, "sw_prop")
	if err != nil {
		t.Fatal(err)
	}
	p := unit.GetProperty()
	if p == nil || unit.GetSegment() != nil || p.Group != "sw_prop" {
		t.Fatalf("a property group is a PropertyInventory unit: %+v", unit)
	}
	if len(p.Shards) != 2 || p.Shards[0].ShardId != 0 || p.Shards[1].ShardId != 2 || p.Shards[0].EstimatedBytes != 64 || p.Shards[1].EstimatedBytes != 32 {
		t.Fatalf("property shard stats wrong: %+v", p.Shards)
	}
	// No bluge snapshot in the directory: the document count falls back to 0 without failing.
	if p.Shards[0].DocCount != 0 {
		t.Fatalf("doc_count must be 0 for a shard without a committed index snapshot, got %d", p.Shards[0].DocCount)
	}
}

func TestStatPropertyGroup_NoShardsIsNil(t *testing.T) {
	u, err := statPropertyGroup(filepath.Join(t.TempDir(), "absent"), "absent")
	if err != nil || u != nil {
		t.Fatalf("want nil unit, got %+v, %v", u, err)
	}
}

func TestShardIDOf(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"shard-0", "shard-abc", "shard-", "not-shard"} {
		if err := os.MkdirAll(filepath.Join(dir, name), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	// shard-1 is a regular file, not a directory.
	writeFile(t, filepath.Join(dir, "shard-1"), []byte("regular file"))
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	byName := make(map[string]os.DirEntry, len(entries))
	for _, e := range entries {
		byName[e.Name()] = e
	}
	cases := []struct {
		name   string
		wantOK bool
		wantID uint32
	}{
		{"shard-0", true, 0},
		{"shard-abc", false, 0},
		{"shard-", false, 0},
		{"not-shard", false, 0},
		{"shard-1", false, 0},
	}
	for _, tc := range cases {
		e, found := byName[tc.name]
		if !found {
			t.Fatalf("entry %q not found in test dir", tc.name)
		}
		id, ok := shardIDOf(e)
		if ok != tc.wantOK {
			t.Errorf("shardIDOf(%q) ok=%v, want %v", tc.name, ok, tc.wantOK)
		}
		if ok && id != tc.wantID {
			t.Errorf("shardIDOf(%q) id=%d, want %d", tc.name, id, tc.wantID)
		}
	}
}

// buildIndex writes a committed bluge index with docs documents into dir, the way the
// segment-level series index and the property shards are written.
func buildIndex(t *testing.T, dir string, docs int) {
	t.Helper()
	store, err := inverted.NewStore(inverted.StoreOpts{Path: dir})
	if err != nil {
		t.Fatal(err)
	}
	key := index.FieldKey{Analyzer: index.AnalyzerKeyword, SeriesID: common.SeriesID(1), IndexRuleID: 1}
	documents := make(index.Documents, 0, docs)
	for i := 0; i < docs; i++ {
		field := index.NewStringField(key, "v"+strconv.Itoa(i))
		field.Index = true
		field.Store = true
		documents = append(documents, index.Document{DocID: uint64(i + 1), Fields: []index.Field{field}})
	}
	if err = store.Batch(index.Batch{Documents: documents}); err != nil {
		t.Fatal(err)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
}

// corruptIndex overwrites every committed snapshot manifest in dir with garbage.
func corruptIndex(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	corrupted := 0
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".snp") {
			writeFile(t, filepath.Join(dir, e.Name()), []byte("garbage"))
			corrupted++
		}
	}
	if corrupted == 0 {
		t.Fatalf("no snapshot manifest to corrupt in %s", dir)
	}
}

func TestStatTSDBGroup_IndexModeCountsCommittedDocuments(t *testing.T) {
	group := filepath.Join(t.TempDir(), "sw_metadata")
	seg := filepath.Join(group, "seg-20260928")
	writeFile(t, filepath.Join(seg, "metadata"), []byte(`{"version":"1.5.0"}`))
	buildIndex(t, filepath.Join(seg, sidxDirName), 3)

	units, err := statTSDBGroup(group, commonv1.Catalog_CATALOG_MEASURE, "sw_metadata", true, testPartReader(commonv1.Catalog_CATALOG_MEASURE))
	if err != nil {
		t.Fatal(err)
	}
	segLevel := units[0].GetSegment().GetSegmentLevel()
	if segLevel.GetDocCount() != 3 || segLevel.GetEstimatedBytes() == 0 {
		t.Fatalf("an index-mode segment must report the committed document count and the index bytes: %+v", segLevel)
	}
	// A plain measure group never opens the index.
	units, err = statTSDBGroup(group, commonv1.Catalog_CATALOG_MEASURE, "sw_metadata", false, testPartReader(commonv1.Catalog_CATALOG_MEASURE))
	if err != nil || units[0].GetSegment().GetSegmentLevel().GetDocCount() != 0 {
		t.Fatalf("a plain measure group must report doc_count 0, got %+v, %v", units, err)
	}
	// A corrupt index fails the plan instead of reporting zero rows.
	corruptIndex(t, filepath.Join(seg, sidxDirName))
	if _, err = statTSDBGroup(group, commonv1.Catalog_CATALOG_MEASURE, "sw_metadata", true, testPartReader(commonv1.Catalog_CATALOG_MEASURE)); err == nil ||
		!errors.Is(err, inverted.ErrCorruptIndex) {
		t.Fatalf("a corrupt index must fail the plan, got %v", err)
	}
}

func TestStatTSDBGroup_IndexModeWithoutCommittedIndexCountsZero(t *testing.T) {
	group := filepath.Join(t.TempDir(), "sw_metadata")
	seg := filepath.Join(group, "seg-20260928")
	writeFile(t, filepath.Join(seg, "metadata"), []byte(`{"version":"1.5.0"}`))
	units, err := statTSDBGroup(group, commonv1.Catalog_CATALOG_MEASURE, "sw_metadata", true, testPartReader(commonv1.Catalog_CATALOG_MEASURE))
	if err != nil || units[0].GetSegment().GetSegmentLevel().GetDocCount() != 0 {
		t.Fatalf("a segment without a flushed index must count 0, got %+v, %v", units, err)
	}
}

func TestStatPropertyGroup_CountsCommittedDocuments(t *testing.T) {
	group := filepath.Join(t.TempDir(), "sw_prop")
	buildIndex(t, filepath.Join(group, "shard-0"), 4)
	unit, err := statPropertyGroup(group, "sw_prop")
	if err != nil {
		t.Fatal(err)
	}
	shards := unit.GetProperty().GetShards()
	if len(shards) != 1 || shards[0].GetDocCount() != 4 || shards[0].GetEstimatedBytes() == 0 {
		t.Fatalf("a property shard must report its committed document count: %+v", shards)
	}
	corruptIndex(t, filepath.Join(group, "shard-0"))
	if _, err = statPropertyGroup(group, "sw_prop"); !errors.Is(err, inverted.ErrCorruptIndex) {
		t.Fatalf("a corrupt property shard must fail the plan, got %v", err)
	}
}
