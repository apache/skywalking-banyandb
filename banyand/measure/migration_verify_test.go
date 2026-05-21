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
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/blugelabs/bluge"

	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/fs"
)

// TestEnumerateGroupTarget_AlignmentAndSidx exercises the verify
// helper's two main signals: (a) seg name → start time → grid alignment
// against an IntervalRule, and (b) sidx doc count. It deliberately
// creates segs WITHOUT shard subdirs so Parts/Rows stay at 0; that path
// is covered separately by the end-to-end migration test.
func TestEnumerateGroupTarget_AlignmentAndSidx(t *testing.T) {
	groupRoot := t.TempDir()
	dayInterval := storage.IntervalRule{Unit: storage.DAY, Num: 1}

	// Aligned: 2026-05-19 at midnight UTC fits the day grid AND
	// carries a metadata file whose endTime = NextTime(start).
	segAligned := filepath.Join(groupRoot, directCopySegPrefix+"20260519")
	if err := os.MkdirAll(segAligned, storage.DirPerm); err != nil {
		t.Fatalf("seed aligned: %v", err)
	}
	wantStart := time.Date(2026, 5, 19, 0, 0, 0, 0, time.Local)
	wantEnd := dayInterval.NextTime(wantStart)
	writeTestSegmentMetadata(t, segAligned, wantEnd)

	// Misaligned: HOUR-format seg name decoded as DAY would parse but
	// the start time keeps the hour offset, which IntervalRule.Standard
	// would round down — easier path is a seg name that DAY can't
	// parse cleanly (parseDirectCopySegStart errors → Aligned stays
	// false because StartTime is zero, which Standard returns as well).
	// Use a name with hour suffix.
	segUnparseable := filepath.Join(groupRoot, directCopySegPrefix+"20260519CRAP")
	if err := os.MkdirAll(segUnparseable, storage.DirPerm); err != nil {
		t.Fatalf("seed unparseable: %v", err)
	}

	// Drop a sidx with two docs into the aligned seg only.
	sidxDir := filepath.Join(segAligned, directCopySidxDirName)
	if err := os.MkdirAll(sidxDir, storage.DirPerm); err != nil {
		t.Fatalf("mkdir sidx: %v", err)
	}
	w, err := bluge.OpenWriter(bluge.DefaultConfig(sidxDir))
	if err != nil {
		t.Fatalf("open writer: %v", err)
	}
	batch := bluge.NewBatch()
	batch.Insert(bluge.NewDocument("id-a").AddField(
		bluge.NewKeywordFieldBytes("k", []byte("v")).StoreValue()))
	batch.Insert(bluge.NewDocument("id-b").AddField(
		bluge.NewKeywordFieldBytes("k", []byte("v2")).StoreValue()))
	if batchErr := w.Batch(batch); batchErr != nil {
		t.Fatalf("batch: %v", batchErr)
	}
	if closeErr := w.Close(); closeErr != nil {
		t.Fatalf("close: %v", closeErr)
	}

	reports, err := EnumerateGroupTarget(groupRoot, dayInterval, fs.NewLocalFileSystem())
	if err != nil {
		t.Fatalf("EnumerateGroupTarget: %v", err)
	}
	if len(reports) != 2 {
		t.Fatalf("expected 2 reports, got %d: %+v", len(reports), reports)
	}

	var aligned, unparseable *SegmentReport
	for i := range reports {
		switch reports[i].Seg {
		case directCopySegPrefix + "20260519":
			aligned = &reports[i]
		case directCopySegPrefix + "20260519CRAP":
			unparseable = &reports[i]
		}
	}
	if aligned == nil || unparseable == nil {
		t.Fatalf("missing reports: %+v", reports)
	}
	if !aligned.StartTime.Equal(wantStart) {
		t.Errorf("aligned StartTime = %v, want %v", aligned.StartTime, wantStart)
	}
	if !aligned.EndTime.Equal(wantEnd) {
		t.Errorf("aligned EndTime = %v, want %v", aligned.EndTime, wantEnd)
	}
	if !aligned.Aligned {
		t.Errorf("aligned.Aligned = false, want true (start aligned, endTime = NextTime(start), both in same bucket)")
	}
	if !aligned.SidxOpened || aligned.SidxDocCount != 2 {
		t.Errorf("aligned sidx: opened=%v docs=%d (want true, 2)", aligned.SidxOpened, aligned.SidxDocCount)
	}
	if unparseable.Aligned {
		t.Errorf("unparseable.Aligned = true, want false (zero StartTime fails grid check)")
	}
	if unparseable.SidxOpened {
		t.Errorf("unparseable.SidxOpened = true, want false (no sidx subdir)")
	}
}

// TestEnumerateGroupTarget_AlignedRequiresMetadata pins the contract that
// a grid-aligned start directory name is NOT enough — the segment must
// also carry a JSON metadata file with a non-empty endTime that equals
// IntervalRule.NextTime(start). Without it Aligned must stay false so
// the verify CLI surfaces "this seg was never finalized".
func TestEnumerateGroupTarget_AlignedRequiresMetadata(t *testing.T) {
	groupRoot := t.TempDir()
	dayInterval := storage.IntervalRule{Unit: storage.DAY, Num: 1}

	// Seg dir name aligns to the DAY grid but we deliberately omit the
	// metadata file so haveEnd stays false.
	segNoMeta := filepath.Join(groupRoot, directCopySegPrefix+"20260601")
	if err := os.MkdirAll(segNoMeta, storage.DirPerm); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// A second seg whose metadata file has a wrong (non-NextTime) endTime
	// must also report Aligned=false.
	segBadEnd := filepath.Join(groupRoot, directCopySegPrefix+"20260603")
	if err := os.MkdirAll(segBadEnd, storage.DirPerm); err != nil {
		t.Fatalf("seed: %v", err)
	}
	wrongEnd := time.Date(2026, 6, 4, 12, 0, 0, 0, time.Local) // mid-day, not on grid
	writeTestSegmentMetadata(t, segBadEnd, wrongEnd)

	reports, err := EnumerateGroupTarget(groupRoot, dayInterval, fs.NewLocalFileSystem())
	if err != nil {
		t.Fatalf("EnumerateGroupTarget: %v", err)
	}
	if len(reports) != 2 {
		t.Fatalf("expected 2 reports, got %d", len(reports))
	}
	for _, r := range reports {
		if r.Aligned {
			t.Errorf("seg %s Aligned=true, want false (metadata missing or endTime mismatched)", r.Seg)
		}
	}
}

// writeTestSegmentMetadata seeds <segDir>/metadata with a JSON-encoded
// SegmentMetadata carrying the supplied endTime. Mirrors what
// writeDirectCopySegmentMetadata + the live segmentController emit.
func writeTestSegmentMetadata(t *testing.T, segDir string, endTime time.Time) {
	t.Helper()
	body := storage.SegmentMetadata{
		Version: storage.CurrentSegmentVersion,
		EndTime: endTime.Format(time.RFC3339Nano),
	}
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal seg metadata: %v", err)
	}
	if writeErr := os.WriteFile(filepath.Join(segDir, storage.SegmentMetadataFilename), raw, storage.FilePerm); writeErr != nil {
		t.Fatalf("write seg metadata: %v", writeErr)
	}
}

// TestEnumerateGroupTarget_MissingRoot ensures a non-existent groupRoot
// returns (nil, nil) — the caller is expected to treat this as "this
// PVC does not host the group" rather than an error.
func TestEnumerateGroupTarget_MissingRoot(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "no-such-group")
	dayInterval := storage.IntervalRule{Unit: storage.DAY, Num: 1}
	reports, err := EnumerateGroupTarget(missing, dayInterval, fs.NewLocalFileSystem())
	if err != nil {
		t.Fatalf("EnumerateGroupTarget on missing dir: %v", err)
	}
	if reports != nil {
		t.Fatalf("expected nil reports, got %+v", reports)
	}
}

// TestSumGroupSourceRows_MissingRootsSkipped confirms the helper
// silently skips srcRoots whose path does not exist, matching the
// resolveEntrySrcRoots contract (it already filters non-existent
// dirs out before calling SumGroupSourceRows).
func TestSumGroupSourceRows_MissingRootsSkipped(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "ghost-pvc", "measure", "data", "sw_metricsMinute")
	rows, parts, err := SumGroupSourceRows([]string{missing}, fs.NewLocalFileSystem())
	if err != nil {
		t.Fatalf("expected no error on missing root, got %v", err)
	}
	if rows != 0 || parts != 0 {
		t.Fatalf("expected 0/0, got rows=%d parts=%d", rows, parts)
	}
}
