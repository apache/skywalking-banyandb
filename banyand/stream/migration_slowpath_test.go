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
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/migration"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/inverted"
	"github.com/apache/skywalking-banyandb/pkg/partition"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
)

// TestMigrationSlowPath_NumGt1_ReBucketing proves that the slow path correctly
// re-buckets source rows when the target SegmentInterval has Num > 1 (specifically
// Num=2 days), which is the production scenario for warm/cold tiers. A single source
// segment holds four rows whose timestamps span two distinct 2-day target buckets:
// two rows fall in the [Jun 2, Jun 3) bucket (target seg-20260602) and two rows
// fall in the [Jun 4, Jun 5) bucket (target seg-20260604). Because the source part's
// alignedMin and alignedMax differ under the 2-day IR, the slow path is taken. The
// test asserts that: all four rows reach the target (stream no-dedup invariant),
// each 2-day target segment receives exactly its two rows, and SlowPathHits() > 0.
func TestMigrationSlowPath_NumGt1_ReBucketing(t *testing.T) {
	tmpDir := t.TempDir()

	const (
		group  = "sw_segment"
		shardN = "shard-0"
	)

	fileSystem := fs.NewLocalFileSystem()
	noFsync := migration.NoFsyncFS{FileSystem: fileSystem}

	// Source uses 1-day interval for directory naming (matches existing on-disk layout).
	srcIR := storage.IntervalRule{Unit: storage.DAY, Num: 1}
	// Target uses a 2-day interval — the grid that does NOT divide the 1-day source grid.
	tgtIR := storage.IntervalRule{Unit: storage.DAY, Num: 2}

	// Choose dates whose 2-day bucket boundaries are distinct and predictable.
	// With the epoch-anchored 2-day grid in local time:
	//   Jun 2 2026 → bucket start 2026-06-02
	//   Jun 3 2026 → bucket start 2026-06-02  (same bucket as Jun 2)
	//   Jun 4 2026 → bucket start 2026-06-04
	//   Jun 5 2026 → bucket start 2026-06-04  (same bucket as Jun 4)
	// Verified by IntervalRule{Num:2,Unit:DAY}.Standard() at runtime below.
	jun2 := time.Date(2026, 6, 2, 0, 0, 0, 0, time.Local)
	jun3 := time.Date(2026, 6, 3, 0, 0, 0, 0, time.Local)
	jun4 := time.Date(2026, 6, 4, 0, 0, 0, 0, time.Local)
	jun5 := time.Date(2026, 6, 5, 0, 0, 0, 0, time.Local)

	bucket1Start := tgtIR.Standard(jun2)
	bucket2Start := tgtIR.Standard(jun4)
	require.Equal(t, bucket1Start, tgtIR.Standard(jun3), "Jun 3 must share bucket with Jun 2")
	require.Equal(t, bucket2Start, tgtIR.Standard(jun5), "Jun 5 must share bucket with Jun 4")
	require.NotEqual(t, bucket1Start, bucket2Start, "Jun 2 and Jun 4 must be in different 2-day buckets")

	// Source segment: named after Jun 2 (1-day naming), contains all four rows.
	srcSegName := formatStreamDirectCopySegName(srcIR.Standard(jun2), storage.DAY)
	sourceGroupRoot := filepath.Join(tmpDir, "source", group)
	srcShardDir := filepath.Join(sourceGroupRoot, srcSegName, shardN)
	require.NoError(t, os.MkdirAll(srcShardDir, storage.DirPerm))

	// Four rows: two in each 2-day target bucket (timestamps mid-day to be unambiguous).
	const (
		eB1row1 = uint64(1001)
		eB1row2 = uint64(1002)
		eB2row1 = uint64(2001)
		eB2row2 = uint64(2002)
	)
	rows := []DumpRow{
		{
			Entity: "svc-A", Timestamp: jun2.Add(6 * time.Hour).UnixNano(), ElementID: eB1row1,
			Tags: []DumpTag{{Family: "default", Name: "svc", Value: "A"}},
		},
		{
			Entity: "svc-A", Timestamp: jun3.Add(6 * time.Hour).UnixNano(), ElementID: eB1row2,
			Tags: []DumpTag{{Family: "default", Name: "svc", Value: "A"}},
		},
		{
			Entity: "svc-A", Timestamp: jun4.Add(6 * time.Hour).UnixNano(), ElementID: eB2row1,
			Tags: []DumpTag{{Family: "default", Name: "svc", Value: "A"}},
		},
		{
			Entity: "svc-A", Timestamp: jun5.Add(6 * time.Hour).UnixNano(), ElementID: eB2row2,
			Tags: []DumpTag{{Family: "default", Name: "svc", Value: "A"}},
		},
	}
	_, rows, cleanup := BuildPartForDump(srcShardDir, noFsync, uint64(1), rows)
	defer cleanup()
	srcRows := uint64(len(rows))

	tasks, err := discoverStreamPartTasks(context.Background(), []string{sourceGroupRoot})
	require.NoError(t, err)
	require.Len(t, tasks, 1)

	dstGroupRoot := filepath.Join(tmpDir, "target", group)
	tagProjection := []model.TagProjection{{Family: "default", Names: []string{"svc"}}}

	res, err := directCopyStreamGroup(
		context.Background(),
		"entry [numgt1]",
		group,
		"warm",
		dstGroupRoot,
		tgtIR, tagProjection,
		tasks, "",
		map[string]*streamIndexLocator{},
	)
	require.NoError(t, err)
	require.EqualValues(t, srcRows, res.Rows, "stream no-dedup invariant: target rows == source rows")

	// Target segment names are derived from tgtIR.Standard(), formatted with storage.DAY.
	seg1Name := formatStreamDirectCopySegName(bucket1Start, storage.DAY)
	seg2Name := formatStreamDirectCopySegName(bucket2Start, storage.DAY)
	require.NotEqual(t, seg1Name, seg2Name)

	// Per-segment row counts: each 2-day bucket must hold exactly its two rows.
	seg1Rows, _, vErr := VerifyShardParts(filepath.Join(dstGroupRoot, seg1Name, shardN), fileSystem)
	require.NoError(t, vErr)
	seg2Rows, _, vErr := VerifyShardParts(filepath.Join(dstGroupRoot, seg2Name, shardN), fileSystem)
	require.NoError(t, vErr)
	require.EqualValues(t, 2, seg1Rows, "2-day bucket %s must hold 2 rows (Jun 2 + Jun 3)", seg1Name)
	require.EqualValues(t, 2, seg2Rows, "2-day bucket %s must hold 2 rows (Jun 4 + Jun 5)", seg2Name)
	require.EqualValues(t, srcRows, seg1Rows+seg2Rows, "no loss across 2-day target segments")

	// The slow path must have been exercised: source part spans two 2-day buckets.
	require.GreaterOrEqual(t, SlowPathHits(), int64(1),
		"slow path must fire when source part rows span multiple 2-day target segments")
}

// TestMigrationSlowPathElementIndexRebuild proves the slow-path element-index
// rebuild. A single source (seg, shard) holds four elements whose timestamps span
// TWO target day-segments; an INVERTED-indexed non-entity string tag "status" is
// present. After running directCopyStreamGroup with a 1-day target SegmentInterval
// (which splits the source rows into two target segs), it asserts:
//  1. Each target seg's data-part row count summed equals the source total (no loss).
//  2. Each target seg has an idx/ that, opened via inverted.NewStore (BatchWaitSec:0)
//     + MatchTerms, returns ONLY the elementIDs whose data is in THAT seg.
func TestMigrationSlowPathElementIndexRebuild(t *testing.T) {
	tmpDir := t.TempDir()

	const (
		group     = "sw_segment"
		streamN   = "sw_segment"
		shardN    = "shard-0"
		statusRID = uint32(7)
	)

	fileSystem := fs.NewLocalFileSystem()
	noFsync := migration.NoFsyncFS{FileSystem: fileSystem}
	ir := storage.IntervalRule{Unit: storage.DAY, Num: 1}

	// Two distinct day segments.
	day1 := ir.Standard(time.Date(2026, 6, 1, 10, 0, 0, 0, time.Local))
	day2 := ir.Standard(time.Date(2026, 6, 3, 10, 0, 0, 0, time.Local))
	require.NotEqual(t, day1, day2)

	// Source seg spans both days; its name comes from the source-min day.
	srcSegName := formatStreamDirectCopySegName(day1, storage.DAY)
	sourceGroupRoot := filepath.Join(tmpDir, "source", group)
	srcShardDir := filepath.Join(sourceGroupRoot, srcSegName, shardN)
	require.NoError(t, os.MkdirAll(srcShardDir, storage.DirPerm))

	// Four elements: two in day1, two in day2; same series; status ok/err per day.
	d1ts0 := day1.Add(1 * time.Hour).UnixNano()
	d1ts1 := day1.Add(2 * time.Hour).UnixNano()
	d2ts0 := day2.Add(1 * time.Hour).UnixNano()
	d2ts1 := day2.Add(2 * time.Hour).UnixNano()
	const (
		eD1ok  = uint64(1001)
		eD1err = uint64(1002)
		eD2ok  = uint64(2001)
		eD2err = uint64(2002)
	)
	rows := []DumpRow{
		{
			Entity: "svc-A", Timestamp: d1ts0, ElementID: eD1ok,
			Tags: []DumpTag{{Family: "default", Name: "status", Value: "ok"}},
		},
		{
			Entity: "svc-A", Timestamp: d1ts1, ElementID: eD1err,
			Tags: []DumpTag{{Family: "default", Name: "status", Value: "err"}},
		},
		{
			Entity: "svc-A", Timestamp: d2ts0, ElementID: eD2ok,
			Tags: []DumpTag{{Family: "default", Name: "status", Value: "ok"}},
		},
		{
			Entity: "svc-A", Timestamp: d2ts1, ElementID: eD2err,
			Tags: []DumpTag{{Family: "default", Name: "status", Value: "err"}},
		},
	}
	const srcPartID = uint64(1)
	_, rows, cleanup := BuildPartForDump(srcShardDir, noFsync, srcPartID, rows)
	defer cleanup()
	srcRows := uint64(len(rows))
	seriesID := rows[0].SeriesID

	// A source idx/ must exist so the finalize step knows this (seg, shard) has an
	// element index (its contents are never read on the rebuild path).
	srcIdxDir := filepath.Join(srcShardDir, elementIndexFilename)
	require.NoError(t, os.MkdirAll(srcIdxDir, storage.DirPerm))
	seedStore, err := inverted.NewStore(inverted.StoreOpts{Path: srcIdxDir, BatchWaitSec: 0})
	require.NoError(t, err)
	require.NoError(t, seedStore.Close())

	// Hand-built locator: one tag family "default" with an INVERTED-indexed
	// non-entity string tag "status"; entity is meta.name (not indexed).
	families := []*databasev1.TagFamilySpec{
		{
			Name: "default",
			Tags: []*databasev1.TagSpec{
				{Name: "status", Type: databasev1.TagType_TAG_TYPE_STRING},
			},
		},
	}
	entity := &databasev1.Entity{TagNames: []string{"name"}}
	statusRule := &databasev1.IndexRule{
		Metadata: &commonv1.Metadata{Id: statusRID, Name: "status"},
		Tags:     []string{"status"},
		Type:     databasev1.IndexRule_TYPE_INVERTED,
		Analyzer: index.AnalyzerKeyword,
	}
	locators, _ := partition.ParseIndexRuleLocators(entity, families, []*databasev1.IndexRule{statusRule}, false)
	indexLocators := map[string]*streamIndexLocator{
		streamN: {
			Locators:   locators,
			IndexRules: []*databasev1.IndexRule{statusRule},
			Entity:     entity,
			Families:   families,
		},
	}

	// Discover tasks and run the group copy.
	tasks, err := discoverStreamPartTasks(context.Background(), []string{sourceGroupRoot})
	require.NoError(t, err)
	require.Len(t, tasks, 1)

	dstGroupRoot := filepath.Join(tmpDir, "target", group)
	tagProjection := []model.TagProjection{{Family: "default", Names: []string{"status"}}}

	res, err := directCopyStreamGroup(
		context.Background(),
		"entry [1/1]",
		group,
		"hot",
		dstGroupRoot,
		ir, tagProjection,
		tasks, "",
		indexLocators,
	)
	require.NoError(t, err)
	require.EqualValues(t, srcRows, res.Rows, "stream invariant: target rows == source rows")

	d1Seg := formatStreamDirectCopySegName(day1, storage.DAY)
	d2Seg := formatStreamDirectCopySegName(day2, storage.DAY)
	require.NotEqual(t, d1Seg, d2Seg)

	// (1) Row counts: each target seg's data rows summed equals the source total.
	d1Rows, _, vErr := VerifyShardParts(filepath.Join(dstGroupRoot, d1Seg, shardN), fileSystem)
	require.NoError(t, vErr)
	d2Rows, _, vErr := VerifyShardParts(filepath.Join(dstGroupRoot, d2Seg, shardN), fileSystem)
	require.NoError(t, vErr)
	require.EqualValues(t, 2, d1Rows, "day1 target must hold 2 elements")
	require.EqualValues(t, 2, d2Rows, "day2 target must hold 2 elements")
	require.EqualValues(t, srcRows, d1Rows+d2Rows, "no loss across target segs")

	// (2) Each target seg has an idx/ returning ONLY its own elementIDs.
	assertIdxTerm := func(seg, value string, want []uint64) {
		idxPath := filepath.Join(dstGroupRoot, seg, shardN, elementIndexFilename)
		info, statErr := os.Stat(idxPath)
		require.NoError(t, statErr, "target seg %s must have an idx/", seg)
		require.True(t, info.IsDir())
		store, openErr := inverted.NewStore(inverted.StoreOpts{Path: idxPath, BatchWaitSec: 0})
		require.NoError(t, openErr)
		defer func() { require.NoError(t, store.Close()) }()
		list, _, matchErr := store.MatchTerms(index.NewStringField(index.FieldKey{
			IndexRuleID: statusRID,
			SeriesID:    seriesID,
		}, value))
		require.NoError(t, matchErr)
		got := map[uint64]struct{}{}
		iter := list.Iterator()
		for iter.Next() {
			got[iter.Current()] = struct{}{}
		}
		require.NoError(t, iter.Close())
		require.Len(t, got, len(want), "seg %s status=%s wrong doc count", seg, value)
		for _, id := range want {
			_, ok := got[id]
			require.Truef(t, ok, "seg %s status=%s missing elementID %d", seg, value, id)
		}
	}

	// day1: only eD1ok for "ok", only eD1err for "err"; no day2 contamination.
	assertIdxTerm(d1Seg, "ok", []uint64{eD1ok})
	assertIdxTerm(d1Seg, "err", []uint64{eD1err})
	// day2: only eD2ok / eD2err.
	assertIdxTerm(d2Seg, "ok", []uint64{eD2ok})
	assertIdxTerm(d2Seg, "err", []uint64{eD2err})

	// Sanity: the slow path was actually exercised (source seg split into 2 targets).
	require.GreaterOrEqual(t, res.Segments, 2, "expected at least two target segments")
	require.GreaterOrEqual(t, SlowPathHits(), int64(1),
		"slow path must have been exercised at least once for a cross-segment source part")
}

// streamSeries builds a marshaled pbv1.Series (subject + one string entity value)
// and returns its SeriesID plus the marshaled buffer used to seed the source seg's
// series index (sidx). This mirrors how the write path keys series documents.
func streamSeries(t *testing.T, subject, entityValue string) (common.SeriesID, []byte) {
	t.Helper()
	s := &pbv1.Series{
		Subject:      subject,
		EntityValues: []*modelv1.TagValue{{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: entityValue}}}},
	}
	require.NoError(t, s.Marshal())
	return s.ID, append([]byte(nil), s.Buffer...)
}

// TestMigrationSlowPathMultiStreamElementIndexRebuild proves the slow-path
// element-index rebuild for a group that owns MORE THAN ONE stream (the case the
// implementation previously aborted on). The group holds two streams:
//   - streamA: entity tag "name", INVERTED non-entity tag "status"
//   - streamB: entity tag "service", INVERTED non-entity tag "code" + INVERTED
//     entity tag "service" (to also exercise per-row entity resolution)
//
// Rows of BOTH streams span two target day-segments. Each row's owning stream is
// recoverable only from its seriesID -> source-sidx Series.Subject. After the copy
// it asserts each target seg's idx/ returns exactly the right docs per stream's
// indexed tag, with no cross-stream or cross-segment contamination.
func TestMigrationSlowPathMultiStreamElementIndexRebuild(t *testing.T) {
	tmpDir := t.TempDir()

	const (
		group   = "multi"
		streamA = "streamA"
		streamB = "streamB"
		shardN  = "shard-0"

		statusRID  = uint32(7) // streamA non-entity tag
		codeRID    = uint32(8) // streamB non-entity tag
		serviceRID = uint32(9) // streamB entity tag (indexed entity)
	)

	fileSystem := fs.NewLocalFileSystem()
	noFsync := migration.NoFsyncFS{FileSystem: fileSystem}
	ir := storage.IntervalRule{Unit: storage.DAY, Num: 1}

	day1 := ir.Standard(time.Date(2026, 6, 1, 10, 0, 0, 0, time.Local))
	day2 := ir.Standard(time.Date(2026, 6, 3, 10, 0, 0, 0, time.Local))
	require.NotEqual(t, day1, day2)

	srcSegName := formatStreamDirectCopySegName(day1, storage.DAY)
	sourceGroupRoot := filepath.Join(tmpDir, "source", group)
	srcShardDir := filepath.Join(sourceGroupRoot, srcSegName, shardN)
	require.NoError(t, os.MkdirAll(srcShardDir, storage.DirPerm))

	// Two distinct series per stream is unnecessary; one series per stream suffices
	// to prove Subject-based routing. Each series carries rows in both day segments.
	aSeriesID, aBuf := streamSeries(t, streamA, "svc-A")
	bSeriesID, bBuf := streamSeries(t, streamB, "svc-B")
	require.NotEqual(t, aSeriesID, bSeriesID)

	d1t0 := day1.Add(1 * time.Hour).UnixNano()
	d1t1 := day1.Add(2 * time.Hour).UnixNano()
	d2t0 := day2.Add(1 * time.Hour).UnixNano()
	d2t1 := day2.Add(2 * time.Hour).UnixNano()

	const (
		aD1ok  = uint64(1101)
		aD1err = uint64(1102)
		aD2ok  = uint64(2101)
		aD2err = uint64(2102)
		bD1ok  = uint64(1201)
		bD1err = uint64(1202)
		bD2ok  = uint64(2201)
		bD2err = uint64(2202)
	)
	rows := []DumpRow{
		// streamA rows (tag family "fa", tag "status").
		{SeriesID: aSeriesID, Timestamp: d1t0, ElementID: aD1ok, Tags: []DumpTag{{Family: "fa", Name: "status", Value: "ok"}}},
		{SeriesID: aSeriesID, Timestamp: d1t1, ElementID: aD1err, Tags: []DumpTag{{Family: "fa", Name: "status", Value: "err"}}},
		{SeriesID: aSeriesID, Timestamp: d2t0, ElementID: aD2ok, Tags: []DumpTag{{Family: "fa", Name: "status", Value: "ok"}}},
		{SeriesID: aSeriesID, Timestamp: d2t1, ElementID: aD2err, Tags: []DumpTag{{Family: "fa", Name: "status", Value: "err"}}},
		// streamB rows (tag family "fb", tag "code").
		{SeriesID: bSeriesID, Timestamp: d1t0, ElementID: bD1ok, Tags: []DumpTag{{Family: "fb", Name: "code", Value: "200"}}},
		{SeriesID: bSeriesID, Timestamp: d1t1, ElementID: bD1err, Tags: []DumpTag{{Family: "fb", Name: "code", Value: "500"}}},
		{SeriesID: bSeriesID, Timestamp: d2t0, ElementID: bD2ok, Tags: []DumpTag{{Family: "fb", Name: "code", Value: "200"}}},
		{SeriesID: bSeriesID, Timestamp: d2t1, ElementID: bD2err, Tags: []DumpTag{{Family: "fb", Name: "code", Value: "500"}}},
	}
	const srcPartID = uint64(1)
	_, rows, cleanup := BuildPartForDump(srcShardDir, noFsync, srcPartID, rows)
	defer cleanup()
	srcRows := uint64(len(rows))

	// Seed the source segment's series index (sidx) so the rebuild can map each
	// row's seriesID -> Series.Subject (the stream name) + entity values.
	srcSegPath := filepath.Join(sourceGroupRoot, srcSegName)
	sidxDir := filepath.Join(srcSegPath, "sidx")
	require.NoError(t, os.MkdirAll(sidxDir, storage.DirPerm))
	sidxStore, err := inverted.NewStore(inverted.StoreOpts{Path: sidxDir, BatchWaitSec: 0})
	require.NoError(t, err)
	require.NoError(t, sidxStore.InsertSeriesBatch(index.Batch{Documents: index.Documents{
		{EntityValues: aBuf},
		{EntityValues: bBuf},
	}}))
	require.NoError(t, sidxStore.Close())

	// A source idx/ must exist so the finalize step knows this (seg, shard) carries
	// an element index (its contents are never read on the rebuild path).
	srcIdxDir := filepath.Join(srcShardDir, elementIndexFilename)
	require.NoError(t, os.MkdirAll(srcIdxDir, storage.DirPerm))
	seedStore, err := inverted.NewStore(inverted.StoreOpts{Path: srcIdxDir, BatchWaitSec: 0})
	require.NoError(t, err)
	require.NoError(t, seedStore.Close())

	// Per-stream locators. streamA: entity "name" (not indexed), INVERTED "status".
	// streamB: entity "service" (INVERTED), INVERTED "code".
	famA := []*databasev1.TagFamilySpec{{
		Name: "fa",
		Tags: []*databasev1.TagSpec{{Name: "status", Type: databasev1.TagType_TAG_TYPE_STRING}},
	}}
	entityA := &databasev1.Entity{TagNames: []string{"name"}}
	statusRule := &databasev1.IndexRule{
		Metadata: &commonv1.Metadata{Id: statusRID, Name: "status"},
		Tags:     []string{"status"}, Type: databasev1.IndexRule_TYPE_INVERTED, Analyzer: index.AnalyzerKeyword,
	}
	locA, _ := partition.ParseIndexRuleLocators(entityA, famA, []*databasev1.IndexRule{statusRule}, false)

	famB := []*databasev1.TagFamilySpec{{
		Name: "fb",
		Tags: []*databasev1.TagSpec{
			{Name: "service", Type: databasev1.TagType_TAG_TYPE_STRING},
			{Name: "code", Type: databasev1.TagType_TAG_TYPE_STRING},
		},
	}}
	entityB := &databasev1.Entity{TagNames: []string{"service"}}
	codeRule := &databasev1.IndexRule{
		Metadata: &commonv1.Metadata{Id: codeRID, Name: "code"},
		Tags:     []string{"code"}, Type: databasev1.IndexRule_TYPE_INVERTED, Analyzer: index.AnalyzerKeyword,
	}
	serviceRule := &databasev1.IndexRule{
		Metadata: &commonv1.Metadata{Id: serviceRID, Name: "service"},
		Tags:     []string{"service"}, Type: databasev1.IndexRule_TYPE_INVERTED, Analyzer: index.AnalyzerKeyword,
	}
	locB, _ := partition.ParseIndexRuleLocators(entityB, famB, []*databasev1.IndexRule{codeRule, serviceRule}, false)

	indexLocators := map[string]*streamIndexLocator{
		streamA: {Locators: locA, IndexRules: []*databasev1.IndexRule{statusRule}, Entity: entityA, Families: famA},
		streamB: {Locators: locB, IndexRules: []*databasev1.IndexRule{codeRule, serviceRule}, Entity: entityB, Families: famB},
	}

	tasks, err := discoverStreamPartTasks(context.Background(), []string{sourceGroupRoot})
	require.NoError(t, err)
	require.Len(t, tasks, 1)

	dstGroupRoot := filepath.Join(tmpDir, "target", group)
	tagProjection := []model.TagProjection{
		{Family: "fa", Names: []string{"status"}},
		{Family: "fb", Names: []string{"service", "code"}},
	}

	res, err := directCopyStreamGroup(
		context.Background(),
		"entry [1/1]",
		group,
		"hot",
		dstGroupRoot,
		ir, tagProjection,
		tasks, "",
		indexLocators,
	)
	require.NoError(t, err)
	require.EqualValues(t, srcRows, res.Rows, "stream invariant: target rows == source rows")

	d1Seg := formatStreamDirectCopySegName(day1, storage.DAY)
	d2Seg := formatStreamDirectCopySegName(day2, storage.DAY)
	require.NotEqual(t, d1Seg, d2Seg)

	d1Rows, _, vErr := VerifyShardParts(filepath.Join(dstGroupRoot, d1Seg, shardN), fileSystem)
	require.NoError(t, vErr)
	d2Rows, _, vErr := VerifyShardParts(filepath.Join(dstGroupRoot, d2Seg, shardN), fileSystem)
	require.NoError(t, vErr)
	require.EqualValues(t, 4, d1Rows, "day1 target must hold 4 elements (2 per stream)")
	require.EqualValues(t, 4, d2Rows, "day2 target must hold 4 elements (2 per stream)")
	require.EqualValues(t, srcRows, d1Rows+d2Rows, "no loss across target segs")

	assertIdxTerm := func(seg string, ruleID uint32, seriesID common.SeriesID, value string, want []uint64) {
		idxPath := filepath.Join(dstGroupRoot, seg, shardN, elementIndexFilename)
		info, statErr := os.Stat(idxPath)
		require.NoError(t, statErr, "target seg %s must have an idx/", seg)
		require.True(t, info.IsDir())
		store, openErr := inverted.NewStore(inverted.StoreOpts{Path: idxPath, BatchWaitSec: 0})
		require.NoError(t, openErr)
		defer func() { require.NoError(t, store.Close()) }()
		list, _, matchErr := store.MatchTerms(index.NewStringField(index.FieldKey{
			IndexRuleID: ruleID,
			SeriesID:    seriesID,
		}, value))
		require.NoError(t, matchErr)
		got := map[uint64]struct{}{}
		iter := list.Iterator()
		for iter.Next() {
			got[iter.Current()] = struct{}{}
		}
		require.NoError(t, iter.Close())
		require.Lenf(t, got, len(want), "seg %s rule %d value=%s wrong doc count", seg, ruleID, value)
		for _, id := range want {
			_, ok := got[id]
			require.Truef(t, ok, "seg %s rule %d value=%s missing elementID %d", seg, ruleID, value, id)
		}
	}

	// streamA "status": each seg holds only its own elements, no cross contamination.
	assertIdxTerm(d1Seg, statusRID, aSeriesID, "ok", []uint64{aD1ok})
	assertIdxTerm(d1Seg, statusRID, aSeriesID, "err", []uint64{aD1err})
	assertIdxTerm(d2Seg, statusRID, aSeriesID, "ok", []uint64{aD2ok})
	assertIdxTerm(d2Seg, statusRID, aSeriesID, "err", []uint64{aD2err})

	// streamB "code": likewise.
	assertIdxTerm(d1Seg, codeRID, bSeriesID, "200", []uint64{bD1ok})
	assertIdxTerm(d1Seg, codeRID, bSeriesID, "500", []uint64{bD1err})
	assertIdxTerm(d2Seg, codeRID, bSeriesID, "200", []uint64{bD2ok})
	assertIdxTerm(d2Seg, codeRID, bSeriesID, "500", []uint64{bD2err})

	// streamB indexed ENTITY tag "service" (value resolved from series index):
	// both rows of streamB in each seg carry service="svc-B".
	assertIdxTerm(d1Seg, serviceRID, bSeriesID, "svc-B", []uint64{bD1ok, bD1err})
	assertIdxTerm(d2Seg, serviceRID, bSeriesID, "svc-B", []uint64{bD2ok, bD2err})

	// Cross-stream isolation: streamA's seriesID must NOT appear under streamB's
	// rule and vice versa (different SeriesID in the FieldKey already enforces this,
	// but assert empties explicitly for the wrong (rule, series) combo).
	assertIdxTerm(d1Seg, codeRID, aSeriesID, "200", nil)
	assertIdxTerm(d1Seg, statusRID, bSeriesID, "ok", nil)

	require.GreaterOrEqual(t, res.Segments, 2, "expected at least two target segments")
	require.GreaterOrEqual(t, SlowPathHits(), int64(1),
		"slow path must have been exercised at least once for a cross-segment source part")
}

// TestMigrationElementIndexByteCopyCollisionFallsBackToRebuild proves the collision
// fallback in finalizeStreamElementIndex: when two distinct source (seg, shard) pairs
// both map ALL their rows to the same single target segment, the second one detects
// that the target idx directory already exists on disk (written by the first via
// byte-copy) and falls back to a rebuild instead of clobbering it. The final target
// idx must contain the UNION of both sources' element-index documents.
//
// Fixture: two source group roots (sourceA and sourceB), same group name. sourceA has
// seg-20260601/shard-0 with elementIDs 1001 and 1002; sourceB has seg-20260602/shard-0
// with elementIDs 2001 and 2002. Both carry timestamps inside 2026-06-01, so both map
// to a single target segment seg-20260601. The snapshot sorts states by srcSegName:
// seg-20260601 (sourceA) is processed first and its idx is byte-copied, creating the
// target idx dir on disk. seg-20260602 (sourceB) then detects existsOnDisk=true and
// falls back to rebuild, appending docs 2001/2002 to the existing bluge index.
// The final assertion verifies all four elementIDs are present in the target idx.
func TestMigrationElementIndexByteCopyCollisionFallsBackToRebuild(t *testing.T) {
	tmpDir := t.TempDir()

	const (
		group     = "sw_segment"
		streamN   = "sw_segment"
		shardN    = "shard-0"
		statusRID = uint32(11)
	)

	fileSystem := fs.NewLocalFileSystem()
	noFsync := migration.NoFsyncFS{FileSystem: fileSystem}
	ir := storage.IntervalRule{Unit: storage.DAY, Num: 1}

	// Both source segments have rows on the same calendar day so they both map to a
	// single target segment — this is what triggers the collision branch.
	targetDay := ir.Standard(time.Date(2026, 6, 1, 0, 0, 0, 0, time.Local))
	targetSegName := formatStreamDirectCopySegName(targetDay, storage.DAY)

	// srcASegName must sort BEFORE srcBSegName so the registry snapshot processes
	// sourceA first (byte-copy) then sourceB (collision → rebuild).
	srcASegName := formatStreamDirectCopySegName(time.Date(2026, 6, 1, 0, 0, 0, 0, time.Local), storage.DAY)
	srcBSegName := formatStreamDirectCopySegName(time.Date(2026, 6, 2, 0, 0, 0, 0, time.Local), storage.DAY)
	require.True(t, srcASegName < srcBSegName, "srcA must sort before srcB so byte-copy runs first")
	require.Equal(t, targetSegName, srcASegName, "sourceA seg name equals the target day bucket")

	// ElementIDs owned by each source shard (must be globally unique to prove union).
	const (
		aOk  = uint64(1001)
		aErr = uint64(1002)
		bOk  = uint64(2001)
		bErr = uint64(2002)
	)

	// Build sourceA: two rows inside targetDay, entity "svc-A".
	srcAGroupRoot := filepath.Join(tmpDir, "sourceA", group)
	srcAShardDir := filepath.Join(srcAGroupRoot, srcASegName, shardN)
	require.NoError(t, os.MkdirAll(srcAShardDir, storage.DirPerm))

	aTS0 := targetDay.Add(1 * time.Hour).UnixNano()
	aTS1 := targetDay.Add(2 * time.Hour).UnixNano()
	rowsA := []DumpRow{
		{Entity: "svc-A", Timestamp: aTS0, ElementID: aOk, Tags: []DumpTag{{Family: "default", Name: "status", Value: "ok"}}},
		{Entity: "svc-A", Timestamp: aTS1, ElementID: aErr, Tags: []DumpTag{{Family: "default", Name: "status", Value: "err"}}},
	}
	_, rowsA, cleanupA := BuildPartForDump(srcAShardDir, noFsync, uint64(1), rowsA)
	defer cleanupA()
	aSeriesID := rowsA[0].SeriesID

	// Seed sourceA's element index with real docs so the byte-copy carries content.
	srcAIdxDir := filepath.Join(srcAShardDir, elementIndexFilename)
	require.NoError(t, os.MkdirAll(srcAIdxDir, storage.DirPerm))
	seedStoreA, openErrA := inverted.NewStore(inverted.StoreOpts{Path: srcAIdxDir, BatchWaitSec: 0})
	require.NoError(t, openErrA)
	require.NoError(t, seedStoreA.Batch(index.Batch{Documents: index.Documents{
		{
			DocID: aOk, Timestamp: aTS0,
			Fields: []index.Field{index.NewStringField(index.FieldKey{IndexRuleID: statusRID, SeriesID: aSeriesID}, "ok")},
		},
		{
			DocID: aErr, Timestamp: aTS1,
			Fields: []index.Field{index.NewStringField(index.FieldKey{IndexRuleID: statusRID, SeriesID: aSeriesID}, "err")},
		},
	}}))
	require.NoError(t, seedStoreA.Close())

	// Build sourceB: two rows ALSO inside targetDay; source seg dir is named "day 2"
	// but the timestamps are still in 2026-06-01 — row routing is driven by actual
	// timestamps, not the source seg dir name.
	srcBGroupRoot := filepath.Join(tmpDir, "sourceB", group)
	srcBShardDir := filepath.Join(srcBGroupRoot, srcBSegName, shardN)
	require.NoError(t, os.MkdirAll(srcBShardDir, storage.DirPerm))

	bTS0 := targetDay.Add(3 * time.Hour).UnixNano()
	bTS1 := targetDay.Add(4 * time.Hour).UnixNano()
	rowsB := []DumpRow{
		{Entity: "svc-B", Timestamp: bTS0, ElementID: bOk, Tags: []DumpTag{{Family: "default", Name: "status", Value: "ok"}}},
		{Entity: "svc-B", Timestamp: bTS1, ElementID: bErr, Tags: []DumpTag{{Family: "default", Name: "status", Value: "err"}}},
	}
	_, rowsB, cleanupB := BuildPartForDump(srcBShardDir, noFsync, uint64(1), rowsB)
	defer cleanupB()
	bSeriesID := rowsB[0].SeriesID
	require.NotEqual(t, aSeriesID, bSeriesID, "distinct entity strings must produce distinct series IDs")

	// Seed sourceB's element index so finalizeStreamElementIndex sees a source idx dir
	// (its contents are irrelevant on the collision-rebuild path — the rebuild reads
	// from the part files and regenerates docs from rows).
	srcBIdxDir := filepath.Join(srcBShardDir, elementIndexFilename)
	require.NoError(t, os.MkdirAll(srcBIdxDir, storage.DirPerm))
	seedStoreB, openErrB := inverted.NewStore(inverted.StoreOpts{Path: srcBIdxDir, BatchWaitSec: 0})
	require.NoError(t, openErrB)
	require.NoError(t, seedStoreB.Close())

	// Single-stream index locators: non-entity "status" tag with INVERTED rule;
	// entity tag "name" is not indexed, so no entity resolution is needed and no
	// sidx is required during the rebuild.
	families := []*databasev1.TagFamilySpec{{
		Name: "default",
		Tags: []*databasev1.TagSpec{{Name: "status", Type: databasev1.TagType_TAG_TYPE_STRING}},
	}}
	entity := &databasev1.Entity{TagNames: []string{"name"}}
	statusRule := &databasev1.IndexRule{
		Metadata: &commonv1.Metadata{Id: statusRID, Name: "status"},
		Tags:     []string{"status"},
		Type:     databasev1.IndexRule_TYPE_INVERTED,
		Analyzer: index.AnalyzerKeyword,
	}
	locators, _ := partition.ParseIndexRuleLocators(entity, families, []*databasev1.IndexRule{statusRule}, false)
	indexLocators := map[string]*streamIndexLocator{
		streamN: {
			Locators:   locators,
			IndexRules: []*databasev1.IndexRule{statusRule},
			Entity:     entity,
			Families:   families,
		},
	}

	// Discover tasks from BOTH source roots and run the group copy.
	tasks, discoverErr := discoverStreamPartTasks(context.Background(), []string{srcAGroupRoot, srcBGroupRoot})
	require.NoError(t, discoverErr)
	require.Len(t, tasks, 2, "one task per source shard")

	dstGroupRoot := filepath.Join(tmpDir, "target", group)
	tagProjection := []model.TagProjection{{Family: "default", Names: []string{"status"}}}

	res, runErr := directCopyStreamGroup(
		context.Background(),
		"entry [collision-test]",
		group,
		"hot",
		dstGroupRoot,
		ir, tagProjection,
		tasks, "",
		indexLocators,
	)
	require.NoError(t, runErr)
	require.EqualValues(t, 4, res.Rows, "all four rows must reach the target")
	require.Equal(t, 1, res.Segments, "both sources collapse into a single target segment")

	// Row count sanity.
	totalRows, _, vErr := VerifyShardParts(filepath.Join(dstGroupRoot, targetSegName, shardN), fileSystem)
	require.NoError(t, vErr)
	require.EqualValues(t, 4, totalRows, "target shard must hold all four elements")

	// Verify that the target idx contains the UNION of both sources' element-index docs.
	// sourceA docs arrived via byte-copy; sourceB docs arrived via the collision rebuild.
	// Opening a new inverted.Store on the byte-copied path and writing to it (as the
	// rebuild does) opens the existing bluge index and adds new segments alongside the
	// copied ones, so both sets of docs are visible after close.
	assertIdxContains := func(seriesID common.SeriesID, value string, wantIDs []uint64) {
		idxPath := filepath.Join(dstGroupRoot, targetSegName, shardN, elementIndexFilename)
		info, statErr := os.Stat(idxPath)
		require.NoError(t, statErr, "target idx dir must exist")
		require.True(t, info.IsDir())
		store, openErr := inverted.NewStore(inverted.StoreOpts{Path: idxPath, BatchWaitSec: 0})
		require.NoError(t, openErr)
		defer func() { require.NoError(t, store.Close()) }()
		list, _, matchErr := store.MatchTerms(index.NewStringField(index.FieldKey{
			IndexRuleID: statusRID,
			SeriesID:    seriesID,
		}, value))
		require.NoError(t, matchErr)
		got := map[uint64]struct{}{}
		iter := list.Iterator()
		for iter.Next() {
			got[iter.Current()] = struct{}{}
		}
		require.NoError(t, iter.Close())
		require.Lenf(t, got, len(wantIDs),
			"series %d value=%s: wrong doc count (got %v, want %v)", seriesID, value, got, wantIDs)
		for _, id := range wantIDs {
			_, present := got[id]
			require.Truef(t, present, "series %d value=%s: missing elementID %d", seriesID, value, id)
		}
	}

	// sourceA docs (written by byte-copy): aOk → status=ok, aErr → status=err.
	assertIdxContains(aSeriesID, "ok", []uint64{aOk})
	assertIdxContains(aSeriesID, "err", []uint64{aErr})

	// sourceB docs (written by collision-fallback rebuild): bOk → status=ok, bErr → status=err.
	assertIdxContains(bSeriesID, "ok", []uint64{bOk})
	assertIdxContains(bSeriesID, "err", []uint64{bErr})
}
