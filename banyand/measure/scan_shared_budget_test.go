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
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

func TestSharedScanBudgetRejectsBeforeDecode(t *testing.T) {
	const limit = 8 << 20
	ctx := vectorized.WithQueryMemoryTracker(context.Background(), limit)
	shared := vectorized.QueryMemoryTracker(ctx, 0)
	budget := newScanBudget(8<<20, nil)
	if err := budget.attachShared(ctx, 6, 1024, 1); err != nil {
		t.Fatal(err)
	}
	bc := &blockCursor{bm: blockMetadata{count: 8, uncompressedSizeBytes: 128}}
	needed := budget.sharedBlockBytes(bc)
	other := int64(limit) - shared.Used() - needed + 1
	if err := shared.Reserve(other); err != nil {
		t.Fatal(err)
	}
	decoded := false
	load := cursorLoader(func(context.Context, *blockCursor, bool) (bool, error) { decoded = true; return true, nil })
	stats := scanStats{}
	pf := newPrefetcher(ctx, nil, budget, nil, &stats, &load, false, []*blockCursor{bc})
	_, err := pf.take(ctx, bc)
	if !errors.Is(err, vectorized.ErrMemoryBudgetExceeded) {
		t.Fatalf("wanted shared budget failure, got %v", err)
	}
	if decoded {
		t.Fatal("block decoded before shared reservation")
	}
	if budget.held.Load() != 0 {
		t.Fatal("failed reservation leaked decode bytes")
	}
	shared.Release(other)
	budget.releaseWorkspace()
	if shared.Used() != 0 {
		t.Fatalf("shared reservation leaked %d bytes", shared.Used())
	}
}

func TestSharedScanBudgetRetainsEnvelopeUntilCursorRelease(t *testing.T) {
	ctx := vectorized.WithQueryMemoryTracker(context.Background(), 8<<20)
	shared := vectorized.QueryMemoryTracker(ctx, 0)
	budget := newScanBudget(8<<20, nil)
	if err := budget.attachShared(ctx, 6, 1024, 1); err != nil {
		t.Fatal(err)
	}
	workspace := shared.Used()
	bc := &blockCursor{bm: blockMetadata{count: 8, uncompressedSizeBytes: 128}}
	reservation := scanReservation{sharedBytes: budget.sharedBlockBytes(bc)}
	if err := budget.reserveCurrent(ctx, 128, &reservation); err != nil {
		t.Fatal(err)
	}
	if shared.Used() != workspace+reservation.sharedBytes {
		t.Fatal("decode envelope not charged to shared tracker")
	}
	if budget.held.Load() != 128 {
		t.Fatal("existing raw-byte quota changed")
	}
	reservation.release()
	reservation.release()
	if shared.Used() != workspace || budget.held.Load() != 0 {
		t.Fatal("cursor reservation not released exactly once")
	}
	budget.releaseWorkspace()
	budget.releaseWorkspace()
	if shared.Used() != 0 {
		t.Fatal("workspace leaked")
	}
}

func TestBoundDecodeBackingDropsOversizedPooledBuffers(t *testing.T) {
	bc := &blockCursor{
		bm:         blockMetadata{count: 8, uncompressedSizeBytes: 128},
		timestamps: make([]int64, 0, 16384), versions: make([]int64, 0, 16384),
		fieldProjection: []string{"value"}, tagProjection: []model.TagProjection{{Family: "default", Names: []string{"id"}}},
	}
	temp := &block{timestamps: make([]int64, 0, 16384), versions: make([]int64, 0, 16384)}
	temp.field.columns = make([]column, 1)
	temp.field.columns[0].values = make([][]byte, 0, 16384)
	temp.field.columns[0].int64s = make([]int64, 0, 16384)
	temp.tagFamilies = make([]columnFamily, 1)
	temp.tagFamilies[0].columns = make([]column, 1)
	temp.tagFamilies[0].columns[0].float64s = make([]float64, 0, 16384)
	temp.reset()
	bc.boundDecodeBacking(temp)
	if cap(bc.timestamps) > 2048 || cap(bc.versions) > 2048 || cap(temp.timestamps) > 2048 || cap(temp.versions) > 2048 {
		t.Fatal("oversized metadata backing survived")
	}
	field := temp.field.columns[:cap(temp.field.columns)][0]
	tag := temp.tagFamilies[:cap(temp.tagFamilies)][0].columns[:1][0]
	if cap(field.values) > 2048 || cap(field.int64s) > 2048 || cap(tag.float64s) > 2048 {
		t.Fatal("oversized column backing survived")
	}
}

// The legacy (TopN) loaders compact a cursor that keeps under half its block, as the
// window scan does. The decoder backing that compaction detached must not stay on the
// cursor next to the compacted copy of its values.
func TestLegacyLoadDoesNotKeepDetachedBacking(t *testing.T) {
	defer setLegacyBatchScan(setLegacyBatchScan(true))
	var part fxPart
	for sid := common.SeriesID(1); sid <= 4; sid++ {
		part = append(part, fxRange{sid: sid, from: 1, to: 600, version: 1})
	}
	// One block of 600 rows per series: over 256 distinct urls, so the url column is not
	// dictionary encoded and decodes into the cursor's decoder backing.
	fx := newFixture(t, part)
	for _, tt := range []struct {
		maxTS     int64
		compacted bool
	}{
		{maxTS: 299, compacted: true},  // under half the block
		{maxTS: 300, compacted: false}, // exactly half: not compacted
	} {
		q := prefetchQuery(1, tt.maxTS, true, true)
		qr := fx.newResult(t, q, q.schema(t))
		t.Cleanup(qr.Release)
		require.NoError(t, qr.loadCursorsForBatch())
		require.Len(t, qr.data, 4)
		for _, bc := range qr.data {
			require.Len(t, bc.timestamps, int(tt.maxTS))
			require.Equal(t, tt.compacted, bc.compacted, "rows 1..%d", tt.maxTS)
			require.Zero(t, cap(bc.spareBacking), "rows 1..%d: the cursor kept the detached backing", tt.maxTS)
			if tt.compacted {
				require.Zero(t, bc.columnValuesDecoder.BackingCap())
			} else {
				require.Positive(t, bc.columnValuesDecoder.BackingCap())
			}
			url := bc.tagFamilies[0].columns[0].values
			require.Equal(t, fmt.Sprintf("/u/%d/%d/1", bc.bm.seriesID, tt.maxTS), string(url[len(url)-1]))
		}
	}
}

func TestSharedScanWorkspaceOutlivesSourceWhileCursorIsBorrowed(t *testing.T) {
	ctx := vectorized.WithQueryMemoryTracker(context.Background(), 8<<20)
	shared := vectorized.QueryMemoryTracker(ctx, 0)
	budget := newScanBudget(8<<20, nil)
	if err := budget.attachShared(ctx, 6, 1024, 1); err != nil {
		t.Fatal(err)
	}
	workspace := shared.Used()
	reservation := scanReservation{sharedBytes: 1024}
	if err := budget.reserveCurrent(ctx, 128, &reservation); err != nil {
		t.Fatal(err)
	}
	budget.releaseWorkspace()
	if shared.Used() != workspace+1024 {
		t.Fatal("source close released workspace retained by borrowed cursor")
	}
	reservation.release()
	if shared.Used() != 0 {
		t.Fatalf("last cursor release leaked %d bytes", shared.Used())
	}
}

func TestIndexBatchWorkspaceRejectsBeforeMaterialization(t *testing.T) {
	tracker := vectorized.NewMemoryTracker(1)
	ctx, scope := query.WithAllocationScope(context.Background(), tracker.Reserve, tracker.Release)
	defer scope.Close()
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{{Role: vectorized.RoleTimestamp, Type: vectorized.ColumnTypeInt64}})
	result := &indexSortResult{batchSchema: schema}
	result.segResults.results = []*segResult{{}}
	batch, err := result.PullBatch(ctx)
	if !errors.Is(err, vectorized.ErrMemoryBudgetExceeded) {
		t.Fatalf("wanted workspace rejection, got %v", err)
	}
	if batch != nil || tracker.Used() != 0 {
		t.Fatal("failed workspace allocation retained memory")
	}
}

func TestSharedScanWorkspaceFitsDefaultLeaseForTinyProjection(t *testing.T) {
	admitted, release, err := protector.QueryBudgetFor(nil).AdmitScanContext(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	ctx := vectorized.WithQueryMemoryTracker(admitted, 256<<20)
	budget := newScanBudget(256<<20, &decodePool{workers: 32})
	if err = budget.attachShared(ctx, 9, 1024, 1); err != nil {
		t.Fatalf("tiny query workspace exceeds default allowance: %v", err)
	}
	defer budget.releaseWorkspace()
	if budget.workspace > 3<<20 {
		t.Fatalf("unused decode workers were charged: %d", budget.workspace)
	}
}

func TestSharedScanWorkspaceCoversBoundedCellSlabs(t *testing.T) {
	ctx := vectorized.WithQueryMemoryTracker(context.Background(), 8<<20)
	budget := newScanBudget(8<<20, nil)
	const columns = 16
	if err := budget.attachShared(ctx, columns, 1, 1); err != nil {
		t.Fatal(err)
	}
	defer budget.releaseWorkspace()
	// Warmed storage pools may retain twice the canonical row capacity even
	// for a one-row pull. Source copies use one canonical slab; the lending
	// path instead has a bounded int64 conversion slab.
	const storageCells = 2 * (2 * vectorized.DefaultBatchSize) * 24
	const copiedCells = vectorized.DefaultBatchSize * 24
	const convertedCells = 2 * vectorized.DefaultBatchSize * 8
	const metadataAndScratch = 2*vectorized.DefaultBatchSize*128 + 256*1024 + 512
	for _, sourceCells := range []int{copiedCells, convertedCells} {
		required := int64(columns*(storageCells+sourceCells) + metadataAndScratch)
		if budget.workspace < required {
			t.Fatalf("workspace %d does not cover bounded slabs %d", budget.workspace, required)
		}
	}
}

func TestSharedBlockBytesChargesOnlyInRangeRows(t *testing.T) {
	ctx := vectorized.WithQueryMemoryTracker(context.Background(), 8<<20)
	budget := newScanBudget(8<<20, nil)
	if err := budget.attachShared(ctx, 6, 1024, 1); err != nil {
		t.Fatal(err)
	}
	defer budget.releaseWorkspace()
	const hour = int64(3600e9)
	newCursor := func(minTS, maxTS int64) *blockCursor {
		bc := &blockCursor{bm: blockMetadata{count: 360, uncompressedSizeBytes: 23 << 10}}
		bc.bm.timestamps.min, bc.bm.timestamps.max = 0, 6*hour
		bc.minTimestamp, bc.maxTimestamp = minTS, maxTS
		return bc
	}
	whole := budget.sharedBlockBytes(newCursor(0, 6*hour))
	narrow := budget.sharedBlockBytes(newCursor(hour, hour+hour/4))
	disjoint := budget.sharedBlockBytes(newCursor(7*hour, 8*hour))
	encoded := uint64(23 << 10)
	// 15 of 360 rows are in range and the floor is 16: the cursor is compacted. A window with
	// no row in range is charged the same floor.
	want := sharedRetainedBytes(6*cursorRowColumnBytes, 16, 360, encoded) + int64(decodeTransientFactor*encoded)
	if narrow != want || disjoint != want {
		t.Fatalf("narrow window charged %d, disjoint %d, want %d (whole block %d)", narrow, disjoint, want, whole)
	}
	if narrow >= whole {
		t.Fatalf("narrow window charged %d, not less than the whole block %d", narrow, whole)
	}
	unset := &blockCursor{bm: blockMetadata{count: 360, uncompressedSizeBytes: 23 << 10}}
	if got := budget.sharedBlockBytes(unset); got != whole {
		t.Fatalf("cursor without a range charged %d, want %d", got, whole)
	}
}

func TestSharedReservationTrueUpReturnsTransientExpansion(t *testing.T) {
	ctx := vectorized.WithQueryMemoryTracker(context.Background(), 64<<20)
	shared := vectorized.QueryMemoryTracker(ctx, 0)
	budget := newScanBudget(64<<20, nil)
	if err := budget.attachShared(ctx, 6, 1024, 1); err != nil {
		t.Fatal(err)
	}
	workspace := shared.Used()
	bc := &blockCursor{bm: blockMetadata{count: 600, uncompressedSizeBytes: 40 << 10}}
	reservation := scanReservation{sharedBytes: budget.sharedBlockBytes(bc)}
	if err := budget.reserveCurrent(ctx, bc.bm.uncompressedSizeBytes, &reservation); err != nil {
		t.Fatal(err)
	}
	before := reservation.sharedBytes
	reservation.trueUp(15, 600)
	want := sharedRetainedBytes(6*cursorRowColumnBytes, 15, 600, 40<<10)
	if reservation.sharedBytes != want || shared.Used() != workspace+want {
		t.Fatalf("after true-up: reservation %d, tracker %d, want %d over workspace %d", reservation.sharedBytes, shared.Used(), want, workspace)
	}
	if want >= before/4 {
		t.Fatalf("true-up kept %d of %d", want, before)
	}
	reservation.trueUp(600, 600) // never raises the reservation
	if reservation.sharedBytes != want {
		t.Fatalf("true-up raised the reservation to %d", reservation.sharedBytes)
	}
	reservation.release()
	reservation.release()
	if shared.Used() != workspace {
		t.Fatalf("leaked %d bytes after release", shared.Used()-workspace)
	}
	budget.releaseWorkspace()
}

// A merge over many series holds every cursor live; with the envelope of each trued up
// after decode, the set fits a quota that the pre-decode envelope of all of them exceeds.
func TestSharedReservationTrueUpLetsManyCursorsBeLive(t *testing.T) {
	const cursors = 100
	ctx := vectorized.WithQueryMemoryTracker(context.Background(), 96<<20)
	budget := newScanBudget(1<<30, nil)
	if err := budget.attachShared(ctx, 6, 1024, cursors); err != nil {
		t.Fatal(err)
	}
	defer budget.releaseWorkspace()
	var held []*scanReservation
	for i := 0; i < cursors; i++ {
		bc := &blockCursor{bm: blockMetadata{count: 611, uncompressedSizeBytes: 40 << 10}}
		r := &scanReservation{sharedBytes: budget.sharedBlockBytes(bc)}
		if err := budget.reserveCurrent(ctx, bc.bm.uncompressedSizeBytes, r); err != nil {
			t.Fatalf("cursor %d: %v", i, err)
		}
		r.trueUp(15, 611)
		held = append(held, r)
	}
	for _, r := range held {
		r.release()
	}
}

func TestProjectedRowBytesFollowsTheColumnTypes(t *testing.T) {
	q := prefetchQuery(0, 10, true, true) // tag url (string), field fi (int64)
	schema := q.schema(t)
	require.EqualValues(t, 16+24+8, projectedRowBytes(schema), "timestamp and version, one string, one int field")
	require.Zero(t, projectedRowBytes(nil))

	budget := &scanBudget{columns: 6}
	require.EqualValues(t, 6*cursorRowColumnBytes, budget.perRowBytes(), "without column types the count is used")
	budget.rowBytes = projectedRowBytes(schema)
	require.EqualValues(t, (16+24+8)*cursorRowFactor, budget.perRowBytes())
}
