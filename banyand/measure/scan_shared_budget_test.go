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
	"testing"

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
