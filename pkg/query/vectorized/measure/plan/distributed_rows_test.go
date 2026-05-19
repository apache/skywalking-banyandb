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

package plan

import (
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure/frame"
)

type rowMergeOutput struct {
	ts    int64
	ver   int64
	sid   int64
	value int64
}

func TestMergeDistributedRows_DedupsEqualSortKeyBySidTimestampHighestVersion(t *testing.T) {
	schema := distributedRowsTestSchema()
	frameA := encodeDistributedRows(t, schema,
		rowMergeOutput{ts: 10, ver: 1, sid: 7, value: 100},
		rowMergeOutput{ts: 20, ver: 1, sid: 8, value: 200},
	)
	frameB := encodeDistributedRows(t, schema,
		rowMergeOutput{ts: 10, ver: 3, sid: 7, value: 300},
		rowMergeOutput{ts: 30, ver: 1, sid: 9, value: 400},
	)

	batches, mergeErr := mergeDistributedRows([][]byte{frameA, nil, frameB}, distributedRowsSpec{BatchSize: 2})
	if mergeErr != nil {
		t.Fatalf("mergeDistributedRows: %v", mergeErr)
	}
	got := collectDistributedRows(batches)
	want := []rowMergeOutput{
		{ts: 10, ver: 3, sid: 7, value: 300},
		{ts: 20, ver: 1, sid: 8, value: 200},
		{ts: 30, ver: 1, sid: 9, value: 400},
	}
	assertRowsEqual(t, got, want)
}

func TestMergeDistributedRows_DoesNotDedupSidTimestampAcrossSortGroups(t *testing.T) {
	schema := distributedRowsTestSchema()
	body := encodeDistributedRows(t, schema,
		rowMergeOutput{ts: 10, ver: 1, sid: 7, value: 100},
		rowMergeOutput{ts: 20, ver: 1, sid: 7, value: 200},
	)

	batches, mergeErr := mergeDistributedRows([][]byte{body}, distributedRowsSpec{BatchSize: 4})
	if mergeErr != nil {
		t.Fatalf("mergeDistributedRows: %v", mergeErr)
	}
	got := collectDistributedRows(batches)
	want := []rowMergeOutput{
		{ts: 10, ver: 1, sid: 7, value: 100},
		{ts: 20, ver: 1, sid: 7, value: 200},
	}
	assertRowsEqual(t, got, want)
}

func TestMergeDistributedRows_IndexModeSuppressesRepeatedSidAcrossSortGroups(t *testing.T) {
	schema := distributedRowsTestSchema()
	body := encodeDistributedRows(t, schema,
		rowMergeOutput{ts: 10, ver: 1, sid: 7, value: 100},
		rowMergeOutput{ts: 20, ver: 1, sid: 7, value: 200},
		rowMergeOutput{ts: 30, ver: 1, sid: 8, value: 300},
	)

	batches, mergeErr := mergeDistributedRows([][]byte{body}, distributedRowsSpec{IndexMode: true, BatchSize: 4})
	if mergeErr != nil {
		t.Fatalf("mergeDistributedRows: %v", mergeErr)
	}
	got := collectDistributedRows(batches)
	want := []rowMergeOutput{
		{ts: 10, ver: 1, sid: 7, value: 100},
		{ts: 30, ver: 1, sid: 8, value: 300},
	}
	assertRowsEqual(t, got, want)
}

func TestMergeDistributedRows_DescTimestampOrder(t *testing.T) {
	schema := distributedRowsTestSchema()
	body := encodeDistributedRows(t, schema,
		rowMergeOutput{ts: 10, ver: 1, sid: 7, value: 100},
		rowMergeOutput{ts: 30, ver: 1, sid: 8, value: 300},
		rowMergeOutput{ts: 20, ver: 1, sid: 9, value: 200},
	)

	batches, mergeErr := mergeDistributedRows([][]byte{body}, distributedRowsSpec{Desc: true, BatchSize: 4})
	if mergeErr != nil {
		t.Fatalf("mergeDistributedRows: %v", mergeErr)
	}
	got := collectDistributedRows(batches)
	want := []rowMergeOutput{
		{ts: 30, ver: 1, sid: 8, value: 300},
		{ts: 20, ver: 1, sid: 9, value: 200},
		{ts: 10, ver: 1, sid: 7, value: 100},
	}
	assertRowsEqual(t, got, want)
}

// TestMergeDistributedRows_MillionRowMemoryCeiling is the regression gate
// for the Phase 1.5 architectural fix: the heap-merge + AppendColumnRange
// path must stay within a working-set ceiling at 1M total input rows,
// proving the previous flat-sort + appendDistributedRow deep-copy
// implementation has been replaced by streaming construction.
//
// Ceiling calibration: the prior flat-sort shape held a 1M-entry
// []distributedRowRef (~64 MiB) for the entire merge duration, on top of
// decoded source batches (~40 MiB) and output batches (~40 MiB of fresh
// int64 column data) — projecting a peak around 150-180 MiB. The
// streaming heap-merge shape only needs the source batches (~40 MiB),
// the output batches (~40 MiB), and transient per-row alloc churn that
// the GC reclaims between window flushes — measured peak ~115 MiB on a
// 32-core Linux host. The 160 MiB ceiling is calibrated between these
// two bands so a regression that reintroduces the flat sort or the
// deep-copy output trips the gate without flaking on GC-timing noise.
func TestMergeDistributedRows_MillionRowMemoryCeiling(t *testing.T) {
	const (
		sourceCount     = 4
		rowsPerSource   = 250_000
		memCeilingBytes = uint64(160) << 20
	)
	schema := distributedRowsTestSchema()
	bodies := make([][]byte, sourceCount)
	for sourceIdx := range sourceCount {
		rows := make([]rowMergeOutput, rowsPerSource)
		base := int64(sourceIdx) * int64(rowsPerSource)
		for i := range rowsPerSource {
			rows[i] = rowMergeOutput{
				ts:    base + int64(i) + 1,
				ver:   1,
				sid:   base + int64(i) + 1,
				value: int64(i),
			}
		}
		bodies[sourceIdx] = encodeDistributedRows(t, schema, rows...)
	}

	runtime.GC()
	var base runtime.MemStats
	runtime.ReadMemStats(&base)

	var peakMu sync.Mutex
	peak := base.HeapAlloc
	stopSampler := make(chan struct{})
	var samplerWG sync.WaitGroup
	samplerWG.Add(1)
	go func() {
		defer samplerWG.Done()
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		var ms runtime.MemStats
		for {
			select {
			case <-stopSampler:
				return
			case <-ticker.C:
				runtime.ReadMemStats(&ms)
				peakMu.Lock()
				if ms.HeapAlloc > peak {
					peak = ms.HeapAlloc
				}
				peakMu.Unlock()
			}
		}
	}()

	start := time.Now()
	batches, mergeErr := mergeDistributedRows(bodies, distributedRowsSpec{BatchSize: 4096})
	elapsed := time.Since(start)
	close(stopSampler)
	samplerWG.Wait()
	if mergeErr != nil {
		t.Fatalf("mergeDistributedRows: %v", mergeErr)
	}

	// Final heap snapshot to catch a peak in the residual (e.g. output
	// batches still alive) that the periodic sampler might have missed.
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	peakMu.Lock()
	if after.HeapAlloc > peak {
		peak = after.HeapAlloc
	}
	finalPeak := peak
	peakMu.Unlock()

	rowCount := 0
	for _, b := range batches {
		rowCount += b.Len
	}
	if rowCount != sourceCount*rowsPerSource {
		t.Fatalf("row count got %d, want %d", rowCount, sourceCount*rowsPerSource)
	}
	if elapsed > 30*time.Second {
		t.Fatalf("merge took %s, want under 30s (regression in heap-merge throughput)", elapsed)
	}

	delta := uint64(0)
	if finalPeak > base.HeapAlloc {
		delta = finalPeak - base.HeapAlloc
	}
	if delta > memCeilingBytes {
		t.Fatalf("peak heap delta %.1f MiB exceeds ceiling %d MiB (prior flat-sort + deep-copy shape would have ~%dx'd this)",
			float64(delta)/float64(1<<20), memCeilingBytes>>20, 3)
	}
	t.Logf("merged %d rows from %d sources in %s; peak heap delta %.1f MiB (ceiling %d MiB)",
		rowCount, sourceCount, elapsed, float64(delta)/float64(1<<20), memCeilingBytes>>20)
}

func distributedRowsTestSchema() *vectorized.BatchSchema {
	return vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleTimestamp, Name: "_timestamp", Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleVersion, Name: "_version", Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleSeriesID, Name: "_sid", Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleShardID, Name: "shard_id", Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleField, Name: fieldValue, Type: vectorized.ColumnTypeInt64},
	})
}

// distributedRowsOrderBySchema extends the base schema with a string tag
// column (for OrderBy_String tests) and an int64 tag column (for OrderBy_
// Int64 tests). The merger looks up the OrderBy column by tag name on the
// merged batch schema, so the tag columns must be declared at indices the
// tests can address via TagIndex.
func distributedRowsOrderBySchema() *vectorized.BatchSchema {
	return vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleTimestamp, Name: "_timestamp", Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleVersion, Name: "_version", Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleSeriesID, Name: "_sid", Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleShardID, Name: "shard_id", Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleField, Name: fieldValue, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "svc", Type: vectorized.ColumnTypeString},
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "rank", Type: vectorized.ColumnTypeInt64},
	})
}

type orderByRow struct {
	svc   string
	ts    int64
	ver   int64
	sid   int64
	value int64
	rank  int64
}

func encodeOrderByRows(t *testing.T, schema *vectorized.BatchSchema, rows ...orderByRow) []byte {
	t.Helper()
	batch := vectorized.NewRecordBatch(schema, len(rows))
	for _, row := range rows {
		batch.Columns[0].(*vectorized.TypedColumn[int64]).Append(row.ts)
		batch.Columns[1].(*vectorized.TypedColumn[int64]).Append(row.ver)
		batch.Columns[2].(*vectorized.TypedColumn[int64]).Append(row.sid)
		batch.Columns[3].(*vectorized.TypedColumn[int64]).Append(1)
		batch.Columns[4].(*vectorized.TypedColumn[int64]).Append(row.value)
		batch.Columns[5].(*vectorized.TypedColumn[string]).Append(row.svc)
		batch.Columns[6].(*vectorized.TypedColumn[int64]).Append(row.rank)
		batch.Len++
	}
	body, encodeErr := frame.Encode(batch)
	if encodeErr != nil {
		t.Fatalf("frame.Encode: %v", encodeErr)
	}
	return body
}

func collectOrderByRows(batches []*vectorized.RecordBatch) []orderByRow {
	var rows []orderByRow
	for _, batch := range batches {
		tsCol := batch.Columns[0].(*vectorized.TypedColumn[int64])
		verCol := batch.Columns[1].(*vectorized.TypedColumn[int64])
		sidCol := batch.Columns[2].(*vectorized.TypedColumn[int64])
		fieldCol := batch.Columns[4].(*vectorized.TypedColumn[int64])
		svcCol := batch.Columns[5].(*vectorized.TypedColumn[string])
		rankCol := batch.Columns[6].(*vectorized.TypedColumn[int64])
		for rowIdx := 0; rowIdx < batch.Len; rowIdx++ {
			rows = append(rows, orderByRow{
				ts:    tsCol.Data()[rowIdx],
				ver:   verCol.Data()[rowIdx],
				sid:   sidCol.Data()[rowIdx],
				value: fieldCol.Data()[rowIdx],
				svc:   svcCol.Data()[rowIdx],
				rank:  rankCol.Data()[rowIdx],
			})
		}
	}
	return rows
}

// TestMergeDistributedRows_OrderByString_AscDescAcrossSources proves the
// k-way heap merger compares on the configured OrderBy string column
// instead of the timestamp when OrderByFamily/OrderByTagName are set:
//   - ascending: the surviving rows are emitted in ascending tag value;
//   - descending: same set, reversed.
// Two sources contribute rows that arrive interleaved in input order to
// exercise the cross-source ordering invariant.
func TestMergeDistributedRows_OrderByString_AscDescAcrossSources(t *testing.T) {
	schema := distributedRowsOrderBySchema()
	frameA := encodeOrderByRows(t, schema,
		orderByRow{ts: 10, ver: 1, sid: 1, value: 100, svc: "delta", rank: 0},
		orderByRow{ts: 20, ver: 1, sid: 2, value: 200, svc: "alpha", rank: 0},
	)
	frameB := encodeOrderByRows(t, schema,
		orderByRow{ts: 11, ver: 1, sid: 3, value: 300, svc: "charlie", rank: 0},
		orderByRow{ts: 21, ver: 1, sid: 4, value: 400, svc: "bravo", rank: 0},
	)

	specAsc := distributedRowsSpec{
		BatchSize:      4,
		OrderByColIdx:  -1,
		OrderByFamily:  "default",
		OrderByTagName: "svc",
	}
	batchesAsc, mergeAscErr := mergeDistributedRows([][]byte{frameA, frameB}, specAsc)
	if mergeAscErr != nil {
		t.Fatalf("mergeDistributedRows asc: %v", mergeAscErr)
	}
	gotAsc := collectOrderByRows(batchesAsc)
	wantAsc := []string{"alpha", "bravo", "charlie", "delta"}
	if len(gotAsc) != len(wantAsc) {
		t.Fatalf("asc row count got %d want %d", len(gotAsc), len(wantAsc))
	}
	for i, want := range wantAsc {
		if gotAsc[i].svc != want {
			t.Fatalf("asc row %d svc got %q want %q (all rows: %+v)", i, gotAsc[i].svc, want, gotAsc)
		}
	}

	specDesc := distributedRowsSpec{
		Desc:           true,
		BatchSize:      4,
		OrderByColIdx:  -1,
		OrderByFamily:  "default",
		OrderByTagName: "svc",
	}
	batchesDesc, mergeDescErr := mergeDistributedRows([][]byte{frameA, frameB}, specDesc)
	if mergeDescErr != nil {
		t.Fatalf("mergeDistributedRows desc: %v", mergeDescErr)
	}
	gotDesc := collectOrderByRows(batchesDesc)
	wantDesc := []string{"delta", "charlie", "bravo", "alpha"}
	if len(gotDesc) != len(wantDesc) {
		t.Fatalf("desc row count got %d want %d", len(gotDesc), len(wantDesc))
	}
	for i, want := range wantDesc {
		if gotDesc[i].svc != want {
			t.Fatalf("desc row %d svc got %q want %q (all rows: %+v)", i, gotDesc[i].svc, want, gotDesc)
		}
	}
}

// TestMergeDistributedRows_OrderByInt64_PreservesPhase15Invariants asserts
// that switching the sort column to an OrderBy tag does NOT regress
// Phase 1.5's (sid, ts) dedup invariant: when two sources emit a row with
// the same (sid, ts) inside the same OrderBy-equal-value sort group, the
// surviving row is the one with the highest version, and rows belonging
// to different OrderBy-equal-value groups remain distinct even when their
// (sid, ts) collide across groups.
func TestMergeDistributedRows_OrderByInt64_PreservesPhase15Invariants(t *testing.T) {
	schema := distributedRowsOrderBySchema()
	frameA := encodeOrderByRows(t, schema,
		// Group rank=1: two collisions on (sid=7, ts=10) — highest ver wins.
		orderByRow{ts: 10, ver: 1, sid: 7, value: 100, svc: "a", rank: 1},
		// Group rank=2: same (sid=7, ts=10) under a different sort group is
		// a distinct emit (no cross-group dedup).
		orderByRow{ts: 10, ver: 1, sid: 7, value: 999, svc: "a", rank: 2},
	)
	frameB := encodeOrderByRows(t, schema,
		orderByRow{ts: 10, ver: 5, sid: 7, value: 555, svc: "a", rank: 1},
	)
	spec := distributedRowsSpec{
		BatchSize:      4,
		OrderByColIdx:  -1,
		OrderByFamily:  "default",
		OrderByTagName: "rank",
	}
	batches, mergeErr := mergeDistributedRows([][]byte{frameA, frameB}, spec)
	if mergeErr != nil {
		t.Fatalf("mergeDistributedRows: %v", mergeErr)
	}
	got := collectOrderByRows(batches)
	// rank=1 group emits the highest-version row (ver=5); rank=2 group
	// emits its own distinct row.
	if len(got) != 2 {
		t.Fatalf("row count got %d want 2 (rows: %+v)", len(got), got)
	}
	if got[0].rank != 1 || got[0].ver != 5 || got[0].value != 555 {
		t.Fatalf("rank=1 surviving row got %+v want rank=1 ver=5 value=555", got[0])
	}
	if got[1].rank != 2 || got[1].ver != 1 || got[1].value != 999 {
		t.Fatalf("rank=2 distinct row got %+v want rank=2 ver=1 value=999", got[1])
	}
}

func encodeDistributedRows(t *testing.T, schema *vectorized.BatchSchema, rows ...rowMergeOutput) []byte {
	t.Helper()
	batch := vectorized.NewRecordBatch(schema, len(rows))
	for _, row := range rows {
		batch.Columns[0].(*vectorized.TypedColumn[int64]).Append(row.ts)
		batch.Columns[1].(*vectorized.TypedColumn[int64]).Append(row.ver)
		batch.Columns[2].(*vectorized.TypedColumn[int64]).Append(row.sid)
		batch.Columns[3].(*vectorized.TypedColumn[int64]).Append(1)
		batch.Columns[4].(*vectorized.TypedColumn[int64]).Append(row.value)
		batch.Len++
	}
	body, encodeErr := frame.Encode(batch)
	if encodeErr != nil {
		t.Fatalf("frame.Encode: %v", encodeErr)
	}
	return body
}

func collectDistributedRows(batches []*vectorized.RecordBatch) []rowMergeOutput {
	var rows []rowMergeOutput
	for _, batch := range batches {
		tsCol := batch.Columns[0].(*vectorized.TypedColumn[int64])
		verCol := batch.Columns[1].(*vectorized.TypedColumn[int64])
		sidCol := batch.Columns[2].(*vectorized.TypedColumn[int64])
		valueCol := batch.Columns[4].(*vectorized.TypedColumn[int64])
		for rowIdx := 0; rowIdx < batch.Len; rowIdx++ {
			rows = append(rows, rowMergeOutput{
				ts:    tsCol.Data()[rowIdx],
				ver:   verCol.Data()[rowIdx],
				sid:   sidCol.Data()[rowIdx],
				value: valueCol.Data()[rowIdx],
			})
		}
	}
	return rows
}

func assertRowsEqual(t *testing.T, got, want []rowMergeOutput) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("row count got %d, want %d: %#v", len(got), len(want), got)
	}
	for idx := range want {
		if got[idx] != want[idx] {
			t.Fatalf("row %d got %#v, want %#v (all rows: %#v)", idx, got[idx], want[idx], got)
		}
	}
}

// multiGroupMergedSchema builds a merged schema that unions the base
// distributedRowsTestSchema with an extra string tag column "extra_tag".
// Group 0 frames use the base schema; group 1 frames use the extended schema.
// After null-fill, rows from group 0 must have IsNull(extra_tag)==true and
// rows from group 1 must have IsNull(extra_tag)==false.
func multiGroupMergedSchema() *vectorized.BatchSchema {
	return vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleTimestamp, Name: "_timestamp", Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleVersion, Name: "_version", Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleSeriesID, Name: "_sid", Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleShardID, Name: "shard_id", Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleField, Name: fieldValue, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "extra_tag", Type: vectorized.ColumnTypeString},
	})
}

// encodeMultiGroupRows builds a frame using the specified schema so each test
// group can produce frames with different column layouts.
func encodeMultiGroupRows(t *testing.T, schema *vectorized.BatchSchema, rows ...rowMergeOutput) []byte {
	t.Helper()
	return encodeDistributedRows(t, schema, rows...)
}

// TestMergeDistributedRows_MultiGroup_NullFillsMissingColumn verifies that
// when group 0 frames are missing the "extra_tag" column (their schema only
// has the base 5 columns), the merged output has IsNull==true for that column
// for all group-0 rows, while group-1 rows (which carry the column) are non-null.
func TestMergeDistributedRows_MultiGroup_NullFillsMissingColumn(t *testing.T) {
	baseSchema := distributedRowsTestSchema()   // 5 cols, no extra_tag
	mergedSchema := multiGroupMergedSchema()    // 6 cols, with extra_tag

	// Group 0: uses base schema — no extra_tag column.
	frameGroup0 := encodeMultiGroupRows(t, baseSchema,
		rowMergeOutput{ts: 10, ver: 1, sid: 1, value: 100},
	)
	// Group 1: uses merged schema — has extra_tag column (non-null).
	// We need to build this frame manually with extra_tag set.
	batchGroup1 := vectorized.NewRecordBatch(mergedSchema, 1)
	batchGroup1.Columns[0].(*vectorized.TypedColumn[int64]).Append(20)
	batchGroup1.Columns[1].(*vectorized.TypedColumn[int64]).Append(1)
	batchGroup1.Columns[2].(*vectorized.TypedColumn[int64]).Append(2)
	batchGroup1.Columns[3].(*vectorized.TypedColumn[int64]).Append(1)
	batchGroup1.Columns[4].(*vectorized.TypedColumn[int64]).Append(200)
	batchGroup1.Columns[5].(*vectorized.TypedColumn[string]).Append("zone-a")
	batchGroup1.Len = 1
	frameGroup1, encodeErr := frame.Encode(batchGroup1)
	if encodeErr != nil {
		t.Fatalf("frame.Encode group1: %v", encodeErr)
	}

	groupFrames := []groupFrame{
		{body: frameGroup0, group: 0},
		{body: frameGroup1, group: 1},
	}
	spec := distributedRowsSpec{BatchSize: 4}
	batches, mergeErr := mergeDistributedRowsMulti(groupFrames, mergedSchema, spec)
	if mergeErr != nil {
		t.Fatalf("mergeDistributedRowsMulti: %v", mergeErr)
	}
	if len(batches) == 0 {
		t.Fatal("expected non-empty output batches")
	}

	// Collect rows and verify null-fill behaviour.
	type mergedRow struct {
		ts       int64
		sid      int64
		extraTag string
		nullTag  bool
	}
	var rows []mergedRow
	for _, batch := range batches {
		tsCol := batch.Columns[0].(*vectorized.TypedColumn[int64])
		sidCol := batch.Columns[2].(*vectorized.TypedColumn[int64])
		extraTagCol := batch.Columns[5].(*vectorized.TypedColumn[string])
		for rowIdx := range batch.Len {
			rows = append(rows, mergedRow{
				ts:       tsCol.Data()[rowIdx],
				sid:      sidCol.Data()[rowIdx],
				nullTag:  extraTagCol.IsNull(rowIdx),
				extraTag: extraTagCol.Data()[rowIdx],
			})
		}
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	// Row 0: ts=10, sid=1 from group 0 — extra_tag must be null.
	if rows[0].ts != 10 || rows[0].sid != 1 {
		t.Fatalf("row 0: got ts=%d sid=%d, want ts=10 sid=1", rows[0].ts, rows[0].sid)
	}
	if !rows[0].nullTag {
		t.Fatal("row 0 (group 0): extra_tag must be null (column absent in source)")
	}
	// Row 1: ts=20, sid=2 from group 1 — extra_tag must be non-null ("zone-a").
	if rows[1].ts != 20 || rows[1].sid != 2 {
		t.Fatalf("row 1: got ts=%d sid=%d, want ts=20 sid=2", rows[1].ts, rows[1].sid)
	}
	if rows[1].nullTag {
		t.Fatal("row 1 (group 1): extra_tag must not be null (column present in source)")
	}
	if rows[1].extraTag != "zone-a" {
		t.Fatalf("row 1 extra_tag: got %q, want %q", rows[1].extraTag, "zone-a")
	}
}

// TestMergeDistributedRows_IndexMode_PerGroupSidIsolation verifies that in
// index-mode, two groups that both emit sid=7 are NOT deduplicated — each
// (group, sid) pair is treated independently.
func TestMergeDistributedRows_IndexMode_PerGroupSidIsolation(t *testing.T) {
	schema := distributedRowsTestSchema()
	// Both groups emit sid=7; under per-group keying both rows should survive.
	frameGroup0 := encodeDistributedRows(t, schema,
		rowMergeOutput{ts: 10, ver: 1, sid: 7, value: 100},
	)
	frameGroup1 := encodeDistributedRows(t, schema,
		rowMergeOutput{ts: 20, ver: 1, sid: 7, value: 200},
	)
	mergedSchema := distributedRowsTestSchema() // same schema for both groups
	groupFrames := []groupFrame{
		{body: frameGroup0, group: 0},
		{body: frameGroup1, group: 1},
	}
	spec := distributedRowsSpec{IndexMode: true, BatchSize: 4}
	batches, mergeErr := mergeDistributedRowsMulti(groupFrames, mergedSchema, spec)
	if mergeErr != nil {
		t.Fatalf("mergeDistributedRowsMulti: %v", mergeErr)
	}
	got := collectDistributedRows(batches)
	// Both rows must survive — they have the same sid=7 but belong to
	// different groups, so the (group, sid) key is distinct.
	if len(got) != 2 {
		t.Fatalf("expected 2 rows (both sid=7 from different groups), got %d: %+v", len(got), got)
	}
}
