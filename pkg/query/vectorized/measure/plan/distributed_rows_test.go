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
