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
	"context"
	"testing"

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

func TestMergeStandaloneBatches_OrdersDeduplicatesAndReleases(t *testing.T) {
	ctx := vectorized.WithQueryMemoryTracker(context.Background(), 16*1024*1024)
	tracker := vectorized.QueryMemoryTracker(ctx, 0)
	schema := distributedRowsTestSchema()
	a := buildDistributedRowsBatchDirect(schema, []rowMergeOutput{{ts: 10, ver: 1, sid: 7, value: 100}, {ts: 30, ver: 1, sid: 9, value: 400}})
	b := buildDistributedRowsBatchDirect(schema, []rowMergeOutput{{ts: 10, ver: 3, sid: 7, value: 300}, {ts: 20, ver: 1, sid: 8, value: 200}})
	var groups []*BatchResult
	for _, batch := range []*vectorized.RecordBatch{a, b} {
		result, err := NewBatchResult(ctx, &batchSliceSource{schema: schema, batches: []*vectorized.RecordBatch{batch}}, schema, 1024)
		if err != nil {
			t.Fatal(err)
		}
		groups = append(groups, result)
	}
	result, err := MergeStandaloneBatches(ctx, groups, schema, &measurev1.QueryRequest{}, nil, execCfg())
	if err != nil {
		t.Fatal(err)
	}
	var batches []*vectorized.RecordBatch
	for {
		batch, nextErr := result.NextBatch(ctx)
		if nextErr != nil {
			t.Fatal(nextErr)
		}
		if batch == nil {
			break
		}
		batches = append(batches, batch)
	}
	assertRowsEqual(t, collectDistributedRows(batches), []rowMergeOutput{
		{ts: 10, ver: 3, sid: 7, value: 300},
		{ts: 20, ver: 1, sid: 8, value: 200},
		{ts: 30, ver: 1, sid: 9, value: 400},
	})
	if tracker.Used() == 0 {
		t.Fatal("retained output is uncharged")
	}
	if err = result.Close(); err != nil {
		t.Fatal(err)
	}
	if tracker.Used() != 0 {
		t.Fatalf("reservation leaked: %d", tracker.Used())
	}
}

func TestMergeStandaloneBatches_PromotesMissingColumnPassthrough(t *testing.T) {
	ctx := vectorized.WithQueryMemoryTracker(context.Background(), 16<<20)
	schema := distributedRowsTestSchema()
	native := buildDistributedRowsBatchDirect(schema, []rowMergeOutput{{ts: 10, ver: 1, sid: 1, value: 42}})
	defs := append([]vectorized.ColumnDef(nil), schema.Columns...)
	defs[4].Type = vectorized.ColumnTypeFieldValue
	missingSchema := vectorized.NewBatchSchema(defs)
	missing := vectorized.NewRecordBatch(missingSchema, 1)
	for idx, value := range []int64{20, 1, 2, 1} {
		missing.Columns[idx].(*vectorized.TypedColumn[int64]).Append(value)
	}
	missing.Columns[4].AppendNull()
	missing.Len = 1
	var groups []*BatchResult
	for _, batch := range []*vectorized.RecordBatch{native, missing} {
		result, err := NewBatchResult(ctx, &batchSliceSource{schema: batch.Schema, batches: []*vectorized.RecordBatch{batch}}, batch.Schema, 1024)
		if err != nil {
			t.Fatal(err)
		}
		groups = append(groups, result)
	}
	result, err := MergeStandaloneBatches(ctx, groups, schema, &measurev1.QueryRequest{}, nil, execCfg())
	if err != nil {
		t.Fatal(err)
	}
	defer result.Close()
	if result.OutputSchema().Columns[4].Type != vectorized.ColumnTypeFieldValue {
		t.Fatal("missing column was not promoted")
	}
	batch, err := result.NextBatch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	values := batch.Columns[4].(*vectorized.TypedColumn[*modelv1.FieldValue])
	if values.Data()[0].GetInt().GetValue() != 42 || !values.IsNull(1) {
		t.Fatal("native value or missing-column null changed")
	}
}

type standaloneTestOwner struct{ refs int }

func (o *standaloneTestOwner) Retain()  { o.refs++ }
func (o *standaloneTestOwner) Release() { o.refs-- }

func TestMergeStandaloneBatches_ReleasesPipelineOutputOwnerPins(t *testing.T) {
	ctx := vectorized.WithQueryMemoryTracker(context.Background(), 16<<20)
	baseline := vectorized.OutstandingOwnerPins()
	schema := distributedRowsTestSchema()
	owner := &standaloneTestOwner{}
	var groups []*BatchResult
	for idx := range 2 {
		batch := buildDistributedRowsBatchDirect(schema, []rowMergeOutput{{ts: int64(idx), ver: 1, sid: int64(idx), value: 42}})
		batch.Owners.Pin(owner)
		scan := NewScan(schema, ScanParams{})
		scan.Source = &fakePullSource{schema: schema, batches: []*vectorized.RecordBatch{batch}}
		result, err := ExecuteBatches(ctx, NewLimit(scan, 0, 10), execCfg())
		if err != nil {
			t.Fatal(err)
		}
		groups = append(groups, result)
	}
	result, err := MergeStandaloneBatches(ctx, groups, schema, &measurev1.QueryRequest{}, nil, execCfg())
	if err != nil {
		t.Fatal(err)
	}
	iterator, err := result.NativeIterator(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for iterator.Next() {
		_ = iterator.Current()
	}
	if err = iterator.Close(); err != nil {
		t.Fatal(err)
	}
	if got := vectorized.OutstandingOwnerPins(); got != baseline {
		t.Fatalf("owner pins leaked: before=%d after=%d", baseline, got)
	}
	if owner.refs != 0 {
		t.Fatalf("owner retained %d references", owner.refs)
	}
}

func TestAllocationScopeSourceRetainsCopiedViewsAfterGroupClose(t *testing.T) {
	tracker := vectorized.NewMemoryTracker(4096)
	ctx, scope := query.WithAllocationScope(context.Background(), tracker.Reserve, tracker.Release)
	if err := query.ReserveAllocation(ctx, 1024); err != nil {
		t.Fatal(err)
	}
	schema := distributedRowsTestSchema()
	batch := buildDistributedRowsBatchDirect(schema, []rowMergeOutput{{ts: 10, sid: 1, ver: 1, value: 42}})
	source := newAllocationScopeSource(&scopedAllocationTestSource{fakePullSource: fakePullSource{schema: schema, batches: []*vectorized.RecordBatch{batch}}}, scope)
	output, err := source.NextBatch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	view := &vectorized.RecordBatch{}
	view.Owners.PinAll(&output.Owners)
	output.ReleaseOwners()
	if err = source.Close(); err != nil {
		t.Fatal(err)
	}
	if tracker.Used() != 1536 {
		t.Fatal("group close released allocations still retained by merged output")
	}
	view.ReleaseOwners()
	if tracker.Used() != 0 {
		t.Fatal("retained allocation scope leaked")
	}
}

type scopedAllocationTestSource struct{ fakePullSource }

func (s *scopedAllocationTestSource) NextBatch(ctx context.Context) (*vectorized.RecordBatch, error) {
	if err := query.ReserveAllocation(ctx, 512); err != nil {
		return nil, err
	}
	return s.fakePullSource.NextBatch(ctx)
}
