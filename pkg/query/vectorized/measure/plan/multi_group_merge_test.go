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
	"fmt"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
	logicalmeasure "github.com/apache/skywalking-banyandb/pkg/query/logical/measure"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	measure "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure/frame"
)

// mgRowSpec is one stored (ts, SID, version, value) row.
type mgRowSpec struct {
	tag                     string
	ts, sid, version, value int64
}

// mgBatchEC serves each storage batch of rows as one MeasureBatch.
type mgBatchEC struct {
	schema  *databasev1.Measure
	result  *mgBatchResult
	batches [][]mgRowSpec
}

func (e *mgBatchEC) Query(_ context.Context, opts model.MeasureQueryOptions) (model.MeasureQueryResult, error) {
	schema, schemaErr := measure.BuildBatchSchema(e.schema, opts)
	if schemaErr != nil {
		return nil, schemaErr
	}
	result := &mgBatchResult{}
	e.result = result
	for _, rows := range e.batches {
		batch := &model.MeasureBatch{Schema: schema}
		for _, row := range rows {
			batch.Timestamps = append(batch.Timestamps, row.ts)
			batch.Versions = append(batch.Versions, row.version)
			batch.ShardIDs = append(batch.ShardIDs, 0)
			batch.SeriesIDs = append(batch.SeriesIDs, common.SeriesID(row.sid))
		}
		for _, def := range schema.Columns {
			switch def.Role {
			case vectorized.RoleTag:
				col := vectorized.NewColumnForType(def.Type, len(rows))
				for _, row := range rows {
					tag := row.tag
					if tag == "" {
						tag = "x"
					}
					col.(*vectorized.TypedColumn[string]).Append(tag)
				}
				batch.Tags = append(batch.Tags, col)
			case vectorized.RoleField:
				col := vectorized.NewColumnForType(def.Type, len(rows))
				for _, row := range rows {
					col.(*vectorized.TypedColumn[int64]).Append(row.value)
				}
				batch.Fields = append(batch.Fields, col)
			case vectorized.RoleTimestamp, vectorized.RoleVersion, vectorized.RoleSeriesID,
				vectorized.RoleShardID, vectorized.RoleElementID, vectorized.RoleOrderKey:
			}
		}
		result.batches = append(result.batches, batch)
	}
	return result, nil
}

type mgBatchResult struct {
	batches  []*model.MeasureBatch
	next     int
	released int
}

func (r *mgBatchResult) Pull() *model.MeasureResult { return nil }

func (r *mgBatchResult) PullBatch(context.Context) (*model.MeasureBatch, error) {
	if r.next >= len(r.batches) {
		return nil, nil
	}
	batch := r.batches[r.next]
	r.next++
	return batch, nil
}

func (r *mgBatchResult) Release() { r.released++ }

// TestMultiGroupMerge_MaterializesRowsOnce pins that the groups are merged as batches: the rows
// are converted to InternalDataPoints once, after the merge, and only the rows that are returned.
// A group turned into rows before the merge would add its own rows to the count.
func TestMultiGroupMerge_MaterializesRowsOnce(t *testing.T) {
	cfg := measure.VectorizedConfig{BatchSize: 1, QueryMemoryMiB: 16}
	groups := []*mgBatchEC{
		{schema: testMeasureSchemaForGroup("a"), batches: [][]mgRowSpec{{{ts: 10, sid: 7, version: 1, value: 100}}, {{ts: 20, sid: 8, version: 1, value: 200}}}},
		{schema: testMeasureSchemaForGroup("b"), batches: [][]mgRowSpec{{{ts: 10, sid: 7, version: 3, value: 300}}, {{ts: 10, sid: 7, version: 2, value: 250}}}},
	}
	var schemas []logical.Schema
	var measureSchemas []*databasev1.Measure
	for _, g := range groups {
		schema, schemaErr := logicalmeasure.BuildSchema(g.schema, nil)
		if schemaErr != nil {
			t.Fatalf("BuildSchema: %v", schemaErr)
		}
		schemas = append(schemas, schema)
		measureSchemas = append(measureSchemas, g.schema)
	}
	req := &measurev1.QueryRequest{
		Name:            "demo",
		Groups:          []string{"a", "b"},
		TagProjection:   projTagProj(),
		FieldProjection: &measurev1.QueryRequest_FieldProjection{Names: []string{fieldValue}},
		TimeRange:       &modelv1.TimeRange{Begin: timestamppb.New(time.Unix(0, 0)), End: timestamppb.New(time.Unix(1, 0))},
		Limit:           10,
	}
	merge, mergeErr := NewMultiGroupMerge(context.Background(), req, schemas, measureSchemas, cfg)
	if mergeErr != nil {
		t.Fatalf("NewMultiGroupMerge: %v", mergeErr)
	}
	serializedBefore := measure.SerializedRowCount()
	var batches []*GroupBatches
	for i, g := range groups {
		result, planStr, dispatchErr := DispatchBatches(context.Background(), merge.GroupRequest(),
			&commonv1.Metadata{Name: "demo", Group: g.schema.GetMetadata().GetGroup()}, g.schema, schemas[i], g, cfg, false, true)
		var group *GroupBatches
		if dispatchErr == nil {
			group, dispatchErr = merge.Collect(context.Background(), result, planStr)
		}
		if dispatchErr != nil {
			t.Fatalf("DispatchBatches: %v", dispatchErr)
		}
		batches = append(batches, group)
	}
	iter, iterErr := merge.Merge(context.Background(), batches)
	if iterErr != nil {
		t.Fatalf("Merge: %v", iterErr)
	}
	defer iter.Close()
	var versions []int64
	for iter.Next() {
		versions = append(versions, iter.Current()[0].GetDataPoint().GetVersion())
	}
	if len(versions) != 2 || versions[0] != 3 || versions[1] != 1 {
		t.Fatalf("versions = %v, want [3 1] (version 3 wins (10,7); (20,8) keeps version 1)", versions)
	}
	if serialized := measure.SerializedRowCount() - serializedBefore; serialized != int64(len(versions)) {
		t.Fatalf("%d rows were materialized for a %d-row result: groups must be merged as batches", serialized, len(versions))
	}
}

func mgTestSetup(t *testing.T, groups []*mgBatchEC) ([]logical.Schema, []*databasev1.Measure) {
	t.Helper()
	var schemas []logical.Schema
	var measureSchemas []*databasev1.Measure
	for _, g := range groups {
		schema, schemaErr := logicalmeasure.BuildSchema(g.schema, nil)
		if schemaErr != nil {
			t.Fatalf("BuildSchema: %v", schemaErr)
		}
		schemas = append(schemas, schema)
		measureSchemas = append(measureSchemas, g.schema)
	}
	return schemas, measureSchemas
}

func mgTestRequest(limit uint32) *measurev1.QueryRequest {
	return &measurev1.QueryRequest{
		Name:            "demo",
		Groups:          []string{"a", "b"},
		TagProjection:   projTagProj(),
		FieldProjection: &measurev1.QueryRequest_FieldProjection{Names: []string{fieldValue}},
		TimeRange:       &modelv1.TimeRange{Begin: timestamppb.New(time.Unix(0, 0)), End: timestamppb.New(time.Unix(1, 0))},
		Limit:           limit,
	}
}

func mgDispatchAll(t *testing.T, merge *MultiGroupMerge, groups []*mgBatchEC, schemas []logical.Schema, cfg measure.VectorizedConfig) ([]*GroupBatches, error) {
	t.Helper()
	var batches []*GroupBatches
	for i, g := range groups {
		result, planStr, dispatchErr := DispatchBatches(context.Background(), merge.GroupRequest(),
			&commonv1.Metadata{Name: "demo", Group: g.schema.GetMetadata().GetGroup()}, g.schema, schemas[i], g, cfg, false, true)
		var group *GroupBatches
		if dispatchErr == nil {
			group, dispatchErr = merge.Collect(context.Background(), result, planStr)
		}
		if dispatchErr != nil {
			for _, obtained := range batches {
				obtained.Release()
			}
			return nil, dispatchErr
		}
		batches = append(batches, group)
	}
	return batches, nil
}

// TestMultiGroupMerge_RetainedMemoryIsBounded pins that the batches the groups hand over, their
// reshaped copies and the merged output are all charged to the query memory budget: two groups
// of 6,000 rows fit as raw input but not once the reshaped copy exists as well.
func TestMultiGroupMerge_RetainedMemoryIsBounded(t *testing.T) {
	cfg := measure.VectorizedConfig{BatchSize: 1024, QueryMemoryMiB: 1}
	rows := func(base int64) [][]mgRowSpec {
		batch := make([]mgRowSpec, 6000)
		for i := range batch {
			batch[i] = mgRowSpec{ts: base + int64(i), sid: base + int64(i), version: 1, value: 1}
		}
		return [][]mgRowSpec{batch}
	}
	groups := []*mgBatchEC{
		{schema: testMeasureSchemaForGroup("a"), batches: rows(0)},
		{schema: testMeasureSchemaForGroup("b"), batches: rows(100000)},
	}
	schemas, measureSchemas := mgTestSetup(t, groups)
	merge, mergeErr := NewMultiGroupMerge(context.Background(), mgTestRequest(20000), schemas, measureSchemas, cfg)
	if mergeErr != nil {
		t.Fatalf("NewMultiGroupMerge: %v", mergeErr)
	}
	batches, dispatchErr := mgDispatchAll(t, merge, groups, schemas, cfg)
	if dispatchErr != nil {
		t.Fatalf("the raw batches alone must fit the budget: %v", dispatchErr)
	}
	iter, iterErr := merge.Merge(context.Background(), batches)
	if iterErr == nil {
		iter.Close()
		t.Fatal("Merge must fail once the reshaped copies exceed the memory budget")
	}
	if used := merge.Tracker().Used(); used != 0 {
		t.Fatalf("a failed merge must give back its reservations, %d bytes still held", used)
	}
	for i, g := range groups {
		if g.result.released == 0 {
			t.Fatalf("group %d was not released after the failed merge", i)
		}
	}
}

// TestMultiGroupMerge_EmitFrameDecodesAndReleases covers the raw-frame path of the merged result:
// the frame decodes to the merged rows and closing the iterator releases every group.
func TestMultiGroupMerge_EmitFrameDecodesAndReleases(t *testing.T) {
	cfg := measure.VectorizedConfig{BatchSize: 1, QueryMemoryMiB: 16}
	groups := []*mgBatchEC{
		{schema: testMeasureSchemaForGroup("a"), batches: [][]mgRowSpec{{{ts: 10, sid: 7, version: 1, value: 100}}, {{ts: 20, sid: 8, version: 1, value: 200}}}},
		{schema: testMeasureSchemaForGroup("b"), batches: [][]mgRowSpec{{{ts: 10, sid: 7, version: 3, value: 300}}}},
	}
	schemas, measureSchemas := mgTestSetup(t, groups)
	merge, mergeErr := NewMultiGroupMerge(context.Background(), mgTestRequest(10), schemas, measureSchemas, cfg)
	if mergeErr != nil {
		t.Fatalf("NewMultiGroupMerge: %v", mergeErr)
	}
	batches, dispatchErr := mgDispatchAll(t, merge, groups, schemas, cfg)
	if dispatchErr != nil {
		t.Fatalf("DispatchBatches: %v", dispatchErr)
	}
	iter, iterErr := merge.Merge(context.Background(), batches)
	if iterErr != nil {
		t.Fatalf("Merge: %v", iterErr)
	}
	emitter, ok := iter.(measure.FrameEmitter)
	if !ok {
		t.Fatalf("%T must emit a raw frame", iter)
	}
	body, emitErr := emitter.EmitFrame(context.Background())
	if emitErr != nil {
		t.Fatalf("EmitFrame: %v", emitErr)
	}
	decoded, decodeErr := frame.Decode(body)
	if decodeErr != nil {
		t.Fatalf("Decode: %v", decodeErr)
	}
	defer decoded.ReleaseOwners()
	timestamps := decoded.Columns[decoded.Schema.TimestampIndex()].(*vectorized.TypedColumn[int64]).Data()
	versions := decoded.Columns[decoded.Schema.VersionIndex()].(*vectorized.TypedColumn[int64]).Data()
	if decoded.ActiveLen() != 2 || timestamps[0] != 10 || timestamps[1] != 20 || versions[0] != 3 || versions[1] != 1 {
		t.Fatalf("frame ts=%v versions=%v rows=%d, want ts=[10 20] versions=[3 1]", timestamps, versions, decoded.ActiveLen())
	}
	if closeErr := iter.Close(); closeErr != nil {
		t.Fatalf("Close: %v", closeErr)
	}
	for i, g := range groups {
		if g.result.released == 0 {
			t.Fatalf("group %d was not released by Close", i)
		}
	}
	if used := merge.Tracker().Used(); used != 0 {
		t.Fatalf("Close must give back the output reservation, %d bytes still held", used)
	}
}

// TestMultiGroupMerge_VariableSizePayloadIsCharged pins that retained memory is charged by what
// the cells hold: 300 rows with 4 KiB strings are 1.2 MB, far over a 1 MiB budget, although a
// fixed per-row width would price them at a few tens of KB.
func TestMultiGroupMerge_VariableSizePayloadIsCharged(t *testing.T) {
	cfg := measure.VectorizedConfig{BatchSize: 1024, QueryMemoryMiB: 1}
	big := string(make([]byte, 4096))
	batch := make([]mgRowSpec, 300)
	for i := range batch {
		batch[i] = mgRowSpec{ts: int64(i), sid: int64(i), version: 1, value: 1, tag: big}
	}
	groups := []*mgBatchEC{
		{schema: testMeasureSchemaForGroup("a"), batches: [][]mgRowSpec{batch}},
		{schema: testMeasureSchemaForGroup("b"), batches: [][]mgRowSpec{batch}},
	}
	schemas, measureSchemas := mgTestSetup(t, groups)
	merge, mergeErr := NewMultiGroupMerge(context.Background(), mgTestRequest(1000), schemas, measureSchemas, cfg)
	if mergeErr != nil {
		t.Fatalf("NewMultiGroupMerge: %v", mergeErr)
	}
	if _, dispatchErr := mgDispatchAll(t, merge, groups, schemas, cfg); dispatchErr == nil {
		t.Fatal("DispatchBatches must fail: the 4 KiB strings exceed the memory budget")
	}
	if used := merge.Tracker().Used(); used != 0 {
		t.Fatalf("a failed dispatch must give back its reservations, %d bytes still held", used)
	}
	if groups[0].result.released == 0 {
		t.Fatal("the group that failed its reservation must be released")
	}
}

func TestMultiGroupMerge_PreservesEmptyGroupSchemas(t *testing.T) {
	for _, withRows := range []bool{false, true} {
		t.Run(fmt.Sprintf("rows=%t", withRows), func(t *testing.T) {
			ctx := vectorized.WithQueryMemoryTracker(context.Background(), 16<<20)
			cfg := measure.VectorizedConfig{BatchSize: 4, QueryMemoryMiB: 16}
			schema := distributedRowsTestSchema()
			defs := append([]vectorized.ColumnDef(nil), schema.Columns...)
			defs = append(defs, vectorized.ColumnDef{Name: "zone", TagFamily: "extra", Role: vectorized.RoleTag, Type: vectorized.ColumnTypeString})
			groups := []*GroupBatches{{Schema: schema}, {Schema: vectorized.NewBatchSchema(defs)}}
			if withRows {
				groups[0].Batches = []*vectorized.RecordBatch{buildDistributedRowsBatchDirect(schema,
					[]rowMergeOutput{{ts: 10, sid: 1, ver: 1, value: 42}})}
			}
			merge, err := NewMultiGroupMerge(ctx, &measurev1.QueryRequest{Limit: 10}, nil, nil, cfg)
			if err != nil {
				t.Fatal(err)
			}
			result, err := merge.MergeBatches(ctx, groups)
			if err != nil {
				t.Fatal(err)
			}
			idx, ok := result.OutputSchema().TagIndex("extra", "zone")
			if !ok {
				t.Fatal("column defined by empty group was dropped")
			}
			batch, err := result.NextBatch(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if withRows && (batch == nil || !batch.Columns[idx].IsNull(0)) {
				t.Fatal("missing tag was not null-filled")
			}
			if !withRows && batch != nil {
				t.Fatal("empty groups produced rows")
			}
			if err = result.Close(); err != nil {
				t.Fatal(err)
			}
			if used := merge.Tracker().Used(); used != 0 {
				t.Fatalf("retained %d bytes after close", used)
			}
		})
	}
}

func TestMultiGroupMerge_CollectCopiesBorrowedBatches(t *testing.T) {
	ctx := vectorized.WithQueryMemoryTracker(context.Background(), 16<<20)
	cfg := measure.VectorizedConfig{BatchSize: 4, QueryMemoryMiB: 16}
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{{Name: "value", Role: vectorized.RoleField, Type: vectorized.ColumnTypeInt64}})
	owner := &standaloneTestOwner{}
	source := &recycledGroupSource{schema: schema, batch: vectorized.NewRecordBatch(schema, 2), owner: owner}
	result, err := NewBatchResult(ctx, source, schema, cfg.BatchSize)
	if err != nil {
		t.Fatal(err)
	}
	merge, err := NewMultiGroupMerge(ctx, &measurev1.QueryRequest{}, nil, nil, cfg)
	if err != nil {
		t.Fatal(err)
	}
	group, err := merge.Collect(ctx, result, "recycled")
	if err != nil {
		t.Fatal(err)
	}
	if len(group.Batches) != 2 {
		t.Fatalf("got %d batches", len(group.Batches))
	}
	for idx, batch := range group.Batches {
		value := batch.Columns[0].(*vectorized.TypedColumn[int64]).Data()[0]
		if value != int64(idx+1) || batch.Selection[0] != uint16(idx) {
			t.Fatalf("borrowed batch changed after next pull: value=%d selection=%v", value, batch.Selection)
		}
	}
	if owner.refs != 2 {
		t.Fatalf("collected batches retained %d owner pins", owner.refs)
	}
	group.Release()
	group.Release()
	if owner.refs != 0 || merge.Tracker().Used() != 0 || !source.closed {
		t.Fatal("collected group leaked resources")
	}
}

type recycledGroupSource struct {
	schema *vectorized.BatchSchema
	batch  *vectorized.RecordBatch
	owner  *standaloneTestOwner
	next   int
	closed bool
}

func (s *recycledGroupSource) Init(ctx context.Context) error        { return ctx.Err() }
func (s *recycledGroupSource) OutputSchema() *vectorized.BatchSchema { return s.schema }
func (s *recycledGroupSource) NextBatch(context.Context) (*vectorized.RecordBatch, error) {
	if s.next >= 2 {
		return nil, nil
	}
	col := s.batch.Columns[0].(*vectorized.TypedColumn[int64])
	col.Reset()
	col.Append(int64(s.next + 1))
	col.Append(int64(s.next + 11))
	s.batch.Len = 2
	if s.batch.Selection == nil {
		s.batch.Selection = []uint16{0}
	}
	s.batch.Selection[0] = uint16(s.next)
	s.batch.Owners.Pin(s.owner)
	s.next++
	return s.batch, nil
}

func (s *recycledGroupSource) Close() error {
	s.batch.ReleaseOwners()
	s.closed = true
	return nil
}
