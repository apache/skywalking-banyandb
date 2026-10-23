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
	"runtime"
	"slices"
	"strings"
	"testing"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure/frame"
)

// evolvedRow is one raw data point of the schema-evolution fixtures. zone is
// only written when the producing group's schema defines it; intZone writes
// it as an int instead (its length), as a group declaring zone INT would.
type evolvedRow struct {
	svc     string
	zone    string
	value   int64
	intZone bool
}

// evolvedRawSchema is the raw layout a data node scans for a group projected
// as (svc[, zone]) + value, grouped by svc. Like BuildBatchSchema, the key
// tag is native and the non-key zone tag is a TagValue passthrough. Without
// a zoneType zone is left out, as for the old schema, whose projection the
// liaison trims to svc.
func evolvedRawSchema(svcType vectorized.ColumnType, zoneType ...vectorized.ColumnType) *vectorized.BatchSchema {
	cols := []vectorized.ColumnDef{
		{Role: vectorized.RoleShardID, Name: shardIDOutputName, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "svc", Type: svcType},
	}
	for _, zt := range zoneType {
		cols = append(cols, vectorized.ColumnDef{Role: vectorized.RoleTag, TagFamily: "default", Name: "zone", Type: zt})
	}
	cols = append(cols, vectorized.ColumnDef{Role: vectorized.RoleField, Name: "value", Type: vectorized.ColumnTypeInt64})
	return vectorized.NewBatchSchema(cols)
}

// encodeEvolvedPartial runs the data-node AggModeMap over rows laid out by
// schema, grouped by svc, and encodes the emitted partial. Every partial
// reports shard 0, as each group numbers its own shards.
func encodeEvolvedPartial(t *testing.T, schema *vectorized.BatchSchema, rows []evolvedRow, fn AggFunc) []byte {
	t.Helper()
	valueIdx := len(schema.Columns) - 1
	op := NewBatchAggregation(schema, []int{1},
		[]AggSpec{{Func: fn, InputCol: valueIdx, Output: "value"}},
		AggModeMap, 1024, vectorized.NewMemoryTracker(1<<30), 0)
	defer op.Close()
	if initErr := op.Init(context.Background()); initErr != nil {
		t.Fatalf("map init: %v", initErr)
	}
	b := vectorized.NewRecordBatch(schema, len(rows))
	for _, r := range rows {
		b.Columns[0].(*vectorized.TypedColumn[int64]).Append(0)
		switch svcCol := b.Columns[1].(type) {
		case *vectorized.TypedColumn[string]:
			svcCol.Append(r.svc)
		case *vectorized.TypedColumn[int64]:
			svcCol.Append(int64(len(r.svc)))
		default:
			t.Fatalf("unexpected svc column %T", svcCol)
		}
		if len(schema.Columns) == 4 {
			switch zoneCol := b.Columns[2].(type) {
			case *vectorized.TypedColumn[*modelv1.TagValue]:
				if r.intZone {
					zoneCol.Append(&modelv1.TagValue{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: int64(len(r.zone))}}})
				} else {
					zoneCol.Append(&modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: r.zone}}})
				}
			case *vectorized.TypedColumn[string]:
				zoneCol.Append(r.zone)
			case *vectorized.TypedColumn[int64]:
				zoneCol.Append(int64(len(r.zone)))
			default:
				t.Fatalf("unexpected zone column %T", zoneCol)
			}
		}
		b.Columns[valueIdx].(*vectorized.TypedColumn[int64]).Append(r.value)
	}
	b.Len = len(rows)
	if consumeErr := op.Consume(context.Background(), b); consumeErr != nil {
		t.Fatalf("map consume: %v", consumeErr)
	}
	if finalErr := op.Finalize(context.Background()); finalErr != nil {
		t.Fatalf("map finalize: %v", finalErr)
	}
	out, nextErr := op.NextBatch(context.Background())
	if nextErr != nil || out == nil {
		t.Fatalf("map next: batch=%v err=%v", out, nextErr)
	}
	body, encErr := frame.Encode(out)
	if encErr != nil {
		t.Fatalf("frame.Encode: %v", encErr)
	}
	return body
}

// evolvedFixture is the oracle from apache/skywalking#14069: an old group
// (svc,value) and a new group (svc,zone,value), both on shard 0.
func evolvedFixture(t *testing.T, fn AggFunc) (oldFrame, newFrame []byte) {
	t.Helper()
	oldFrame = encodeEvolvedPartial(t, evolvedRawSchema(vectorized.ColumnTypeString),
		[]evolvedRow{{svc: "old", value: 2}, {svc: "old", value: 3}}, fn)
	newFrame = encodeEvolvedPartial(t, evolvedRawSchema(vectorized.ColumnTypeString, vectorized.ColumnTypeTagValue),
		[]evolvedRow{{svc: "new", zone: "west", value: 7}, {svc: "new", zone: "west", value: 11}}, fn)
	return oldFrame, newFrame
}

// reducedRows renders each reduced row as "svc,zone,value" with NULL for a
// null zone, and returns the visible tag/field column names in order.
func reducedRows(t *testing.T, batches []*vectorized.RecordBatch) (rows map[string]string, cols []string) {
	t.Helper()
	rows = make(map[string]string)
	for _, b := range batches {
		svcIdx, hasSvc := b.Schema.TagIndex("default", "svc")
		zoneIdx, hasZone := b.Schema.TagIndex("default", "zone")
		valueIdx := -1
		cols = cols[:0]
		for i, def := range b.Schema.Columns {
			if def.Role == vectorized.RoleTag || def.Role == vectorized.RoleField {
				cols = append(cols, def.Name)
			}
			if def.Role == vectorized.RoleField && def.Name == "value" {
				valueIdx = i
			}
		}
		if !hasSvc || !hasZone || valueIdx < 0 {
			t.Fatalf("reduced schema is missing svc, zone or value: %+v", b.Schema.Columns)
		}
		svcVals := b.Columns[svcIdx].(*vectorized.TypedColumn[string]).Data()
		for r := 0; r < b.Len; r++ {
			zone := "NULL"
			if !b.Columns[zoneIdx].IsNull(r) {
				switch zoneCol := b.Columns[zoneIdx].(type) {
				case *vectorized.TypedColumn[*modelv1.TagValue]:
					zone = zoneCol.Data()[r].GetStr().GetValue()
				case *vectorized.TypedColumn[string]:
					zone = zoneCol.Data()[r]
				}
			}
			rows[svcVals[r]] = strings.Join([]string{svcVals[r], zone, columnStringAt(b.Columns[valueIdx], r)}, ",")
		}
	}
	return rows, cols
}

// partialSchema is the layout of an encoded partial, used as the reduce target.
func partialSchema(t *testing.T, body []byte) *vectorized.BatchSchema {
	t.Helper()
	b, decodeErr := frame.Decode(body)
	if decodeErr != nil {
		t.Fatalf("frame.Decode: %v", decodeErr)
	}
	defer b.ReleaseOwners()
	return b.Schema
}

func reduceToSchema(frames [][]byte, target *vectorized.BatchSchema, keyTags []string, fn AggFunc) ([]*vectorized.RecordBatch, error) {
	got, _, reduceErr := ReduceRawFramesToSchema(frames, target, "default", keyTags, false,
		[]AggReduceSpec{{OutputName: "value", Func: fn}}, 1024, vectorized.NewMemoryTracker(1<<30))
	return got, reduceErr
}

func TestReduceRawFramesToSchema_NullFillsMissingTag(t *testing.T) {
	cases := []struct {
		want map[string]string
		name string
		fn   AggFunc
	}{
		{name: "SUM", fn: AggSum, want: map[string]string{"old": "old,NULL,5", "new": "new,west,18"}},
		// MEAN of an int field stays int, so (2+3)/2 is 2. It still needs the count sidecar to line up.
		{name: "MEAN", fn: AggMean, want: map[string]string{"old": "old,NULL,2", "new": "new,west,9"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			oldFrame, newFrame := evolvedFixture(t, tc.fn)
			target := partialSchema(t, newFrame)
			for orderName, frames := range map[string][][]byte{
				"old-first": {oldFrame, newFrame},
				"new-first": {newFrame, oldFrame},
			} {
				t.Run(orderName, func(t *testing.T) {
					got, reduceErr := reduceToSchema(frames, target, []string{"svc"}, tc.fn)
					if reduceErr != nil {
						t.Fatalf("ReduceRawFramesToSchema: %v", reduceErr)
					}
					rows, cols := reducedRows(t, got)
					if !mapsEqualExact(rows, tc.want) {
						t.Fatalf("rows = %v, want %v", rows, tc.want)
					}
					if strings.Join(cols, ",") != "svc,zone,value" {
						t.Fatalf("columns = %v, want svc,zone,value", cols)
					}
				})
			}
		})
	}
}

func TestReduceRawFramesToSchema_KeepsTagNoPartialCarries(t *testing.T) {
	oldFrame, newFrame := evolvedFixture(t, AggSum)
	got, reduceErr := reduceToSchema([][]byte{oldFrame}, partialSchema(t, newFrame), []string{"svc"}, AggSum)
	if reduceErr != nil {
		t.Fatalf("ReduceRawFramesToSchema: %v", reduceErr)
	}
	rows, cols := reducedRows(t, got)
	if want := map[string]string{"old": "old,NULL,5"}; !mapsEqualExact(rows, want) {
		t.Fatalf("rows = %v, want %v", rows, want)
	}
	if strings.Join(cols, ",") != "svc,zone,value" {
		t.Fatalf("columns = %v, want svc,zone,value", cols)
	}
}

func TestReduceRawFramesToSchema_WrapsMixedWireTypes(t *testing.T) {
	// A data node sends a uniform passthrough column as its native type, so
	// zone can arrive as string from one partial and TagValue from another.
	_, newFrame := evolvedFixture(t, AggSum)
	narrowFrame := encodeEvolvedPartial(t, evolvedRawSchema(vectorized.ColumnTypeString, vectorized.ColumnTypeString),
		[]evolvedRow{{svc: "narrow", zone: "east", value: 4}}, AggSum)
	for orderName, frames := range map[string][][]byte{
		"tagvalue-first": {newFrame, narrowFrame},
		"string-first":   {narrowFrame, newFrame},
	} {
		t.Run(orderName, func(t *testing.T) {
			got, reduceErr := reduceToSchema(frames, partialSchema(t, newFrame), []string{"svc"}, AggSum)
			if reduceErr != nil {
				t.Fatalf("ReduceRawFramesToSchema: %v", reduceErr)
			}
			rows, _ := reducedRows(t, got)
			if want := map[string]string{"new": "new,west,18", "narrow": "narrow,east,4"}; !mapsEqualExact(rows, want) {
				t.Fatalf("rows = %v, want %v", rows, want)
			}
		})
	}
}

func TestReduceRawFramesToSchema_ReplicasDoNotDoubleCount(t *testing.T) {
	oldFrame, newFrame := evolvedFixture(t, AggSum)
	got, reduceErr := reduceToSchema([][]byte{oldFrame, newFrame, oldFrame, newFrame}, partialSchema(t, newFrame), []string{"svc"}, AggSum)
	if reduceErr != nil {
		t.Fatalf("ReduceRawFramesToSchema: %v", reduceErr)
	}
	rows, _ := reducedRows(t, got)
	if want := map[string]string{"old": "old,NULL,5", "new": "new,west,18"}; !mapsEqualExact(rows, want) {
		t.Fatalf("rows = %v, want %v", rows, want)
	}
}

func TestReduceRawFramesToSchema_RejectsIncompatibleLayouts(t *testing.T) {
	oldFrame, newFrame := evolvedFixture(t, AggSum)
	// A third group declares svc as an int, so its native key column changes type.
	intSvcFrame := encodeEvolvedPartial(t, evolvedRawSchema(vectorized.ColumnTypeInt64),
		[]evolvedRow{{svc: "other", value: 1}}, AggSum)
	// The plan declares zone a string; these partials carry it as an int,
	// natively or as TagValue cells.
	stringZone := partialSchema(t, encodeEvolvedPartial(t, evolvedRawSchema(vectorized.ColumnTypeString, vectorized.ColumnTypeString),
		[]evolvedRow{{svc: "plan", zone: "west", value: 1}}, AggSum))
	intZoneFrame := encodeEvolvedPartial(t, evolvedRawSchema(vectorized.ColumnTypeString, vectorized.ColumnTypeInt64),
		[]evolvedRow{{svc: "other", zone: "east", value: 1}}, AggSum)
	intCellsFrame := encodeEvolvedPartial(t, evolvedRawSchema(vectorized.ColumnTypeString, vectorized.ColumnTypeTagValue),
		[]evolvedRow{{svc: "other", zone: "east", value: 1, intZone: true}}, AggSum)
	cases := []struct {
		target   *vectorized.BatchSchema
		name     string
		wantText string
		keyTags  []string
		frames   [][]byte
	}{
		{name: "a group-by key changes type", target: partialSchema(t, newFrame), keyTags: []string{"svc"}, frames: [][]byte{intSvcFrame}, wantText: "default.svc"},
		{name: "a group-by key is missing", target: partialSchema(t, newFrame), keyTags: []string{"svc", "zone"}, frames: [][]byte{oldFrame}, wantText: "default.zone"},
		{name: "a partial has a column the target lacks", target: partialSchema(t, oldFrame), keyTags: []string{"svc"}, frames: [][]byte{newFrame}, wantText: "default.zone"},
		{name: "a non-key tag arrives as another type", target: stringZone, keyTags: []string{"svc"}, frames: [][]byte{intZoneFrame}, wantText: "default.zone"},
		{name: "a non-key tag carries cells of another type", target: stringZone, keyTags: []string{"svc"}, frames: [][]byte{intCellsFrame}, wantText: "default.zone"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, reduceErr := reduceToSchema(tc.frames, tc.target, tc.keyTags, AggSum)
			if reduceErr == nil {
				t.Fatal("expected an error")
			}
			if !strings.Contains(reduceErr.Error(), tc.wantText) {
				t.Fatalf("error %q does not name %s", reduceErr, tc.wantText)
			}
		})
	}
}

func TestReduceRawFramesToSchema_ReadsTagValueAsPlannedType(t *testing.T) {
	// A node sends zone as TagValue when its cells are mixed or all null; with
	// zone planned as a string, those cells are read back as strings.
	_, newFrame := evolvedFixture(t, AggSum)
	stringFrame := encodeEvolvedPartial(t, evolvedRawSchema(vectorized.ColumnTypeString, vectorized.ColumnTypeString),
		[]evolvedRow{{svc: "narrow", zone: "east", value: 4}}, AggSum)
	got, reduceErr := reduceToSchema([][]byte{newFrame, stringFrame}, partialSchema(t, stringFrame), []string{"svc"}, AggSum)
	if reduceErr != nil {
		t.Fatalf("ReduceRawFramesToSchema: %v", reduceErr)
	}
	rows, _ := reducedRows(t, got)
	if want := map[string]string{"new": "new,west,18", "narrow": "narrow,east,4"}; !mapsEqualExact(rows, want) {
		t.Fatalf("rows = %v, want %v", rows, want)
	}
	zoneIdx, _ := got[0].Schema.TagIndex("default", "zone")
	if zoneType := got[0].Schema.Columns[zoneIdx].Type; zoneType != vectorized.ColumnTypeString {
		t.Fatalf("zone is %v, want the planned string", zoneType)
	}
}

func TestReduceRawFramesToSchema_ReservesWidenedPartials(t *testing.T) {
	// A populated narrow group plus a wide group with no rows: the target adds
	// 1,000 projected tags the narrow partial lacks.
	rows := make([]evolvedRow, 4096)
	for i := range rows {
		rows[i] = evolvedRow{svc: fmt.Sprintf("svc-%d", i), value: 1}
	}
	narrowFrame := encodeEvolvedPartial(t, evolvedRawSchema(vectorized.ColumnTypeString), rows, AggSum)
	narrow := partialSchema(t, narrowFrame)
	cols := slices.Clone(narrow.Columns[:2])
	for i := range 1000 {
		cols = append(cols, vectorized.ColumnDef{Role: vectorized.RoleTag, TagFamily: "default", Name: fmt.Sprintf("tag-%d", i), Type: vectorized.ColumnTypeTagValue})
	}
	wide := vectorized.NewBatchSchema(append(cols, narrow.Columns[2:]...))
	const budget = 64 << 20
	reduce := func(target *vectorized.BatchSchema, tracker *vectorized.MemoryTracker) error {
		_, _, reduceErr := ReduceRawFramesToSchema([][]byte{narrowFrame}, target, "default", []string{"svc"}, false,
			[]AggReduceSpec{{OutputName: "value", Func: AggSum}}, 1024, tracker)
		return reduceErr
	}
	if reduceErr := reduce(narrow, vectorized.NewMemoryTracker(budget)); reduceErr != nil {
		t.Fatalf("the narrow layout should fit the budget: %v", reduceErr)
	}
	tracker := vectorized.NewMemoryTracker(budget)
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	reduceErr := reduce(wide, tracker)
	runtime.ReadMemStats(&after)
	if !errors.Is(reduceErr, vectorized.ErrMemoryBudgetExceeded) {
		t.Fatalf("error = %v, want ErrMemoryBudgetExceeded", reduceErr)
	}
	if allocated := after.TotalAlloc - before.TotalAlloc; allocated > 16<<20 {
		t.Fatalf("allocated %d bytes before failing the budget", allocated)
	}
	if used := tracker.Used(); used != 0 {
		t.Fatalf("tracker still holds %d bytes", used)
	}
}

func TestReduceRawFrames_WithoutTargetStillRequiresOneLayout(t *testing.T) {
	oldFrame, newFrame := evolvedFixture(t, AggSum)
	_, _, reduceErr := ReduceRawFrames([][]byte{oldFrame, newFrame}, "default", []string{"svc"}, false,
		[]AggReduceSpec{{OutputName: "value", Func: AggSum}}, 1024, vectorized.NewMemoryTracker(1<<30))
	if reduceErr == nil || !strings.Contains(reduceErr.Error(), "schema mismatch") {
		t.Fatalf("error = %v, want a schema mismatch", reduceErr)
	}
}

func mapsEqualExact(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, va := range a {
		if vb, ok := b[k]; !ok || va != vb {
			return false
		}
	}
	return true
}
