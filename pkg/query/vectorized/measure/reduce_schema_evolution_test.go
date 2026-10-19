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
	"strings"
	"testing"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure/frame"
)

// evolvedRow is one raw data point of the schema-evolution fixtures. zone is
// only written when the producing group's schema defines it.
type evolvedRow struct {
	svc   string
	zone  string
	value int64
}

// evolvedRawSchema is the raw layout a data node scans for a group projected
// as (svc[, zone]) + value, grouped by svc. Like BuildBatchSchema, the key
// tag is native and the non-key zone tag is a TagValue passthrough. The old
// schema has no zone tag, so the liaison drops zone from that group's
// projection.
func evolvedRawSchema(withZone bool, svcType vectorized.ColumnType) *vectorized.BatchSchema {
	cols := []vectorized.ColumnDef{
		{Role: vectorized.RoleShardID, Name: shardIDOutputName, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "svc", Type: svcType},
	}
	if withZone {
		cols = append(cols, vectorized.ColumnDef{Role: vectorized.RoleTag, TagFamily: "default", Name: "zone", Type: vectorized.ColumnTypeTagValue})
	}
	cols = append(cols, vectorized.ColumnDef{Role: vectorized.RoleField, Name: "value", Type: vectorized.ColumnTypeInt64})
	return vectorized.NewBatchSchema(cols)
}

// encodeEvolvedPartial runs the data-node AggModeMap over rows laid out by
// schema, grouped by svc, and encodes the emitted partial.
func encodeEvolvedPartial(t *testing.T, schema *vectorized.BatchSchema, shardID int64, rows []evolvedRow, fn AggFunc) []byte {
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
		b.Columns[0].(*vectorized.TypedColumn[int64]).Append(shardID)
		switch svcCol := b.Columns[1].(type) {
		case *vectorized.TypedColumn[string]:
			svcCol.Append(r.svc)
		case *vectorized.TypedColumn[int64]:
			svcCol.Append(int64(len(r.svc)))
		default:
			t.Fatalf("unexpected svc column %T", svcCol)
		}
		if len(schema.Columns) == 4 {
			b.Columns[2].(*vectorized.TypedColumn[*modelv1.TagValue]).Append(
				&modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: r.zone}}})
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
	oldFrame = encodeEvolvedPartial(t, evolvedRawSchema(false, vectorized.ColumnTypeString), 0,
		[]evolvedRow{{svc: "old", value: 2}, {svc: "old", value: 3}}, fn)
	newFrame = encodeEvolvedPartial(t, evolvedRawSchema(true, vectorized.ColumnTypeString), 0,
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
				zone = b.Columns[zoneIdx].(*vectorized.TypedColumn[*modelv1.TagValue]).Data()[r].GetStr().GetValue()
			}
			rows[svcVals[r]] = strings.Join([]string{svcVals[r], zone, columnStringAt(b.Columns[valueIdx], r)}, ",")
		}
	}
	return rows, cols
}

func TestReduceRawFrames_SchemaEvolvedGroups_NullFillsMissingTag(t *testing.T) {
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
			// The first non-empty partial used to define the schema, so check both arrival orders.
			for orderName, frames := range map[string][][]byte{
				"old-first": {oldFrame, newFrame},
				"new-first": {newFrame, oldFrame},
			} {
				t.Run(orderName, func(t *testing.T) {
					got, _, reduceErr := ReduceRawFrames(frames, "default", []string{"svc"}, false,
						[]AggReduceSpec{{OutputName: "value", Func: tc.fn}}, 1024, vectorized.NewMemoryTracker(1<<30))
					if reduceErr != nil {
						t.Fatalf("ReduceRawFrames: %v", reduceErr)
					}
					rows, cols := reducedRows(t, got)
					if !mapsEqualExact(rows, tc.want) {
						t.Fatalf("rows = %v, want %v", rows, tc.want)
					}
					if strings.Join(cols, ",") != "svc,zone,value" {
						t.Fatalf("columns = %v, want projection order svc,zone,value", cols)
					}
				})
			}
		})
	}
}

func TestReduceRawFrames_SchemaEvolvedGroups_ReplicasDoNotDoubleCount(t *testing.T) {
	oldFrame, newFrame := evolvedFixture(t, AggSum)
	got, _, reduceErr := ReduceRawFrames([][]byte{oldFrame, newFrame, oldFrame, newFrame}, "default", []string{"svc"}, false,
		[]AggReduceSpec{{OutputName: "value", Func: AggSum}}, 1024, vectorized.NewMemoryTracker(1<<30))
	if reduceErr != nil {
		t.Fatalf("ReduceRawFrames: %v", reduceErr)
	}
	rows, _ := reducedRows(t, got)
	want := map[string]string{"old": "old,NULL,5", "new": "new,west,18"}
	if !mapsEqualExact(rows, want) {
		t.Fatalf("rows = %v, want %v", rows, want)
	}
}

func TestReduceRawFrames_SchemaEvolvedGroups_StillRejectsIncompatibleLayouts(t *testing.T) {
	sumSpec := []AggReduceSpec{{OutputName: "value", Func: AggSum}}
	oldFrame, newFrame := evolvedFixture(t, AggSum)
	// A third group declares svc as an int, so its native key column changes type.
	intSvcFrame := encodeEvolvedPartial(t, evolvedRawSchema(false, vectorized.ColumnTypeInt64), 0,
		[]evolvedRow{{svc: "other", value: 1}}, AggSum)
	cases := []struct {
		name     string
		wantText string
		keyTags  []string
		frames   [][]byte
	}{
		{
			name:     "a group-by key changes type",
			keyTags:  []string{"svc"},
			frames:   [][]byte{newFrame, intSvcFrame},
			wantText: "default.svc",
		},
		{
			name:     "a group-by key is missing",
			keyTags:  []string{"svc", "zone"},
			frames:   [][]byte{newFrame, oldFrame},
			wantText: "default.zone",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, reduceErr := ReduceRawFrames(tc.frames, "default", tc.keyTags, false, sumSpec, 1024, vectorized.NewMemoryTracker(1<<30))
			if reduceErr == nil {
				t.Fatal("expected an error")
			}
			if !strings.Contains(reduceErr.Error(), tc.wantText) {
				t.Fatalf("error %q does not name %s", reduceErr, tc.wantText)
			}
		})
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
