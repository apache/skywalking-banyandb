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
	"bytes"
	"testing"

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// serializeFullSchema is "ts (int64), version (int64), sid (int64),
// tag.svc (string), field v_int (int64), field v_float (float64), field v_str (string),
// field v_bytes (bytes)".
func serializeFullSchema() *vectorized.BatchSchema {
	return vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleTimestamp, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleVersion, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleSeriesID, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "svc", Type: vectorized.ColumnTypeString},
		{Role: vectorized.RoleField, Name: "v_int", Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleField, Name: "v_float", Type: vectorized.ColumnTypeFloat64},
		{Role: vectorized.RoleField, Name: "v_str", Type: vectorized.ColumnTypeString},
		{Role: vectorized.RoleField, Name: "v_bytes", Type: vectorized.ColumnTypeBytes},
	})
}

func mkSerializeRow(b *vectorized.RecordBatch, ts, ver, sid int64, svc string,
	vInt int64, vFloat float64, vStr string, vBytes []byte,
) {
	b.Columns[0].(*vectorized.TypedColumn[int64]).Append(ts)
	b.Columns[1].(*vectorized.TypedColumn[int64]).Append(ver)
	b.Columns[2].(*vectorized.TypedColumn[int64]).Append(sid)
	b.Columns[3].(*vectorized.TypedColumn[string]).Append(svc)
	b.Columns[4].(*vectorized.TypedColumn[int64]).Append(vInt)
	b.Columns[5].(*vectorized.TypedColumn[float64]).Append(vFloat)
	b.Columns[6].(*vectorized.TypedColumn[string]).Append(vStr)
	b.Columns[7].(*vectorized.TypedColumn[[]byte]).Append(vBytes)
	b.Len++
}

func TestSerializeBatchToProto_RoundTrips_AllScalarVariants(t *testing.T) {
	s := serializeFullSchema()
	b := vectorized.NewRecordBatch(s, 2)
	mkSerializeRow(b, 100, 1, 7, "svcA", 42, 3.14, "hello", []byte("xyz"))

	out := serializeBatchToProto(b, nil)
	if len(out) != 1 {
		t.Fatalf("len: want 1, got %d", len(out))
	}
	dp := out[0].DataPoint
	if dp.Sid != 7 {
		t.Fatalf("sid: want 7, got %d", dp.Sid)
	}
	if dp.Version != 1 {
		t.Fatalf("version: want 1, got %d", dp.Version)
	}
	// Tag round-trip: svc must be Str variant with value "svcA".
	tf := dp.TagFamilies[0]
	if tf.Name != "default" || tf.Tags[0].Key != "svc" {
		t.Fatalf("tag family/key mismatch: %+v", tf)
	}
	if got := tf.Tags[0].Value.GetStr().GetValue(); got != "svcA" {
		t.Fatalf("svc tag: want svcA, got %q", got)
	}
	// Field round-trips.
	if got := dp.Fields[0].Value.GetInt().GetValue(); got != 42 {
		t.Fatalf("v_int: want 42, got %d", got)
	}
	if got := dp.Fields[1].Value.GetFloat().GetValue(); got != 3.14 {
		t.Fatalf("v_float: want 3.14, got %v", got)
	}
	if got := dp.Fields[2].Value.GetStr().GetValue(); got != "hello" {
		t.Fatalf("v_str: want hello, got %q", got)
	}
	if got := dp.Fields[3].Value.GetBinaryData(); !bytes.Equal(got, []byte("xyz")) {
		t.Fatalf("v_bytes: want xyz, got %q", got)
	}
}

func TestSerializeBatchToProto_RespectsSelectionVector(t *testing.T) {
	s := serializeFullSchema()
	b := vectorized.NewRecordBatch(s, 4)
	mkSerializeRow(b, 100, 1, 1, "a", 10, 1.0, "", nil)
	mkSerializeRow(b, 200, 1, 2, "b", 20, 2.0, "", nil)
	mkSerializeRow(b, 300, 1, 3, "c", 30, 3.0, "", nil)
	b.Selection = []uint16{0, 2} // only rows 0 and 2 are active

	out := serializeBatchToProto(b, nil)
	if len(out) != 2 {
		t.Fatalf("len: want 2, got %d", len(out))
	}
	if out[0].DataPoint.Sid != 1 || out[1].DataPoint.Sid != 3 {
		t.Fatalf("selected sids: want [1, 3], got [%d, %d]",
			out[0].DataPoint.Sid, out[1].DataPoint.Sid)
	}
}

func TestSerializeBatchToProto_NullsRoundtripped(t *testing.T) {
	s := serializeFullSchema()
	b := vectorized.NewRecordBatch(s, 1)
	// Build a row with explicit nulls in the tag and one field.
	b.Columns[0].(*vectorized.TypedColumn[int64]).Append(100)
	b.Columns[1].(*vectorized.TypedColumn[int64]).Append(1)
	b.Columns[2].(*vectorized.TypedColumn[int64]).Append(7)
	b.Columns[3].(*vectorized.TypedColumn[string]).AppendNull()
	b.Columns[4].(*vectorized.TypedColumn[int64]).AppendNull()
	b.Columns[5].(*vectorized.TypedColumn[float64]).Append(0)
	b.Columns[6].(*vectorized.TypedColumn[string]).Append("")
	b.Columns[7].(*vectorized.TypedColumn[[]byte]).Append(nil)
	b.Len = 1

	out := serializeBatchToProto(b, nil)
	tagVal := out[0].DataPoint.TagFamilies[0].Tags[0].Value
	if _, ok := tagVal.Value.(*modelv1.TagValue_Null); !ok {
		t.Fatalf("null tag must serialize to TagValue_Null, got %T", tagVal.Value)
	}
	fieldVal := out[0].DataPoint.Fields[0].Value
	if _, ok := fieldVal.Value.(*modelv1.FieldValue_Null); !ok {
		t.Fatalf("null field must serialize to FieldValue_Null, got %T", fieldVal.Value)
	}
}

func TestSerializeBatchToProto_RowOrderMatchesRowPath(t *testing.T) {
	// Parity contract: serialized row order matches batch row order.
	s := serializeFullSchema()
	b := vectorized.NewRecordBatch(s, 5)
	for i := range 5 {
		mkSerializeRow(b, int64(i*100), 1, int64(i+1), "svc", 0, 0, "", nil)
	}

	out := serializeBatchToProto(b, nil)
	for i, idp := range out {
		want := uint64(i + 1)
		if idp.DataPoint.Sid != want {
			t.Fatalf("row %d: sid want %d, got %d", i, want, idp.DataPoint.Sid)
		}
	}
}

func TestSerializeBatchToProto_ReusesDestinationSlice(t *testing.T) {
	s := serializeFullSchema()
	b := vectorized.NewRecordBatch(s, 1)
	mkSerializeRow(b, 100, 1, 7, "x", 0, 0, "", nil)

	dst := make([]*measurev1.InternalDataPoint, 0, 16)
	beforeCap := cap(dst)
	out := serializeBatchToProto(b, dst[:0])
	if cap(out) != beforeCap {
		t.Fatalf("cap must be reused: want %d, got %d", beforeCap, cap(out))
	}
}
