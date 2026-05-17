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

package frame

import (
	"bytes"
	"errors"
	"math"
	"testing"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// TestDecode_EmptyFrame_GoldenBytes covers the smallest legal frame: 0 rows,
// 0 cols. The decoded batch has a nil/empty schema and Len=0.
func TestDecode_EmptyFrame_GoldenBytes(t *testing.T) {
	raw := []byte{0x00, 'V', 'F', 'R', 0x02, 0x00, 0x00}
	b, err := Decode(raw)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if b == nil {
		t.Fatal("Decode returned nil batch")
	}
	if b.Len != 0 {
		t.Fatalf("Len = %d, want 0", b.Len)
	}
	if len(b.Schema.Columns) != 0 {
		t.Fatalf("Schema.Columns = %d, want 0", len(b.Schema.Columns))
	}
	if len(b.Columns) != 0 {
		t.Fatalf("Columns = %d, want 0", len(b.Columns))
	}
}

// TestDecode_RoundTrip_Int64 covers a 3-row int64 column with no nulls.
func TestDecode_RoundTrip_Int64(t *testing.T) {
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleField, Name: "n", Type: vectorized.ColumnTypeInt64},
	})
	in := vectorized.NewRecordBatch(schema, 3)
	col := in.Columns[0].(*vectorized.TypedColumn[int64])
	col.Append(10)
	col.Append(20)
	col.Append(30)
	in.Len = 3

	raw, err := Encode(in)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	out, decodeErr := Decode(raw)
	if decodeErr != nil {
		t.Fatalf("Decode: %v", decodeErr)
	}
	if out.Len != 3 {
		t.Fatalf("Len = %d, want 3", out.Len)
	}
	if len(out.Columns) != 1 {
		t.Fatalf("Columns = %d, want 1", len(out.Columns))
	}
	got := out.Columns[0].(*vectorized.TypedColumn[int64]).Data()
	if want := []int64{10, 20, 30}; !int64Equal(got, want) {
		t.Fatalf("data = %v, want %v", got, want)
	}
	if def := out.Schema.Columns[0]; def.Role != vectorized.RoleField || def.Name != "n" || def.Type != vectorized.ColumnTypeInt64 {
		t.Fatalf("ColumnDef = %+v, want {Role:Field Name:n Type:Int64}", def)
	}
}

// TestDecode_RoundTrip_NullInMiddle verifies the validity bitmap survives
// the wire: a middle null row decodes as IsNull(1) and untouched neighbours.
func TestDecode_RoundTrip_NullInMiddle(t *testing.T) {
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleField, Name: "v", Type: vectorized.ColumnTypeInt64},
	})
	in := vectorized.NewRecordBatch(schema, 3)
	col := in.Columns[0].(*vectorized.TypedColumn[int64])
	col.Append(100)
	col.AppendNull()
	col.Append(300)
	in.Len = 3

	raw, err := Encode(in)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	out, decodeErr := Decode(raw)
	if decodeErr != nil {
		t.Fatalf("Decode: %v", decodeErr)
	}
	outCol := out.Columns[0].(*vectorized.TypedColumn[int64])
	if outCol.IsNull(0) {
		t.Fatal("row 0 should be non-null")
	}
	if !outCol.IsNull(1) {
		t.Fatal("row 1 should be null")
	}
	if outCol.IsNull(2) {
		t.Fatal("row 2 should be non-null")
	}
	if outCol.Data()[0] != 100 {
		t.Fatalf("row 0 = %d, want 100", outCol.Data()[0])
	}
	if outCol.Data()[2] != 300 {
		t.Fatalf("row 2 = %d, want 300", outCol.Data()[2])
	}
}

// TestDecode_RoundTrip_Float64 covers IEEE-754 float64 — including a NaN to
// make sure the bit-pattern survives the LE encode/decode trip unchanged.
func TestDecode_RoundTrip_Float64(t *testing.T) {
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleField, Name: "f", Type: vectorized.ColumnTypeFloat64},
	})
	in := vectorized.NewRecordBatch(schema, 3)
	col := in.Columns[0].(*vectorized.TypedColumn[float64])
	col.Append(3.14)
	col.Append(math.Inf(1))
	col.Append(math.NaN())
	in.Len = 3

	raw, err := Encode(in)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	out, decodeErr := Decode(raw)
	if decodeErr != nil {
		t.Fatalf("Decode: %v", decodeErr)
	}
	got := out.Columns[0].(*vectorized.TypedColumn[float64]).Data()
	if got[0] != 3.14 {
		t.Fatalf("row 0 = %g, want 3.14", got[0])
	}
	if !math.IsInf(got[1], 1) {
		t.Fatalf("row 1 = %g, want +Inf", got[1])
	}
	if !math.IsNaN(got[2]) {
		t.Fatalf("row 2 = %g, want NaN", got[2])
	}
}

// TestDecode_RoundTrip_StringWithEmptyAndNull pins the null/empty
// disambiguation: an empty string row carries len=0 in the data section AND
// is NOT marked null in the validity bitmap, while an explicitly-null row
// is len=0 AND null. Decode must distinguish them via the bitmap.
func TestDecode_RoundTrip_StringWithEmptyAndNull(t *testing.T) {
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleTag, TagFamily: "geo", Name: "region", Type: vectorized.ColumnTypeString},
	})
	in := vectorized.NewRecordBatch(schema, 3)
	col := in.Columns[0].(*vectorized.TypedColumn[string])
	col.Append("us-east")
	col.Append("") // empty but non-null
	col.AppendNull()
	in.Len = 3

	raw, err := Encode(in)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	out, decodeErr := Decode(raw)
	if decodeErr != nil {
		t.Fatalf("Decode: %v", decodeErr)
	}
	outCol := out.Columns[0].(*vectorized.TypedColumn[string])
	if outCol.IsNull(0) {
		t.Fatal("row 0 should be non-null")
	}
	if outCol.IsNull(1) {
		t.Fatal("row 1 should be non-null (empty string, not null)")
	}
	if !outCol.IsNull(2) {
		t.Fatal("row 2 should be null")
	}
	if outCol.Data()[0] != "us-east" {
		t.Fatalf("row 0 = %q, want %q", outCol.Data()[0], "us-east")
	}
	if outCol.Data()[1] != "" {
		t.Fatalf("row 1 = %q, want empty string", outCol.Data()[1])
	}
}

// TestDecode_RoundTrip_BytesColumn covers the []byte type — same wire shape
// as string, but the decoder copies into a freshly allocated slice so the
// caller cannot alias the input frame buffer (which is typically pooled).
func TestDecode_RoundTrip_BytesColumn(t *testing.T) {
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleTag, TagFamily: "raw", Name: "payload", Type: vectorized.ColumnTypeBytes},
	})
	in := vectorized.NewRecordBatch(schema, 2)
	col := in.Columns[0].(*vectorized.TypedColumn[[]byte])
	col.Append([]byte{0xDE, 0xAD, 0xBE, 0xEF})
	col.Append([]byte{0x01, 0x02})
	in.Len = 2

	raw, err := Encode(in)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	out, decodeErr := Decode(raw)
	if decodeErr != nil {
		t.Fatalf("Decode: %v", decodeErr)
	}
	outCol := out.Columns[0].(*vectorized.TypedColumn[[]byte])
	if !bytes.Equal(outCol.Data()[0], []byte{0xDE, 0xAD, 0xBE, 0xEF}) {
		t.Fatalf("row 0 = %v, want [DE AD BE EF]", outCol.Data()[0])
	}
	if !bytes.Equal(outCol.Data()[1], []byte{0x01, 0x02}) {
		t.Fatalf("row 1 = %v, want [01 02]", outCol.Data()[1])
	}
}

// TestDecode_RoundTrip_MixedColumns covers a realistic AggModeMap partial
// batch: shard_id + region tag + sum value + count sidecar. Confirms multi-
// column roundtrip preserves order, role, and per-column type.
func TestDecode_RoundTrip_MixedColumns(t *testing.T) {
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleShardID, Name: "shard_id", Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleTag, TagFamily: "geo", Name: "region", Type: vectorized.ColumnTypeString},
		{Role: vectorized.RoleField, Name: "sum_value", Type: vectorized.ColumnTypeFloat64},
		{Role: vectorized.RoleField, Name: "sum_value__agg_count", Type: vectorized.ColumnTypeFloat64},
	})
	in := vectorized.NewRecordBatch(schema, 2)
	in.Columns[0].(*vectorized.TypedColumn[int64]).Append(3)
	in.Columns[0].(*vectorized.TypedColumn[int64]).Append(3)
	in.Columns[1].(*vectorized.TypedColumn[string]).Append("us-east")
	in.Columns[1].(*vectorized.TypedColumn[string]).Append("us-west")
	in.Columns[2].(*vectorized.TypedColumn[float64]).Append(15.0)
	in.Columns[2].(*vectorized.TypedColumn[float64]).Append(27.5)
	in.Columns[3].(*vectorized.TypedColumn[float64]).Append(3)
	in.Columns[3].(*vectorized.TypedColumn[float64]).Append(5)
	in.Len = 2

	raw, err := Encode(in)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	out, decodeErr := Decode(raw)
	if decodeErr != nil {
		t.Fatalf("Decode: %v", decodeErr)
	}
	if out.Len != 2 {
		t.Fatalf("Len = %d, want 2", out.Len)
	}
	if len(out.Columns) != 4 {
		t.Fatalf("Columns = %d, want 4", len(out.Columns))
	}
	roles := []vectorized.ColumnRole{
		vectorized.RoleShardID, vectorized.RoleTag, vectorized.RoleField, vectorized.RoleField,
	}
	for i, want := range roles {
		if got := out.Schema.Columns[i].Role; got != want {
			t.Fatalf("col %d role = %v, want %v", i, got, want)
		}
	}
	if got := out.Columns[0].(*vectorized.TypedColumn[int64]).Data(); !int64Equal(got, []int64{3, 3}) {
		t.Fatalf("shard_id = %v, want [3 3]", got)
	}
	if got := out.Columns[1].(*vectorized.TypedColumn[string]).Data(); got[0] != "us-east" || got[1] != "us-west" {
		t.Fatalf("region = %v, want [us-east us-west]", got)
	}
	if got := out.Columns[2].(*vectorized.TypedColumn[float64]).Data(); got[0] != 15.0 || got[1] != 27.5 {
		t.Fatalf("sum_value = %v, want [15.0 27.5]", got)
	}
	if got := out.Columns[3].(*vectorized.TypedColumn[float64]).Data(); got[0] != 3 || got[1] != 5 {
		t.Fatalf("count = %v, want [3 5]", got)
	}
}

// TestDecode_BadMagic_FailsLoud covers the load-bearing fail-loud guard:
// a body whose first 4 bytes do not match Magic is rejected with ErrBadMagic.
func TestDecode_BadMagic_FailsLoud(t *testing.T) {
	cases := [][]byte{
		// Pure proto-shaped body (field 1 varint tag = 0x08).
		{0x08, 0x01, 0x10, 0x02, 0x00, 0x00, 0x00},
		// Magic with wrong second byte.
		{0x00, 'X', 'F', 'R', 0x01, 0x00, 0x00},
	}
	for _, body := range cases {
		_, err := Decode(body)
		if err == nil {
			t.Fatalf("Decode(%#x) returned nil error; want ErrBadMagic", body)
		}
		if !errors.Is(err, ErrBadMagic) {
			t.Fatalf("Decode(%#x) error = %v; want ErrBadMagic", body, err)
		}
	}
}

// TestDecode_BadVersion_FailsLoud covers Principle 3: any wire-version
// byte other than the current WireVersion is rejected with ErrBadVersion.
// The hard-cutover model forbids dual-wire so version skew is a botched
// rollout, not coexistence.
func TestDecode_BadVersion_FailsLoud(t *testing.T) {
	body := []byte{0x00, 'V', 'F', 'R', 0x03, 0x00, 0x00}
	_, err := Decode(body)
	if err == nil {
		t.Fatal("Decode returned nil error; want ErrBadVersion")
	}
	if !errors.Is(err, ErrBadVersion) {
		t.Fatalf("Decode error = %v; want ErrBadVersion", err)
	}
}

// TestDecode_TruncatedColumnData_FailsLoud verifies that a body whose
// column data section is shorter than NumRows×8 fails with ErrTruncated.
func TestDecode_TruncatedColumnData_FailsLoud(t *testing.T) {
	// 3 rows declared but only 2 int64 worth of data.
	body := []byte{
		0x00, 'V', 'F', 'R',
		0x02,                  // version (v2: TagFamily-on-the-wire)
		0x03,                  // nrows = 3
		0x01,                  // ncols = 1
		0x06,                  // role = Field
		0x01,                  // type = Int64
		0x01, 'n',             // name "n"
		0x00,                  // TagFamilyLen = 0 (RoleField has no family)
		0x00,                  // validity (1 byte, all valid)
		1, 0, 0, 0, 0, 0, 0, 0, // row 0
		2, 0, 0, 0, 0, 0, 0, 0, // row 1 — row 2 missing
	}
	_, err := Decode(body)
	if err == nil {
		t.Fatal("Decode returned nil error; want ErrTruncated")
	}
	if !errors.Is(err, ErrTruncated) {
		t.Fatalf("Decode error = %v; want ErrTruncated", err)
	}
}

// TestDecode_TrailingBytes_FailsLoud verifies trailing garbage past the
// last declared column is rejected — defensive against length-skew bugs
// in producers.
func TestDecode_TrailingBytes_FailsLoud(t *testing.T) {
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleField, Name: "n", Type: vectorized.ColumnTypeInt64},
	})
	in := vectorized.NewRecordBatch(schema, 1)
	in.Columns[0].(*vectorized.TypedColumn[int64]).Append(7)
	in.Len = 1
	raw, err := Encode(in)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	corrupt := append(raw[:len(raw):len(raw)], 0xFF, 0xAA)
	_, decodeErr := Decode(corrupt)
	if decodeErr == nil {
		t.Fatal("Decode returned nil error on trailing bytes; want ErrTruncated")
	}
	if !errors.Is(decodeErr, ErrTruncated) {
		t.Fatalf("Decode error = %v; want ErrTruncated", decodeErr)
	}
}

// TestDecode_UnknownRoleByte_FailsLoud verifies that an unknown role byte
// (one outside the assigned frameColRole values) is rejected loudly via
// ErrUnsupportedColumnRole — never silently coerced to a default role.
func TestDecode_UnknownRoleByte_FailsLoud(t *testing.T) {
	body := []byte{
		0x00, 'V', 'F', 'R',
		0x02,         // version (v2)
		0x00,         // nrows = 0
		0x01,         // ncols = 1
		0xFE,         // role byte 254 — unassigned (rejected before TagFamily read)
		0x01,         // type = Int64
		0x01, 'n',    // name "n"
	}
	_, err := Decode(body)
	if err == nil {
		t.Fatal("Decode returned nil error on unknown role; want ErrUnsupportedColumnRole")
	}
	if !errors.Is(err, ErrUnsupportedColumnRole) {
		t.Fatalf("Decode error = %v; want ErrUnsupportedColumnRole", err)
	}
}

func int64Equal(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
