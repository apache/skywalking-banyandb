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
	"encoding/binary"
	"errors"
	"math"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/apache/skywalking-banyandb/api/data"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// testCodec mirrors measure's exact numbering so the round-trip and negative
// tests exercise the full type/role set the shared base supports.
var testCodec = Codec{
	Magic:       [4]byte{data.RawFrameMagicLeadingByte, 'V', 'F', 'R'},
	WireVersion: 3,
	RoleToWire:  measureRoleToWire,
	WireToRole:  measureWireToRole,
	TypeToWire:  measureTypeToWire,
	WireToType:  measureWireToType,
}

func measureRoleToWire(r vectorized.ColumnRole) (uint8, error) {
	switch r {
	case vectorized.RoleTimestamp:
		return 1, nil
	case vectorized.RoleVersion:
		return 2, nil
	case vectorized.RoleSeriesID:
		return 3, nil
	case vectorized.RoleShardID:
		return 4, nil
	case vectorized.RoleTag:
		return 5, nil
	case vectorized.RoleField:
		return 6, nil
	default:
		return 0, ErrUnsupportedColumnRole
	}
}

func measureWireToRole(b uint8) (vectorized.ColumnRole, error) {
	switch b {
	case 1:
		return vectorized.RoleTimestamp, nil
	case 2:
		return vectorized.RoleVersion, nil
	case 3:
		return vectorized.RoleSeriesID, nil
	case 4:
		return vectorized.RoleShardID, nil
	case 5:
		return vectorized.RoleTag, nil
	case 6:
		return vectorized.RoleField, nil
	default:
		return 0, ErrUnsupportedColumnRole
	}
}

func measureTypeToWire(t vectorized.ColumnType) (uint8, error) {
	switch t { //nolint:exhaustive // test codec deliberately maps only the supported subset
	case vectorized.ColumnTypeInt64:
		return 1, nil
	case vectorized.ColumnTypeFloat64:
		return 2, nil
	case vectorized.ColumnTypeString:
		return 3, nil
	case vectorized.ColumnTypeBytes:
		return 4, nil
	case vectorized.ColumnTypeTagValue:
		return 5, nil
	case vectorized.ColumnTypeFieldValue:
		return 6, nil
	default:
		return 0, ErrUnsupportedColumnType
	}
}

func measureWireToType(b uint8) (vectorized.ColumnType, error) {
	switch b {
	case 1:
		return vectorized.ColumnTypeInt64, nil
	case 2:
		return vectorized.ColumnTypeFloat64, nil
	case 3:
		return vectorized.ColumnTypeString, nil
	case 4:
		return vectorized.ColumnTypeBytes, nil
	case 5:
		return vectorized.ColumnTypeTagValue, nil
	case 6:
		return vectorized.ColumnTypeFieldValue, nil
	default:
		return 0, ErrUnsupportedColumnType
	}
}

func strTagValue(s string) *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: s}}}
}

func intFieldValue(v int64) *modelv1.FieldValue {
	return &modelv1.FieldValue{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: v}}}
}

// buildRichBatch builds a 3-row batch touching every supported column type with
// a null cell in each variable-width column and a populated tag family.
func buildRichBatch(t *testing.T) *vectorized.RecordBatch {
	t.Helper()
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleTimestamp, Name: "ts", Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleField, Name: "f", Type: vectorized.ColumnTypeFloat64},
		{Role: vectorized.RoleTag, Name: "s", TagFamily: "fam", Type: vectorized.ColumnTypeString},
		{Role: vectorized.RoleTag, Name: "b", TagFamily: "fam", Type: vectorized.ColumnTypeBytes},
		{Role: vectorized.RoleTag, Name: "tv", TagFamily: "fam", Type: vectorized.ColumnTypeTagValue},
		{Role: vectorized.RoleField, Name: "fv", Type: vectorized.ColumnTypeFieldValue},
	})
	b := vectorized.NewRecordBatch(schema, 3)

	ts := b.Columns[0].(*vectorized.TypedColumn[int64])
	ts.Append(100)
	ts.Append(200)
	ts.Append(300)

	f := b.Columns[1].(*vectorized.TypedColumn[float64])
	f.Append(1.5)
	f.Append(2.5)
	f.Append(3.5)

	s := b.Columns[2].(*vectorized.TypedColumn[string])
	s.Append("alpha")
	s.AppendNull()
	s.Append("gamma")

	bb := b.Columns[3].(*vectorized.TypedColumn[[]byte])
	bb.Append([]byte{0x01, 0x02})
	bb.Append([]byte{})
	bb.AppendNull()

	tv := b.Columns[4].(*vectorized.TypedColumn[*modelv1.TagValue])
	tv.Append(strTagValue("t0"))
	tv.Append(strTagValue("t1"))
	tv.AppendNull()

	fv := b.Columns[5].(*vectorized.TypedColumn[*modelv1.FieldValue])
	fv.Append(intFieldValue(11))
	fv.AppendNull()
	fv.Append(intFieldValue(33))

	b.Len = 3
	return b
}

func assertBatchesEqual(t *testing.T, want, got *vectorized.RecordBatch) {
	t.Helper()
	if got.Len != want.Len {
		t.Fatalf("Len mismatch: got %d, want %d", got.Len, want.Len)
	}
	if len(got.Columns) != len(want.Columns) {
		t.Fatalf("column count mismatch: got %d, want %d", len(got.Columns), len(want.Columns))
	}
	for ci := range want.Columns {
		wd := want.Schema.Columns[ci]
		gd := got.Schema.Columns[ci]
		if wd.Role != gd.Role || wd.Type != gd.Type || wd.Name != gd.Name || wd.TagFamily != gd.TagFamily {
			t.Fatalf("column %d def mismatch: got %+v, want %+v", ci, gd, wd)
		}
		assertColumnEqual(t, ci, want.Columns[ci], got.Columns[ci])
	}
}

// nolint:gocyclo // exhaustive per-type comparison for the test assertion is intentionally flat
func assertColumnEqual(t *testing.T, ci int, want, got vectorized.Column) {
	t.Helper()
	if got.Len() != want.Len() {
		t.Fatalf("column %d row count mismatch: got %d, want %d", ci, got.Len(), want.Len())
	}
	for r := 0; r < want.Len(); r++ {
		if got.IsNull(r) != want.IsNull(r) {
			t.Fatalf("column %d row %d null mismatch: got %v, want %v", ci, r, got.IsNull(r), want.IsNull(r))
		}
		if want.IsNull(r) {
			continue
		}
		switch wc := want.(type) {
		case *vectorized.TypedColumn[int64]:
			if got.(*vectorized.TypedColumn[int64]).Data()[r] != wc.Data()[r] {
				t.Fatalf("column %d row %d int64 mismatch", ci, r)
			}
		case *vectorized.TypedColumn[float64]:
			if got.(*vectorized.TypedColumn[float64]).Data()[r] != wc.Data()[r] {
				t.Fatalf("column %d row %d float64 mismatch", ci, r)
			}
		case *vectorized.TypedColumn[string]:
			if got.(*vectorized.TypedColumn[string]).Data()[r] != wc.Data()[r] {
				t.Fatalf("column %d row %d string mismatch", ci, r)
			}
		case *vectorized.TypedColumn[[]byte]:
			if !bytes.Equal(got.(*vectorized.TypedColumn[[]byte]).Data()[r], wc.Data()[r]) {
				t.Fatalf("column %d row %d bytes mismatch", ci, r)
			}
		case *vectorized.TypedColumn[*modelv1.TagValue]:
			if !proto.Equal(got.(*vectorized.TypedColumn[*modelv1.TagValue]).Data()[r], wc.Data()[r]) {
				t.Fatalf("column %d row %d TagValue mismatch", ci, r)
			}
		case *vectorized.TypedColumn[*modelv1.FieldValue]:
			if !proto.Equal(got.(*vectorized.TypedColumn[*modelv1.FieldValue]).Data()[r], wc.Data()[r]) {
				t.Fatalf("column %d row %d FieldValue mismatch", ci, r)
			}
		default:
			t.Fatalf("column %d unexpected type %T", ci, want)
		}
	}
}

func TestCodec_RoundTrip_AllColumnTypes(t *testing.T) {
	want := buildRichBatch(t)
	encoded, err := testCodec.Encode(want)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	got, decErr := testCodec.Decode(encoded)
	if decErr != nil {
		t.Fatalf("Decode: %v", decErr)
	}
	assertBatchesEqual(t, want, got)
}

func TestCodec_RoundTrip_EmptyBatch(t *testing.T) {
	want := vectorized.NewRecordBatch(vectorized.NewBatchSchema(nil), 0)
	encoded, err := testCodec.Encode(want)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if len(encoded) != MinHeaderLen {
		t.Fatalf("empty frame length = %d, want %d", len(encoded), MinHeaderLen)
	}
	got, decErr := testCodec.Decode(encoded)
	if decErr != nil {
		t.Fatalf("Decode: %v", decErr)
	}
	if got.Len != 0 || len(got.Columns) != 0 {
		t.Fatalf("empty batch decoded to Len=%d cols=%d", got.Len, len(got.Columns))
	}
}

func TestCodec_RoundTrip_Selection(t *testing.T) {
	want := buildRichBatch(t)
	want.Selection = []uint16{2, 0} // reorder + drop row 1
	encoded, err := testCodec.Encode(want)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	got, decErr := testCodec.Decode(encoded)
	if decErr != nil {
		t.Fatalf("Decode: %v", decErr)
	}
	if got.Len != 2 {
		t.Fatalf("selected Len = %d, want 2", got.Len)
	}
	ts := got.Columns[0].(*vectorized.TypedColumn[int64])
	if ts.Data()[0] != 300 || ts.Data()[1] != 100 {
		t.Fatalf("selection order wrong: %v", ts.Data())
	}
}

func TestCodec_Decode_BadMagic(t *testing.T) {
	encoded, _ := testCodec.Encode(buildRichBatch(t))
	encoded[1] = 'X'
	_, err := testCodec.Decode(encoded)
	if !errors.Is(err, ErrBadMagic) {
		t.Fatalf("got %v, want ErrBadMagic", err)
	}
}

func TestCodec_Decode_BadVersion(t *testing.T) {
	encoded, _ := testCodec.Encode(buildRichBatch(t))
	encoded[MagicLen] = 99
	_, err := testCodec.Decode(encoded)
	if !errors.Is(err, ErrBadVersion) {
		t.Fatalf("got %v, want ErrBadVersion", err)
	}
}

func TestCodec_Decode_Truncated(t *testing.T) {
	encoded, _ := testCodec.Encode(buildRichBatch(t))
	_, err := testCodec.Decode(encoded[:len(encoded)-3])
	if !errors.Is(err, ErrTruncated) {
		t.Fatalf("got %v, want ErrTruncated", err)
	}
}

func TestCodec_Decode_TooShortForHeader(t *testing.T) {
	_, err := testCodec.Decode([]byte{0x00, 'V'})
	if !errors.Is(err, ErrTruncated) {
		t.Fatalf("got %v, want ErrTruncated", err)
	}
}

// TestCodec_Decode_NumRowsExceedsBody guards the adversarial-header path: a
// near-2^64 NumRows must be rejected loudly at ValidateHeader, never reaching
// the column decoder where (NumRows+7)/8 would overflow and make([]bool, NumRows)
// would OOM-panic. The test passing without a panic is itself the assertion.
func TestCodec_Decode_NumRowsExceedsBody(t *testing.T) {
	var b []byte
	b = append(b, testCodec.Magic[:]...)
	b = append(b, testCodec.WireVersion)
	b = binary.AppendUvarint(b, math.MaxUint64) // NumRows far larger than any body
	b = binary.AppendUvarint(b, 1)              // NumCols=1
	b = append(b, 0x01, 0x01)                   // partial column bytes; never reached
	_, err := testCodec.Decode(b)
	if !errors.Is(err, ErrTruncated) {
		t.Fatalf("got %v, want ErrTruncated for NumRows exceeding body", err)
	}
}

// TestCodec_Decode_NumColsExceedsBody guards the same class for NumCols.
func TestCodec_Decode_NumColsExceedsBody(t *testing.T) {
	var b []byte
	b = append(b, testCodec.Magic[:]...)
	b = append(b, testCodec.WireVersion)
	b = binary.AppendUvarint(b, 0)              // NumRows=0
	b = binary.AppendUvarint(b, math.MaxUint64) // NumCols far larger than any body
	_, err := testCodec.Decode(b)
	if !errors.Is(err, ErrTruncated) {
		t.Fatalf("got %v, want ErrTruncated for NumCols exceeding body", err)
	}
}

func TestCodec_Decode_TrailingBytes(t *testing.T) {
	encoded, _ := testCodec.Encode(buildRichBatch(t))
	encoded = append(encoded, 0xFF)
	_, err := testCodec.Decode(encoded)
	if !errors.Is(err, ErrTruncated) {
		t.Fatalf("got %v, want ErrTruncated for trailing bytes", err)
	}
}

func TestCodec_Decode_UnknownRoleByte(t *testing.T) {
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleTimestamp, Name: "ts", Type: vectorized.ColumnTypeInt64},
	})
	b := vectorized.NewRecordBatch(schema, 1)
	b.Columns[0].(*vectorized.TypedColumn[int64]).Append(1)
	b.Len = 1
	encoded, _ := testCodec.Encode(b)
	// role byte sits right after magic(4)+version(1)+nrows(1)+ncols(1) = offset 7.
	encoded[7] = 200
	_, err := testCodec.Decode(encoded)
	if !errors.Is(err, ErrUnsupportedColumnRole) {
		t.Fatalf("got %v, want ErrUnsupportedColumnRole", err)
	}
}

func TestCodec_Decode_UnknownTypeByte(t *testing.T) {
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleTimestamp, Name: "ts", Type: vectorized.ColumnTypeInt64},
	})
	b := vectorized.NewRecordBatch(schema, 1)
	b.Columns[0].(*vectorized.TypedColumn[int64]).Append(1)
	b.Len = 1
	encoded, _ := testCodec.Encode(b)
	encoded[8] = 200 // type byte
	_, err := testCodec.Decode(encoded)
	if !errors.Is(err, ErrUnsupportedColumnType) {
		t.Fatalf("got %v, want ErrUnsupportedColumnType", err)
	}
}

func TestCodec_Encode_NilBatch(t *testing.T) {
	if _, err := testCodec.Encode(nil); err == nil {
		t.Fatal("expected error for nil batch")
	}
}

func TestCodec_Encode_UnsupportedRole(t *testing.T) {
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleOrderKey, Name: "ok", Type: vectorized.ColumnTypeBytes},
	})
	b := vectorized.NewRecordBatch(schema, 0)
	if _, err := testCodec.Encode(b); !errors.Is(err, ErrUnsupportedColumnRole) {
		t.Fatalf("got %v, want ErrUnsupportedColumnRole", err)
	}
}
