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
	"errors"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/apache/skywalking-banyandb/api/data"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// TestMagic_LeadingByteIs00 pins the codec contract at the package boundary:
// frame.Magic[0] MUST equal api/data.RawFrameMagicLeadingByte (0x00). The
// remaining bytes are the distinctive 'VFR' signature so a flag-on decoder
// can recognize the frame, but the load-bearing safety property is that the
// first byte is the field-0 varint tag.
func TestMagic_LeadingByteIs00(t *testing.T) {
	if Magic[0] != data.RawFrameMagicLeadingByte {
		t.Fatalf("Magic[0]=%#x, want %#x (data.RawFrameMagicLeadingByte)", Magic[0], data.RawFrameMagicLeadingByte)
	}
	if Magic[0] != 0x00 {
		t.Fatalf("Magic[0]=%#x, want 0x00 (field-0 varint tag)", Magic[0])
	}
	if want := [4]byte{0x00, 'V', 'F', 'R'}; Magic != want {
		t.Fatalf("Magic=%#x, want %#x", Magic[:], want[:])
	}
}

// TestEncode_RespectsSelection asserts the encoder honors RecordBatch.Selection
// — only the listed rows are emitted, and in the listed order. With Len=5 and
// Selection={0,2,4} the frame contains 3 rows pulled from those source indices.
func TestEncode_RespectsSelection(t *testing.T) {
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleField, Name: "v", Type: vectorized.ColumnTypeInt64},
	})
	b := vectorized.NewRecordBatch(schema, 5)
	col := b.Columns[0].(*vectorized.TypedColumn[int64])
	for _, v := range []int64{10, 20, 30, 40, 50} {
		col.Append(v)
	}
	b.Len = 5
	b.Selection = []uint16{0, 2, 4}

	got, err := Encode(b)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	decoded, decodeErr := Decode(got)
	if decodeErr != nil {
		t.Fatalf("Decode: %v", decodeErr)
	}
	defer decoded.ReleaseOwners()
	if decoded.Len != 3 {
		t.Fatalf("decoded rows = %d, want 3 active rows from Selection", decoded.Len)
	}
	if gotVals := decoded.Columns[0].(*vectorized.TypedColumn[int64]).Data(); !int64Equal(gotVals, []int64{10, 30, 50}) {
		t.Fatalf("decoded values = %v, want [10 30 50]", gotVals)
	}
}

// TestEncode_ProtoUnmarshal_FailsLoud is the G9f spec's load-bearing proof
// test: any raw frame produced by Encode, when fed to proto.Unmarshal into a
// *measurev1.InternalQueryResponse{}, MUST return a non-nil error. The 0x00
// leading magic byte (field-0 varint tag) forces protowire.ConsumeTag to
// errCodeFieldNumber and proto/decode.go to errDecode *before* any
// unknown-field skip — collapsing "garbage-but-parsed silently-empty" into
// a loud decode error (verified against google.golang.org/protobuf@v1.36.11).
// This is the engineered fail-loud guarantee that makes the hard-cutover
// model safe against a botched partial restart.
func TestEncode_ProtoUnmarshal_FailsLoud(t *testing.T) {
	cases := []struct {
		build func() *vectorized.RecordBatch
		name  string
	}{
		{
			name:  "header-only",
			build: func() *vectorized.RecordBatch { return vectorized.NewRecordBatch(vectorized.NewBatchSchema(nil), 0) },
		},
		{
			name: "single-int64-row",
			build: func() *vectorized.RecordBatch {
				s := vectorized.NewBatchSchema([]vectorized.ColumnDef{
					{Role: vectorized.RoleField, Name: "n", Type: vectorized.ColumnTypeInt64},
				})
				b := vectorized.NewRecordBatch(s, 1)
				b.Columns[0].(*vectorized.TypedColumn[int64]).Append(42)
				b.Len = 1
				return b
			},
		},
		{
			name: "string-and-shard-id",
			build: func() *vectorized.RecordBatch {
				s := vectorized.NewBatchSchema([]vectorized.ColumnDef{
					{Role: vectorized.RoleShardID, Name: "shard_id", Type: vectorized.ColumnTypeInt64},
					{Role: vectorized.RoleTag, TagFamily: "geo", Name: "region", Type: vectorized.ColumnTypeString},
					{Role: vectorized.RoleField, Name: "sum_value", Type: vectorized.ColumnTypeFloat64},
				})
				b := vectorized.NewRecordBatch(s, 2)
				b.Columns[0].(*vectorized.TypedColumn[int64]).Append(7)
				b.Columns[0].(*vectorized.TypedColumn[int64]).Append(7)
				b.Columns[1].(*vectorized.TypedColumn[string]).Append("us-east")
				b.Columns[1].(*vectorized.TypedColumn[string]).Append("us-west")
				b.Columns[2].(*vectorized.TypedColumn[float64]).Append(3.14)
				b.Columns[2].(*vectorized.TypedColumn[float64]).Append(2.72)
				b.Len = 2
				return b
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := Encode(tc.build())
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			if len(raw) == 0 || raw[0] != 0x00 {
				t.Fatalf("frame missing 0x00 leading byte: %#x", raw[:minInt(len(raw), 8)])
			}
			var m measurev1.InternalQueryResponse
			if unmarshalErr := proto.Unmarshal(raw, &m); unmarshalErr == nil {
				t.Fatalf("proto.Unmarshal of 0x00-leading raw frame into *InternalQueryResponse{} returned nil error; want fail-loud (silently-parsed: %+v)", &m)
			}
		})
	}
}

// TestValidateHeader_Roundtrip checks the positive path: a frame produced by
// Encode validates and reports the expected header dimensions.
func TestValidateHeader_Roundtrip(t *testing.T) {
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleField, Name: "n", Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleTag, TagFamily: "geo", Name: "region", Type: vectorized.ColumnTypeString},
	})
	b := vectorized.NewRecordBatch(schema, 3)
	b.Columns[0].(*vectorized.TypedColumn[int64]).Append(1)
	b.Columns[0].(*vectorized.TypedColumn[int64]).Append(2)
	b.Columns[0].(*vectorized.TypedColumn[int64]).Append(3)
	b.Columns[1].(*vectorized.TypedColumn[string]).Append("a")
	b.Columns[1].(*vectorized.TypedColumn[string]).Append("b")
	b.Columns[1].(*vectorized.TypedColumn[string]).Append("c")
	b.Len = 3

	raw, err := Encode(b)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	h, n, err := ValidateHeader(raw)
	if err != nil {
		t.Fatalf("ValidateHeader: %v", err)
	}
	if h.Magic != Magic {
		t.Fatalf("Magic = %#x, want %#x", h.Magic[:], Magic[:])
	}
	if h.WireVersion != WireVersion {
		t.Fatalf("WireVersion = %d, want %d", h.WireVersion, WireVersion)
	}
	if h.NumRows != 3 {
		t.Fatalf("NumRows = %d, want 3", h.NumRows)
	}
	if h.NumCols != 2 {
		t.Fatalf("NumCols = %d, want 2", h.NumCols)
	}
	if n != 7 {
		t.Fatalf("bytes consumed = %d, want 7 (minimal header for nrows<128, ncols<128)", n)
	}
}

// TestValidateHeader_Negatives covers the fail-loud arms: a flag-off node
// (or a partial restart) receiving a non-frame body, a wrong-magic body, a
// wrong-version body, or a truncated body must each return a loud typed
// error — never silently succeed.
func TestValidateHeader_Negatives(t *testing.T) {
	cases := []struct {
		wantErr error
		name    string
		body    []byte
	}{
		{name: "empty", body: nil, wantErr: ErrTruncated},
		{name: "short-of-min", body: []byte{0x00, 'V', 'F'}, wantErr: ErrTruncated},
		{name: "bad-magic-first-byte", body: []byte{0x08, 'V', 'F', 'R', 0x01, 0x00, 0x00}, wantErr: ErrBadMagic},
		{name: "bad-magic-signature", body: []byte{0x00, 'X', 'Y', 'Z', 0x01, 0x00, 0x00}, wantErr: ErrBadMagic},
		{name: "bad-version", body: []byte{0x00, 'V', 'F', 'R', 0x03, 0x00, 0x00}, wantErr: ErrBadVersion},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := ValidateHeader(tc.body)
			if err == nil {
				t.Fatalf("ValidateHeader returned nil err; want %v", tc.wantErr)
			}
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("ValidateHeader err = %v; want errors.Is %v", err, tc.wantErr)
			}
		})
	}
}

// TestEncode_UnsupportedColumnType_FailsLoud confirms that handing the
// encoder a column whose type has no frame mapping yields a loud typed
// error at encode time — never silently-wrong wire bytes downstream.
func TestEncode_UnsupportedColumnType_FailsLoud(t *testing.T) {
	// Array column types (Int64Array / StrArray) remain unsupported by
	// the frame; only scalar-or-passthrough types are encodable today.
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleTag, Name: "tag", Type: vectorized.ColumnTypeInt64Array},
	})
	b := vectorized.NewRecordBatch(schema, 0)
	_, err := Encode(b)
	if err == nil {
		t.Fatal("Encode of unsupported column type returned nil err; want fail-loud")
	}
	if !errors.Is(err, ErrUnsupportedColumnType) {
		t.Fatalf("err = %v; want errors.Is ErrUnsupportedColumnType", err)
	}
}

// TestEncode_NilBatch_FailsLoud asserts a defensive nil-guard so callers
// surface a missing batch loudly rather than emitting an invalid frame.
func TestEncode_NilBatch_FailsLoud(t *testing.T) {
	if _, err := Encode(nil); err == nil {
		t.Fatal("Encode(nil) returned nil err; want fail-loud")
	}
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
