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
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/apache/skywalking-banyandb/api/data"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	querymodel "github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	baseframe "github.com/apache/skywalking-banyandb/pkg/query/vectorized/frame"
	vstream "github.com/apache/skywalking-banyandb/pkg/query/vectorized/stream"
)

func strTagValue(s string) *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: s}}}
}

// buildStreamBatch builds a 2-row index-ordered stream batch via the production
// schema builder: ts / elementID / seriesID (Int64), one tag (TagValue) and an
// order-key (Bytes) column.
func buildStreamBatch(t *testing.T) *vectorized.RecordBatch {
	t.Helper()
	schema := vstream.BuildStreamBatchSchema(
		[]querymodel.TagProjection{{Family: "searchable", Names: []string{"service_id"}}},
		"searchable", "service_id",
	)
	b := vectorized.NewRecordBatch(schema, 2)

	ts := b.Columns[schema.TimestampIndex()].(*vectorized.TypedColumn[int64])
	ts.Append(111)
	ts.Append(222)

	eid := b.Columns[schema.ElementIDIndex()].(*vectorized.TypedColumn[int64])
	eid.Append(vstream.ElementIDToColumn(0xFFFFFFFFFFFFFFFF)) // high-bit exercise
	eid.Append(vstream.ElementIDToColumn(7))

	sid := b.Columns[schema.SeriesIDIndex()].(*vectorized.TypedColumn[int64])
	sid.Append(vstream.SeriesIDToColumn(9))
	sid.Append(vstream.SeriesIDToColumn(9))

	tagIdx := schema.Columns[3]
	if tagIdx.Role != vectorized.RoleTag {
		t.Fatalf("expected column 3 to be the tag column, got role %d", tagIdx.Role)
	}
	tag := b.Columns[3].(*vectorized.TypedColumn[*modelv1.TagValue])
	tag.Append(strTagValue("svc-A"))
	tag.AppendNull()

	ok := b.Columns[schema.OrderKeyIndex()].(*vectorized.TypedColumn[[]byte])
	ok.Append([]byte{0x01, 0x02})
	ok.Append([]byte{0x03})

	b.Len = 2
	return b
}

func TestStreamFrame_Magic(t *testing.T) {
	if Magic[0] != data.RawFrameMagicLeadingByte {
		t.Fatalf("Magic[0]=%#x, want %#x", Magic[0], data.RawFrameMagicLeadingByte)
	}
	if want := [4]byte{0x00, 'V', 'F', 'R'}; Magic != want {
		t.Fatalf("Magic=%#x, want %#x", Magic[:], want[:])
	}
	if WireVersion != 1 {
		t.Fatalf("WireVersion=%d, want 1", WireVersion)
	}
}

func TestStreamFrame_RoundTrip(t *testing.T) {
	want := buildStreamBatch(t)
	encoded, err := Encode(want)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	got, decErr := Decode(encoded)
	if decErr != nil {
		t.Fatalf("Decode: %v", decErr)
	}
	if got.Len != want.Len || len(got.Columns) != len(want.Columns) {
		t.Fatalf("shape mismatch: got Len=%d cols=%d", got.Len, len(got.Columns))
	}
	// ElementID high-bit survives the int64 bit-cast round-trip.
	gotEID := got.Columns[got.Schema.ElementIDIndex()].(*vectorized.TypedColumn[int64])
	if vstream.ColumnToElementID(gotEID.Data()[0]) != 0xFFFFFFFFFFFFFFFF {
		t.Fatalf("elementID high-bit lost: %#x", vstream.ColumnToElementID(gotEID.Data()[0]))
	}
	// Tag passthrough survives, including the null cell.
	gotTag := got.Columns[3].(*vectorized.TypedColumn[*modelv1.TagValue])
	if !proto.Equal(gotTag.Data()[0], strTagValue("svc-A")) {
		t.Fatalf("tag[0] mismatch: %v", gotTag.Data()[0])
	}
	if !gotTag.IsNull(1) {
		t.Fatalf("tag[1] should be null")
	}
	// Order key bytes survive.
	gotOK := got.Columns[got.Schema.OrderKeyIndex()].(*vectorized.TypedColumn[[]byte])
	if !bytes.Equal(gotOK.Data()[0], []byte{0x01, 0x02}) || !bytes.Equal(gotOK.Data()[1], []byte{0x03}) {
		t.Fatalf("order key mismatch: %v", gotOK.Data())
	}
}

func TestStreamFrame_EmptyBatch(t *testing.T) {
	want := vectorized.NewRecordBatch(vectorized.NewBatchSchema(nil), 0)
	encoded, err := Encode(want)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	got, decErr := Decode(encoded)
	if decErr != nil {
		t.Fatalf("Decode: %v", decErr)
	}
	if got.Len != 0 || len(got.Columns) != 0 {
		t.Fatalf("empty batch decoded to Len=%d cols=%d", got.Len, len(got.Columns))
	}
}

func TestStreamFrame_BadMagic(t *testing.T) {
	encoded, _ := Encode(buildStreamBatch(t))
	encoded[3] = 'Z'
	if _, err := Decode(encoded); !errors.Is(err, baseframe.ErrBadMagic) {
		t.Fatalf("got %v, want ErrBadMagic", err)
	}
}

func TestStreamFrame_BadVersion(t *testing.T) {
	encoded, _ := Encode(buildStreamBatch(t))
	encoded[baseframe.MagicLen] = 99
	if _, err := Decode(encoded); !errors.Is(err, baseframe.ErrBadVersion) {
		t.Fatalf("got %v, want ErrBadVersion", err)
	}
}

func TestStreamFrame_Truncated(t *testing.T) {
	encoded, _ := Encode(buildStreamBatch(t))
	if _, err := Decode(encoded[:len(encoded)-2]); !errors.Is(err, baseframe.ErrTruncated) {
		t.Fatalf("got %v, want ErrTruncated", err)
	}
}

func TestStreamFrame_UnsupportedType(t *testing.T) {
	// Float64 is not in stream's wire type set; Encode must reject it.
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleTimestamp, Name: "ts", Type: vectorized.ColumnTypeFloat64},
	})
	b := vectorized.NewRecordBatch(schema, 0)
	if _, err := Encode(b); !errors.Is(err, baseframe.ErrUnsupportedColumnType) {
		t.Fatalf("got %v, want ErrUnsupportedColumnType", err)
	}
}
