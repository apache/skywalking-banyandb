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

package frame_test

import (
	"bytes"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/apache/skywalking-banyandb/api/data"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	sharedframe "github.com/apache/skywalking-banyandb/pkg/query/vectorized/frame"
	measureframe "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure/frame"
)

// measureCodec configures the shared base with measure's exact wire numbering so
// its output can be compared byte-for-byte against measure's shipped frame
// package. Any behavioral divergence between the two implementations (validity
// bitmap packing, uvarint encoding, header layout, per-cell proto marshaling)
// fails the byte-equality assertion below — stronger than a constant-only check.
var measureCodec = sharedframe.Codec{
	Magic:       [4]byte{data.RawFrameMagicLeadingByte, 'V', 'F', 'R'},
	WireVersion: 3,
	RoleToWire:  goldenRoleToWire,
	WireToRole:  goldenWireToRole,
	TypeToWire:  goldenTypeToWire,
	WireToType:  goldenWireToType,
}

func goldenRoleToWire(r vectorized.ColumnRole) (uint8, error) {
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
		return 0, sharedframe.ErrUnsupportedColumnRole
	}
}

func goldenWireToRole(b uint8) (vectorized.ColumnRole, error) {
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
		return 0, sharedframe.ErrUnsupportedColumnRole
	}
}

func goldenTypeToWire(t vectorized.ColumnType) (uint8, error) {
	switch t { //nolint:exhaustive // measure's supported subset only
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
		return 0, sharedframe.ErrUnsupportedColumnType
	}
}

func goldenWireToType(b uint8) (vectorized.ColumnType, error) {
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
		return 0, sharedframe.ErrUnsupportedColumnType
	}
}

func goldenStrTagValue(s string) *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: s}}}
}

// buildGoldenBatch builds a representative measure-shaped batch: int64 + string
// + bytes + TagValue columns, some null cells, and a populated tag family.
func buildGoldenBatch() *vectorized.RecordBatch {
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleTimestamp, Name: "ts", Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleTag, Name: "svc", TagFamily: "meta", Type: vectorized.ColumnTypeString},
		{Role: vectorized.RoleTag, Name: "raw", TagFamily: "meta", Type: vectorized.ColumnTypeBytes},
		{Role: vectorized.RoleTag, Name: "tv", TagFamily: "meta", Type: vectorized.ColumnTypeTagValue},
	})
	b := vectorized.NewRecordBatch(schema, 3)

	ts := b.Columns[0].(*vectorized.TypedColumn[int64])
	ts.Append(1000)
	ts.Append(-2000)
	ts.Append(3000)

	svc := b.Columns[1].(*vectorized.TypedColumn[string])
	svc.Append("checkout")
	svc.AppendNull()
	svc.Append("")

	raw := b.Columns[2].(*vectorized.TypedColumn[[]byte])
	raw.Append([]byte{0xDE, 0xAD})
	raw.Append([]byte{})
	raw.AppendNull()

	tv := b.Columns[3].(*vectorized.TypedColumn[*modelv1.TagValue])
	tv.Append(goldenStrTagValue("v0"))
	tv.AppendNull()
	tv.Append(goldenStrTagValue("v2"))

	b.Len = 3
	return b
}

// TestGoldenBytes_SharedEqualsMeasure is the drift gate: the shared base
// configured with measure's numbering and measure's shipped Encode must produce
// byte-identical frames, and each side must decode the other's bytes into equal
// batches. This proves no behavioral divergence exists — the whole reason a
// shared base was chosen over a third fork.
func TestGoldenBytes_SharedEqualsMeasure(t *testing.T) {
	b := buildGoldenBatch()

	sharedBytes, sharedErr := measureCodec.Encode(b)
	if sharedErr != nil {
		t.Fatalf("shared Encode: %v", sharedErr)
	}
	measureBytes, measureErr := measureframe.Encode(buildGoldenBatch())
	if measureErr != nil {
		t.Fatalf("measure Encode: %v", measureErr)
	}
	if !bytes.Equal(sharedBytes, measureBytes) {
		t.Fatalf("encoded bytes diverge:\n  shared  %#x\n  measure %#x", sharedBytes, measureBytes)
	}

	// Cross-decode: measure decodes shared bytes.
	fromMeasure, decMErr := measureframe.Decode(sharedBytes)
	if decMErr != nil {
		t.Fatalf("measure.Decode(sharedBytes): %v", decMErr)
	}
	// Cross-decode: shared decodes measure bytes.
	fromShared, decSErr := measureCodec.Decode(measureBytes)
	if decSErr != nil {
		t.Fatalf("shared.Decode(measureBytes): %v", decSErr)
	}
	assertGoldenBatchEqual(t, buildGoldenBatch(), fromMeasure)
	assertGoldenBatchEqual(t, buildGoldenBatch(), fromShared)
}

// nolint:gocyclo // exhaustive per-type comparison for the assertion is intentionally flat
func assertGoldenBatchEqual(t *testing.T, want, got *vectorized.RecordBatch) {
	t.Helper()
	if got.Len != want.Len || len(got.Columns) != len(want.Columns) {
		t.Fatalf("shape mismatch: got Len=%d cols=%d want Len=%d cols=%d", got.Len, len(got.Columns), want.Len, len(want.Columns))
	}
	for ci := range want.Columns {
		wd, gd := want.Schema.Columns[ci], got.Schema.Columns[ci]
		if wd.Role != gd.Role || wd.Type != gd.Type || wd.Name != gd.Name || wd.TagFamily != gd.TagFamily {
			t.Fatalf("column %d def mismatch: got %+v want %+v", ci, gd, wd)
		}
		wc, gc := want.Columns[ci], got.Columns[ci]
		for r := 0; r < want.Len; r++ {
			if wc.IsNull(r) != gc.IsNull(r) {
				t.Fatalf("column %d row %d null mismatch", ci, r)
			}
			if wc.IsNull(r) {
				continue
			}
			switch w := wc.(type) {
			case *vectorized.TypedColumn[int64]:
				if gc.(*vectorized.TypedColumn[int64]).Data()[r] != w.Data()[r] {
					t.Fatalf("column %d row %d int64 mismatch", ci, r)
				}
			case *vectorized.TypedColumn[string]:
				if gc.(*vectorized.TypedColumn[string]).Data()[r] != w.Data()[r] {
					t.Fatalf("column %d row %d string mismatch", ci, r)
				}
			case *vectorized.TypedColumn[[]byte]:
				if !bytes.Equal(gc.(*vectorized.TypedColumn[[]byte]).Data()[r], w.Data()[r]) {
					t.Fatalf("column %d row %d bytes mismatch", ci, r)
				}
			case *vectorized.TypedColumn[*modelv1.TagValue]:
				if !proto.Equal(gc.(*vectorized.TypedColumn[*modelv1.TagValue]).Data()[r], w.Data()[r]) {
					t.Fatalf("column %d row %d TagValue mismatch", ci, r)
				}
			default:
				t.Fatalf("column %d unexpected type %T", ci, wc)
			}
		}
	}
}
