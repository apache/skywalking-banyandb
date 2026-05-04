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
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// Test helpers — minimal constructors for *modelv1.TagValue / *modelv1.FieldValue.

func tvNull() *modelv1.TagValue { return &modelv1.TagValue{Value: &modelv1.TagValue_Null{}} }

func tvInt(v int64) *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: v}}}
}

func tvStr(v string) *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: v}}}
}

func tvBytes(v []byte) *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_BinaryData{BinaryData: v}}
}

func tvIntArr(v []int64) *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_IntArray{IntArray: &modelv1.IntArray{Value: v}}}
}

func tvStrArr(v []string) *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_StrArray{StrArray: &modelv1.StrArray{Value: v}}}
}

func tvTimestamp(ts time.Time) *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_Timestamp{Timestamp: timestamppb.New(ts)}}
}

func fvNull() *modelv1.FieldValue {
	return &modelv1.FieldValue{Value: &modelv1.FieldValue_Null{}}
}

func fvInt(v int64) *modelv1.FieldValue {
	return &modelv1.FieldValue{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: v}}}
}

func fvFloat(v float64) *modelv1.FieldValue {
	return &modelv1.FieldValue{Value: &modelv1.FieldValue_Float{Float: &modelv1.Float{Value: v}}}
}

func fvStr(v string) *modelv1.FieldValue {
	return &modelv1.FieldValue{Value: &modelv1.FieldValue_Str{Str: &modelv1.Str{Value: v}}}
}

func fvBytes(v []byte) *modelv1.FieldValue {
	return &modelv1.FieldValue{Value: &modelv1.FieldValue_BinaryData{BinaryData: v}}
}

// preallocInt64 grows an Int64 column by n zero rows so extractTagRow can
// write into pre-existing slots (mirrors how BatchScan pre-grows columns
// before calling per-row extractors at series boundaries).
func preallocInt64(c *vectorized.TypedColumn[int64], n int) {
	for range n {
		c.Append(0)
	}
}

// extractTagRow positives.

func TestExtractTagRow_Int_WritesValue(t *testing.T) {
	col := vectorized.NewInt64Column(4)
	preallocInt64(col, 3)
	if err := extractTagRow(col, 1, tvInt(42)); err != nil {
		t.Fatal(err)
	}
	if col.Data()[1] != 42 {
		t.Fatalf("row 1: got %d, want 42", col.Data()[1])
	}
	if col.IsNull(1) {
		t.Fatal("row 1 must not be null after writing valid int")
	}
}

func TestExtractTagRow_Str_WritesValue(t *testing.T) {
	col := vectorized.NewStringColumn(4)
	col.Append("")
	col.Append("")
	if err := extractTagRow(col, 0, tvStr("hello")); err != nil {
		t.Fatal(err)
	}
	if col.Data()[0] != "hello" {
		t.Fatalf("row 0: got %q, want hello", col.Data()[0])
	}
}

// Per Copilot G3 review (option A): TagValue_Timestamp is unsupported in v1.
// Without schema-level tag-kind metadata, extract+serialize cannot round-trip
// the variant; the row path's mustDecodeTagValue panics on Timestamp tags
// anyway. This test pins the explicit-error contract — silent degradation is
// forbidden.
func TestExtractTagRow_Timestamp_ReturnsError(t *testing.T) {
	ts := time.Date(2026, 5, 4, 12, 34, 56, 789, time.UTC)
	col := vectorized.NewInt64Column(1)
	preallocInt64(col, 1)
	if err := extractTagRow(col, 0, tvTimestamp(ts)); err == nil {
		t.Fatal("Timestamp variant unsupported in v1 — extract must return error, not silently degrade")
	}
}

// extractTagRow defensive-copy parity-killers.

func TestExtractTagRow_BinaryData_DefensiveCopy(t *testing.T) {
	src := []byte("abc")
	tv := tvBytes(src)
	col := vectorized.NewBytesColumn(2)
	col.Append(nil)
	if err := extractTagRow(col, 0, tv); err != nil {
		t.Fatal(err)
	}
	src[0] = 'z' // mutate source AFTER extract
	if !bytes.Equal(col.Data()[0], []byte("abc")) {
		t.Fatalf("aliasing detected: column observed mutation of source, got %q", col.Data()[0])
	}
}

func TestExtractTagRow_IntArray_DefensiveCopy(t *testing.T) {
	src := []int64{1, 2, 3}
	tv := tvIntArr(src)
	col := vectorized.NewInt64ArrayColumn(2)
	col.Append(nil)
	if err := extractTagRow(col, 0, tv); err != nil {
		t.Fatal(err)
	}
	src[0] = 99
	if col.Data()[0][0] != 1 {
		t.Fatalf("aliasing detected: column observed source mutation, got %v", col.Data()[0])
	}
}

func TestExtractTagRow_StrArray_DefensiveCopy(t *testing.T) {
	src := []string{"a", "b"}
	tv := tvStrArr(src)
	col := vectorized.NewStrArrayColumn(2)
	col.Append(nil)
	if err := extractTagRow(col, 0, tv); err != nil {
		t.Fatal(err)
	}
	src[0] = "z"
	if col.Data()[0][0] != "a" {
		t.Fatalf("aliasing detected: column observed source mutation, got %v", col.Data()[0])
	}
}

// extractTagRow Null.

func TestExtractTagRow_Null_MarksValidityWithoutGrowingLen(t *testing.T) {
	col := vectorized.NewInt64Column(4)
	preallocInt64(col, 3)
	beforeLen := col.Len()
	if err := extractTagRow(col, 1, tvNull()); err != nil {
		t.Fatal(err)
	}
	if col.Len() != beforeLen {
		t.Fatalf("Len must not change on Null extract: before=%d after=%d", beforeLen, col.Len())
	}
	if !col.IsNull(1) {
		t.Fatal("row 1 must be null")
	}
}

// extractTagRow error paths.

func TestExtractTagRow_UnknownVariant_ReturnsError(t *testing.T) {
	tv := &modelv1.TagValue{} // Value is nil — no oneof set
	col := vectorized.NewInt64Column(1)
	preallocInt64(col, 1)
	if err := extractTagRow(col, 0, tv); err == nil {
		t.Fatal("unknown TagValue variant must return error (silent null fallback would mask schema-evolution bugs)")
	}
}

func TestExtractTagRow_NilTagValue_ReturnsError(t *testing.T) {
	col := vectorized.NewInt64Column(1)
	preallocInt64(col, 1)
	if err := extractTagRow(col, 0, nil); err == nil {
		t.Fatal("nil TagValue must return error")
	}
}

func TestExtractTagRow_ColumnTypeMismatch_ReturnsError(t *testing.T) {
	intCol := vectorized.NewInt64Column(1)
	preallocInt64(intCol, 1)
	if err := extractTagRow(intCol, 0, tvStr("oops")); err == nil {
		t.Fatal("string TagValue into int64 column must return error")
	}
}

// extractFieldRow positives.

func TestExtractFieldRow_Int_WritesValue(t *testing.T) {
	col := vectorized.NewInt64Column(2)
	preallocInt64(col, 1)
	if err := extractFieldRow(col, 0, fvInt(7)); err != nil {
		t.Fatal(err)
	}
	if col.Data()[0] != 7 {
		t.Fatalf("got %d, want 7", col.Data()[0])
	}
}

func TestExtractFieldRow_Float_WritesValue(t *testing.T) {
	col := vectorized.NewFloat64Column(1)
	col.Append(0)
	if err := extractFieldRow(col, 0, fvFloat(3.14)); err != nil {
		t.Fatal(err)
	}
	if col.Data()[0] != 3.14 {
		t.Fatalf("got %v, want 3.14", col.Data()[0])
	}
}

func TestExtractFieldRow_Str_WritesValue(t *testing.T) {
	col := vectorized.NewStringColumn(1)
	col.Append("")
	if err := extractFieldRow(col, 0, fvStr("hi")); err != nil {
		t.Fatal(err)
	}
	if col.Data()[0] != "hi" {
		t.Fatal("string field roundtrip failed")
	}
}

func TestExtractFieldRow_BinaryData_DefensiveCopy(t *testing.T) {
	src := []byte("xyz")
	fv := fvBytes(src)
	col := vectorized.NewBytesColumn(1)
	col.Append(nil)
	if err := extractFieldRow(col, 0, fv); err != nil {
		t.Fatal(err)
	}
	src[0] = 'q'
	if !bytes.Equal(col.Data()[0], []byte("xyz")) {
		t.Fatalf("aliasing detected on FieldValue_BinaryData: %q", col.Data()[0])
	}
}

func TestExtractFieldRow_Null_MarksValidity(t *testing.T) {
	col := vectorized.NewInt64Column(1)
	preallocInt64(col, 1)
	if err := extractFieldRow(col, 0, fvNull()); err != nil {
		t.Fatal(err)
	}
	if !col.IsNull(0) {
		t.Fatal("Null FieldValue must mark column null")
	}
}

// extractFieldRow error paths.

func TestExtractFieldRow_UnknownVariant_ReturnsError(t *testing.T) {
	fv := &modelv1.FieldValue{}
	col := vectorized.NewInt64Column(1)
	preallocInt64(col, 1)
	if err := extractFieldRow(col, 0, fv); err == nil {
		t.Fatal("unknown FieldValue variant must return error")
	}
}

func TestExtractFieldRow_NilFieldValue_ReturnsError(t *testing.T) {
	col := vectorized.NewInt64Column(1)
	preallocInt64(col, 1)
	if err := extractFieldRow(col, 0, nil); err == nil {
		t.Fatal("nil FieldValue must return error")
	}
}

func TestExtractFieldRow_ColumnTypeMismatch_ReturnsError(t *testing.T) {
	intCol := vectorized.NewInt64Column(1)
	preallocInt64(intCol, 1)
	if err := extractFieldRow(intCol, 0, fvFloat(1.5)); err == nil {
		t.Fatal("Float FieldValue into int64 column must return error")
	}
}

// extract bulk variants.

func TestExtractTagBulk_HappyPath(t *testing.T) {
	col := vectorized.NewStringColumn(4)
	for range 4 {
		col.Append("")
	}
	src := []*modelv1.TagValue{tvStr("a"), tvStr("b"), tvStr("c"), tvStr("d")}
	if err := extractTagBulk(col, 0, src, 4); err != nil {
		t.Fatal(err)
	}
	for i, want := range []string{"a", "b", "c", "d"} {
		if col.Data()[i] != want {
			t.Fatalf("row %d: got %q, want %q", i, col.Data()[i], want)
		}
	}
}

func TestExtractTagBulk_NTooLarge_ReturnsError(t *testing.T) {
	col := vectorized.NewStringColumn(4)
	for range 4 {
		col.Append("")
	}
	src := []*modelv1.TagValue{tvStr("a")}
	if err := extractTagBulk(col, 0, src, 5); err == nil {
		t.Fatal("n > len(src) must return error")
	}
}

func TestExtractFieldBulk_HappyPath(t *testing.T) {
	col := vectorized.NewInt64Column(3)
	preallocInt64(col, 3)
	src := []*modelv1.FieldValue{fvInt(10), fvInt(20), fvInt(30)}
	if err := extractFieldBulk(col, 0, src, 3); err != nil {
		t.Fatal(err)
	}
	for i, want := range []int64{10, 20, 30} {
		if col.Data()[i] != want {
			t.Fatalf("row %d: got %d, want %d", i, col.Data()[i], want)
		}
	}
}

func TestExtractFieldBulk_NTooLarge_ReturnsError(t *testing.T) {
	col := vectorized.NewInt64Column(2)
	preallocInt64(col, 2)
	src := []*modelv1.FieldValue{fvInt(1)}
	if err := extractFieldBulk(col, 0, src, 2); err == nil {
		t.Fatal("n > len(src) must return error")
	}
}
