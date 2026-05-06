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

package vectorized

// ColumnType is the runtime type tag for a Column.
type ColumnType int

// ColumnType variants. Each value corresponds to a TypedColumn[T] specialization.
//
// The TagValue / FieldValue variants are passthrough columns: they hold
// the original *modelv1.TagValue / *modelv1.FieldValue pointers from the
// scan source unchanged, eliminating the decode/re-encode round trip
// when no operator consumes the typed value. They are only useful when
// the scan output is destined for the egress serializer; an operator that
// needs typed primitives should pick a typed column type instead.
const (
	ColumnTypeInt64 ColumnType = iota
	ColumnTypeFloat64
	ColumnTypeString
	ColumnTypeBytes
	ColumnTypeInt64Array
	ColumnTypeStrArray
	ColumnTypeTagValue
	ColumnTypeFieldValue
)

// String returns a human label used in diagnostics and error messages.
func (c ColumnType) String() string {
	switch c {
	case ColumnTypeInt64:
		return "int64"
	case ColumnTypeFloat64:
		return "float64"
	case ColumnTypeString:
		return "string"
	case ColumnTypeBytes:
		return "bytes"
	case ColumnTypeInt64Array:
		return "int64[]"
	case ColumnTypeStrArray:
		return "string[]"
	case ColumnTypeTagValue:
		return "tagvalue"
	case ColumnTypeFieldValue:
		return "fieldvalue"
	}
	return "unknown"
}

// Column is the storage-agnostic view of one column in a RecordBatch.
type Column interface {
	Type() ColumnType
	Len() int
	IsNull(i int) bool
	Reset()
	AppendNull()
	MarkNullAt(i int)
}

// validityBitmap tracks per-row null state. nil bits = all valid.
// Allocation is lazy on first MarkNull.
type validityBitmap struct {
	bits []uint64
}

// IsNull reports whether bit i is null.
func (v *validityBitmap) IsNull(i int) bool {
	if i < 0 {
		return false
	}
	word := i / 64
	if word >= len(v.bits) {
		return false
	}
	return (v.bits[word]>>uint(i%64))&1 == 1
}

// MarkNull sets bit i to null, growing the bitmap as needed.
func (v *validityBitmap) MarkNull(i int) {
	word := i / 64
	for word >= len(v.bits) {
		v.bits = append(v.bits, 0)
	}
	v.bits[word] |= 1 << uint(i%64)
}

// Reset clears every null mark but keeps the underlying slice for reuse.
func (v *validityBitmap) Reset() {
	for i := range v.bits {
		v.bits[i] = 0
	}
}
