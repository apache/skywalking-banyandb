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

import "encoding/binary"

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
	ColumnTypeDecimal
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
	case ColumnTypeDecimal:
		return "decimal"
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
	// marked is set by MarkNull and cleared by Reset. It never reads false while a
	// null is set, but may stay true after ClearNull removed the last one, so a false
	// value proves the column has no nulls and a true one proves nothing.
	marked bool
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
	v.marked = true
	word := i / 64
	for word >= len(v.bits) {
		v.bits = append(v.bits, 0)
	}
	v.bits[word] |= 1 << uint(i%64)
}

// ClearNull clears bit i (marks row i as valid). No-op if i is beyond
// the current bitmap length (meaning it was never marked null).
func (v *validityBitmap) ClearNull(i int) {
	word := i / 64
	if word >= len(v.bits) {
		return
	}
	v.bits[word] &^= 1 << uint(i%64)
}

// Reset clears every null mark but keeps the underlying slice for reuse.
func (v *validityBitmap) Reset() {
	if v.marked {
		clear(v.bits)
		v.marked = false
	}
}

// setFromValidity replaces the null set with an Arrow validity bitmap over n rows:
// bit i of valid (least significant bit first) set means row i holds a value, clear
// means it is null. valid must hold at least (n+7)/8 bytes. Bits past n are ignored.
func (v *validityBitmap) setFromValidity(valid []byte, n int) {
	words := (n + 63) / 64
	if cap(v.bits) >= words {
		v.bits = v.bits[:words]
	} else {
		v.bits = make([]uint64, words)
	}
	v.marked = false
	for w := range words {
		var word uint64
		if lo := w * 8; lo+8 <= len(valid) {
			word = binary.LittleEndian.Uint64(valid[lo:])
		} else {
			for k := lo; k < len(valid) && k < lo+8; k++ {
				word |= uint64(valid[k]) << (8 * uint(k-lo))
			}
		}
		nulls := ^word
		if rem := n - w*64; rem < 64 {
			nulls &= (uint64(1) << uint(rem)) - 1
		}
		v.bits[w] = nulls
		if nulls != 0 {
			v.marked = true
		}
	}
}
