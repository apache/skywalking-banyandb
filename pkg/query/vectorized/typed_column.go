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

import (
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/decimal"
)

// TypedColumn is a generic Column with element type T.
// One instance per supported T — use the typed constructors below.
//
// A column normally owns its backing slice. Borrow lends it a slice owned by
// someone else (a storage cursor's decoded values); such a column is read-only
// as far as the backing array goes: every mutator that would write element
// memory first replaces the slice with a private copy, and Reset and the pool
// release drop the slice instead of truncating it, so a pooled column can never
// write into memory it only borrowed.
type TypedColumn[T any] struct {
	zero T
	data []T
	// borrow is nil until the column first borrows; it keeps the column's own
	// array while data is a view of someone else's.
	borrow    *borrowState[T]
	validity  validityBitmap
	typ       ColumnType
	poolClass int
}

// borrowState is the part of a TypedColumn that exists only if it ever borrows,
// so a column that never does costs one pointer.
type borrowState[T any] struct {
	// spare is the column's own array, parked while data is borrowed so that a
	// pooled column keeps its capacity across a borrow.
	spare  []T
	active bool
}

// Type returns the static ColumnType this column was built with.
func (c *TypedColumn[T]) Type() ColumnType { return c.typ }

// Len returns the current row count.
func (c *TypedColumn[T]) Len() int { return len(c.data) }

// IsNull reports whether row i is null.
func (c *TypedColumn[T]) IsNull(i int) bool { return c.validity.IsNull(i) }

// MayHaveNulls reports whether any row may be null. False is a guarantee, so a
// range loop over Data can skip the per-row validity check; true only means the
// check is needed.
func (c *TypedColumn[T]) MayHaveNulls() bool { return c.validity.marked }

// Data returns the backing slice for bulk access. The caller must not write
// through it when Borrowed reports true.
func (c *TypedColumn[T]) Data() []T { return c.data }

// Borrow makes the column a zero-copy view of src with every row valid. src
// must stay valid and unmodified until the column is Reset or released; the
// caller is responsible for pinning whatever owns it. The column's previous
// contents are discarded.
//
// The slice is capacity-limited, so even a stray append reallocates rather than
// writing past src.
func (c *TypedColumn[T]) Borrow(src []T) {
	if c.borrow == nil {
		c.borrow = &borrowState[T]{}
	}
	if !c.borrow.active {
		c.borrow.spare = c.data[:0]
		c.borrow.active = true
	}
	c.data = src[:len(src):len(src)]
	c.validity.Reset()
}

// Borrowed reports whether the column's backing slice belongs to someone else.
func (c *TypedColumn[T]) Borrowed() bool { return c.borrow != nil && c.borrow.active }

// own replaces a borrowed slice with a private copy so the column can be
// mutated. The copy has room to grow.
func (c *TypedColumn[T]) own() {
	if !c.Borrowed() {
		return
	}
	c.data = append(c.borrow.spare[:0], c.data...)
	c.borrow.spare = nil
	c.borrow.active = false
}

// Append adds a value, marking it valid.
func (c *TypedColumn[T]) Append(v T) {
	c.own()
	c.data = append(c.data, v)
}

// AppendSlice adds every value of vs, marking them valid.
func (c *TypedColumn[T]) AppendSlice(vs []T) {
	c.own()
	c.data = append(c.data, vs...)
}

// AppendNull adds a zero-value placeholder and marks it null.
func (c *TypedColumn[T]) AppendNull() {
	c.own()
	idx := len(c.data)
	c.data = append(c.data, c.zero)
	c.validity.MarkNull(idx)
}

// MarkNullAt marks an existing row at index i as null. Length is unchanged.
func (c *TypedColumn[T]) MarkNullAt(i int) {
	c.validity.MarkNull(i)
}

// SetAt overwrites the value at index i without changing length or validity.
// The validity bit at i is cleared (row becomes valid). Panics if i is out
// of range — matches the same contract as a direct slice write.
func (c *TypedColumn[T]) SetAt(i int, v T) {
	c.own()
	c.data[i] = v
	if c.validity.IsNull(i) {
		c.validity.ClearNull(i)
	}
}

// Reset clears length and validity. Capacity is retained, except that a
// borrowed slice is dropped and the column returns to its own array: the borrowed
// one is never the column's to truncate and refill.
func (c *TypedColumn[T]) Reset() {
	if c.Borrowed() {
		c.data = c.borrow.spare[:0]
		c.borrow.spare = nil
		c.borrow.active = false
	} else {
		c.data = c.data[:0]
	}
	c.validity.Reset()
}

// NewInt64Column constructs a TypedColumn[int64] with the given capacity.
func NewInt64Column(capacity int) *TypedColumn[int64] {
	return &TypedColumn[int64]{typ: ColumnTypeInt64, data: make([]int64, 0, capacity)}
}

// NewFloat64Column constructs a TypedColumn[float64] with the given capacity.
func NewFloat64Column(capacity int) *TypedColumn[float64] {
	return &TypedColumn[float64]{typ: ColumnTypeFloat64, data: make([]float64, 0, capacity)}
}

// NewStringColumn constructs a TypedColumn[string] with the given capacity.
func NewStringColumn(capacity int) *TypedColumn[string] {
	return &TypedColumn[string]{typ: ColumnTypeString, data: make([]string, 0, capacity)}
}

// NewBytesColumn constructs a TypedColumn[[]byte] with the given capacity.
func NewBytesColumn(capacity int) *TypedColumn[[]byte] {
	return &TypedColumn[[]byte]{typ: ColumnTypeBytes, data: make([][]byte, 0, capacity)}
}

// NewInt64ArrayColumn constructs a TypedColumn[[]int64] with the given capacity.
func NewInt64ArrayColumn(capacity int) *TypedColumn[[]int64] {
	return &TypedColumn[[]int64]{typ: ColumnTypeInt64Array, data: make([][]int64, 0, capacity)}
}

// NewStrArrayColumn constructs a TypedColumn[[]string] with the given capacity.
func NewStrArrayColumn(capacity int) *TypedColumn[[]string] {
	return &TypedColumn[[]string]{typ: ColumnTypeStrArray, data: make([][]string, 0, capacity)}
}

// NewTagValueColumn constructs a TypedColumn[*modelv1.TagValue] passthrough
// column. Cells hold the original *modelv1.TagValue pointers from the scan
// source; the egress serializer returns those pointers directly, avoiding
// the decode-into-typed / re-encode-into-protobuf round trip that otherwise
// dominates allocation cost when no operator consumes the column.
func NewTagValueColumn(capacity int) *TypedColumn[*modelv1.TagValue] {
	return &TypedColumn[*modelv1.TagValue]{typ: ColumnTypeTagValue, data: make([]*modelv1.TagValue, 0, capacity)}
}

// NewFieldValueColumn is the field-side counterpart of NewTagValueColumn.
func NewFieldValueColumn(capacity int) *TypedColumn[*modelv1.FieldValue] {
	return &TypedColumn[*modelv1.FieldValue]{typ: ColumnTypeFieldValue, data: make([]*modelv1.FieldValue, 0, capacity)}
}

// NewColumnForType constructs a fresh Column of the given type with the
// requested backing capacity. Callers that want pool recycling should use
// AcquireColumn / ReleaseColumn directly — those are wired into the
// MeasureBatch pool's Acquire path. NewColumnForType deliberately stays
// non-pooled so callers (operator-internal slot buffers, the egress
// coalesce buffer, etc.) that never Release their columns do not skew the
// pool refcount used by the test-teardown HaveZeroRef invariant.
//
// Panics on unknown type — programmer error, not data error.
func NewColumnForType(t ColumnType, capacity int) Column {
	switch t {
	case ColumnTypeDecimal:
		return &TypedColumn[decimal.Int128]{typ: t, data: make([]decimal.Int128, 0, capacity)}
	case ColumnTypeInt64:
		return NewInt64Column(capacity)
	case ColumnTypeFloat64:
		return NewFloat64Column(capacity)
	case ColumnTypeString:
		return NewStringColumn(capacity)
	case ColumnTypeBytes:
		return NewBytesColumn(capacity)
	case ColumnTypeInt64Array:
		return NewInt64ArrayColumn(capacity)
	case ColumnTypeStrArray:
		return NewStrArrayColumn(capacity)
	case ColumnTypeTagValue:
		return NewTagValueColumn(capacity)
	case ColumnTypeFieldValue:
		return NewFieldValueColumn(capacity)
	}
	panic("vectorized: unknown ColumnType " + t.String())
}

// NewColumnFromSlice returns a column of type t that owns data, with no nulls. The
// column keeps data as its backing array, so the caller must not use data afterwards.
func NewColumnFromSlice[T any](t ColumnType, data []T) *TypedColumn[T] {
	return &TypedColumn[T]{typ: t, data: data}
}

// SetNullsFromValidity replaces the column's null set with an Arrow validity bitmap
// over the column's rows: bit i of valid (least significant bit first) set means row
// i holds a value, clear means it is null. valid must hold at least (Len()+7)/8 bytes
// and is not retained.
func (c *TypedColumn[T]) SetNullsFromValidity(valid []byte) {
	c.validity.setFromValidity(valid, len(c.data))
}
