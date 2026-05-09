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
)

// TypedColumn is a generic Column with element type T.
// One instance per supported T — use the typed constructors below.
type TypedColumn[T any] struct {
	zero     T
	data     []T
	validity validityBitmap
	typ      ColumnType
}

// Type returns the static ColumnType this column was built with.
func (c *TypedColumn[T]) Type() ColumnType { return c.typ }

// Len returns the current row count.
func (c *TypedColumn[T]) Len() int { return len(c.data) }

// IsNull reports whether row i is null.
func (c *TypedColumn[T]) IsNull(i int) bool { return c.validity.IsNull(i) }

// Data returns the backing slice for bulk access.
func (c *TypedColumn[T]) Data() []T { return c.data }

// Append adds a value, marking it valid.
func (c *TypedColumn[T]) Append(v T) { c.data = append(c.data, v) }

// AppendNull adds a zero-value placeholder and marks it null.
func (c *TypedColumn[T]) AppendNull() {
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
	c.data[i] = v
	if c.validity.IsNull(i) {
		c.validity.ClearNull(i)
	}
}

// Reset clears length and validity. Capacity is retained.
func (c *TypedColumn[T]) Reset() {
	c.data = c.data[:0]
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

// NewColumnForType constructs a Column with the given type and capacity.
// Panics on unknown type — programmer error, not data error.
func NewColumnForType(t ColumnType, capacity int) Column {
	switch t {
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
