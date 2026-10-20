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
	"bytes"
	"fmt"

	"github.com/apache/skywalking-banyandb/pkg/pool"
)

var constColumnPool = pool.Register[*ConstColumn]("vectorized.column.const")

// ConstColumn is a Column whose every row holds the same value: one typed scalar
// (int64, float64, string or bytes) or a typed null, plus a row count. A storage
// batch lent from a single series uses it for the series' entity tags, which are
// the same in every row of the batch, so the value is stored once instead of per
// row, grouping by it costs one key per batch, and a predicate on it keeps or
// drops the whole batch.
//
// A ConstColumn reports the logical ColumnType of the typed column it stands for
// (ColumnTypeInt64, ColumnTypeFloat64, ColumnTypeString or ColumnTypeBytes), so a
// BatchSchema and the planners built on it do not change: only a consumer that
// reaches into the cells has to know the column may be constant. It tells them
// apart with AsConst; a consumer that does not specialize calls Expand (or
// Materialize) for an ordinary TypedColumn.
//
// A string or bytes value may borrow memory a storage cursor owns. The column
// follows the rules of any other borrowing column: the batch that carries it pins
// the owner through RecordBatch.Owners, and a consumer that keeps the value past
// the batch's life clones it first.
//
// A ConstColumn is immutable once built: it is read-only and cannot be appended to.
type ConstColumn struct {
	str    string
	bytes  []byte
	i64    int64
	f64    float64
	length int
	typ    ColumnType
	null   bool
	// embedded marks a column whose storage belongs to another object (a MeasureBatch
	// holds a few), so ReleaseColumn resets it instead of pooling it.
	embedded bool
}

// NewConstInt64 returns a column of n rows that all hold v.
func NewConstInt64(v int64, n int) *ConstColumn {
	return &ConstColumn{typ: ColumnTypeInt64, i64: v, length: n}
}

// NewConstFloat64 returns a column of n rows that all hold v.
func NewConstFloat64(v float64, n int) *ConstColumn {
	return &ConstColumn{typ: ColumnTypeFloat64, f64: v, length: n}
}

// NewConstString returns a column of n rows that all hold v. The column does not
// copy v.
func NewConstString(v string, n int) *ConstColumn {
	return &ConstColumn{typ: ColumnTypeString, str: v, length: n}
}

// NewConstBytes returns a column of n rows that all hold v. The column does not
// copy v.
func NewConstBytes(v []byte, n int) *ConstColumn {
	return &ConstColumn{typ: ColumnTypeBytes, bytes: v, length: n}
}

// NewConstNull returns a column of n null rows of type t. t must be one a
// ConstColumn can stand for (see ConstSupported).
func NewConstNull(t ColumnType, n int) *ConstColumn {
	return &ConstColumn{typ: t, null: true, length: n}
}

// ConstSupported reports whether a ConstColumn can stand for a column of type t.
func ConstSupported(t ColumnType) bool {
	switch t {
	case ColumnTypeInt64, ColumnTypeFloat64, ColumnTypeString, ColumnTypeBytes:
		return true
	default:
		return false
	}
}

// AcquireConstFrom returns a pooled ConstColumn of n rows holding the value of row
// of src, or a null of src's type when that row is null. It reports false, having
// built nothing, when src is not a typed column of a ConstSupported type or row is
// out of range. String and bytes values are not copied. Release the column with
// ReleaseColumn.
func AcquireConstFrom(src Column, row, n int) (*ConstColumn, bool) {
	c := constColumnPool.Get()
	if c == nil {
		c = &ConstColumn{}
	}
	if !c.SetFrom(src, row, n) {
		constColumnPool.Put(c)
		return nil, false
	}
	return c, true
}

// FillEmbeddedConstFrom is AcquireConstFrom for a column whose storage the caller
// provides: c becomes a constant of n rows holding the value of row of src, and
// ReleaseColumn only resets it, never pools it. It reports false, having changed
// nothing, when src cannot be constant. c must stay put until it is released.
func FillEmbeddedConstFrom(c *ConstColumn, src Column, row, n int) bool {
	if !c.SetFrom(src, row, n) {
		return false
	}
	c.embedded = true
	return true
}

// SetFrom makes c a constant of n rows holding the value of row of src (a null of
// src's type when that row is null). It reports false, having changed nothing, when
// src is not a typed column of a ConstSupported type or row is out of range.
func (c *ConstColumn) SetFrom(src Column, row, n int) bool {
	if row < 0 || row >= src.Len() {
		return false
	}
	var (
		i64 int64
		f64 float64
		str string
		raw []byte
	)
	switch s := src.(type) {
	case *TypedColumn[int64]:
		i64 = s.Data()[row]
	case *TypedColumn[float64]:
		f64 = s.Data()[row]
	case *TypedColumn[string]:
		str = s.Data()[row]
	case *TypedColumn[[]byte]:
		raw = s.Data()[row]
	default:
		return false
	}
	c.length = n
	c.typ = src.Type()
	c.null = src.IsNull(row)
	if c.null {
		// A null cell carries no value, whatever placeholder the source holds.
		i64, f64, str, raw = 0, 0, "", nil
	}
	c.i64, c.f64, c.str, c.bytes = i64, f64, str, raw
	return true
}

// AsConst returns col as a ConstColumn when it is one.
func AsConst(col Column) (*ConstColumn, bool) {
	c, ok := col.(*ConstColumn)
	return c, ok
}

// Type returns the logical type of the column.
func (c *ConstColumn) Type() ColumnType { return c.typ }

// Len returns the number of rows.
func (c *ConstColumn) Len() int { return c.length }

// IsNull reports whether row i is null: every row of a null column is, none of any
// other.
func (c *ConstColumn) IsNull(i int) bool { return c.null && i >= 0 && i < c.length }

// IsNullValue reports whether the constant is null.
func (c *ConstColumn) IsNullValue() bool { return c.null }

// Int64 returns the value of an int64 column; zero for a null one.
func (c *ConstColumn) Int64() int64 { return c.i64 }

// Float64 returns the value of a float64 column; zero for a null one.
func (c *ConstColumn) Float64() float64 { return c.f64 }

// Str returns the value of a string column; empty for a null one.
func (c *ConstColumn) Str() string { return c.str }

// Bytes returns the value of a bytes column; nil for a null one. The caller must
// not modify it.
func (c *ConstColumn) Bytes() []byte { return c.bytes }

// Reset drops the value and the row count, so a pooled column keeps no reference to
// borrowed memory.
func (c *ConstColumn) Reset() {
	c.str, c.bytes, c.i64, c.f64 = "", nil, 0, 0
	c.length, c.null = 0, false
}

// AppendNull panics: a ConstColumn is immutable.
func (c *ConstColumn) AppendNull() { panic("vectorized: ConstColumn is immutable") }

// MarkNullAt panics: a ConstColumn is immutable.
func (c *ConstColumn) MarkNullAt(int) { panic("vectorized: ConstColumn is immutable") }

// Equal reports whether o holds the same value and type as c. Row counts are not
// compared.
func (c *ConstColumn) Equal(o *ConstColumn) bool {
	if c.typ != o.typ || c.null != o.null {
		return false
	}
	if c.null {
		return true
	}
	switch c.typ {
	case ColumnTypeInt64:
		return c.i64 == o.i64
	case ColumnTypeFloat64:
		return c.f64 == o.f64 || (c.f64 != c.f64 && o.f64 != o.f64)
	case ColumnTypeString:
		return c.str == o.str
	case ColumnTypeBytes:
		return bytes.Equal(c.bytes, o.bytes)
	default:
		return false
	}
}

// AppendTo appends n copies of the value to dst, a TypedColumn of the same type,
// marking them null when the constant is. Strings and bytes are shared, not copied.
func (c *ConstColumn) AppendTo(dst Column, n int) error {
	switch d := dst.(type) {
	case *TypedColumn[int64]:
		if c.typ != ColumnTypeInt64 {
			return fmt.Errorf("ConstColumn.AppendTo: dst int64 vs src %s", c.typ)
		}
		for range n {
			if c.null {
				d.AppendNull()
			} else {
				d.Append(c.i64)
			}
		}
	case *TypedColumn[float64]:
		if c.typ != ColumnTypeFloat64 {
			return fmt.Errorf("ConstColumn.AppendTo: dst float64 vs src %s", c.typ)
		}
		for range n {
			if c.null {
				d.AppendNull()
			} else {
				d.Append(c.f64)
			}
		}
	case *TypedColumn[string]:
		if c.typ != ColumnTypeString {
			return fmt.Errorf("ConstColumn.AppendTo: dst string vs src %s", c.typ)
		}
		for range n {
			if c.null {
				d.AppendNull()
			} else {
				d.Append(c.str)
			}
		}
	case *TypedColumn[[]byte]:
		if c.typ != ColumnTypeBytes {
			return fmt.Errorf("ConstColumn.AppendTo: dst bytes vs src %s", c.typ)
		}
		for range n {
			if c.null {
				d.AppendNull()
			} else {
				d.Append(c.bytes)
			}
		}
	default:
		return fmt.Errorf("ConstColumn.AppendTo: unsupported dst %s for src %s", dst.Type(), c.typ)
	}
	return nil
}

// Expand returns an ordinary TypedColumn holding the value in each of the
// column's rows. It is the shared materialization for a consumer that does not
// specialize on constants. Strings and bytes are shared with c, so the expanded
// column is valid for as long as c is.
func (c *ConstColumn) Expand() Column {
	out := NewColumnForType(c.typ, c.length)
	// AppendTo cannot fail: out was built from c's own type.
	_ = c.AppendTo(out, c.length)
	return out
}

// Materialize returns col itself, or an expanded copy when it is a ConstColumn.
func Materialize(col Column) Column {
	if c, ok := col.(*ConstColumn); ok {
		return c.Expand()
	}
	return col
}
