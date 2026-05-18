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
	"github.com/apache/skywalking-banyandb/pkg/pool"
)

// One pool per supported column element type. Pools hold *TypedColumn[T]
// instances that have been Reset; on Get the caller may need to grow the
// backing slice if the prior capacity is smaller than the new request.
var (
	int64ColumnPool      = pool.Register[*TypedColumn[int64]]("vectorized.column.int64")
	float64ColumnPool    = pool.Register[*TypedColumn[float64]]("vectorized.column.float64")
	stringColumnPool     = pool.Register[*TypedColumn[string]]("vectorized.column.string")
	bytesColumnPool      = pool.Register[*TypedColumn[[]byte]]("vectorized.column.bytes")
	int64ArrayColumnPool = pool.Register[*TypedColumn[[]int64]]("vectorized.column.int64array")
	strArrayColumnPool   = pool.Register[*TypedColumn[[]string]]("vectorized.column.strarray")
	tagValueColumnPool   = pool.Register[*TypedColumn[*modelv1.TagValue]]("vectorized.column.tagvalue")
	fieldValueColumnPool = pool.Register[*TypedColumn[*modelv1.FieldValue]]("vectorized.column.fieldvalue")
)

// AcquireColumn returns a recycled Column of the requested type. The
// returned column is Reset (length 0, validity cleared) and has a backing
// capacity of at least the requested size. Callers must pair every
// AcquireColumn with a matching ReleaseColumn once they are done with the
// column.
//
// Capacity behaviour: pooled columns retain whichever capacity they had at
// the last Release. If a pooled column is too small for the new request the
// backing slice is reallocated; otherwise the existing backing slice is
// reused. Over time the pool converges to the largest capacity seen.
func AcquireColumn(t ColumnType, capacity int) Column {
	switch t {
	case ColumnTypeInt64:
		c := int64ColumnPool.Get()
		if c == nil {
			return NewInt64Column(capacity)
		}
		if cap(c.data) < capacity {
			c.data = make([]int64, 0, capacity)
		}
		return c
	case ColumnTypeFloat64:
		c := float64ColumnPool.Get()
		if c == nil {
			return NewFloat64Column(capacity)
		}
		if cap(c.data) < capacity {
			c.data = make([]float64, 0, capacity)
		}
		return c
	case ColumnTypeString:
		c := stringColumnPool.Get()
		if c == nil {
			return NewStringColumn(capacity)
		}
		if cap(c.data) < capacity {
			c.data = make([]string, 0, capacity)
		}
		return c
	case ColumnTypeBytes:
		c := bytesColumnPool.Get()
		if c == nil {
			return NewBytesColumn(capacity)
		}
		if cap(c.data) < capacity {
			c.data = make([][]byte, 0, capacity)
		}
		return c
	case ColumnTypeInt64Array:
		c := int64ArrayColumnPool.Get()
		if c == nil {
			return NewInt64ArrayColumn(capacity)
		}
		if cap(c.data) < capacity {
			c.data = make([][]int64, 0, capacity)
		}
		return c
	case ColumnTypeStrArray:
		c := strArrayColumnPool.Get()
		if c == nil {
			return NewStrArrayColumn(capacity)
		}
		if cap(c.data) < capacity {
			c.data = make([][]string, 0, capacity)
		}
		return c
	case ColumnTypeTagValue:
		c := tagValueColumnPool.Get()
		if c == nil {
			return NewTagValueColumn(capacity)
		}
		if cap(c.data) < capacity {
			c.data = make([]*modelv1.TagValue, 0, capacity)
		}
		return c
	case ColumnTypeFieldValue:
		c := fieldValueColumnPool.Get()
		if c == nil {
			return NewFieldValueColumn(capacity)
		}
		if cap(c.data) < capacity {
			c.data = make([]*modelv1.FieldValue, 0, capacity)
		}
		return c
	}
	panic("vectorized: unknown ColumnType " + t.String())
}

// ReleaseColumn returns a column to its per-type pool. The column is Reset
// (length cleared, validity cleared) before being put back. A nil column is
// a no-op so callers can release defensively. Unknown column types are also
// no-ops — the column is simply dropped on the floor for the GC.
func ReleaseColumn(c Column) {
	if c == nil {
		return
	}
	switch col := c.(type) {
	case *TypedColumn[int64]:
		col.Reset()
		int64ColumnPool.Put(col)
	case *TypedColumn[float64]:
		col.Reset()
		float64ColumnPool.Put(col)
	case *TypedColumn[string]:
		col.Reset()
		stringColumnPool.Put(col)
	case *TypedColumn[[]byte]:
		col.Reset()
		bytesColumnPool.Put(col)
	case *TypedColumn[[]int64]:
		col.Reset()
		int64ArrayColumnPool.Put(col)
	case *TypedColumn[[]string]:
		col.Reset()
		strArrayColumnPool.Put(col)
	case *TypedColumn[*modelv1.TagValue]:
		col.Reset()
		tagValueColumnPool.Put(col)
	case *TypedColumn[*modelv1.FieldValue]:
		col.Reset()
		fieldValueColumnPool.Put(col)
	}
}
