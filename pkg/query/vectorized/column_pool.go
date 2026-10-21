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
	"strconv"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/pool"
)

// One pool per supported column element type. Pools hold *TypedColumn[T]
// instances that have been Reset; on Get the caller may need to grow the
// backing slice if the prior capacity is smaller than the new request.
var (
	int64ColumnPool      = newTypedColumnPool[int64]("vectorized.column.int64")
	float64ColumnPool    = newTypedColumnPool[float64]("vectorized.column.float64")
	stringColumnPool     = newTypedColumnPool[string]("vectorized.column.string")
	bytesColumnPool      = newTypedColumnPool[[]byte]("vectorized.column.bytes")
	int64ArrayColumnPool = newTypedColumnPool[[]int64]("vectorized.column.int64array")
	strArrayColumnPool   = newTypedColumnPool[[]string]("vectorized.column.strarray")
	tagValueColumnPool   = newTypedColumnPool[*modelv1.TagValue]("vectorized.column.tagvalue")
	fieldValueColumnPool = newTypedColumnPool[*modelv1.FieldValue]("vectorized.column.fieldvalue")
)

// Separate capacity classes keep bounded small acquisitions from destroying
// reusable native block slabs. The acquisition class also identifies the pool
// checkout; a column may grow before it is released.
type typedColumnPool[T any] struct {
	classes [7]*pool.Synced[*TypedColumn[T]]
}

func newTypedColumnPool[T any](name string) *typedColumnPool[T] {
	p := &typedColumnPool[T]{}
	for i := range p.classes {
		p.classes[i] = pool.Register[*TypedColumn[T]](name + "." + strconv.Itoa(i))
	}
	return p
}

func (p *typedColumnPool[T]) Get(capacity int) *TypedColumn[T] {
	class := ColumnCapacityClass(capacity)
	c := p.classes[class].Get()
	if c == nil {
		c = &TypedColumn[T]{}
	}
	c.poolClass = class
	return c
}

func (p *typedColumnPool[T]) Put(c *TypedColumn[T]) {
	p.classes[c.poolClass].Put(c)
}

// ColumnCapacityClass groups capacities by powers of two above the canonical
// batch size, so normal small batches never evict cached native block slabs.
func ColumnCapacityClass(capacity int) int {
	class := 0
	for bound := DefaultBatchSize; capacity > bound && class < 6; bound *= 2 {
		class++
	}
	return class
}

// AcquireColumn returns a recycled Column of the requested type. The
// returned column is Reset (length 0, validity cleared) and has a backing
// capacity of at least the requested size. Callers must pair every
// AcquireColumn with a matching ReleaseColumn once they are done with the
// column.
//
// Capacity behavior: pooled columns retain whichever capacity they had at
// the last Release. If a pooled column is too small for the new request the
// backing slice is reallocated; otherwise the existing backing slice is
// reused. Capacity classes isolate native block slabs from ordinary operator batches.
func AcquireColumn(t ColumnType, capacity int) Column {
	switch t {
	case ColumnTypeInt64:
		c := int64ColumnPool.Get(capacity)
		c.typ = t
		if cap(c.data) < capacity {
			c.data = make([]int64, 0, capacity)
		}
		return c
	case ColumnTypeFloat64:
		c := float64ColumnPool.Get(capacity)
		c.typ = t
		if cap(c.data) < capacity {
			c.data = make([]float64, 0, capacity)
		}
		return c
	case ColumnTypeString:
		c := stringColumnPool.Get(capacity)
		c.typ = t
		if cap(c.data) < capacity {
			c.data = make([]string, 0, capacity)
		}
		return c
	case ColumnTypeBytes:
		c := bytesColumnPool.Get(capacity)
		c.typ = t
		if cap(c.data) < capacity {
			c.data = make([][]byte, 0, capacity)
		}
		return c
	case ColumnTypeInt64Array:
		c := int64ArrayColumnPool.Get(capacity)
		c.typ = t
		if cap(c.data) < capacity {
			c.data = make([][]int64, 0, capacity)
		}
		return c
	case ColumnTypeStrArray:
		c := strArrayColumnPool.Get(capacity)
		c.typ = t
		if cap(c.data) < capacity {
			c.data = make([][]string, 0, capacity)
		}
		return c
	case ColumnTypeTagValue:
		c := tagValueColumnPool.Get(capacity)
		c.typ = t
		if cap(c.data) < capacity {
			c.data = make([]*modelv1.TagValue, 0, capacity)
		}
		return c
	case ColumnTypeFieldValue:
		c := fieldValueColumnPool.Get(capacity)
		c.typ = t
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
	case *ConstColumn:
		col.Reset()
		if !col.embedded {
			constColumnPool.Put(col)
		}
	}
}

// AcquireBoundedColumn acquires a reset column and drops oversized cached backing
// buffers before the column is attached to a query batch. Normal batch-size reuse
// is preserved; retained capacity is at most twice max(capacity, DefaultBatchSize).
func AcquireBoundedColumn(t ColumnType, capacity int) Column {
	column := AcquireColumn(t, capacity)
	limit := 2 * max(capacity, DefaultBatchSize)
	switch col := column.(type) {
	case *TypedColumn[int64]:
		boundColumnBacking(col, capacity, limit)
	case *TypedColumn[float64]:
		boundColumnBacking(col, capacity, limit)
	case *TypedColumn[string]:
		boundColumnBacking(col, capacity, limit)
	case *TypedColumn[[]byte]:
		boundColumnBacking(col, capacity, limit)
	case *TypedColumn[[]int64]:
		boundColumnBacking(col, capacity, limit)
	case *TypedColumn[[]string]:
		boundColumnBacking(col, capacity, limit)
	case *TypedColumn[*modelv1.TagValue]:
		boundColumnBacking(col, capacity, limit)
	case *TypedColumn[*modelv1.FieldValue]:
		boundColumnBacking(col, capacity, limit)
	}
	return column
}

func boundColumnBacking[T any](column *TypedColumn[T], capacity, limit int) {
	if cap(column.data) > limit {
		column.data = make([]T, 0, capacity)
	}
	if cap(column.validity.bits) > (limit+63)/64 {
		column.validity.bits = nil
		column.validity.marked = false
	}
}
