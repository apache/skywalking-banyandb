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
	"testing"

	"github.com/stretchr/testify/require"
)

type lentCountOwner struct{ refs int }

func (o *lentCountOwner) Retain()  { o.refs++ }
func (o *lentCountOwner) Release() { o.refs-- }

func lentSchema() *BatchSchema {
	return NewBatchSchema([]ColumnDef{
		{Role: RoleTimestamp, Type: ColumnTypeInt64},
		{Role: RoleField, Name: "v", Type: ColumnTypeInt64},
	})
}

func TestLentBatchIsNotPooled(t *testing.T) {
	schema := lentSchema()
	pool := NewBatchPool(schema, 8)
	owner := &lentCountOwner{refs: 1}
	col := NewInt64Column(0)
	col.Borrow([]int64{1, 2, 3})
	lent := &RecordBatch{Schema: schema, Columns: []Column{col, NewInt64Column(0)}, Len: 3, Lent: true}
	lent.Owners.Pin(owner)
	require.Equal(t, 2, owner.refs)

	pool.Put(lent)
	require.Equal(t, 1, owner.refs, "Put releases the owners of a lent batch")
	require.Nil(t, lent.Columns, "a lent batch gives up its columns with its owners")
	// Nothing lent comes back out of the pool.
	for range 4 {
		got := pool.Get()
		require.False(t, got.Lent)
		require.NotSame(t, lent, got)
		pool.Put(got)
	}
}

func TestLentBatchResetDoesNotClearColumnsItNoLongerOwns(t *testing.T) {
	schema := lentSchema()
	owner := &lentCountOwner{refs: 1}
	data := []int64{5, 6, 7}
	col := NewInt64Column(0)
	col.Borrow(data)
	lent := &RecordBatch{Schema: schema, Columns: []Column{col, col}, Len: 3, Selection: []uint16{2, 1, 0}, Lent: true}
	lent.Owners.Pin(owner)
	lent.Reset()
	require.Equal(t, 1, owner.refs)
	require.Equal(t, 0, lent.Len)
	require.Nil(t, lent.Selection)
	require.Nil(t, lent.Columns)
	require.Equal(t, []int64{5, 6, 7}, data)
	// Idempotent.
	lent.Reset()
	require.Equal(t, 1, owner.refs)
}

func TestSelectionOrderIsPreservedByActiveLen(t *testing.T) {
	b := &RecordBatch{Len: 5, Selection: []uint16{4, 3, 2, 1, 0}}
	require.Equal(t, 5, b.ActiveLen())
}
