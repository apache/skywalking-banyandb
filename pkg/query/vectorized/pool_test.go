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

import "testing"

func TestBatchPool_Get_ReturnsResetBatch(t *testing.T) {
	s := NewBatchSchema([]ColumnDef{{Role: RoleTimestamp, Type: ColumnTypeInt64}})
	p := NewBatchPool(s, DefaultBatchSize)
	dirty := p.Get()
	dirty.Columns[0].(*TypedColumn[int64]).Append(99)
	dirty.Len = 1
	dirty.Selection = []uint16{0}
	p.Put(dirty)
	clean := p.Get()
	if clean.Len != 0 {
		t.Fatalf("Get must return Reset batch: Len=%d", clean.Len)
	}
	if clean.Selection != nil {
		t.Fatalf("Get must return Reset batch: Selection=%v", clean.Selection)
	}
	if clean.Columns[0].Len() != 0 {
		t.Fatalf("Get must return Reset columns: col Len=%d", clean.Columns[0].Len())
	}
}

func TestBatchPool_Put_NilBatch_NoOp(t *testing.T) {
	s := NewBatchSchema([]ColumnDef{{Role: RoleTimestamp, Type: ColumnTypeInt64}})
	p := NewBatchPool(s, 8)
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Put(nil) should not panic; got %v", r)
		}
	}()
	p.Put(nil)
}

func TestBatchPool_Put_ForeignSchema_NoOp(t *testing.T) {
	s1 := NewBatchSchema([]ColumnDef{{Role: RoleTimestamp, Type: ColumnTypeInt64}})
	s2 := NewBatchSchema([]ColumnDef{{Role: RoleTimestamp, Type: ColumnTypeInt64}})
	p1 := NewBatchPool(s1, 8)
	foreign := NewRecordBatch(s2, 8)
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Put(foreign) should not panic; got %v", r)
		}
	}()
	p1.Put(foreign)
	// Confirm pool is still functional after rejecting the foreign batch.
	got := p1.Get()
	if got.Schema != s1 {
		t.Fatal("pool returned a batch with foreign schema after rejection")
	}
}
