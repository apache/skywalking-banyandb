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

func TestRecordBatch_New_AllocatesColumnPerSchemaEntry(t *testing.T) {
	s := NewBatchSchema([]ColumnDef{
		{Role: RoleTimestamp, Type: ColumnTypeInt64},
		{Role: RoleField, Name: "v", Type: ColumnTypeFloat64},
		{Role: RoleTag, TagFamily: "default", Name: "service", Type: ColumnTypeString},
	})
	b := NewRecordBatch(s, 8)
	if len(b.Columns) != 3 {
		t.Fatalf("Columns: want 3, got %d", len(b.Columns))
	}
	if b.Columns[0].Type() != ColumnTypeInt64 {
		t.Fatalf("col 0 type: want int64, got %v", b.Columns[0].Type())
	}
	if b.Columns[1].Type() != ColumnTypeFloat64 {
		t.Fatalf("col 1 type: want float64, got %v", b.Columns[1].Type())
	}
	if b.Columns[2].Type() != ColumnTypeString {
		t.Fatalf("col 2 type: want string, got %v", b.Columns[2].Type())
	}
}

func TestRecordBatch_Reset_ClearsLenSelectionAndColumns(t *testing.T) {
	s := NewBatchSchema([]ColumnDef{{Role: RoleTimestamp, Type: ColumnTypeInt64}})
	b := NewRecordBatch(s, 8)
	col := b.Columns[0].(*TypedColumn[int64])
	col.Append(100)
	col.Append(200)
	b.Len = 2
	b.Selection = []uint16{0, 1}
	b.Reset()
	if b.Len != 0 {
		t.Fatalf("Len: want 0, got %d", b.Len)
	}
	if b.Selection != nil {
		t.Fatalf("Selection: want nil, got %v", b.Selection)
	}
	if b.Columns[0].Len() != 0 {
		t.Fatalf("column not reset: Len=%d", b.Columns[0].Len())
	}
}

func TestRecordBatch_Reset_PreservesSchemaPointer(t *testing.T) {
	s := NewBatchSchema([]ColumnDef{{Role: RoleTimestamp, Type: ColumnTypeInt64}})
	b := NewRecordBatch(s, 8)
	before := b.Schema
	b.Reset()
	if b.Schema != before {
		t.Fatal("Reset must preserve Schema pointer")
	}
}

func TestRecordBatch_ActiveLen_NilSelection_ReturnsLen(t *testing.T) {
	s := NewBatchSchema([]ColumnDef{{Role: RoleTimestamp, Type: ColumnTypeInt64}})
	b := NewRecordBatch(s, 8)
	b.Len = 5
	b.Selection = nil
	if got := b.ActiveLen(); got != 5 {
		t.Fatalf("ActiveLen with nil Selection: want 5, got %d", got)
	}
}

func TestRecordBatch_ActiveLen_NonNilSelection_ReturnsSelectionLen(t *testing.T) {
	s := NewBatchSchema([]ColumnDef{{Role: RoleTimestamp, Type: ColumnTypeInt64}})
	b := NewRecordBatch(s, 8)
	b.Len = 5
	b.Selection = []uint16{0, 2, 4}
	if got := b.ActiveLen(); got != 3 {
		t.Fatalf("ActiveLen with selection: want 3, got %d", got)
	}
}

func TestRecordBatch_ActiveLen_EmptyNonNilSelection_ReturnsZero(t *testing.T) {
	s := NewBatchSchema([]ColumnDef{{Role: RoleTimestamp, Type: ColumnTypeInt64}})
	b := NewRecordBatch(s, 8)
	b.Len = 5
	b.Selection = []uint16{}
	if got := b.ActiveLen(); got != 0 {
		t.Fatalf("ActiveLen with empty non-nil Selection (post-fusible-filter footgun): want 0, got %d", got)
	}
}
