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
	"testing"
)

func TestTypedColumnInt64_AppendAndData(t *testing.T) {
	c := NewInt64Column(8)
	c.Append(1)
	c.Append(2)
	c.Append(3)
	if c.Len() != 3 {
		t.Fatalf("Len: want 3, got %d", c.Len())
	}
	if c.Type() != ColumnTypeInt64 {
		t.Fatalf("Type: want Int64, got %v", c.Type())
	}
	want := []int64{1, 2, 3}
	for i, v := range want {
		if c.Data()[i] != v {
			t.Fatalf("Data[%d]: want %d, got %d", i, v, c.Data()[i])
		}
	}
}

func TestTypedColumnInt64_AppendNull_GrowsLengthAndMarksNull(t *testing.T) {
	c := NewInt64Column(8)
	c.Append(10)
	c.AppendNull()
	c.Append(20)
	if c.Len() != 3 {
		t.Fatalf("Len: want 3, got %d", c.Len())
	}
	if c.IsNull(0) || !c.IsNull(1) || c.IsNull(2) {
		t.Fatalf("validity: want [valid,null,valid], got [%v,%v,%v]", c.IsNull(0), c.IsNull(1), c.IsNull(2))
	}
}

func TestTypedColumnInt64_MarkNullAt_TogglesPreExistingRow(t *testing.T) {
	c := NewInt64Column(4)
	c.Append(0)
	c.Append(0)
	c.Append(0)
	c.MarkNullAt(1)
	if c.IsNull(0) || !c.IsNull(1) || c.IsNull(2) {
		t.Fatalf("MarkNullAt(1): want [valid,null,valid], got [%v,%v,%v]", c.IsNull(0), c.IsNull(1), c.IsNull(2))
	}
	if c.Len() != 3 {
		t.Fatalf("MarkNullAt must not grow length: got Len=%d", c.Len())
	}
}

func TestTypedColumnInt64_Reset_LengthZero_CapacityRetained(t *testing.T) {
	c := NewInt64Column(64)
	for i := range 64 {
		c.Append(int64(i))
	}
	c.MarkNullAt(10)
	beforeCap := cap(c.Data())
	c.Reset()
	if c.Len() != 0 {
		t.Fatalf("Reset Len: want 0, got %d", c.Len())
	}
	if cap(c.Data()) != beforeCap {
		t.Fatalf("Reset must retain capacity: before=%d after=%d", beforeCap, cap(c.Data()))
	}
	if c.IsNull(10) {
		t.Fatal("Reset must clear validity")
	}
}

func TestTypedColumn_AllSixTypes_RoundTrip(t *testing.T) {
	t.Run("int64", func(t *testing.T) {
		c := NewInt64Column(2)
		c.Append(42)
		if c.Type() != ColumnTypeInt64 || c.Data()[0] != 42 {
			t.Fatalf("int64 roundtrip failed")
		}
	})
	t.Run("float64", func(t *testing.T) {
		c := NewFloat64Column(2)
		c.Append(3.14)
		if c.Type() != ColumnTypeFloat64 || c.Data()[0] != 3.14 {
			t.Fatalf("float64 roundtrip failed")
		}
	})
	t.Run("string", func(t *testing.T) {
		c := NewStringColumn(2)
		c.Append("hello")
		if c.Type() != ColumnTypeString || c.Data()[0] != "hello" {
			t.Fatalf("string roundtrip failed")
		}
	})
	t.Run("bytes", func(t *testing.T) {
		c := NewBytesColumn(2)
		c.Append([]byte("hi"))
		if c.Type() != ColumnTypeBytes || !bytes.Equal(c.Data()[0], []byte("hi")) {
			t.Fatalf("bytes roundtrip failed")
		}
	})
	t.Run("int64-array", func(t *testing.T) {
		c := NewInt64ArrayColumn(2)
		c.Append([]int64{1, 2, 3})
		if c.Type() != ColumnTypeInt64Array || len(c.Data()[0]) != 3 {
			t.Fatalf("int64[] roundtrip failed")
		}
	})
	t.Run("string-array", func(t *testing.T) {
		c := NewStrArrayColumn(2)
		c.Append([]string{"a", "b"})
		if c.Type() != ColumnTypeStrArray || len(c.Data()[0]) != 2 {
			t.Fatalf("string[] roundtrip failed")
		}
	})
}

func TestNewColumnForType_UnknownType_Panics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on unknown ColumnType")
		}
	}()
	_ = NewColumnForType(ColumnType(999), 4)
}
