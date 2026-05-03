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

func TestValidityBitmap_Default_AllValid(t *testing.T) {
	var v validityBitmap
	for i := range 64 {
		if v.IsNull(i) {
			t.Fatalf("default bitmap: bit %d unexpectedly null", i)
		}
	}
}

func TestValidityBitmap_MarkNull_SetsExpectedBit(t *testing.T) {
	var v validityBitmap
	v.MarkNull(3)
	if !v.IsNull(3) {
		t.Fatal("bit 3 should be null after MarkNull(3)")
	}
	if v.IsNull(2) || v.IsNull(4) {
		t.Fatalf("only bit 3 should be null; got 2=%v 4=%v", v.IsNull(2), v.IsNull(4))
	}
}

func TestValidityBitmap_GrowthBeyondInitialWord(t *testing.T) {
	var v validityBitmap
	v.MarkNull(150)
	if !v.IsNull(150) {
		t.Fatal("bit 150 should be null after growth")
	}
	if v.IsNull(149) || v.IsNull(151) {
		t.Fatal("only bit 150 should be null after growth")
	}
}

func TestValidityBitmap_Reset_ClearsAllBits_RetainsSlice(t *testing.T) {
	var v validityBitmap
	v.MarkNull(10)
	v.MarkNull(200)
	beforeCap := cap(v.bits)
	v.Reset()
	if v.IsNull(10) || v.IsNull(200) {
		t.Fatal("Reset should clear all nulls")
	}
	if cap(v.bits) != beforeCap {
		t.Fatalf("Reset must retain slice capacity: before=%d after=%d", beforeCap, cap(v.bits))
	}
}

func TestColumnType_String_NotEmpty(t *testing.T) {
	types := []ColumnType{
		ColumnTypeInt64,
		ColumnTypeFloat64,
		ColumnTypeString,
		ColumnTypeBytes,
		ColumnTypeInt64Array,
		ColumnTypeStrArray,
	}
	for _, ct := range types {
		if ct.String() == "" {
			t.Fatalf("ColumnType(%d).String() must not be empty", int(ct))
		}
	}
}
