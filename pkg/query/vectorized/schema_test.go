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

func TestBatchSchema_LookupHelpers_TimestampVersionSeriesIDShardID(t *testing.T) {
	s := NewBatchSchema([]ColumnDef{
		{Role: RoleTimestamp, Type: ColumnTypeInt64},
		{Role: RoleVersion, Type: ColumnTypeInt64},
		{Role: RoleSeriesID, Type: ColumnTypeInt64},
		{Role: RoleShardID, Type: ColumnTypeInt64},
	})
	if got := s.TimestampIndex(); got != 0 {
		t.Fatalf("TimestampIndex: want 0, got %d", got)
	}
	if got := s.VersionIndex(); got != 1 {
		t.Fatalf("VersionIndex: want 1, got %d", got)
	}
	if got := s.SeriesIDIndex(); got != 2 {
		t.Fatalf("SeriesIDIndex: want 2, got %d", got)
	}
	if got := s.ShardIDIndex(); got != 3 {
		t.Fatalf("ShardIDIndex: want 3, got %d", got)
	}
}

func TestBatchSchema_TagIndex_FoundAndMissing(t *testing.T) {
	s := NewBatchSchema([]ColumnDef{
		{Role: RoleTimestamp, Type: ColumnTypeInt64},
		{Role: RoleTag, TagFamily: "default", Name: "service", Type: ColumnTypeString},
		{Role: RoleTag, TagFamily: "default", Name: "endpoint", Type: ColumnTypeString},
	})
	idx, ok := s.TagIndex("default", "service")
	if !ok || idx != 1 {
		t.Fatalf("TagIndex(default,service): want (1,true), got (%d,%v)", idx, ok)
	}
	idx, ok = s.TagIndex("default", "endpoint")
	if !ok || idx != 2 {
		t.Fatalf("TagIndex(default,endpoint): want (2,true), got (%d,%v)", idx, ok)
	}
	if _, ok := s.TagIndex("default", "missing"); ok {
		t.Fatalf("TagIndex(default,missing): expected miss")
	}
}

func TestBatchSchema_FieldIndex_FoundAndMissing(t *testing.T) {
	s := NewBatchSchema([]ColumnDef{
		{Role: RoleTimestamp, Type: ColumnTypeInt64},
		{Role: RoleField, Name: "value", Type: ColumnTypeFloat64},
		{Role: RoleField, Name: "count", Type: ColumnTypeInt64},
	})
	idx, ok := s.FieldIndex("value")
	if !ok || idx != 1 {
		t.Fatalf("FieldIndex(value): want (1,true), got (%d,%v)", idx, ok)
	}
	idx, ok = s.FieldIndex("count")
	if !ok || idx != 2 {
		t.Fatalf("FieldIndex(count): want (2,true), got (%d,%v)", idx, ok)
	}
	if _, ok := s.FieldIndex("missing"); ok {
		t.Fatalf("FieldIndex(missing): expected miss")
	}
}

func TestBatchSchema_MultipleTagFamilies_NoCollision(t *testing.T) {
	s := NewBatchSchema([]ColumnDef{
		{Role: RoleTag, TagFamily: "a", Name: "id", Type: ColumnTypeString},
		{Role: RoleTag, TagFamily: "b", Name: "id", Type: ColumnTypeString},
	})
	aIdx, aOK := s.TagIndex("a", "id")
	bIdx, bOK := s.TagIndex("b", "id")
	if !aOK || !bOK {
		t.Fatalf("both lookups should succeed: aOK=%v bOK=%v", aOK, bOK)
	}
	if aIdx == bIdx {
		t.Fatalf("same tag name in different families must map to distinct columns: a=%d b=%d", aIdx, bIdx)
	}
}

func TestBatchSchema_TagFamilyDotInName_NoCollision(t *testing.T) {
	s := NewBatchSchema([]ColumnDef{
		{Role: RoleTag, TagFamily: "a.b", Name: "c", Type: ColumnTypeString},
		{Role: RoleTag, TagFamily: "a", Name: "b.c", Type: ColumnTypeString},
	})
	i1, ok1 := s.TagIndex("a.b", "c")
	i2, ok2 := s.TagIndex("a", "b.c")
	if !ok1 || !ok2 {
		t.Fatalf("both lookups must succeed: ok1=%v ok2=%v", ok1, ok2)
	}
	if i1 == i2 {
		t.Fatalf("collision: (a.b,c) and (a,b.c) resolve to the same index %d", i1)
	}
}

func TestBatchSchema_NoMetadata_ReturnsMinusOne(t *testing.T) {
	s := NewBatchSchema([]ColumnDef{
		{Role: RoleTag, TagFamily: "default", Name: "service", Type: ColumnTypeString},
	})
	if got := s.TimestampIndex(); got != -1 {
		t.Fatalf("absent timestamp: want -1, got %d", got)
	}
	if got := s.VersionIndex(); got != -1 {
		t.Fatalf("absent version: want -1, got %d", got)
	}
	if got := s.SeriesIDIndex(); got != -1 {
		t.Fatalf("absent seriesID: want -1, got %d", got)
	}
	if got := s.ShardIDIndex(); got != -1 {
		t.Fatalf("absent shardID: want -1, got %d", got)
	}
}
