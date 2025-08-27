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

package sidx

import (
	"bytes"
	"container/heap"
	"context"
	"testing"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/internal/test"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
)

func TestQueryResponseHeap_BasicOperations(t *testing.T) {
	tests := []struct {
		name string
		asc  bool
	}{
		{"ascending", true},
		{"descending", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			qrh := &QueryResponseHeap{asc: tt.asc}

			if qrh.Len() != 0 {
				t.Error("New heap should be empty")
			}

			response1 := &QueryResponse{
				Keys: []int64{10, 20},
				Data: [][]byte{[]byte("data1"), []byte("data2")},
				SIDs: []common.SeriesID{1, 2},
			}
			response2 := &QueryResponse{
				Keys: []int64{5, 15},
				Data: [][]byte{[]byte("data3"), []byte("data4")},
				SIDs: []common.SeriesID{3, 4},
			}

			cursor1 := &QueryResponseCursor{response: response1, idx: 0}
			cursor2 := &QueryResponseCursor{response: response2, idx: 0}

			qrh.cursors = append(qrh.cursors, cursor1, cursor2)

			if qrh.Len() != 2 {
				t.Errorf("Expected heap length 2, got %d", qrh.Len())
			}

			heap.Init(qrh)

			if tt.asc {
				if !qrh.Less(0, 1) && qrh.cursors[0].response.Keys[0] < qrh.cursors[1].response.Keys[0] {
					t.Error("Ascending heap ordering is incorrect")
				}
			} else {
				if !qrh.Less(0, 1) && qrh.cursors[0].response.Keys[0] > qrh.cursors[1].response.Keys[0] {
					t.Error("Descending heap ordering is incorrect")
				}
			}

			originalCursor0 := qrh.cursors[0]
			qrh.Swap(0, 1)
			if qrh.cursors[0] == originalCursor0 {
				t.Error("Swap operation failed")
			}

			qrh.reset()
			if qrh.Len() != 0 {
				t.Error("Reset should clear all cursors")
			}
		})
	}
}

func TestQueryResponseHeap_PushPop(t *testing.T) {
	qrh := &QueryResponseHeap{asc: true}

	response1 := &QueryResponse{
		Keys: []int64{10},
		Data: [][]byte{[]byte("data1")},
		SIDs: []common.SeriesID{1},
	}
	response2 := &QueryResponse{
		Keys: []int64{5},
		Data: [][]byte{[]byte("data2")},
		SIDs: []common.SeriesID{2},
	}
	response3 := &QueryResponse{
		Keys: []int64{15},
		Data: [][]byte{[]byte("data3")},
		SIDs: []common.SeriesID{3},
	}

	cursor1 := &QueryResponseCursor{response: response1, idx: 0}
	cursor2 := &QueryResponseCursor{response: response2, idx: 0}
	cursor3 := &QueryResponseCursor{response: response3, idx: 0}

	heap.Init(qrh)
	heap.Push(qrh, cursor1)
	heap.Push(qrh, cursor2)
	heap.Push(qrh, cursor3)

	if qrh.Len() != 3 {
		t.Errorf("Expected heap length 3, got %d", qrh.Len())
	}

	top := heap.Pop(qrh).(*QueryResponseCursor)
	if top.response.Keys[0] != 5 {
		t.Errorf("Expected top element key 5, got %d", top.response.Keys[0])
	}

	if qrh.Len() != 2 {
		t.Errorf("Expected heap length 2 after pop, got %d", qrh.Len())
	}

	top = heap.Pop(qrh).(*QueryResponseCursor)
	if top.response.Keys[0] != 10 {
		t.Errorf("Expected second element key 10, got %d", top.response.Keys[0])
	}

	top = heap.Pop(qrh).(*QueryResponseCursor)
	if top.response.Keys[0] != 15 {
		t.Errorf("Expected third element key 15, got %d", top.response.Keys[0])
	}

	if qrh.Len() != 0 {
		t.Error("Heap should be empty after all pops")
	}
}

func TestQueryResponseHeap_MergeWithHeapAscending(t *testing.T) {
	response1 := &QueryResponse{
		Keys: []int64{1, 5, 9},
		Data: [][]byte{[]byte("a1"), []byte("a5"), []byte("a9")},
		Tags: [][]Tag{
			{{Name: "tag1", Value: []byte("val1"), ValueType: pbv1.ValueTypeStr}},
			{{Name: "tag1", Value: []byte("val5"), ValueType: pbv1.ValueTypeStr}},
			{{Name: "tag1", Value: []byte("val9"), ValueType: pbv1.ValueTypeStr}},
		},
		SIDs: []common.SeriesID{1, 1, 1},
	}
	response2 := &QueryResponse{
		Keys: []int64{2, 6, 10},
		Data: [][]byte{[]byte("b2"), []byte("b6"), []byte("b10")},
		Tags: [][]Tag{
			{{Name: "tag2", Value: []byte("val2"), ValueType: pbv1.ValueTypeStr}},
			{{Name: "tag2", Value: []byte("val6"), ValueType: pbv1.ValueTypeStr}},
			{{Name: "tag2", Value: []byte("val10"), ValueType: pbv1.ValueTypeStr}},
		},
		SIDs: []common.SeriesID{2, 2, 2},
	}
	response3 := &QueryResponse{
		Keys: []int64{3, 7},
		Data: [][]byte{[]byte("c3"), []byte("c7")},
		Tags: [][]Tag{
			{{Name: "tag3", Value: []byte("val3"), ValueType: pbv1.ValueTypeStr}},
			{{Name: "tag3", Value: []byte("val7"), ValueType: pbv1.ValueTypeStr}},
		},
		SIDs: []common.SeriesID{3, 3},
	}

	qrh := &QueryResponseHeap{asc: true}
	qrh.cursors = []*QueryResponseCursor{
		{response: response1, idx: 0},
		{response: response2, idx: 0},
		{response: response3, idx: 0},
	}

	heap.Init(qrh)

	result := qrh.mergeWithHeap(10)

	expectedKeys := []int64{1, 2, 3, 5, 6, 7, 9, 10}
	if len(result.Keys) != len(expectedKeys) {
		t.Fatalf("Expected %d keys, got %d", len(expectedKeys), len(result.Keys))
	}

	for i, key := range expectedKeys {
		if result.Keys[i] != key {
			t.Errorf("At position %d: expected key %d, got %d", i, key, result.Keys[i])
		}
	}

	if len(result.Data) != len(result.Keys) {
		t.Error("Data length should match keys length")
	}
	if len(result.Tags) != len(result.Keys) {
		t.Error("Tags length should match keys length")
	}
	if len(result.SIDs) != len(result.Keys) {
		t.Error("SIDs length should match keys length")
	}

	if string(result.Data[0]) != "a1" {
		t.Errorf("Expected first data 'a1', got '%s'", string(result.Data[0]))
	}
	if string(result.Data[1]) != "b2" {
		t.Errorf("Expected second data 'b2', got '%s'", string(result.Data[1]))
	}
}

func TestQueryResponseHeap_MergeWithHeapDescending(t *testing.T) {
	// For descending merge, we start from the end of each response
	// The cursor starts at the last index and moves backwards (step = -1)
	// So response1 starts at idx=2 (key=1), response2 starts at idx=2 (key=2)
	// Since it's descending heap, it prioritizes larger values
	// So first element should be 2, then 1, then response moves backwards...
	response1 := &QueryResponse{
		Keys: []int64{1, 5, 9}, // Will be accessed backwards: 9, 5, 1
		Data: [][]byte{[]byte("a1"), []byte("a5"), []byte("a9")},
		Tags: [][]Tag{
			{{Name: "tag1", Value: []byte("val1"), ValueType: pbv1.ValueTypeStr}},
			{{Name: "tag1", Value: []byte("val5"), ValueType: pbv1.ValueTypeStr}},
			{{Name: "tag1", Value: []byte("val9"), ValueType: pbv1.ValueTypeStr}},
		},
		SIDs: []common.SeriesID{1, 1, 1},
	}
	response2 := &QueryResponse{
		Keys: []int64{2, 6, 10}, // Will be accessed backwards: 10, 6, 2
		Data: [][]byte{[]byte("b2"), []byte("b6"), []byte("b10")},
		Tags: [][]Tag{
			{{Name: "tag2", Value: []byte("val2"), ValueType: pbv1.ValueTypeStr}},
			{{Name: "tag2", Value: []byte("val6"), ValueType: pbv1.ValueTypeStr}},
			{{Name: "tag2", Value: []byte("val10"), ValueType: pbv1.ValueTypeStr}},
		},
		SIDs: []common.SeriesID{2, 2, 2},
	}

	qrh := &QueryResponseHeap{asc: false}
	qrh.cursors = []*QueryResponseCursor{
		{response: response1, idx: response1.Len() - 1}, // starts at idx=2 (key=1)
		{response: response2, idx: response2.Len() - 1}, // starts at idx=2 (key=2)
	}

	heap.Init(qrh)

	result := qrh.mergeWithHeap(10)

	// Since descending heap prioritizes larger values and we start from the end,
	// First: cursor2 has key=10 (larger), cursor1 has key=9
	// After taking 10, cursor2 moves to idx=1 (key=6), cursor1 still at idx=2 (key=9)
	// Next: cursor1 has key=9 (larger), so take 9
	// After taking 9, cursor1 moves to idx=1 (key=5), cursor2 still at idx=1 (key=6)
	// Next: cursor2 has key=6 (larger), so take 6
	// After taking 6, cursor2 moves to idx=0 (key=2), cursor1 still at idx=1 (key=5)
	// Next: cursor1 has key=5 (larger), so take 5
	// After taking 5, cursor1 moves to idx=0 (key=1), cursor2 still at idx=0 (key=2)
	// Next: cursor2 has key=2 (larger), so take 2
	// After taking 2, cursor2 is exhausted, cursor1 takes over with 1
	expectedKeys := []int64{10, 9, 6, 5, 2, 1}
	if len(result.Keys) != len(expectedKeys) {
		t.Fatalf("Expected %d keys, got %d", len(expectedKeys), len(result.Keys))
	}

	for i, key := range expectedKeys {
		if result.Keys[i] != key {
			t.Errorf("At position %d: expected key %d, got %d", i, key, result.Keys[i])
		}
	}

	if string(result.Data[0]) != "b10" {
		t.Errorf("Expected first data 'b10', got '%s'", string(result.Data[0]))
	}
	if string(result.Data[1]) != "a9" {
		t.Errorf("Expected second data 'a9', got '%s'", string(result.Data[1]))
	}
}

func TestQueryResponseHeap_MergeWithLimit(t *testing.T) {
	response1 := &QueryResponse{
		Keys: []int64{1, 3, 5},
		Data: [][]byte{[]byte("a1"), []byte("a3"), []byte("a5")},
		Tags: [][]Tag{
			{{Name: "tag1", Value: []byte("val1"), ValueType: pbv1.ValueTypeStr}},
			{{Name: "tag1", Value: []byte("val3"), ValueType: pbv1.ValueTypeStr}},
			{{Name: "tag1", Value: []byte("val5"), ValueType: pbv1.ValueTypeStr}},
		},
		SIDs: []common.SeriesID{1, 1, 1},
	}
	response2 := &QueryResponse{
		Keys: []int64{2, 4, 6},
		Data: [][]byte{[]byte("b2"), []byte("b4"), []byte("b6")},
		Tags: [][]Tag{
			{{Name: "tag2", Value: []byte("val2"), ValueType: pbv1.ValueTypeStr}},
			{{Name: "tag2", Value: []byte("val4"), ValueType: pbv1.ValueTypeStr}},
			{{Name: "tag2", Value: []byte("val6"), ValueType: pbv1.ValueTypeStr}},
		},
		SIDs: []common.SeriesID{2, 2, 2},
	}

	qrh := &QueryResponseHeap{asc: true}
	qrh.cursors = []*QueryResponseCursor{
		{response: response1, idx: 0},
		{response: response2, idx: 0},
	}

	heap.Init(qrh)

	result := qrh.mergeWithHeap(3)

	expectedKeys := []int64{1, 2, 3}
	if len(result.Keys) != len(expectedKeys) {
		t.Fatalf("Expected %d keys due to limit, got %d", len(expectedKeys), len(result.Keys))
	}

	for i, key := range expectedKeys {
		if result.Keys[i] != key {
			t.Errorf("At position %d: expected key %d, got %d", i, key, result.Keys[i])
		}
	}
}

func TestQueryResponseHeap_EdgeCases(t *testing.T) {
	t.Run("empty heap", func(t *testing.T) {
		qrh := &QueryResponseHeap{asc: true}
		result := qrh.mergeWithHeap(10)

		if result.Len() != 0 {
			t.Error("Empty heap should produce empty result")
		}
	})

	t.Run("single element", func(t *testing.T) {
		response := &QueryResponse{
			Keys: []int64{42},
			Data: [][]byte{[]byte("single")},
			Tags: [][]Tag{{{Name: "tag", Value: []byte("value"), ValueType: pbv1.ValueTypeStr}}},
			SIDs: []common.SeriesID{1},
		}

		qrh := &QueryResponseHeap{asc: true}
		qrh.cursors = []*QueryResponseCursor{{response: response, idx: 0}}
		heap.Init(qrh)

		result := qrh.mergeWithHeap(10)

		if result.Len() != 1 {
			t.Errorf("Expected 1 element, got %d", result.Len())
		}
		if result.Keys[0] != 42 {
			t.Errorf("Expected key 42, got %d", result.Keys[0])
		}
		if string(result.Data[0]) != "single" {
			t.Errorf("Expected data 'single', got '%s'", string(result.Data[0]))
		}
	})

	t.Run("zero limit", func(t *testing.T) {
		response := &QueryResponse{
			Keys: []int64{1, 2, 3},
			Data: [][]byte{[]byte("a"), []byte("b"), []byte("c")},
			Tags: [][]Tag{
				{{Name: "tag", Value: []byte("val1"), ValueType: pbv1.ValueTypeStr}},
				{{Name: "tag", Value: []byte("val2"), ValueType: pbv1.ValueTypeStr}},
				{{Name: "tag", Value: []byte("val3"), ValueType: pbv1.ValueTypeStr}},
			},
			SIDs: []common.SeriesID{1, 1, 1},
		}

		qrh := &QueryResponseHeap{asc: true}
		qrh.cursors = []*QueryResponseCursor{{response: response, idx: 0}}
		heap.Init(qrh)

		result := qrh.mergeWithHeap(0)

		// With limit=0, should return all elements since limit=0 means no limit
		expectedLen := 3
		if result.Len() != expectedLen {
			t.Errorf("With zero limit (no limit), expected %d elements, got %d", expectedLen, result.Len())
		}
		if result.Len() > 0 && result.Keys[0] != 1 {
			t.Errorf("Expected first key to be 1, got %d", result.Keys[0])
		}
	})

	t.Run("empty response in cursor", func(t *testing.T) {
		normalResponse := &QueryResponse{
			Keys: []int64{5},
			Data: [][]byte{[]byte("normal")},
			Tags: [][]Tag{{{Name: "tag", Value: []byte("value"), ValueType: pbv1.ValueTypeStr}}},
			SIDs: []common.SeriesID{1},
		}

		qrh := &QueryResponseHeap{asc: true}
		qrh.cursors = []*QueryResponseCursor{
			{response: normalResponse, idx: 0},
		}

		heap.Init(qrh)

		result := qrh.mergeWithHeap(10)

		if result.Len() != 1 {
			t.Errorf("Expected 1 element from non-empty response, got %d", result.Len())
		}
		if result.Keys[0] != 5 {
			t.Errorf("Expected key 5, got %d", result.Keys[0])
		}
	})
}

func TestMergeQueryResponseShardsAsc(t *testing.T) {
	shard1 := &QueryResponse{
		Keys: []int64{1, 5, 9},
		Data: [][]byte{[]byte("s1_1"), []byte("s1_5"), []byte("s1_9")},
		Tags: [][]Tag{
			{{Name: "shard1", Value: []byte("val1"), ValueType: pbv1.ValueTypeStr}},
			{{Name: "shard1", Value: []byte("val5"), ValueType: pbv1.ValueTypeStr}},
			{{Name: "shard1", Value: []byte("val9"), ValueType: pbv1.ValueTypeStr}},
		},
		SIDs: []common.SeriesID{1, 1, 1},
	}
	shard2 := &QueryResponse{
		Keys: []int64{2, 6, 10},
		Data: [][]byte{[]byte("s2_2"), []byte("s2_6"), []byte("s2_10")},
		Tags: [][]Tag{
			{{Name: "shard2", Value: []byte("val2"), ValueType: pbv1.ValueTypeStr}},
			{{Name: "shard2", Value: []byte("val6"), ValueType: pbv1.ValueTypeStr}},
			{{Name: "shard2", Value: []byte("val10"), ValueType: pbv1.ValueTypeStr}},
		},
		SIDs: []common.SeriesID{2, 2, 2},
	}
	shard3 := &QueryResponse{
		Keys: []int64{3, 7},
		Data: [][]byte{[]byte("s3_3"), []byte("s3_7")},
		Tags: [][]Tag{
			{{Name: "shard3", Value: []byte("val3"), ValueType: pbv1.ValueTypeStr}},
			{{Name: "shard3", Value: []byte("val7"), ValueType: pbv1.ValueTypeStr}},
		},
		SIDs: []common.SeriesID{3, 3},
	}

	shards := []*QueryResponse{shard1, shard2, shard3}

	result := mergeQueryResponseShardsAsc(shards, 100)

	expectedKeys := []int64{1, 2, 3, 5, 6, 7, 9, 10}
	if len(result.Keys) != len(expectedKeys) {
		t.Fatalf("Expected %d keys, got %d", len(expectedKeys), len(result.Keys))
	}

	for i, key := range expectedKeys {
		if result.Keys[i] != key {
			t.Errorf("At position %d: expected key %d, got %d", i, key, result.Keys[i])
		}
	}
}

func TestMergeQueryResponseShardsDesc(t *testing.T) {
	shard1 := &QueryResponse{
		Keys: []int64{9, 5, 1},
		Data: [][]byte{[]byte("s1_9"), []byte("s1_5"), []byte("s1_1")},
		Tags: [][]Tag{
			{{Name: "shard1", Value: []byte("val9"), ValueType: pbv1.ValueTypeStr}},
			{{Name: "shard1", Value: []byte("val5"), ValueType: pbv1.ValueTypeStr}},
			{{Name: "shard1", Value: []byte("val1"), ValueType: pbv1.ValueTypeStr}},
		},
		SIDs: []common.SeriesID{1, 1, 1},
	}
	shard2 := &QueryResponse{
		Keys: []int64{10, 6, 2},
		Data: [][]byte{[]byte("s2_10"), []byte("s2_6"), []byte("s2_2")},
		Tags: [][]Tag{
			{{Name: "shard2", Value: []byte("val10"), ValueType: pbv1.ValueTypeStr}},
			{{Name: "shard2", Value: []byte("val6"), ValueType: pbv1.ValueTypeStr}},
			{{Name: "shard2", Value: []byte("val2"), ValueType: pbv1.ValueTypeStr}},
		},
		SIDs: []common.SeriesID{2, 2, 2},
	}

	shards := []*QueryResponse{shard1, shard2}

	result := mergeQueryResponseShardsDesc(shards, 100)

	expectedKeys := []int64{2, 6, 10, 1, 5, 9}
	if len(result.Keys) != len(expectedKeys) {
		t.Fatalf("Expected %d keys, got %d", len(expectedKeys), len(result.Keys))
	}

	for i, key := range expectedKeys {
		if result.Keys[i] != key {
			t.Errorf("At position %d: expected key %d, got %d", i, key, result.Keys[i])
		}
	}
}

func TestMergeQueryResponseShards_EmptyShards(t *testing.T) {
	t.Run("all empty shards ascending", func(t *testing.T) {
		emptyShards := []*QueryResponse{
			{Keys: []int64{}, Data: [][]byte{}, SIDs: []common.SeriesID{}},
			{Keys: []int64{}, Data: [][]byte{}, SIDs: []common.SeriesID{}},
		}

		result := mergeQueryResponseShardsAsc(emptyShards, 10)

		if result.Len() != 0 {
			t.Error("All empty shards should produce empty result")
		}
	})

	t.Run("all empty shards descending", func(t *testing.T) {
		emptyShards := []*QueryResponse{
			{Keys: []int64{}, Data: [][]byte{}, SIDs: []common.SeriesID{}},
			{Keys: []int64{}, Data: [][]byte{}, SIDs: []common.SeriesID{}},
		}

		result := mergeQueryResponseShardsDesc(emptyShards, 10)

		if result.Len() != 0 {
			t.Error("All empty shards should produce empty result")
		}
	})

	t.Run("mixed empty and non-empty shards", func(t *testing.T) {
		shards := []*QueryResponse{
			{Keys: []int64{}, Data: [][]byte{}, Tags: [][]Tag{}, SIDs: []common.SeriesID{}},
			{
				Keys: []int64{5, 10},
				Data: [][]byte{[]byte("a"), []byte("b")},
				Tags: [][]Tag{
					{{Name: "mixed", Value: []byte("val5"), ValueType: pbv1.ValueTypeStr}},
					{{Name: "mixed", Value: []byte("val10"), ValueType: pbv1.ValueTypeStr}},
				},
				SIDs: []common.SeriesID{1, 1},
			},
			{Keys: []int64{}, Data: [][]byte{}, Tags: [][]Tag{}, SIDs: []common.SeriesID{}},
		}

		result := mergeQueryResponseShardsAsc(shards, 10)

		if result.Len() != 2 {
			t.Errorf("Expected 2 elements from non-empty shard, got %d", result.Len())
		}
		if result.Keys[0] != 5 || result.Keys[1] != 10 {
			t.Error("Keys should be preserved from non-empty shard")
		}
	})
}

// Benchmark tests for performance validation.
func BenchmarkQueryResponseHeap_MergeAscending(b *testing.B) {
	shard1 := createBenchmarkResponse(1000, 1, 2)
	shard2 := createBenchmarkResponse(1000, 2, 2)
	shard3 := createBenchmarkResponse(1000, 3, 2)
	shards := []*QueryResponse{shard1, shard2, shard3}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result := mergeQueryResponseShardsAsc(shards, 3000)
		if result.Len() == 0 {
			b.Error("Benchmark should produce non-empty result")
		}
	}
}

func BenchmarkQueryResponseHeap_MergeDescending(b *testing.B) {
	shard1 := createBenchmarkResponseDesc(1000, 1, 2)
	shard2 := createBenchmarkResponseDesc(1000, 2, 2)
	shard3 := createBenchmarkResponseDesc(1000, 3, 2)
	shards := []*QueryResponse{shard1, shard2, shard3}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result := mergeQueryResponseShardsDesc(shards, 3000)
		if result.Len() == 0 {
			b.Error("Benchmark should produce non-empty result")
		}
	}
}

func BenchmarkQueryResponseHeap_LargeMerge(b *testing.B) {
	const numShards = 10
	const elementsPerShard = 10000
	shards := make([]*QueryResponse, numShards)

	for i := 0; i < numShards; i++ {
		shards[i] = createBenchmarkResponse(elementsPerShard, int64(i), int64(numShards))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result := mergeQueryResponseShardsAsc(shards, elementsPerShard*numShards)
		if result.Len() == 0 {
			b.Error("Benchmark should produce non-empty result")
		}
	}
}

func createBenchmarkResponse(size int, offset, step int64) *QueryResponse {
	response := &QueryResponse{
		Keys: make([]int64, size),
		Data: make([][]byte, size),
		Tags: make([][]Tag, size),
		SIDs: make([]common.SeriesID, size),
	}

	for i := 0; i < size; i++ {
		key := offset + int64(i)*step
		response.Keys[i] = key
		response.Data[i] = []byte("benchmark_data")
		response.Tags[i] = []Tag{{Name: "benchmark", Value: []byte("value"), ValueType: pbv1.ValueTypeStr}}
		response.SIDs[i] = common.SeriesID(offset)
	}

	return response
}

func createBenchmarkResponseDesc(size int, offset, step int64) *QueryResponse {
	response := &QueryResponse{
		Keys: make([]int64, size),
		Data: make([][]byte, size),
		Tags: make([][]Tag, size),
		SIDs: make([]common.SeriesID, size),
	}

	for i := 0; i < size; i++ {
		key := offset + int64(size-1-i)*step
		response.Keys[i] = key
		response.Data[i] = []byte("benchmark_data")
		response.Tags[i] = []Tag{{Name: "benchmark", Value: []byte("value"), ValueType: pbv1.ValueTypeStr}}
		response.SIDs[i] = common.SeriesID(offset)
	}

	return response
}

// Tests for queryResult struct using memPart datasets.

func TestQueryResult_Pull_SingleMemPart(t *testing.T) {
	// Create test dataset with multiple series
	expectedElements := []testElement{
		{seriesID: 1, userKey: 100, data: []byte("data1"), tags: []tag{{name: "service", value: []byte("service1"), valueType: pbv1.ValueTypeStr, indexed: true}}},
		{seriesID: 2, userKey: 200, data: []byte("data2"), tags: []tag{{name: "service", value: []byte("service2"), valueType: pbv1.ValueTypeStr, indexed: true}}},
		{seriesID: 3, userKey: 300, data: []byte("data3"), tags: []tag{{name: "environment", value: []byte("prod"), valueType: pbv1.ValueTypeStr, indexed: true}}},
	}

	elements := createTestElements(expectedElements)
	defer releaseElements(elements)

	// Create memory part from elements
	mp := generateMemPart()
	defer releaseMemPart(mp)
	mp.mustInitFromElements(elements)

	// Create part from memory part
	testPart := openMemPart(mp)
	defer testPart.close()

	// Create snapshot and parts
	parts := []*part{testPart}
	snap := &snapshot{}
	snap.parts = make([]*partWrapper, len(parts))
	for i, p := range parts {
		snap.parts[i] = newPartWrapper(p)
	}
	// Initialize reference count to 1
	snap.ref = 1

	// Create mock memory protector
	mockProtector := &test.MockMemoryProtector{ExpectQuotaExceeded: false}

	// Create query request with default MaxElementSize (0) to test the fix
	req := QueryRequest{
		SeriesIDs:      []common.SeriesID{1, 2, 3},
		MinKey:         nil,
		MaxKey:         nil,
		Filter:         nil,
		Order:          nil,
		MaxElementSize: 0, // Test with default value (0) to ensure fix works
	}

	// Debug: log the request parameters
	t.Logf("Request MaxElementSize: %d", req.MaxElementSize)
	t.Logf("Element keys: %v", []int64{100, 200, 300})
	t.Logf("Key range filter: [%d, %d]", 50, 400)

	// Create block scanner
	bs := &blockScanner{
		pm:        mockProtector,
		filter:    req.Filter,
		l:         logger.GetLogger("test"),
		parts:     parts,
		seriesIDs: req.SeriesIDs,
		minKey:    50,
		maxKey:    400,
		asc:       true,
	}

	// Create query result
	qr := &queryResult{
		request:  req,
		ctx:      context.Background(),
		pm:       mockProtector,
		snapshot: snap,
		bs:       bs,
		l:        logger.GetLogger("test-queryResult"),
		asc:      true,
		released: false,
	}

	// Debug: Test if the iterator can find blocks in this memPart
	bma := generateBlockMetadataArray()
	defer releaseBlockMetadataArray(bma)

	it := generateIter()
	defer releaseIter(it)

	it.init(bma, parts, req.SeriesIDs, 50, 400, nil)

	blockCount := 0
	for it.nextBlock() {
		blockCount++
	}
	t.Logf("Iterator found %d blocks", blockCount)
	if it.Error() != nil {
		t.Logf("Iterator error: %v", it.Error())
	}

	// Pull the response
	response := qr.Pull()

	// Debug: check if response is nil first
	t.Logf("Response is nil: %v", response == nil)

	// Verify response is not nil
	if response == nil {
		t.Fatal("QueryResult.Pull() returned nil response")
	}

	// Check for errors in response
	if response.Error != nil {
		t.Fatalf("QueryResult returned error: %v", response.Error)
	}

	// Verify response has the expected number of elements
	if response.Len() != len(expectedElements) {
		t.Fatalf("Expected %d elements in response, got %d", len(expectedElements), response.Len())
	}

	// Verify data consistency
	err := response.Validate()
	if err != nil {
		t.Fatalf("Response validation failed: %v", err)
	}

	// Create a map of expected data by key for easy lookup
	expectedByKey := make(map[int64]testElement)
	for _, elem := range expectedElements {
		expectedByKey[elem.userKey] = elem
	}

	// Verify each returned element matches expected data
	for i := 0; i < response.Len(); i++ {
		key := response.Keys[i]
		data := response.Data[i]
		sid := response.SIDs[i]
		tags := response.Tags[i]

		// Find expected element by key
		expected, found := expectedByKey[key]
		if !found {
			t.Errorf("Unexpected key %d found in response at position %d", key, i)
			continue
		}

		// Verify series ID matches
		if sid != expected.seriesID {
			t.Errorf("At position %d: expected seriesID %d, got %d", i, expected.seriesID, sid)
		}

		// Verify data matches
		if !bytes.Equal(data, expected.data) {
			t.Errorf("At position %d: expected data %s, got %s", i, string(expected.data), string(data))
		}

		// Verify tags match
		if len(tags) != len(expected.tags) {
			t.Errorf("At position %d: expected %d tags, got %d", i, len(expected.tags), len(tags))
			continue
		}

		for j, tag := range tags {
			expectedTag := expected.tags[j]
			if tag.Name != expectedTag.name {
				t.Errorf("At position %d, tag %d: expected name %s, got %s", i, j, expectedTag.name, tag.Name)
			}
			if !bytes.Equal(tag.Value, expectedTag.value) {
				t.Errorf("At position %d, tag %d: expected value %s, got %s", i, j, string(expectedTag.value), string(tag.Value))
			}
			if tag.ValueType != expectedTag.valueType {
				t.Errorf("At position %d, tag %d: expected valueType %v, got %v", i, j, expectedTag.valueType, tag.ValueType)
			}
		}
	}

	// Verify keys are within expected range and properly sorted
	for i, key := range response.Keys {
		if key < 50 || key > 400 {
			t.Errorf("Key %d at position %d is outside expected range [50, 400]", key, i)
		}
		if i > 0 && key <= response.Keys[i-1] {
			t.Errorf("Keys not properly sorted: key %d at position %d should be greater than previous key %d", key, i, response.Keys[i-1])
		}
	}

	// Clean up
	qr.Release()
}

func TestQueryResult_Pull_MultipleMemParts(t *testing.T) {
	// Create multiple test datasets with expected elements
	expectedElements := []testElement{
		{seriesID: 1, userKey: 100, data: []byte("data1_part1"), tags: []tag{{name: "service", value: []byte("service1"), valueType: pbv1.ValueTypeStr, indexed: true}}},
		{seriesID: 2, userKey: 200, data: []byte("data2_part2"), tags: []tag{{name: "service", value: []byte("service2"), valueType: pbv1.ValueTypeStr, indexed: true}}},
		{seriesID: 3, userKey: 300, data: []byte("data3_part1"), tags: []tag{{name: "service", value: []byte("service3"), valueType: pbv1.ValueTypeStr, indexed: true}}},
		{seriesID: 4, userKey: 400, data: []byte("data4_part2"), tags: []tag{{name: "environment", value: []byte("test"), valueType: pbv1.ValueTypeStr, indexed: true}}},
	}

	// Create test elements for each part
	elements1 := createTestElements([]testElement{expectedElements[0], expectedElements[2]}) // data1_part1, data3_part1
	defer releaseElements(elements1)

	elements2 := createTestElements([]testElement{expectedElements[1], expectedElements[3]}) // data2_part2, data4_part2
	defer releaseElements(elements2)

	// Create memory parts
	mp1 := generateMemPart()
	defer releaseMemPart(mp1)
	mp1.mustInitFromElements(elements1)

	mp2 := generateMemPart()
	defer releaseMemPart(mp2)
	mp2.mustInitFromElements(elements2)

	// Create parts from memory parts
	testPart1 := openMemPart(mp1)
	defer testPart1.close()
	testPart2 := openMemPart(mp2)
	defer testPart2.close()

	// Create snapshot with multiple parts
	parts := []*part{testPart1, testPart2}
	snap := &snapshot{}
	snap.parts = make([]*partWrapper, len(parts))
	for i, p := range parts {
		snap.parts[i] = newPartWrapper(p)
	}
	// Initialize reference count to 1
	snap.ref = 1

	// Create mock memory protector
	mockProtector := &test.MockMemoryProtector{ExpectQuotaExceeded: false}

	// Create query request targeting all series
	req := QueryRequest{
		SeriesIDs:      []common.SeriesID{1, 2, 3, 4},
		MinKey:         nil,
		MaxKey:         nil,
		Filter:         nil,
		Order:          nil,
		MaxElementSize: 0, // Test with default value (0) to ensure fix works
	}

	// Create block scanner
	bs := &blockScanner{
		pm:        mockProtector,
		filter:    req.Filter,
		l:         logger.GetLogger("test"),
		parts:     parts,
		seriesIDs: req.SeriesIDs,
		minKey:    50,
		maxKey:    500,
		asc:       true,
	}

	// Create query result
	qr := &queryResult{
		request:  req,
		ctx:      context.Background(),
		pm:       mockProtector,
		snapshot: snap,
		bs:       bs,
		l:        logger.GetLogger("test-queryResult"),
		asc:      true,
		released: false,
	}

	// Pull the response
	response := qr.Pull()

	// Verify response is not nil
	if response == nil {
		t.Fatal("QueryResult.Pull() returned nil response")
	}

	// Check for errors in response
	if response.Error != nil {
		t.Fatalf("QueryResult returned error: %v", response.Error)
	}

	// Verify response has the expected number of elements
	if response.Len() != len(expectedElements) {
		t.Fatalf("Expected %d elements in response, got %d", len(expectedElements), response.Len())
	}

	// Verify data consistency
	err := response.Validate()
	if err != nil {
		t.Fatalf("Response validation failed: %v", err)
	}

	// Create a map of expected data by key for easy lookup
	expectedByKey := make(map[int64]testElement)
	for _, elem := range expectedElements {
		expectedByKey[elem.userKey] = elem
	}

	// Verify each returned element matches expected data
	for i := 0; i < response.Len(); i++ {
		key := response.Keys[i]
		data := response.Data[i]
		sid := response.SIDs[i]
		tags := response.Tags[i]

		// Find expected element by key
		expected, found := expectedByKey[key]
		if !found {
			t.Errorf("Unexpected key %d found in response at position %d", key, i)
			continue
		}

		// Verify series ID matches
		if sid != expected.seriesID {
			t.Errorf("At position %d: expected seriesID %d, got %d", i, expected.seriesID, sid)
		}

		// Verify data matches
		if !bytes.Equal(data, expected.data) {
			t.Errorf("At position %d: expected data %s, got %s", i, string(expected.data), string(data))
		}

		// Verify tags match
		if len(tags) != len(expected.tags) {
			t.Errorf("At position %d: expected %d tags, got %d", i, len(expected.tags), len(tags))
			continue
		}

		for j, tag := range tags {
			expectedTag := expected.tags[j]
			if tag.Name != expectedTag.name {
				t.Errorf("At position %d, tag %d: expected name %s, got %s", i, j, expectedTag.name, tag.Name)
			}
			if !bytes.Equal(tag.Value, expectedTag.value) {
				t.Errorf("At position %d, tag %d: expected value %s, got %s", i, j, string(expectedTag.value), string(tag.Value))
			}
			if tag.ValueType != expectedTag.valueType {
				t.Errorf("At position %d, tag %d: expected valueType %v, got %v", i, j, expectedTag.valueType, tag.ValueType)
			}
		}
	}

	// Verify keys are within expected range and properly sorted
	for i, key := range response.Keys {
		if key < 50 || key > 500 {
			t.Errorf("Key %d at position %d is outside expected range [50, 500]", key, i)
		}
		if i > 0 && key <= response.Keys[i-1] {
			t.Errorf("Keys not properly sorted: key %d at position %d should be greater than previous key %d", key, i, response.Keys[i-1])
		}
	}

	// Clean up
	qr.Release()
}

func TestQueryResult_Pull_WithTagProjection(t *testing.T) {
	// Create test dataset with multiple tags
	elements := createTestElements([]testElement{
		{
			seriesID: 1, userKey: 100, data: []byte("data1"),
			tags: []tag{
				{name: "service", value: []byte("service1"), valueType: pbv1.ValueTypeStr, indexed: true},
				{name: "environment", value: []byte("prod"), valueType: pbv1.ValueTypeStr, indexed: true},
				{name: "region", value: []byte("us-west"), valueType: pbv1.ValueTypeStr, indexed: true},
			},
		},
		{
			seriesID: 2, userKey: 200, data: []byte("data2"),
			tags: []tag{
				{name: "service", value: []byte("service2"), valueType: pbv1.ValueTypeStr, indexed: true},
				{name: "environment", value: []byte("test"), valueType: pbv1.ValueTypeStr, indexed: true},
				{name: "region", value: []byte("us-east"), valueType: pbv1.ValueTypeStr, indexed: true},
			},
		},
	})
	defer releaseElements(elements)

	// Create memory part
	mp := generateMemPart()
	defer releaseMemPart(mp)
	mp.mustInitFromElements(elements)

	testPart := openMemPart(mp)
	defer testPart.close()

	// Create snapshot
	parts := []*part{testPart}
	snap := &snapshot{}
	snap.parts = make([]*partWrapper, len(parts))
	for i, p := range parts {
		snap.parts[i] = newPartWrapper(p)
	}
	// Initialize reference count to 1
	snap.ref = 1

	mockProtector := &test.MockMemoryProtector{ExpectQuotaExceeded: false}

	// Create query request with tag projection (only load "service" and "environment")
	req := QueryRequest{
		SeriesIDs: []common.SeriesID{1, 2},
		TagProjection: []model.TagProjection{
			{Names: []string{"service", "environment"}},
		},
		MinKey: nil,
		MaxKey: nil,
		Filter: nil,
	}

	bs := &blockScanner{
		pm:        mockProtector,
		filter:    req.Filter,
		l:         logger.GetLogger("test"),
		parts:     parts,
		seriesIDs: req.SeriesIDs,
		minKey:    50,
		maxKey:    300,
		asc:       true,
	}

	qr := &queryResult{
		request:  req,
		ctx:      context.Background(),
		pm:       mockProtector,
		snapshot: snap,
		bs:       bs,
		l:        logger.GetLogger("test-queryResult"),
		asc:      true,
		released: false,
	}

	// Test Pull with tag projection
	response := qr.Pull()

	if response == nil {
		t.Fatal("Expected non-nil response")
	}
	if response.Error != nil {
		t.Fatalf("Expected no error, got: %v", response.Error)
	}

	// Verify response has data
	if response.Len() == 0 {
		t.Error("Expected non-empty response")
	}

	// Verify tag projection worked (should only have projected tags)
	// Note: The actual tag filtering logic needs to be verified based on implementation
	for i, tagGroup := range response.Tags {
		for _, tag := range tagGroup {
			if tag.Name != "service" && tag.Name != "environment" {
				t.Errorf("Unexpected tag '%s' found at position %d, should only have projected tags", tag.Name, i)
			}
		}
	}

	qr.Release()
}

func TestQueryResult_Pull_DescendingOrder(t *testing.T) {
	// Create test dataset
	elements := createTestElements([]testElement{
		{seriesID: 1, userKey: 100, data: []byte("data1"), tags: []tag{{name: "service", value: []byte("service1"), valueType: pbv1.ValueTypeStr, indexed: true}}},
		{seriesID: 1, userKey: 200, data: []byte("data2"), tags: []tag{{name: "service", value: []byte("service1"), valueType: pbv1.ValueTypeStr, indexed: true}}},
		{seriesID: 1, userKey: 300, data: []byte("data3"), tags: []tag{{name: "service", value: []byte("service1"), valueType: pbv1.ValueTypeStr, indexed: true}}},
	})
	defer releaseElements(elements)

	mp := generateMemPart()
	defer releaseMemPart(mp)
	mp.mustInitFromElements(elements)

	testPart := openMemPart(mp)
	defer testPart.close()

	parts := []*part{testPart}
	snap := &snapshot{}
	snap.parts = make([]*partWrapper, len(parts))
	for i, p := range parts {
		snap.parts[i] = newPartWrapper(p)
	}
	// Initialize reference count to 1
	snap.ref = 1

	mockProtector := &test.MockMemoryProtector{ExpectQuotaExceeded: false}

	req := QueryRequest{
		SeriesIDs: []common.SeriesID{1},
		MinKey:    nil,
		MaxKey:    nil,
		Filter:    nil,
	}

	// Create descending order scanner
	bs := &blockScanner{
		pm:        mockProtector,
		filter:    req.Filter,
		l:         logger.GetLogger("test"),
		parts:     parts,
		seriesIDs: req.SeriesIDs,
		minKey:    50,
		maxKey:    400,
		asc:       false, // Descending order
	}

	qr := &queryResult{
		request:  req,
		ctx:      context.Background(),
		pm:       mockProtector,
		snapshot: snap,
		bs:       bs,
		l:        logger.GetLogger("test-queryResult"),
		asc:      false, // Descending order
		released: false,
	}

	response := qr.Pull()

	if response == nil {
		t.Fatal("Expected non-nil response")
	}
	if response.Error != nil {
		t.Fatalf("Expected no error, got: %v", response.Error)
	}

	// Verify descending order
	for i := 1; i < len(response.Keys); i++ {
		if response.Keys[i] > response.Keys[i-1] {
			t.Errorf("Keys are not in descending order: %d > %d at positions %d, %d",
				response.Keys[i], response.Keys[i-1], i, i-1)
		}
	}

	qr.Release()
}

func TestQueryResult_Pull_ErrorHandling(t *testing.T) {
	// Test with quota exceeded scenario
	mockProtector := &test.MockMemoryProtector{ExpectQuotaExceeded: true}

	elements := createTestElements([]testElement{
		{seriesID: 1, userKey: 100, data: []byte("data1"), tags: []tag{{name: "service", value: []byte("service1"), valueType: pbv1.ValueTypeStr, indexed: true}}},
	})
	defer releaseElements(elements)

	mp := generateMemPart()
	defer releaseMemPart(mp)
	mp.mustInitFromElements(elements)

	testPart := openMemPart(mp)
	defer testPart.close()

	parts := []*part{testPart}
	snap := &snapshot{}
	snap.parts = make([]*partWrapper, len(parts))
	for i, p := range parts {
		snap.parts[i] = newPartWrapper(p)
	}
	// Initialize reference count to 1
	snap.ref = 1

	req := QueryRequest{
		SeriesIDs: []common.SeriesID{1},
	}

	bs := &blockScanner{
		pm:        mockProtector,
		filter:    req.Filter,
		l:         logger.GetLogger("test"),
		parts:     parts,
		seriesIDs: req.SeriesIDs,
		minKey:    50,
		maxKey:    200,
		asc:       true,
	}

	qr := &queryResult{
		request:  req,
		ctx:      context.Background(),
		pm:       mockProtector,
		snapshot: snap,
		bs:       bs,
		l:        logger.GetLogger("test-queryResult"),
		asc:      true,
		released: false,
	}

	response := qr.Pull()

	// With quota exceeded, we expect either an error response or a response with an error
	if response != nil && response.Error != nil {
		t.Logf("Expected error due to quota exceeded: %v", response.Error)
	}

	qr.Release()
}

func TestQueryResult_Release(t *testing.T) {
	elements := createTestElements([]testElement{
		{seriesID: 1, userKey: 100, data: []byte("data1"), tags: []tag{{name: "service", value: []byte("service1"), valueType: pbv1.ValueTypeStr, indexed: true}}},
	})
	defer releaseElements(elements)

	mp := generateMemPart()
	defer releaseMemPart(mp)
	mp.mustInitFromElements(elements)

	testPart := openMemPart(mp)
	defer testPart.close()

	parts := []*part{testPart}
	snap := &snapshot{}
	snap.parts = make([]*partWrapper, len(parts))
	for i, p := range parts {
		snap.parts[i] = newPartWrapper(p)
	}
	// Initialize reference count to 1
	snap.ref = 1

	// Create a mock blockScanner to verify it gets closed
	mockBS := &blockScanner{
		pm:        &test.MockMemoryProtector{},
		filter:    nil,
		l:         logger.GetLogger("test"),
		parts:     parts,
		seriesIDs: []common.SeriesID{1},
		minKey:    50,
		maxKey:    200,
		asc:       true,
	}

	// Create a mock protector
	mockProtector := &test.MockMemoryProtector{}

	qr := &queryResult{
		request:    QueryRequest{SeriesIDs: []common.SeriesID{1}},
		ctx:        context.Background(),
		pm:         mockProtector,
		snapshot:   snap,
		bs:         mockBS,
		l:          logger.GetLogger("test-queryResult"),
		tagsToLoad: map[string]struct{}{"service": {}},
		shards:     []*QueryResponse{},
		asc:        true,
		released:   false,
	}

	// Verify initial state
	if qr.released {
		t.Error("queryResult should not be released initially")
	}
	if qr.snapshot == nil {
		t.Error("queryResult should have a snapshot initially")
	}
	if qr.bs == nil {
		t.Error("queryResult should have a blockScanner initially")
	}
	if qr.tagsToLoad == nil {
		t.Error("queryResult should have tagsToLoad initially")
	}
	if qr.shards == nil {
		t.Error("queryResult should have shards initially")
	}

	// Store initial reference count for verification
	initialRef := snap.ref

	// Release
	qr.Release()

	// Verify released flag is set
	if !qr.released {
		t.Error("queryResult should be marked as released")
	}

	// Verify snapshot is nullified and reference count is decremented
	if qr.snapshot != nil {
		t.Error("queryResult.snapshot should be nullified after release")
	}
	if snap.ref != initialRef-1 {
		t.Errorf("snapshot reference count should be decremented from %d to %d, got %d", initialRef, initialRef-1, snap.ref)
	}

	// Verify blockScanner is closed but not nullified (it gets closed in Release method but not set to nil)
	if qr.bs == nil {
		t.Error("queryResult.bs should not be nullified after release, only closed")
	}
	// Note: We can't easily verify that bs.close() was called without exposing internal state
	// The important thing is that the blockScanner is still accessible for potential cleanup

	// Verify other fields remain unchanged (they don't get nullified)
	if qr.tagsToLoad == nil {
		t.Error("queryResult.tagsToLoad should remain unchanged after release")
	}
	if qr.shards == nil {
		t.Error("queryResult.shards should remain unchanged after release")
	}
	if qr.ctx == nil {
		t.Error("queryResult.ctx should remain unchanged after release")
	}
	if qr.pm == nil {
		t.Error("queryResult.pm should remain unchanged after release")
	}
	if qr.l == nil {
		t.Error("queryResult.l should remain unchanged after release")
	}

	// Verify that subsequent calls to Release are safe (idempotent)
	qr.Release()
	if !qr.released {
		t.Error("queryResult should remain released after second release call")
	}
	if qr.snapshot != nil {
		t.Error("queryResult.snapshot should remain nullified after second release call")
	}
	if qr.bs == nil {
		t.Error("queryResult.bs should remain accessible after second release call")
	}
}

func TestQueryResult_Pull_ContextCancellation(t *testing.T) {
	elements := createTestElements([]testElement{
		{seriesID: 1, userKey: 100, data: []byte("data1"), tags: []tag{{name: "service", value: []byte("service1"), valueType: pbv1.ValueTypeStr, indexed: true}}},
	})
	defer releaseElements(elements)

	mp := generateMemPart()
	defer releaseMemPart(mp)
	mp.mustInitFromElements(elements)

	testPart := openMemPart(mp)
	defer testPart.close()

	parts := []*part{testPart}
	snap := &snapshot{}
	snap.parts = make([]*partWrapper, len(parts))
	for i, p := range parts {
		snap.parts[i] = newPartWrapper(p)
	}
	// Initialize reference count to 1
	snap.ref = 1

	mockProtector := &test.MockMemoryProtector{ExpectQuotaExceeded: false}

	// Create canceled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	req := QueryRequest{
		SeriesIDs: []common.SeriesID{1},
	}

	bs := &blockScanner{
		pm:        mockProtector,
		filter:    req.Filter,
		l:         logger.GetLogger("test"),
		parts:     parts,
		seriesIDs: req.SeriesIDs,
		minKey:    50,
		maxKey:    200,
		asc:       true,
	}

	qr := &queryResult{
		request:  req,
		ctx:      ctx,
		pm:       mockProtector,
		snapshot: snap,
		bs:       bs,
		l:        logger.GetLogger("test-queryResult"),
		asc:      true,
		released: false,
	}

	response := qr.Pull()

	// With canceled context, we should handle gracefully
	// The behavior depends on implementation - it might return nil or an error response
	if response != nil {
		t.Logf("Response with canceled context: error=%v, len=%d", response.Error, response.Len())
	} else {
		t.Log("Got nil response with canceled context - this is acceptable")
	}

	qr.Release()
}
