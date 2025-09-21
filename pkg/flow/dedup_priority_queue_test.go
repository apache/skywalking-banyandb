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

package flow

import (
	"container/heap"
	"fmt"
	"testing"
)

type testHashableElement struct {
	id    string
	val   int
	index int
}

func (t *testHashableElement) GetIndex() int {
	return t.index
}

func (t *testHashableElement) SetIndex(idx int) {
	t.index = idx
}

func (t *testHashableElement) Hash() uint64 {
	// Simple hash based on id
	hash := uint64(0)
	for _, b := range []byte(t.id) {
		hash = hash*31 + uint64(b)
	}
	return hash
}

func (t *testHashableElement) Equal(other HashableElement) bool {
	if otherElem, ok := other.(*testHashableElement); ok {
		return t.id == otherElem.id
	}
	return false
}

type testElement struct {
	val   int
	index int
}

func (t *testElement) GetIndex() int {
	return t.index
}

func (t *testElement) SetIndex(idx int) {
	t.index = idx
}

func TestHashableElementHash(t *testing.T) {
	elem1 := &testHashableElement{id: "test"}
	elem2 := &testHashableElement{id: "test"}
	elem3 := &testHashableElement{id: "different"}

	if elem1.Hash() != elem2.Hash() {
		t.Errorf("Expected same hash for elements with same id, got %d and %d", elem1.Hash(), elem2.Hash())
	}

	if elem1.Hash() == elem3.Hash() {
		t.Errorf("Expected different hash for elements with different id, got same hash %d", elem1.Hash())
	}
}

func TestHashableElementEqual(t *testing.T) {
	elem1 := &testHashableElement{id: "test", val: 10}
	elem2 := &testHashableElement{id: "test", val: 20}      // Same id, different val
	elem3 := &testHashableElement{id: "different", val: 10} // Different id, same val

	// Same id should be equal regardless of other fields
	if !elem1.Equal(elem2) {
		t.Error("Expected elements with same id to be equal")
	}

	// Different id should not be equal
	if elem1.Equal(elem3) {
		t.Error("Expected elements with different id to not be equal")
	}
}

func TestDedupPriorityQueue_ContentBasedDeduplication(t *testing.T) {
	// Create a priority queue with deduplication enabled
	pq := NewPriorityQueue(func(a, b interface{}) int {
		return a.(*testHashableElement).val - b.(*testHashableElement).val
	}, false)

	// Create two elements with the same id (should be deduplicated)
	elem1 := &testHashableElement{id: "test", val: 10}
	elem2 := &testHashableElement{id: "test", val: 10}

	// Push both elements
	heap.Push(pq, elem1)
	heap.Push(pq, elem2)

	// Should only have one element in the heap
	if pq.Len() != 1 {
		t.Errorf("Expected 1 element in heap after deduplication, got %d", pq.Len())
	}

	// Create elements with different ids (should not be deduplicated)
	elem3 := &testHashableElement{id: "different", val: 20}
	heap.Push(pq, elem3)

	// Should now have two elements
	if pq.Len() != 2 {
		t.Errorf("Expected 2 elements in heap after adding different element, got %d", pq.Len())
	}
}

func TestDedupPriorityQueue_AllowDuplicates(t *testing.T) {
	// Create a priority queue with duplicates allowed
	pq := NewPriorityQueue(func(a, b interface{}) int {
		return a.(*testHashableElement).val - b.(*testHashableElement).val
	}, true)

	// Create two elements with the same id
	elem1 := &testHashableElement{id: "test", val: 10}
	elem2 := &testHashableElement{id: "test", val: 10}

	// Push both elements
	heap.Push(pq, elem1)
	heap.Push(pq, elem2)

	// Should have two elements in the heap (duplicates allowed)
	if pq.Len() != 2 {
		t.Errorf("Expected 2 elements in heap when duplicates are allowed, got %d", pq.Len())
	}

	// Even pushing the same reference should be allowed
	heap.Push(pq, elem1)
	if pq.Len() != 3 {
		t.Errorf("Expected 3 elements in heap after pushing same reference when duplicates allowed, got %d", pq.Len())
	}
}

func TestDedupPriorityQueue_HashCollisionHandling(t *testing.T) {
	// Create a priority queue with deduplication enabled
	pq := NewPriorityQueue(func(a, b interface{}) int {
		return a.(*testHashableElement).val - b.(*testHashableElement).val
	}, false)

	// Create elements that might have hash collisions
	elem1 := &testHashableElement{id: "ab", val: 10} // Hash might collide with "ba"
	elem2 := &testHashableElement{id: "ba", val: 10} // Different id, might have same hash
	elem3 := &testHashableElement{id: "ab", val: 20} // Same id as elem1, should be deduplicated

	heap.Push(pq, elem1)
	heap.Push(pq, elem2)
	heap.Push(pq, elem3)

	// Should have 2 elements: elem1 (or elem3, they're equivalent) and elem2
	if pq.Len() != 2 {
		t.Errorf("Expected 2 elements after hash collision test, got %d", pq.Len())
	}
}

func TestDedupPriorityQueue_PopCleanup(t *testing.T) {
	// Create a priority queue with deduplication enabled
	pq := NewPriorityQueue(func(a, b interface{}) int {
		return a.(*testHashableElement).val - b.(*testHashableElement).val
	}, false)

	// Add elements
	elem1 := &testHashableElement{id: "test1", val: 10}
	elem2 := &testHashableElement{id: "test2", val: 20}
	elem3 := &testHashableElement{id: "test1", val: 30} // Same id as elem1

	heap.Push(pq, elem1)
	heap.Push(pq, elem2)
	heap.Push(pq, elem3) // Should be deduplicated

	// Should have 2 elements (elem1 and elem2)
	if pq.Len() != 2 {
		t.Errorf("Expected 2 elements after deduplication, got %d", pq.Len())
	}

	// Pop an element
	popped := heap.Pop(pq).(*testHashableElement)

	// Should have 1 element left
	if pq.Len() != 1 {
		t.Errorf("Expected 1 element after pop, got %d", pq.Len())
	}

	// Try to add the same element again - should be allowed since it was popped
	heap.Push(pq, popped)

	// Should now have 2 elements again
	if pq.Len() != 2 {
		t.Errorf("Expected 2 elements after re-adding popped element, got %d", pq.Len())
	}
}

func TestDedupPriorityQueue_MixedElementTypes(t *testing.T) {
	// Create a priority queue that can handle both Element and HashableElement
	pq := NewPriorityQueue(func(a, b interface{}) int {
		switch va := a.(type) {
		case *testHashableElement:
			switch vb := b.(type) {
			case *testHashableElement:
				return va.val - vb.val
			case *testElement:
				return va.val - vb.val
			}
		case *testElement:
			switch vb := b.(type) {
			case *testHashableElement:
				return va.val - vb.val
			case *testElement:
				return va.val - vb.val
			}
		}
		return 0
	}, false)

	// Add mixed element types
	hashElem1 := &testHashableElement{id: "test", val: 10}
	hashElem2 := &testHashableElement{id: "test", val: 20} // Same id, should be deduplicated
	regularElem := &testElement{val: 15}

	heap.Push(pq, hashElem1)
	heap.Push(pq, regularElem)
	heap.Push(pq, hashElem2) // Should be deduplicated with hashElem1

	// Should have 2 elements: hashElem1 and regularElem
	if pq.Len() != 2 {
		t.Errorf("Expected 2 elements with mixed types, got %d", pq.Len())
	}
}

func TestDedupPriorityQueue_EmptyHeapOperations(t *testing.T) {
	// Create an empty priority queue
	pq := NewPriorityQueue(func(a, b interface{}) int {
		return a.(*testHashableElement).val - b.(*testHashableElement).val
	}, false)

	// Test operations on empty heap
	if pq.Len() != 0 {
		t.Errorf("Expected empty heap to have length 0, got %d", pq.Len())
	}

	if pq.Peek() != nil {
		t.Error("Expected Peek() on empty heap to return nil")
	}

	// Test that Pop panics on empty heap
	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected Pop() on empty heap to panic")
		}
	}()
	heap.Pop(pq)
}

func TestDedupPriorityQueue_PriorityOrdering(t *testing.T) {
	// Test that deduplication doesn't affect priority ordering
	pq := NewPriorityQueue(func(a, b interface{}) int {
		return a.(*testHashableElement).val - b.(*testHashableElement).val
	}, false)

	// Add elements in random order
	elem1 := &testHashableElement{id: "high", val: 30}
	elem2 := &testHashableElement{id: "low", val: 10}
	elem3 := &testHashableElement{id: "high", val: 40} // Same id as elem1, should be deduplicated
	elem4 := &testHashableElement{id: "medium", val: 20}

	heap.Push(pq, elem1)
	heap.Push(pq, elem2)
	heap.Push(pq, elem3) // Should be deduplicated
	heap.Push(pq, elem4)

	// Should have 3 elements (elem3 deduplicated)
	if pq.Len() != 3 {
		t.Errorf("Expected 3 elements after deduplication, got %d", pq.Len())
	}

	// Pop elements and verify they come out in priority order
	expectedOrder := []int{10, 20, 30} // elem2, elem4, elem1
	for i, expectedVal := range expectedOrder {
		if pq.Len() == 0 {
			t.Errorf("Expected more elements, but heap is empty at position %d", i)
			break
		}
		popped := heap.Pop(pq).(*testHashableElement)
		if popped.val != expectedVal {
			t.Errorf("Expected value %d at position %d, got %d", expectedVal, i, popped.val)
		}
	}
}

func TestDedupPriorityQueue_LargeScaleDeduplication(t *testing.T) {
	// Test deduplication with many elements
	pq := NewPriorityQueue(func(a, b interface{}) int {
		return a.(*testHashableElement).val - b.(*testHashableElement).val
	}, false)

	// Add 100 elements, but only 10 unique ids
	for i := 0; i < 100; i++ {
		id := "id" + string(rune(i%10+'0')) // id0, id1, ..., id9, id0, id1, ...
		elem := &testHashableElement{id: id, val: i}
		heap.Push(pq, elem)
	}

	// Should have only 10 unique elements
	if pq.Len() != 10 {
		t.Errorf("Expected 10 unique elements, got %d", pq.Len())
	}

	// Verify all remaining elements have unique ids
	seen := make(map[string]bool)
	for pq.Len() > 0 {
		elem := heap.Pop(pq).(*testHashableElement)
		if seen[elem.id] {
			t.Errorf("Found duplicate id %s in final heap", elem.id)
		}
		seen[elem.id] = true
	}

	// Should have seen exactly 10 unique ids
	if len(seen) != 10 {
		t.Errorf("Expected 10 unique ids, got %d", len(seen))
	}
}

func BenchmarkDedupPriorityQueue_Push(b *testing.B) {
	pq := NewPriorityQueue(func(a, b interface{}) int {
		return a.(*testHashableElement).val - b.(*testHashableElement).val
	}, false)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		elem := &testHashableElement{id: "benchmark", val: i}
		heap.Push(pq, elem)
	}
}

func BenchmarkDedupPriorityQueue_Pop(b *testing.B) {
	pq := NewPriorityQueue(func(a, b interface{}) int {
		return a.(*testHashableElement).val - b.(*testHashableElement).val
	}, false)

	// Pre-populate with unique elements
	for i := 0; i < b.N; i++ {
		elem := &testHashableElement{id: fmt.Sprintf("unique_%d", i), val: i}
		heap.Push(pq, elem)
	}

	b.ResetTimer()
	for i := 0; i < b.N && pq.Len() > 0; i++ {
		heap.Pop(pq)
	}
}
