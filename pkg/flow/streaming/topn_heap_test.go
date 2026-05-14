// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for additional
// information regarding copyright ownership. Apache Software Foundation (ASF)
// licenses this file to you under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package streaming

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/flow"
)

func newInt64MaxHeap() *topNHeap[int64] {
	return newTopNHeap[int64](func(a, b int64) bool { return a > b })
}

func newInt64MinHeap() *topNHeap[int64] {
	return newTopNHeap[int64](func(a, b int64) bool { return a < b })
}

func newFloat64MaxHeap() *topNHeap[float64] {
	return newTopNHeap[float64](func(a, b float64) bool { return a > b })
}

func TestTopNHeap_PutAndGet(t *testing.T) {
	h := newInt64MaxHeap()
	require.Zero(t, h.Len())

	h.put(10, flow.NewStreamRecord("a", 1))
	require.Equal(t, 1, h.Len())

	entry := h.get(10)
	require.NotNil(t, entry)
	require.Equal(t, int64(10), entry.sortKey)
	require.Len(t, entry.records, 1)

	require.Nil(t, h.get(999))
}

func TestTopNHeap_PutExistingKeyAppendsRecords(t *testing.T) {
	h := newInt64MaxHeap()

	h.put(10, flow.NewStreamRecord("a", 1))
	h.put(10, flow.NewStreamRecord("b", 2))
	require.Equal(t, 1, h.Len())

	entry := h.get(10)
	require.NotNil(t, entry)
	require.Len(t, entry.records, 2)
	require.Equal(t, 2, h.totalRecords())
}

func TestTopNHeap_Remove(t *testing.T) {
	h := newInt64MaxHeap()

	h.put(10, flow.NewStreamRecord("a", 1))
	h.put(20, flow.NewStreamRecord("b", 2))
	require.Equal(t, 2, h.Len())

	h.remove(10)
	require.Equal(t, 1, h.Len())
	require.Nil(t, h.get(10))
	require.NotNil(t, h.get(20))

	// Removing non-existent key does nothing.
	h.remove(999)
	require.Equal(t, 1, h.Len())
}

func TestTopNHeap_Update(t *testing.T) {
	h := newInt64MaxHeap()

	h.put(10, flow.NewStreamRecord("a", 1))
	newRecords := []flow.StreamRecord{flow.NewStreamRecord("c", 3)}
	h.update(10, newRecords)

	entry := h.get(10)
	require.NotNil(t, entry)
	require.Equal(t, newRecords, entry.records)

	// Updating non-existent key does nothing.
	h.update(999, newRecords)
	require.Equal(t, 1, h.Len())
}

func TestTopNHeap_PopWorst_MaxHeap(t *testing.T) {
	// Max-heap: root is the largest, popWorst removes largest.
	h := newInt64MaxHeap()
	h.put(10, flow.NewStreamRecord("a", 1))
	h.put(30, flow.NewStreamRecord("b", 2))
	h.put(20, flow.NewStreamRecord("c", 3))

	worst := h.popWorst()
	require.NotNil(t, worst)
	require.Equal(t, int64(30), worst.sortKey)
	require.Equal(t, 2, h.Len())
}

func TestTopNHeap_PopWorst_MinHeap(t *testing.T) {
	// Min-heap: root is the smallest, popWorst removes smallest.
	h := newInt64MinHeap()
	h.put(10, flow.NewStreamRecord("a", 1))
	h.put(30, flow.NewStreamRecord("b", 2))
	h.put(20, flow.NewStreamRecord("c", 3))

	worst := h.popWorst()
	require.NotNil(t, worst)
	require.Equal(t, int64(10), worst.sortKey)
	require.Equal(t, 2, h.Len())
}

func TestTopNHeap_PopWorst_Empty(t *testing.T) {
	h := newInt64MaxHeap()
	require.Nil(t, h.popWorst())
}

func TestTopNHeap_Peek(t *testing.T) {
	h := newInt64MaxHeap()
	require.Nil(t, h.peek())

	h.put(10, flow.NewStreamRecord("a", 1))
	h.put(30, flow.NewStreamRecord("b", 2))
	h.put(20, flow.NewStreamRecord("c", 3))

	// Max-heap peek should return the largest.
	require.Equal(t, int64(30), h.peek().sortKey)
	require.Equal(t, 3, h.Len(), "peek should not remove")
}

func TestTopNHeap_TotalRecords(t *testing.T) {
	h := newInt64MaxHeap()
	require.Zero(t, h.totalRecords())

	h.put(10, flow.NewStreamRecord("a", 1))
	h.put(10, flow.NewStreamRecord("b", 2))
	h.put(20, flow.NewStreamRecord("c", 3))
	require.Equal(t, 3, h.totalRecords())
}

func TestTopNHeap_IterAll(t *testing.T) {
	h := newInt64MaxHeap()
	h.put(10, flow.NewStreamRecord("a", 1))
	h.put(20, flow.NewStreamRecord("b", 2))

	entries := h.iterAll()
	require.Len(t, entries, 2)

	keys := make([]int64, 0, len(entries))
	for _, entry := range entries {
		keys = append(keys, entry.sortKey)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	require.Equal(t, []int64{10, 20}, keys)
}

func TestTopNHeap_Float64(t *testing.T) {
	h := newFloat64MaxHeap()
	h.put(1.5, flow.NewStreamRecord("a", 1))
	h.put(3.14, flow.NewStreamRecord("b", 2))
	h.put(2.71, flow.NewStreamRecord("c", 3))

	worst := h.popWorst()
	require.NotNil(t, worst)
	require.Equal(t, 3.14, worst.sortKey)
	require.Equal(t, 2, h.Len())
}

func TestTopNHeap_HeapPropertyPreserved(t *testing.T) {
	// Insert many elements and verify that repeated popWorst returns in sorted order.
	h := newInt64MinHeap()
	values := []int64{50, 30, 10, 40, 20, 60, 70, 80, 90, 100}
	for idx, v := range values {
		h.put(v, flow.NewStreamRecord(idx, int64(idx)))
	}

	var popped []int64
	for h.Len() > 0 {
		popped = append(popped, h.popWorst().sortKey)
	}
	require.IsIncreasing(t, popped, "min-heap popWorst should return in ascending order")
}

func TestTopNHeap_RemoveMiddleElement(t *testing.T) {
	h := newInt64MinHeap()
	h.put(10, flow.NewStreamRecord("a", 1))
	h.put(20, flow.NewStreamRecord("b", 2))
	h.put(30, flow.NewStreamRecord("c", 3))

	h.remove(20)
	require.Equal(t, 2, h.Len())
	require.Nil(t, h.get(20))

	// Verify remaining elements are still valid.
	var remaining []int64
	for h.Len() > 0 {
		remaining = append(remaining, h.popWorst().sortKey)
	}
	require.Equal(t, []int64{10, 30}, remaining)
}
