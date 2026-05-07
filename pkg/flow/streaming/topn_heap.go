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

package streaming

import (
	"container/heap"

	"github.com/apache/skywalking-banyandb/pkg/flow"
)

// topNHeapEntry represents a single entry in the TopN heap.
type topNHeapEntry[K TopSortKey] struct {
	sortKey K
	records []flow.StreamRecord
	index   int
}

// topNHeap implements heap.Interface for TopN operations.
// The heap ordering is determined by a less function provided at construction.
// For ASC (keep smallest N): use a max-heap (Less returns true if a > b).
// For DESC (keep largest N): use a min-heap (Less returns true if a < b).
type topNHeap[K TopSortKey] struct {
	lessFn  func(a, b K) bool
	index   map[K]*topNHeapEntry[K]
	entries []*topNHeapEntry[K]
}

// newTopNHeap creates a new TopN heap with the given less function.
func newTopNHeap[K TopSortKey](lessFn func(a, b K) bool) *topNHeap[K] {
	return &topNHeap[K]{
		entries: make([]*topNHeapEntry[K], 0),
		index:   make(map[K]*topNHeapEntry[K]),
		lessFn:  lessFn,
	}
}

// Len returns the number of entries in the heap.
func (h *topNHeap[K]) Len() int {
	return len(h.entries)
}

// Less compares two entries based on their sort keys.
func (h *topNHeap[K]) Less(i, j int) bool {
	return h.lessFn(h.entries[i].sortKey, h.entries[j].sortKey)
}

// Swap swaps two entries in the heap.
func (h *topNHeap[K]) Swap(i, j int) {
	h.entries[i], h.entries[j] = h.entries[j], h.entries[i]
	h.entries[i].index = i
	h.entries[j].index = j
}

// Push adds an entry to the heap.
func (h *topNHeap[K]) Push(x interface{}) {
	entry := x.(*topNHeapEntry[K])
	entry.index = len(h.entries)
	h.entries = append(h.entries, entry)
	h.index[entry.sortKey] = entry
}

// Pop removes and returns the root entry from the heap.
func (h *topNHeap[K]) Pop() interface{} {
	old := h.entries
	n := len(old)
	entry := old[n-1]
	old[n-1] = nil
	entry.index = -1
	h.entries = old[0 : n-1]
	delete(h.index, entry.sortKey)
	return entry
}

// peek returns the root entry (worst candidate) without removing it.
// Returns nil if the heap is empty.
func (h *topNHeap[K]) peek() *topNHeapEntry[K] {
	if len(h.entries) == 0 {
		return nil
	}
	return h.entries[0]
}

// get finds an entry by sort key in O(1).
// Returns nil if not found.
func (h *topNHeap[K]) get(sortKey K) *topNHeapEntry[K] {
	return h.index[sortKey]
}

// put adds a record to the entry with the given sort key.
// If the entry exists, the record is appended to its records.
// Otherwise, a new entry is created and pushed onto the heap.
func (h *topNHeap[K]) put(sortKey K, record flow.StreamRecord) {
	entry, found := h.index[sortKey]
	if found {
		entry.records = append(entry.records, record)
	} else {
		newEntry := &topNHeapEntry[K]{
			sortKey: sortKey,
			records: []flow.StreamRecord{record},
		}
		heap.Push(h, newEntry)
	}
}

// remove removes the entry with the given sort key from the heap.
// Does nothing if the key is not found.
func (h *topNHeap[K]) remove(sortKey K) {
	entry, found := h.index[sortKey]
	if !found {
		return
	}
	heap.Remove(h, entry.index)
}

// update replaces the records for the entry with the given sort key.
// If the entry does not exist, it does nothing.
func (h *topNHeap[K]) update(sortKey K, records []flow.StreamRecord) {
	entry, found := h.index[sortKey]
	if !found {
		return
	}
	entry.records = records
}

// popWorst removes and returns the root entry (worst candidate).
// Returns nil if the heap is empty.
func (h *topNHeap[K]) popWorst() *topNHeapEntry[K] {
	if len(h.entries) == 0 {
		return nil
	}
	return heap.Pop(h).(*topNHeapEntry[K])
}

// iterAll returns all entries in heap storage order.
// The returned slice should not be modified.
func (h *topNHeap[K]) iterAll() []*topNHeapEntry[K] {
	return h.entries
}

// totalRecords returns the total number of records across all entries.
func (h *topNHeap[K]) totalRecords() int {
	count := 0
	for _, entry := range h.entries {
		count += len(entry.records)
	}
	return count
}
