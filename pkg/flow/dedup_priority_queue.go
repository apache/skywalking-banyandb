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

	"github.com/emirpasic/gods/utils"
)

var _ heap.Interface = (*DedupPriorityQueue)(nil)

// Element represents an item in the DedupPriorityQueue.
type Element interface {
	GetIndex() int
	SetIndex(int)
}

// HashableElement represents an element that can be hashed and compared for equality.
type HashableElement interface {
	Element
	// Hash returns a hash value for this element
	Hash() uint64
	// Equal compares this element with another for content equality
	Equal(HashableElement) bool
}

// DedupPriorityQueue implements heap.Interface.
// DedupPriorityQueue is not thread-safe.
type DedupPriorityQueue struct {
	comparator      utils.Comparator
	cache           map[Element]struct{}
	hashCache       map[uint64][]HashableElement // For content-based deduplication
	Items           []Element
	allowDuplicates bool
}

// NewPriorityQueue returns a new DedupPriorityQueue.
func NewPriorityQueue(comparator utils.Comparator, allowDuplicates bool) *DedupPriorityQueue {
	return &DedupPriorityQueue{
		comparator:      comparator,
		Items:           make([]Element, 0),
		cache:           make(map[Element]struct{}),
		hashCache:       make(map[uint64][]HashableElement),
		allowDuplicates: allowDuplicates,
	}
}

// Len returns the DedupPriorityQueue length.
func (pq *DedupPriorityQueue) Len() int { return len(pq.Items) }

// Less is the items less comparator.
func (pq *DedupPriorityQueue) Less(i, j int) bool {
	return pq.comparator(pq.Items[i], pq.Items[j]) < 0
}

// Swap exchanges indexes of the items.
func (pq *DedupPriorityQueue) Swap(i, j int) {
	if i < 0 || i >= len(pq.Items) || j < 0 || j >= len(pq.Items) {
		panic("index out of range in DedupPriorityQueue.Swap")
	}
	pq.Items[i], pq.Items[j] = pq.Items[j], pq.Items[i]
	pq.Items[i].SetIndex(i)
	pq.Items[j].SetIndex(j)
}

// Push implements heap.Interface.Push.
// Appends an item to the DedupPriorityQueue.
func (pq *DedupPriorityQueue) Push(x interface{}) {
	item := x.(Element)
	// if duplicates is not allowed
	if !pq.allowDuplicates {
		// Check for reference-based duplicates first
		if _, ok := pq.cache[item]; ok {
			return
		}

		// Check for content-based duplicates if the item implements HashableElement
		if hashableItem, ok := item.(HashableElement); ok {
			hash := hashableItem.Hash()
			if existingItems, exists := pq.hashCache[hash]; exists {
				// Check if any existing item has the same content
				for _, existing := range existingItems {
					if hashableItem.Equal(existing) {
						return // Duplicate found, don't add
					}
				}
				// No duplicate found, add to hash cache
				pq.hashCache[hash] = append(pq.hashCache[hash], hashableItem)
			} else {
				// First item with this hash
				pq.hashCache[hash] = []HashableElement{hashableItem}
			}
		}

		pq.cache[item] = struct{}{}
	}
	n := len(pq.Items)
	item.SetIndex(n)
	pq.Items = append(pq.Items, item)
}

// Pop implements heap.Interface.Pop.
// Removes and returns the Len() - 1 element.
func (pq *DedupPriorityQueue) Pop() interface{} {
	n := len(pq.Items)
	item := pq.Items[n-1]
	item.SetIndex(-1) // for safety
	delete(pq.cache, item)

	// Clean up hash cache if item implements HashableElement
	if hashableItem, ok := item.(HashableElement); ok {
		hash := hashableItem.Hash()
		if existingItems, exists := pq.hashCache[hash]; exists {
			// Remove the specific item from the hash cache
			for i, existing := range existingItems {
				if hashableItem.Equal(existing) {
					pq.hashCache[hash] = append(existingItems[:i], existingItems[i+1:]...)
					if len(pq.hashCache[hash]) == 0 {
						delete(pq.hashCache, hash)
					}
					break
				}
			}
		}
	}

	pq.Items = pq.Items[0 : n-1]
	return item
}

// Peek returns the first item of the DedupPriorityQueue without removing it.
func (pq *DedupPriorityQueue) Peek() Element {
	if len(pq.Items) > 0 {
		return (pq.Items)[0]
	}
	return nil
}

// ReplaceLowest replaces the lowest item with the newLowest.
func (pq *DedupPriorityQueue) ReplaceLowest(newLowest Element) {
	pq.Items[0] = newLowest
	heap.Fix(pq, 0)
}

// Values returns all items.
func (pq *DedupPriorityQueue) Values() []Element {
	values := make([]Element, pq.Len())
	for pq.Len() > 0 {
		item := heap.Pop(pq).(Element)
		values[pq.Len()] = item
	}
	return values
}
