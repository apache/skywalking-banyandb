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

// DedupPriorityQueue implements heap.Interface.
// DedupPriorityQueue is not thread-safe.
type DedupPriorityQueue struct {
	comparator      utils.Comparator
	cache           map[Element]struct{}
	Items           []Element
	allowDuplicates bool
}

// NewPriorityQueue returns a new DedupPriorityQueue.
func NewPriorityQueue(comparator utils.Comparator, allowDuplicates bool) *DedupPriorityQueue {
	return &DedupPriorityQueue{
		comparator:      comparator,
		Items:           make([]Element, 0),
		cache:           make(map[Element]struct{}),
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
		// use mutex to protect cache and items
		// check existence
		if _, ok := pq.cache[item]; ok {
			return
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
