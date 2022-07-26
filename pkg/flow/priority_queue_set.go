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

	"github.com/pkg/errors"
)

var _ heap.Interface = (*PriorityQueue)(nil)

// Element represents an item in the PriorityQueue.
type Element interface {
	GetIndex() int
	SetIndex(int)
	// Compare returns positive number to indicate this object is greater than
	// the other logic, 0 to indicate equality, and negative to indicate
	// less than the other.
	Compare(other Element) int
}

// PriorityQueue implements heap.Interface.
// PriorityQueue is not thread-safe
type PriorityQueue struct {
	Items           []Element
	cache           map[Element]struct{}
	allowDuplicates bool
}

func NewPriorityQueue(allowDuplicates bool) *PriorityQueue {
	return &PriorityQueue{
		Items:           make([]Element, 0),
		cache:           make(map[Element]struct{}),
		allowDuplicates: allowDuplicates,
	}
}

func (pq *PriorityQueue) initCache() error {
	if pq.allowDuplicates || len(pq.Items) == 0 {
		return nil
	}
	for _, elem := range pq.Items {
		if _, ok := pq.cache[elem]; !ok {
			pq.cache[elem] = struct{}{}
		} else {
			return errors.New("duplicated item is not allowed")
		}
	}
	return nil
}

// Len returns the PriorityQueue length.
func (pq *PriorityQueue) Len() int { return len(pq.Items) }

// Less is the items less comparator.
func (pq *PriorityQueue) Less(i, j int) bool {
	return pq.Items[i].Compare(pq.Items[j]) < 0
}

// Swap exchanges indexes of the items.
func (pq *PriorityQueue) Swap(i, j int) {
	pq.Items[i], pq.Items[j] = pq.Items[j], pq.Items[i]
	pq.Items[i].SetIndex(i)
	pq.Items[j].SetIndex(j)
}

// Push implements heap.Interface.Push.
// Appends an item to the PriorityQueue.
func (pq *PriorityQueue) Push(x interface{}) {
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
func (pq *PriorityQueue) Pop() interface{} {
	n := len(pq.Items)
	item := pq.Items[n-1]
	item.SetIndex(-1) // for safety
	delete(pq.cache, item)
	pq.Items = pq.Items[0 : n-1]
	return item
}

// Peek returns the first item of the PriorityQueue without removing it.
func (pq *PriorityQueue) Peek() Element {
	if len(pq.Items) > 0 {
		return (pq.Items)[0]
	}
	return nil
}

// Slice returns a sliced PriorityQueue using the given bounds.
func (pq *PriorityQueue) Slice(start, end int) []Element {
	return pq.Items[start:end]
}

func (pq *PriorityQueue) WithNewItems(items []Element) (*PriorityQueue, error) {
	newPq := &PriorityQueue{
		Items:           items,
		cache:           make(map[Element]struct{}),
		allowDuplicates: pq.allowDuplicates,
	}
	err := newPq.initCache()
	if err != nil {
		return nil, err
	}
	return newPq, nil
}
