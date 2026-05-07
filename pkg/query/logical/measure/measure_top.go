// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package measure

import (
	"container/heap"
	"fmt"
	"sort"

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	"github.com/apache/skywalking-banyandb/pkg/flow/streaming"
)

// TopElement seals a sortable value and its data point which this value belongs to.
type TopElement[K streaming.TopSortKey] struct {
	idp   *measurev1.InternalDataPoint
	value K
}

// NewTopElement returns a TopElement.
func NewTopElement[K streaming.TopSortKey](idp *measurev1.InternalDataPoint, value K) TopElement[K] {
	return TopElement[K]{
		idp:   idp,
		value: value,
	}
}

// Val returns the sortable value.
func (e TopElement[K]) Val() K {
	return e.value
}

type topSortedList[K streaming.TopSortKey] struct {
	elements []TopElement[K]
	reverted bool
}

func (h topSortedList[K]) Len() int {
	return len(h.elements)
}

func (h topSortedList[K]) Less(i, j int) bool {
	if h.reverted {
		return h.elements[i].value < h.elements[j].value
	}
	return h.elements[i].value > h.elements[j].value
}

func (h *topSortedList[K]) Swap(i, j int) {
	h.elements[i], h.elements[j] = h.elements[j], h.elements[i]
}

type topHeap[K streaming.TopSortKey] struct {
	elements []TopElement[K]
	reverted bool
}

func (h topHeap[K]) Len() int {
	return len(h.elements)
}

func (h topHeap[K]) Less(i, j int) bool {
	if h.reverted {
		return h.elements[i].value > h.elements[j].value
	}
	return h.elements[i].value < h.elements[j].value
}

func (h *topHeap[K]) Swap(i, j int) {
	h.elements[i], h.elements[j] = h.elements[j], h.elements[i]
}

func (h *topHeap[K]) Push(x interface{}) {
	h.elements = append(h.elements, x.(TopElement[K]))
}

func (h *topHeap[K]) Pop() interface{} {
	var e TopElement[K]
	e, h.elements = h.elements[len(h.elements)-1], h.elements[:len(h.elements)-1]
	return e
}

// TopQueue is a sortable queue only keeps top-n members when pushed new elements.
type TopQueue[K streaming.TopSortKey] struct {
	th topHeap[K]
	n  int
}

// NewTopQueue returns a new TopQueue.
func NewTopQueue[K streaming.TopSortKey](n int, reverted bool) *TopQueue[K] {
	return &TopQueue[K]{
		n: n,
		th: topHeap[K]{
			elements: make([]TopElement[K], 0, n),
			reverted: reverted,
		},
	}
}

// Insert pushes a new element to the queue.
// It returns true if the element are accepted by the queue,
// returns false if it's evicted.
func (s *TopQueue[K]) Insert(element TopElement[K]) bool {
	if len(s.th.elements) < s.n {
		heap.Push(&s.th, element)
		return true
	}
	minElement := heap.Pop(&s.th).(TopElement[K])
	if s.th.reverted {
		if minElement.value < element.value {
			heap.Push(&s.th, minElement)
			return false
		}
	} else {
		if minElement.value > element.value {
			heap.Push(&s.th, minElement)
			return false
		}
	}
	heap.Push(&s.th, element)
	return true
}

// Purge resets the queue.
func (s *TopQueue[K]) Purge() {
	s.th.elements = s.th.elements[:0]
}

// Elements returns all elements accepted by the queue.
func (s *TopQueue[K]) Elements() []TopElement[K] {
	l := &topSortedList[K]{
		elements: append([]TopElement[K](nil), s.th.elements...),
		reverted: s.th.reverted,
	}
	sort.Sort(l)
	return l.elements
}

// Strings shows the string represent.
func (s TopQueue[K]) String() string {
	if s.th.reverted {
		return fmt.Sprintf("bottom(%d)", s.n)
	}
	return fmt.Sprintf("top(%d)", s.n)
}

// Equal reports whether s and other have the queue's max acceptable number and sorting order.
func (s *TopQueue[K]) Equal(other *TopQueue[K]) bool {
	return s.th.reverted == other.th.reverted && s.n == other.n
}
