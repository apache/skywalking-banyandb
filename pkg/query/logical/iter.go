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

package logical

import (
	"container/heap"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
)

var _ ItemIterator = (*itemIter)(nil)

// ItemIterator allow iterating over a tsdb's series.
type ItemIterator interface {
	HasNext() bool
	Next() tsdb.Item
}

var _ heap.Interface = (*containerHeap)(nil)

// container contains both iter and its current item.
type container struct {
	c    comparator
	item tsdb.Item
	iter tsdb.Iterator
}

type containerHeap []*container

func (h containerHeap) Len() int           { return len(h) }
func (h containerHeap) Less(i, j int) bool { return h[i].c(h[i].item, h[j].item) }
func (h containerHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *containerHeap) Push(x interface{}) {
	*h = append(*h, x.(*container))
}

func (h *containerHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type itemIter struct {
	c     comparator
	h     *containerHeap
	iters []tsdb.Iterator
}

// NewItemIter returns a ItemIterator which mergers several tsdb.Iterator by input sorting order.
func NewItemIter(iters []tsdb.Iterator, sort modelv1.Sort) ItemIterator {
	it := &itemIter{
		c:     createComparator(sort),
		iters: iters,
		h:     &containerHeap{},
	}
	it.init()
	return it
}

// init function MUST be called while initialization.
// 1. Move all iterator to the first item by invoking their Next.
// 2. Load all first items into a slice.
func (it *itemIter) init() {
	for _, iter := range it.iters {
		it.pushIterator(iter)
	}
	// heap initialization
	heap.Init(it.h)
}

// pushIterator pushes the given iterator into the underlying deque.
// Status will be immediately checked if the Iterator has a next value.
// 1 - If not, it will be close at once and will not be added to the slice,
//
//	which means inactive iterator does not exist in the deq.
//
// 2 - If so, it will be wrapped into a container and push to the deq.
//
//	Then we call SliceStable sort to sort the deq.
func (it *itemIter) pushIterator(iter tsdb.Iterator) {
	if !iter.Next() {
		_ = iter.Close()
		return
	}
	heap.Push(it.h, &container{
		item: iter.Val(),
		iter: iter,
		c:    it.c,
	})
}

func (it *itemIter) HasNext() bool {
	return it.h.Len() > 0
}

func (it *itemIter) Next() tsdb.Item {
	// 3. Pop up the minimal item through the order value
	c := heap.Pop(it.h).(*container)

	// 4. Move the iterator whose value is popped in step 3, push the next value of this iterator into the slice.
	it.pushIterator(c.iter)

	return c.item
}
