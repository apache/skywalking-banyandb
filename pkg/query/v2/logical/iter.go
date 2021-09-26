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
	"sort"

	"github.com/apache/skywalking-banyandb/banyand/tsdb"
)

var _ ItemIterator = (*itemIter)(nil)

type ItemIterator interface {
	HasNext() bool
	Next() tsdb.Item
}

// container contains both iter and its current item
type container struct {
	item tsdb.Item
	iter tsdb.Iterator
}

type itemIter struct {
	// c is the comparator to sort items
	c comparator
	// iters is the list of initial Iterator
	iters []tsdb.Iterator
	// deq is the deque of the container
	// 1. When we push a new container, we can normally append it to the tail of the deq,
	//    and then sort the whole slice
	// 2. When we pop a new container, we can just pop out the first element in the deq.
	//    The rest of the slice is still sorted.
	deq []*container
}

func NewItemIter(iters []tsdb.Iterator, c comparator) ItemIterator {
	iT := &itemIter{
		c:     c,
		iters: iters,
		deq:   make([]*container, 0),
	}
	iT.init()
	return iT
}

// init function MUST be called while initialization.
// 1. Move all iterator to the first item by invoking their Next.
// 2. Load all first items into a slice.
func (it *itemIter) init() {
	for _, iter := range it.iters {
		it.pushIterator(iter)
	}
}

// pushIterator pushes the given iterator into the underlying deque.
// Status will be immediately checked if the Iterator has a next value.
// 1 - If not, it will be close at once and will not be added to the slice,
//     which means inactive iterator does not exist in the deq.
// 2 - If so, it will be wrapped into a container and push to the deq.
//     Then we call SliceStable sort to sort the deq.
func (it *itemIter) pushIterator(iter tsdb.Iterator) {
	if !iter.Next() {
		_ = iter.Close()
		return
	}
	it.deq = append(it.deq, &container{
		item: iter.Val(),
		iter: iter,
	})
	sort.SliceStable(it.deq, func(i, j int) bool {
		return it.c(it.deq[i].item, it.deq[j].item)
	})
}

func (it *itemIter) HasNext() bool {
	return len(it.deq) > 0
}

func (it *itemIter) Next() tsdb.Item {
	var c *container
	// 3. Pop up the minimal item through the order value
	c, it.deq = it.deq[0], it.deq[1:]

	// 4. Move the iterator whose value is popped in step 3, push the next value of this iterator into the slice.
	it.pushIterator(c.iter)

	return c.item
}
