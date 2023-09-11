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

// Package sort provides a generic iterator that merges multiple sorted iterators.
package sort

import (
	"bytes"
	"container/heap"
	"fmt"
)

// Comparable is an interface that allows sorting of items.
type Comparable interface {
	SortedField() []byte
}

// Iterator is a stream of items of Comparable type.
type Iterator[T Comparable] interface {
	Next() bool
	Val() T
	Close() error
}

type container[T Comparable] struct {
	item T
	iter Iterator[T]
}

type containerHeap[T Comparable] struct {
	items []*container[T]
	desc  bool
}

func (h containerHeap[T]) Len() int {
	return len(h.items)
}

func (h containerHeap[T]) Less(i, j int) bool {
	if h.desc {
		return bytes.Compare(h.items[i].item.SortedField(), h.items[j].item.SortedField()) > 0
	}
	return bytes.Compare(h.items[i].item.SortedField(), h.items[j].item.SortedField()) < 0
}

func (h containerHeap[T]) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
}

func (h *containerHeap[T]) Push(x interface{}) {
	item := x.(*container[T])
	h.items = append(h.items, item)
}

func (h *containerHeap[T]) Pop() interface{} {
	old := h.items
	n := len(old)
	item := old[n-1]
	h.items = old[0 : n-1]
	return item
}

type itemIter[T Comparable] struct {
	curr  T
	h     *containerHeap[T]
	iters []Iterator[T]
}

// NewItemIter returns a new iterator that merges multiple sorted iterators.
func NewItemIter[T Comparable](iters []Iterator[T], desc bool) Iterator[T] {
	var def T
	it := &itemIter[T]{
		iters: iters,
		h:     &containerHeap[T]{items: make([]*container[T], 0), desc: desc},
		curr:  def,
	}
	it.initialize()
	return it
}

func (it *itemIter[T]) initialize() {
	heap.Init(it.h)
	for _, iter := range it.iters {
		if iter.Next() {
			heap.Push(it.h, &container[T]{item: iter.Val(), iter: iter})
		}
	}
}

func (it *itemIter[T]) pushIterator(iter Iterator[T]) {
	if iter.Next() {
		heap.Push(it.h, &container[T]{item: iter.Val(), iter: iter})
	}
}

func (it *itemIter[T]) Next() bool {
	if it.h.Len() == 0 {
		return false
	}
	top := heap.Pop(it.h).(*container[T])
	it.curr = top.item
	it.pushIterator(top.iter)
	return true
}

func (it *itemIter[T]) Val() T {
	return it.curr
}

func (it *itemIter[T]) Close() error {
	var err error
	for _, iter := range it.iters {
		if e := iter.Close(); e != nil {
			fmt.Println("Error closing iterator:", e)
			err = e
		}
	}
	return err
}
