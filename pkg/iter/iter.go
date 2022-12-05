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

// Package iter implement a generic Iterator.
package iter

// An Iterator is a stream of items of some type.
type Iterator[T any] interface {
	// Next checks whether if the iteration has more elements and
	// returns the next one if exists.
	Next() (T, bool)
}

// FromSlice creates a new iterator which returns all items from the slice starting at index 0 until
// all items are consumed.
func FromSlice[T any](slice []T) Iterator[T] {
	return &sliceIterator[T]{slice: slice}
}

type sliceIterator[T any] struct {
	slice []T
}

func (iter *sliceIterator[T]) Next() (T, bool) {
	if len(iter.slice) == 0 {
		var zero T
		return zero, false
	}
	item := iter.slice[0]
	iter.slice = iter.slice[1:]
	return item, true
}

// Map returns a new iterator which applies a function to all items from the input iterator which
// are subsequently returned.
//
// The mapping function should not mutate the state outside its scope.
func Map[T any, O any](from Iterator[T], mapFunc func(T) O) Iterator[O] {
	return &mapIterator[T, O]{from: from, mapFunc: mapFunc}
}

type mapIterator[T any, O any] struct {
	from    Iterator[T]
	mapFunc func(T) O
}

func (iter *mapIterator[T, O]) Next() (O, bool) {
	item, ok := iter.from.Next()
	if !ok {
		var zero O
		return zero, false
	}
	mapped := iter.mapFunc(item)
	return mapped, true
}

// Flatten applies a function to all items of the specified iterator, returning an iterator for each
// item. The resulting iterators are then concatenated into a single iterator.
func Flatten[T any](from Iterator[Iterator[T]]) Iterator[T] {
	return &flattenIterator[T]{from: from}
}

type flattenIterator[T any] struct {
	from Iterator[Iterator[T]]
	head Iterator[T]
}

func (iter *flattenIterator[T]) Next() (T, bool) {
	for {
		if iter.head == nil {
			item, ok := iter.from.Next()
			if !ok {
				var zero T
				return zero, false
			}
			iter.head = item
		}
		item, ok := iter.head.Next()
		if ok {
			return item, true
		}
		iter.head = nil
	}
}

// Empty returns an iterator that never returns anything.
func Empty[T any]() Iterator[T] {
	return emptyIterator[T]{}
}

type emptyIterator[T any] struct{}

func (emptyIterator[T]) Next() (T, bool) {
	var zero T
	return zero, false
}
