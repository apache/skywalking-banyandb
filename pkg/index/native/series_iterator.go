// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package native

import (
	"bytes"
	"container/heap"
	"context"
	"errors"

	"github.com/apache/skywalking-banyandb/pkg/index/internal/nativeice"
)

// SeriesIterator enumerates distinct _id terms from one pinned generation in
// lexical order. Values returned by Next are owned by the caller.
type SeriesIterator struct {
	ctx      context.Context
	err      error
	closeErr error
	cursors  []*nativeice.DictionaryTermIterator
	heap     seriesCursorHeap
	closed   bool
}

type seriesCursor struct {
	term  []byte
	index int
}
type seriesCursorHeap []*seriesCursor

func (h seriesCursorHeap) Len() int { return len(h) }
func (h seriesCursorHeap) Less(i, j int) bool {
	return string(h[i].term) < string(h[j].term)
}
func (h seriesCursorHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *seriesCursorHeap) Push(x any)   { *h = append(*h, x.(*seriesCursor)) }
func (h *seriesCursorHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// NewSeriesIterator creates a bounded k-way dictionary merge over the pinned
// generation. Deleted terms remain visible because this is metadata access.
func (g *ReadOnlyGeneration) NewSeriesIterator(ctx context.Context) (*SeriesIterator, error) {
	iterator := &SeriesIterator{ctx: ctx}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if g == nil || g.reader == nil {
		iterator.closed = true
		return iterator, nil
	}
	cursors, err := g.reader.NewDictionaryTermIterators(ctx, identifierField)
	if err != nil {
		return nil, err
	}
	iterator.cursors = cursors
	for index, cursor := range cursors {
		if err := ctx.Err(); err != nil {
			_ = iterator.Close()
			return nil, err
		}
		term, err := cursor.NextTerm()
		if err != nil {
			_ = iterator.Close()
			return nil, err
		}
		if term != nil {
			heap.Push(&iterator.heap, &seriesCursor{index: index, term: term})
		}
	}
	return iterator, nil
}

// Next returns the next distinct identifier. A nil identifier with nil error
// indicates exhaustion. Cancellation and malformed dictionary data are
// returned unchanged (or wrapped by nativeice as corruption).
func (i *SeriesIterator) Next() ([]byte, error) {
	if i == nil || i.closed {
		return nil, nil
	}
	if i.err != nil {
		return nil, i.err
	}
	if err := i.ctx.Err(); err != nil {
		i.err = errors.Join(err, i.Close())
		return nil, i.err
	}
	if len(i.heap) == 0 {
		if err := i.Close(); err != nil {
			i.err = err
			return nil, err
		}
		return nil, nil
	}
	entry := heap.Pop(&i.heap).(*seriesCursor)
	result := append([]byte(nil), entry.term...)
	for len(i.heap) > 0 && bytes.Equal(i.heap[0].term, result) {
		duplicate := heap.Pop(&i.heap).(*seriesCursor)
		if err := i.advance(duplicate); err != nil {
			return nil, err
		}
	}
	if err := i.advance(entry); err != nil {
		return nil, err
	}
	return result, nil
}

func (i *SeriesIterator) advance(entry *seriesCursor) error {
	if err := i.ctx.Err(); err != nil {
		i.err = errors.Join(err, i.Close())
		return i.err
	}
	term, err := i.cursors[entry.index].NextTerm()
	if err != nil {
		i.err = err
		_ = i.Close()
		return err
	}
	if term != nil {
		entry.term = term
		heap.Push(&i.heap, entry)
	}
	return nil
}

// Close releases all dictionary cursors. It is safe to call repeatedly.
func (i *SeriesIterator) Close() error {
	if i == nil {
		return nil
	}
	if i.closed {
		return i.closeErr
	}
	i.closed = true
	for _, cursor := range i.cursors {
		i.closeErr = errors.Join(i.closeErr, cursor.Close())
	}
	i.cursors = nil
	i.heap = nil
	return i.closeErr
}
