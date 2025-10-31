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

package sidx

import (
	"container/heap"
	"errors"
	"fmt"
	"io"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/pool"
)

type iter struct {
	err           error
	parts         []*part
	partIters     []*partKeyIter
	heap          partKeyIterHeap
	nextBlockNoop bool
}

func (it *iter) reset() {
	for i := range it.parts {
		it.parts[i] = nil
	}
	it.parts = it.parts[:0]

	for i := range it.partIters {
		if it.partIters[i] != nil {
			releasePartKeyIter(it.partIters[i])
			it.partIters[i] = nil
		}
	}
	it.partIters = it.partIters[:0]

	for i := range it.heap {
		it.heap[i] = nil
	}
	it.heap = it.heap[:0]

	it.err = nil
	it.nextBlockNoop = false
}

func (it *iter) init(parts []*part, sids []common.SeriesID, minKey, maxKey int64, blockFilter index.Filter) {
	it.reset()
	it.parts = append(it.parts[:0], parts...)

	if cap(it.partIters) < len(parts) {
		it.partIters = make([]*partKeyIter, len(parts))
	} else {
		it.partIters = it.partIters[:len(parts)]
		for i := range it.partIters {
			if it.partIters[i] != nil {
				releasePartKeyIter(it.partIters[i])
				it.partIters[i] = nil
			}
		}
	}

	it.heap = it.heap[:0]

	for i, p := range parts {
		pki := generatePartKeyIter()
		it.partIters[i] = pki

		pki.init(p, sids, minKey, maxKey, blockFilter)
		if err := pki.error(); err != nil {
			if !errors.Is(err, io.EOF) {
				releasePartKeyIter(pki)
				it.partIters[i] = nil
				it.err = fmt.Errorf("cannot initialize sidx iteration: %w", err)
				return
			}
			releasePartKeyIter(pki)
			it.partIters[i] = nil
			continue
		}

		if !pki.nextBlock() {
			if err := pki.error(); err != nil && !errors.Is(err, io.EOF) {
				releasePartKeyIter(pki)
				it.partIters[i] = nil
				it.err = fmt.Errorf("cannot initialize sidx iteration: %w", err)
				return
			}
			releasePartKeyIter(pki)
			it.partIters[i] = nil
			continue
		}

		group := pki.currentGroup()
		if group == nil || len(group.blocks) == 0 {
			releasePartKeyIter(pki)
			it.partIters[i] = nil
			continue
		}

		it.heap = append(it.heap, pki)
	}

	if len(it.heap) == 0 {
		it.err = io.EOF
		return
	}

	heap.Init(&it.heap)
	it.nextBlockNoop = true
}

func (it *iter) nextBlock() bool {
	if it.err != nil {
		return false
	}
	if it.nextBlockNoop {
		it.nextBlockNoop = false
		return true
	}

	it.err = it.next()
	if it.err != nil {
		if errors.Is(it.err, io.EOF) {
			it.err = fmt.Errorf("cannot obtain the next block to search in the partition: %w", it.err)
		}
		return false
	}
	return true
}

func (it *iter) next() error {
	if len(it.heap) == 0 {
		return io.EOF
	}

	head := it.heap[0]

	if !head.nextBlock() {
		err := head.error()
		it.releasePartIter(head)
		heap.Pop(&it.heap)
		if err != nil && !errors.Is(err, io.EOF) {
			return err
		}
		if len(it.heap) == 0 {
			return io.EOF
		}
		return nil
	}

	group := head.currentGroup()
	if group == nil || len(group.blocks) == 0 {
		it.releasePartIter(head)
		heap.Pop(&it.heap)
		if len(it.heap) == 0 {
			return io.EOF
		}
		return nil
	}

	heap.Fix(&it.heap, 0)
	return nil
}

func (it *iter) Error() error {
	if errors.Is(it.err, io.EOF) {
		return nil
	}
	return it.err
}

func generateIter() *iter {
	v := iterPool.Get()
	if v == nil {
		return &iter{}
	}
	return v
}

func releaseIter(it *iter) {
	it.reset()
	iterPool.Put(it)
}

var iterPool = pool.Register[*iter]("sidx-iter")

type partKeyIterHeap []*partKeyIter

func (pih *partKeyIterHeap) Len() int {
	return len(*pih)
}

func (pih *partKeyIterHeap) Less(i, j int) bool {
	x := *pih
	gi := x[i].currentGroup()
	gj := x[j].currentGroup()
	if gi == nil {
		return false
	}
	if gj == nil {
		return true
	}
	return gi.less(gj)
}

func (pih *partKeyIterHeap) Swap(i, j int) {
	x := *pih
	x[i], x[j] = x[j], x[i]
}

func (pih *partKeyIterHeap) Push(x any) {
	*pih = append(*pih, x.(*partKeyIter))
}

func (pih *partKeyIterHeap) Pop() any {
	a := *pih
	v := a[len(a)-1]
	*pih = a[:len(a)-1]
	return v
}

func (it *iter) releasePartIter(p *partKeyIter) {
	if p == nil {
		return
	}
	for i := range it.partIters {
		if it.partIters[i] == p {
			releasePartKeyIter(p)
			it.partIters[i] = nil
			return
		}
	}
	releasePartKeyIter(p)
}
