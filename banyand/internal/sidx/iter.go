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
	currentGroups []*blockGroup
	activeIters   []*partKeyIter
	asc           bool
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

	it.currentGroups = it.currentGroups[:0]
	it.activeIters = it.activeIters[:0]

	it.err = nil
	it.nextBlockNoop = false
	it.asc = false
}

func (it *iter) init(parts []*part, sids []common.SeriesID, minKey, maxKey int64, blockFilter index.Filter, asc bool) {
	it.reset()
	it.parts = append(it.parts[:0], parts...)
	it.asc = asc

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

		pki.init(p, sids, minKey, maxKey, blockFilter, asc)
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

		if group.part == nil {
			group.part = p
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
		if err := it.aggregateGroups(); err != nil {
			it.err = err
			if errors.Is(err, io.EOF) {
				return false
			}
			return false
		}
		return true
	}

	if err := it.advanceActive(); err != nil {
		it.err = err
		if errors.Is(err, io.EOF) {
			return false
		}
		return false
	}

	if err := it.aggregateGroups(); err != nil {
		it.err = err
		if errors.Is(err, io.EOF) {
			return false
		}
		return false
	}

	return true
}

func (it *iter) aggregateGroups() error {
	it.currentGroups = it.currentGroups[:0]
	it.activeIters = it.activeIters[:0]

	for len(it.heap) > 0 {
		top := it.heap[0]
		group := top.currentGroup()
		if group == nil || len(group.blocks) == 0 {
			heap.Pop(&it.heap)
			it.releasePartIter(top)
			continue
		}
		if group.part == nil {
			group.part = top.p
		}
		break
	}

	if len(it.heap) == 0 {
		return io.EOF
	}

	boundary := int64(0)
	firstGroup := true

	for len(it.heap) > 0 {
		top := heap.Pop(&it.heap).(*partKeyIter)
		group := top.currentGroup()
		if group == nil || len(group.blocks) == 0 {
			it.releasePartIter(top)
			continue
		}
		if group.part == nil {
			group.part = top.p
		}

		if firstGroup {
			if it.asc {
				boundary = group.maxKey
			} else {
				boundary = group.minKey
			}
			firstGroup = false
		} else {
			if it.asc {
				if group.minKey > boundary {
					heap.Push(&it.heap, top)
					break
				}
			} else {
				if group.maxKey < boundary {
					heap.Push(&it.heap, top)
					break
				}
			}
		}

		it.currentGroups = append(it.currentGroups, group)
		it.activeIters = append(it.activeIters, top)

		if it.asc {
			if group.maxKey > boundary {
				boundary = group.maxKey
			}
		} else {
			if group.minKey < boundary {
				boundary = group.minKey
			}
		}
	}

	if len(it.currentGroups) == 0 {
		return io.EOF
	}

	return nil
}

func (it *iter) advanceActive() error {
	for _, pki := range it.activeIters {
		for {
			if !pki.nextBlock() {
				err := pki.error()
				it.releasePartIter(pki)
				if err != nil && !errors.Is(err, io.EOF) {
					return err
				}
				break
			}

			group := pki.currentGroup()
			if group == nil || len(group.blocks) == 0 {
				continue
			}
			if group.part == nil {
				group.part = pki.p
			}
			heap.Push(&it.heap, pki)
			break
		}
	}

	it.activeIters = it.activeIters[:0]
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
	asc := true
	if x[i] != nil {
		asc = x[i].asc
	} else if x[j] != nil {
		asc = x[j].asc
	}
	return gi.less(gj, asc)
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
