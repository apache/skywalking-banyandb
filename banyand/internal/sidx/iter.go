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
	err       error
	curBlock  *blockMetadata
	curPart   *part
	parts     []*part
	partIters []*partKeyIter
	heap      partKeyIterHeap
	asc       bool
}

func (it *iter) releaseCurBlock() {
	if it.curBlock != nil {
		releaseBlockMetadata(it.curBlock)
		it.curBlock = nil
	}
	it.curPart = nil
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

	it.releaseCurBlock()

	it.err = nil
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

		bm, _ := pki.current()
		if bm == nil {
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
}

func (it *iter) nextBlock() bool {
	if it.err != nil {
		return false
	}

	it.releaseCurBlock()

	if len(it.heap) == 0 {
		it.err = io.EOF
		return false
	}

	pki := heap.Pop(&it.heap).(*partKeyIter)
	bm, p := pki.current()
	if bm == nil {
		it.releasePartIter(pki)
		it.err = fmt.Errorf("partKeyIter has no current block")
		return false
	}

	it.curBlock = generateBlockMetadata()
	it.curBlock.copyFrom(bm)
	it.curPart = p

	if !pki.nextBlock() {
		err := pki.error()
		it.releasePartIter(pki)
		if err != nil && !errors.Is(err, io.EOF) {
			it.releaseCurBlock()
			it.err = err
			return false
		}
	} else {
		heap.Push(&it.heap, pki)
	}

	return true
}

func (it *iter) current() (*blockMetadata, *part) {
	return it.curBlock, it.curPart
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
	bmi, _ := x[i].current()
	bmj, _ := x[j].current()
	if bmi == nil {
		return false
	}
	if bmj == nil {
		return true
	}
	asc := true
	if x[i] != nil {
		asc = x[i].asc
	} else if x[j] != nil {
		asc = x[j].asc
	}
	if asc {
		return bmi.lessByKey(bmj)
	}
	return bmj.lessByKey(bmi)
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
