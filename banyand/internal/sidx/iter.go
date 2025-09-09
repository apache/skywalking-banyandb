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
	piPool        []partIter
	piHeap        partIterHeap
	nextBlockNoop bool
}

func (it *iter) reset() {
	for i := range it.parts {
		it.parts[i] = nil
	}
	it.parts = it.parts[:0]

	for i := range it.piPool {
		it.piPool[i].reset()
	}
	it.piPool = it.piPool[:0]

	for i := range it.piHeap {
		it.piHeap[i] = nil
	}
	it.piHeap = it.piHeap[:0]

	it.err = nil
	it.nextBlockNoop = false
}

func (it *iter) init(bma *blockMetadataArray, parts []*part, sids []common.SeriesID, minKey, maxKey int64, blockFilter index.Filter) {
	it.reset()
	it.parts = parts

	if n := len(it.parts) - cap(it.piPool); n > 0 {
		it.piPool = append(it.piPool[:cap(it.piPool)], make([]partIter, n)...)
	}
	it.piPool = it.piPool[:len(it.parts)]
	for i, p := range it.parts {
		it.piPool[i].init(bma, p, sids, minKey, maxKey, blockFilter)
	}

	it.piHeap = it.piHeap[:0]
	for i := range it.piPool {
		ps := &it.piPool[i]
		if !ps.nextBlock() {
			if err := ps.error(); err != nil {
				it.err = fmt.Errorf("cannot initialize sidx iteration: %w", err)
				return
			}
			continue
		}
		it.piHeap = append(it.piHeap, ps)
	}
	if len(it.piHeap) == 0 {
		it.err = io.EOF
		return
	}
	heap.Init(&it.piHeap)
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
	psMin := it.piHeap[0]
	if psMin.nextBlock() {
		heap.Fix(&it.piHeap, 0)
		return nil
	}

	if err := psMin.error(); err != nil {
		return err
	}

	heap.Pop(&it.piHeap)

	if len(it.piHeap) == 0 {
		return io.EOF
	}
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

type partIterHeap []*partIter

func (pih *partIterHeap) Len() int {
	return len(*pih)
}

func (pih *partIterHeap) Less(i, j int) bool {
	x := *pih
	return x[i].curBlock.less(x[j].curBlock)
}

func (pih *partIterHeap) Swap(i, j int) {
	x := *pih
	x[i], x[j] = x[j], x[i]
}

func (pih *partIterHeap) Push(x any) {
	*pih = append(*pih, x.(*partIter))
}

func (pih *partIterHeap) Pop() any {
	a := *pih
	v := a[len(a)-1]
	*pih = a[:len(a)-1]
	return v
}
