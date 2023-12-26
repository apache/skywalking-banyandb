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

package stream

import (
	"container/heap"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

func newTSTable(_ string, _ common.Position, _ *logger.Logger, _ timestamp.TimeRange) (*tsTable, error) {
	return &tsTable{}, nil
}

type tsTable struct {
	index    *elementIndex
	memParts []*partWrapper
	sync.RWMutex
}

func (tst *tsTable) Index() *elementIndex {
	return tst.index
}

func (tst *tsTable) Close() error {
	tst.Lock()
	defer tst.Unlock()
	for _, p := range tst.memParts {
		p.decRef()
	}
	return nil
}

func (tst *tsTable) mustAddDataPoints(dps *dataPoints) {
	if len(dps.seriesIDs) == 0 {
		return
	}

	mp := generateMemPart()
	mp.mustInitFromDataPoints(dps)
	p := openMemPart(mp)

	pw := newMemPartWrapper(mp, p)

	tst.Lock()
	defer tst.Unlock()
	tst.memParts = append(tst.memParts, pw)
}

func (tst *tsTable) getParts(dst []*partWrapper, dstPart []*part, opts queryOptions) ([]*partWrapper, []*part) {
	tst.RLock()
	defer tst.RUnlock()
	for _, p := range tst.memParts {
		pm := p.mp.partMetadata
		if opts.maxTimestamp < pm.MinTimestamp || opts.minTimestamp > pm.MaxTimestamp {
			continue
		}
		p.incRef()
		dst = append(dst, p)
		dstPart = append(dstPart, p.p)
	}
	return dst, dstPart
}

type tstIter struct {
	err           error
	parts         []*part
	piPool        []partIter
	piHeap        partIterHeap
	nextBlockNoop bool
	// fieldIter     index.FieldIterator
}

func (ti *tstIter) reset() {
	for i := range ti.parts {
		ti.parts[i] = nil
	}
	ti.parts = ti.parts[:0]

	for i := range ti.piPool {
		ti.piPool[i].reset()
	}
	ti.piPool = ti.piPool[:0]

	for i := range ti.piHeap {
		ti.piHeap[i] = nil
	}
	ti.piHeap = ti.piHeap[:0]

	ti.err = nil
	ti.nextBlockNoop = false
}

func (ti *tstIter) init(parts []*part, sids []common.SeriesID, minTimestamp, maxTimestamp int64) {
	ti.reset()
	ti.parts = parts

	if n := len(ti.parts) - cap(ti.piPool); n > 0 {
		ti.piPool = append(ti.piPool[:cap(ti.piPool)], make([]partIter, n)...)
	}
	ti.piPool = ti.piPool[:len(ti.parts)]
	for i, p := range ti.parts {
		ti.piPool[i].init(p, sids, minTimestamp, maxTimestamp)
	}

	ti.piHeap = ti.piHeap[:0]
	for i := range ti.piPool {
		ps := &ti.piPool[i]
		if !ps.nextBlock() {
			if err := ps.error(); err != nil {
				ti.err = fmt.Errorf("cannot initialize tsTable iteration: %w", err)
				return
			}
			continue
		}
		ti.piHeap = append(ti.piHeap, ps)
	}
	if len(ti.piHeap) == 0 {
		ti.err = io.EOF
		return
	}
	heap.Init(&ti.piHeap)
	ti.nextBlockNoop = true
}

func (ti *tstIter) nextBlock() bool {
	if ti.err != nil {
		return false
	}
	if ti.nextBlockNoop {
		ti.nextBlockNoop = false
		return true
	}

	ti.err = ti.next()
	if ti.err != nil {
		if errors.Is(ti.err, io.EOF) {
			ti.err = fmt.Errorf("cannot obtain the next block to search in the partition: %w", ti.err)
		}
		return false
	}
	return true
}

func (ti *tstIter) next() error {
	psMin := ti.piHeap[0]
	if psMin.nextBlock() {
		heap.Fix(&ti.piHeap, 0)
		return nil
	}

	if err := psMin.error(); err != nil {
		return err
	}

	heap.Pop(&ti.piHeap)

	if len(ti.piHeap) == 0 {
		return io.EOF
	}
	return nil
}

func (ti *tstIter) Error() error {
	if errors.Is(ti.err, io.EOF) {
		return nil
	}
	return ti.err
}

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
