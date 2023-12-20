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

// package stream

// import (
// 	"fmt"
// 	"path"
// 	"strings"
// 	"sync"
// 	"time"

// 	"github.com/dgraph-io/badger/v3"
// 	"github.com/dgraph-io/badger/v3/skl"
// 	"go.uber.org/multierr"

// 	"github.com/apache/skywalking-banyandb/api/common"
// 	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
// 	"github.com/apache/skywalking-banyandb/banyand/kv"
// 	"github.com/apache/skywalking-banyandb/banyand/tsdb"
// 	"github.com/apache/skywalking-banyandb/pkg/logger"
// 	"github.com/apache/skywalking-banyandb/pkg/timestamp"
// )

// const (
// 	defaultNumBufferShards  = 2
// 	defaultWriteConcurrency = 1000
// 	id                      = "tst"
// )

// var _ tsdb.TSTable = (*tsTable)(nil)

// type tsTable struct {
// 	sst            kv.TimeSeriesStore
// 	l              *logger.Logger
// 	buffer         *tsdb.Buffer
// 	bufferSupplier *tsdb.BufferSupplier
// 	position       common.Position
// 	id             string
// 	bufferSize     int64
// 	lock           sync.RWMutex
// }

// func (t *tsTable) SizeOnDisk() int64 {
// 	return t.sst.SizeOnDisk()
// }

// func (t *tsTable) openBuffer() (err error) {
// 	t.lock.Lock()
// 	defer t.lock.Unlock()
// 	if t.buffer != nil {
// 		return nil
// 	}
// 	bufferSize := int(t.bufferSize / defaultNumBufferShards)
// 	if t.buffer, err = t.bufferSupplier.Borrow(id, t.id, bufferSize, t.flush); err != nil {
// 		return fmt.Errorf("failed to create buffer: %w", err)
// 	}
// 	return nil
// }

// func (t *tsTable) Close() (err error) {
// 	t.lock.Lock()
// 	defer t.lock.Unlock()
// 	if t.buffer != nil {
// 		t.bufferSupplier.Return(id, t.id)
// 	}
// 	return multierr.Combine(err, t.sst.Close())
// }

// func (t *tsTable) CollectStats() *badger.Statistics {
// 	return t.sst.CollectStats()
// }

// func (t *tsTable) Get(key []byte, ts time.Time) ([]byte, error) {
// 	if v, ok := t.buffer.Read(key, ts); ok {
// 		return v, nil
// 	}
// 	return t.sst.Get(key, uint64(ts.UnixNano()))
// }

// func (t *tsTable) Put(key []byte, val []byte, ts time.Time) error {
// 	t.lock.RLock()
// 	if t.buffer != nil {
// 		defer t.lock.RUnlock()
// 		return t.buffer.Write(key, val, ts)
// 	}
// 	t.lock.RUnlock()
// 	if err := t.openBuffer(); err != nil {
// 		return err
// 	}
// 	return t.buffer.Write(key, val, ts)
// }

// func (t *tsTable) flush(shardIndex int, skl *skl.Skiplist) error {
// 	t.l.Info().Int("shard", shardIndex).Msg("flushing buffer")
// 	return t.sst.Handover(skl)
// }

// var _ tsdb.TSTableFactory = (*tsTableFactory)(nil)

// type tsTableFactory struct {
// 	bufferSize        int64
// 	compressionMethod databasev1.CompressionMethod
// 	chunkSize         int
// }

// func (ttf *tsTableFactory) NewTSTable(bufferSupplier *tsdb.BufferSupplier, root string, position common.Position,
// 	l *logger.Logger, timeRange timestamp.TimeRange,
// ) (tsdb.TSTable, error) {
// 	sst, err := kv.OpenTimeSeriesStore(path.Join(root, id), timeRange, kv.TSSWithMemTableSize(ttf.bufferSize), kv.TSSWithLogger(l.Named(id)),
// 		kv.TSSWithZSTDCompression(ttf.chunkSize))
// 	if err != nil {
// 		return nil, fmt.Errorf("failed to create time series table: %w", err)
// 	}
// 	table := &tsTable{
// 		bufferSupplier: bufferSupplier,
// 		bufferSize:     ttf.bufferSize,
// 		l:              l,
// 		position:       position,
// 		sst:            sst,
// 		id:             strings.Join([]string{position.Segment, position.Block}, "-"),
// 	}
// 	return table, nil
// }

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
	memParts []*partWrapper
	sync.RWMutex
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
