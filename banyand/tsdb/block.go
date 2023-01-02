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

package tsdb

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/multierr"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/kv"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/banyand/tsdb/bucket"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/inverted"
	"github.com/apache/skywalking-banyandb/pkg/index/lsm"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

const (
	componentMain              = "main"
	componentSecondInvertedIdx = "inverted"
	componentSecondLSMIdx      = "lsm"

	defaultMainMemorySize = 8 << 20
	defaultEnqueueTimeout = 500 * time.Millisecond
)

var errBlockClosingInterrupted = errors.New("interrupt to close the block")

type block struct {
	store    kv.TimeSeriesStore
	openOpts openOpts
	queue    bucket.Queue
	bucket.Reporter
	clock         timestamp.Clock
	lsmIndex      index.Store
	invertedIndex index.Store
	closed        *atomic.Bool
	l             *logger.Logger
	deleted       *atomic.Bool
	ref           *atomic.Int32
	position      common.Position
	timestamp.TimeRange
	segSuffix   string
	suffix      string
	path        string
	closableLst []io.Closer
	lock        sync.RWMutex
	segID       SectionID
	blockID     SectionID
}

type openOpts struct {
	store     []kv.TimeSeriesOptions
	storePath string
	inverted  inverted.StoreOpts
	lsm       lsm.StoreOpts
}

type blockOpts struct {
	queue     bucket.Queue
	scheduler *timestamp.Scheduler
	timeRange timestamp.TimeRange
	segSuffix string
	suffix    string
	path      string
	blockSize IntervalRule
	segID     SectionID
}

func newBlock(ctx context.Context, opts blockOpts) (b *block, err error) {
	suffixInteger, err := strconv.Atoi(opts.suffix)
	if err != nil {
		return nil, err
	}
	id := GenerateInternalID(opts.blockSize.Unit, suffixInteger)
	clock, _ := timestamp.GetClock(ctx)
	b = &block{
		segID:     opts.segID,
		segSuffix: opts.segSuffix,
		suffix:    opts.suffix,
		blockID:   id,
		path:      opts.path,
		TimeRange: opts.timeRange,
		clock:     clock,
		ref:       &atomic.Int32{},
		closed:    &atomic.Bool{},
		deleted:   &atomic.Bool{},
		queue:     opts.queue,
	}
	b.l = logger.Fetch(ctx, b.String())
	b.Reporter = bucket.NewTimeBasedReporter(b.String(), opts.timeRange, clock, opts.scheduler)
	b.closed.Store(true)
	b.options(ctx)
	position := ctx.Value(common.PositionKey)
	if position != nil {
		b.position = position.(common.Position)
	}

	return b, nil
}

func (b *block) options(ctx context.Context) {
	var options DatabaseOpts
	o := ctx.Value(optionsKey)
	if o != nil {
		options = o.(DatabaseOpts)
	}
	if options.EncodingMethod.EncoderPool != nil && options.EncodingMethod.DecoderPool != nil {
		b.openOpts.store = append(b.openOpts.store, kv.TSSWithEncoding(options.EncodingMethod.EncoderPool,
			options.EncodingMethod.DecoderPool, options.EncodingMethod.ChunkSizeInBytes))
	}
	if options.CompressionMethod.Type == CompressionTypeZSTD {
		b.openOpts.store = append(b.openOpts.store, kv.TSSWithZSTDCompression(options.CompressionMethod.ChunkSizeInBytes))
	}
	var memSize int64
	if options.BlockMemSize < 1 {
		memSize = defaultMainMemorySize
	} else {
		memSize = options.BlockMemSize
	}
	b.openOpts.store = append(b.openOpts.store, kv.TSSWithMemTableSize(memSize), kv.TSSWithLogger(b.l.Named(componentMain)))
	b.openOpts.storePath = path.Join(b.path, componentMain)
	b.openOpts.inverted = inverted.StoreOpts{
		Path:         path.Join(b.path, componentSecondInvertedIdx),
		Logger:       b.l.Named(componentSecondInvertedIdx),
		BatchWaitSec: options.BlockInvertedIndex.BatchWaitSec,
	}
	lsmMemSize := memSize / 8
	if lsmMemSize < defaultKVMemorySize {
		lsmMemSize = defaultKVMemorySize
	}
	b.openOpts.lsm = lsm.StoreOpts{
		Path:         path.Join(b.path, componentSecondLSMIdx),
		Logger:       b.l.Named(componentSecondLSMIdx),
		MemTableSize: lsmMemSize,
	}
}

func (b *block) openSafely() (err error) {
	if b.deleted.Load() || !b.Closed() {
		return nil
	}
	b.lock.Lock()
	defer b.lock.Unlock()
	if !b.Closed() {
		return
	}
	return b.open()
}

func (b *block) open() (err error) {
	if b.store, err = kv.OpenTimeSeriesStore(b.openOpts.storePath, b.openOpts.store...); err != nil {
		return err
	}
	b.closableLst = append(b.closableLst, b.store)
	if b.invertedIndex, err = inverted.NewStore(b.openOpts.inverted); err != nil {
		return err
	}
	if b.lsmIndex, err = lsm.NewStore(b.openOpts.lsm); err != nil {
		return err
	}
	b.closableLst = append(b.closableLst, b.invertedIndex, b.lsmIndex)
	b.ref.Store(0)
	b.closed.Store(false)
	return nil
}

func (b *block) delegate(ctx context.Context) (blockDelegate, error) {
	if b.deleted.Load() {
		return nil, errors.WithMessagef(errBlockAbsent, "block %s is deleted", b)
	}
	blockID := BlockID{
		BlockID: b.blockID,
		SegID:   b.segID,
	}
	if b.incRef() {
		b.queue.Touch(blockID)
		return &bDelegate{
			delegate: b,
		}, nil
	}
	b.lock.Lock()
	defer b.lock.Unlock()
	if err := b.queue.Push(ctx, blockID, func() error {
		if b.deleted.Load() || !b.Closed() {
			return nil
		}
		return b.open()
	}); err != nil {
		b.l.Error().Err(err).Stringer("block", b).Msg("fail to open block")
		return nil, err
	}
	b.incRef()
	return &bDelegate{
		delegate: b,
	}, nil
}

func (b *block) incRef() bool {
loop:
	if b.Closed() {
		return false
	}
	r := b.ref.Load()
	if b.ref.CompareAndSwap(r, r+1) {
		return true
	}
	runtime.Gosched()
	goto loop
}

func (b *block) Done() {
loop:
	r := b.ref.Load()
	if r < 1 {
		return
	}
	if b.ref.CompareAndSwap(r, r-1) {
		return
	}
	runtime.Gosched()
	goto loop
}

func (b *block) waitDone(stopped *atomic.Bool) <-chan struct{} {
	ch := make(chan struct{})
	go func() {
	loop:
		if b.ref.Load() < 1 {
			b.ref.Store(0)
			close(ch)
			return
		}
		if stopped.Load() {
			close(ch)
			return
		}
		runtime.Gosched()
		goto loop
	}()
	return ch
}

func (b *block) close(ctx context.Context) (err error) {
	b.lock.Lock()
	defer b.lock.Unlock()
	if b.closed.Load() {
		return nil
	}
	stopWaiting := &atomic.Bool{}
	ch := b.waitDone(stopWaiting)
	select {
	case <-ctx.Done():
		stopWaiting.Store(true)
		return errors.Wrapf(errBlockClosingInterrupted, "block:%s", b)
	case <-ch:
	}
	b.closed.Store(true)
	for _, closer := range b.closableLst {
		err = multierr.Append(err, closer.Close())
	}
	return err
}

func (b *block) delete(ctx context.Context) error {
	if b.deleted.Load() {
		return nil
	}
	b.deleted.Store(true)
	b.close(ctx)
	return os.RemoveAll(b.path)
}

func (b *block) Closed() bool {
	return b.closed.Load()
}

func (b *block) String() string {
	return fmt.Sprintf("BlockID-%s-%s", b.segSuffix, b.suffix)
}

func (b *block) stats() (names []string, stats []observability.Statistics) {
	names = append(names, componentMain, componentSecondInvertedIdx, componentSecondLSMIdx)
	if b.Closed() {
		stats = make([]observability.Statistics, 3)
		return
	}
	stats = append(stats, b.store.Stats(), b.invertedIndex.Stats(), b.lsmIndex.Stats())
	return names, stats
}

type blockDelegate interface {
	io.Closer
	contains(ts time.Time) bool
	write(key []byte, val []byte, ts time.Time) error
	writePrimaryIndex(field index.Field, id common.ItemID) error
	writeLSMIndex(fields []index.Field, id common.ItemID) error
	writeInvertedIndex(fields []index.Field, id common.ItemID) error
	dataReader() kv.TimeSeriesReader
	lsmIndexReader() index.Searcher
	invertedIndexReader() index.Searcher
	primaryIndexReader() index.FieldIterable
	identity() (segID SectionID, blockID SectionID)
	startTime() time.Time
	String() string
}

var _ blockDelegate = (*bDelegate)(nil)

type bDelegate struct {
	delegate *block
}

func (d *bDelegate) dataReader() kv.TimeSeriesReader {
	return d.delegate.store
}

func (d *bDelegate) lsmIndexReader() index.Searcher {
	return d.delegate.lsmIndex
}

func (d *bDelegate) invertedIndexReader() index.Searcher {
	return d.delegate.invertedIndex
}

func (d *bDelegate) primaryIndexReader() index.FieldIterable {
	return d.delegate.lsmIndex
}

func (d *bDelegate) startTime() time.Time {
	return d.delegate.Start
}

func (d *bDelegate) identity() (segID SectionID, blockID SectionID) {
	return d.delegate.segID, d.delegate.blockID
}

func (d *bDelegate) write(key []byte, val []byte, ts time.Time) error {
	return d.delegate.store.Put(key, val, uint64(ts.UnixNano()))
}

func (d *bDelegate) writePrimaryIndex(field index.Field, id common.ItemID) error {
	return d.delegate.lsmIndex.Write([]index.Field{field}, id)
}

func (d *bDelegate) writeLSMIndex(fields []index.Field, id common.ItemID) error {
	return d.delegate.lsmIndex.Write(fields, id)
}

func (d *bDelegate) writeInvertedIndex(fields []index.Field, id common.ItemID) error {
	return d.delegate.invertedIndex.Write(fields, id)
}

func (d *bDelegate) contains(ts time.Time) bool {
	return d.delegate.Contains(uint64(ts.UnixNano()))
}

func (d *bDelegate) String() string {
	return d.delegate.String()
}

func (d *bDelegate) Close() error {
	d.delegate.Done()
	return nil
}
