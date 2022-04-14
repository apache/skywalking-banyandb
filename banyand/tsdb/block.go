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
	"io"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/dgraph-io/ristretto/z"
	"go.uber.org/atomic"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/kv"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/banyand/tsdb/bucket"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/inverted"
	"github.com/apache/skywalking-banyandb/pkg/index/lsm"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

const (
	componentMain              = "main"
	componentPrimaryIdx        = "primary"
	componentSecondInvertedIdx = "inverted"
	componentSecondLSMIdx      = "lsm"
)

type block struct {
	path     string
	l        *logger.Logger
	suffix   string
	ref      *z.Closer
	lock     sync.RWMutex
	closed   *atomic.Bool
	position common.Position

	store         kv.TimeSeriesStore
	primaryIndex  index.Store
	invertedIndex index.Store
	lsmIndex      index.Store
	closableLst   []io.Closer
	timestamp.TimeRange
	bucket.Reporter
	segID          uint16
	blockID        uint16
	encodingMethod EncodingMethod
	flushCh        chan struct{}
}

type blockOpts struct {
	segID     uint16
	blockSize IntervalRule
	startTime time.Time
	suffix    string
	path      string
}

func newBlock(ctx context.Context, opts blockOpts) (b *block, err error) {
	suffixInteger, err := strconv.Atoi(opts.suffix)
	if err != nil {
		return nil, err
	}
	id := uint16(opts.blockSize.Unit)<<12 | ((uint16(suffixInteger) << 4) >> 4)
	timeRange := timestamp.NewTimeRange(opts.startTime, opts.blockSize.NextTime(opts.startTime), true, false)
	encodingMethodObject := ctx.Value(encodingMethodKey)
	if encodingMethodObject == nil {
		encodingMethodObject = EncodingMethod{
			EncoderPool: encoding.NewPlainEncoderPool(0),
			DecoderPool: encoding.NewPlainDecoderPool(0),
		}
	}
	b = &block{
		segID:          opts.segID,
		blockID:        id,
		path:           opts.path,
		l:              logger.Fetch(ctx, "block"),
		TimeRange:      timeRange,
		Reporter:       bucket.NewTimeBasedReporter(timeRange),
		closed:         atomic.NewBool(true),
		encodingMethod: encodingMethodObject.(EncodingMethod),
	}
	position := ctx.Value(common.PositionKey)
	if position != nil {
		b.position = position.(common.Position)
	}
	return b, err
}

func (b *block) open() (err error) {
	b.lock.Lock()
	defer b.lock.Unlock()
	if !b.closed.Load() {
		return nil
	}
	b.ref = z.NewCloser(1)
	if b.store, err = kv.OpenTimeSeriesStore(
		0,
		path.Join(b.path, componentMain),
		kv.TSSWithEncoding(b.encodingMethod.EncoderPool, b.encodingMethod.DecoderPool),
		kv.TSSWithLogger(b.l.Named(componentMain)),
		kv.TSSWithFlushCallback(func() {

		}),
	); err != nil {
		return err
	}
	if b.primaryIndex, err = lsm.NewStore(lsm.StoreOpts{
		Path:   path.Join(b.path, componentPrimaryIdx),
		Logger: b.l.Named(componentPrimaryIdx),
	}); err != nil {
		return err
	}
	b.closableLst = append(b.closableLst, b.store, b.primaryIndex)
	if b.invertedIndex, err = inverted.NewStore(inverted.StoreOpts{
		Path:   path.Join(b.path, componentSecondInvertedIdx),
		Logger: b.l.Named(componentSecondInvertedIdx),
	}); err != nil {
		return err
	}
	if b.lsmIndex, err = lsm.NewStore(lsm.StoreOpts{
		Path:   path.Join(b.path, componentSecondLSMIdx),
		Logger: b.l.Named(componentSecondLSMIdx),
	}); err != nil {
		return err
	}
	b.closableLst = append(b.closableLst, b.invertedIndex, b.lsmIndex)
	b.closed.Store(false)
	b.flushCh = make(chan struct{})
	go func() {
		for {
			_, more := <-b.flushCh
			if !more {
				return
			}
			b.flush()
		}
	}()
	return nil
}

func (b *block) delegate() blockDelegate {
	if b.isClosed() {
		return nil
	}
	b.incRef()
	return &bDelegate{
		delegate: b,
	}
}

func (b *block) dscRef() {
	b.ref.Done()
}

func (b *block) incRef() {
	b.ref.AddRunning(1)
}

func (b *block) flush() {
	for i := 0; i < 10; i++ {
		err := b.invertedIndex.Flush()
		if err == nil {
			break
		}
		time.Sleep(time.Second)
		b.l.Warn().Err(err).Int("retried", i).Msg("failed to flush inverted index")
	}
}

func (b *block) close() {
	b.lock.Lock()
	defer b.lock.Unlock()
	if b.isClosed() {
		return
	}
	b.dscRef()
	b.ref.SignalAndWait()
	for _, closer := range b.closableLst {
		_ = closer.Close()
	}
	b.closed.Store(true)
	close(b.flushCh)
}

func (b *block) isClosed() bool {
	return b.closed.Load()
}

func (b *block) String() string {
	return b.Reporter.String()
}

func (b *block) stats() (names []string, stats []observability.Statistics) {
	names = append(names, componentMain, componentPrimaryIdx, componentSecondInvertedIdx, componentSecondLSMIdx)
	stats = append(stats, b.store.Stats(), b.primaryIndex.Stats(), b.invertedIndex.Stats(), b.lsmIndex.Stats())
	return names, stats
}

type blockDelegate interface {
	io.Closer
	contains(ts time.Time) bool
	write(key []byte, val []byte, ts time.Time) error
	writePrimaryIndex(field index.Field, id common.ItemID) error
	writeLSMIndex(field index.Field, id common.ItemID) error
	writeInvertedIndex(field index.Field, id common.ItemID) error
	dataReader() kv.TimeSeriesReader
	lsmIndexReader() index.Searcher
	invertedIndexReader() index.Searcher
	primaryIndexReader() index.Searcher
	identity() (segID uint16, blockID uint16)
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

func (d *bDelegate) primaryIndexReader() index.Searcher {
	return d.delegate.primaryIndex
}

func (d *bDelegate) startTime() time.Time {
	return d.delegate.Start
}

func (d *bDelegate) identity() (segID uint16, blockID uint16) {
	return d.delegate.segID, d.delegate.blockID
}

func (d *bDelegate) write(key []byte, val []byte, ts time.Time) error {
	return d.delegate.store.Put(key, val, uint64(ts.UnixNano()))
}

func (d *bDelegate) writePrimaryIndex(field index.Field, id common.ItemID) error {
	return d.delegate.primaryIndex.Write(field, id)
}

func (d *bDelegate) writeLSMIndex(field index.Field, id common.ItemID) error {
	if d.delegate.lsmIndex == nil {
		return nil
	}
	return d.delegate.lsmIndex.Write(field, id)
}

func (d *bDelegate) writeInvertedIndex(field index.Field, id common.ItemID) error {
	if d.delegate.invertedIndex == nil {
		return nil
	}
	return d.delegate.invertedIndex.Write(field, id)
}

func (d *bDelegate) contains(ts time.Time) bool {
	return d.delegate.Contains(uint64(ts.UnixNano()))
}

func (d *bDelegate) String() string {
	return d.delegate.String()
}

func (d *bDelegate) Close() error {
	d.delegate.dscRef()
	return nil
}
