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
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/dgraph-io/badger/v3"
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
	"github.com/apache/skywalking-banyandb/pkg/meter"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

const (
	componentSecondInvertedIdx = "inverted"
	componentSecondLSMIdx      = "lsm"

	defaultEnqueueTimeout = 500 * time.Millisecond
	itemIDLength          = unsafe.Sizeof(common.ItemID(0))
)

var (
	blockMeterProvider          = observability.NewMeterProvider(meterTSDB.SubScope("block"))
	blockOpenedTimeSecondsGauge = blockMeterProvider.Gauge("opened_time_seconds", common.LabelNames()...)
	blockReferencesGauge        = blockMeterProvider.Gauge("refs", common.LabelNames()...)

	blockCompressedSize     = blockMeterProvider.Gauge("compressed_size", append(common.LabelNames(), "from", "to")...)
	blockUncompressedSize   = blockMeterProvider.Gauge("uncompressed_size", append(common.LabelNames(), "from", "to")...)
	blockCompressedBlockNum = blockMeterProvider.Gauge("compressed_block_num", append(common.LabelNames(), "from", "to")...)
	blockCompressedEntryNum = blockMeterProvider.Gauge("compressed_entry_num", append(common.LabelNames(), "from", "to")...)
	blockEncodedSize        = blockMeterProvider.Gauge("encoded_size", append(common.LabelNames(), "from", "to")...)
	blockUnencodedSize      = blockMeterProvider.Gauge("unencoded_size", append(common.LabelNames(), "from", "to")...)
	blockEncodedBlockNum    = blockMeterProvider.Gauge("encoded_block_num", append(common.LabelNames(), "from", "to")...)
	blockEncodedEntryNum    = blockMeterProvider.Gauge("encoded_entry_num", append(common.LabelNames(), "from", "to")...)
	blockCompactionMap      = map[badger.TableBuilderSizeKeyType]meter.Gauge{
		badger.TableBuilderSizeKeyCompressedSize:     blockCompressedSize,
		badger.TableBuilderSizeKeyUncompressedSize:   blockUncompressedSize,
		badger.TableBuilderSizeKeyCompressedBlockNum: blockCompressedBlockNum,
		badger.TableBuilderSizeKeyCompressedEntryNum: blockCompressedEntryNum,
		badger.TableBuilderSizeKeyEncodedSize:        blockEncodedSize,
		badger.TableBuilderSizeKeyUnEncodedSize:      blockUnencodedSize,
		badger.TableBuilderSizeKeyEncodedBlockNum:    blockEncodedBlockNum,
		badger.TableBuilderSizeKeyEncodedEntryNum:    blockEncodedEntryNum,
	}

	errBlockClosingInterrupted = errors.New("interrupt to close the block")
)

type block struct {
	openOpts      openOpts
	invertedIndex index.Store
	tsTable       TSTable
	queue         bucket.Queue
	bucket.Reporter
	clock            timestamp.Clock
	lsmIndex         index.Store
	closeBufferTimer *time.Timer
	closed           *atomic.Bool
	ref              *atomic.Int32
	l                *logger.Logger
	deleted          *atomic.Bool
	position         common.Position
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
	tsTableFactory TSTableFactory
	inverted       inverted.StoreOpts
	lsm            lsm.StoreOpts
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
		position:  common.GetPosition(ctx),
	}
	l := logger.Fetch(ctx, b.String())
	b.openOpts, err = options(ctx, opts.path, l)
	if err != nil {
		return nil, err
	}
	b.l = l
	b.Reporter = bucket.NewTimeBasedReporter(b.String(), opts.timeRange, clock, opts.scheduler)
	b.closed.Store(true)
	return b, nil
}

func options(ctx context.Context, root string, l *logger.Logger) (openOpts, error) {
	var options DatabaseOpts
	o := ctx.Value(OptionsKey)
	if o == nil {
		return openOpts{}, errors.New("database options not found")
	}
	options = o.(DatabaseOpts)
	var opts openOpts
	opts.inverted = inverted.StoreOpts{
		Path:         path.Join(root, componentSecondInvertedIdx),
		Logger:       l.Named(componentSecondInvertedIdx),
		BatchWaitSec: options.BlockInvertedIndex.BatchWaitSec,
	}
	opts.lsm = lsm.StoreOpts{
		Path:         path.Join(root, componentSecondLSMIdx),
		Logger:       l.Named(componentSecondLSMIdx),
		MemTableSize: defaultKVMemorySize,
	}
	opts.tsTableFactory = options.TSTableFactory
	if opts.tsTableFactory == nil {
		return opts, errors.New("ts table factory is nil")
	}
	return opts, nil
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
	if b.tsTable, err = b.openOpts.tsTableFactory.NewTSTable(BlockExpiryTracker{ttl: b.End, clock: b.clock},
		b.path, b.position, b.l); err != nil {
		return err
	}
	b.closableLst = append(b.closableLst, b.tsTable)
	if b.invertedIndex, err = inverted.NewStore(b.openOpts.inverted); err != nil {
		return err
	}
	if b.lsmIndex, err = lsm.NewStore(b.openOpts.lsm); err != nil {
		return err
	}
	b.closableLst = append(b.closableLst, b.invertedIndex, b.lsmIndex)
	b.ref.Store(0)
	b.closed.Store(false)
	blockOpenedTimeSecondsGauge.Set(float64(time.Now().Unix()), b.position.LabelValues()...)
	plv := b.position.LabelValues()
	observability.MetricsCollector.Register(strings.Join(plv, "-"), func() {
		stats := b.tsTable.CollectStats()
		stats.TableBuilderSize.Range(func(label interface{}, value interface{}) bool {
			key := label.(badger.TableBuilderSizeKey)
			counter := value.(*atomic.Int64)
			from, to := getLevelLabels(key.FromLevel, key.ToLevel)
			labelValues := append(b.position.LabelValues(), from, to)
			if block, ok := blockCompactionMap[key.Type]; ok {
				block.Set(float64(counter.Load()), labelValues...)
			}
			return true
		})
	})
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
	if b.Closed() {
		return false
	}
	b.ref.Add(1)
	blockReferencesGauge.Set(float64(b.ref.Load()), b.position.LabelValues()...)
	return true
}

func (b *block) Done() {
	b.ref.Add(-1)
	blockReferencesGauge.Set(float64(b.ref.Load()), b.position.LabelValues()...)
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
	if b.closeBufferTimer != nil {
		b.closeBufferTimer.Stop()
	}
	plv := b.position.LabelValues()
	observability.MetricsCollector.Unregister(strings.Join(plv, "-"))
	stats := b.tsTable.CollectStats()
	stats.TableBuilderSize.Range(func(label interface{}, value interface{}) bool {
		key := label.(badger.TableBuilderSizeKey)
		from, to := getLevelLabels(key.FromLevel, key.ToLevel)
		labelValues := append(b.position.LabelValues(), from, to)
		if block, ok := blockCompactionMap[key.Type]; ok {
			block.Delete(labelValues...)
		}
		return true
	})
	for _, closer := range b.closableLst {
		err = multierr.Append(err, closer.Close())
	}
	for _, g := range []meter.Gauge{blockOpenedTimeSecondsGauge, blockReferencesGauge} {
		g.Delete(b.position.LabelValues()...)
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

func (b *block) Get(key []byte, ts uint64) ([]byte, error) {
	return b.tsTable.Get(key, time.Unix(0, int64(ts)))
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
	return d.delegate
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
	if err := d.delegate.tsTable.Put(key, val, ts); err != nil {
		receivedNumCounter.Inc(1, append(d.delegate.position.ShardLabelValues(), "tst", "true")...)
		return err
	}
	receivedBytesCounter.Inc(float64(len(key)+len(val)), append(d.delegate.position.ShardLabelValues(), "tst")...)
	receivedNumCounter.Inc(1, append(d.delegate.position.ShardLabelValues(), "tst", "false")...)
	return nil
}

func (d *bDelegate) writePrimaryIndex(field index.Field, id common.ItemID) error {
	if err := d.delegate.lsmIndex.Write([]index.Field{field}, uint64(id)); err != nil {
		receivedNumCounter.Inc(1, append(d.delegate.position.ShardLabelValues(), "primary", "true")...)
		return err
	}
	receivedBytesCounter.Inc(float64(len(field.Marshal())+int(itemIDLength)), append(d.delegate.position.ShardLabelValues(), "primary")...)
	receivedNumCounter.Inc(1, append(d.delegate.position.ShardLabelValues(), "primary", "false")...)
	return nil
}

func (d *bDelegate) writeLSMIndex(fields []index.Field, id common.ItemID) error {
	total := 0
	for _, f := range fields {
		total += len(f.Marshal())
	}

	if err := d.delegate.lsmIndex.Write(fields, uint64(id)); err != nil {
		receivedNumCounter.Inc(1, append(d.delegate.position.ShardLabelValues(), "local_lsm", "true")...)
		return err
	}
	receivedBytesCounter.Inc(float64(total+int(itemIDLength)), append(d.delegate.position.ShardLabelValues(), "local_lsm")...)
	receivedNumCounter.Inc(1, append(d.delegate.position.ShardLabelValues(), "local_lsm", "false")...)
	return nil
}

func (d *bDelegate) writeInvertedIndex(fields []index.Field, id common.ItemID) error {
	total := 0
	for _, f := range fields {
		total += len(f.Marshal())
	}

	if err := d.delegate.invertedIndex.Write(fields, uint64(id)); err != nil {
		receivedNumCounter.Inc(1, append(d.delegate.position.ShardLabelValues(), "local_inverted", "true")...)
		return err
	}
	receivedBytesCounter.Inc(float64(total+int(itemIDLength)), append(d.delegate.position.ShardLabelValues(), "local_inverted")...)
	receivedNumCounter.Inc(1, append(d.delegate.position.ShardLabelValues(), "local_inverted", "false")...)
	return nil
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

func getLevelLabels(fromLevel, toLevel int) (string, string) {
	from := fmt.Sprintf("l%d", fromLevel)
	to := fmt.Sprintf("l%d", toLevel)

	if fromLevel < 0 {
		from = "mem"
	}

	return from, to
}
