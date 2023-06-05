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
	"fmt"
	"sync"
	"time"
	"unsafe"

	"github.com/dgraph-io/badger/v3/skl"
	"github.com/dgraph-io/badger/v3/y"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/meter"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

const (
	defaultSize = 1 << 20 // 1MB
	nodeAlign   = int(unsafe.Sizeof(uint64(0))) - 1
)

var (
	bufferMeterProvider meter.Provider
	maxBytes            meter.Gauge
	mutableBytes        meter.Gauge
)

func init() {
	bufferMeterProvider = observability.NewMeterProvider(meterTSDB.SubScope("buffer"))
	labelNames := append(common.LabelNames(), "bucket")
	maxBytes = bufferMeterProvider.Gauge("max_bytes", labelNames...)
	mutableBytes = bufferMeterProvider.Gauge("mutable_bytes", labelNames...)
}

type operation struct {
	key   []byte
	value []byte
	epoch uint64
}

type flushEvent struct {
	data *skl.Skiplist
}

type onFlush func(shardIndex int, skl *skl.Skiplist) error

type bufferShardBucket struct {
	mutable          *skl.Skiplist
	writeCh          chan operation
	flushCh          chan flushEvent
	writeWaitGroup   *sync.WaitGroup
	flushWaitGroup   *sync.WaitGroup
	log              *logger.Logger
	immutables       []*skl.Skiplist
	labelValues      []string
	shardLabelValues []string
	index            int
	capacity         int
	mutex            sync.RWMutex
}

// Buffer is an exported struct that represents a buffer composed of multiple shard buckets.
type Buffer struct {
	onFlushFn      onFlush
	entryCloser    *run.Closer
	log            *logger.Logger
	buckets        []bufferShardBucket
	writeWaitGroup sync.WaitGroup
	flushWaitGroup sync.WaitGroup
	numShards      int
	closerOnce     sync.Once
}

// NewBuffer creates a new Buffer instance with the given parameters.
func NewBuffer(log *logger.Logger, position common.Position, flushSize, writeConcurrency, numShards int, onFlushFn onFlush) (*Buffer, error) {
	buckets := make([]bufferShardBucket, numShards)
	buffer := &Buffer{
		buckets:     buckets,
		numShards:   numShards,
		onFlushFn:   onFlushFn,
		entryCloser: run.NewCloser(1),
		log:         log.Named("buffer"),
	}
	buffer.writeWaitGroup.Add(numShards)
	buffer.flushWaitGroup.Add(numShards)
	for i := 0; i < numShards; i++ {
		buckets[i] = bufferShardBucket{
			index:            i,
			capacity:         flushSize,
			mutable:          skl.NewSkiplist(int64(flushSize)),
			writeCh:          make(chan operation, writeConcurrency),
			flushCh:          make(chan flushEvent, 1),
			writeWaitGroup:   &buffer.writeWaitGroup,
			flushWaitGroup:   &buffer.flushWaitGroup,
			log:              buffer.log.Named(fmt.Sprintf("shard-%d", i)),
			labelValues:      append(position.LabelValues(), fmt.Sprintf("%d", i)),
			shardLabelValues: position.ShardLabelValues(),
		}
		buckets[i].start(onFlushFn)
		maxBytes.Set(float64(flushSize), buckets[i].labelValues...)
	}
	return buffer, nil
}

// Write adds a key-value pair with a timestamp to the appropriate shard bucket in the buffer.
func (b *Buffer) Write(key, value []byte, timestamp time.Time) {
	if b == nil || !b.entryCloser.AddRunning() {
		return
	}
	defer b.entryCloser.Done()
	index := b.getShardIndex(key)
	if b.log.Debug().Enabled() {
		b.log.Debug().Uint64("shard", index).Bytes("key", key).
			Time("ts", timestamp).Msg("route a shard")
	}
	b.buckets[index].writeCh <- operation{key: key, value: value, epoch: uint64(timestamp.UnixNano())}
}

// Read retrieves the value associated with the given key and timestamp from the appropriate shard bucket in the buffer.
func (b *Buffer) Read(key []byte, ts time.Time) ([]byte, bool) {
	if b == nil || !b.entryCloser.AddRunning() {
		return nil, false
	}
	defer b.entryCloser.Done()
	keyWithTS := y.KeyWithTs(key, uint64(ts.UnixNano()))
	index := b.getShardIndex(key)
	epoch := uint64(ts.UnixNano())
	ll, deferFn := b.buckets[index].getAll()
	defer deferFn()
	for _, bk := range ll {
		value := bk.Get(keyWithTS)
		if value.Meta == 0 && value.Value == nil {
			continue
		}
		if value.Version == epoch {
			return value.Value, true
		}
	}
	return nil, false
}

// Close gracefully closes the Buffer and ensures that all pending operations are completed.
func (b *Buffer) Close() error {
	b.closerOnce.Do(func() {
		b.entryCloser.Done()
		b.entryCloser.CloseThenWait()
		for i := 0; i < b.numShards; i++ {
			close(b.buckets[i].writeCh)
		}
		b.writeWaitGroup.Wait()
		for i := 0; i < b.numShards; i++ {
			if err := b.onFlushFn(i, b.buckets[i].mutable); err != nil {
				b.buckets[i].log.Err(err).Msg("flushing mutable buffer failed")
			}
			b.buckets[i].mutable.DecrRef()
		}
		for i := 0; i < b.numShards; i++ {
			close(b.buckets[i].flushCh)
		}
		b.flushWaitGroup.Wait()
	})
	return nil
}

func (b *Buffer) getShardIndex(key []byte) uint64 {
	return convert.Hash(key) % uint64(b.numShards)
}

func (bsb *bufferShardBucket) getAll() ([]*skl.Skiplist, func()) {
	bsb.mutex.RLock()
	defer bsb.mutex.RUnlock()
	allList := make([]*skl.Skiplist, len(bsb.immutables)+1)
	bsb.mutable.IncrRef()
	allList[0] = bsb.mutable
	last := len(bsb.immutables) - 1
	for i := range bsb.immutables {
		allList[i+1] = bsb.immutables[last-i]
		bsb.immutables[last-i].IncrRef()
	}
	return allList, func() {
		for _, l := range allList {
			l.DecrRef()
		}
	}
}

func (bsb *bufferShardBucket) start(onFlushFn onFlush) {
	go func() {
		defer func() {
			for _, g := range []meter.Gauge{maxBytes, mutableBytes} {
				g.Delete(bsb.labelValues...)
			}
		}()
		defer bsb.flushWaitGroup.Done()
		for event := range bsb.flushCh {
			oldSkipList := event.data
			memSize := oldSkipList.MemSize()
			t1 := time.Now()
			for {
				if err := onFlushFn(bsb.index, oldSkipList); err != nil {
					bsb.log.Err(err).Msg("flushing immutable buffer failed. Retrying...")
					flushNum.Inc(1, append(bsb.labelValues[:2], "true")...)
					time.Sleep(time.Second)
					continue
				}
				break
			}
			flushLatency.Observe(time.Since(t1).Seconds(), bsb.shardLabelValues...)
			flushBytes.Inc(float64(memSize), bsb.shardLabelValues...)
			flushNum.Inc(1, append(bsb.shardLabelValues, "false")...)

			bsb.mutex.Lock()
			if len(bsb.immutables) > 0 {
				bsb.immutables = bsb.immutables[1:]
			}
			bsb.mutex.Unlock()
			oldSkipList.DecrRef()
		}
	}()
	go func() {
		defer bsb.writeWaitGroup.Done()
		volume := 0
		for op := range bsb.writeCh {
			k := y.KeyWithTs(op.key, op.epoch)
			v := y.ValueStruct{Value: op.value}
			volume += len(k) + int(v.EncodedSize()) + skl.MaxNodeSize + nodeAlign
			memSize := bsb.mutable.MemSize()
			mutableBytes.Set(float64(memSize), bsb.labelValues...)
			if volume >= bsb.capacity || memSize >= int64(bsb.capacity) {
				bsb.triggerFlushing()
				volume = 0
			}
			bsb.mutable.Put(k, v)
		}
	}()
}

func (bsb *bufferShardBucket) triggerFlushing() {
	for {
		select {
		case bsb.flushCh <- flushEvent{data: bsb.mutable}:
			bsb.mutex.Lock()
			defer bsb.mutex.Unlock()
			bsb.swap()
			return
		default:
		}
		time.Sleep(10 * time.Second)
	}
}

func (bsb *bufferShardBucket) swap() {
	bsb.immutables = append(bsb.immutables, bsb.mutable)
	bsb.mutable = skl.NewSkiplist(int64(bsb.capacity))
}
