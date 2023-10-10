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

package kv

import (
	"bytes"
	"errors"
	"log"
	"math"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/dgraph-io/badger/v3/banyandb"
	"github.com/dgraph-io/badger/v3/skl"
	"github.com/dgraph-io/badger/v3/y"

	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

var (
	_             Store           = (*badgerDB)(nil)
	_             IndexStore      = (*badgerDB)(nil)
	_             y.Iterator      = (*mergedIter)(nil)
	_             TimeSeriesStore = (*badgerTSS)(nil)
	bitMergeEntry byte            = 1 << 3
	// ErrKeyNotFound denotes the expected key can not be got from the kv service.
	ErrKeyNotFound = badger.ErrKeyNotFound
)

type badgerTSS struct {
	badger.TSet
	timeRange timestamp.TimeRange
	db        *badger.DB
	dbOpts    badger.Options
}

func (b *badgerTSS) Handover(skl *skl.Skiplist) error {
	return b.db.HandoverIterator(&timeRangeIterator{
		timeRange:   b.timeRange,
		UniIterator: skl.NewUniIterator(false),
	})
}

func (b *badgerTSS) Close() error {
	if b.db != nil && !b.db.IsClosed() {
		return b.db.Close()
	}
	return nil
}

func (b *badgerTSS) SizeOnDisk() int64 {
	lsmSize, vlogSize := b.db.Size()
	return lsmSize + vlogSize
}

func (b *badgerTSS) CollectStats() *badger.Statistics {
	return b.db.CollectStats()
}

type mergedIter struct {
	delegated Iterator
	data      []byte
	valid     bool
}

func (i *mergedIter) Next() {
	i.delegated.Next()
	i.parseData()
}

func (i *mergedIter) Rewind() {
	i.delegated.Rewind()
	i.parseData()
}

func (i *mergedIter) Seek(key []byte) {
	i.delegated.Seek(y.KeyWithTs(key, math.MaxInt64))
}

func (i *mergedIter) Key() []byte {
	return y.KeyWithTs(i.delegated.Key(), uint64(time.Now().UnixNano()))
}

func (i *mergedIter) Valid() bool {
	return i.valid
}

func (i *mergedIter) parseData() {
	i.data = nil
	i.valid = i.delegated.Valid()
	if !i.valid {
		return
	}
	i.data = i.delegated.Val()
}

func (i *mergedIter) Close() error {
	i.data = nil
	i.valid = false
	return i.delegated.Close()
}

func (i mergedIter) Value() y.ValueStruct {
	return y.ValueStruct{
		Value: i.data,
		Meta:  bitMergeEntry,
	}
}

type timeRangeIterator struct {
	*skl.UniIterator
	timeRange timestamp.TimeRange
}

func (i *timeRangeIterator) Next() {
	i.UniIterator.Next()
	for !i.validTime() {
		i.UniIterator.Next()
	}
}

func (i *timeRangeIterator) Rewind() {
	i.UniIterator.Rewind()
	if !i.validTime() {
		i.Next()
	}
}

func (i *timeRangeIterator) Seek(key []byte) {
	i.UniIterator.Seek(key)
	if !i.validTime() {
		i.Next()
	}
}

func (i *timeRangeIterator) validTime() bool {
	if !i.Valid() {
		// If the underlying iterator is invalid, we should return true to stop iterating.
		return true
	}
	ts := y.ParseTs(i.Key())
	return i.timeRange.Contains(ts)
}

type badgerDB struct {
	db     *badger.DB
	dbOpts badger.Options
}

func (b *badgerDB) Scan(prefix, seekKey []byte, opt ScanOpts, f ScanFunc) error {
	opts := badger.DefaultIteratorOptions
	opts.PrefetchSize = opt.PrefetchSize
	opts.PrefetchValues = opt.PrefetchValues
	opts.Reverse = opt.Reverse
	it := b.db.NewIterator(opts)
	defer func() {
		_ = it.Close()
	}()
	for it.Seek(seekKey); it.Valid(); it.Next() {
		k := y.ParseKey(it.Key())
		if len(k) < len(seekKey) {
			continue
		}
		if !bytes.Equal(prefix, k[0:len(prefix)]) {
			continue
		}
		err := f(k, func() ([]byte, error) {
			return y.Copy(it.Value().Value), nil
		})
		if errors.Is(err, errStopScan) {
			break
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *badgerDB) SizeOnDisk() int64 {
	lsmSize, vlogSize := b.db.Size()
	return lsmSize + vlogSize
}

var _ Iterator = (*iterator)(nil)

type iterator struct {
	delegated y.Iterator
	reverse   bool
}

func (i *iterator) Next() {
	i.delegated.Next()
}

func (i *iterator) Rewind() {
	i.delegated.Rewind()
}

func (i *iterator) Seek(key []byte) {
	if i.reverse {
		i.delegated.Seek(y.KeyWithTs(key, 0))
	} else {
		i.delegated.Seek(y.KeyWithTs(key, math.MaxInt64))
	}
}

func (i *iterator) Key() []byte {
	return y.ParseKey(i.delegated.Key())
}

func (i *iterator) RawKey() []byte {
	return i.delegated.Key()
}

func (i *iterator) Val() []byte {
	return y.Copy(i.delegated.Value().Value)
}

func (i *iterator) Valid() bool {
	return i.delegated.Valid()
}

func (i *iterator) Close() error {
	return i.delegated.Close()
}

func (b *badgerDB) NewIterator(opt ScanOpts) Iterator {
	opts := badger.DefaultIteratorOptions
	opts.PrefetchSize = opt.PrefetchSize
	opts.PrefetchValues = opt.PrefetchValues
	opts.Reverse = opt.Reverse
	opts.Prefix = opt.Prefix
	it := b.db.NewIterator(opts)
	return &iterator{
		delegated: it,
		reverse:   opts.Reverse,
	}
}

func (b *badgerDB) Close() error {
	if b.db != nil && !b.db.IsClosed() {
		return b.db.Close()
	}
	return nil
}

func (b *badgerDB) Put(key, val []byte) error {
	return b.db.Put(y.KeyWithTs(key, math.MaxInt64), val)
}

func (b *badgerDB) PutWithVersion(key, val []byte, version uint64) error {
	return b.db.Put(y.KeyWithTs(key, version), val)
}

func (b *badgerDB) Get(key []byte) ([]byte, error) {
	v, err := b.db.Get(y.KeyWithTs(key, math.MaxInt64))
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, ErrKeyNotFound
	}
	if err != nil {
		return nil, err
	}
	return v.Value, nil
}

func (b *badgerDB) GetAll(key []byte, applyFn func([]byte) error) error {
	iter := b.db.NewIterator(badger.DefaultIteratorOptions)
	var count int
	for iter.Seek(y.KeyWithTs(key, math.MaxUint64)); iter.Valid(); iter.Next() {
		if !bytes.Equal(y.ParseKey(iter.Key()), key) {
			break
		}
		count++
		err := applyFn(y.Copy(iter.Value().Value))
		if err != nil {
			return err
		}
	}
	if count > 0 {
		return nil
	}
	return ErrKeyNotFound
}

// badgerLog delegates the zap log to the badger logger.
type badgerLog struct {
	*log.Logger
	delegated *logger.Logger
}

func (l *badgerLog) Errorf(f string, v ...interface{}) {
	l.delegated.Error().Msgf(f, v...)
}

func (l *badgerLog) Warningf(f string, v ...interface{}) {
	l.delegated.Warn().Msgf(f, v...)
}

func (l *badgerLog) Infof(f string, v ...interface{}) {
	l.delegated.Info().Msgf(f, v...)
}

func (l *badgerLog) Debugf(f string, v ...interface{}) {
	l.delegated.Debug().Msgf(f, v...)
}

var _ banyandb.SeriesEncoderPool = (*encoderPoolDelegate)(nil)

type encoderPoolDelegate struct {
	encoding.SeriesEncoderPool
}

func (e *encoderPoolDelegate) Get(metadata []byte, buffer banyandb.Buffer) banyandb.SeriesEncoder {
	encoder := e.SeriesEncoderPool.Get(metadata, buffer)
	if encoder == nil {
		return nil
	}
	return &encoderDelegate{SeriesEncoder: encoder}
}

func (e *encoderPoolDelegate) Put(encoder banyandb.SeriesEncoder) {
	ee := encoder.(*encoderDelegate)
	e.SeriesEncoderPool.Put(ee.SeriesEncoder)
}

var _ banyandb.SeriesEncoder = (*encoderDelegate)(nil)

type encoderDelegate struct {
	encoding.SeriesEncoder
}

func (e *encoderDelegate) Reset(key []byte, buffer banyandb.Buffer) {
	e.SeriesEncoder.Reset(key, &bufferDelegate{BufferWriter: buffer})
}

var _ encoding.BufferWriter = (*bufferDelegate)(nil)

type bufferDelegate struct {
	encoding.BufferWriter
}

var _ banyandb.SeriesDecoderPool = (*decoderPoolDelegate)(nil)

type decoderPoolDelegate struct {
	encoding.SeriesDecoderPool
}

func (e *decoderPoolDelegate) Get(metadata []byte) banyandb.SeriesDecoder {
	if decoder := e.SeriesDecoderPool.Get(metadata); decoder != nil {
		return &decoderDelegate{
			e.SeriesDecoderPool.Get(metadata),
		}
	}
	return nil
}

func (e *decoderPoolDelegate) Put(decoder banyandb.SeriesDecoder) {
	if decoder != nil {
		dd := decoder.(*decoderDelegate)
		e.SeriesDecoderPool.Put(dd.SeriesDecoder)
	}
}

var _ banyandb.SeriesDecoder = (*decoderDelegate)(nil)

type decoderDelegate struct {
	encoding.SeriesDecoder
}

func (d *decoderDelegate) Iterator() banyandb.SeriesIterator {
	return &iterDelegate{
		SeriesIterator: d.SeriesDecoder.Iterator(),
	}
}

type iterDelegate struct {
	encoding.SeriesIterator
}
