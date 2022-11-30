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
	"log"
	"math"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/dgraph-io/badger/v3/bydb"
	"github.com/dgraph-io/badger/v3/y"

	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

var (
	_              Store           = (*badgerDB)(nil)
	_              IndexStore      = (*badgerDB)(nil)
	_              y.Iterator      = (*mergedIter)(nil)
	_              TimeSeriesStore = (*badgerTSS)(nil)
	bitMergeEntry  byte            = 1 << 3
	ErrKeyNotFound                 = badger.ErrKeyNotFound
)

type badgerTSS struct {
	shardID int
	dbOpts  badger.Options
	db      *badger.DB
	badger.TSet
}

func (b *badgerTSS) Context(key []byte, ts uint64, n int) (pre Iterator, next Iterator) {
	preOpts := badger.DefaultIteratorOptions
	preOpts.PrefetchSize = n
	preOpts.PrefetchValues = false
	preOpts.Prefix = key
	preOpts.Reverse = false
	nextOpts := badger.DefaultIteratorOptions
	nextOpts.PrefetchSize = n
	nextOpts.PrefetchValues = false
	nextOpts.Prefix = key
	nextOpts.Reverse = true
	seekKey := y.KeyWithTs(key, ts)
	preIter := b.db.NewIterator(preOpts)
	preIter.Seek(seekKey)
	nextIter := b.db.NewIterator(nextOpts)
	nextIter.Seek(seekKey)
	return &iterator{delegated: preIter}, &iterator{delegated: nextIter, reverse: true}
}

func (b *badgerTSS) Stats() (s observability.Statistics) {
	return badgerStats(b.db)
}

func badgerStats(db *badger.DB) (s observability.Statistics) {
	stat := db.Stats()
	return observability.Statistics{
		MemBytes:    stat.MemBytes,
		MaxMemBytes: db.Opts().MemTableSize,
	}
}

func (b *badgerTSS) Close() error {
	if b.db != nil && !b.db.IsClosed() {
		return b.db.Close()
	}
	return nil
}

type mergedIter struct {
	delegated Iterator
	valid     bool
	data      []byte
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

type badgerDB struct {
	shardID int
	dbOpts  badger.Options
	db      *badger.DB
}

func (b *badgerDB) Stats() observability.Statistics {
	return badgerStats(b.db)
}

func (b *badgerDB) Handover(iterator Iterator) error {
	return b.db.HandoverIterator(&mergedIter{
		delegated: iterator,
	})
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
		err := f(b.shardID, k, func() ([]byte, error) {
			return y.Copy(it.Value().Value), nil
		})
		if err == ErrStopScan {
			break
		}
		if err != nil {
			return err
		}
	}
	return nil
}

var _ Iterator = (*iterator)(nil)

type iterator struct {
	reverse   bool
	delegated y.Iterator
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
	if err == badger.ErrKeyNotFound {
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
	for iter.Seek(y.KeyWithTs(key, math.MaxInt64)); iter.Valid(); iter.Next() {
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

// badgerLog delegates the zap log to the badger logger
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

var _ bydb.TSetEncoderPool = (*encoderPoolDelegate)(nil)

type encoderPoolDelegate struct {
	encoding.SeriesEncoderPool
}

func (e *encoderPoolDelegate) Get(metadata []byte) bydb.TSetEncoder {
	return e.SeriesEncoderPool.Get(metadata)
}

func (e *encoderPoolDelegate) Put(encoder bydb.TSetEncoder) {
	e.SeriesEncoderPool.Put(encoder)
}

var _ bydb.TSetDecoderPool = (*decoderPoolDelegate)(nil)

type decoderPoolDelegate struct {
	encoding.SeriesDecoderPool
}

func (e *decoderPoolDelegate) Get(metadata []byte) bydb.TSetDecoder {
	return &decoderDelegate{
		e.SeriesDecoderPool.Get(metadata),
	}
}

func (e *decoderPoolDelegate) Put(decoder bydb.TSetDecoder) {
	dd := decoder.(*decoderDelegate)
	e.SeriesDecoderPool.Put(dd.SeriesDecoder)
}

var _ bydb.TSetDecoder = (*decoderDelegate)(nil)

type decoderDelegate struct {
	encoding.SeriesDecoder
}

func (d *decoderDelegate) Iterator() bydb.TSetIterator {
	return &iterDelegate{
		SeriesIterator: d.SeriesDecoder.Iterator(),
	}
}

var _ bydb.TSetDecoder = (*decoderDelegate)(nil)

type iterDelegate struct {
	encoding.SeriesIterator
}
