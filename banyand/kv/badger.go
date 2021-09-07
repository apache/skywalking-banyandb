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
	"github.com/dgraph-io/badger/v3/y"
	"go.uber.org/multierr"

	"github.com/apache/skywalking-banyandb/api/common"
	modelv2 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v2"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/posting"
	"github.com/apache/skywalking-banyandb/pkg/index/posting/roaring"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	posting2 "github.com/apache/skywalking-banyandb/pkg/posting"
	roaring2 "github.com/apache/skywalking-banyandb/pkg/posting/roaring"
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

func (b *badgerTSS) Close() error {
	if b.db != nil && !b.db.IsClosed() {
		return b.db.Close()
	}
	return nil
}

type mergedIter struct {
	delegated Iterator2
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
	data, err := i.delegated.Val().Marshall()
	if err != nil {
		i.valid = false
		return
	}
	i.data = data
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

func (b *badgerDB) Handover(iterator Iterator2) error {
	return b.db.HandoverIterator(&mergedIter{
		delegated: iterator,
	})
}

func (b *badgerDB) Seek(key []byte, limit int) (posting2.List, error) {
	opts := badger.DefaultIteratorOptions
	it := b.db.NewIterator(opts)
	defer func() {
		_ = it.Close()
	}()
	result := roaring2.NewPostingList()
	var errMerged error
	for it.Seek(y.KeyWithTs(key, math.MaxInt64)); it.Valid(); it.Next() {
		k := y.ParseKey(it.Key())
		if !bytes.Equal(key, k) {
			break
		}
		list := roaring2.NewPostingList()
		err := list.Unmarshall(it.Value().Value)
		if err != nil {
			errMerged = multierr.Append(errMerged, err)
			continue
		}
		_ = result.Union(list)
		if result.Len() > limit {
			break
		}
	}
	return result, errMerged
}

func (b *badgerDB) Scan(key []byte, opt ScanOpts, f ScanFunc) error {
	opts := badger.DefaultIteratorOptions
	opts.PrefetchSize = opt.PrefetchSize
	opts.PrefetchValues = opt.PrefetchValues
	opts.Reverse = opt.Reverse
	it := b.db.NewIterator(opts)
	defer func() {
		_ = it.Close()
	}()
	for it.Seek(y.KeyWithTs(key, math.MaxInt64)); it.Valid(); it.Next() {
		k := y.ParseKey(it.Key())
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
	delegated y.Iterator
}

func (i *iterator) Next() {
	i.delegated.Next()
}

func (i *iterator) Rewind() {
	i.delegated.Rewind()
}

func (i *iterator) Seek(key []byte) {
	i.delegated.Seek(key)
}

func (i *iterator) Key() []byte {
	return y.ParseKey(i.delegated.Key())
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
	for iter.Seek(key); iter.Valid(); iter.Next() {
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

func (b *badgerDB) MatchField(fieldName []byte) (list posting.List) {
	panic("implement me")
}

func (b *badgerDB) MatchTerms(field index.Field) (list posting.List) {
	panic("implement me")
}

func (b *badgerDB) Range(fieldName []byte, opts index.RangeOpts) (list posting.List) {
	panic("implement me")
}

var _ index.FieldIterator = (*fIterator)(nil)

type fIterator struct {
	init     bool
	delegate Iterator
	curr     *index.PostingValue
}

func (f *fIterator) Next() bool {
	if !f.init {
		f.init = true
		f.delegate.Rewind()
	}
	if !f.delegate.Valid() {
		return false
	}
	pv := &index.PostingValue{
		Key:   f.delegate.Key(),
		Value: roaring.NewPostingListWithInitialData(convert.BytesToUint64(f.delegate.Val())),
	}
	for ; f.delegate.Valid() && bytes.Equal(pv.Key, f.delegate.Key()); f.delegate.Next() {
		pv.Value.Insert(common.ItemID(convert.BytesToUint64(f.delegate.Val())))
	}
	f.curr = pv
	return true
}

func (f *fIterator) Val() *index.PostingValue {
	return f.curr
}

func (f *fIterator) Close() error {
	return f.delegate.Close()
}

func (b *badgerDB) FieldIterator(fieldName []byte, order modelv2.QueryOrder_Sort) index.FieldIterator {
	var reverse bool
	if order == modelv2.QueryOrder_SORT_DESC {
		reverse = true
	}
	iter := b.NewIterator(ScanOpts{
		Prefix:  fieldName,
		Reverse: reverse,
	})
	return &fIterator{
		delegate: iter,
	}
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
