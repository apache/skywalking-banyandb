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
	"log"

	"github.com/dgraph-io/badger/v3"

	"github.com/apache/skywalking-banyandb/pkg/logger"
)

var _ TimeSeriesStore = (*badgerTSS)(nil)

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

var _ Store = (*badgerDB)(nil)

type badgerDB struct {
	shardID int
	dbOpts  badger.Options
	db      *badger.DB
	seqKey  string
	seq     *badger.Sequence
}

func (b *badgerDB) Scan(key []byte, opt ScanOpts, f ScanFunc) error {
	err := b.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = opt.PrefetchSize
		opts.PrefetchValues = opt.PrefetchValues
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(key); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()
			err := f(b.shardID, k, func() ([]byte, error) {
				var val []byte
				err := item.Value(func(v []byte) error {
					val = v
					return nil
				})
				if err != nil {
					return nil, err
				}
				return val, nil
			})
			if err == ErrStopScan {
				break
			}
			if err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

func (b *badgerDB) Close() error {
	if b.db != nil && !b.db.IsClosed() {
		return b.db.Close()
	}
	return nil
}

func (b *badgerDB) Put(key, val []byte) error {
	return b.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, val)
	})
}

func (b *badgerDB) Get(key []byte) ([]byte, error) {
	var bb []byte
	err := b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			bb = val
			return nil
		})
	})
	return bb, err
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
