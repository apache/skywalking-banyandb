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
	"fmt"
	"io"
	"math"

	"github.com/dgraph-io/badger/v3"
	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

var (
	ErrStopScan     = errors.New("stop scanning")
	DefaultScanOpts = ScanOpts{
		PrefetchSize:   100,
		PrefetchValues: true,
	}
)

type Writer interface {
	// Put a value
	Put(key, val []byte) error
	PutWithVersion(key, val []byte, version uint64) error
}

type ScanFunc func(shardID int, key []byte, getVal func() ([]byte, error)) error

type ScanOpts struct {
	PrefetchSize   int
	PrefetchValues bool
	Reverse        bool
	Prefix         []byte
}

type Reader interface {
	Iterable
	// Get a value by its key
	Get(key []byte) ([]byte, error)
	GetAll(key []byte, applyFn func([]byte) error) error
	Scan(prefix, seekKey []byte, opt ScanOpts, f ScanFunc) error
}

// Store is a common kv storage with auto-generated key
type Store interface {
	observability.Observable
	io.Closer
	Writer
	Reader
}

type TimeSeriesWriter interface {
	// Put a value with a timestamp/version
	Put(key, val []byte, ts uint64) error
	// PutAsync a value with a timestamp/version asynchronously.
	// Injected "f" func will notice the result of value write.
	PutAsync(key, val []byte, ts uint64, f func(error)) error
}

type TimeSeriesReader interface {
	// Get a value by its key and timestamp/version
	Get(key []byte, ts uint64) ([]byte, error)
}

// TimeSeriesStore is time series storage
type TimeSeriesStore interface {
	observability.Observable
	io.Closer
	TimeSeriesWriter
	TimeSeriesReader
}

type TimeSeriesOptions func(TimeSeriesStore)

// TSSWithLogger sets a external logger into underlying TimeSeriesStore
func TSSWithLogger(l *logger.Logger) TimeSeriesOptions {
	return func(store TimeSeriesStore) {
		if btss, ok := store.(*badgerTSS); ok {
			btss.dbOpts = btss.dbOpts.WithLogger(&badgerLog{
				delegated: l.Named("ts-kv"),
			})
		}
	}
}

func TSSWithEncoding(encoderPool encoding.SeriesEncoderPool, decoderPool encoding.SeriesDecoderPool) TimeSeriesOptions {
	return func(store TimeSeriesStore) {
		if btss, ok := store.(*badgerTSS); ok {
			btss.dbOpts = btss.dbOpts.WithExternalCompactor(
				&encoderPoolDelegate{
					encoderPool,
				}, &decoderPoolDelegate{
					decoderPool,
				})
		}
	}
}

func TSSWithFlushCallback(callback func()) TimeSeriesOptions {
	return func(store TimeSeriesStore) {
		if btss, ok := store.(*badgerTSS); ok {
			btss.dbOpts.FlushCallBack = callback
		}
	}
}

func TSSWithMemTableSize(size int64) TimeSeriesOptions {
	return func(store TimeSeriesStore) {
		if size < 1 {
			return
		}
		if btss, ok := store.(*badgerTSS); ok {
			btss.dbOpts.MemTableSize = size
		}
	}
}

type Iterator interface {
	Next()
	Rewind()
	Seek(key []byte)
	Key() []byte
	Val() []byte
	Valid() bool
	Close() error
}

type Iterable interface {
	NewIterator(opt ScanOpts) Iterator
}

type HandoverCallback func()

type IndexStore interface {
	observability.Observable
	Iterable
	Reader
	Handover(iterator Iterator) error
	Close() error
}

// OpenTimeSeriesStore creates a new TimeSeriesStore
func OpenTimeSeriesStore(shardID int, path string, options ...TimeSeriesOptions) (TimeSeriesStore, error) {
	btss := new(badgerTSS)
	btss.shardID = shardID
	btss.dbOpts = badger.DefaultOptions(path)
	for _, opt := range options {
		opt(btss)
	}
	// Put all values into LSM
	btss.dbOpts = btss.dbOpts.WithVLogPercentile(1.0)
	if btss.dbOpts.MemTableSize < 8<<20 {
		btss.dbOpts = btss.dbOpts.WithValueThreshold(1 << 10)
	}
	var err error
	btss.db, err = badger.Open(btss.dbOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to open time series store: %v", err)
	}
	btss.TSet = *badger.NewTSet(btss.db)
	return btss, nil
}

type StoreOptions func(Store)

// StoreWithLogger sets a external logger into underlying Store
func StoreWithLogger(l *logger.Logger) StoreOptions {
	return StoreWithNamedLogger("normal-kv", l)
}

// StoreWithNamedLogger sets a external logger with a name into underlying Store
func StoreWithNamedLogger(name string, l *logger.Logger) StoreOptions {
	return func(store Store) {
		if bdb, ok := store.(*badgerDB); ok {
			bdb.dbOpts = bdb.dbOpts.WithLogger(&badgerLog{
				delegated: l.Named(name),
			})
		}
	}
}

// StoreWithMemTableSize sets MemTable size
func StoreWithMemTableSize(size int64) StoreOptions {
	return func(store Store) {
		if size < 1 {
			return
		}
		if bdb, ok := store.(*badgerDB); ok {
			bdb.dbOpts = bdb.dbOpts.WithMemTableSize(size)
		}
	}
}

// OpenStore creates a new Store
func OpenStore(shardID int, path string, options ...StoreOptions) (Store, error) {
	bdb := new(badgerDB)
	bdb.shardID = shardID
	bdb.dbOpts = badger.DefaultOptions(path)
	for _, opt := range options {
		opt(bdb)
	}
	bdb.dbOpts = bdb.dbOpts.WithNumVersionsToKeep(math.MaxUint32)
	if bdb.dbOpts.MemTableSize > 0 && bdb.dbOpts.MemTableSize < 8<<20 {
		bdb.dbOpts = bdb.dbOpts.WithValueThreshold(1 << 10)
	}

	var err error
	bdb.db, err = badger.Open(bdb.dbOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to open normal store: %v", err)
	}
	return bdb, nil
}

type IndexOptions func(store IndexStore)

// IndexWithLogger sets a external logger into underlying IndexStore
func IndexWithLogger(l *logger.Logger) IndexOptions {
	return func(store IndexStore) {
		if bdb, ok := store.(*badgerDB); ok {
			bdb.dbOpts = bdb.dbOpts.WithLogger(&badgerLog{
				delegated: l.Named("index-kv"),
			})
		}
	}
}

// OpenIndexStore creates a new IndexStore
func OpenIndexStore(shardID int, path string, options ...IndexOptions) (IndexStore, error) {
	bdb := new(badgerDB)
	bdb.shardID = shardID
	bdb.dbOpts = badger.DefaultOptions(path)
	for _, opt := range options {
		opt(bdb)
	}
	bdb.dbOpts = bdb.dbOpts.WithNumVersionsToKeep(math.MaxUint32)
	bdb.dbOpts = bdb.dbOpts.WithNumCompactors(2)
	bdb.dbOpts = bdb.dbOpts.WithMemTableSize(2 << 20)
	bdb.dbOpts = bdb.dbOpts.WithValueThreshold(1 << 10)

	var err error
	bdb.db, err = badger.Open(bdb.dbOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to index store: %v", err)
	}
	return bdb, nil
}
