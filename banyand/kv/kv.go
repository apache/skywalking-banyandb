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

// Package kv implements a key-value engine.
package kv

import (
	"fmt"
	"io"
	"math"

	"github.com/dgraph-io/badger/v3"
	"github.com/dgraph-io/badger/v3/options"
	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

var (
	errStopScan = errors.New("stop scanning")

	// DefaultScanOpts is a helper to provides canonical options for scanning.
	DefaultScanOpts = ScanOpts{
		PrefetchSize:   100,
		PrefetchValues: true,
	}
)

type writer interface {
	// Put a value
	Put(key, val []byte) error
	PutWithVersion(key, val []byte, version uint64) error
}

// ScanFunc is the closure executed on scanning out a pair of key-value.
type ScanFunc func(key []byte, getVal func() ([]byte, error)) error

// ScanOpts wraps options for scanning the kv storage.
type ScanOpts struct {
	Prefix         []byte
	PrefetchSize   int
	PrefetchValues bool
	Reverse        bool
}

// Reader allows retrieving data from kv.
type Reader interface {
	Iterable
	// Get a value by its key
	Get(key []byte) ([]byte, error)
	GetAll(key []byte, applyFn func([]byte) error) error
	Scan(prefix, seekKey []byte, opt ScanOpts, f ScanFunc) error
}

// Store is a common kv storage with auto-generated key.
type Store interface {
	observability.Observable
	io.Closer
	writer
	Reader
}

// TimeSeriesWriter allows writing to a time-series storage.
type TimeSeriesWriter interface {
	// Put a value with a timestamp/version
	Put(key, val []byte, ts uint64) error
	// PutAsync a value with a timestamp/version asynchronously.
	// Injected "f" func will notice the result of value write.
	PutAsync(key, val []byte, ts uint64, f func(error)) error
}

// TimeSeriesReader allows retrieving data from a time-series storage.
type TimeSeriesReader interface {
	// Get a value by its key and timestamp/version
	Get(key []byte, ts uint64) ([]byte, error)
}

// TimeSeriesStore is time series storage.
type TimeSeriesStore interface {
	observability.Observable
	io.Closer
	TimeSeriesWriter
	TimeSeriesReader
}

// TimeSeriesOptions sets an options for creating a TimeSeriesStore.
type TimeSeriesOptions func(TimeSeriesStore)

// TSSWithLogger sets a external logger into underlying TimeSeriesStore.
func TSSWithLogger(l *logger.Logger) TimeSeriesOptions {
	return func(store TimeSeriesStore) {
		if btss, ok := store.(*badgerTSS); ok {
			btss.dbOpts = btss.dbOpts.WithLogger(&badgerLog{
				delegated: l.Named("ts-kv"),
			})
		}
	}
}

// TSSWithEncoding sets encoding and decoding pools for building chunks.
func TSSWithEncoding(encoderPool encoding.SeriesEncoderPool, decoderPool encoding.SeriesDecoderPool, chunkSize int) TimeSeriesOptions {
	return func(store TimeSeriesStore) {
		if btss, ok := store.(*badgerTSS); ok {
			btss.dbOpts = btss.dbOpts.WithKeyBasedEncoder(
				&encoderPoolDelegate{
					encoderPool,
				}, &decoderPoolDelegate{
					decoderPool,
				}, chunkSize)
		}
	}
}

// TSSWithZSTDCompression sets a ZSTD based compression method.
func TSSWithZSTDCompression(chunkSize int) TimeSeriesOptions {
	return func(store TimeSeriesStore) {
		if btss, ok := store.(*badgerTSS); ok {
			btss.dbOpts = btss.dbOpts.
				WithCompression(options.ZSTD).
				WithBlockSize(chunkSize).
				WithSameKeyBlock()
		}
	}
}

// TSSWithMemTableSize sets the size of memory table in bytes.
func TSSWithMemTableSize(sizeInBytes int64) TimeSeriesOptions {
	return func(store TimeSeriesStore) {
		if sizeInBytes < 1 {
			return
		}
		if btss, ok := store.(*badgerTSS); ok {
			btss.dbOpts.MemTableSize = sizeInBytes
		}
	}
}

// Iterator allows iterating the kv tables.
// TODO: use generic to provide a unique iterator.
type Iterator interface {
	Next()
	Rewind()
	Seek(key []byte)
	Key() []byte
	RawKey() []byte
	Val() []byte
	Valid() bool
	Close() error
}

// Iterable allows creating a Iterator.
type Iterable interface {
	NewIterator(opt ScanOpts) Iterator
}

// IndexStore allows writing and reading index format data.
type IndexStore interface {
	observability.Observable
	Iterable
	Reader
	Close() error
}

// OpenTimeSeriesStore creates a new TimeSeriesStore.
// nolint: contextcheck
func OpenTimeSeriesStore(path string, options ...TimeSeriesOptions) (TimeSeriesStore, error) {
	btss := new(badgerTSS)
	btss.dbOpts = badger.DefaultOptions(path)
	for _, opt := range options {
		opt(btss)
	}
	// Put all values into LSM
	btss.dbOpts = btss.dbOpts.WithNumVersionsToKeep(math.MaxUint32)
	btss.dbOpts = btss.dbOpts.WithVLogPercentile(1.0)
	if btss.dbOpts.MemTableSize < 8<<20 {
		btss.dbOpts = btss.dbOpts.WithValueThreshold(1 << 10)
	}
	var err error
	btss.db, err = badger.Open(btss.dbOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to open time series store: %w", err)
	}
	btss.TSet = *badger.NewTSet(btss.db)
	return btss, nil
}

// StoreOptions sets options for creating Store.
type StoreOptions func(Store)

// StoreWithLogger sets a external logger into underlying Store.
func StoreWithLogger(l *logger.Logger) StoreOptions {
	return StoreWithNamedLogger("normal-kv", l)
}

// StoreWithNamedLogger sets a external logger with a name into underlying Store.
func StoreWithNamedLogger(name string, l *logger.Logger) StoreOptions {
	return func(store Store) {
		if bdb, ok := store.(*badgerDB); ok {
			bdb.dbOpts = bdb.dbOpts.WithLogger(&badgerLog{
				delegated: l.Named(name),
			})
		}
	}
}

// StoreWithMemTableSize sets MemTable size.
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

// OpenStore creates a new Store.
// nolint: contextcheck
func OpenStore(path string, options ...StoreOptions) (Store, error) {
	bdb := new(badgerDB)
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
		return nil, fmt.Errorf("failed to open normal store: %w", err)
	}
	return bdb, nil
}

// IndexOptions sets options for creating the index store.
type IndexOptions func(store IndexStore)

// IndexWithLogger sets a external logger into underlying IndexStore.
func IndexWithLogger(l *logger.Logger) IndexOptions {
	return func(store IndexStore) {
		if bdb, ok := store.(*badgerDB); ok {
			bdb.dbOpts = bdb.dbOpts.WithLogger(&badgerLog{
				delegated: l.Named("index-kv"),
			})
		}
	}
}

// OpenIndexStore creates a new IndexStore.
func OpenIndexStore(path string, options ...IndexOptions) (IndexStore, error) {
	bdb := new(badgerDB)
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
		return nil, fmt.Errorf("failed to index store: %w", err)
	}
	return bdb, nil
}
