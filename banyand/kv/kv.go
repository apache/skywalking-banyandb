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

	"github.com/dgraph-io/badger/v3"
	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/pkg/logger"
)

var ErrStopScan = errors.New("stop scanning")
var DefaultScanOpts = ScanOpts{
	PrefetchSize:   100,
	PrefetchValues: true,
}

type Writer interface {
	// Put a value
	Put(key, val []byte) error
}

type ScanFunc func(shardID int, key []byte, getVal func() ([]byte, error)) error

type ScanOpts struct {
	PrefetchSize   int
	PrefetchValues bool
}

type Reader interface {
	// Get a value by its key
	Get(key []byte) ([]byte, error)
	Scan(prefix []byte, opt ScanOpts, f ScanFunc) error
}

// Store is a common kv storage with auto-generated key
type Store interface {
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
	// GetAll values with an identical key
	GetAll(key []byte) ([][]byte, error)
}

// TimeSeriesStore is time series storage
type TimeSeriesStore interface {
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

// OpenTimeSeriesStore creates a new TimeSeriesStore
func OpenTimeSeriesStore(shardID int, path string, compressLevel int, valueSize int, options ...TimeSeriesOptions) (TimeSeriesStore, error) {
	btss := new(badgerTSS)
	btss.shardID = shardID
	btss.dbOpts = badger.DefaultOptions(path)
	for _, opt := range options {
		opt(btss)
	}
	btss.dbOpts = btss.dbOpts.WithMaxLevels(1)
	var err error
	btss.db, err = badger.Open(btss.dbOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to open time series store: %v", err)
	}
	btss.TSet = *badger.NewTSet(btss.db, compressLevel, valueSize)
	return btss, nil
}

type StoreOptions func(Store)

// StoreWithLogger sets a external logger into underlying Store
func StoreWithLogger(l *logger.Logger) StoreOptions {
	return func(store Store) {
		if bdb, ok := store.(*badgerDB); ok {
			bdb.dbOpts = bdb.dbOpts.WithLogger(&badgerLog{
				delegated: l.Named("normal-kv"),
			})
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
	bdb.dbOpts = bdb.dbOpts.WithMaxLevels(1)

	var err error
	bdb.db, err = badger.Open(bdb.dbOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to open time series store: %v", err)
	}
	if bdb.seqKey != "" {
		bdb.seq, err = bdb.db.GetSequence([]byte(bdb.seqKey), 100)
		if err != nil {
			return nil, fmt.Errorf("failed to get sequence: %v", err)
		}
	}
	return bdb, nil
}
