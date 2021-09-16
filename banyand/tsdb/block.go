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
	"time"

	"github.com/dgraph-io/ristretto/z"

	"github.com/apache/skywalking-banyandb/api/common"
	databasev2 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v2"
	"github.com/apache/skywalking-banyandb/banyand/kv"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/inverted"
	"github.com/apache/skywalking-banyandb/pkg/index/lsm"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

type block struct {
	path string
	l    *logger.Logger
	ref  *z.Closer

	store         kv.TimeSeriesStore
	primaryIndex  index.Store
	invertedIndex index.Store
	lsmIndex      index.Store
	closableLst   []io.Closer
	endTime       time.Time
	startTime     time.Time
	segID         uint16
	blockID       uint16
}

type blockOpts struct {
	segID         uint16
	blockID       uint16
	path          string
	compressLevel int
	valueSize     int
}

func newBlock(ctx context.Context, opts blockOpts) (b *block, err error) {
	b = &block{
		segID:     opts.segID,
		blockID:   opts.blockID,
		path:      opts.path,
		ref:       z.NewCloser(1),
		startTime: time.Now(),
	}
	parentLogger := ctx.Value(logger.ContextKey)
	if parentLogger != nil {
		if pl, ok := parentLogger.(*logger.Logger); ok {
			b.l = pl.Named("block")
		}
	}
	if b.store, err = kv.OpenTimeSeriesStore(0, b.path+"/store", opts.compressLevel, opts.valueSize,
		kv.TSSWithLogger(b.l)); err != nil {
		return nil, err
	}
	if b.primaryIndex, err = lsm.NewStore(lsm.StoreOpts{
		Path:   b.path + "/primary",
		Logger: b.l,
	}); err != nil {
		return nil, err
	}
	b.closableLst = append(b.closableLst, b.store, b.primaryIndex)
	rules, ok := ctx.Value(indexRulesKey).([]*databasev2.IndexRule)
	if !ok || len(rules) == 0 {
		return b, nil
	}
	if b.invertedIndex, err = inverted.NewStore(inverted.StoreOpts{
		Path:   b.path + "/inverted",
		Logger: b.l,
	}); err != nil {
		return nil, err
	}
	if b.lsmIndex, err = lsm.NewStore(lsm.StoreOpts{
		Path:   b.path + "/lsm",
		Logger: b.l,
	}); err != nil {
		return nil, err
	}
	b.closableLst = append(b.closableLst, b.invertedIndex, b.lsmIndex)
	return b, err
}

func (b *block) delegate() blockDelegate {
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

func (b *block) close() {
	b.dscRef()
	b.ref.SignalAndWait()
	for _, closer := range b.closableLst {
		_ = closer.Close()
	}
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
	return d.delegate.startTime
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
	greaterAndEqualStart := d.delegate.startTime.Equal(ts) || d.delegate.startTime.Before(ts)
	if d.delegate.endTime.IsZero() {
		return greaterAndEqualStart
	}
	return greaterAndEqualStart && d.delegate.endTime.After(ts)
}

func (d *bDelegate) Close() error {
	d.delegate.dscRef()
	return nil
}
