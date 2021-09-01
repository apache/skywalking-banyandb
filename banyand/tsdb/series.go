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
	"bytes"
	"io"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/multierr"

	"github.com/apache/skywalking-banyandb/api/common"
	modelv2 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v2"
	"github.com/apache/skywalking-banyandb/pkg/convert"
)

var ErrEmptySeriesSpan = errors.New("there is no data in such time range")

type Iterator interface {
	Next() bool
	Val() Item
	Close() error
}

type Item interface {
	Val(family string) []byte
	SortingVal() []byte
}

type ConditionValue struct {
	Values [][]byte
	Op     modelv2.PairQuery_BinaryOp
}

type Condition map[string][]ConditionValue

type ItemID struct {
	//shardID int
	//segID   []byte
	//blockID []byte
	id []byte
}

type TimeRange struct {
	Start    time.Time
	Duration time.Duration
}

type Series interface {
	ID() common.SeriesID
	Span(timeRange TimeRange) (SeriesSpan, error)
	Get(id ItemID) (Item, error)
}

type SeriesSpan interface {
	io.Closer
	WriterBuilder() WriterBuilder
	Iterator() Iterator
	SeekerBuilder() SeekerBuilder
}

type WriterBuilder interface {
	Family(name string, val []byte) WriterBuilder
	Time(ts time.Time) WriterBuilder
	Val(val []byte) WriterBuilder
	Build() (Writer, error)
}

type Writer interface {
	Write() (ItemID, error)
	WriteLSMIndex(name string, val []byte) error
	WriteInvertedIndex(name string, val []byte) error
}

type SeekerBuilder interface {
	Filter(condition Condition) SeekerBuilder
	OrderByIndex(name string, order modelv2.QueryOrder_Sort) SeekerBuilder
	OrderByTime(order modelv2.QueryOrder_Sort) SeekerBuilder
	Build() Seeker
}

type Seeker interface {
	Seek() Iterator
}

var _ Series = (*series)(nil)

type series struct {
	id      common.SeriesID
	blockDB blockDatabase
}

func newSeries(id common.SeriesID, blockDB blockDatabase) *series {
	return &series{
		id:      id,
		blockDB: blockDB,
	}
}

func (s *series) ID() common.SeriesID {
	return s.id
}

func (s *series) Span(timeRange TimeRange) (SeriesSpan, error) {
	blocks := s.blockDB.span(timeRange)
	if len(blocks) < 1 {
		return nil, ErrEmptySeriesSpan
	}
	return newSeriesSpan(blocks, s.id), nil
}

func (s *series) Get(id ItemID) (Item, error) {
	panic("not implemented")
}

var _ SeriesSpan = (*seriesSpan)(nil)

type seriesSpan struct {
	blocks   []blockDelegate
	seriesID common.SeriesID
}

func (s *seriesSpan) Close() (err error) {
	for _, delegate := range s.blocks {
		err = multierr.Append(err, delegate.Close())
	}
	return err
}

func (s *seriesSpan) WriterBuilder() WriterBuilder {
	return newWriterBuilder(s)
}

func (s *seriesSpan) Iterator() Iterator {
	panic("implement me")
}

func (s *seriesSpan) SeekerBuilder() SeekerBuilder {
	panic("implement me")
}

func newSeriesSpan(blocks []blockDelegate, id common.SeriesID) *seriesSpan {
	return &seriesSpan{
		blocks:   blocks,
		seriesID: id,
	}
}

var _ WriterBuilder = (*writerBuilder)(nil)

type writerBuilder struct {
	series *seriesSpan
	block  blockDelegate
	values []struct {
		family []byte
		val    []byte
	}
	ts            time.Time
	seriesIDBytes []byte
}

func (w *writerBuilder) Family(name string, val []byte) WriterBuilder {
	w.values = append(w.values, struct {
		family []byte
		val    []byte
	}{family: bytes.Join([][]byte{w.seriesIDBytes, hash([]byte(name))}, nil), val: val})
	return w
}

func (w *writerBuilder) Time(ts time.Time) WriterBuilder {
	w.ts = ts
	for _, b := range w.series.blocks {
		if b.contains(ts) {
			w.block = b
			break
		}
	}
	return w
}

func (w *writerBuilder) Val(val []byte) WriterBuilder {
	w.values = append(w.values, struct {
		family []byte
		val    []byte
	}{val: val})
	return w
}

var ErrNoTime = errors.New("no time specified")
var ErrNoVal = errors.New("no value specified")

func (w *writerBuilder) Build() (Writer, error) {
	if w.block == nil {
		return nil, ErrNoTime
	}
	if len(w.values) < 1 {
		return nil, ErrNoVal
	}
	wt := &writer{
		block:    w.block,
		ts:       w.ts,
		seriesID: w.seriesIDBytes,
		itemID:   bytes.Join([][]byte{w.seriesIDBytes, convert.Int64ToBytes(w.ts.UnixNano())}, nil),
		columns:  w.values,
	}
	return wt, nil
}

func newWriterBuilder(seriesSpan *seriesSpan) WriterBuilder {
	return &writerBuilder{
		series:        seriesSpan,
		seriesIDBytes: convert.Uint64ToBytes(uint64(seriesSpan.seriesID)),
	}
}

var _ Writer = (*writer)(nil)

type writer struct {
	block    blockDelegate
	ts       time.Time
	seriesID []byte
	columns  []struct {
		family []byte
		val    []byte
	}
	itemID []byte
}

func (w *writer) WriteLSMIndex(name string, val []byte) error {
	panic("implement me")
}

func (w *writer) WriteInvertedIndex(name string, val []byte) error {
	panic("implement me")
}

func (w *writer) Write() (id ItemID, err error) {
	for _, c := range w.columns {
		err = w.block.write(c.family, c.val, w.ts)
		if err != nil {
			return id, err
		}
	}
	id.id = w.itemID
	return id, nil
}
