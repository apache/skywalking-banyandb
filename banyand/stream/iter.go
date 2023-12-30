// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package stream

import (
	"github.com/apache/skywalking-banyandb/api/common"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/posting"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

type Iterator interface {
	Next() bool
	Val() *item
	Close() error
}

type searcherIterator struct {
	fieldIterator index.FieldIterator
	cur           posting.Iterator
	table         *tsTable
	l             *logger.Logger
	curKey        []byte
	timeFilter    filterFn
	indexFilter   index.Filter
	seriesID      common.SeriesID
}

func newSearcherIterator(l *logger.Logger, fieldIterator index.FieldIterator, table *tsTable,
	seriesID common.SeriesID, indexFilter index.Filter, timeFilter filterFn) *searcherIterator {
	return &searcherIterator{
		fieldIterator: fieldIterator,
		table:         table,
		seriesID:      seriesID,
		indexFilter:   indexFilter,
		timeFilter:    timeFilter,
		l:             l,
	}
}

func (s *searcherIterator) Next() bool {
	if s.cur == nil {
		if s.fieldIterator.Next() {
			v := s.fieldIterator.Val()
			s.cur = v.Value.Iterator()
			s.curKey = v.Term
		} else {
			return false
		}
	}
	if s.cur.Next() {
		if !s.timeFilter(s.Val()) {
			return s.Next()
		}
		if e := s.l.Debug(); e.Enabled() {
			e.Uint64("series_id", uint64(s.seriesID)).Uint64("item_id", uint64(s.Val().ID())).Msg("got an item")
		}
		return true
	}
	s.cur = nil
	return s.Next()
}

func (s *searcherIterator) Val() item {
	return item{
		sortedField: s.curKey,
		itemID:      common.ItemID(s.cur.Current()),
		table:       s.table,
		seriesID:    s.seriesID,
	}
}

func (s *searcherIterator) Close() error {
	return s.fieldIterator.Close()
}

type item struct {
	table       *tsTable
	sortedField []byte
	itemID      common.ItemID
	seriesID    common.SeriesID
}

func (i *item) Element() (*streamv1.Element, error) {
	// return i.table.getElement(i.seriesID, i.itemID)
	panic("not implemented")
}

func (i *item) Time() uint64 {
	return uint64(i.itemID)
}

func (i item) SortedField() []byte {
	return i.sortedField
}

func (i *item) Family(family []byte) ([]byte, error) {
	panic("not implemented")
}

func (i item) ID() common.ItemID {
	return i.itemID
}
