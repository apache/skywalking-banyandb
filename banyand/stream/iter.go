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
	"errors"
	"io"

	"go.uber.org/multierr"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/posting"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

type searcherIterator struct {
	fieldIterator     index.FieldIterator
	cur               posting.Iterator
	err               error
	indexFilter       filterFn
	timeFilter        filterFn
	table             *tsTable
	l                 *logger.Logger
	tagProjection     []pbv1.TagProjection
	currItem          item
	sortedTagLocation tagLocation
	seriesID          common.SeriesID
}

func newSearcherIterator(l *logger.Logger, fieldIterator index.FieldIterator, table *tsTable,
	seriesID common.SeriesID, indexFilter filterFn, timeFilter filterFn, tagProjection []pbv1.TagProjection,
	sortedTagLocation tagLocation,
) *searcherIterator {
	return &searcherIterator{
		fieldIterator:     fieldIterator,
		table:             table,
		seriesID:          seriesID,
		indexFilter:       indexFilter,
		timeFilter:        timeFilter,
		l:                 l,
		tagProjection:     tagProjection,
		sortedTagLocation: sortedTagLocation,
	}
}

func (s *searcherIterator) Next() bool {
	if s.err != nil {
		return false
	}
	if s.cur == nil {
		if s.fieldIterator.Next() {
			v := s.fieldIterator.Val()
			s.cur = v.Value.Iterator()
		} else {
			s.err = io.EOF
			return false
		}
	}
	if s.cur.Next() {
		itemID := s.cur.Current()
		if !s.timeFilter(itemID) {
			return s.Next()
		}
		if s.indexFilter != nil && !s.indexFilter(itemID) {
			return s.Next()
		}
		if e := s.l.Debug(); e.Enabled() {
			e.Uint64("series_id", uint64(s.seriesID)).Uint64("item_id", itemID).Msg("got an item")
		}
		e, c, err := s.table.getElement(s.seriesID, int64(itemID), s.tagProjection)
		if err != nil {
			s.err = err
			return false
		}
		sv, err := s.sortedTagLocation.getTagValue(e)
		if err != nil {
			s.err = err
			return false
		}
		s.currItem = item{
			element:        e,
			count:          c,
			sortedTagValue: sv,
			seriesID:       s.seriesID,
		}
		return true
	}
	s.cur = nil
	return s.Next()
}

func (s *searcherIterator) Val() item {
	return s.currItem
}

func (s *searcherIterator) Close() error {
	if errors.Is(s.err, io.EOF) {
		return s.fieldIterator.Close()
	}
	return multierr.Combine(s.err, s.fieldIterator.Close())
}

type item struct {
	element        *element
	sortedTagValue []byte
	count          int
	seriesID       common.SeriesID
}

func (i item) SortedField() []byte {
	return i.sortedTagValue
}
