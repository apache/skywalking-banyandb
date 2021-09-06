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
	"sort"

	"github.com/apache/skywalking-banyandb/api/common"
	databasev2 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v2"
	modelv2 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v2"
	"github.com/apache/skywalking-banyandb/banyand/kv"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/posting"
)

func (s *seekerBuilder) OrderByIndex(indexRule *databasev2.IndexRule, order modelv2.QueryOrder_Sort) SeekerBuilder {
	s.indexRuleForSorting = indexRule
	s.order = order
	return s
}

func (s *seekerBuilder) OrderByTime(order modelv2.QueryOrder_Sort) SeekerBuilder {
	s.order = order
	s.indexRuleForSorting = nil
	return s
}

func (s *seekerBuilder) buildSeries(filters []filterFn) []Iterator {
	if s.indexRuleForSorting == nil {
		return s.buildSeriesByIndex(filters)
	}
	return s.buildSeriesByTime(filters)
}

func (s *seekerBuilder) buildSeriesByIndex(filters []filterFn) (series []Iterator) {
	for _, b := range s.seriesSpan.blocks {
		switch s.indexRuleForSorting.GetType() {
		case databasev2.IndexRule_TYPE_TREE:
			series = append(series, newSearcherIterator(
				b.lsmIndexReader().
					FieldIterator([]byte(s.indexRuleForSorting.GetMetadata().GetName()), s.order),
				b.dataReader(),
				s.seriesSpan.seriesID,
				filters,
			))
		case databasev2.IndexRule_TYPE_INVERTED:
			series = append(series, newSearcherIterator(b.invertedIndexReader().
				FieldIterator([]byte(s.indexRuleForSorting.GetMetadata().GetName()), s.order),
				b.dataReader(),
				s.seriesSpan.seriesID,
				filters,
			))
		}
	}
	return
}

func (s *seekerBuilder) buildSeriesByTime(filters []filterFn) []Iterator {
	bb := s.seriesSpan.blocks
	switch s.order {
	case modelv2.QueryOrder_SORT_ASC:
		sort.SliceStable(bb, func(i, j int) bool {
			return bb[i].startTime().Before(bb[j].startTime())
		})

	case modelv2.QueryOrder_SORT_DESC:
		sort.SliceStable(bb, func(i, j int) bool {
			return bb[i].startTime().After(bb[j].startTime())
		})
	}
	delegated := make([]Iterator, len(bb))
	for _, b := range bb {
		delegated = append(delegated, newSearcherIterator(b.
			primaryIndexReader().
			FieldIterator(
				s.seriesSpan.seriesID.Marshal(),
				s.order,
			), b.dataReader(), s.seriesSpan.seriesID, filters))
	}
	return []Iterator{newMergedIterator(delegated)}
}

var _ Iterator = (*searcherIterator)(nil)

type searcherIterator struct {
	fieldIterator index.FieldIterator
	curKey        []byte
	cur           posting.Iterator
	data          kv.TimeSeriesReader
	seriesID      common.SeriesID
	filters       []filterFn
}

func (s *searcherIterator) Next() bool {
	if s.cur == nil {
		if s.fieldIterator.Next() {
			v := s.fieldIterator.Val()
			s.cur = v.Value.Iterator()
			s.curKey = v.Key
		} else {
			_ = s.Close()
			return false
		}
	}
	if s.cur.Next() {
		for _, filter := range s.filters {
			if !filter(s.Val()) {
				return s.Next()
			}
		}
		return true
	}
	_ = s.cur.Close()
	s.cur = nil
	return s.Next()
}

func (s *searcherIterator) Val() Item {
	return &item{
		sortedField: s.curKey,
		itemID:      s.cur.Current(),
		data:        s.data,
		seriesID:    s.seriesID,
	}
}

func (s *searcherIterator) Close() error {
	return s.fieldIterator.Close()
}

func newSearcherIterator(fieldIterator index.FieldIterator, data kv.TimeSeriesReader, seriesID common.SeriesID, filters []filterFn) Iterator {
	return &searcherIterator{
		fieldIterator: fieldIterator,
		data:          data,
		seriesID:      seriesID,
		filters:       filters,
	}
}

var _ Iterator = (*mergedIterator)(nil)

type mergedIterator struct {
	curr      Iterator
	index     int
	delegated []Iterator
	closed    bool
}

func (m *mergedIterator) Next() bool {
	if m.closed {
		return false
	}
	if m.curr == nil {
		m.index++
		if m.index >= len(m.delegated) {
			_ = m.Close()
			return false
		} else {
			m.curr = m.delegated[m.index]
		}
	}
	hasNext := m.curr.Next()
	if !hasNext {
		_ = m.curr.Close()
		m.curr = nil
		return m.Next()
	}
	return true
}

func (m *mergedIterator) Val() Item {
	return m.curr.Val()
}

func (m *mergedIterator) Close() error {
	m.closed = true
	return nil
}

func newMergedIterator(delegated []Iterator) Iterator {
	return &mergedIterator{
		index:     -1,
		delegated: delegated,
	}
}
