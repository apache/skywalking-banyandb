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
	"time"

	"go.uber.org/multierr"

	"github.com/apache/skywalking-banyandb/api/common"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/kv"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/posting"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

var emptyFilters = make([]FilterFn, 0)

func (s *seekerBuilder) OrderByIndex(indexRule *databasev1.IndexRule, order modelv1.Sort) SeekerBuilder {
	s.indexRuleForSorting = indexRule
	s.order = order
	return s
}

func (s *seekerBuilder) OrderByTime(order modelv1.Sort) SeekerBuilder {
	s.order = order
	s.indexRuleForSorting = nil
	return s
}

func (s *seekerBuilder) buildSeries(conditions []condWithIRT) ([]ItemIterator, error) {
	if s.indexRuleForSorting == nil {
		return s.buildSeriesByTime(conditions)
	}
	return s.buildSeriesByIndex(conditions)
}

func (s *seekerBuilder) buildSeriesByIndex(conditions []condWithIRT) (series []ItemIterator, err error) {
	timeFilter := func(item Item) bool {
		valid := s.seriesSpan.timeRange.Contains(item.Time())
		timeRange := s.seriesSpan.timeRange
		s.seriesSpan.l.Trace().
			Times("time_range", []time.Time{timeRange.Start, timeRange.End}).
			Bool("valid", valid).Msg("filter item by time range")
		return valid
	}
	for _, b := range s.seriesSpan.blocks {
		var inner index.FieldIterator
		var err error
		fieldKey := index.FieldKey{
			SeriesID:    s.seriesSpan.seriesID,
			IndexRuleID: s.indexRuleForSorting.GetMetadata().GetId(),
		}
		filters := []FilterFn{timeFilter}
		filter, err := s.buildIndexFilter(b, conditions)
		if err != nil {
			return nil, err
		}
		if filter != nil {
			filters = append(filters, filter)
		}
		switch s.indexRuleForSorting.GetType() {
		case databasev1.IndexRule_TYPE_TREE:
			inner, err = b.lsmIndexReader().Iterator(fieldKey, s.rangeOptsForSorting, s.order)
		case databasev1.IndexRule_TYPE_INVERTED:
			inner, err = b.invertedIndexReader().Iterator(fieldKey, s.rangeOptsForSorting, s.order)
		}
		if err != nil {
			return nil, err
		}
		if inner != nil {
			series = append(series, newSearcherIterator(s.seriesSpan.l, inner, b.dataReader(), s.seriesSpan.seriesID, filters))
		}
	}
	return
}

func (s *seekerBuilder) buildSeriesByTime(conditions []condWithIRT) ([]ItemIterator, error) {
	bb := s.seriesSpan.blocks
	switch s.order {
	case modelv1.Sort_SORT_ASC, modelv1.Sort_SORT_UNSPECIFIED:
		sort.SliceStable(bb, func(i, j int) bool {
			return bb[i].startTime().Before(bb[j].startTime())
		})

	case modelv1.Sort_SORT_DESC:
		sort.SliceStable(bb, func(i, j int) bool {
			return bb[i].startTime().After(bb[j].startTime())
		})
	}
	delegated := make([]ItemIterator, 0, len(bb))
	bTimes := make([]time.Time, 0, len(bb))
	timeRange := s.seriesSpan.timeRange
	termRange := index.RangeOpts{
		Lower:         convert.Int64ToBytes(timeRange.Start.UnixNano()),
		Upper:         convert.Int64ToBytes(timeRange.End.UnixNano()),
		IncludesLower: true,
	}
	for _, b := range bb {
		bTimes = append(bTimes, b.startTime())
		inner, err := b.primaryIndexReader().
			Iterator(
				index.FieldKey{
					SeriesID: s.seriesSpan.seriesID,
				},
				termRange,
				s.order,
			)
		if err != nil {
			return nil, err
		}
		if inner != nil {
			filter, err := s.buildIndexFilter(b, conditions)
			if err != nil {
				return nil, err
			}
			if filter == nil {
				delegated = append(delegated, newSearcherIterator(s.seriesSpan.l, inner, b.dataReader(), s.seriesSpan.seriesID, emptyFilters))
			} else {
				delegated = append(delegated, newSearcherIterator(s.seriesSpan.l, inner, b.dataReader(), s.seriesSpan.seriesID, []FilterFn{filter}))
			}
		}
	}
	s.seriesSpan.l.Debug().
		Str("order", modelv1.Sort_name[int32(s.order)]).
		Times("blocks", bTimes).
		Uint64("series_id", uint64(s.seriesSpan.seriesID)).
		Int("shard_id", int(s.seriesSpan.shardID)).
		Msg("seek series by time")
	return []ItemIterator{newMergedIterator(delegated)}, nil
}

var _ ItemIterator = (*searcherIterator)(nil)

type searcherIterator struct {
	fieldIterator index.FieldIterator
	curKey        []byte
	cur           posting.Iterator
	data          kv.TimeSeriesReader
	seriesID      common.SeriesID
	filters       []FilterFn
	l             *logger.Logger
}

func (s *searcherIterator) Next() (Item, bool) {
	if s.cur == nil {
		if s.fieldIterator.Next() {
			v := s.fieldIterator.Val()
			s.cur = v.Value.Iterator()
			s.curKey = v.Term
			s.l.Trace().Uint64("series_id", uint64(s.seriesID)).Hex("term", s.curKey).Msg("got a new field")
		} else {
			return nil, false
		}
	}
	if s.cur.Next() {
		for _, filter := range s.filters {
			if !filter(s.val()) {
				s.l.Trace().Uint64("series_id", uint64(s.seriesID)).Uint64("item_id", uint64(s.val().ID())).Msg("ignore the item")
				return s.Next()
			}
		}
		s.l.Trace().Uint64("series_id", uint64(s.seriesID)).Uint64("item_id", uint64(s.val().ID())).Msg("got an item")
		return s.val(), true
	}
	s.cur = nil
	return s.Next()
}

func (s *searcherIterator) val() Item {
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

func newSearcherIterator(l *logger.Logger, fieldIterator index.FieldIterator, data kv.TimeSeriesReader,
	seriesID common.SeriesID, filters []FilterFn,
) ItemIterator {
	return &searcherIterator{
		fieldIterator: fieldIterator,
		data:          data,
		seriesID:      seriesID,
		filters:       filters,
		l:             l,
	}
}

var _ ItemIterator = (*mergedIterator)(nil)

type mergedIterator struct {
	curr      ItemIterator
	index     int
	delegated []ItemIterator
}

func (m *mergedIterator) Next() (Item, bool) {
	if m.curr == nil {
		m.index++
		if m.index >= len(m.delegated) {
			return nil, false
		}
		m.curr = m.delegated[m.index]
	}
	nextItem, hasNext := m.curr.Next()
	if !hasNext {
		m.curr = nil
		return m.Next()
	}
	return nextItem, true
}

func (m *mergedIterator) Close() error {
	var err error
	for _, d := range m.delegated {
		err = multierr.Append(err, d.Close())
	}
	return err
}

func newMergedIterator(delegated []ItemIterator) ItemIterator {
	return &mergedIterator{
		index:     -1,
		delegated: delegated,
	}
}
