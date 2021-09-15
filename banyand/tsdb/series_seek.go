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
	"github.com/apache/skywalking-banyandb/api/common"
	databasev2 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v2"
	modelv2 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v2"
	"github.com/apache/skywalking-banyandb/banyand/kv"
	"github.com/apache/skywalking-banyandb/pkg/index"
)

type Iterator interface {
	Next() bool
	Val() Item
	Close() error
}

type Item interface {
	Val(family string) ([]byte, error)
	ID() common.ItemID
	SortedField() []byte
	Time() uint64
}

type SeekerBuilder interface {
	Filter(indexRule *databasev2.IndexRule, condition Condition) SeekerBuilder
	OrderByIndex(indexRule *databasev2.IndexRule, order modelv2.QueryOrder_Sort) SeekerBuilder
	OrderByTime(order modelv2.QueryOrder_Sort) SeekerBuilder
	Build() (Seeker, error)
}

type Seeker interface {
	Seek() ([]Iterator, error)
}

var _ SeekerBuilder = (*seekerBuilder)(nil)

type seekerBuilder struct {
	seriesSpan *seriesSpan

	conditions []struct {
		indexRuleType databasev2.IndexRule_Type
		indexRuleID   uint32
		condition     Condition
	}
	order               modelv2.QueryOrder_Sort
	indexRuleForSorting *databasev2.IndexRule
	rangeOptsForSorting index.RangeOpts
}

func (s *seekerBuilder) Build() (Seeker, error) {
	if s.order == modelv2.QueryOrder_SORT_UNSPECIFIED {
		s.order = modelv2.QueryOrder_SORT_DESC
	}
	indexFilter, err := s.buildIndexFilter()
	if err != nil {
		return nil, err
	}
	filters := make([]filterFn, 0, 2)
	if indexFilter != nil {
		filters = append(filters, indexFilter)
	}
	se, err := s.buildSeries(filters)
	if err != nil {
		return nil, err
	}
	return newSeeker(se), nil
}

func newSeekerBuilder(s *seriesSpan) SeekerBuilder {
	return &seekerBuilder{
		seriesSpan: s,
	}
}

var _ Seeker = (*seeker)(nil)

type seeker struct {
	series []Iterator
}

func (s *seeker) Seek() ([]Iterator, error) {
	return s.series, nil
}

func newSeeker(series []Iterator) Seeker {
	return &seeker{
		series: series,
	}
}

var _ Item = (*item)(nil)

type item struct {
	itemID      common.ItemID
	data        kv.TimeSeriesReader
	seriesID    common.SeriesID
	sortedField []byte
}

func (i *item) Time() uint64 {
	return uint64(i.itemID)
}

func (i *item) SortedField() []byte {
	return i.sortedField
}

func (i *item) Val(family string) ([]byte, error) {
	d := dataBucket{
		seriesID: i.seriesID,
		family:   []byte(family),
	}
	return i.data.Get(d.marshal(), uint64(i.itemID))
}

func (i *item) ID() common.ItemID {
	return i.itemID
}
