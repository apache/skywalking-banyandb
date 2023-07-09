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
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/kv"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// Iterator allows iterating a series in a time span.
type Iterator interface {
	Next() bool
	Val() Item
	Close() error
}

// Item allows retrieving raw data from an item.
type Item interface {
	Family(family []byte) ([]byte, error)
	Val() ([]byte, error)
	ID() common.ItemID
	SortedField() []byte
	Time() uint64
}

// SeekerBuilder a helper to build a Seeker.
type SeekerBuilder interface {
	Filter(predicator index.Filter) SeekerBuilder
	OrderByIndex(indexRule *databasev1.IndexRule, order modelv1.Sort) SeekerBuilder
	OrderByTime(order modelv1.Sort) SeekerBuilder
	Build() (Seeker, error)
}

// Seeker allows searching data in a Database.
type Seeker interface {
	Seek() ([]Iterator, error)
}

var _ SeekerBuilder = (*seekerBuilder)(nil)

type seekerBuilder struct {
	predicator          index.Filter
	seriesSpan          *seriesSpan
	indexRuleForSorting *databasev1.IndexRule
	l                   *logger.Logger
	order               modelv1.Sort
}

func (s *seekerBuilder) Build() (Seeker, error) {
	if s.order == modelv1.Sort_SORT_UNSPECIFIED {
		s.order = modelv1.Sort_SORT_ASC
	}
	se, err := s.buildSeries()
	if err != nil {
		return nil, err
	}
	return newSeeker(se), nil
}

func newSeekerBuilder(s *seriesSpan) SeekerBuilder {
	return &seekerBuilder{
		seriesSpan: s,
		l:          logger.GetLogger("seeker-builder"),
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
	data        kv.TimeSeriesReader
	sortedField []byte
	itemID      common.ItemID
	seriesID    common.SeriesID
}

func (i *item) Time() uint64 {
	return uint64(i.itemID)
}

func (i *item) SortedField() []byte {
	return i.sortedField
}

func (i *item) Family(family []byte) ([]byte, error) {
	d := dataBucket{
		seriesID: i.seriesID,
		family:   family,
	}
	return i.data.Get(d.marshal(), uint64(i.itemID))
}

func (i *item) Val() ([]byte, error) {
	d := dataBucket{
		seriesID: i.seriesID,
	}
	return i.data.Get(d.marshal(), uint64(i.itemID))
}

func (i *item) ID() common.ItemID {
	return i.itemID
}
