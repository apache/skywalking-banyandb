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
	"encoding/hex"
	"time"

	"github.com/dgraph-io/badger/v3/y"

	"github.com/apache/skywalking-banyandb/api/common"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/kv"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

type Iterator interface {
	Next() bool
	Val() Item
	Close() error
}

type Item interface {
	Family(family []byte) ([]byte, error)
	PrintContext(l *logger.Logger, family []byte, n int)
	Val() ([]byte, error)
	ID() common.ItemID
	SortedField() []byte
	Time() uint64
}

type SeekerBuilder interface {
	Filter(predicator index.Filter) SeekerBuilder
	OrderByIndex(indexRule *databasev1.IndexRule, order modelv1.Sort) SeekerBuilder
	OrderByTime(order modelv1.Sort) SeekerBuilder
	Build() (Seeker, error)
}

type Seeker interface {
	Seek() ([]Iterator, error)
}

var _ SeekerBuilder = (*seekerBuilder)(nil)

type seekerBuilder struct {
	seriesSpan *seriesSpan

	predicator          index.Filter
	order               modelv1.Sort
	indexRuleForSorting *databasev1.IndexRule
	rangeOptsForSorting index.RangeOpts
	l                   *logger.Logger
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
	itemID      common.ItemID
	data        kv.TimeSeriesReader
	seriesID    common.SeriesID
	sortedField []byte
	decoderPool encoding.SeriesDecoderPool
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

func (i *item) PrintContext(l *logger.Logger, family []byte, n int) {
	decoder := i.decoderPool.Get(family)
	defer i.decoderPool.Put(decoder)
	d := dataBucket{
		seriesID: i.seriesID,
		family:   family,
	}
	key := d.marshal()
	pre, next := i.data.Context(key, uint64(i.itemID), n)
	defer pre.Close()
	defer next.Close()
	j := 0
	currentTS := uint64(i.itemID)

	each := func(iter kv.Iterator) {
		if !bytes.Equal(key, iter.Key()) {
			return
		}
		j++

		ts := y.ParseTs(iter.RawKey())

		logEvent := l.Info().Int("i", j).
			Time("ts", time.Unix(0, int64(y.ParseTs(iter.RawKey()))))
		if err := decoder.Decode(family, iter.Val()); err != nil {
			logEvent = logEvent.Str("loc", "mem")
			if ts == currentTS {
				logEvent = logEvent.Bool("at", true)
			}
		} else {
			start, end := decoder.Range()
			logEvent = logEvent.Time("start", time.Unix(0, int64(start))).
				Time("end", time.Unix(0, int64(end))).Int("num", decoder.Len()).Str("loc", "table")
			if start <= currentTS && currentTS <= end {
				if dd, err := decoder.Get(currentTS); err == nil && len(dd) > 0 {
					logEvent = logEvent.Bool("at", true)
				}
			}
		}
		logEvent.Send()
	}

	s := hex.EncodeToString(key)
	if len(s) > 7 {
		s = s[:7]
	}
	l.Info().Str("prefix", s).Time("ts", time.Unix(0, int64(i.itemID))).Msg("print previous lines")
	for ; pre.Valid() && j < n; pre.Next() {
		each(pre)
	}
	j = 0
	l.Info().Str("prefix", s).Time("ts", time.Unix(0, int64(i.itemID))).Msg("print next lines")
	for ; next.Valid() && j < n; next.Next() {
		each(next)
	}
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
