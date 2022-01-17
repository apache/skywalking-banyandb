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
	"context"
	"io"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/multierr"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

var (
	ErrEmptySeriesSpan = errors.New("there is no data in such time range")
	ErrItemIDMalformed = errors.New("serialized item id is malformed")
	ErrBlockAbsent     = errors.New("block is absent")
)

type GlobalItemID struct {
	ShardID  common.ShardID
	segID    uint16
	blockID  uint16
	SeriesID common.SeriesID
	ID       common.ItemID
}

func (i *GlobalItemID) Marshal() []byte {
	return bytes.Join([][]byte{
		convert.Uint32ToBytes(uint32(i.ShardID)),
		convert.Uint16ToBytes(i.segID),
		convert.Uint16ToBytes(i.blockID),
		convert.Uint64ToBytes(uint64(i.SeriesID)),
		convert.Uint64ToBytes(uint64(i.ID)),
	}, nil)
}

func (i *GlobalItemID) UnMarshal(data []byte) error {
	if len(data) != 4+2+2+8+8 {
		return ErrItemIDMalformed
	}
	var offset int
	i.ShardID = common.ShardID(convert.BytesToUint32(data[offset : offset+4]))
	offset += 4
	i.segID = convert.BytesToUint16(data[offset : offset+2])
	offset += 2
	i.blockID = convert.BytesToUint16(data[offset : offset+2])
	offset += 2
	i.SeriesID = common.SeriesID(convert.BytesToUint64(data[offset : offset+8]))
	offset += 8
	i.ID = common.ItemID(convert.BytesToUint64(data[offset:]))
	return nil
}

type TimeRange struct {
	Start        time.Time
	End          time.Time
	IncludeStart bool
	IncludeEnd   bool
}

func (t TimeRange) Contains(unixNano uint64) bool {
	tp := time.Unix(0, int64(unixNano))
	if t.Start.Equal(tp) {
		return t.IncludeStart
	}
	if t.End.Equal(tp) {
		return t.IncludeEnd
	}
	return !tp.Before(t.Start) && !tp.After(t.End)
}

func (t TimeRange) Overlapping(other TimeRange) bool {
	if t.Start.Equal(other.End) {
		return t.IncludeStart && other.IncludeEnd
	}
	if other.Start.Equal(t.End) {
		return t.IncludeEnd && other.IncludeStart
	}
	return !t.Start.After(other.End) && !other.Start.After(t.End)
}

func (t TimeRange) Duration() time.Duration {
	return t.End.Sub(t.Start)
}

func NewInclusiveTimeRange(start, end time.Time) TimeRange {
	return TimeRange{
		Start:        start,
		End:          end,
		IncludeStart: true,
		IncludeEnd:   true,
	}
}

func NewInclusiveTimeRangeDuration(start time.Time, duration time.Duration) TimeRange {
	return NewTimeRangeDuration(start, duration, true, true)
}

func NewTimeRange(start, end time.Time, includeStart, includeEnd bool) TimeRange {
	return TimeRange{
		Start:        start,
		End:          end,
		IncludeStart: includeStart,
		IncludeEnd:   includeEnd,
	}
}

func NewTimeRangeDuration(start time.Time, duration time.Duration, includeStart, includeEnd bool) TimeRange {
	return NewTimeRange(start, start.Add(duration), includeStart, includeEnd)
}

type Series interface {
	ID() common.SeriesID
	Span(timeRange TimeRange) (SeriesSpan, error)
	Get(id GlobalItemID) (Item, io.Closer, error)
}

type SeriesSpan interface {
	io.Closer
	WriterBuilder() WriterBuilder
	SeekerBuilder() SeekerBuilder
}

var _ Series = (*series)(nil)

type series struct {
	id      common.SeriesID
	blockDB blockDatabase
	shardID common.ShardID
	l       *logger.Logger
}

func (s *series) Get(id GlobalItemID) (Item, io.Closer, error) {
	b := s.blockDB.block(id)
	if b == nil {
		return nil, nil, errors.WithMessagef(ErrBlockAbsent, "id: %v", id)
	}
	return &item{
		data:     b.dataReader(),
		itemID:   id.ID,
		seriesID: s.id,
	}, b, nil
}

func (s *series) ID() common.SeriesID {
	return s.id
}

func (s *series) Span(timeRange TimeRange) (SeriesSpan, error) {
	blocks := s.blockDB.span(timeRange)
	if len(blocks) < 1 {
		return nil, ErrEmptySeriesSpan
	}
	s.l.Debug().
		Times("time_range", []time.Time{timeRange.Start, timeRange.End}).
		Msg("select series span")
	return newSeriesSpan(context.WithValue(context.Background(), logger.ContextKey, s.l), timeRange, blocks, s.id, s.shardID), nil
}

func newSeries(ctx context.Context, id common.SeriesID, blockDB blockDatabase) *series {
	s := &series{
		id:      id,
		blockDB: blockDB,
		shardID: blockDB.shardID(),
	}
	parentLogger := ctx.Value(logger.ContextKey)
	if pl, ok := parentLogger.(*logger.Logger); ok {
		s.l = pl.Named("series")
	} else {
		s.l = logger.GetLogger("series")
	}
	return s
}

var _ SeriesSpan = (*seriesSpan)(nil)

type seriesSpan struct {
	blocks    []blockDelegate
	seriesID  common.SeriesID
	shardID   common.ShardID
	timeRange TimeRange
	l         *logger.Logger
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

func (s *seriesSpan) SeekerBuilder() SeekerBuilder {
	return newSeekerBuilder(s)
}

func newSeriesSpan(ctx context.Context, timeRange TimeRange, blocks []blockDelegate, id common.SeriesID, shardID common.ShardID) *seriesSpan {
	s := &seriesSpan{
		blocks:    blocks,
		seriesID:  id,
		shardID:   shardID,
		timeRange: timeRange,
	}
	parentLogger := ctx.Value(logger.ContextKey)
	if pl, ok := parentLogger.(*logger.Logger); ok {
		s.l = pl.Named("series_span")
	} else {
		s.l = logger.GetLogger("series_span")
	}
	return s
}
