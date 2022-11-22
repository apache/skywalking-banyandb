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
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

var (
	ErrEmptySeriesSpan = errors.New("there is no data in such time range")
	ErrItemIDMalformed = errors.New("serialized item id is malformed")
	ErrBlockAbsent     = errors.New("block is absent")
)

type GlobalItemID struct {
	ShardID  common.ShardID
	segID    SectionID
	blockID  SectionID
	SeriesID common.SeriesID
	ID       common.ItemID
}

func (i *GlobalItemID) Marshal() []byte {
	return bytes.Join([][]byte{
		convert.Uint32ToBytes(uint32(i.ShardID)),
		sectionIDToBytes(i.segID),
		sectionIDToBytes(i.blockID),
		convert.Uint64ToBytes(uint64(i.SeriesID)),
		convert.Uint64ToBytes(uint64(i.ID)),
	}, nil)
}

func (i *GlobalItemID) UnMarshal(data []byte) error {
	if len(data) != 4+4+4+8+8 {
		return ErrItemIDMalformed
	}
	var offset int
	i.ShardID = common.ShardID(convert.BytesToUint32(data[offset : offset+4]))
	offset += 4
	i.segID, offset = readSectionID(data, offset)
	i.blockID, offset = readSectionID(data, offset)
	i.SeriesID = common.SeriesID(convert.BytesToUint64(data[offset : offset+8]))
	offset += 8
	i.ID = common.ItemID(convert.BytesToUint64(data[offset:]))
	return nil
}

type Series interface {
	ID() common.SeriesID
	Span(ctx context.Context, timeRange timestamp.TimeRange) (SeriesSpan, error)
	Create(ctx context.Context, t time.Time) (SeriesSpan, error)
	Get(ctx context.Context, id GlobalItemID) (Item, io.Closer, error)
	String() string
}

type SeriesSpan interface {
	io.Closer
	WriterBuilder() WriterBuilder
	SeekerBuilder() SeekerBuilder
}

var _ Series = (*series)(nil)

type series struct {
	id        common.SeriesID
	idLiteral string
	blockDB   blockDatabase
	shardID   common.ShardID
	l         *logger.Logger
}

func (s *series) Get(ctx context.Context, id GlobalItemID) (Item, io.Closer, error) {
	b, err := s.blockDB.block(ctx, id)
	if err != nil {
		return nil, nil, err
	}
	if b == nil {
		return nil, nil, errors.WithMessagef(ErrBlockAbsent, "id: %v", id)
	}
	return &item{
		data:        b.dataReader(),
		itemID:      id.ID,
		seriesID:    s.id,
		decoderPool: b.decoderPool(),
	}, b, nil
}

func (s *series) ID() common.SeriesID {
	return s.id
}

func (s *series) String() string {
	return s.idLiteral
}

func (s *series) Span(ctx context.Context, timeRange timestamp.TimeRange) (SeriesSpan, error) {
	blocks, err := s.blockDB.span(ctx, timeRange)
	if err != nil {
		return nil, err
	}
	if len(blocks) < 1 {
		return nil, ErrEmptySeriesSpan
	}
	l := logger.FetchOrDefault(ctx, "series", s.l)
	if e := l.Debug(); e.Enabled() {
		e.Times("time_range", []time.Time{timeRange.Start, timeRange.End}).
			Uint64("series_id", uint64(s.id)).
			Str("series", s.idLiteral).
			Msg("select series span")
	}
	return newSeriesSpan(context.WithValue(context.Background(), logger.ContextKey, l), timeRange, blocks,
		s.id, s.idLiteral, s.shardID), nil
}

func (s *series) Create(ctx context.Context, t time.Time) (SeriesSpan, error) {
	tr := timestamp.NewInclusiveTimeRange(t, t)
	blocks, err := s.blockDB.span(ctx, tr)
	if err != nil {
		return nil, err
	}
	if len(blocks) > 0 {
		if e := s.l.Debug(); e.Enabled() {
			e.Time("time", t).
				Uint64("series_id", uint64(s.id)).
				Str("series", s.idLiteral).
				Msg("load a series span")
		}
		return newSeriesSpan(context.WithValue(context.Background(), logger.ContextKey, s.l), tr, blocks,
			s.id, s.idLiteral, s.shardID), nil
	}
	b, err := s.blockDB.create(ctx, t)
	if err != nil {
		return nil, err
	}
	blocks = append(blocks, b)
	if e := s.l.Debug(); e.Enabled() {
		e.Time("time", t).
			Uint64("series_id", uint64(s.id)).
			Str("series", s.idLiteral).
			Msg("create a series span")
	}
	return newSeriesSpan(context.WithValue(context.Background(), logger.ContextKey, s.l), tr, blocks,
		s.id, s.idLiteral, s.shardID), nil
}

func newSeries(ctx context.Context, id common.SeriesID, idLiteral string, blockDB blockDatabase) *series {
	s := &series{
		id:        id,
		idLiteral: idLiteral,
		blockDB:   blockDB,
		shardID:   blockDB.shardID(),
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
	blocks    []BlockDelegate
	seriesID  common.SeriesID
	series    string
	shardID   common.ShardID
	timeRange timestamp.TimeRange
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

func newSeriesSpan(ctx context.Context, timeRange timestamp.TimeRange, blocks []BlockDelegate, id common.SeriesID, series string, shardID common.ShardID) *seriesSpan {
	s := &seriesSpan{
		blocks:    blocks,
		seriesID:  id,
		series:    series,
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
