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
	"github.com/apache/skywalking-banyandb/pkg/convert"
)

var (
	ErrEmptySeriesSpan = errors.New("there is no data in such time range")
	ErrItemIDMalformed = errors.New("serialized item id is malformed")
)

type GlobalItemID struct {
	shardID  common.ShardID
	segID    uint16
	blockID  uint16
	seriesID common.SeriesID
	id       common.ItemID
}

func (i *GlobalItemID) Marshal() []byte {
	return bytes.Join([][]byte{
		convert.Uint32ToBytes(uint32(i.shardID)),
		convert.Uint16ToBytes(i.segID),
		convert.Uint16ToBytes(i.blockID),
		convert.Uint64ToBytes(uint64(i.seriesID)),
		convert.Uint64ToBytes(uint64(i.id)),
	}, nil)
}

func (i *GlobalItemID) UnMarshal(data []byte) error {
	if len(data) <= 32+16+16+64+64 {
		return ErrItemIDMalformed
	}
	var offset int
	i.shardID = common.ShardID(convert.BytesToUint32(data[offset : offset+4]))
	offset += 4
	i.segID = convert.BytesToUint16(data[offset : offset+2])
	offset += 2
	i.blockID = convert.BytesToUint16(data[offset : offset+2])
	offset += 2
	i.seriesID = common.SeriesID(convert.BytesToUint64(data[offset : offset+8]))
	offset += 8
	i.id = common.ItemID(convert.BytesToUint64(data[offset:]))
	return nil
}

type TimeRange struct {
	Start    time.Time
	Duration time.Duration
	End      time.Time
}

func (t TimeRange) contains(unixNano uint64) bool {
	tp := time.Unix(0, int64(unixNano))
	if tp.Equal(t.End) || tp.After(t.End) {
		return false
	}
	return tp.Equal(t.Start) || tp.After(t.Start)
}

func NewTimeRange(Start time.Time, Duration time.Duration) TimeRange {
	return TimeRange{
		Start:    Start,
		Duration: Duration,
		End:      Start.Add(Duration),
	}
}

type Series interface {
	ID() common.SeriesID
	Span(timeRange TimeRange) (SeriesSpan, error)
	Get(id GlobalItemID) (Item, error)
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
}

func (s *series) Get(id GlobalItemID) (Item, error) {
	panic("implement me")
}

func newSeries(id common.SeriesID, blockDB blockDatabase) *series {
	return &series{
		id:      id,
		blockDB: blockDB,
		shardID: blockDB.shardID(),
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
	return newSeriesSpan(timeRange, blocks, s.id, s.shardID), nil
}

var _ SeriesSpan = (*seriesSpan)(nil)

type seriesSpan struct {
	blocks    []blockDelegate
	seriesID  common.SeriesID
	shardID   common.ShardID
	timeRange TimeRange
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

func newSeriesSpan(timeRange TimeRange, blocks []blockDelegate,
	id common.SeriesID, shardID common.ShardID) *seriesSpan {
	return &seriesSpan{
		blocks:    blocks,
		seriesID:  id,
		shardID:   shardID,
		timeRange: timeRange,
	}
}
