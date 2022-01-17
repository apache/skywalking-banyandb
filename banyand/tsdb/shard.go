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
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/tsdb/bucket"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

var _ Shard = (*shard)(nil)

type shard struct {
	l  *logger.Logger
	id common.ShardID

	seriesDatabase        SeriesDatabase
	indexDatabase         IndexDatabase
	segmentController     *segmentController
	segmentManageStrategy *bucket.Strategy
}

func (s *shard) ID() common.ShardID {
	return s.id
}

func (s *shard) Series() SeriesDatabase {
	return s.seriesDatabase
}

func (s *shard) Index() IndexDatabase {
	return s.indexDatabase
}

func OpenShard(ctx context.Context, id common.ShardID, root string, intervalRule IntervalRule) (Shard, error) {
	path, err := mkdir(shardTemplate, root, int(id))
	if err != nil {
		return nil, errors.Wrapf(err, "make the directory of the shard %d ", int(id))
	}
	l := logger.Fetch(ctx, "shard"+strconv.Itoa(int(id)))
	l.Info().Int("shard_id", int(id)).Str("path", path).Msg("creating a shard")
	s := &shard{
		id:                id,
		segmentController: newSegmentController(path, intervalRule),
		l:                 l,
	}
	shardCtx := context.WithValue(ctx, logger.ContextKey, s.l)
	err = s.segmentController.open(shardCtx)
	if err != nil {
		return nil, err
	}
	seriesPath, err := mkdir(seriesTemplate, path)
	if err != nil {
		return nil, err
	}
	sdb, err := newSeriesDataBase(shardCtx, s.id, seriesPath, s.segmentController)
	if err != nil {
		return nil, err
	}
	s.seriesDatabase = sdb
	idb, err := newIndexDatabase(shardCtx, s.id, s.segmentController)
	if err != nil {
		return nil, err
	}
	s.indexDatabase = idb
	s.segmentManageStrategy, err = bucket.NewStrategy(s.segmentController, bucket.WithLogger(s.l))
	if err != nil {
		return nil, err
	}
	s.segmentManageStrategy.Run()
	return s, nil
}

func (s *shard) Close() error {
	s.segmentManageStrategy.Close()
	s.segmentController.close()
	return s.seriesDatabase.Close()
}

type IntervalUnit int

const (
	DAY IntervalUnit = iota
	MONTH
	YEAR
	MILLISECOND // only for testing
)

func (iu IntervalUnit) String() string {
	switch iu {
	case DAY:
		return "day"
	case MONTH:
		return "month"
	case YEAR:
		return "year"
	case MILLISECOND:
		return "millis"

	}
	panic("invalid interval unit")
}

func (iu IntervalUnit) Format(tm time.Time) string {
	switch iu {
	case DAY:
		return tm.Format(segDayFormat)
	case MONTH:
		return tm.Format(segMonthFormat)
	case YEAR:
		return tm.Format(segYearFormat)
	case MILLISECOND:
		return tm.Format(segMillisecondFormat)
	}
	panic("invalid interval unit")
}

func (iu IntervalUnit) Parse(value string) (time.Time, error) {
	switch iu {
	case DAY:
		return time.Parse(segDayFormat, value)
	case MONTH:
		return time.Parse(segMonthFormat, value)
	case YEAR:
		return time.Parse(segYearFormat, value)
	case MILLISECOND:
		return time.Parse(segMillisecondFormat, value)
	}
	panic("invalid interval unit")
}

type IntervalRule struct {
	Unit IntervalUnit
	Num  int
}

func (ir IntervalRule) NextTime(current time.Time) time.Time {
	switch ir.Unit {
	case DAY:
		return current.AddDate(0, 0, ir.Num)
	case MONTH:
		return current.AddDate(0, ir.Num, 0)
	case YEAR:
		return current.AddDate(ir.Num, 0, 0)
	case MILLISECOND:
		return current.Add(time.Millisecond * time.Duration(ir.Num))
	}
	panic("invalid interval unit")
}

type segmentController struct {
	sync.RWMutex
	location     string
	intervalRule IntervalRule
	lst          []*segment
}

func newSegmentController(location string, intervalRule IntervalRule) *segmentController {
	return &segmentController{
		location:     location,
		intervalRule: intervalRule,
	}
}

func (sc *segmentController) get(segID uint16) *segment {
	sc.RLock()
	defer sc.RUnlock()
	last := len(sc.lst) - 1
	for i := range sc.lst {
		s := sc.lst[last-i]
		if s.id == segID {
			return s
		}
	}
	return nil
}

func (sc *segmentController) span(timeRange TimeRange) (ss []*segment) {
	sc.RLock()
	defer sc.RUnlock()
	last := len(sc.lst) - 1
	for i := range sc.lst {
		s := sc.lst[last-i]
		if s.Overlapping(timeRange) {
			ss = append(ss, s)
		}
	}
	return ss
}

func (sc *segmentController) segments() (ss []*segment) {
	sc.RLock()
	defer sc.RUnlock()
	r := make([]*segment, len(sc.lst))
	copy(r, sc.lst)
	return r
}

func (sc *segmentController) Current() bucket.Reporter {
	sc.RLock()
	defer sc.RUnlock()
	now := time.Now()
	for _, s := range sc.lst {
		if s.suffix == sc.intervalRule.Unit.Format(now) {
			return s
		}
	}
	// return the latest segment before now
	if len(sc.lst) > 0 {
		return sc.lst[len(sc.lst)-1]
	}
	return nil
}

func (sc *segmentController) Next() (bucket.Reporter, error) {
	return sc.create(context.TODO(), sc.intervalRule.Unit.Format(
		sc.intervalRule.NextTime(time.Now())))
}

func (sc *segmentController) open(ctx context.Context) error {
	err := walkDir(
		sc.location,
		segPathPrefix,
		func(suffix, absolutePath string) error {
			_, err := sc.load(ctx, suffix, absolutePath)
			return err
		})
	if err != nil {
		return err
	}
	if sc.Current() == nil {
		_, err = sc.create(ctx, sc.intervalRule.Unit.Format(time.Now()))
		if err != nil {
			return err
		}
	}
	return nil
}

func (sc *segmentController) create(ctx context.Context, suffix string) (*segment, error) {
	segPath, err := mkdir(segTemplate, sc.location, suffix)
	if err != nil {
		return nil, err
	}
	return sc.load(ctx, suffix, segPath)
}

func (sc *segmentController) load(ctx context.Context, suffix, path string) (seg *segment, err error) {
	seg, err = openSegment(ctx, suffix, path, sc.intervalRule)
	if err != nil {
		return nil, err
	}
	{
		sc.Lock()
		defer sc.Unlock()
		sc.lst = append(sc.lst, seg)
	}
	return seg, nil
}

func (sc *segmentController) close() {
	for _, s := range sc.lst {
		s.close()
	}
}
