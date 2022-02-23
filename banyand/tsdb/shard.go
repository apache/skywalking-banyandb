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
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

const defaultBlockQueueSize = 1 << 4

var _ Shard = (*shard)(nil)

type shard struct {
	l  *logger.Logger
	id common.ShardID

	seriesDatabase        SeriesDatabase
	indexDatabase         IndexDatabase
	segmentController     *segmentController
	segmentManageStrategy *bucket.Strategy
}

func OpenShard(ctx context.Context, id common.ShardID,
	root string, segmentSize, blockSize IntervalRule, openedBlockSize int) (Shard, error) {
	path, err := mkdir(shardTemplate, root, int(id))
	if err != nil {
		return nil, errors.Wrapf(err, "make the directory of the shard %d ", int(id))
	}
	l := logger.Fetch(ctx, "shard"+strconv.Itoa(int(id)))
	l.Info().Int("shard_id", int(id)).Str("path", path).Msg("creating a shard")
	if openedBlockSize < 1 {
		openedBlockSize = defaultBlockQueueSize
	}
	sc, err := newSegmentController(path, segmentSize, blockSize, openedBlockSize, l)
	if err != nil {
		return nil, errors.Wrapf(err, "create the segment controller of the shard %d", int(id))
	}
	s := &shard{
		id:                id,
		segmentController: sc,
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

func (s *shard) ID() common.ShardID {
	return s.id
}

func (s *shard) Series() SeriesDatabase {
	return s.seriesDatabase
}

func (s *shard) Index() IndexDatabase {
	return s.indexDatabase
}

func (s *shard) State() (shardState ShardState) {
	for _, seg := range s.segmentController.segments() {
		for _, b := range seg.blockController.blocks() {
			shardState.OpenedBlocks = append(shardState.OpenedBlocks, BlockState{
				ID: BlockID{
					SegID:   b.segID,
					BlockID: b.blockID,
				},
				TimeRange: b.TimeRange,
				Closed:    b.isClosed(),
			})
		}
	}
	return shardState
}

func (s *shard) Close() error {
	s.segmentManageStrategy.Close()
	s.segmentController.close()
	return s.seriesDatabase.Close()
}

type IntervalUnit int

const (
	MILLISECOND IntervalUnit = iota // only for testing
	HOUR
	DAY
)

func (iu IntervalUnit) String() string {
	switch iu {
	case HOUR:
		return "hour"
	case DAY:
		return "day"
	case MILLISECOND:
		return "millis"

	}
	panic("invalid interval unit")
}

type IntervalRule struct {
	Unit IntervalUnit
	Num  int
}

func (ir IntervalRule) NextTime(current time.Time) time.Time {
	switch ir.Unit {
	case HOUR:
		return current.Add(time.Hour * time.Duration(ir.Num))
	case DAY:
		return current.AddDate(0, 0, ir.Num)
	case MILLISECOND:
		return current.Add(time.Millisecond * time.Duration(ir.Num))
	}
	panic("invalid interval unit")
}

func (ir IntervalRule) EstimatedDuration() time.Duration {
	switch ir.Unit {
	case HOUR:
		return time.Hour * time.Duration(ir.Num)
	case DAY:
		return 24 * time.Hour * time.Duration(ir.Num)
	case MILLISECOND:
		return time.Microsecond * time.Duration(ir.Num)
	}
	panic("invalid interval unit")
}

type segmentController struct {
	sync.RWMutex
	location    string
	segmentSize IntervalRule
	blockSize   IntervalRule
	lst         []*segment
	blockQueue  bucket.Queue

	l *logger.Logger
}

func newSegmentController(location string, segmentSize, blockSize IntervalRule, openedBlockSize int, l *logger.Logger) (*segmentController, error) {
	sc := &segmentController{
		location:    location,
		segmentSize: segmentSize,
		blockSize:   blockSize,
		l:           l,
	}
	var err error
	sc.blockQueue, err = bucket.NewQueue(openedBlockSize, func(id interface{}) {
		bsID := id.(BlockID)
		seg := sc.get(bsID.SegID)
		if seg == nil {
			l.Warn().Uint16("segID", bsID.SegID).Msg("segment is absent")
			return
		}
		l.Info().Uint16("blockID", bsID.BlockID).Uint16("segID", bsID.SegID).Msg("closing the block")
		seg.closeBlock(bsID.BlockID)
	})
	return sc, err
}

func (sc *segmentController) get(segID uint16) *segment {
	lst := sc.snapshotLst()
	last := len(lst) - 1
	for i := range lst {
		s := lst[last-i]
		if s.id == segID {
			return s
		}
	}
	return nil
}

func (sc *segmentController) span(timeRange timestamp.TimeRange) (ss []*segment) {
	lst := sc.snapshotLst()
	last := len(lst) - 1
	for i := range lst {
		s := lst[last-i]
		if s.Overlapping(timeRange) {
			ss = append(ss, s)
		}
	}
	return ss
}

func (sc *segmentController) snapshotLst() []*segment {
	sc.RLock()
	defer sc.RUnlock()
	return sc.lst
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
		if s.suffix == sc.Format(now) {
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
	seg := sc.Current().(*segment)
	reporter, err := sc.create(context.TODO(), sc.Format(
		sc.segmentSize.NextTime(seg.Start)))
	if errors.Is(err, ErrEndOfSegment) {
		return nil, bucket.ErrNoMoreBucket
	}
	return reporter, err
}

func (sc *segmentController) OnMove(prev bucket.Reporter, next bucket.Reporter) {
	event := sc.l.Info()
	if prev != nil {
		event.Stringer("prev", prev)
	}
	if next != nil {
		event.Stringer("next", next)
	}
	event.Msg("move to the next segment")
}

func (sc *segmentController) Format(tm time.Time) string {
	switch sc.segmentSize.Unit {
	case HOUR:
		return tm.Format(segHourFormat)
	case DAY:
		return tm.Format(segDayFormat)
	case MILLISECOND:
		return tm.Format(millisecondFormat)
	}
	panic("invalid interval unit")
}

func (sc *segmentController) Parse(value string) (time.Time, error) {
	switch sc.segmentSize.Unit {
	case HOUR:
		return time.ParseInLocation(segHourFormat, value, time.Local)
	case DAY:
		return time.ParseInLocation(segDayFormat, value, time.Local)
	case MILLISECOND:
		return time.ParseInLocation(millisecondFormat, value, time.Local)
	}
	panic("invalid interval unit")
}

func (sc *segmentController) open(ctx context.Context) error {
	err := WalkDir(
		sc.location,
		segPathPrefix,
		func(suffix, absolutePath string) error {
			_, err := sc.load(ctx, suffix, absolutePath)
			if errors.Is(err, ErrEndOfSegment) {
				return nil
			}
			return err
		})
	if err != nil {
		return err
	}
	if sc.Current() == nil {
		_, err = sc.create(ctx, sc.Format(time.Now()))
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
	startTime, err := sc.Parse(suffix)
	if err != nil {
		return nil, err
	}
	seg, err = openSegment(ctx, startTime, path, suffix, sc.segmentSize, sc.blockSize, sc.blockQueue)
	if err != nil {
		return nil, err
	}
	sc.Lock()
	defer sc.Unlock()
	sc.lst = append(sc.lst, seg)
	return seg, nil
}

func (sc *segmentController) close() {
	for _, s := range sc.lst {
		s.close()
	}
}
