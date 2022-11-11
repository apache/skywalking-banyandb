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
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/multierr"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/tsdb/bucket"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

const (
	defaultBlockQueueSize    = 4
	defaultMaxBlockQueueSize = 64
	defaultKVMemorySize      = 1 << 20
)

var _ Shard = (*shard)(nil)

type shard struct {
	l        *logger.Logger
	id       common.ShardID
	position common.Position

	seriesDatabase        SeriesDatabase
	indexDatabase         IndexDatabase
	segmentController     *segmentController
	segmentManageStrategy *bucket.Strategy
	retentionController   *retentionController

	closeOnce sync.Once
	closer    *run.Closer
}

func OpenShard(ctx context.Context, id common.ShardID,
	root string, segmentSize, blockSize, ttl IntervalRule, openedBlockSize, maxOpenedBlockSize int,
) (Shard, error) {
	path, err := mkdir(shardTemplate, root, int(id))
	if err != nil {
		return nil, errors.Wrapf(err, "make the directory of the shard %d ", int(id))
	}
	l := logger.Fetch(ctx, "shard"+strconv.Itoa(int(id)))
	l.Info().Int("shard_id", int(id)).Str("path", path).Msg("creating a shard")
	if openedBlockSize < 1 {
		openedBlockSize = defaultBlockQueueSize
	}
	shardCtx := context.WithValue(ctx, logger.ContextKey, l)
	shardCtx = common.SetPosition(shardCtx, func(p common.Position) common.Position {
		p.Shard = strconv.Itoa(int(id))
		return p
	})
	sc, err := newSegmentController(shardCtx, path, segmentSize, blockSize, openedBlockSize, maxOpenedBlockSize, l)
	if err != nil {
		return nil, errors.Wrapf(err, "create the segment controller of the shard %d", int(id))
	}
	s := &shard{
		id:                id,
		segmentController: sc,
		l:                 l,
		closer:            run.NewCloser(1),
	}
	err = s.segmentController.open()
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
	position := shardCtx.Value(common.PositionKey)
	if position != nil {
		s.position = position.(common.Position)
	}
	s.runStat()
	if s.retentionController, err = newRetentionController(s.segmentController, ttl); err != nil {
		return nil, err
	}
	s.retentionController.start()
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
	shardState.StrategyManagers = append(shardState.StrategyManagers, s.segmentManageStrategy.String())
	for _, seg := range s.segmentController.segments() {
		if seg.blockManageStrategy != nil {
			shardState.StrategyManagers = append(shardState.StrategyManagers, seg.blockManageStrategy.String())
		}
		for _, b := range seg.blockController.blocks() {
			shardState.Blocks = append(shardState.Blocks, BlockState{
				ID: BlockID{
					SegID:   b.segID,
					BlockID: b.blockID,
				},
				TimeRange: b.TimeRange,
				Closed:    b.Closed(),
			})
		}
	}
	all := s.segmentController.blockQueue.All()
	shardState.OpenBlocks = make([]BlockID, len(all))
	for i, v := range s.segmentController.blockQueue.All() {
		shardState.OpenBlocks[i] = v.(BlockID)
	}
	sort.Slice(shardState.OpenBlocks, func(i, j int) bool {
		x := shardState.OpenBlocks[i]
		y := shardState.OpenBlocks[j]
		if x.SegID == y.SegID {
			return x.BlockID < y.BlockID
		}
		return x.SegID < y.SegID
	})
	s.l.Debug().Interface("", shardState).Msg("shard state")
	return shardState
}

func (s *shard) Close() (err error) {
	s.closeOnce.Do(func() {
		s.closer.CloseThenWait()
		s.retentionController.stop()
		s.segmentManageStrategy.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err = multierr.Combine(s.segmentController.close(ctx), s.seriesDatabase.Close())
	})
	return err
}

type IntervalUnit int

const (
	HOUR IntervalUnit = iota
	DAY
)

func (iu IntervalUnit) String() string {
	switch iu {
	case HOUR:
		return "hour"
	case DAY:
		return "day"
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
	}
	panic("invalid interval unit")
}

func (ir IntervalRule) PreviousTime(current time.Time) time.Time {
	switch ir.Unit {
	case HOUR:
		return current.Add(-time.Hour * time.Duration(ir.Num))
	case DAY:
		return current.AddDate(0, 0, -ir.Num)
	}
	panic("invalid interval unit")
}

func (ir IntervalRule) EstimatedDuration() time.Duration {
	switch ir.Unit {
	case HOUR:
		return time.Hour * time.Duration(ir.Num)
	case DAY:
		return 24 * time.Hour * time.Duration(ir.Num)
	}
	panic("invalid interval unit")
}

type segmentController struct {
	sync.RWMutex
	shardCtx    context.Context
	location    string
	segmentSize IntervalRule
	blockSize   IntervalRule
	lst         []*segment
	blockQueue  bucket.Queue
	clock       timestamp.Clock

	l *logger.Logger
}

func newSegmentController(shardCtx context.Context, location string, segmentSize, blockSize IntervalRule,
	openedBlockSize, maxOpenedBlockSize int, l *logger.Logger,
) (*segmentController, error) {
	clock, _ := timestamp.GetClock(shardCtx)
	sc := &segmentController{
		shardCtx:    shardCtx,
		location:    location,
		segmentSize: segmentSize,
		blockSize:   blockSize,
		l:           l,
		clock:       clock,
	}
	var err error
	sc.blockQueue, err = bucket.NewQueue(
		l.Named("block-queue"),
		openedBlockSize,
		maxOpenedBlockSize,
		clock,
		func(ctx context.Context, id interface{}) error {
			bsID := id.(BlockID)
			seg := sc.get(bsID.SegID)
			if seg == nil {
				l.Warn().Int("segID", parseSuffix(bsID.SegID)).Msg("segment is absent")
				return nil
			}
			l.Info().Uint16("blockID", bsID.BlockID).Int("segID", parseSuffix(bsID.SegID)).Msg("closing the block")
			return seg.closeBlock(ctx, bsID.BlockID)
		})
	return sc, err
}

func (sc *segmentController) get(segID uint16) *segment {
	lst := sc.segments()
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
	lst := sc.segments()
	last := len(lst) - 1
	for i := range lst {
		s := lst[last-i]
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

func (sc *segmentController) Current() (bucket.Reporter, error) {
	now := sc.clock.Now()
	ns := uint64(now.UnixNano())
	if b := func() bucket.Reporter {
		sc.RLock()
		defer sc.RUnlock()
		for _, s := range sc.lst {
			if s.Contains(ns) {
				return s
			}
		}
		return nil
	}(); b != nil {
		return b, nil
	}
	return sc.create(sc.Format(now), true)
}

func (sc *segmentController) Next() (bucket.Reporter, error) {
	c, err := sc.Current()
	if err != nil {
		return nil, err
	}
	seg := c.(*segment)
	reporter, err := sc.create(sc.Format(
		sc.segmentSize.NextTime(seg.Start)), true)
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
	}
	panic("invalid interval unit")
}

func (sc *segmentController) Parse(value string) (time.Time, error) {
	switch sc.segmentSize.Unit {
	case HOUR:
		return time.ParseInLocation(segHourFormat, value, time.Local)
	case DAY:
		return time.ParseInLocation(segDayFormat, value, time.Local)
	}
	panic("invalid interval unit")
}

func (sc *segmentController) open() error {
	return WalkDir(
		sc.location,
		segPathPrefix,
		func(suffix, absolutePath string) error {
			sc.Lock()
			defer sc.Unlock()
			_, err := sc.load(suffix, absolutePath, false)
			if errors.Is(err, ErrEndOfSegment) {
				return nil
			}
			return err
		})
}

func (sc *segmentController) create(suffix string, createBlockIfEmpty bool) (*segment, error) {
	sc.Lock()
	defer sc.Unlock()
	for _, s := range sc.lst {
		if s.suffix == suffix {
			return s, nil
		}
	}
	segPath, err := mkdir(segTemplate, sc.location, suffix)
	if err != nil {
		return nil, err
	}
	return sc.load(suffix, segPath, createBlockIfEmpty)
}

func (sc *segmentController) sortLst() {
	sort.Slice(sc.lst, func(i, j int) bool {
		return sc.lst[i].id < sc.lst[j].id
	})
}

func (sc *segmentController) load(suffix, path string, createBlockIfEmpty bool) (seg *segment, err error) {
	startTime, err := sc.Parse(suffix)
	if err != nil {
		return nil, err
	}
	seg, err = openSegment(common.SetPosition(sc.shardCtx, func(p common.Position) common.Position {
		p.Segment = suffix
		return p
	}), startTime, path, suffix, sc.segmentSize, sc.blockSize, sc.blockQueue)
	if err != nil {
		return nil, err
	}
	sc.lst = append(sc.lst, seg)
	sc.sortLst()
	return seg, nil
}

func (sc *segmentController) remove(ctx context.Context, deadline time.Time) (err error) {
	sc.l.Info().Time("deadline", deadline).Msg("start to remove before deadline")
	for _, s := range sc.segments() {
		if s.End.Before(deadline) || s.Contains(uint64(deadline.UnixNano())) {
			sc.l.Debug().Stringer("segment", s).Msg("start to remove data in a segment")
			err = multierr.Append(err, s.blockController.remove(ctx, deadline))
			if s.End.Before(deadline) {
				sc.Lock()
				if errDel := s.delete(ctx); errDel != nil {
					err = multierr.Append(err, errDel)
				} else {
					sc.removeSeg(s.id)
				}
				sc.Unlock()
			}
		}
	}
	return err
}

func (sc *segmentController) removeSeg(segID uint16) {
	for i, b := range sc.lst {
		if b.id == segID {
			sc.lst = append(sc.lst[:i], sc.lst[i+1:]...)
			break
		}
	}
}

func (sc *segmentController) close(ctx context.Context) (err error) {
	sc.Lock()
	defer sc.Unlock()
	for _, s := range sc.lst {
		err = multierr.Append(err, s.close(ctx))
	}
	err = multierr.Append(err, sc.blockQueue.Close())
	sc.lst = sc.lst[:0]
	return err
}
