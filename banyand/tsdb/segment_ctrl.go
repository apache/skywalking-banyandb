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
	"sync"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/multierr"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/tsdb/bucket"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

type segmentController struct {
	sync.RWMutex
	shardCtx    context.Context
	location    string
	segmentSize IntervalRule
	blockSize   IntervalRule
	lst         []*segment
	blockQueue  bucket.Queue
	scheduler   *timestamp.Scheduler
	clock       timestamp.Clock

	l *logger.Logger
}

func newSegmentController(shardCtx context.Context, location string, segmentSize, blockSize IntervalRule,
	openedBlockSize, maxOpenedBlockSize int, l *logger.Logger, scheduler *timestamp.Scheduler,
) (*segmentController, error) {
	clock, _ := timestamp.GetClock(shardCtx)
	sc := &segmentController{
		shardCtx:    shardCtx,
		location:    location,
		segmentSize: segmentSize,
		blockSize:   blockSize,
		l:           l,
		clock:       clock,
		scheduler:   scheduler,
	}
	var err error
	sc.blockQueue, err = bucket.NewQueue(
		l.Named("block-queue"),
		openedBlockSize,
		maxOpenedBlockSize,
		scheduler,
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
	}), startTime, path, suffix, sc.segmentSize, sc.blockSize, sc.blockQueue, sc.scheduler)
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
	sc.lst = sc.lst[:0]
	return err
}
