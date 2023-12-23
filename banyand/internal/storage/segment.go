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

package storage

import (
	"context"
	"errors"
	"fmt"
	"path"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/tsdb/bucket"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

var errEndOfSegment = errors.New("reached the end of the segment")

type segment[T TSTable] struct {
	bucket.Reporter
	tsTable  T
	l        *logger.Logger
	position common.Position
	timestamp.TimeRange
	path          string
	suffix        string
	refCount      int32
	mustBeDeleted uint32
	id            segmentID
}

func openSegment[T TSTable](ctx context.Context, startTime, endTime time.Time, path, suffix string,
	segmentSize IntervalRule, scheduler *timestamp.Scheduler, tsTable T,
) (s *segment[T], err error) {
	suffixInteger, err := strconv.Atoi(suffix)
	if err != nil {
		return nil, err
	}
	id := generateSegID(segmentSize.Unit, suffixInteger)
	timeRange := timestamp.NewSectionTimeRange(startTime, endTime)
	s = &segment[T]{
		id:        id,
		path:      path,
		suffix:    suffix,
		TimeRange: timeRange,
		position:  common.GetPosition(ctx),
		tsTable:   tsTable,
		refCount:  1,
	}
	l := logger.Fetch(ctx, s.String())
	s.l = l
	clock, _ := timestamp.GetClock(ctx)
	s.Reporter = bucket.NewTimeBasedReporter(s.String(), timeRange, clock, scheduler)
	return s, nil
}

func (s *segment[T]) incRef() {
	atomic.AddInt32(&s.refCount, 1)
}

func (s *segment[T]) DecRef() {
	n := atomic.AddInt32(&s.refCount, -1)
	if n > 0 {
		return
	}

	deletePath := ""
	if atomic.LoadUint32(&s.mustBeDeleted) != 0 {
		deletePath = s.path
	}

	if err := s.tsTable.Close(); err != nil {
		s.l.Panic().Err(err).Msg("failed to close tsTable")
	}

	if deletePath != "" {
		lfs.MustRMAll(deletePath)
	}
}

func (s *segment[T]) Table() T {
	return s.tsTable
}

func (s *segment[T]) GetTimeRange() timestamp.TimeRange {
	return s.TimeRange
}

func (s *segment[T]) delete() {
	atomic.StoreUint32(&s.mustBeDeleted, 1)
	s.DecRef()
}

func (s *segment[T]) String() string {
	return "SegID-" + s.suffix
}

type segmentController[T TSTable] struct {
	clock          timestamp.Clock
	scheduler      *timestamp.Scheduler
	l              *logger.Logger
	tsTableCreator TSTableCreator[T]
	position       common.Position
	location       string
	lst            []*segment[T]
	segmentSize    IntervalRule
	sync.RWMutex
}

func newSegmentController[T TSTable](ctx context.Context, location string,
	segmentSize IntervalRule, l *logger.Logger, scheduler *timestamp.Scheduler,
	tsTableCreator TSTableCreator[T],
) *segmentController[T] {
	clock, _ := timestamp.GetClock(ctx)
	return &segmentController[T]{
		location:       location,
		segmentSize:    segmentSize,
		l:              l,
		clock:          clock,
		scheduler:      scheduler,
		position:       common.GetPosition(ctx),
		tsTableCreator: tsTableCreator,
	}
}

func (sc *segmentController[T]) selectTSTables(timeRange timestamp.TimeRange) (tt []TSTableWrapper[T]) {
	sc.RLock()
	defer sc.RUnlock()
	last := len(sc.lst) - 1
	for i := range sc.lst {
		s := sc.lst[last-i]
		if s.Overlapping(timeRange) {
			s.incRef()
			tt = append(tt, s)
		}
	}
	return tt
}

func (sc *segmentController[T]) createTSTable(ts time.Time) (TSTableWrapper[T], error) {
	s, err := sc.create(ts)
	if err != nil {
		return nil, err
	}
	s.incRef()
	return s, nil
}

func (sc *segmentController[T]) segments() (ss []*segment[T]) {
	sc.RLock()
	defer sc.RUnlock()
	r := make([]*segment[T], len(sc.lst))
	for i := range sc.lst {
		sc.lst[i].incRef()
		r[i] = sc.lst[i]
	}
	return r
}

func (sc *segmentController[T]) Current() (bucket.Reporter, error) {
	now := sc.Standard(sc.clock.Now())
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
	return sc.create(now)
}

func (sc *segmentController[T]) Next() (bucket.Reporter, error) {
	c, err := sc.Current()
	if err != nil {
		return nil, err
	}
	seg := c.(*segment[T])
	reporter, err := sc.create(sc.segmentSize.nextTime(seg.Start))
	if errors.Is(err, errEndOfSegment) {
		return nil, bucket.ErrNoMoreBucket
	}
	return reporter, err
}

func (sc *segmentController[T]) OnMove(prev bucket.Reporter, next bucket.Reporter) {
	event := sc.l.Info()
	if prev != nil {
		event.Stringer("prev", prev)
	}
	if next != nil {
		event.Stringer("next", next)
	}
	event.Msg("move to the next segment")
}

func (sc *segmentController[T]) Standard(t time.Time) time.Time {
	switch sc.segmentSize.Unit {
	case HOUR:
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, t.Location())
	case DAY:
		return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
	}
	panic("invalid interval unit")
}

func (sc *segmentController[T]) Format(tm time.Time) string {
	switch sc.segmentSize.Unit {
	case HOUR:
		return tm.Format(hourFormat)
	case DAY:
		return tm.Format(dayFormat)
	}
	panic("invalid interval unit")
}

func (sc *segmentController[T]) Parse(value string) (time.Time, error) {
	switch sc.segmentSize.Unit {
	case HOUR:
		return time.ParseInLocation(hourFormat, value, time.Local)
	case DAY:
		return time.ParseInLocation(dayFormat, value, time.Local)
	}
	panic("invalid interval unit")
}

func (sc *segmentController[T]) open() error {
	sc.Lock()
	defer sc.Unlock()
	return loadSegments(sc.location, segPathPrefix, sc, sc.segmentSize, func(start, end time.Time) error {
		_, err := sc.load(start, end, sc.location)
		if errors.Is(err, errEndOfSegment) {
			return nil
		}
		return err
	})
}

func (sc *segmentController[T]) create(start time.Time) (*segment[T], error) {
	sc.Lock()
	defer sc.Unlock()
	start = sc.Standard(start)
	var next *segment[T]
	for _, s := range sc.lst {
		if s.Contains(uint64(start.UnixNano())) {
			return s, nil
		}
		if next == nil && s.Start.After(start) {
			next = s
		}
	}
	stdEnd := sc.segmentSize.nextTime(start)
	var end time.Time
	if next != nil && next.Start.Before(stdEnd) {
		end = next.Start
	} else {
		end = stdEnd
	}
	lfs.MkdirPanicIfExist(path.Join(sc.location, fmt.Sprintf(segTemplate, sc.Format(start))), dirPerm)
	return sc.load(start, end, sc.location)
}

func (sc *segmentController[T]) sortLst() {
	sort.Slice(sc.lst, func(i, j int) bool {
		return sc.lst[i].id < sc.lst[j].id
	})
}

func (sc *segmentController[T]) load(start, end time.Time, root string) (seg *segment[T], err error) {
	var tsTable T
	if tsTable, err = sc.tsTableCreator(lfs, sc.location, sc.position, sc.l, timestamp.NewSectionTimeRange(start, end)); err != nil {
		return nil, err
	}
	suffix := sc.Format(start)
	ctx := context.WithValue(context.Background(), logger.ContextKey, sc.l)
	seg, err = openSegment[T](common.SetPosition(ctx, func(p common.Position) common.Position {
		p.Segment = suffix
		return p
	}), start, end, path.Join(root, fmt.Sprintf(segTemplate, suffix)), suffix, sc.segmentSize, sc.scheduler, tsTable)
	if err != nil {
		return nil, err
	}
	sc.lst = append(sc.lst, seg)
	sc.sortLst()
	return seg, nil
}

func (sc *segmentController[T]) remove(deadline time.Time) (err error) {
	sc.l.Info().Time("deadline", deadline).Msg("start to remove before deadline")
	for _, s := range sc.segments() {
		if s.End.Before(deadline) || s.Contains(uint64(deadline.UnixNano())) {
			if e := sc.l.Debug(); e.Enabled() {
				e.Stringer("segment", s).Msg("start to remove data in a segment")
			}
			if s.End.Before(deadline) {
				s.delete()
				sc.Lock()
				sc.removeSeg(s.id)
				sc.Unlock()
			}
		}
		s.DecRef()
	}
	return err
}

func (sc *segmentController[T]) removeSeg(segID segmentID) {
	for i, b := range sc.lst {
		if b.id == segID {
			sc.lst = append(sc.lst[:i], sc.lst[i+1:]...)
			break
		}
	}
}

func (sc *segmentController[T]) close() {
	sc.Lock()
	defer sc.Unlock()
	for _, s := range sc.lst {
		s.DecRef()
	}
	sc.lst = sc.lst[:0]
}

type parser interface {
	Parse(value string) (time.Time, error)
}

func loadSegments(root, prefix string, parser parser, intervalRule IntervalRule, loadFn func(start, end time.Time) error) error {
	var startTimeLst []time.Time
	if err := walkDir(
		root,
		prefix,
		func(suffix string) error {
			startTime, err := parser.Parse(suffix)
			if err != nil {
				return err
			}
			startTimeLst = append(startTimeLst, startTime)
			return nil
		}); err != nil {
		return err
	}
	sort.Slice(startTimeLst, func(i, j int) bool { return i < j })
	for i, start := range startTimeLst {
		var end time.Time
		if i < len(startTimeLst)-1 {
			end = startTimeLst[i+1]
		} else {
			end = intervalRule.nextTime(start)
		}
		if err := loadFn(start, end); err != nil {
			return err
		}
	}
	return nil
}
