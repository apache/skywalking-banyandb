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
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/internal/bucket"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

var ErrExpiredData = errors.New("expired data")

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
	segmentSize IntervalRule, scheduler *timestamp.Scheduler, tsTable T, p common.Position,
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
	s.Reporter = bucket.NewTimeBasedReporter(fmt.Sprintf("%s-%s", p.Shard, s.String()), timeRange, clock, scheduler)
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

type segmentController[T TSTable, O any] struct {
	clock          timestamp.Clock
	option         O
	scheduler      *timestamp.Scheduler
	l              *logger.Logger
	tsTableCreator TSTableCreator[T, O]
	position       common.Position
	location       string
	lst            []*segment[T]
	segmentSize    IntervalRule
	deadline       atomic.Int64
	sync.RWMutex
}

func newSegmentController[T TSTable, O any](ctx context.Context, location string,
	segmentSize IntervalRule, l *logger.Logger, scheduler *timestamp.Scheduler,
	tsTableCreator TSTableCreator[T, O], option O,
) *segmentController[T, O] {
	clock, _ := timestamp.GetClock(ctx)
	return &segmentController[T, O]{
		location:       location,
		segmentSize:    segmentSize,
		l:              l,
		clock:          clock,
		scheduler:      scheduler,
		position:       common.GetPosition(ctx),
		tsTableCreator: tsTableCreator,
		option:         option,
	}
}

func (sc *segmentController[T, O]) selectTSTables(timeRange timestamp.TimeRange) (tt []TSTableWrapper[T]) {
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

func (sc *segmentController[T, O]) createTSTable(ts time.Time) (TSTableWrapper[T], error) {
	// Before the first remove old segment run, any segment should be created.
	if sc.deadline.Load() > ts.UnixNano() {
		return nil, ErrExpiredData
	}
	s, err := sc.create(ts)
	if err != nil {
		return nil, err
	}
	s.incRef()
	return s, nil
}

func (sc *segmentController[T, O]) segments() (ss []*segment[T]) {
	sc.RLock()
	defer sc.RUnlock()
	r := make([]*segment[T], len(sc.lst))
	for i := range sc.lst {
		sc.lst[i].incRef()
		r[i] = sc.lst[i]
	}
	return r
}

func (sc *segmentController[T, O]) Format(tm time.Time) string {
	switch sc.segmentSize.Unit {
	case HOUR:
		return tm.Format(hourFormat)
	case DAY:
		return tm.Format(dayFormat)
	}
	panic("invalid interval unit")
}

func (sc *segmentController[T, O]) Parse(value string) (time.Time, error) {
	switch sc.segmentSize.Unit {
	case HOUR:
		return time.ParseInLocation(hourFormat, value, time.Local)
	case DAY:
		return time.ParseInLocation(dayFormat, value, time.Local)
	}
	panic("invalid interval unit")
}

func (sc *segmentController[T, O]) open() error {
	sc.Lock()
	defer sc.Unlock()
	return loadSegments(sc.location, segPathPrefix, sc, sc.segmentSize, func(start, end time.Time) error {
		compatibleVersions, err := readCompatibleVersions()
		if err != nil {
			return err
		}
		suffix := sc.Format(start)
		metadataPath := path.Join(sc.location, fmt.Sprintf(segTemplate, suffix), metadataFilename)
		version, err := lfs.Read(metadataPath)
		if err != nil {
			return err
		}
		for _, cv := range compatibleVersions[compatibleVersionsKey] {
			if string(version) == cv {
				_, err := sc.load(start, end, sc.location)
				return err
			}
		}
		return errVersionIncompatible
	})
}

func (sc *segmentController[T, O]) create(start time.Time) (*segment[T], error) {
	sc.Lock()
	defer sc.Unlock()
	last := len(sc.lst) - 1
	for i := range sc.lst {
		s := sc.lst[last-i]
		if s.Contains(start.UnixNano()) {
			s.incRef()
			return s, nil
		}
	}
	start = sc.segmentSize.Unit.standard(start)
	var next *segment[T]
	for _, s := range sc.lst {
		if s.Contains(start.UnixNano()) {
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
	segPath := path.Join(sc.location, fmt.Sprintf(segTemplate, sc.Format(start)))
	lfs.MkdirPanicIfExist(segPath, dirPerm)
	data := []byte(currentVersion)
	metadataPath := filepath.Join(segPath, metadataFilename)
	lf, err := lfs.CreateLockFile(metadataPath, filePermission)
	if err != nil {
		logger.Panicf("cannot create lock file %s: %s", metadataPath, err)
	}
	n, err := lf.Write(data)
	if err != nil {
		logger.Panicf("cannot write metadata %s: %s", metadataPath, err)
	}
	if n != len(data) {
		logger.Panicf("unexpected number of bytes written to %s; got %d; want %d", metadataPath, n, len(data))
	}
	return sc.load(start, end, sc.location)
}

func (sc *segmentController[T, O]) sortLst() {
	sort.Slice(sc.lst, func(i, j int) bool {
		return sc.lst[i].id < sc.lst[j].id
	})
}

func (sc *segmentController[T, O]) load(start, end time.Time, root string) (seg *segment[T], err error) {
	suffix := sc.Format(start)
	segPath := path.Join(root, fmt.Sprintf(segTemplate, suffix))
	var tsTable T
	p := sc.position
	p.Segment = suffix
	if tsTable, err = sc.tsTableCreator(lfs, segPath, p, sc.l, timestamp.NewSectionTimeRange(start, end), sc.option); err != nil {
		return nil, err
	}
	seg, err = openSegment[T](context.WithValue(context.Background(), logger.ContextKey, sc.l), start, end, segPath, suffix, sc.segmentSize, sc.scheduler, tsTable, p)
	if err != nil {
		return nil, err
	}
	sc.lst = append(sc.lst, seg)
	sc.sortLst()
	return seg, nil
}

func (sc *segmentController[T, O]) remove(deadline time.Time) (err error) {
	for _, s := range sc.segments() {
		if s.Before(deadline) {
			s.delete()
			sc.Lock()
			sc.removeSeg(s.id)
			sc.Unlock()
			sc.l.Info().Stringer("segment", s).Msg("removed a segment")
		}
		s.DecRef()
	}
	return err
}

func (sc *segmentController[T, O]) removeSeg(segID segmentID) {
	for i, b := range sc.lst {
		if b.id == segID {
			sc.lst = append(sc.lst[:i], sc.lst[i+1:]...)
			sc.deadline.Store(sc.lst[0].Start.UnixNano())
			break
		}
	}
}

func (sc *segmentController[T, O]) close() {
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
