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
	"fmt"
	"io/fs"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index/inverted"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

// ErrExpiredData is returned when the data is expired.
var ErrExpiredData = errors.New("expired data")

type segment[T TSTable, O any] struct {
	metrics  any
	option   O
	creator  TSTableCreator[T, O]
	l        *logger.Logger
	index    *seriesIndex
	sLst     atomic.Pointer[[]*shard[T]]
	position common.Position
	timestamp.TimeRange
	suffix        string
	location      string
	mu            sync.Mutex
	refCount      int32
	mustBeDeleted uint32
	id            segmentID
}

func (sc *segmentController[T, O]) openSegment(ctx context.Context, startTime, endTime time.Time, path, suffix string,
) (s *segment[T, O], err error) {
	suffixInteger, err := strconv.Atoi(suffix)
	if err != nil {
		return nil, err
	}
	p := common.GetPosition(ctx)
	p.Segment = suffix
	ctx = common.SetPosition(ctx, func(_ common.Position) common.Position {
		return p
	})
	options := sc.getOptions()
	id := generateSegID(options.SegmentInterval.Unit, suffixInteger)
	sir, err := newSeriesIndex(ctx, path, options.SeriesIndexFlushTimeoutSeconds, sc.indexMetrics)
	if err != nil {
		return nil, errors.Wrap(errOpenDatabase, errors.WithMessage(err, "create series index controller failed").Error())
	}
	s = &segment[T, O]{
		id:        id,
		location:  path,
		suffix:    suffix,
		TimeRange: timestamp.NewSectionTimeRange(startTime, endTime),
		position:  p,
		refCount:  1,
		index:     sir,
		metrics:   sc.metrics,
		creator:   options.TSTableCreator,
		option:    options.Option,
	}
	s.l = logger.Fetch(ctx, s.String())
	return s, s.loadShards(int(options.ShardNum))
}

func (s *segment[T, O]) loadShards(shardNum int) error {
	return walkDir(s.location, shardPathPrefix, func(suffix string) error {
		shardID, err := strconv.Atoi(suffix)
		if err != nil {
			return err
		}
		if shardID >= shardNum {
			return nil
		}
		s.l.Info().Int("shard_id", shardID).Msg("loaded a existed shard")
		_, err = s.CreateTSTableIfNotExist(common.ShardID(shardID))
		return err
	})
}

func (s *segment[T, O]) GetTimeRange() timestamp.TimeRange {
	return s.TimeRange
}

func (s *segment[T, O]) Tables() (tt []T) {
	sLst := s.sLst.Load()
	if sLst != nil {
		for _, s := range *sLst {
			tt = append(tt, s.table)
		}
	}
	return tt
}

func (s *segment[T, O]) incRef() {
	atomic.AddInt32(&s.refCount, 1)
}

func (s *segment[T, O]) DecRef() {
	n := atomic.AddInt32(&s.refCount, -1)
	if n > 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	deletePath := ""
	if atomic.LoadUint32(&s.mustBeDeleted) != 0 {
		deletePath = s.location
	}

	if err := s.index.Close(); err != nil {
		s.l.Panic().Err(err).Msg("failed to close the series index")
	}

	sLst := s.sLst.Load()
	if sLst != nil {
		for _, s := range *sLst {
			s.close()
		}
	}

	if deletePath != "" {
		lfs.MustRMAll(deletePath)
	}
}

func (s *segment[T, O]) delete() {
	atomic.StoreUint32(&s.mustBeDeleted, 1)
	s.DecRef()
}

func (s *segment[T, O]) CreateTSTableIfNotExist(id common.ShardID) (T, error) {
	if s, ok := s.getShard(id); ok {
		return s.table, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s, ok := s.getShard(id); ok {
		return s.table, nil
	}
	ctx := context.WithValue(context.Background(), logger.ContextKey, s.l)
	ctx = common.SetPosition(ctx, func(_ common.Position) common.Position {
		return s.position
	})
	so, err := s.openShard(ctx, id)
	if err != nil {
		var t T
		return t, err
	}
	var shardList []*shard[T]
	sLst := s.sLst.Load()
	if sLst != nil {
		shardList = *sLst
	}
	shardList = append(shardList, so)
	s.sLst.Store(&shardList)
	return so.table, nil
}

func (s *segment[T, O]) getShard(shardID common.ShardID) (*shard[T], bool) {
	sLst := s.sLst.Load()
	if sLst != nil {
		for _, s := range *sLst {
			if s.id == shardID {
				return s, true
			}
		}
	}
	return nil, false
}

func (s *segment[T, O]) String() string {
	return "SegID-" + s.suffix
}

type segmentController[T TSTable, O any] struct {
	clock        timestamp.Clock
	metrics      Metrics
	l            *logger.Logger
	indexMetrics *inverted.Metrics
	opts         *TSDBOpts[T, O]
	position     common.Position
	location     string
	lst          []*segment[T, O]
	deadline     atomic.Int64
	optsMutex    sync.RWMutex
	sync.RWMutex
}

func newSegmentController[T TSTable, O any](ctx context.Context, location string,
	l *logger.Logger, opts TSDBOpts[T, O], indexMetrics *inverted.Metrics, metrics Metrics,
) *segmentController[T, O] {
	clock, _ := timestamp.GetClock(ctx)
	return &segmentController[T, O]{
		location:     location,
		opts:         &opts,
		l:            l,
		clock:        clock,
		position:     common.GetPosition(ctx),
		metrics:      metrics,
		indexMetrics: indexMetrics,
	}
}

func (sc *segmentController[T, O]) getOptions() *TSDBOpts[T, O] {
	sc.optsMutex.RLock()
	defer sc.optsMutex.RUnlock()
	return sc.opts
}

func (sc *segmentController[T, O]) updateOptions(resourceOpts *commonv1.ResourceOpts) {
	sc.optsMutex.Lock()
	defer sc.optsMutex.Unlock()
	si := MustToIntervalRule(resourceOpts.SegmentInterval)
	if sc.opts.SegmentInterval.Unit != si.Unit {
		sc.l.Panic().Msg("segment interval unit cannot be changed")
		return
	}
	sc.opts.SegmentInterval = si
	sc.opts.TTL = MustToIntervalRule(resourceOpts.Ttl)
	sc.opts.ShardNum = resourceOpts.ShardNum
}

func (sc *segmentController[T, O]) selectSegments(timeRange timestamp.TimeRange) (tt []Segment[T, O]) {
	sc.RLock()
	defer sc.RUnlock()
	last := len(sc.lst) - 1
	for i := range sc.lst {
		s := sc.lst[last-i]
		if s.GetTimeRange().End.Before(timeRange.Start) {
			break
		}
		if s.Overlapping(timeRange) {
			s.incRef()
			tt = append(tt, s)
		}
	}
	return tt
}

func (sc *segmentController[T, O]) createSegment(ts time.Time) (*segment[T, O], error) {
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

func (sc *segmentController[T, O]) segments() (ss []*segment[T, O]) {
	sc.RLock()
	defer sc.RUnlock()
	r := make([]*segment[T, O], len(sc.lst))
	for i := range sc.lst {
		sc.lst[i].incRef()
		r[i] = sc.lst[i]
	}
	return r
}

func (sc *segmentController[T, O]) format(tm time.Time) string {
	switch sc.getOptions().SegmentInterval.Unit {
	case HOUR:
		return tm.Format(hourFormat)
	case DAY:
		return tm.Format(dayFormat)
	}
	panic("invalid interval unit")
}

func (sc *segmentController[T, O]) parse(value string) (time.Time, error) {
	switch sc.getOptions().SegmentInterval.Unit {
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
	emptySegments := make([]string, 0)
	err := loadSegments(sc.location, segPathPrefix, sc, sc.getOptions().SegmentInterval, func(start, end time.Time) error {
		suffix := sc.format(start)
		segmentPath := path.Join(sc.location, fmt.Sprintf(segTemplate, suffix))
		metadataPath := path.Join(segmentPath, metadataFilename)
		version, err := lfs.Read(metadataPath)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				emptySegments = append(emptySegments, segmentPath)
				return nil
			}
			return err
		}
		if len(version) == 0 {
			emptySegments = append(emptySegments, segmentPath)
			return nil
		}
		if err = checkVersion(convert.BytesToString(version)); err != nil {
			return err
		}
		_, err = sc.load(start, end, sc.location)
		return err
	})
	if len(emptySegments) > 0 {
		sc.l.Warn().Strs("segments", emptySegments).Msg("empty segments found, removing them.")
		for i := range emptySegments {
			lfs.MustRMAll(emptySegments[i])
		}
	}
	return err
}

func (sc *segmentController[T, O]) create(start time.Time) (*segment[T, O], error) {
	sc.Lock()
	defer sc.Unlock()
	last := len(sc.lst) - 1
	for i := range sc.lst {
		s := sc.lst[last-i]
		if s.Contains(start.UnixNano()) {
			return s, nil
		}
	}
	options := sc.getOptions()
	start = options.SegmentInterval.Unit.standard(start)
	var next *segment[T, O]
	for _, s := range sc.lst {
		if s.Contains(start.UnixNano()) {
			return s, nil
		}
		if next == nil && s.Start.After(start) {
			next = s
		}
	}
	stdEnd := options.SegmentInterval.nextTime(start)
	var end time.Time
	if next != nil && next.Start.Before(stdEnd) {
		end = next.Start
	} else {
		end = stdEnd
	}
	segPath := path.Join(sc.location, fmt.Sprintf(segTemplate, sc.format(start)))
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

func (sc *segmentController[T, O]) load(start, end time.Time, root string) (seg *segment[T, O], err error) {
	suffix := sc.format(start)
	segPath := path.Join(root, fmt.Sprintf(segTemplate, suffix))
	ctx := common.SetPosition(context.WithValue(context.Background(), logger.ContextKey, sc.l), func(_ common.Position) common.Position {
		return sc.position
	})
	seg, err = sc.openSegment(ctx, start, end, segPath, suffix)
	if err != nil {
		return nil, err
	}
	sc.lst = append(sc.lst, seg)
	sc.sortLst()
	return seg, nil
}

func (sc *segmentController[T, O]) remove(deadline time.Time) (hasSegment bool, err error) {
	for _, s := range sc.segments() {
		if s.Before(deadline) {
			hasSegment = true
			id := s.id
			s.delete()
			sc.Lock()
			sc.removeSeg(id)
			sc.Unlock()
			sc.l.Info().Stringer("segment", s).Msg("removed a segment")
		}
		s.DecRef()
	}
	return hasSegment, err
}

func (sc *segmentController[T, O]) removeSeg(segID segmentID) {
	for i, b := range sc.lst {
		if b.id == segID {
			sc.lst = append(sc.lst[:i], sc.lst[i+1:]...)
			if len(sc.lst) < 1 {
				sc.deadline.Store(0)
			} else {
				sc.deadline.Store(sc.lst[0].Start.UnixNano())
			}
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
	if sc.metrics != nil {
		sc.metrics.DeleteAll()
	}
}

func loadSegments[T TSTable, O any](root, prefix string, parser *segmentController[T, O], intervalRule IntervalRule, loadFn func(start, end time.Time) error) error {
	var startTimeLst []time.Time
	if err := walkDir(
		root,
		prefix,
		func(suffix string) error {
			startTime, err := parser.parse(suffix)
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
