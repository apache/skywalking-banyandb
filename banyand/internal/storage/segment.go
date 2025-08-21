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
	banyanfs "github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/index/inverted"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

// ErrExpiredData is returned when the data is expired.
var ErrExpiredData = errors.New("expired data")

// ErrSegmentClosed is returned when trying to access a closed segment.
var ErrSegmentClosed = errors.New("segment closed")

var _ Cache = (*segmentCache)(nil)

type segmentCache struct {
	*groupCache
	segmentID segmentID
}

func (sc *segmentCache) get(key EntryKey) Sizable {
	key.segmentID = sc.segmentID
	return sc.groupCache.get(key)
}

func (sc *segmentCache) put(key EntryKey, value Sizable) {
	key.segmentID = sc.segmentID
	sc.groupCache.put(key, value)
}

type segment[T TSTable, O any] struct {
	metrics  any
	tsdbOpts *TSDBOpts[T, O]
	l        *logger.Logger
	index    *seriesIndex
	sLst     atomic.Pointer[[]*shard[T]]
	*segmentCache
	indexMetrics *inverted.Metrics
	lfs          banyanfs.FileSystem
	position     common.Position
	timestamp.TimeRange
	suffix        string
	location      string
	lastAccessed  atomic.Int64
	mu            sync.Mutex
	refCount      int32
	mustBeDeleted uint32
	id            segmentID
}

func (sc *segmentController[T, O]) openSegment(ctx context.Context, startTime, endTime time.Time, path, suffix string, groupCache *groupCache,
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

	s = &segment[T, O]{
		id:           id,
		location:     path,
		suffix:       suffix,
		TimeRange:    timestamp.NewSectionTimeRange(startTime, endTime),
		position:     p,
		metrics:      sc.metrics,
		indexMetrics: sc.indexMetrics,
		tsdbOpts:     options,
		lfs:          sc.lfs,
		segmentCache: &segmentCache{groupCache: groupCache, segmentID: id},
	}
	s.l = logger.Fetch(ctx, s.String())
	s.lastAccessed.Store(time.Now().UnixNano())
	return s, s.initialize(ctx)
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
		_, err = s.createShardIfNotExist(common.ShardID(shardID))
		return err
	})
}

func (s *segment[T, O]) GetTimeRange() timestamp.TimeRange {
	return s.TimeRange
}

func (s *segment[T, O]) Tables() (tt []T, cc []Cache) {
	sLst := s.sLst.Load()
	if sLst != nil {
		for _, s := range *sLst {
			tt = append(tt, s.table)
			cc = append(cc, s.shardCache)
		}
	}
	return tt, cc
}

func (s *segment[T, O]) incRef(ctx context.Context) error {
	s.lastAccessed.Store(time.Now().UnixNano())
	if atomic.LoadInt32(&s.refCount) <= 0 {
		return s.initialize(ctx)
	}
	atomic.AddInt32(&s.refCount, 1)
	return nil
}

func (s *segment[T, O]) initialize(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if atomic.LoadInt32(&s.refCount) > 0 {
		return nil
	}

	ctx = context.WithValue(ctx, logger.ContextKey, s.l)
	ctx = common.SetPosition(ctx, func(_ common.Position) common.Position {
		return s.position
	})

	sir, err := newSeriesIndex(ctx, s.location, s.tsdbOpts.SeriesIndexFlushTimeoutSeconds, s.tsdbOpts.SeriesIndexCacheMaxBytes, s.indexMetrics)
	if err != nil {
		return errors.Wrap(errOpenDatabase, errors.WithMessage(err, "create series index controller failed").Error())
	}
	s.index = sir

	err = s.loadShards(int(s.tsdbOpts.ShardNum))
	if err != nil {
		s.index.Close()
		s.index = nil
		return errors.Wrap(errOpenDatabase, errors.WithMessage(err, "load shards failed").Error())
	}
	atomic.StoreInt32(&s.refCount, 1)

	s.l.Info().Stringer("seg", s).Msg("segment initialized")
	return nil
}

func (s *segment[T, O]) DecRef() {
	shouldCleanup := false

	if atomic.LoadInt32(&s.refCount) <= 0 && atomic.LoadUint32(&s.mustBeDeleted) != 0 {
		shouldCleanup = true
	} else {
		for {
			current := atomic.LoadInt32(&s.refCount)
			if current <= 0 {
				return
			}

			if atomic.CompareAndSwapInt32(&s.refCount, current, current-1) {
				shouldCleanup = current == 1
				break
			}
		}
	}

	if !shouldCleanup {
		return
	}

	s.performCleanup()
}

func (s *segment[T, O]) performCleanup() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if atomic.LoadInt32(&s.refCount) > 0 && atomic.LoadUint32(&s.mustBeDeleted) == 0 {
		return
	}

	deletePath := ""
	if atomic.LoadUint32(&s.mustBeDeleted) != 0 {
		deletePath = s.location
	}

	if s.index != nil {
		if err := s.index.Close(); err != nil {
			s.l.Panic().Err(err).Msg("failed to close the series index")
		}
		s.index = nil
	}

	sLst := s.sLst.Load()
	if sLst != nil {
		for _, shard := range *sLst {
			shard.close()
		}
		if deletePath == "" {
			s.sLst.Store(&[]*shard[T]{})
		}
	}

	if deletePath != "" {
		s.lfs.MustRMAll(deletePath)
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
	return s.createShardIfNotExist(id)
}

func (s *segment[T, O]) createShardIfNotExist(id common.ShardID) (T, error) {
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
	opts         *TSDBOpts[T, O]
	l            *logger.Logger
	indexMetrics *inverted.Metrics
	*groupCache
	lfs         banyanfs.FileSystem
	position    common.Position
	db          string
	stage       string
	location    string
	lst         []*segment[T, O]
	deadline    atomic.Int64
	idleTimeout time.Duration
	optsMutex   sync.RWMutex
	sync.RWMutex
}

func newSegmentController[T TSTable, O any](ctx context.Context, location string,
	l *logger.Logger, opts TSDBOpts[T, O], indexMetrics *inverted.Metrics, metrics Metrics,
	idleTimeout time.Duration, lfs banyanfs.FileSystem, cache Cache, group string,
) *segmentController[T, O] {
	clock, _ := timestamp.GetClock(ctx)
	p := common.GetPosition(ctx)
	return &segmentController[T, O]{
		location:     location,
		opts:         &opts,
		l:            l,
		clock:        clock,
		position:     common.GetPosition(ctx),
		metrics:      metrics,
		indexMetrics: indexMetrics,
		stage:        p.Stage,
		db:           p.Database,
		idleTimeout:  idleTimeout,
		lfs:          lfs,
		groupCache:   &groupCache{cache, group},
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

func (sc *segmentController[T, O]) selectSegments(timeRange timestamp.TimeRange) (tt []Segment[T, O], err error) {
	sc.RLock()
	defer sc.RUnlock()
	last := len(sc.lst) - 1
	ctx := context.WithValue(context.Background(), logger.ContextKey, sc.l)
	for i := range sc.lst {
		s := sc.lst[last-i]
		if s.GetTimeRange().End.Before(timeRange.Start) {
			break
		}
		if s.Overlapping(timeRange) {
			if err = s.incRef(ctx); err != nil {
				return nil, err
			}
			tt = append(tt, s)
		}
	}
	return tt, nil
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
	return s, s.incRef(context.WithValue(context.Background(), logger.ContextKey, sc.l))
}

func (sc *segmentController[T, O]) segments(reopenClosed bool) (ss []*segment[T, O], err error) {
	sc.RLock()
	defer sc.RUnlock()
	r := make([]*segment[T, O], len(sc.lst))
	ctx := context.WithValue(context.Background(), logger.ContextKey, sc.l)
	for i := range sc.lst {
		if reopenClosed {
			if err = sc.lst[i].incRef(ctx); err != nil {
				return nil, err
			}
		} else {
			if atomic.LoadInt32(&sc.lst[i].refCount) > 0 {
				atomic.AddInt32(&sc.lst[i].refCount, 1)
			}
		}
		r[i] = sc.lst[i]
	}
	return r, nil
}

func (sc *segmentController[T, O]) closeIdleSegments() int {
	maxIdleTime := sc.idleTimeout

	now := time.Now().UnixNano()
	idleThreshold := now - maxIdleTime.Nanoseconds()

	segs, _ := sc.segments(false)
	closedCount := 0

	for _, seg := range segs {
		lastAccess := seg.lastAccessed.Load()
		// Only consider segments that have been idle for longer than the threshold
		// and have active references (are not already closed)
		if lastAccess < idleThreshold && atomic.LoadInt32(&seg.refCount) > 0 {
			seg.DecRef()
		}
		seg.DecRef()
		if atomic.LoadInt32(&seg.refCount) == 0 {
			closedCount++
		}
	}

	return closedCount
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

// parseSegmentTime parses a segment suffix into a time based on the interval unit.
func parseSegmentTime(value string, unit IntervalUnit) (time.Time, error) {
	switch unit {
	case HOUR:
		return time.ParseInLocation(hourFormat, value, time.Local)
	case DAY:
		return time.ParseInLocation(dayFormat, value, time.Local)
	}
	panic("invalid interval unit")
}

func (sc *segmentController[T, O]) parse(value string) (time.Time, error) {
	return parseSegmentTime(value, sc.getOptions().SegmentInterval.Unit)
}

func (sc *segmentController[T, O]) open() error {
	sc.Lock()
	defer sc.Unlock()
	emptySegments := make([]string, 0)
	err := loadSegments(sc.location, segPathPrefix, sc, sc.getOptions().SegmentInterval, func(start, end time.Time) error {
		suffix := sc.format(start)
		segmentPath := path.Join(sc.location, fmt.Sprintf(segTemplate, suffix))
		metadataPath := path.Join(segmentPath, metadataFilename)
		version, err := sc.lfs.Read(metadataPath)
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
			sc.lfs.MustRMAll(emptySegments[i])
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
	start = options.SegmentInterval.Unit.Standard(start)
	var next *segment[T, O]
	for _, s := range sc.lst {
		if s.Contains(start.UnixNano()) {
			return s, nil
		}
		if next == nil && s.Start.After(start) {
			next = s
		}
	}
	stdEnd := options.SegmentInterval.NextTime(start)
	var end time.Time
	if next != nil && next.Start.Before(stdEnd) {
		end = next.Start
	} else {
		end = stdEnd
	}
	segPath := path.Join(sc.location, fmt.Sprintf(segTemplate, sc.format(start)))
	sc.lfs.MkdirPanicIfExist(segPath, DirPerm)
	data := []byte(currentVersion)
	metadataPath := filepath.Join(segPath, metadataFilename)
	lf, err := sc.lfs.CreateLockFile(metadataPath, FilePerm)
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
	seg, err = sc.openSegment(ctx, start, end, segPath, suffix, sc.groupCache)
	if err != nil {
		return nil, err
	}
	sc.lst = append(sc.lst, seg)
	sc.sortLst()
	return seg, nil
}

func (sc *segmentController[T, O]) remove(deadline time.Time) (hasSegment bool, err error) {
	ss, _ := sc.segments(false)
	for _, s := range ss {
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

func (sc *segmentController[T, O]) getExpiredSegmentsTimeRange() *timestamp.TimeRange {
	deadline := time.Now().Local().Add(-sc.opts.TTL.estimatedDuration())
	timeRange := &timestamp.TimeRange{
		IncludeStart: true,
		IncludeEnd:   false,
	}
	ss, _ := sc.segments(false)
	for _, s := range ss {
		if s.Before(deadline) {
			if timeRange.Start.IsZero() {
				timeRange.Start = s.Start
			}
			timeRange.End = s.End
		}
		s.DecRef()
	}
	return timeRange
}

func (sc *segmentController[T, O]) deleteExpiredSegments(timeRange timestamp.TimeRange) int64 {
	deadline := time.Now().Local().Add(-sc.opts.TTL.estimatedDuration())
	var count int64
	ss, _ := sc.segments(false)
	for _, s := range ss {
		if s.Before(deadline) && s.Overlapping(timeRange) {
			s.delete()
			sc.Lock()
			sc.removeSeg(s.id)
			sc.Unlock()
			count++
		}
		s.DecRef()
	}
	return count
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
			end = intervalRule.NextTime(start)
		}
		if err := loadFn(start, end); err != nil {
			return err
		}
	}
	return nil
}
