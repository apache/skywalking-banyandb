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
	"encoding/json"
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
	banyanfs "github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/index/inverted"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

var (
	// ErrSegmentClosed is returned when trying to access a closed segment.
	ErrSegmentClosed = errors.New("segment closed")
	// ErrInvalidSegmentTimestamp is returned when a segment timestamp is zero or near-epoch,
	// indicating a corrupted timestamp (e.g., unset MinTimestamp flowing through sync paths).
	ErrInvalidSegmentTimestamp = errors.New("invalid segment timestamp: epoch or near-epoch time is not valid for APM data")
)

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
	mu            sync.RWMutex
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

// TablesWithShardIDs returns tables with their corresponding shard IDs.
func (s *segment[T, O]) TablesWithShardIDs() (tt []T, shardIDs []common.ShardID, cc []Cache) {
	sLst := s.sLst.Load()
	if sLst != nil {
		for _, sh := range *sLst {
			tt = append(tt, sh.table)
			shardIDs = append(shardIDs, sh.id)
			cc = append(cc, sh.shardCache)
		}
	}
	return tt, shardIDs, cc
}

// incRef acquires one reference on the segment, opening it via initialize
// if it is currently closed (refCount<=0). On return, the caller is
// guaranteed that the segment is alive (s.index != nil, shards loaded) and
// will stay alive until the matching DecRef.
//
// incRef intentionally does NOT touch lastAccessed: the idle-segment
// reclaimer (closeIdleSegments) decides eligibility from that field, and
// the rotation-tick scan iterates every segment via incRef on each tick.
// Refreshing lastAccessed here would defeat the reclaimer for any segment
// outside the active write window. Callers that represent a real read or
// write touch must bump lastAccessed explicitly (see selectSegments and
// createSegment); housekeeping iterators must not.
//
// The CAS loop closes a TOCTOU race that the older "Load + AddInt32"
// shape suffered from: a goroutine could read refCount=1, race against a
// concurrent DecRef that drives it to 0 and triggers performCleanup
// (which sets s.index=nil and tears down shards), and then AddInt32 it
// back to 1. The caller would walk away with refCount=1 -- formally a
// valid reference -- but pointing at an already-cleaned-up segment, so
// every subsequent IndexDB() / Tables() call would observe zero state.
//
// The CAS variant retries whenever a concurrent DecRef beats us to the
// counter: if CAS fails, we re-Load and either succeed at the new value
// or fall through to initialize() to reopen the segment under s.mu.
// Symmetric to the CAS loop in DecRef, which guarantees the dual: a
// DecRef that drives refCount to 0 sees no concurrent +1 sneak in
// before performCleanup runs.
func (s *segment[T, O]) incRef(ctx context.Context) error {
	for {
		current := atomic.LoadInt32(&s.refCount)
		if current <= 0 {
			// Either the segment was never opened or DecRef just drove
			// refCount to 0; reopen under the mutex.
			return s.initialize(ctx)
		}
		// CAS so a concurrent DecRef cannot flip refCount to 0 and run
		// performCleanup between our Load and our increment. On failure
		// we re-Load and re-evaluate the branch.
		if atomic.CompareAndSwapInt32(&s.refCount, current, current+1) {
			return nil
		}
	}
}

func (s *segment[T, O]) initialize(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if atomic.LoadInt32(&s.refCount) > 0 {
		// Another goroutine reopened the segment while we waited for the
		// mutex. Add the +1 our caller (incRef) skipped before entering
		// the slow path; otherwise concurrent reopens would share one
		// refCount and the first DecRef would cleanup while we are still
		// using it.
		atomic.AddInt32(&s.refCount, 1)
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

func (s *segment[T, O]) collectMetrics() {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.index == nil {
		return
	}
	s.index.store.CollectMetrics(s.index.p.SegLabelValues()...)
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
	now := time.Now().UnixNano()
	for i := range sc.lst {
		s := sc.lst[last-i]
		if s.GetTimeRange().End.Before(timeRange.Start) {
			break
		}
		if s.Overlapping(timeRange) {
			if err = s.incRef(ctx); err != nil {
				return nil, err
			}
			s.lastAccessed.Store(now)
			tt = append(tt, s)
		}
	}
	return tt, nil
}

func (sc *segmentController[T, O]) createSegment(ts time.Time) (*segment[T, O], error) {
	s, err := sc.create(context.Background(), ts)
	if err != nil {
		return nil, err
	}
	if err = s.incRef(context.WithValue(context.Background(), logger.ContextKey, sc.l)); err != nil {
		return nil, err
	}
	s.lastAccessed.Store(time.Now().UnixNano())
	return s, nil
}

func (sc *segmentController[T, O]) segments(ctx context.Context, reopenClosed bool) (ss []*segment[T, O], err error) {
	sc.RLock()
	defer sc.RUnlock()
	r := make([]*segment[T, O], len(sc.lst))
	ctx = context.WithValue(ctx, logger.ContextKey, sc.l)
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

	// Take our own snapshot rather than going through segments(false): that
	// helper's "Load>0 then Add" bump is non-atomic and can resurrect a
	// segment that DecRef just cleaned up, and its caller has no way to tell
	// which entries it actually bumped. closeIdleSegments must know, because
	// it issues an unconditional DecRef per entry to release the bump; if a
	// concurrent selectSegments reopens an entry we didn't bump, that DecRef
	// would drop the query's only live ref and trigger performCleanup under
	// active use. Iterate sc.lst under RLock and CAS-bump each open segment;
	// closed segments are recorded as "not bumped" and skipped in the loop.
	sc.RLock()
	segs := make([]*segment[T, O], 0, len(sc.lst))
	bumped := make([]bool, 0, len(sc.lst))
	refAtBump := make([]int32, 0, len(sc.lst))
	for _, s := range sc.lst {
		didBump := false
		var snapRef int32
		for {
			current := atomic.LoadInt32(&s.refCount)
			if current <= 0 {
				break
			}
			if atomic.CompareAndSwapInt32(&s.refCount, current, current+1) {
				didBump = true
				snapRef = current
				break
			}
		}
		segs = append(segs, s)
		bumped = append(bumped, didBump)
		refAtBump = append(refAtBump, snapRef)
	}
	sc.RUnlock()

	closedCount := 0
	for i, seg := range segs {
		if !bumped[i] {
			continue
		}
		// Only close when the snapshot proved refCount==1 (baseline only):
		// a higher value means other callers hold active references, and
		// decrementing past our bump would steal one of their refs,
		// potentially triggering performCleanup under active use on a
		// subsequent tick.
		if refAtBump[i] == 1 && seg.lastAccessed.Load() < idleThreshold {
			seg.DecRef()
			closedCount++
		}
		seg.DecRef()
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
	invalidSegments := make([]string, 0)
	err := loadSegments(sc.location, segPathPrefix, sc, sc.getOptions().SegmentInterval, func(start, end time.Time) error {
		suffix := sc.format(start)
		segmentPath := path.Join(sc.location, fmt.Sprintf(segTemplate, suffix))
		// Detect epoch/near-epoch segments created by zero MinTimestamp in sync paths.
		// BanyanDB is an APM database -- no legitimate data predates the year 2000.
		if start.UnixNano() <= 0 {
			invalidSegments = append(invalidSegments, segmentPath)
			return nil
		}
		metadataPath := path.Join(segmentPath, metadataFilename)
		rawMeta, readErr := sc.lfs.Read(metadataPath)
		if readErr != nil {
			if errors.Is(readErr, fs.ErrNotExist) {
				invalidSegments = append(invalidSegments, segmentPath)
				return nil
			}
			return readErr
		}
		if len(rawMeta) == 0 {
			invalidSegments = append(invalidSegments, segmentPath)
			return nil
		}
		meta, parseErr := readSegmentMeta(rawMeta)
		if parseErr != nil {
			return parseErr
		}
		segmentEnd := end
		if meta.EndTime != "" {
			parsedEnd, timeErr := time.Parse(time.RFC3339Nano, meta.EndTime)
			if timeErr != nil {
				return timeErr
			}
			segmentEnd = parsedEnd
		}
		_, loadErr := sc.load(context.Background(), start, segmentEnd, sc.location)
		return loadErr
	})
	if len(invalidSegments) > 0 {
		sc.l.Warn().Strs("segments", invalidSegments).Msg("invalid segments found (empty or epoch-dated), removing them")
		for i := range invalidSegments {
			sc.lfs.MustRMAll(invalidSegments[i])
		}
	}
	return err
}

func (sc *segmentController[T, O]) create(ctx context.Context, start time.Time) (*segment[T, O], error) {
	// Reject epoch/near-epoch timestamps caused by zero MinTimestamp flowing through sync paths.
	// BanyanDB is an APM database -- no legitimate data predates the year 2000.
	if start.UnixNano() <= 0 {
		return nil, ErrInvalidSegmentTimestamp
	}
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
	// Anchor stdEnd to the aligned start before any bump so end stays on the
	// global grid even when start is bumped past a legacy off-grid neighbor;
	// subsequent segments then self-heal back to the grid.
	alignedStart := options.SegmentInterval.Standard(start)
	stdEnd := options.SegmentInterval.NextTime(alignedStart)
	start = alignedStart
	// sc.lst is sorted ascending by start time with non-overlapping ranges;
	// a single pass bumps start past every legacy segment that swallows it
	// (each next segment.Start >= previous.End).
	var next *segment[T, O]
	for _, s := range sc.lst {
		if s.Contains(start.UnixNano()) {
			start = s.End
			continue
		}
		if next == nil && s.Start.After(start) {
			next = s
		}
	}
	var end time.Time
	if next != nil && next.Start.Before(stdEnd) {
		// `next` starts inside the current grid bucket - a legacy off-grid
		// segment whose TTL hasn't elapsed. Cap end at next.Start to avoid
		// overlap; surfacing this at Info level lets operators see the
		// abnormal span until the legacy neighbor ages out.
		sc.l.Info().
			Stringer("alignedStart", alignedStart).
			Stringer("bumpedStart", start).
			Stringer("nextStart", next.Start).
			Stringer("stdEnd", stdEnd).
			Msg("new segment span is shorter than configured SegmentInterval due to an unaligned legacy neighbor")
		end = next.Start
	} else {
		end = stdEnd
	}
	segPath := path.Join(sc.location, fmt.Sprintf(segTemplate, sc.format(start)))
	sc.lfs.MkdirPanicIfExist(segPath, DirPerm)
	meta := segmentMeta{
		Version: currentVersion,
		EndTime: end.Format(time.RFC3339Nano),
	}
	data, marshalErr := json.Marshal(meta)
	if marshalErr != nil {
		logger.Panicf("cannot marshal segment metadata: %s", marshalErr)
	}
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
	return sc.load(ctx, start, end, sc.location)
}

func (sc *segmentController[T, O]) sortLst() {
	sort.Slice(sc.lst, func(i, j int) bool {
		return sc.lst[i].id < sc.lst[j].id
	})
}

func (sc *segmentController[T, O]) load(ctx context.Context, start, end time.Time, root string) (seg *segment[T, O], err error) {
	suffix := sc.format(start)
	segPath := path.Join(root, fmt.Sprintf(segTemplate, suffix))
	ctx = common.SetPosition(context.WithValue(ctx, logger.ContextKey, sc.l), func(_ common.Position) common.Position {
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
	ss, _ := sc.segments(context.Background(), false)
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
	ss, _ := sc.segments(context.Background(), false)
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

func (sc *segmentController[T, O]) deleteExpiredSegments(segmentSuffixes []string) int64 {
	deadline := time.Now().Local().Add(-sc.opts.TTL.estimatedDuration())
	var count int64
	ss, _ := sc.segments(context.Background(), false)
	sc.l.Info().Str("segment_suffixes", fmt.Sprintf("%s", segmentSuffixes)).
		Str("ttl", fmt.Sprintf("%d(%s)", sc.opts.TTL.Num, sc.opts.TTL.Unit)).
		Str("deadline", deadline.String()).
		Int("total_segment_count", len(ss)).Msg("deleting expired segments")
	shouldDeleteSuffixes := make(map[string]bool)
	for _, s := range segmentSuffixes {
		shouldDeleteSuffixes[s] = true
	}
	for _, s := range ss {
		if shouldDeleteSuffixes[s.suffix] {
			sc.l.Info().Str("suffix", s.suffix).
				Str("deadline", deadline.String()).
				Str("segment_name", s.String()).
				Str("segment_time_range", s.GetTimeRange().String()).
				Msg("deleting an expired segment")
			s.delete()
			sc.Lock()
			sc.removeSeg(s.id)
			sc.Unlock()
			count++
		} else {
			sc.l.Info().Str("suffix", s.suffix).
				Str("deadline", deadline.String()).
				Str("segment_name", s.String()).
				Str("segment_time_range", s.GetTimeRange().String()).
				Msg("segment is not expired or not in the time range, skipping deletion")
		}
		s.DecRef()
	}
	return count
}

func (sc *segmentController[T, O]) removeSeg(segID segmentID) {
	for i, b := range sc.lst {
		if b.id == segID {
			sc.lst = append(sc.lst[:i], sc.lst[i+1:]...)
			break
		}
	}
}

// peekOldestSegmentEndTime returns the end time of the oldest segment.
// It returns the zero time and false if no segments exist or all segments have refCount <= 0.
func (sc *segmentController[T, O]) peekOldestSegmentEndTime() (time.Time, bool) {
	sc.RLock()
	defer sc.RUnlock()

	if len(sc.lst) <= 1 {
		return time.Time{}, false
	}

	// Segments are sorted by ID (which correlates with start time),
	// so the first one is the oldest
	oldest := sc.lst[0]

	// Only return segments that are still active (have references > 0)
	if atomic.LoadInt32(&oldest.refCount) > 0 {
		return oldest.End, true
	}

	return time.Time{}, false
}

// removeOldest removes exactly one oldest segment if it exists and meets the keep-one rule.
// Returns true if a segment was deleted, false if no segments to delete or keep-one rule prevents deletion.
func (sc *segmentController[T, O]) removeOldest() (bool, error) {
	sc.Lock()
	defer sc.Unlock()

	if len(sc.lst) <= 1 {
		// Keep-one rule: never delete the last remaining segment
		return false, nil
	}

	// Find the oldest segment (first in sorted list)
	oldest := sc.lst[0]

	// Delete the segment and remove from list
	oldest.delete()
	sc.removeSeg(oldest.id)

	sc.l.Info().Stringer("segment", oldest).Str("remaining_segments", fmt.Sprintf("%v", sc.lst)).Msg("removed oldest segment via forced cleanup")
	return true, nil
}

func (sc *segmentController[T, O]) close() {
	sc.Lock()
	defer sc.Unlock()
	for _, s := range sc.lst {
		for atomic.LoadInt32(&s.refCount) > 0 {
			s.DecRef()
		}
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
	sort.Slice(startTimeLst, func(i, j int) bool { return startTimeLst[i].Before(startTimeLst[j]) })
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
