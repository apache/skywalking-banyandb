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
	// New segments start dormant: resources open (index!=nil) but refCount==0,
	// so an idle, unused segment is reclaimable without any caller releasing a
	// baseline reference.
	s.mu.Lock()
	defer s.mu.Unlock()
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

// incRef acquires one active reference on the segment, opening its resources
// (series index + shards) via acquire if it is currently closed. On return the
// segment is guaranteed alive (s.index != nil) until the matching DecRef.
//
// refCount counts only active users and is orthogonal to whether the segment is
// open (index != nil): a segment with refCount==0 but index!=nil is "dormant"
// -- open and reusable, yet eligible for the idle reclaimer. incRef therefore
// does NOT touch lastAccessed: the reclaimer (closeIdleSegments) decides
// eligibility from that field and the rotation-tick scan iterates every segment
// via incRef on each tick, so refreshing it here would defeat the reclaimer.
// Real read/write touches bump lastAccessed explicitly (see selectSegments and
// createSegment); housekeeping iterators must not.
//
// The fast path CAS-bumps only while refCount>0, so it can never resurrect a
// segment the idle reclaimer is closing (closeIfIdle / performDelete act only
// at refCount==0 under s.mu). When refCount<=0 it falls to the s.mu slow path
// (acquire), which serializes the reopen and the 0->1 transition against them.
func (s *segment[T, O]) incRef(ctx context.Context) error {
	for {
		current := atomic.LoadInt32(&s.refCount)
		if current <= 0 {
			// Dormant (index!=nil) or closed (index==nil); reopen/acquire under
			// the mutex.
			return s.acquire(ctx)
		}
		// CAS bumps only while refCount>0, so it can never resurrect a segment
		// the idle reclaimer is closing (closeIfIdle acts only at refCount==0).
		if atomic.CompareAndSwapInt32(&s.refCount, current, current+1) {
			return nil
		}
	}
}

// initialize opens the segment's series index and shards if not already
// open, leaving the segment dormant (index!=nil) WITHOUT changing refCount.
// Must be called with s.mu held.
func (s *segment[T, O]) initialize(ctx context.Context) error {
	if s.index != nil {
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

	s.l.Info().Stringer("seg", s).Msg("segment initialized")
	return nil
}

// acquire is the slow path of incRef: under s.mu it opens the segment's
// resources via initialize if closed, then adds the caller's reference (0->1).
// Holding s.mu serializes the reopen and the 0->1 transition against
// closeIfIdle / performDelete, which act only at refCount==0 under the same lock.
func (s *segment[T, O]) acquire(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if atomic.LoadInt32(&s.refCount) > 0 {
		// Another goroutine acquired the segment while we waited for the mutex.
		atomic.AddInt32(&s.refCount, 1)
		return nil
	}
	if atomic.LoadUint32(&s.mustBeDeleted) != 0 {
		// Flagged for deletion with no active reference: performDelete has run
		// (or is about to, blocked on s.mu) and the directory may already be
		// gone. Refuse to reopen so we never open resources on a removed dir.
		// A still-referenced (refCount>0) flagged segment is handled by the
		// fast-bump above, since its directory stays until the last DecRef.
		return ErrSegmentClosed
	}
	if err := s.initialize(ctx); err != nil {
		return err
	}
	atomic.StoreInt32(&s.refCount, 1)
	return nil
}

// resetIndex resets the series-index cache if the segment is open, holding
// s.mu.RLock so the index pointer read is synchronized against a concurrent
// close (which clears it under the write lock).
func (s *segment[T, O]) resetIndex() {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.index != nil {
		s.index.store.Reset()
	}
}

// collectOpenMetrics gathers the segment's shard-table and series-index metrics
// if it is open, holding s.mu.RLock so a concurrent reclaim (which takes the
// write lock) cannot tear the resources down mid-collection. Returns whether
// the segment was open.
func (s *segment[T, O]) collectOpenMetrics(shardMetrics Metrics) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.index == nil {
		return false
	}
	if sLst := s.sLst.Load(); sLst != nil {
		for _, sh := range *sLst {
			sh.table.Collect(shardMetrics)
		}
	}
	s.index.store.CollectMetrics(s.index.p.SegLabelValues()...)
	return true
}

// DecRef releases one active reference. Reaching refCount==0 does NOT close the
// segment -- it becomes dormant (open, reclaimable by the idle reclaimer). The
// sole exception: a segment flagged for deletion (mustBeDeleted) while held runs
// its deferred delete on the last DecRef.
func (s *segment[T, O]) DecRef() {
	for {
		current := atomic.LoadInt32(&s.refCount)
		if current <= 0 {
			// Already dormant; nothing to release. Deletion of a dormant
			// segment is driven by delete()/performDelete, not here.
			return
		}
		if atomic.CompareAndSwapInt32(&s.refCount, current, current-1) {
			if current == 1 && atomic.LoadUint32(&s.mustBeDeleted) != 0 {
				s.performDelete()
			}
			return
		}
	}
}

// closeResourcesLocked closes the segment's series index and shards but keeps
// its on-disk directory, leaving it closed (index==nil) and reopenable. Must be
// called with s.mu held. Idempotent.
func (s *segment[T, O]) closeResourcesLocked() {
	if s.index != nil {
		if err := s.index.Close(); err != nil {
			s.l.Panic().Err(err).Msg("failed to close the series index")
		}
		s.index = nil
	}
	if sLst := s.sLst.Load(); sLst != nil {
		for _, shard := range *sLst {
			shard.close()
		}
		s.sLst.Store(&[]*shard[T]{})
	}
}

// closeIfIdle releases a dormant segment's resources (index writer + shards) if
// it is open, unreferenced, not flagged for deletion, and idle past the
// threshold. The directory is kept so the segment reopens on next access.
// Returns whether it closed the segment.
func (s *segment[T, O]) closeIfIdle(idleThreshold int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.index == nil || atomic.LoadInt32(&s.refCount) != 0 ||
		atomic.LoadUint32(&s.mustBeDeleted) != 0 || s.lastAccessed.Load() >= idleThreshold {
		return false
	}
	s.closeResourcesLocked()
	return true
}

// performDelete closes the segment's resources and removes its directory. It
// runs once the last active reference is dropped, or immediately from delete()
// when the segment is already dormant. Idempotent. Must NOT be called with s.mu
// held.
func (s *segment[T, O]) performDelete() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if atomic.LoadInt32(&s.refCount) > 0 {
		// A reference was acquired before we ran; the next DecRef to zero
		// retries the delete.
		return
	}
	s.closeResourcesLocked()
	s.lfs.MustRMAll(s.location)
}

// delete flags the segment for deletion. If it is dormant (no active reference)
// the delete happens now; otherwise it is deferred to the last DecRef.
func (s *segment[T, O]) delete() {
	atomic.StoreUint32(&s.mustBeDeleted, 1)
	if atomic.LoadInt32(&s.refCount) == 0 {
		s.performDelete()
	}
}

// Location returns the on-disk directory of the segment.
func (s *segment[T, O]) Location() string {
	return s.location
}

// SeriesIndexStats returns the series index document count and on-disk size.
// An open segment is reported from its live index; a closed segment is read
// from disk read-only (via OpenReader + directory walk), so inspecting a cold
// segment never reopens its writable index.
func (s *segment[T, O]) SeriesIndexStats() (int64, int64) {
	s.mu.RLock()
	if s.index != nil {
		// Hold the read lock while reading live stats so a concurrent reclaim
		// (closeIfIdle / performDelete) cannot close the index out from under us.
		defer s.mu.RUnlock()
		return s.index.Stats()
	}
	s.mu.RUnlock()
	// Closed: release the lock (so a concurrent reopen is not blocked) and read
	// read-only. Best-effort -- a missing/unflushed/concurrently-removed index
	// reports 0.
	indexPath := filepath.Join(s.location, seriesIndexDirName)
	count, err := inverted.ReadOnlyDocCount(indexPath)
	if err != nil {
		s.l.Debug().Err(err).Str("path", indexPath).Msg("closed series index has no readable doc count")
	}
	size, _ := calculatePathSize(indexPath)
	return count, int64(size)
}

// snapshotInto writes a point-in-time snapshot of this segment under dst.
//
// It NEVER reopens a closed segment -- reopening an idle-closed cold segment is
// the root cause of the nil-index panic and the bluge "exclusive lock" churn.
// A closed (quiescent) segment is hard-linked directly from its immutable
// on-disk files; an open segment is snapshotted through its live series index
// and shard tables while a reference is held to keep it open.
//
// Returns whether anything was written (false when the segment is being
// deleted, so the caller can skip it).
func (s *segment[T, O]) snapshotInto(dst string) (bool, error) {
	s.mu.Lock()
	if atomic.LoadUint32(&s.mustBeDeleted) != 0 {
		s.mu.Unlock()
		return false, nil
	}
	idx := s.index
	if idx != nil {
		// Open: the bump only pins it (never reopens). Release the mutex before
		// the slow backup so other callers are not blocked.
		atomic.AddInt32(&s.refCount, 1)
		s.mu.Unlock()
		defer s.DecRef()
		return s.snapshotOpen(dst, idx)
	}
	// Closed and quiescent: hold s.mu for the fast hard-link so a concurrent
	// reopen cannot write into the directory mid-copy.
	defer s.mu.Unlock()
	return s.snapshotClosed(dst)
}

// snapshotClosed hard-links the whole quiescent segment directory into dst.
// Must be called with s.mu held and s.index == nil.
func (s *segment[T, O]) snapshotClosed(dst string) (bool, error) {
	segDir := filepath.Base(s.location)
	segPath := filepath.Join(dst, segDir)
	if err := s.lfs.CreateHardLink(s.location, segPath, includeInClosedSnapshot); err != nil {
		return false, errors.Wrapf(err, "failed to hard-link closed segment %s", segDir)
	}
	return true, nil
}

// includeInClosedSnapshot reports whether a file or directory under a closed
// segment should be hard-linked into a snapshot. It excludes the transient and
// non-current artifacts that the open-path snapshot never copies: the bluge
// lock file, the failed-parts directory, the external-segment temp directory,
// and partial ".tmp" atomic-write files. Current part directories and their
// ".snp" manifests are kept.
func includeInClosedSnapshot(p string) bool {
	switch base := filepath.Base(p); {
	case base == inverted.LockFilename, base == FailedPartsDirName, base == inverted.ExternalSegmentTempDirName:
		return false
	default:
		return filepath.Ext(base) != ".tmp"
	}
}

// snapshotOpen snapshots an open segment through its live state. idx is the
// series index captured under s.mu; a reference is held by the caller so the
// segment (and idx) stays open for the duration.
func (s *segment[T, O]) snapshotOpen(dst string, idx *seriesIndex) (bool, error) {
	segDir := filepath.Base(s.location)
	segPath := filepath.Join(dst, segDir)
	s.lfs.MkdirIfNotExist(segPath, DirPerm)

	metadataSrc := filepath.Join(s.location, metadataFilename)
	metadataDest := filepath.Join(segPath, metadataFilename)
	if err := s.lfs.CreateHardLink(metadataSrc, metadataDest, nil); err != nil {
		return false, errors.Wrapf(err, "failed to snapshot metadata for segment %s", segDir)
	}

	indexPath := filepath.Join(segPath, seriesIndexDirName)
	s.lfs.MkdirIfNotExist(indexPath, DirPerm)
	if err := idx.store.TakeFileSnapshot(indexPath); err != nil {
		return false, errors.Wrapf(err, "failed to snapshot index for segment %s", segDir)
	}

	sLst := s.sLst.Load()
	if sLst != nil {
		for _, shard := range *sLst {
			shardDir := filepath.Base(shard.location)
			shardPath := filepath.Join(segPath, shardDir)
			s.lfs.MkdirIfNotExist(shardPath, DirPerm)
			if _, err := shard.table.TakeFileSnapshot(shardPath); err != nil {
				if errors.Is(err, ErrNoCurrentSnapshot) {
					s.l.Debug().Str("shard", shardDir).Str("segment", segDir).
						Msg("skipping empty shard snapshot")
					continue
				}
				return false, errors.Wrapf(err, "failed to snapshot shard %s in segment %s", shardDir, segDir)
			}
		}
	}
	return true, nil
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

func (sc *segmentController[T, O]) getSegmentInterval() IntervalRule {
	sc.optsMutex.RLock()
	defer sc.optsMutex.RUnlock()
	return sc.opts.SegmentInterval
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

func (sc *segmentController[T, O]) selectSegments(timeRange timestamp.TimeRange, reopenClosed bool) (tt []Segment[T, O], err error) {
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
			if reopenClosed {
				// Real read: reopen if closed and mark as accessed.
				if err = s.incRef(ctx); err != nil {
					return nil, err
				}
				s.lastAccessed.Store(now)
			} else {
				// Stats peek: pin only if already open, never reopen.
				for {
					current := atomic.LoadInt32(&s.refCount)
					if current <= 0 {
						break
					}
					if atomic.CompareAndSwapInt32(&s.refCount, current, current+1) {
						break
					}
				}
			}
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
			// Pin only if already open (CAS while refCount>0); a dormant/closed
			// segment is returned unbumped so the idle reclaimer is never raced
			// into resurrecting it, and the caller's DecRef on it is a no-op.
			for {
				current := atomic.LoadInt32(&sc.lst[i].refCount)
				if current <= 0 {
					break
				}
				if atomic.CompareAndSwapInt32(&sc.lst[i].refCount, current, current+1) {
					break
				}
			}
		}
		r[i] = sc.lst[i]
	}
	return r, nil
}

// copySegments returns a snapshot of the current segment list WITHOUT touching
// reference counts or reopening anything. Callers (e.g. TakeFileSnapshot) that
// must not force a reopen decide per-segment, under the segment's own lock,
// whether it is open or closed.
func (sc *segmentController[T, O]) copySegments() []*segment[T, O] {
	sc.RLock()
	defer sc.RUnlock()
	r := make([]*segment[T, O], len(sc.lst))
	copy(r, sc.lst)
	return r
}

// closeIdleSegments releases the resources of every dormant, idle segment
// (index!=nil, refCount==0, not flagged for deletion, lastAccessed past the
// idle threshold), keeping their directories so they reopen on next access.
// Each segment is decided under its own s.mu, where the refCount==0 check is
// stable against concurrent acquire (which takes the same lock to go 0->1).
func (sc *segmentController[T, O]) closeIdleSegments() int {
	idleThreshold := time.Now().UnixNano() - sc.idleTimeout.Nanoseconds()
	sc.RLock()
	segs := make([]*segment[T, O], len(sc.lst))
	copy(segs, sc.lst)
	sc.RUnlock()

	closedCount := 0
	for _, s := range segs {
		if s.closeIfIdle(idleThreshold) {
			closedCount++
		}
	}
	return closedCount
}

func (sc *segmentController[T, O]) format(tm time.Time) string {
	return FormatSegmentTime(tm, sc.getOptions().SegmentInterval)
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

// ParseSegmentTime parses a segment-directory suffix back into its start time
// for the given interval unit, the inverse of the suffix formatting used when a
// segment directory is named. Callers outside this package use it to map the
// suffixes returned by VisitSegmentsInTimeRange to the segment instants a
// visitor observed.
func ParseSegmentTime(suffix string, rule IntervalRule) (time.Time, error) {
	return parseSegmentTime(suffix, rule.Unit)
}

// FormatSegmentTime formats a segment start time into its directory suffix for
// the given interval rule, the inverse of ParseSegmentTime. The returned suffix
// excludes the "seg-" directory prefix.
func FormatSegmentTime(t time.Time, rule IntervalRule) string {
	switch rule.Unit {
	case HOUR:
		return t.Format(hourFormat)
	case DAY:
		return t.Format(dayFormat)
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

// getRetentionDeadline returns the earliest timestamp that is still within the
// retention window. Data points with a timestamp before this deadline are
// expired by the TTL policy. Retention removes a segment only once its whole
// time range falls before the deadline (see (*segmentController).remove), so a
// fully expired segment can linger on disk until the next retention run.
// Queries should exclude such fully expired segments to avoid serving TTL-expired
// data; partially expired segments remain visible until their end passes the deadline.
func (sc *segmentController[T, O]) getRetentionDeadline() time.Time {
	return sc.clock.Now().Local().Add(-sc.getOptions().TTL.estimatedDuration())
}

func (sc *segmentController[T, O]) getExpiredSegmentsTimeRange() *timestamp.TimeRange {
	deadline := sc.clock.Now().Local().Add(-sc.opts.TTL.estimatedDuration())
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
	deadline := sc.clock.Now().Local().Add(-sc.opts.TTL.estimatedDuration())
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
// It returns the zero time and false if no segments exist or the oldest segment is closed.
func (sc *segmentController[T, O]) peekOldestSegmentEndTime() (time.Time, bool) {
	sc.RLock()
	defer sc.RUnlock()

	if len(sc.lst) <= 1 {
		return time.Time{}, false
	}

	// Segments are sorted by ID (which correlates with start time),
	// so the first one is the oldest
	oldest := sc.lst[0]

	oldest.mu.RLock()
	open := oldest.index != nil
	oldest.mu.RUnlock()
	if open {
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
	// Full shutdown: release every segment's resources regardless of refCount
	// (DecRef no longer closes on reaching zero). A segment flagged for deletion
	// also has its directory removed; the rest keep their data on disk.
	for _, s := range sc.lst {
		s.mu.Lock()
		// Persist pending in-memory series documents before closing so an offline
		// reader (dump/migration) sees a complete index; skip segments flagged for
		// deletion since their directory is about to be removed. This runs only on
		// shutdown, not on the idle-reclaim/delete paths.
		if s.index != nil && atomic.LoadUint32(&s.mustBeDeleted) == 0 {
			if err := s.index.Flush(); err != nil {
				s.l.Warn().Err(err).Msg("failed to flush the series index on shutdown")
			}
		}
		s.closeResourcesLocked()
		if atomic.LoadUint32(&s.mustBeDeleted) != 0 {
			s.lfs.MustRMAll(s.location)
		}
		s.mu.Unlock()
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
