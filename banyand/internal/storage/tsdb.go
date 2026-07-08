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
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	obsservice "github.com/apache/skywalking-banyandb/banyand/observability/services"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/index/inverted"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

// IndexGranularity denotes the granularity of the local index.
type IndexGranularity int

// The options of the local index granularity.
const (
	IndexGranularityBlock IndexGranularity = iota
	IndexGranularitySeries
)

const (
	lockFilename = "lock"
)

// TSDBOpts wraps options to create a tsdb.
type TSDBOpts[T TSTable, O any] struct {
	Option                         O
	TableMetrics                   Metrics
	TSTableCreator                 TSTableCreator[T, O]
	StorageMetricsFactory          observability.Factory
	Location                       string
	SegmentInterval                IntervalRule
	TTL                            IntervalRule
	SeriesIndexFlushTimeoutSeconds int64
	SeriesIndexCacheMaxBytes       int
	ShardNum                       uint32
	DisableRetention               bool
	DisableRotation                bool
	SegmentIdleTimeout             time.Duration
	MemoryLimit                    uint64
}

type (
	segmentID uint32
)

func generateSegID(unit IntervalUnit, suffix int) segmentID {
	return segmentID(unit)<<31 | ((segmentID(suffix) << 1) >> 1)
}

var _ Cache = (*groupCache)(nil)

type groupCache struct {
	cache Cache
	group string
}

func (gc *groupCache) Get(key EntryKey) Sizable {
	if gc.cache == nil {
		return nil
	}
	key.group = gc.group
	return gc.cache.Get(key)
}

func (gc *groupCache) Put(key EntryKey, value Sizable) {
	if gc.cache == nil {
		return
	}
	key.group = gc.group
	gc.cache.Put(key, value)
}

func (gc *groupCache) Close() {
	if gc.cache != nil {
		gc.cache.Close()
	}
}

func (gc *groupCache) Requests() uint64 {
	if gc.cache == nil {
		return 0
	}
	return gc.cache.Requests()
}

func (gc *groupCache) Misses() uint64 {
	if gc.cache == nil {
		return 0
	}
	return gc.cache.Misses()
}

func (gc *groupCache) Entries() uint64 {
	if gc.cache == nil {
		return 0
	}
	return gc.cache.Entries()
}

func (gc *groupCache) Size() uint64 {
	if gc.cache == nil {
		return 0
	}
	return gc.cache.Size()
}

func (gc *groupCache) get(key EntryKey) Sizable {
	return gc.Get(key)
}

func (gc *groupCache) put(key EntryKey, value Sizable) {
	gc.Put(key, value)
}

type database[T TSTable, O any] struct {
	lock              fs.File
	lfs               fs.FileSystem
	tsEventCh         chan int64
	scheduler         *timestamp.Scheduler
	segmentController *segmentController[T, O]
	metricsFactory    observability.Factory
	*metrics
	logger         *logger.Logger
	retentionGate  chan struct{}
	p              common.Position
	location       string
	latestTickTime atomic.Int64
	sync.RWMutex
	rotationProcessOn atomic.Bool
	closed            atomic.Bool
	disableRetention  bool
	disableRotation   bool
}

func (d *database[T, O]) Close() error {
	if d.closed.Load() {
		return nil
	}
	d.closed.Store(true)
	d.Lock()
	defer d.Unlock()
	d.scheduler.Close()
	close(d.tsEventCh)
	d.segmentController.close()
	d.lock.Close()
	if err := d.lfs.DeleteFile(d.lock.Path()); err != nil {
		panic(fmt.Errorf("cannot delete lock file %s: %w", d.lock.Path(), err))
	}
	if d.metricsFactory != nil {
		d.metricsFactory.Close()
	}
	obsservice.MetricsCollector.Unregister(d.location)
	return nil
}

// Drop closes the database and removes all data files from disk.
func (d *database[T, O]) Drop() (err error) {
	if closeErr := d.Close(); closeErr != nil {
		return closeErr
	}
	defer func() {
		if r := recover(); r != nil {
			err = errors.Errorf("failed to remove database directory %s: %v", d.location, r)
		}
	}()
	d.lfs.MustRMAll(d.location)
	return nil
}

// OpenTSDB returns a new tsdb runtime. This constructor will create a new database if it's absent,
// or load an existing one.
func OpenTSDB[T TSTable, O any](ctx context.Context, opts TSDBOpts[T, O], cache Cache, group string) (TSDB[T, O], error) {
	if opts.SegmentInterval.Num == 0 {
		return nil, errors.Wrap(errOpenDatabase, "segment interval is absent")
	}
	if opts.TTL.Num == 0 {
		return nil, errors.Wrap(errOpenDatabase, "ttl is absent")
	}
	p := common.GetPosition(ctx)
	location := filepath.Clean(opts.Location)
	tsdbLfs := fs.NewLocalFileSystemWithLoggerAndLimit(logger.GetLogger("storage"), opts.MemoryLimit)
	tsdbLfs.MkdirIfNotExist(location, DirPerm)
	l := logger.Fetch(ctx, p.Database)
	clock, _ := timestamp.GetClock(ctx)
	scheduler := timestamp.NewScheduler(l, clock)

	var indexMetrics *inverted.Metrics
	if opts.StorageMetricsFactory != nil {
		indexMetrics = inverted.NewMetrics(opts.StorageMetricsFactory, common.SegLabelNames()...)
	}
	var sc Cache
	if cache != nil {
		sc = cache
	}
	db := &database[T, O]{
		location:  location,
		scheduler: scheduler,
		logger:    l,
		tsEventCh: make(chan int64),
		p:         p,
		segmentController: newSegmentController(ctx, location,
			l, opts, indexMetrics, opts.TableMetrics, opts.SegmentIdleTimeout, tsdbLfs, sc, group),
		metricsFactory:   opts.StorageMetricsFactory,
		metrics:          newMetrics(opts.StorageMetricsFactory),
		disableRetention: opts.DisableRetention,
		lfs:              tsdbLfs,
		retentionGate:    make(chan struct{}, 1),
	}
	db.logger.Info().Str("path", opts.Location).Msg("initialized")
	lockPath := filepath.Join(opts.Location, lockFilename)
	lock, err := tsdbLfs.CreateLockFile(lockPath, FilePerm)
	if err != nil {
		panic(fmt.Errorf("cannot create lock file %s: %w", lockPath, err))
	}
	db.lock = lock
	// Release the lock fd if any subsequent step in OpenTSDB returns an error.
	// Otherwise the file descriptor leaks until process exit and a retry on the
	// same data dir hits "resource temporarily unavailable" from the next
	// CreateLockFile call. Ownership transfers to (*database).Close() only on
	// success — released = true is set on the success return below.
	released := false
	defer func() {
		if !released {
			_ = lock.Close()
			_ = tsdbLfs.DeleteFile(lockPath)
		}
	}()
	if err := db.segmentController.open(); err != nil {
		return nil, err
	}
	obsservice.MetricsCollector.Register(location, db.collect)
	db.disableRotation = opts.DisableRotation
	// db (and its lock fd) is now owned by the caller; on rotation-task error
	// the partially-initialized db is still returned so the caller can Close
	// it and unregister the metrics collector.
	released = true
	return db, db.startRotationTask()
}

func (d *database[T, O]) CreateSegmentIfNotExist(ts time.Time) (Segment[T, O], error) {
	if d.closed.Load() {
		return nil, errors.New("database is closed")
	}
	return d.segmentController.createSegment(ts)
}

func (d *database[T, O]) SelectSegments(timeRange timestamp.TimeRange, reopenClosed bool) ([]Segment[T, O], error) {
	if d.closed.Load() {
		return nil, nil
	}
	segments, err := d.segmentController.selectSegments(timeRange, reopenClosed)
	if err != nil || d.disableRetention {
		return segments, err
	}
	// Exclude segments whose whole time range has already passed the retention
	// deadline. Retention removes such a segment only on its next cron run (see
	// (*segmentController).remove), so between runs a fully expired segment is
	// still on disk and would otherwise serve TTL-expired data to queries. Data
	// in partially expired segments that retention still keeps stays visible.
	deadline := d.segmentController.getRetentionDeadline()
	kept := segments[:0]
	for _, s := range segments {
		if s.GetTimeRange().Before(deadline) {
			s.DecRef()
			continue
		}
		kept = append(kept, s)
	}
	return kept, nil
}

func (d *database[T, O]) PeekSegments(timeRange timestamp.TimeRange) []SegmentPeek {
	if d.closed.Load() {
		return nil
	}
	return d.segmentController.peekSegments(timeRange)
}

func (d *database[T, O]) SegmentInterval() IntervalRule {
	return d.segmentController.getSegmentInterval()
}

func (d *database[T, O]) UpdateOptions(resourceOpts *commonv1.ResourceOpts) {
	if d.closed.Load() {
		return
	}
	d.segmentController.updateOptions(resourceOpts)
}

// TakeFileSnapshot writes a point-in-time snapshot of every segment under dst.
// success is true only when at least one segment actually wrote content; it is
// false (with a nil error) when there are no segments, or when every segment is
// concurrently being deleted and thus skipped.
func (d *database[T, O]) TakeFileSnapshot(dst string) (success bool, err error) {
	if d.closed.Load() {
		return false, errors.New("database is closed")
	}

	// The snapshot must NOT reopen a closed segment: reopening an idle-closed
	// cold segment is the source of the nil-index panic and the bluge
	// "exclusive lock" churn. Take the current segment objects WITHOUT forcing
	// a reopen. A closed (quiescent) segment is hard-linked directly from its
	// immutable on-disk files; an open segment is snapshotted through its live
	// series index and shard tables.
	segments := d.segmentController.copySegments()
	if len(segments) == 0 {
		return false, nil
	}

	defer func() {
		if err != nil {
			d.lfs.MustRMAll(dst)
		}
	}()

	log.Info().Int("segment_count", len(segments)).Str("db_location", d.location).
		Msgf("taking file snapshot for %s", dst)
	for _, seg := range segments {
		wrote, segErr := seg.snapshotInto(dst)
		if segErr != nil {
			return false, segErr
		}
		success = success || wrote
	}

	return success, nil
}

func (d *database[T, O]) GetExpiredSegmentsTimeRange() *timestamp.TimeRange {
	return d.segmentController.getExpiredSegmentsTimeRange()
}

func (d *database[T, O]) DeleteExpiredSegments(segmentSuffixes []string) int64 {
	return d.segmentController.deleteExpiredSegments(segmentSuffixes)
}

// PeekOldestSegmentEndTime returns the end time of the oldest segment.
// It acquires the retention gate to ensure exclusivity with TTL operations.
// Returns the zero time and false if no segments exist, database is closed,
// or the retention gate cannot be acquired.
func (d *database[T, O]) PeekOldestSegmentEndTime() (time.Time, bool) {
	if d.closed.Load() {
		return time.Time{}, false
	}

	// Try to acquire retention gate with non-blocking select
	select {
	case d.retentionGate <- struct{}{}:
		defer func() { <-d.retentionGate }()
		return d.segmentController.peekOldestSegmentEndTime()
	default:
		// Retention gate is busy (TTL is running), return false
		return time.Time{}, false
	}
}

// DeleteOldestSegment deletes exactly one oldest segment if it exists and meets safety rules.
// It acquires the retention gate to ensure exclusivity with TTL operations.
// Returns true if a segment was deleted, false if no segments to delete,
// keep-one rule prevents deletion, database is closed, or retention gate cannot be acquired.
func (d *database[T, O]) DeleteOldestSegment() (bool, error) {
	if d.closed.Load() {
		return false, nil
	}

	// Try to acquire retention gate with non-blocking select
	select {
	case d.retentionGate <- struct{}{}:
		defer func() { <-d.retentionGate }()
		return d.segmentController.removeOldest()
	default:
		// Retention gate is busy (TTL is running), skip deletion
		return false, nil
	}
}

func (d *database[T, O]) collect() {
	if d.closed.Load() {
		return
	}
	if d.metrics == nil {
		return
	}
	d.metrics.lastTickTime.Set(float64(d.latestTickTime.Load()))
	// Collect metrics for every open segment. Under the dormant-refcount model
	// an open segment usually has refCount==0, so we cannot rely on a reference
	// bump to pin it: collectOpenMetrics gathers under the segment's read lock,
	// which a concurrent reclaim (closeIfIdle takes the write lock) cannot race.
	openSegments := int32(0)
	for _, s := range d.segmentController.copySegments() {
		if s.collectOpenMetrics(d.segmentController.metrics) {
			openSegments++
		}
	}
	d.totalSegRefs.Set(float64(openSegments))
	if d.metrics.schedulerMetrics == nil {
		return
	}
	metrics := d.scheduler.Metrics()
	for job, m := range metrics {
		d.metrics.schedulerMetrics.Collect(job, m)
	}
}

type walkFn func(suffix string) error

func walkDir(root, prefix string, wf walkFn) error {
	for _, f := range lfs.ReadDir(root) {
		if !f.IsDir() || !strings.HasPrefix(f.Name(), prefix) {
			continue
		}
		segs := strings.Split(f.Name(), "-")
		errWalk := wf(segs[len(segs)-1])
		if errWalk != nil {
			return errors.WithMessagef(errWalk, "failed to load: %s", f.Name())
		}
	}
	return nil
}
