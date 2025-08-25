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
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/banyand/observability"
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
	StorageMetricsFactory          *observability.Factory
	Location                       string
	SegmentInterval                IntervalRule
	TTL                            IntervalRule
	SeriesIndexFlushTimeoutSeconds int64
	SeriesIndexCacheMaxBytes       int
	ShardNum                       uint32
	DisableRetention               bool
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
	logger            *logger.Logger
	scheduler         *timestamp.Scheduler
	tsEventCh         chan int64
	segmentController *segmentController[T, O]
	*metrics
	lfs            fs.FileSystem
	p              common.Position
	location       string
	latestTickTime atomic.Int64
	sync.RWMutex
	rotationProcessOn atomic.Bool
	closed            atomic.Bool
	disableRetention  bool
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
		logger.Panicf("cannot delete lock file %s: %s", d.lock.Path(), err)
	}
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
		metrics:          newMetrics(opts.StorageMetricsFactory),
		disableRetention: opts.DisableRetention,
		lfs:              tsdbLfs,
	}
	db.logger.Info().Str("path", opts.Location).Msg("initialized")
	lockPath := filepath.Join(opts.Location, lockFilename)
	lock, err := tsdbLfs.CreateLockFile(lockPath, FilePerm)
	if err != nil {
		logger.Panicf("cannot create lock file %s: %s", lockPath, err)
	}
	db.lock = lock
	if err := db.segmentController.open(); err != nil {
		return nil, err
	}
	observability.MetricsCollector.Register(location, db.collect)
	return db, db.startRotationTask()
}

func (d *database[T, O]) CreateSegmentIfNotExist(ts time.Time) (Segment[T, O], error) {
	if d.closed.Load() {
		return nil, errors.New("database is closed")
	}
	return d.segmentController.createSegment(ts)
}

func (d *database[T, O]) SelectSegments(timeRange timestamp.TimeRange) ([]Segment[T, O], error) {
	if d.closed.Load() {
		return nil, nil
	}
	return d.segmentController.selectSegments(timeRange)
}

func (d *database[T, O]) UpdateOptions(resourceOpts *commonv1.ResourceOpts) {
	if d.closed.Load() {
		return
	}
	d.segmentController.updateOptions(resourceOpts)
}

func (d *database[T, O]) TakeFileSnapshot(dst string) error {
	if d.closed.Load() {
		return errors.New("database is closed")
	}

	segments, err := d.segmentController.segments(true)
	if err != nil {
		return errors.Wrap(err, "failed to get segments")
	}
	defer func() {
		for _, seg := range segments {
			seg.DecRef()
		}
	}()

	for _, seg := range segments {
		segDir := filepath.Base(seg.location)
		segPath := filepath.Join(dst, segDir)
		d.lfs.MkdirIfNotExist(segPath, DirPerm)

		metadataSrc := filepath.Join(seg.location, metadataFilename)
		metadataDest := filepath.Join(segPath, metadataFilename)
		if err := d.lfs.CreateHardLink(metadataSrc, metadataDest, nil); err != nil {
			return errors.Wrapf(err, "failed to snapshot metadata for segment %s", segDir)
		}

		indexPath := filepath.Join(segPath, seriesIndexDirName)
		d.lfs.MkdirIfNotExist(indexPath, DirPerm)
		if err := seg.index.store.TakeFileSnapshot(indexPath); err != nil {
			return errors.Wrapf(err, "failed to snapshot index for segment %s", segDir)
		}

		sLst := seg.sLst.Load()
		if sLst == nil {
			continue
		}
		for _, shard := range *sLst {
			shardDir := filepath.Base(shard.location)
			shardPath := filepath.Join(segPath, shardDir)
			d.lfs.MkdirIfNotExist(shardPath, DirPerm)
			if err := shard.table.TakeFileSnapshot(shardPath); err != nil {
				return errors.Wrapf(err, "failed to snapshot shard %s in segment %s", shardDir, segDir)
			}
		}
	}

	return nil
}

func (d *database[T, O]) GetExpiredSegmentsTimeRange() *timestamp.TimeRange {
	return d.segmentController.getExpiredSegmentsTimeRange()
}

func (d *database[T, O]) DeleteExpiredSegments(timeRange timestamp.TimeRange) int64 {
	return d.segmentController.deleteExpiredSegments(timeRange)
}

func (d *database[T, O]) collect() {
	if d.closed.Load() {
		return
	}
	if d.metrics == nil {
		return
	}
	d.metrics.lastTickTime.Set(float64(d.latestTickTime.Load()))
	refCount := int32(0)
	ss, _ := d.segmentController.segments(false)
	for _, s := range ss {
		if atomic.LoadInt32(&s.refCount) <= 0 {
			continue
		}
		tables, _ := s.Tables()
		for _, t := range tables {
			t.Collect(d.segmentController.metrics)
		}
		s.index.store.CollectMetrics(s.index.p.SegLabelValues()...)
		s.DecRef()
		refCount += atomic.LoadInt32(&s.refCount)
	}
	d.totalSegRefs.Set(float64(refCount))
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
