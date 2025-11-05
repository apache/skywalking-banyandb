// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package wqueue

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

const (
	shardPathPrefix = "shard"
	shardTemplate   = shardPathPrefix + "-%d"
	lockFilename    = "lock"
)

var timeEventSnapDuration = (10 * time.Minute).Nanoseconds()

// ErrUnknownShard indicates that the shard is not found.
var ErrUnknownShard = errors.New("unknown shard")

// Metrics is the interface of metrics.
type Metrics interface {
	// DeleteAll deletes all metrics.
	DeleteAll()
}

// SubQueue represents a sub-queue interface that can be closed.
type SubQueue interface {
	io.Closer
}

// SubQueueCreator is a function type that creates a sub-queue with the given parameters.
type SubQueueCreator[S SubQueue, O any] func(fileSystem fs.FileSystem, root string, position common.Position,
	l *logger.Logger, option O, metrics any, group string, shardID common.ShardID, getNodes func() []string) (S, error)

// Opts contains configuration options for creating a queue.
type Opts[S SubQueue, O any] struct {
	SubQueueCreator SubQueueCreator[S, O]
	GetNodes        func(common.ShardID) []string
	Metrics         Metrics
	Option          O
	Group           string
	Location        string
	SegmentInterval storage.IntervalRule
	TTL             storage.IntervalRule
	ShardNum        uint32
}

// Queue represents a write queue that manages multiple shards.
type Queue[S SubQueue, O any] struct {
	lfs            fs.FileSystem
	lock           fs.File
	logger         *logger.Logger
	validTimeRange atomic.Pointer[timestamp.TimeRange]
	p              common.Position
	location       string
	sLst           []*Shard[S]
	opts           Opts[S, O]
	latestTickTime atomic.Int64
	sync.RWMutex
	closed atomic.Bool
}

// UpdateOptions updates the queue options with new resource options.
func (q *Queue[S, O]) UpdateOptions(resourceOpts *commonv1.ResourceOpts) {
	if q.closed.Load() {
		return
	}
	q.Lock()
	defer q.Unlock()
	si := storage.MustToIntervalRule(resourceOpts.SegmentInterval)
	if q.opts.SegmentInterval.Unit != si.Unit {
		q.logger.Panic().Msg("segment interval unit cannot be changed")
		return
	}
	q.opts.SegmentInterval = si
	q.opts.TTL = storage.MustToIntervalRule(resourceOpts.Ttl)
	q.opts.ShardNum = resourceOpts.ShardNum
}

func (q *Queue[S, O]) getOpts() Opts[S, O] {
	q.RLock()
	defer q.RUnlock()
	return q.opts
}

// Close closes the queue and all its shards.
func (q *Queue[S, O]) Close() error {
	if q.closed.Load() {
		return nil
	}
	q.closed.Store(true)
	q.Lock()
	defer q.Unlock()
	for _, shard := range q.sLst {
		shard.Close()
	}
	q.lock.Close()
	if err := q.lfs.DeleteFile(q.lock.Path()); err != nil {
		logger.Panicf("cannot delete lock file %s: %s", q.lock.Path(), err)
	}
	return nil
}

// Open creates and initializes a new queue with the given options.
func Open[S SubQueue, O any](ctx context.Context, opts Opts[S, O], _ string) (*Queue[S, O], error) {
	p := common.GetPosition(ctx)
	location := filepath.Clean(opts.Location)
	lfs := fs.NewLocalFileSystemWithLogger(logger.GetLogger("wqueue"))
	lfs.MkdirIfNotExist(location, storage.DirPerm)
	l := logger.Fetch(ctx, p.Database)
	q := &Queue[S, O]{
		location: location,
		logger:   l,
		p:        p,
		lfs:      lfs,
		opts:     opts,
	}
	q.logger.Info().Str("path", opts.Location).Msg("initialized")
	lockPath := filepath.Join(opts.Location, lockFilename)
	lock, err := lfs.CreateLockFile(lockPath, storage.FilePerm)
	if err != nil {
		logger.Panicf("cannot create lock file %s: %s", lockPath, err)
	}
	q.lock = lock
	return q, nil
}

// GetOrCreateShard gets or creates a shard with the given ShardID.
// If the shard already exists, it returns it without locking.
// If the shard doesn't exist, it creates a new one with proper locking.
func (q *Queue[S, O]) GetOrCreateShard(shardID common.ShardID) (*Shard[S], error) {
	// First check if shard exists without locking
	if shard := q.getShard(shardID); shard != nil {
		return shard, nil
	}

	// Shard doesn't exist, need to create it with locking
	q.Lock()
	defer q.Unlock()

	// Double-check after acquiring lock
	if shard := q.getShard(shardID); shard != nil {
		return shard, nil
	}

	// Create the shard directory path
	shardPath := filepath.Join(q.location, fmt.Sprintf(shardTemplate, int(shardID)))

	// Create the shard directory if it doesn't exist
	q.lfs.MkdirIfNotExist(shardPath, storage.DirPerm)

	// Create the sub-queue using the provided creator
	subQueue, err := q.opts.SubQueueCreator(q.lfs, shardPath, q.p, q.logger, q.opts.Option, q.opts.Metrics, q.opts.Group, shardID, func() []string {
		return q.opts.GetNodes(shardID)
	})
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create sub-queue for shard %d", shardID)
	}

	// Create the shard
	shard := &Shard[S]{
		sq:       subQueue,
		l:        q.logger,
		location: shardPath,
		id:       shardID,
	}

	// Add the shard to the list
	q.sLst = append(q.sLst, shard)

	q.logger.Info().Uint32("shard_id", uint32(shardID)).Str("path", shardPath).Msg("created new shard")
	return shard, nil
}

// getShard returns the shard with the given ID if it exists, nil otherwise.
// This method is not thread-safe and should only be called when the caller holds the appropriate lock.
func (q *Queue[S, O]) getShard(shardID common.ShardID) *Shard[S] {
	for _, shard := range q.sLst {
		if shard.id == shardID {
			return shard
		}
	}
	return nil
}

// GetTimeRange returns a valid time range based on an input timestamp.
// It uses the Queue's SegmentInterval to generate the time range.
func (q *Queue[S, O]) GetTimeRange(ts time.Time) timestamp.TimeRange {
	opts := q.getOpts()
	start := opts.SegmentInterval.Unit.Standard(ts)
	end := opts.SegmentInterval.NextTime(start)
	return timestamp.NewSectionTimeRange(start, end)
}

// GetNodes returns the nodes for the given shard ID.
func (q *Queue[S, O]) GetNodes(shardID common.ShardID) []string {
	return q.opts.GetNodes(shardID)
}

// updateValidTimeRange calculates and updates the valid time range based on TTL and SegmentInterval.
func (q *Queue[S, O]) updateValidTimeRange(ts int64) {
	opts := q.getOpts()
	t := time.Unix(0, ts)

	// Calculate oldest segment start: beginning of oldest valid segment
	oldestTime := t.Add(-estimatedDuration(opts.TTL))
	oldestStart := opts.SegmentInterval.Unit.Standard(oldestTime)

	// Calculate latest segment end: end of current segment
	currentSegmentStart := opts.SegmentInterval.Unit.Standard(t)
	latestEnd := opts.SegmentInterval.NextTime(currentSegmentStart)

	// Create and store the time range
	tr := timestamp.TimeRange{
		Start:        oldestStart,
		End:          latestEnd,
		IncludeStart: true,
		IncludeEnd:   false,
	}
	q.validTimeRange.Store(&tr)
}

// estimatedDuration calculates the estimated duration for an IntervalRule.
func estimatedDuration(ir storage.IntervalRule) time.Duration {
	switch ir.Unit {
	case storage.HOUR:
		return time.Hour * time.Duration(ir.Num)
	case storage.DAY:
		return 24 * time.Hour * time.Duration(ir.Num)
	}
	panic("invalid interval unit")
}

// IsValidTime checks if a given timestamp is within the valid time range.
// On the first call, it initializes the time range based on the provided timestamp.
// If the timestamp exceeds the current time range's end, it updates the time range
// to accommodate the new timestamp, respecting time snapping to avoid excessive updates.
// The time range is automatically maintained based on TTL and SegmentInterval.
func (q *Queue[S, O]) IsValidTime(t time.Time) bool {
	ts := t.UnixNano()
	vtr := q.validTimeRange.Load()

	// Initialize time range on first call
	if vtr == nil {
		q.latestTickTime.Store(ts)
		q.updateValidTimeRange(ts)
		return true
	}

	// If timestamp is beyond the current range's end, update the time range
	// but only if enough time has passed since the last update (time snapping)
	if ts >= vtr.End.UnixNano() {
		// Check if we should update based on time snapping
		if (ts - timeEventSnapDuration) >= q.latestTickTime.Load() {
			q.latestTickTime.Store(ts)
			q.updateValidTimeRange(ts)
		}
		// Even if we didn't update due to snapping, the timestamp is still valid
		// because it's in the future and will be included in the next segment
		return true
	}

	return vtr.Contains(ts)
}
