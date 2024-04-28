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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/fs"
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
	lockFilename   = "lock"
	filePermission = 0o600
)

// TSDBOpts wraps options to create a tsdb.
type TSDBOpts[T TSTable, O any] struct {
	Option                         O
	TSTableCreator                 TSTableCreator[T, O]
	Location                       string
	SegmentInterval                IntervalRule
	TTL                            IntervalRule
	ShardNum                       uint32
	SeriesIndexFlushTimeoutSeconds int64
}

type (
	segmentID uint32
)

func generateSegID(unit IntervalUnit, suffix int) segmentID {
	return segmentID(unit)<<31 | ((segmentID(suffix) << 1) >> 1)
}

type database[T TSTable, O any] struct {
	logger          *logger.Logger
	lock            fs.File
	indexController *seriesIndexController[T, O]
	scheduler       *timestamp.Scheduler
	p               common.Position
	location        string
	sLst            atomic.Pointer[[]*shard[T, O]]
	opts            TSDBOpts[T, O]
	sync.RWMutex
}

func (d *database[T, O]) Close() error {
	d.Lock()
	defer d.Unlock()
	d.scheduler.Close()
	sLst := d.sLst.Load()
	if sLst != nil {
		for _, s := range *sLst {
			s.close()
		}
	}
	d.lock.Close()
	if err := lfs.DeleteFile(d.lock.Path()); err != nil {
		logger.Panicf("cannot delete lock file %s: %s", d.lock.Path(), err)
	}
	return d.indexController.Close()
}

// OpenTSDB returns a new tsdb runtime. This constructor will create a new database if it's absent,
// or load an existing one.
func OpenTSDB[T TSTable, O any](ctx context.Context, opts TSDBOpts[T, O]) (TSDB[T, O], error) {
	if opts.SegmentInterval.Num == 0 {
		return nil, errors.Wrap(errOpenDatabase, "segment interval is absent")
	}
	if opts.TTL.Num == 0 {
		return nil, errors.Wrap(errOpenDatabase, "ttl is absent")
	}
	p := common.GetPosition(ctx)
	location := filepath.Clean(opts.Location)
	lfs.MkdirIfNotExist(location, dirPerm)
	sir, err := newSeriesIndexController(ctx, opts)
	if err != nil {
		return nil, errors.Wrap(errOpenDatabase, errors.WithMessage(err, "create series index controller failed").Error())
	}
	l := logger.Fetch(ctx, p.Database)
	clock, _ := timestamp.GetClock(ctx)
	scheduler := timestamp.NewScheduler(l, clock)
	db := &database[T, O]{
		location:        location,
		scheduler:       scheduler,
		logger:          l,
		indexController: sir,
		opts:            opts,
		p:               p,
	}
	db.logger.Info().Str("path", opts.Location).Msg("initialized")
	lockPath := filepath.Join(opts.Location, lockFilename)
	lock, err := lfs.CreateLockFile(lockPath, filePermission)
	if err != nil {
		logger.Panicf("cannot create lock file %s: %s", lockPath, err)
	}
	db.lock = lock
	if err = db.loadDatabase(); err != nil {
		return nil, errors.Wrap(errOpenDatabase, errors.WithMessage(err, "load database failed").Error())
	}
	retentionTask := newRetentionTask(db, opts.TTL)
	if err = db.scheduler.Register("retention", retentionTask.option, retentionTask.expr, retentionTask.run); err != nil {
		return nil, err
	}
	return db, nil
}

func (d *database[T, O]) CreateTSTableIfNotExist(shardID common.ShardID, ts time.Time) (TSTableWrapper[T], error) {
	if s, ok := d.getShard(shardID); ok {
		d.RLock()
		defer d.RUnlock()
		return d.createTSTTable(s, ts)
	}
	d.Lock()
	defer d.Unlock()
	if s, ok := d.getShard(shardID); ok {
		return d.createTSTTable(s, ts)
	}
	d.logger.Info().Int("shard_id", int(shardID)).Msg("creating a shard")
	s, err := d.registerShard(shardID)
	if err != nil {
		return nil, err
	}
	return d.createTSTTable(s, ts)
}

func (d *database[T, O]) getShard(shardID common.ShardID) (*shard[T, O], bool) {
	sLst := d.sLst.Load()
	if sLst != nil {
		for _, s := range *sLst {
			if s.id == shardID {
				return s, true
			}
		}
	}
	return nil, false
}

func (d *database[T, O]) createTSTTable(shard *shard[T, O], ts time.Time) (TSTableWrapper[T], error) {
	timeRange := timestamp.NewInclusiveTimeRange(ts, ts)
	ss := shard.segmentController.selectTSTables(timeRange)
	if len(ss) > 0 {
		return ss[0], nil
	}
	return shard.segmentController.createTSTable(ts)
}

func (d *database[T, O]) SelectTSTables(timeRange timestamp.TimeRange) []TSTableWrapper[T] {
	var result []TSTableWrapper[T]
	sLst := d.sLst.Load()
	if sLst == nil {
		return result
	}
	for _, s := range *sLst {
		result = append(result, s.segmentController.selectTSTables(timeRange)...)
	}
	return result
}

func (d *database[T, O]) registerShard(id common.ShardID) (*shard[T, O], error) {
	if s, ok := d.getShard(id); ok {
		return s, nil
	}
	ctx := context.WithValue(context.Background(), logger.ContextKey, d.logger)
	ctx = common.SetPosition(ctx, func(_ common.Position) common.Position {
		return d.p
	})
	so, err := d.openShard(ctx, id)
	if err != nil {
		return nil, err
	}
	var shardList []*shard[T, O]
	sLst := d.sLst.Load()
	if sLst != nil {
		shardList = *sLst
	}
	shardList = append(shardList, so)
	d.sLst.Store(&shardList)
	return so, nil
}

func (d *database[T, O]) loadDatabase() error {
	d.Lock()
	defer d.Unlock()
	return walkDir(d.location, shardPathPrefix, func(suffix string) error {
		shardID, err := strconv.Atoi(suffix)
		if err != nil {
			return err
		}
		if shardID >= int(d.opts.ShardNum) {
			return nil
		}
		d.logger.Info().Int("shard_id", shardID).Msg("loaded a existed shard")
		_, err = d.registerShard(common.ShardID(shardID))
		return err
	})
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
