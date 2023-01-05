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

package tsdb

import (
	"context"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/robfig/cron/v3"
	"go.uber.org/multierr"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/tsdb/bucket"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

const (
	defaultBlockQueueSize    = 4
	defaultMaxBlockQueueSize = 64
	defaultKVMemorySize      = 1 << 20
)

var _ Shard = (*shard)(nil)

type shard struct {
	seriesDatabase        SeriesDatabase
	indexDatabase         IndexDatabase
	l                     *logger.Logger
	segmentController     *segmentController
	segmentManageStrategy *bucket.Strategy
	scheduler             *timestamp.Scheduler
	position              common.Position
	closeOnce             sync.Once
	id                    common.ShardID
}

// OpenShard returns an existed Shard or create a new one if not existed.
func OpenShard(ctx context.Context, id common.ShardID,
	root string, segmentSize, blockSize, ttl IntervalRule, openedBlockSize, maxOpenedBlockSize int,
) (Shard, error) {
	path, err := mkdir(shardTemplate, root, int(id))
	if err != nil {
		return nil, errors.Wrapf(err, "make the directory of the shard %d ", int(id))
	}
	l := logger.Fetch(ctx, "shard"+strconv.Itoa(int(id)))
	l.Info().Int("shard_id", int(id)).Str("path", path).Msg("creating a shard")
	if openedBlockSize < 1 {
		openedBlockSize = defaultBlockQueueSize
	}
	shardCtx := context.WithValue(ctx, logger.ContextKey, l)
	shardCtx = common.SetPosition(shardCtx, func(p common.Position) common.Position {
		p.Shard = strconv.Itoa(int(id))
		return p
	})
	clock, _ := timestamp.GetClock(shardCtx)
	scheduler := timestamp.NewScheduler(l, clock)
	sc, err := newSegmentController(shardCtx, path, segmentSize, blockSize, openedBlockSize, maxOpenedBlockSize, l, scheduler)
	if err != nil {
		return nil, errors.Wrapf(err, "create the segment controller of the shard %d", int(id))
	}
	s := &shard{
		id:                id,
		segmentController: sc,
		l:                 l,
		scheduler:         scheduler,
	}
	err = s.segmentController.open()
	if err != nil {
		return nil, err
	}
	seriesPath, err := mkdir(seriesTemplate, path)
	if err != nil {
		return nil, err
	}
	sdb, err := newSeriesDataBase(shardCtx, s.id, seriesPath, s.segmentController)
	if err != nil {
		return nil, err
	}
	s.seriesDatabase = sdb
	idb := newIndexDatabase(shardCtx, s.id, s.segmentController)
	s.indexDatabase = idb
	s.segmentManageStrategy, err = bucket.NewStrategy(s.segmentController, bucket.WithLogger(s.l))
	if err != nil {
		return nil, err
	}
	s.segmentManageStrategy.Run()
	position := shardCtx.Value(common.PositionKey)
	if position != nil {
		s.position = position.(common.Position)
	}
	if err := scheduler.Register("stat", cron.Descriptor, "@every 5s", s.stat); err != nil {
		return nil, err
	}
	retentionTask := newRetentionTask(s.segmentController, ttl)
	if err := scheduler.Register("retention", retentionTask.option, retentionTask.expr, retentionTask.run); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *shard) ID() common.ShardID {
	return s.id
}

func (s *shard) Series() SeriesDatabase {
	return s.seriesDatabase
}

func (s *shard) Index() IndexDatabase {
	return s.indexDatabase
}

func (s *shard) State() (shardState ShardState) {
	shardState.StrategyManagers = append(shardState.StrategyManagers, s.segmentManageStrategy.String())
	for _, seg := range s.segmentController.segments() {
		if seg.blockManageStrategy != nil {
			shardState.StrategyManagers = append(shardState.StrategyManagers, seg.blockManageStrategy.String())
		}
		for _, b := range seg.blockController.blocks() {
			shardState.Blocks = append(shardState.Blocks, BlockState{
				ID: BlockID{
					SegID:   b.segID,
					BlockID: b.blockID,
				},
				TimeRange: b.TimeRange,
				Closed:    b.Closed(),
			})
		}
	}
	all := s.segmentController.blockQueue.All()
	shardState.OpenBlocks = make([]BlockID, len(all))
	for i, v := range all {
		shardState.OpenBlocks[i] = v.(BlockID)
	}
	sort.Slice(shardState.OpenBlocks, func(i, j int) bool {
		x := shardState.OpenBlocks[i]
		y := shardState.OpenBlocks[j]
		if x.SegID == y.SegID {
			return x.BlockID < y.BlockID
		}
		return x.SegID < y.SegID
	})
	s.l.Trace().Interface("", shardState).Msg("shard state")
	return shardState
}

func (s *shard) TriggerSchedule(task string) bool {
	return s.scheduler.Trigger(task)
}

func (s *shard) Close() (err error) {
	s.closeOnce.Do(func() {
		s.scheduler.Close()
		s.segmentManageStrategy.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err = multierr.Combine(s.segmentController.close(ctx), s.seriesDatabase.Close())
	})
	return err
}

// IntervalUnit denotes the unit of a time point.
type IntervalUnit int

// Available IntervalUnits. HOUR and DAY are adequate for the APM scenario.
const (
	HOUR IntervalUnit = iota
	DAY
)

func (iu IntervalUnit) String() string {
	switch iu {
	case HOUR:
		return "hour"
	case DAY:
		return "day"
	}
	panic("invalid interval unit")
}

// IntervalRule defines a length of two points in time.
type IntervalRule struct {
	Unit IntervalUnit
	Num  int
}

func (ir IntervalRule) nextTime(current time.Time) time.Time {
	switch ir.Unit {
	case HOUR:
		return current.Add(time.Hour * time.Duration(ir.Num))
	case DAY:
		return current.AddDate(0, 0, ir.Num)
	}
	panic("invalid interval unit")
}

func (ir IntervalRule) estimatedDuration() time.Duration {
	switch ir.Unit {
	case HOUR:
		return time.Hour * time.Duration(ir.Num)
	case DAY:
		return 24 * time.Hour * time.Duration(ir.Num)
	}
	panic("invalid interval unit")
}

type parser interface {
	Parse(value string) (time.Time, error)
}

func loadSections(root, prefix string, parser parser, intervalRule IntervalRule, loadFn func(start, end time.Time) error) error {
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
