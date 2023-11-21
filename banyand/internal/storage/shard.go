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
	"path"
	"strconv"
	"sync"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/tsdb/bucket"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

type shard[T TSTable[T]] struct {
	l                     *logger.Logger
	segmentController     *segmentController[T]
	segmentManageStrategy *bucket.Strategy
	scheduler             *timestamp.Scheduler
	position              common.Position
	closeOnce             sync.Once
	id                    common.ShardID
}

func (d *database[T]) openShard(ctx context.Context, id common.ShardID) (*shard[T], error) {
	location := path.Join(d.location, fmt.Sprintf(shardTemplate, int(id)))
	lfs.MkdirIfNotExist(location, dirPerm)
	l := logger.Fetch(ctx, "shard"+strconv.Itoa(int(id)))
	l.Info().Int("shard_id", int(id)).Str("path", location).Msg("creating a shard")
	shardCtx := context.WithValue(ctx, logger.ContextKey, l)
	shardCtx = common.SetPosition(shardCtx, func(p common.Position) common.Position {
		p.Shard = strconv.Itoa(int(id))
		return p
	})
	clock, _ := timestamp.GetClock(shardCtx)

	scheduler := timestamp.NewScheduler(l, clock)
	s := &shard[T]{
		id:                id,
		l:                 l,
		scheduler:         scheduler,
		position:          common.GetPosition(shardCtx),
		segmentController: newSegmentController[T](shardCtx, location, d.opts.SegmentInterval, l, scheduler, d.opts.TSTableCreator),
	}
	var err error
	if err = s.segmentController.open(); err != nil {
		return nil, err
	}
	if s.segmentManageStrategy, err = bucket.NewStrategy(s.segmentController, bucket.WithLogger(s.l)); err != nil {
		return nil, err
	}
	s.segmentManageStrategy.Run()
	retentionTask := newRetentionTask(s.segmentController, d.opts.TTL)
	if err := scheduler.Register("retention", retentionTask.option, retentionTask.expr, retentionTask.run); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *shard[T]) closer() {
	s.closeOnce.Do(func() {
		s.scheduler.Close()
		s.segmentManageStrategy.Close()
		s.segmentController.close()
	})
}
