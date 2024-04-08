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
	"github.com/apache/skywalking-banyandb/banyand/internal/bucket"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

type shard[T TSTable, O any] struct {
	l                     *logger.Logger
	segmentController     *segmentController[T, O]
	segmentManageStrategy *bucket.Strategy
	position              common.Position
	closeOnce             sync.Once
	id                    common.ShardID
}

func (d *database[T, O]) openShard(ctx context.Context, id common.ShardID) (*shard[T, O], error) {
	location := path.Join(d.location, fmt.Sprintf(shardTemplate, int(id)))
	lfs.MkdirIfNotExist(location, dirPerm)
	l := logger.Fetch(ctx, "shard"+strconv.Itoa(int(id)))
	l.Info().Int("shard_id", int(id)).Str("path", location).Msg("creating a shard")
	shardCtx := context.WithValue(ctx, logger.ContextKey, l)
	shardCtx = common.SetPosition(shardCtx, func(p common.Position) common.Position {
		p.Shard = strconv.Itoa(int(id))
		return p
	})

	s := &shard[T, O]{
		id:       id,
		l:        l,
		position: common.GetPosition(shardCtx),
		segmentController: newSegmentController[T](shardCtx, location,
			d.opts.SegmentInterval, l, d.scheduler, d.opts.TSTableCreator, d.opts.Option),
	}
	var err error
	if err = s.segmentController.open(); err != nil {
		return nil, err
	}
	if s.segmentManageStrategy, err = bucket.NewStrategy(s.segmentController, bucket.WithLogger(s.l)); err != nil {
		return nil, err
	}
	s.segmentManageStrategy.Run()
	return s, nil
}

func (s *shard[T, O]) close() {
	s.closeOnce.Do(func() {
		s.segmentManageStrategy.Close()
		s.segmentController.close()
	})
}
