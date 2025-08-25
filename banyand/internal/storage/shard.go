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

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

var _ Cache = (*shardCache)(nil)

type shardCache struct {
	*segmentCache
	shardID common.ShardID
}

// NewShardCache creates a new shard cache.
func NewShardCache(group string, segmentID segmentID, shardID common.ShardID) Cache {
	serviceCache := NewServiceCache()
	groupCache := &groupCache{
		cache: serviceCache,
		group: group,
	}
	segmentCache := &segmentCache{
		groupCache: groupCache,
		segmentID:  segmentID,
	}
	return &shardCache{
		segmentCache: segmentCache,
		shardID:      shardID,
	}
}

func (sc *shardCache) Get(key EntryKey) Sizable {
	key.shardID = sc.shardID
	return sc.segmentCache.get(key)
}

func (sc *shardCache) Put(key EntryKey, value Sizable) {
	key.shardID = sc.shardID
	sc.segmentCache.put(key, value)
}

type shard[T TSTable] struct {
	table T
	*shardCache
	l         *logger.Logger
	timeRange timestamp.TimeRange
	location  string
	id        common.ShardID
}

func (s *segment[T, O]) openShard(ctx context.Context, id common.ShardID) (*shard[T], error) {
	location := path.Join(s.location, fmt.Sprintf(shardTemplate, int(id)))
	s.lfs.MkdirIfNotExist(location, DirPerm)
	l := logger.Fetch(ctx, "shard"+strconv.Itoa(int(id)))
	l.Info().Int("shard_id", int(id)).Str("path", location).Msg("loading a shard")
	p := common.GetPosition(ctx)
	p.Shard = strconv.Itoa(int(id))
	t, err := s.tsdbOpts.TSTableCreator(s.lfs, location, p, l, s.TimeRange, s.tsdbOpts.Option, s.metrics)
	if err != nil {
		return nil, err
	}

	return &shard[T]{
		id:    id,
		l:     l,
		table: t,
		shardCache: &shardCache{
			segmentCache: s.segmentCache,
			shardID:      id,
		},
		timeRange: s.TimeRange,
		location:  location,
	}, nil
}

func (s *shard[T]) Table() T {
	return s.table
}

func (s *shard[T]) close() error {
	return s.table.Close()
}
