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
	"sync"
	"time"

	"github.com/apache/skywalking-banyandb/api/common"
)

var _ Shard = (*shard)(nil)

type shard struct {
	sync.Mutex
	id common.ShardID

	location       string
	seriesDatabase SeriesDatabase
	indexDatabase  IndexDatabase
	lst            []*segment
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

func newShard(ctx context.Context, id common.ShardID, location string) (*shard, error) {
	s := &shard{
		id:       id,
		location: location,
	}
	segPath, err := mkdir(segTemplate, location, time.Now().Format(segFormat))
	if err != nil {
		return nil, err
	}
	seg, err := newSegment(ctx, segPath)
	if err != nil {
		return nil, err
	}
	{
		s.Lock()
		defer s.Unlock()
		s.lst = append(s.lst, seg)
	}
	seriesPath, err := mkdir(seriesTemplate, s.location)
	if err != nil {
		return nil, err
	}
	sdb, err := newSeriesDataBase(ctx, s.id, seriesPath, s.lst)
	if err != nil {
		return nil, err
	}
	s.seriesDatabase = sdb
	idb, err := newIndexDatabase(ctx, s.id, s.lst)
	if err != nil {
		return nil, err
	}
	s.indexDatabase = idb
	return s, nil
}

func (s *shard) Close() error {
	return s.seriesDatabase.Close()
}
