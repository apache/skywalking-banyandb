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
)

var _ Shard = (*shard)(nil)

type shard struct {
	id int

	location       string
	seriesDatabase SeriesDatabase
}

func (s *shard) Series() SeriesDatabase {
	panic("implement me")
}

func (s *shard) Index() IndexDatabase {
	panic("implement me")
}

func newShard(ctx context.Context, id int, location string) (*shard, error) {
	s := &shard{
		id:       id,
		location: location,
	}
	seriesPath, err := mkdir(seriesTemplate, s.location)
	if err != nil {
		return nil, err
	}
	sdb, err := newSeriesDataBase(ctx, seriesPath)
	if err != nil {
		return nil, err
	}
	s.seriesDatabase = sdb

	return s, nil
}

func (s *shard) Close() error {
	return s.seriesDatabase.Close()
}
