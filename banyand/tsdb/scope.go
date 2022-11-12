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

package tsdb

import (
	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/observability"
)

var _ Shard = (*ScopedShard)(nil)

type ScopedShard struct {
	scope     Entry
	delegated Shard
}

func NewScopedShard(scope Entry, delegated Shard) Shard {
	return &ScopedShard{
		scope:     scope,
		delegated: delegated,
	}
}

func (sd *ScopedShard) Close() error {
	// the delegate can't close the underlying shard
	return nil
}

func (sd *ScopedShard) ID() common.ShardID {
	return sd.delegated.ID()
}

func (sd *ScopedShard) Series() SeriesDatabase {
	return &scopedSeriesDatabase{
		scope:     sd.scope,
		delegated: sd.delegated.Series(),
	}
}

func (sd *ScopedShard) Index() IndexDatabase {
	return sd.delegated.Index()
}

func (sd *ScopedShard) TriggerSchedule(task string) bool {
	return sd.delegated.TriggerSchedule(task)
}

func (sd *ScopedShard) State() ShardState {
	return sd.delegated.State()
}

var _ SeriesDatabase = (*scopedSeriesDatabase)(nil)

type scopedSeriesDatabase struct {
	scope     Entry
	delegated SeriesDatabase
}

func (sdd *scopedSeriesDatabase) Stats() observability.Statistics {
	return sdd.delegated.Stats()
}

func (sdd *scopedSeriesDatabase) Close() error {
	return nil
}

func (sdd *scopedSeriesDatabase) GetByHashKey(key []byte) (Series, error) {
	return sdd.delegated.GetByHashKey(key)
}

func (sdd *scopedSeriesDatabase) GetByID(id common.SeriesID) (Series, error) {
	return sdd.delegated.GetByID(id)
}

func (sdd *scopedSeriesDatabase) Get(entity Entity) (Series, error) {
	return sdd.delegated.Get(entity.Prepend(sdd.scope))
}

func (sdd *scopedSeriesDatabase) List(path Path) (SeriesList, error) {
	return sdd.delegated.List(path.Prepend(sdd.scope))
}
