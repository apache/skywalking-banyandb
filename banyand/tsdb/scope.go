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
	"context"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/index"
)

var _ Shard = (*scopedShard)(nil)

type scopedShard struct {
	delegated Shard
	scope     Entry
}

// NewScopedShard returns a shard in a scope.
func NewScopedShard(scope Entry, delegated Shard) Shard {
	return &scopedShard{
		scope:     scope,
		delegated: delegated,
	}
}

func (sd *scopedShard) Close() error {
	// the delegate can't close the underlying shard
	return nil
}

func (sd *scopedShard) ID() common.ShardID {
	return sd.delegated.ID()
}

func (sd *scopedShard) Series() SeriesDatabase {
	return &scopedSeriesDatabase{
		scope:     sd.scope,
		delegated: sd.delegated.Series(),
	}
}

func (sd *scopedShard) Index() IndexDatabase {
	return sd.delegated.Index()
}

func (sd *scopedShard) TriggerSchedule(task string) bool {
	return sd.delegated.TriggerSchedule(task)
}

func (sd *scopedShard) State() ShardState {
	return sd.delegated.State()
}

var _ SeriesDatabase = (*scopedSeriesDatabase)(nil)

type scopedSeriesDatabase struct {
	delegated SeriesDatabase
	scope     Entry
}

func (sdd *scopedSeriesDatabase) writeInvertedIndex(fields []index.Field, seriesID common.SeriesID) error {
	return sdd.delegated.writeInvertedIndex(fields, seriesID)
}

func (sdd *scopedSeriesDatabase) writeLSMIndex(fields []index.Field, seriesID common.SeriesID) error {
	return sdd.delegated.writeLSMIndex(fields, seriesID)
}

func (sdd *scopedSeriesDatabase) Search(ctx context.Context, path Path, filter index.Filter, order *OrderBy) (SeriesList, error) {
	return sdd.delegated.Search(ctx, path.prepend(sdd.scope), filter, order)
}

func (sdd *scopedSeriesDatabase) Close() error {
	return nil
}

func (sdd *scopedSeriesDatabase) Get(key []byte, entityValues EntityValues) (Series, error) {
	return sdd.delegated.Get(key, entityValues)
}

func (sdd *scopedSeriesDatabase) GetByID(id common.SeriesID) (Series, error) {
	return sdd.delegated.GetByID(id)
}

func (sdd *scopedSeriesDatabase) List(ctx context.Context, path Path) (SeriesList, error) {
	return sdd.delegated.List(ctx, path.prepend(sdd.scope))
}

func (sdd *scopedSeriesDatabase) SizeOnDisk() int64 {
	return sdd.delegated.SizeOnDisk()
}
