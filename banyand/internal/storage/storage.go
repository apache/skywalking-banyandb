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

// Package tsdb implements a time-series-based storage engine.
// It provides:
//   - Partition data based on a time axis.
//   - Sharding data based on a series id which represents a unique entity of stream/measure
//   - Retrieving data based on index.Filter.
//   - Cleaning expired data, or the data retention.
package storage

import (
	"io"
	"time"

	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/api/common"
)

const (
	shardPathPrefix = "shard"
	shardTemplate   = shardPathPrefix + "-%d"
	metadataPath    = "metadata"
	segTemplate     = "seg-%s"

	hourFormat = "2006010215"
	dayFormat  = "20060102"

	dirPerm = 0o700
)

var (
	// ErrUnknownShard indicates that the shard is not found.
	ErrUnknownShard = errors.New("unknown shard")
	errOpenDatabase = errors.New("fails to open the database")
)

// Supplier allows getting a tsdb's runtime.
type Supplier interface {
	SupplyTSDB() Database
}

// Database allows listing and getting shard details.
type Database interface {
	io.Closer
	CreateShardsAndGetByID(id common.ShardID) (Shard, error)
	Shards() []Shard
	Shard(id common.ShardID) (Shard, error)
}

// Shard allows accessing data of tsdb.
type Shard interface {
	io.Closer
	ID() common.ShardID
	// Series() SeriesDatabase
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
