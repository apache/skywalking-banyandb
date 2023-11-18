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
	"context"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
	"go.uber.org/multierr"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// IndexGranularity denotes the granularity of the local index.
type IndexGranularity int

// The options of the local index granularity.
const (
	IndexGranularityBlock IndexGranularity = iota
	IndexGranularitySeries
)

var _ Database = (*database)(nil)

// DatabaseOpts wraps options to create a tsdb.
type DatabaseOpts struct {
	Location         string
	SegmentInterval  IntervalRule
	TTL              IntervalRule
	IndexGranularity IndexGranularity
	ShardNum         uint32
}

type (
	// SegID is the kind of a segment.
	SegID uint32
)

func GenerateSegID(unit IntervalUnit, suffix int) SegID {
	return SegID(unit)<<31 | ((SegID(suffix) << 1) >> 1)
}

func parseSuffix(id SegID) int {
	return int((id << 1) >> 1)
}

func segIDToBytes(id SegID) []byte {
	return convert.Uint32ToBytes(uint32(id))
}

func readSegID(data []byte, offset int) (SegID, int) {
	end := offset + 4
	return SegID(convert.BytesToUint32(data[offset:end])), end
}

type database struct {
	fileSystem  fs.FileSystem
	logger      *logger.Logger
	index       *seriesIndex
	location    string
	sLst        []Shard
	segmentSize IntervalRule
	ttl         IntervalRule
	sync.RWMutex
	shardNum           uint32
	shardCreationState uint32
}

func (d *database) CreateShardsAndGetByID(id common.ShardID) (Shard, error) {
	if atomic.LoadUint32(&d.shardCreationState) != 0 {
		return d.shard(id)
	}
	d.Lock()
	defer d.Unlock()
	if atomic.LoadUint32(&d.shardCreationState) != 0 {
		return d.shard(id)
	}
	loadedShardsNum := len(d.sLst)
	if loadedShardsNum < int(d.shardNum) {
		_, err := createDatabase(d, loadedShardsNum)
		if err != nil {
			return nil, errors.WithMessage(err, "create the database failed")
		}
	}
	atomic.StoreUint32(&d.shardCreationState, 1)
	return d.shard(id)
}

func (d *database) Shards() []Shard {
	d.RLock()
	defer d.RUnlock()
	return d.sLst
}

func (d *database) Shard(id common.ShardID) (Shard, error) {
	d.RLock()
	defer d.RUnlock()
	return d.shard(id)
}

func (d *database) shard(id common.ShardID) (Shard, error) {
	if uint(id) >= uint(len(d.sLst)) {
		return nil, ErrUnknownShard
	}
	return d.sLst[id], nil
}

func (d *database) Close() error {
	d.Lock()
	defer d.Unlock()
	var err error
	for _, s := range d.sLst {
		innerErr := s.Close()
		if innerErr != nil {
			err = multierr.Append(err, innerErr)
		}
	}
	return err
}

// OpenDatabase returns a new tsdb runtime. This constructor will create a new database if it's absent,
// or load an existing one.
func OpenDatabase(ctx context.Context, opts DatabaseOpts) (Database, error) {
	if opts.SegmentInterval.Num == 0 {
		return nil, errors.Wrap(errOpenDatabase, "segment interval is absent")
	}
	if opts.TTL.Num == 0 {
		return nil, errors.Wrap(errOpenDatabase, "ttl is absent")
	}
	p := common.GetPosition(ctx)
	l := logger.Fetch(ctx, p.Database)
	fileSystem := fs.NewLocalFileSystemWithLogger(l)
	path := filepath.Clean(opts.Location)
	fileSystem.Mkdir(path, dirPerm)
	si, err := newSeriesIndex(ctx, path)
	if err != nil {
		return nil, errors.Wrap(errOpenDatabase, errors.WithMessage(err, "create series index failed").Error())
	}
	db := &database{
		location:    path,
		shardNum:    opts.ShardNum,
		logger:      logger.Fetch(ctx, p.Database),
		segmentSize: opts.SegmentInterval,
		ttl:         opts.TTL,
		fileSystem:  fileSystem,
		index:       si,
	}
	db.logger.Info().Str("path", opts.Location).Msg("initialized")
	return db, nil
}

func createDatabase(db *database, startID int) (Database, error) {
	var err error
	for i := startID; i < int(db.shardNum); i++ {
		db.logger.Info().Int("shard_id", i).Msg("creating a shard")
		// so, errNewShard := OpenShard(common.ShardID(i),
		// 	db.location, db.segmentSize, db.blockSize, db.ttl, defaultBlockQueueSize, defaultMaxBlockQueueSize, db.enableWAL)
		// if errNewShard != nil {
		// 	err = multierr.Append(err, errNewShard)
		// 	continue
		// }
		// db.sLst = append(db.sLst, so)
	}
	return db, err
}
