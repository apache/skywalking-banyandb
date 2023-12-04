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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

// IndexGranularity denotes the granularity of the local index.
type IndexGranularity int

// The options of the local index granularity.
const (
	IndexGranularityBlock IndexGranularity = iota
	IndexGranularitySeries
)

// TSDBOpts wraps options to create a tsdb.
type TSDBOpts[T TSTable] struct {
	TSTableCreator  TSTableCreator[T]
	Location        string
	SegmentInterval IntervalRule
	TTL             IntervalRule
	ShardNum        uint32
}

type (
	// SegID is the kind of a segment.
	SegID uint32
)

func GenerateSegID(unit IntervalUnit, suffix int) SegID {
	return SegID(unit)<<31 | ((SegID(suffix) << 1) >> 1)
}

type database[T TSTable] struct {
	logger   *logger.Logger
	index    *seriesIndex
	location string
	sLst     []*shard[T]
	opts     TSDBOpts[T]
	sync.RWMutex
	sLen uint32
}

func (d *database[T]) Close() error {
	d.Lock()
	defer d.Unlock()
	for _, s := range d.sLst {
		s.closer()
	}
	return nil
}

// OpenTSDB returns a new tsdb runtime. This constructor will create a new database if it's absent,
// or load an existing one.
func OpenTSDB[T TSTable](ctx context.Context, opts TSDBOpts[T]) (TSDB[T], error) {
	if opts.SegmentInterval.Num == 0 {
		return nil, errors.Wrap(errOpenDatabase, "segment interval is absent")
	}
	if opts.TTL.Num == 0 {
		return nil, errors.Wrap(errOpenDatabase, "ttl is absent")
	}
	p := common.GetPosition(ctx)
	location := filepath.Clean(opts.Location)
	lfs.MkdirIfNotExist(location, dirPerm)
	si, err := newSeriesIndex(ctx, location)
	if err != nil {
		return nil, errors.Wrap(errOpenDatabase, errors.WithMessage(err, "create series index failed").Error())
	}
	db := &database[T]{
		location: location,
		logger:   logger.Fetch(ctx, p.Database),
		index:    si,
		opts:     opts,
	}
	db.logger.Info().Str("path", opts.Location).Msg("initialized")
	if err = db.loadDatabase(); err != nil {
		return nil, errors.Wrap(errOpenDatabase, errors.WithMessage(err, "load database failed").Error())
	}
	return db, nil
}

func (d *database[T]) Register(shardID common.ShardID, series *Series) (*Series, error) {
	var err error
	if series, err = d.index.createPrimary(series); err != nil {
		return nil, err
	}
	id := int(shardID)
	if id < int(atomic.LoadUint32(&d.sLen)) {
		return series, nil
	}
	d.Lock()
	defer d.Unlock()
	if id < len(d.sLst) {
		return series, nil
	}
	d.logger.Info().Int("shard_id", id).Msg("creating a shard")
	if err = d.registerShard(id); err != nil {
		return nil, err
	}
	return series, nil
}

func (d *database[T]) CreateTSTableIfNotExist(shardID common.ShardID, ts time.Time) (TSTableWrapper[T], error) {
	timeRange := timestamp.NewInclusiveTimeRange(ts, ts)
	ss := d.sLst[shardID].segmentController.selectTSTables(timeRange)
	if len(ss) > 0 {
		return ss[0], nil
	}
	return d.sLst[shardID].segmentController.createTSTable(timeRange.Start)
}

func (d *database[T]) SelectTSTables(shardID common.ShardID, timeRange timestamp.TimeRange) ([]TSTableWrapper[T], error) {
	if int(shardID) >= int(atomic.LoadUint32(&d.sLen)) {
		return nil, ErrUnknownShard
	}
	return d.sLst[shardID].segmentController.selectTSTables(timeRange), nil
}

func (d *database[T]) registerShard(id int) error {
	ctx := context.WithValue(context.Background(), logger.ContextKey, d.logger)
	so, err := d.openShard(ctx, common.ShardID(id))
	if err != nil {
		return err
	}
	d.sLst = append(d.sLst, so)
	d.sLen++
	return nil
}

func (d *database[T]) loadDatabase() error {
	d.Lock()
	defer d.Unlock()
	return walkDir(d.location, shardPathPrefix, func(suffix string) error {
		shardID, err := strconv.Atoi(suffix)
		if err != nil {
			return err
		}
		if shardID >= int(d.opts.ShardNum) {
			return nil
		}
		d.logger.Info().Int("shard_id", shardID).Msg("opening a existed shard")
		return d.registerShard(shardID)
	})
}

type walkFn func(suffix string) error

func walkDir(root, prefix string, wf walkFn) error {
	for _, f := range lfs.ReadDir(root) {
		if !f.IsDir() || !strings.HasPrefix(f.Name(), prefix) {
			continue
		}
		segs := strings.Split(f.Name(), "-")
		errWalk := wf(segs[len(segs)-1])
		if errWalk != nil {
			return errors.WithMessagef(errWalk, "failed to load: %s", f.Name())
		}
	}
	return nil
}
