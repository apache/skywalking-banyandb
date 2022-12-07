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
package tsdb

import (
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/pkg/errors"
	"go.uber.org/multierr"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

const (
	shardPathPrefix     = "shard"
	pathSeparator       = string(os.PathSeparator)
	rootPrefix          = "%s" + pathSeparator
	shardTemplate       = rootPrefix + shardPathPrefix + "-%d"
	seriesTemplate      = rootPrefix + "series"
	segPathPrefix       = "seg"
	segTemplate         = rootPrefix + segPathPrefix + "-%s"
	blockPathPrefix     = "block"
	blockTemplate       = rootPrefix + blockPathPrefix + "-%s"
	globalIndexTemplate = rootPrefix + "index"

	hourFormat = "2006010215"
	dayFormat  = "20060102"

	dirPerm = 0o700
)

var (
	errInvalidShardID = errors.New("invalid shard id")
	errOpenDatabase   = errors.New("fails to open the database")

	optionsKey = contextOptionsKey{}
)

type contextOptionsKey struct{}

// Supplier allows getting a tsdb's runtime.
type Supplier interface {
	SupplyTSDB() Database
}

// Database allows listing and getting shard details.
type Database interface {
	io.Closer
	Shards() []Shard
	Shard(id common.ShardID) (Shard, error)
}

// Shard allows accessing data of tsdb.
type Shard interface {
	io.Closer
	ID() common.ShardID
	Series() SeriesDatabase
	Index() IndexDatabase
	State() ShardState
	// Only works with MockClock
	TriggerSchedule(task string) bool
}

var _ Database = (*database)(nil)

// DatabaseOpts wraps options to create a tsdb.
type DatabaseOpts struct {
	EncodingMethod     EncodingMethod
	Location           string
	SegmentInterval    IntervalRule
	BlockInterval      IntervalRule
	TTL                IntervalRule
	BlockMemSize       int64
	SeriesMemSize      int64
	GlobalIndexMemSize int64
	ShardNum           uint32
	EnableGlobalIndex  bool
}

// EncodingMethod wraps encoder/decoder pools to flush/compact data on disk.
type EncodingMethod struct {
	EncoderPool encoding.SeriesEncoderPool
	DecoderPool encoding.SeriesDecoderPool
}

type (
	// SectionID is the kind of a block/segment.
	SectionID uint32

	// BlockID is the identity of a block in a shard.
	BlockID struct {
		SegID   SectionID
		BlockID SectionID
	}
)

func (b BlockID) String() string {
	return fmt.Sprintf("BlockID-%d-%d", parseSuffix(b.SegID), parseSuffix(b.BlockID))
}

// GenerateInternalID returns a identity of a section(segment or block) based on IntervalRule.
func GenerateInternalID(unit IntervalUnit, suffix int) SectionID {
	return SectionID(unit)<<31 | ((SectionID(suffix) << 1) >> 1)
}

func parseSuffix(id SectionID) int {
	return int((id << 1) >> 1)
}

func sectionIDToBytes(id SectionID) []byte {
	return convert.Uint32ToBytes(uint32(id))
}

func readSectionID(data []byte, offset int) (SectionID, int) {
	end := offset + 4
	return SectionID(convert.BytesToUint32(data[offset:end])), end
}

// BlockState is a sample of a block's runtime state.
type BlockState struct {
	TimeRange timestamp.TimeRange
	ID        BlockID
	Closed    bool
}

// ShardState is a sample of a shard's runtime state.
type ShardState struct {
	Blocks           []BlockState
	OpenBlocks       []BlockID
	StrategyManagers []string
}

type database struct {
	logger      *logger.Logger
	location    string
	sLst        []Shard
	segmentSize IntervalRule
	blockSize   IntervalRule
	ttl         IntervalRule
	sync.Mutex
	shardNum uint32
}

func (d *database) Shards() []Shard {
	return d.sLst
}

func (d *database) Shard(id common.ShardID) (Shard, error) {
	if uint(id) >= uint(len(d.sLst)) {
		return nil, errInvalidShardID
	}
	return d.sLst[id], nil
}

func (d *database) Close() error {
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
	if opts.EncodingMethod.EncoderPool == nil || opts.EncodingMethod.DecoderPool == nil {
		return nil, errors.Wrap(errOpenDatabase, "encoding method is absent")
	}
	if _, err := mkdir(opts.Location); err != nil {
		return nil, err
	}
	if opts.SegmentInterval.Num == 0 {
		return nil, errors.Wrap(errOpenDatabase, "segment interval is absent")
	}
	if opts.BlockInterval.Num == 0 {
		return nil, errors.Wrap(errOpenDatabase, "block interval is absent")
	}
	if opts.BlockInterval.estimatedDuration() > opts.SegmentInterval.estimatedDuration() {
		return nil, errors.Wrapf(errOpenDatabase, "the block size is bigger than the segment size")
	}
	if opts.TTL.Num == 0 {
		return nil, errors.Wrap(errOpenDatabase, "ttl is absent")
	}
	db := &database{
		location:    opts.Location,
		shardNum:    opts.ShardNum,
		logger:      logger.Fetch(ctx, "tsdb"),
		segmentSize: opts.SegmentInterval,
		blockSize:   opts.BlockInterval,
		ttl:         opts.TTL,
	}
	db.logger.Info().Str("path", opts.Location).Msg("initialized")
	var entries []os.DirEntry
	var err error
	if entries, err = os.ReadDir(opts.Location); err != nil {
		return nil, errors.Wrap(err, "failed to read directory contents failed")
	}
	thisContext := context.WithValue(ctx, logger.ContextKey, db.logger)
	thisContext = context.WithValue(thisContext, optionsKey, opts)
	if len(entries) > 0 {
		return loadDatabase(thisContext, db)
	}
	return initDatabase(thisContext, db)
}

func initDatabase(ctx context.Context, db *database) (Database, error) {
	db.Lock()
	defer db.Unlock()
	return createDatabase(ctx, db, 0)
}

func createDatabase(ctx context.Context, db *database, startID int) (Database, error) {
	var err error
	for i := startID; i < int(db.shardNum); i++ {
		db.logger.Info().Int("shard_id", i).Msg("creating a shard")
		so, errNewShard := OpenShard(ctx, common.ShardID(i),
			db.location, db.segmentSize, db.blockSize, db.ttl, defaultBlockQueueSize, defaultMaxBlockQueueSize)
		if errNewShard != nil {
			err = multierr.Append(err, errNewShard)
			continue
		}
		db.sLst = append(db.sLst, so)
	}
	return db, err
}

func loadDatabase(ctx context.Context, db *database) (Database, error) {
	// TODO: open the lock file
	// TODO: open the manifest file
	db.Lock()
	defer db.Unlock()
	err := walkDir(db.location, shardPathPrefix, func(suffix string) error {
		shardID, err := strconv.Atoi(suffix)
		if err != nil {
			return err
		}
		if shardID >= int(db.shardNum) {
			return nil
		}
		db.logger.Info().Int("shard_id", shardID).Msg("opening a existing shard")
		so, errOpenShard := OpenShard(
			context.WithValue(ctx, logger.ContextKey, db.logger),
			common.ShardID(shardID),
			db.location,
			db.segmentSize,
			db.blockSize,
			db.ttl,
			defaultBlockQueueSize,
			defaultMaxBlockQueueSize,
		)
		if errOpenShard != nil {
			return errOpenShard
		}
		db.sLst = append(db.sLst, so)
		return nil
	})
	if err != nil {
		return nil, errors.WithMessage(err, "load the database failed")
	}

	loadedShardsNum := len(db.sLst)
	if loadedShardsNum < int(db.shardNum) {
		_, err := createDatabase(ctx, db, loadedShardsNum)
		if err != nil {
			return nil, errors.WithMessage(err, "load the database failed")
		}
	}
	return db, nil
}

type walkFn func(suffix string) error

func walkDir(root, prefix string, wf walkFn) error {
	files, err := os.ReadDir(root)
	if err != nil {
		return errors.Wrapf(err, "failed to walk the database path: %s", root)
	}
	for _, f := range files {
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

func mkdir(format string, a ...interface{}) (path string, err error) {
	path = fmt.Sprintf(format, a...)
	if err = os.MkdirAll(path, dirPerm); err != nil {
		return "", errors.Wrapf(err, "failed to create %s", path)
	}
	return path, err
}
