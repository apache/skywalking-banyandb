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
	"fmt"
	"io"
	"io/fs"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/pkg/errors"
	"go.uber.org/multierr"

	"github.com/apache/skywalking-banyandb/api/common"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/logger"
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

	segDayFormat         = "20060102"
	segMonthFormat       = "200601"
	segYearFormat        = "2006"
	segMillisecondFormat = "20060102150405000"
	blockFormat          = "1504"

	dirPerm = 0700
)

var (
	ErrInvalidShardID       = errors.New("invalid shard id")
	ErrEncodingMethodAbsent = errors.New("encoding method is absent")

	indexRulesKey     = contextIndexRulesKey{}
	encodingMethodKey = contextEncodingMethodKey{}
)

type contextIndexRulesKey struct{}
type contextEncodingMethodKey struct{}

type Database interface {
	io.Closer
	Shards() []Shard
	Shard(id common.ShardID) (Shard, error)
}

type Shard interface {
	io.Closer
	ID() common.ShardID
	Series() SeriesDatabase
	Index() IndexDatabase
}

var _ Database = (*database)(nil)

type DatabaseOpts struct {
	Location       string
	ShardNum       uint32
	IndexRules     []*databasev1.IndexRule
	EncodingMethod EncodingMethod
}

type EncodingMethod struct {
	EncoderPool encoding.SeriesEncoderPool
	DecoderPool encoding.SeriesDecoderPool
}

type database struct {
	logger   *logger.Logger
	location string
	shardNum uint32

	sLst []Shard
	sync.Mutex
}

func (d *database) Shards() []Shard {
	return d.sLst
}

func (d *database) Shard(id common.ShardID) (Shard, error) {
	if uint(id) >= uint(len(d.sLst)) {
		return nil, ErrInvalidShardID
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

func OpenDatabase(ctx context.Context, opts DatabaseOpts) (Database, error) {
	if opts.EncodingMethod.EncoderPool == nil || opts.EncodingMethod.DecoderPool == nil {
		return nil, errors.Wrap(ErrEncodingMethodAbsent, "failed to open database")
	}
	if _, err := mkdir(opts.Location); err != nil {
		return nil, err
	}
	db := &database{
		location: opts.Location,
		shardNum: opts.ShardNum,
		logger:   logger.Fetch(ctx, "tsdb"),
	}
	db.logger.Info().Str("path", opts.Location).Msg("initialized")
	var entries []fs.FileInfo
	var err error
	if entries, err = ioutil.ReadDir(opts.Location); err != nil {
		return nil, errors.Wrap(err, "failed to read directory contents failed")
	}
	thisContext := context.WithValue(ctx, logger.ContextKey, db.logger)
	thisContext = context.WithValue(thisContext, indexRulesKey, opts.IndexRules)
	thisContext = context.WithValue(thisContext, encodingMethodKey, opts.EncodingMethod)
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
		so, errNewShard := OpenShard(ctx, common.ShardID(i), db.location, IntervalRule{
			Unit: DAY,
			Num:  1,
		})
		if errNewShard != nil {
			err = multierr.Append(err, errNewShard)
			continue
		}
		db.sLst = append(db.sLst, so)
	}
	return db, err
}

func loadDatabase(ctx context.Context, db *database) (Database, error) {
	//TODO: open the lock file
	//TODO: open the manifest file
	db.Lock()
	defer db.Unlock()
	err := walkDir(db.location, shardPathPrefix, func(suffix, _ string) error {
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
			IntervalRule{
				Unit: DAY,
				Num:  1,
			})
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

type walkFn func(suffix, absolutePath string) error

func walkDir(root, prefix string, walkFn walkFn) error {
	files, err := ioutil.ReadDir(root)
	if err != nil {
		return errors.Wrapf(err, "failed to walk the database path: %s", root)
	}
	for _, f := range files {
		if !f.IsDir() || !strings.HasPrefix(f.Name(), prefix) {
			continue
		}
		segs := strings.Split(f.Name(), "-")
		errWalk := walkFn(segs[len(segs)-1], fmt.Sprintf(rootPrefix, root)+f.Name())
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
