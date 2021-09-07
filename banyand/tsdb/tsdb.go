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
	"sync"

	"github.com/pkg/errors"
	"go.uber.org/multierr"

	"github.com/apache/skywalking-banyandb/api/common"
	databasev2 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v2"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

const (
	shardTemplate       = "%s/shard-%d"
	seriesTemplate      = "%s/seriesSpan"
	segTemplate         = "%s/seg-%s"
	blockTemplate       = "%s/block-%s"
	globalIndexTemplate = "%s/index"

	segFormat   = "20060102"
	blockFormat = "1504"

	dirPerm = 0700
)

var ErrInvalidShardID = errors.New("invalid shard id")
var indexRulesKey = contextIndexRulesKey{}

type contextIndexRulesKey struct{}

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
	Location   string
	ShardNum   uint32
	IndexRules []*databasev2.IndexRule
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
	for _, s := range d.sLst {
		_ = s.Close()
	}
	return nil
}

func OpenDatabase(ctx context.Context, opts DatabaseOpts) (Database, error) {
	db := &database{
		location: opts.Location,
		shardNum: opts.ShardNum,
	}
	parentLogger := ctx.Value(logger.ContextKey)
	if parentLogger != nil {
		if pl, ok := parentLogger.(*logger.Logger); ok {
			db.logger = pl.Named("tsdb")
		}
	}
	if _, err := mkdir(opts.Location); err != nil {
		return nil, err
	}
	db.logger.Info().Str("path", opts.Location).Msg("initialized")
	var entries []fs.FileInfo
	var err error
	if entries, err = ioutil.ReadDir(opts.Location); err != nil {
		return nil, errors.Wrap(err, "failed to read directory contents failed")
	}
	thisContext := context.WithValue(ctx, logger.ContextKey, db.logger)
	thisContext = context.WithValue(thisContext, indexRulesKey, opts.IndexRules)
	if len(entries) > 0 {
		return loadDatabase(thisContext, db)
	}
	return createDatabase(thisContext, db)
}

func createDatabase(ctx context.Context, db *database) (Database, error) {
	var err error
	db.Lock()
	defer db.Unlock()
	for i := uint32(0); i < db.shardNum; i++ {
		shardLocation, errInternal := mkdir(shardTemplate, db.location, i)
		if errInternal != nil {
			err = multierr.Append(err, errInternal)
			continue
		}
		so, errNewShard := newShard(ctx, common.ShardID(i), shardLocation)
		if errNewShard != nil {
			err = multierr.Append(err, errNewShard)
			continue
		}
		db.sLst = append(db.sLst, so)
	}
	return db, err
}

func loadDatabase(ctx context.Context, db *database) (Database, error) {
	//TODO: load the existing database
	return db, nil
}

func mkdir(format string, a ...interface{}) (path string, err error) {
	path = fmt.Sprintf(format, a...)
	if err = os.MkdirAll(path, dirPerm); err != nil {
		return "", errors.Wrapf(err, "failed to create %s", path)
	}
	return path, err
}
