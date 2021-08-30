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

	"github.com/apache/skywalking-banyandb/pkg/logger"
)

const (
	shardTemplate  = "%s/shard-%d"
	seriesTemplate = "%s/series"
	segTemplate    = "%s/seg-%s"
	blockTemplate  = "%s/block-%s"

	segFormat   = "20060102"
	blockFormat = "1504"

	dirPerm = 0700
)

type Database interface {
	io.Closer
	Shards() []Shard
}

type Shard interface {
	io.Closer
	Series() SeriesDatabase
	Index() IndexDatabase
}

var _ Database = (*database)(nil)

type DatabaseOpts struct {
	Location string
	ShardNum uint
}

type database struct {
	logger   *logger.Logger
	location string
	shardNum uint

	sLst []Shard
	sync.Mutex
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
	if len(entries) > 0 {
		return loadDatabase(thisContext, db)
	}
	return createDatabase(thisContext, db)
}

func (d *database) Close() error {
	for _, s := range d.sLst {
		_ = s.Close()
	}
	return nil
}

func (d *database) Shards() []Shard {
	return d.sLst
}

func createDatabase(ctx context.Context, db *database) (Database, error) {
	var err error
	db.Lock()
	defer db.Unlock()
	for i := 0; i < int(db.shardNum); i++ {
		shardLocation, errInternal := mkdir(shardTemplate, db.location, i)
		if errInternal != nil {
			err = multierr.Append(err, errInternal)
			continue
		}
		so, errNewShard := newShard(ctx, i, shardLocation)
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
