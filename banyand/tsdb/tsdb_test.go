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
	"os"
	"sync"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

func TestOpenDatabase(t *testing.T) {
	tester := assert.New(t)
	req := require.New(t)
	tempDir, deferFunc := test.Space(req)
	db := openDatabase(context.Background(), req, tempDir)
	defer func() {
		req.NoError(db.Close())
		deferFunc()
	}()
	verifyDatabaseStructure(tester, tempDir, time.Now())
}

func TestReOpenDatabase(t *testing.T) {
	tester := assert.New(t)
	req := require.New(t)
	tempDir, deferFunc := test.Space(req)
	defer deferFunc()
	db := openDatabase(context.Background(), req, tempDir)
	req.NoError(db.Close())
	verifyDatabaseStructure(tester, tempDir, time.Now())
	db = openDatabase(context.Background(), req, tempDir)
	req.NoError(db.Close())
	verifyDatabaseStructure(tester, tempDir, time.Now())
}

func TestReOpenDatabaseNextBlock(t *testing.T) {
	tester := assert.New(t)
	req := require.New(t)
	tempDir, deferFunc := test.Space(req)
	defer deferFunc()
	clock := timestamp.NewMockClock()
	clock.Set(time.Date(1970, 0o1, 0o1, 0, 0, 0, 0, time.Local))
	db := openDatabase(timestamp.SetClock(context.Background(), clock), req, tempDir)
	req.NoError(db.Close())
	verifyDatabaseStructure(tester, tempDir, clock.Now())
	clock.Add(5 * time.Hour)
	db = openDatabase(timestamp.SetClock(context.Background(), clock), req, tempDir)
	req.NoError(db.Close())
	verifyDatabaseStructure(tester, tempDir, clock.Now())
}

func TestReOpenDatabaseNextDay(t *testing.T) {
	tester := assert.New(t)
	req := require.New(t)
	tempDir, deferFunc := test.Space(req)
	defer deferFunc()
	clock := timestamp.NewMockClock()
	clock.Set(time.Date(1970, 0o1, 0o1, 0, 0, 0, 0, time.Local))
	db := openDatabase(timestamp.SetClock(context.Background(), clock), req, tempDir)
	req.NoError(db.Close())
	verifyDatabaseStructure(tester, tempDir, clock.Now())
	clock.Add(26 * time.Hour)
	db = openDatabase(timestamp.SetClock(context.Background(), clock), req, tempDir)
	req.NoError(db.Close())
	verifyDatabaseStructure(tester, tempDir, clock.Now())
}

func verifyDatabaseStructure(tester *assert.Assertions, tempDir string, now time.Time) {
	shardPath := fmt.Sprintf(shardTemplate, tempDir, 0)
	validateDirectory(tester, shardPath)
	seriesPath := fmt.Sprintf(seriesTemplate, shardPath)
	validateDirectory(tester, seriesPath)
	segPath := fmt.Sprintf(segTemplate, shardPath, now.Format(dayFormat))
	validateDirectory(tester, segPath)
	validateDirectory(tester, fmt.Sprintf(blockTemplate, segPath, now.Format(hourFormat)))
}

func openDatabase(ctx context.Context, t *require.Assertions, path string) (db Database) {
	t.NoError(logger.Init(logger.Logging{
		Env:   "dev",
		Level: flags.LogLevel,
	}))
	db, err := OpenDatabase(
		context.WithValue(ctx, logger.ContextKey, logger.GetLogger("test")),
		DatabaseOpts{
			Location:        path,
			ShardNum:        1,
			BlockInterval:   IntervalRule{Num: 2},
			SegmentInterval: IntervalRule{Num: 1, Unit: DAY},
			TTL:             IntervalRule{Num: 7, Unit: DAY},
			TSTableFactory:  bypassTSTableFactory{},
		})
	t.NoError(err)
	t.NotNil(db)
	shard, err := db.CreateShardsAndGetByID(0)
	t.NoError(err)
	t.Equal(common.ShardID(0), shard.ID())
	return db
}

func validateDirectory(t *assert.Assertions, dir string) {
	info, err := os.Stat(dir)
	t.False(os.IsNotExist(err), "Directory does not exist: %v", dir)
	t.NoError(err, "Directory error: %v", dir)
	t.True(info.IsDir(), "Directory is a file, not a directory: %#v\n", dir)
}

// bypassTSTableFactory is a bypass implementation of TSTableFactory.
type bypassTSTableFactory struct{}

// NewByPassTSTableFactory creates a bypass implementation of TSTableFactory.
func NewByPassTSTableFactory() TSTableFactory {
	return bypassTSTableFactory{}
}

func (bypassTSTableFactory) NewTSTable(_ *BufferSupplier, _ string, _ common.Position, _ *logger.Logger, _ timestamp.TimeRange) (TSTable, error) {
	return newBypassTSTable()
}

// bypassTSTable is a bypass implementation of TSTable.
type bypassTSTable struct{}

func newBypassTSTable() (TSTable, error) {
	return &bypassTSTable{}, nil
}

func (b *bypassTSTable) OpenBuffer() error {
	return nil
}

func (b *bypassTSTable) Put(_, _ []byte, _ time.Time) error {
	return nil
}

func (b *bypassTSTable) Get(_ []byte, _ time.Time) ([]byte, error) {
	return nil, nil
}

func (b *bypassTSTable) CollectStats() *badger.Statistics {
	return &badger.Statistics{TableBuilderSize: &sync.Map{}}
}

func (b *bypassTSTable) SizeOnDisk() int64 {
	return 0
}

func (b *bypassTSTable) Close() error {
	return nil
}
