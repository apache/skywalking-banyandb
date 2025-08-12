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

package property

import (
	"context"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/multierr"

	"github.com/apache/skywalking-banyandb/api/common"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/banyand/property/gossip"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/index/inverted"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

const (
	lockFilename = "lock"
)

var (
	lfs           = fs.NewLocalFileSystemWithLogger(logger.GetLogger("property"))
	propertyScope = observability.RootScope.SubScope("property")
)

type database struct {
	metadata            metadata.Repo
	omr                 observability.MetricsRegistry
	lfs                 fs.FileSystem
	lock                fs.File
	logger              *logger.Logger
	repairScheduler     *repairScheduler
	sLst                atomic.Pointer[[]*shard]
	location            string
	repairBaseDir       string
	flushInterval       time.Duration
	expireDelete        time.Duration
	repairTreeSlotCount int
	mu                  sync.RWMutex
	closed              atomic.Bool
}

func openDB(
	ctx context.Context,
	location string,
	flushInterval time.Duration,
	expireToDeleteDuration time.Duration,
	repairSlotCount int,
	omr observability.MetricsRegistry,
	lfs fs.FileSystem,
	repairEnabled bool,
	repairBaseDir string,
	repairBuildTreeCron string,
	repairQuickBuildTreeTime time.Duration,
	repairTriggerCron string,
	gossipMessenger gossip.Messenger,
	metadata metadata.Repo,
	buildSnapshotFunc func(context.Context) (string, error),
) (*database, error) {
	loc := filepath.Clean(location)
	lfs.MkdirIfNotExist(loc, storage.DirPerm)
	l := logger.GetLogger("property")

	db := &database{
		location:            loc,
		logger:              l,
		omr:                 omr,
		flushInterval:       flushInterval,
		expireDelete:        expireToDeleteDuration,
		repairTreeSlotCount: repairSlotCount,
		repairBaseDir:       repairBaseDir,
		lfs:                 lfs,
		metadata:            metadata,
	}
	var err error
	// init repair scheduler
	if repairEnabled {
		scheduler, schedulerErr := newRepairScheduler(l, omr, repairBuildTreeCron, repairQuickBuildTreeTime, repairTriggerCron,
			gossipMessenger, repairSlotCount, db, buildSnapshotFunc)
		if schedulerErr != nil {
			return nil, errors.Wrapf(schedulerErr, "failed to create repair scheduler for %s", loc)
		}
		db.repairScheduler = scheduler
	}
	if err = db.load(ctx); err != nil {
		return nil, err
	}
	db.logger.Info().Str("path", loc).Msg("initialized")
	lockPath := filepath.Join(loc, lockFilename)
	lock, err := lfs.CreateLockFile(lockPath, storage.FilePerm)
	if err != nil {
		logger.Panicf("cannot create lock file %s: %s", lockPath, err)
	}
	db.lock = lock
	observability.MetricsCollector.Register(loc, db.collect)
	return db, nil
}

func (db *database) load(ctx context.Context) error {
	if db.closed.Load() {
		return errors.New("database is closed")
	}
	return walkDir(db.location, "shard-", func(suffix string) error {
		id, err := strconv.Atoi(suffix)
		if err != nil {
			return err
		}
		_, err = db.loadShard(ctx, common.ShardID(id))
		return err
	})
}

func (db *database) update(ctx context.Context, shardID common.ShardID, id []byte, property *propertyv1.Property) error {
	sd, err := db.loadShard(ctx, shardID)
	if err != nil {
		return err
	}
	err = sd.update(id, property)
	if err != nil {
		return err
	}
	return nil
}

func (db *database) delete(ctx context.Context, docIDs [][]byte) error {
	sLst := db.sLst.Load()
	if sLst == nil {
		return nil
	}
	var err error
	for _, s := range *sLst {
		multierr.AppendInto(&err, s.delete(ctx, docIDs))
	}
	return err
}

func (db *database) query(ctx context.Context, req *propertyv1.QueryRequest) ([]*queryProperty, error) {
	iq, err := inverted.BuildPropertyQuery(req, groupField, entityID)
	if err != nil {
		return nil, err
	}
	sLst := db.sLst.Load()
	if sLst == nil {
		return nil, nil
	}
	var res []*queryProperty
	for _, s := range *sLst {
		r, err := s.search(ctx, iq, int(req.Limit))
		if err != nil {
			return nil, err
		}
		res = append(res, r...)
	}
	return res, nil
}

func (db *database) loadShard(ctx context.Context, id common.ShardID) (*shard, error) {
	if db.closed.Load() {
		return nil, errors.New("database is closed")
	}
	if s, ok := db.getShard(id); ok {
		return s, nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if s, ok := db.getShard(id); ok {
		return s, nil
	}
	sd, err := db.newShard(context.WithValue(ctx, logger.ContextKey, db.logger), id, int64(db.flushInterval.Seconds()),
		int64(db.expireDelete.Seconds()), db.repairBaseDir, db.repairTreeSlotCount)
	if err != nil {
		return nil, err
	}
	sLst := db.sLst.Load()
	if sLst == nil {
		sLst = &[]*shard{}
	}
	*sLst = append(*sLst, sd)
	db.sLst.Store(sLst)
	return sd, nil
}

func (db *database) getShard(id common.ShardID) (*shard, bool) {
	sLst := db.sLst.Load()
	if sLst == nil {
		return nil, false
	}
	for _, s := range *sLst {
		if s.id == id {
			return s, true
		}
	}
	return nil, false
}

func (db *database) close() error {
	if db.closed.Swap(true) {
		return nil
	}
	if db.repairScheduler != nil {
		db.repairScheduler.close()
	}
	sLst := db.sLst.Load()
	var err error
	if sLst != nil {
		for _, s := range *sLst {
			multierr.AppendInto(&err, s.close())
		}
	}
	db.lock.Close()
	return err
}

func (db *database) collect() {
	if db.closed.Load() {
		return
	}
	sLst := db.sLst.Load()
	if sLst == nil {
		return
	}
	for _, s := range *sLst {
		s.store.CollectMetrics()
	}
}

func (db *database) repair(ctx context.Context, id []byte, shardID uint64, property *propertyv1.Property, deleteTime int64) error {
	s, err := db.loadShard(ctx, common.ShardID(shardID))
	if err != nil {
		return errors.WithMessagef(err, "failed to load shard %d", id)
	}
	_, _, err = s.repair(ctx, id, property, deleteTime)
	return err
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

type queryProperty struct {
	id         []byte
	source     []byte
	timestamp  int64
	deleteTime int64
}
