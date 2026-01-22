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
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	obsservice "github.com/apache/skywalking-banyandb/banyand/observability/services"
	"github.com/apache/skywalking-banyandb/banyand/property/gossip"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/index/inverted"
	"github.com/apache/skywalking-banyandb/pkg/iter/sort"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

const (
	lockFilename = "lock"
)

var (
	lfs           = fs.NewLocalFileSystemWithLogger(logger.GetLogger("property"))
	propertyScope = observability.RootScope.SubScope("property")
)

type groupShards struct {
	shards   atomic.Pointer[[]*shard]
	group    string
	location string
	mu       sync.RWMutex
}

type database struct {
	metadata            metadata.Repo
	omr                 observability.MetricsRegistry
	lfs                 fs.FileSystem
	lock                fs.File
	logger              *logger.Logger
	repairScheduler     *repairScheduler
	groups              atomic.Pointer[map[string]*groupShards]
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
	obsservice.MetricsCollector.Register(loc, db.collect)
	return db, nil
}

func (db *database) load(ctx context.Context) error {
	if db.closed.Load() {
		return errors.New("database is closed")
	}
	for _, groupDir := range lfs.ReadDir(db.location) {
		if !groupDir.IsDir() {
			continue
		}
		groupName := groupDir.Name()
		groupPath := filepath.Join(db.location, groupName)
		walkErr := walkDir(groupPath, "shard-", func(suffix string) error {
			id, parseErr := strconv.Atoi(suffix)
			if parseErr != nil {
				return parseErr
			}
			_, loadErr := db.loadShard(ctx, groupName, common.ShardID(id))
			return loadErr
		})
		if walkErr != nil {
			return walkErr
		}
	}
	return nil
}

func (db *database) update(ctx context.Context, shardID common.ShardID, id []byte, property *propertyv1.Property) error {
	sd, err := db.loadShard(ctx, property.Metadata.Group, shardID)
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
	groupsMap := db.groups.Load()
	if groupsMap == nil {
		return nil
	}
	var err error
	for _, gs := range *groupsMap {
		sLst := gs.shards.Load()
		if sLst == nil {
			continue
		}
		for _, s := range *sLst {
			multierr.AppendInto(&err, s.delete(ctx, docIDs))
		}
	}
	return err
}

func (db *database) query(ctx context.Context, req *propertyv1.QueryRequest) ([]*queryProperty, error) {
	iq, err := inverted.BuildPropertyQuery(req, groupField, entityID)
	if err != nil {
		return nil, err
	}
	groupsMap := db.groups.Load()
	if groupsMap == nil {
		return nil, nil
	}
	requestedGroups := make(map[string]bool, len(req.Groups))
	for _, g := range req.Groups {
		requestedGroups[g] = true
	}
	shards := db.collectGroupShards(groupsMap, requestedGroups)
	if len(shards) == 0 {
		return nil, nil
	}

	if req.OrderBy == nil {
		var res []*queryProperty
		for _, s := range shards {
			r, searchErr := s.search(ctx, iq, nil, int(req.Limit))
			if searchErr != nil {
				return nil, searchErr
			}
			res = append(res, r...)
		}
		return res, nil
	}

	iters := make([]sort.Iterator[*queryProperty], 0, len(shards))
	for _, s := range shards {
		// Each shard returns pre-sorted results (via SeriesSort)
		r, searchErr := s.search(ctx, iq, req.OrderBy, int(req.Limit))
		if searchErr != nil {
			return nil, searchErr
		}
		if len(r) > 0 {
			// Wrap result slice as iterator and add to merge
			iters = append(iters, newQueryPropertyIterator(r))
		}
	}

	if len(iters) == 0 {
		return nil, nil
	}

	// K-way merge
	isDesc := req.OrderBy.Sort == modelv1.Sort_SORT_DESC
	mergeIter := sort.NewItemIter(iters, isDesc)
	defer mergeIter.Close()

	// Collect merged results up to limit
	result := make([]*queryProperty, 0, req.Limit)
	for mergeIter.Next() {
		result = append(result, mergeIter.Val())
	}

	return result, nil
}

func (db *database) collectGroupShards(groupsMap *map[string]*groupShards, requestedGroups map[string]bool) []*shard {
	var shards []*shard
	for groupName, gs := range *groupsMap {
		if len(requestedGroups) > 0 {
			if _, ok := requestedGroups[groupName]; !ok {
				continue
			}
		}
		sLst := gs.shards.Load()
		if sLst == nil {
			continue
		}
		shards = append(shards, *sLst...)
	}
	return shards
}

func (db *database) loadShard(ctx context.Context, group string, id common.ShardID) (*shard, error) {
	if db.closed.Load() {
		return nil, errors.New("database is closed")
	}
	if s, ok := db.getShard(group, id); ok {
		return s, nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if s, ok := db.getShard(group, id); ok {
		return s, nil
	}

	gs := db.getOrCreateGroupShards(group)
	sd, err := db.newShard(context.WithValue(ctx, logger.ContextKey, db.logger),
		group, id, int64(db.flushInterval.Seconds()),
		int64(db.expireDelete.Seconds()), db.repairBaseDir, db.repairTreeSlotCount)
	if err != nil {
		return nil, err
	}

	gs.mu.Lock()
	sLst := gs.shards.Load()
	if sLst == nil {
		sLst = &[]*shard{}
	}
	newList := append(*sLst, sd)
	gs.shards.Store(&newList)
	gs.mu.Unlock()
	return sd, nil
}

func (db *database) getOrCreateGroupShards(group string) *groupShards {
	groupsMap := db.groups.Load()
	if groupsMap != nil {
		if gs, ok := (*groupsMap)[group]; ok {
			return gs
		}
	}
	gs := &groupShards{
		group:    group,
		location: filepath.Join(db.location, group),
	}
	if groupsMap == nil {
		newMap := make(map[string]*groupShards)
		newMap[group] = gs
		db.groups.Store(&newMap)
	} else {
		(*groupsMap)[group] = gs
		db.groups.Store(groupsMap)
	}
	return gs
}

func (db *database) getShard(group string, id common.ShardID) (*shard, bool) {
	groupsMap := db.groups.Load()
	if groupsMap == nil {
		return nil, false
	}
	gs, ok := (*groupsMap)[group]
	if !ok {
		return nil, false
	}
	sLst := gs.shards.Load()
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
	groupsMap := db.groups.Load()
	var err error
	if groupsMap != nil {
		for _, gs := range *groupsMap {
			sLst := gs.shards.Load()
			if sLst == nil {
				continue
			}
			for _, s := range *sLst {
				multierr.AppendInto(&err, s.close())
			}
		}
	}
	db.lock.Close()
	return err
}

func (db *database) collect() {
	if db.closed.Load() {
		return
	}
	groupsMap := db.groups.Load()
	if groupsMap == nil {
		return
	}
	for _, gs := range *groupsMap {
		sLst := gs.shards.Load()
		if sLst == nil {
			continue
		}
		for _, s := range *sLst {
			s.store.CollectMetrics()
		}
	}
}

func (db *database) repair(ctx context.Context, id []byte, shardID uint64, property *propertyv1.Property, deleteTime int64) error {
	s, err := db.loadShard(ctx, property.Metadata.Group, common.ShardID(shardID))
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
	id          []byte
	source      []byte
	sortedValue []byte
	timestamp   int64
	deleteTime  int64
}

// SortedField implements sort.Comparable interface for k-way merge sorting.
func (q *queryProperty) SortedField() []byte {
	return q.sortedValue
}

// queryPropertyIterator wraps a slice of queryProperty to implement sort.Iterator interface.
type queryPropertyIterator struct {
	data  []*queryProperty
	index int
}

func newQueryPropertyIterator(data []*queryProperty) *queryPropertyIterator {
	return &queryPropertyIterator{
		data:  data,
		index: -1,
	}
}

func (it *queryPropertyIterator) Next() bool {
	it.index++
	return it.index < len(it.data)
}

func (it *queryPropertyIterator) Val() *queryProperty {
	if it.index < 0 || it.index >= len(it.data) {
		return nil
	}
	return it.data[it.index]
}

func (it *queryPropertyIterator) Close() error {
	return nil
}
