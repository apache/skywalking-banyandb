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

// Package db introduce the property storage database.
package db

import (
	"context"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/multierr"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
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

// QueriedProperty represents a property returned from a query.
type QueriedProperty interface {
	ID() []byte
	Timestamp() int64
	Source() []byte
	DeleteTime() int64
	SortedValue() []byte
}

// Database defines the interface for the property database.
type Database interface {
	// Update updates or inserts a property into the database.
	Update(ctx context.Context, shardID common.ShardID, id []byte, property *propertyv1.Property) error
	// Delete deletes properties with the given IDs from the database.
	Delete(ctx context.Context, id [][]byte) error
	// Query queries properties based on the given request.
	Query(ctx context.Context, request *propertyv1.QueryRequest) ([]QueriedProperty, error)
	// Repair repairs a property in the database.
	Repair(ctx context.Context, id []byte, shardID uint64, property *propertyv1.Property, deleteTime int64) error
	// TakeSnapShot takes a snapshot of the database.
	TakeSnapShot(ctx context.Context, sn string) *databasev1.Snapshot
	// RegisterGossip registers the repair scheduler's gossip services with the given messenger.
	RegisterGossip(messenger gossip.Messenger)
	// Close closes the database.
	Close() error
}

type groupShards struct {
	shards   atomic.Pointer[[]*shard]
	group    string
	location string
	mu       sync.RWMutex
}

// RepairConfig holds configuration for the repair build tree scheduler.
type RepairConfig struct {
	Location           string
	BuildTreeCron      string
	QuickBuildTreeTime time.Duration
	TreeSlotCount      int
	Enabled            bool
}

// SnapshotConfig holds configuration for snapshots.
type SnapshotConfig struct {
	Func     func(context.Context) (string, error)
	Location string
}

// IndexConfig holds storage configuration for the inverted index.
type IndexConfig struct {
	BatchWaitSec       int64
	WaitForPersistence bool
}

// Config holds the configuration for the property database.
type Config struct {
	Snapshot               SnapshotConfig
	Location               string
	Repair                 RepairConfig
	Index                  IndexConfig
	FlushInterval          time.Duration
	ExpireToDeleteDuration time.Duration
}

type database struct {
	omr                 observability.MetricsRegistry
	lfs                 fs.FileSystem
	lock                fs.File
	logger              *logger.Logger
	repairScheduler     *repairScheduler
	groups              sync.Map
	location            string
	snapshotDir         string
	repairBaseDir       string
	indexConfig         IndexConfig
	flushInterval       time.Duration
	expireDelete        time.Duration
	repairTreeSlotCount int
	mu                  sync.RWMutex
	closed              atomic.Bool
}

// OpenDB opens a property database with the given configuration.
func OpenDB(ctx context.Context, cfg Config, omr observability.MetricsRegistry, lfs fs.FileSystem) (Database, error) {
	loc := filepath.Clean(cfg.Location)
	lfs.MkdirIfNotExist(loc, storage.DirPerm)
	l := logger.GetLogger("property")

	db := &database{
		location:            loc,
		logger:              l,
		omr:                 omr,
		flushInterval:       cfg.FlushInterval,
		expireDelete:        cfg.ExpireToDeleteDuration,
		repairTreeSlotCount: cfg.Repair.TreeSlotCount,
		repairBaseDir:       cfg.Repair.Location,
		snapshotDir:         cfg.Snapshot.Location,
		lfs:                 lfs,
		indexConfig:         cfg.Index,
	}
	var err error
	// init repair scheduler
	if cfg.Repair.Enabled {
		scheduler, schedulerErr := newRepairScheduler(l, omr, cfg.Repair.BuildTreeCron, cfg.Repair.QuickBuildTreeTime,
			cfg.Repair.TreeSlotCount, db, cfg.Snapshot.Func)
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

func (db *database) Update(ctx context.Context, shardID common.ShardID, id []byte, property *propertyv1.Property) error {
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

func (db *database) Delete(ctx context.Context, docIDs [][]byte) error {
	var err error
	db.groups.Range(func(_, value any) bool {
		gs := value.(*groupShards)
		sLst := gs.shards.Load()
		if sLst == nil {
			return true
		}
		for _, s := range *sLst {
			multierr.AppendInto(&err, s.delete(ctx, docIDs))
		}
		return true
	})
	return err
}

func (db *database) Query(ctx context.Context, req *propertyv1.QueryRequest) ([]QueriedProperty, error) {
	iq, err := inverted.BuildPropertyQuery(req, groupField, entityID)
	if err != nil {
		return nil, err
	}
	requestedGroups := make(map[string]bool, len(req.Groups))
	for _, g := range req.Groups {
		requestedGroups[g] = true
	}
	shards := db.collectGroupShards(requestedGroups)
	if len(shards) == 0 {
		return nil, nil
	}

	if req.OrderBy == nil {
		var res []QueriedProperty
		for _, s := range shards {
			results, searchErr := s.search(ctx, iq, nil, int(req.Limit))
			if searchErr != nil {
				return nil, searchErr
			}
			for _, r := range results {
				res = append(res, r)
			}
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
	result := make([]QueriedProperty, 0, req.Limit)
	for mergeIter.Next() {
		result = append(result, mergeIter.Val())
	}

	return result, nil
}

func (db *database) collectGroupShards(requestedGroups map[string]bool) []*shard {
	var shards []*shard
	db.groups.Range(func(key, value any) bool {
		groupName := key.(string)
		if len(requestedGroups) > 0 {
			if _, ok := requestedGroups[groupName]; !ok {
				return true
			}
		}
		gs := value.(*groupShards)
		sLst := gs.shards.Load()
		if sLst == nil {
			return true
		}
		shards = append(shards, *sLst...)
		return true
	})
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
	var oldList []*shard
	if sLst != nil {
		oldList = *sLst
	}
	newList := make([]*shard, len(oldList)+1)
	copy(newList, oldList)
	newList[len(oldList)] = sd
	gs.shards.Store(&newList)
	gs.mu.Unlock()
	return sd, nil
}

func (db *database) getOrCreateGroupShards(group string) *groupShards {
	gs := &groupShards{
		group:    group,
		location: filepath.Join(db.location, group),
	}
	actual, _ := db.groups.LoadOrStore(group, gs)
	return actual.(*groupShards)
}

func (db *database) getShard(group string, id common.ShardID) (*shard, bool) {
	value, ok := db.groups.Load(group)
	if !ok {
		return nil, false
	}
	gs := value.(*groupShards)
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

// RegisterGossip registers the repair scheduler's gossip services with the given messenger.
func (db *database) RegisterGossip(messenger gossip.Messenger) {
	if db.repairScheduler == nil {
		return
	}
	messenger.RegisterServices(db.repairScheduler.registerServerToGossip())
	db.repairScheduler.registerClientToGossip(messenger)
}

func (db *database) Close() error {
	if db.closed.Swap(true) {
		return nil
	}
	if db.repairScheduler != nil {
		db.repairScheduler.close()
	}
	var err error
	db.groups.Range(func(_, value any) bool {
		gs := value.(*groupShards)
		sLst := gs.shards.Load()
		if sLst == nil {
			return true
		}
		for _, s := range *sLst {
			multierr.AppendInto(&err, s.close())
		}
		return true
	})
	db.lock.Close()
	return err
}

func (db *database) collect() {
	if db.closed.Load() {
		return
	}
	db.groups.Range(func(_, value any) bool {
		gs := value.(*groupShards)
		sLst := gs.shards.Load()
		if sLst == nil {
			return true
		}
		for _, s := range *sLst {
			s.store.CollectMetrics()
		}
		return true
	})
}

func (db *database) Repair(ctx context.Context, id []byte, shardID uint64, property *propertyv1.Property, deleteTime int64) error {
	s, err := db.loadShard(ctx, property.Metadata.Group, common.ShardID(shardID))
	if err != nil {
		return errors.WithMessagef(err, "failed to load shard %d", id)
	}
	_, _, err = s.repair(ctx, id, property, deleteTime)
	return err
}

func (db *database) TakeSnapShot(ctx context.Context, sn string) *databasev1.Snapshot {
	var snapshotResult *databasev1.Snapshot
	db.groups.Range(func(_, value any) bool {
		gs := value.(*groupShards)
		sLst := gs.shards.Load()
		if sLst == nil {
			return true
		}
		for _, shardRef := range *sLst {
			select {
			case <-ctx.Done():
				// Context canceled: record an error snapshot and stop iteration.
				if ctxErr := ctx.Err(); ctxErr != nil {
					snapshotResult = &databasev1.Snapshot{
						Name:    sn,
						Catalog: commonv1.Catalog_CATALOG_PROPERTY,
						Error:   ctxErr.Error(),
					}
				}
				return false
			default:
			}
			snpDir := path.Join(db.snapshotDir, sn, storage.DataDir, shardRef.group, filepath.Base(shardRef.location))
			db.lfs.MkdirPanicIfExist(snpDir, storage.DirPerm)
			snapshotErr := shardRef.store.TakeFileSnapshot(snpDir)
			if snapshotErr != nil {
				db.logger.Error().Err(snapshotErr).Str("group", shardRef.group).
					Str("shard", filepath.Base(shardRef.location)).Msg("fail to take shard snapshot")
				snapshotResult = &databasev1.Snapshot{
					Name:    sn,
					Catalog: commonv1.Catalog_CATALOG_PROPERTY,
					Error:   snapshotErr.Error(),
				}
				return false
			}
		}
		return true
	})
	if snapshotResult != nil {
		return snapshotResult
	}
	return &databasev1.Snapshot{
		Name:    sn,
		Catalog: commonv1.Catalog_CATALOG_PROPERTY,
	}
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

func (q *queryProperty) ID() []byte {
	return q.id
}

func (q *queryProperty) Source() []byte {
	return q.source
}

func (q *queryProperty) SortedValue() []byte {
	return q.sortedValue
}

func (q *queryProperty) Timestamp() int64 {
	return q.timestamp
}

func (q *queryProperty) DeleteTime() int64 {
	return q.deleteTime
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
