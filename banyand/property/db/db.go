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
	"bytes"
	"container/heap"
	"context"
	"errors"
	"fmt"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	pkgerrors "github.com/pkg/errors"
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
	"github.com/apache/skywalking-banyandb/pkg/index/native"
	"github.com/apache/skywalking-banyandb/pkg/iter/sort"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/meter"
	"github.com/apache/skywalking-banyandb/pkg/query"
)

const (
	lockFilename = "lock"
)

var lfs = fs.NewLocalFileSystemWithLogger(logger.GetLogger("property"))

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
	Delete(ctx context.Context, id [][]byte, delTime time.Time) error
	// Query queries properties based on the given request.
	Query(ctx context.Context, request *propertyv1.QueryRequest) ([]QueriedProperty, error)
	// Repair repairs a property in the database.
	Repair(ctx context.Context, id []byte, shardID uint64, property *propertyv1.Property, deleteTime int64) error
	// TakeSnapShot takes a snapshot of the database.
	TakeSnapShot(ctx context.Context, sn string) *databasev1.Snapshot
	// Drop closes and removes all shards for the given group and deletes the group directory.
	Drop(groupName string) error
	// RegisterGossip registers the repair scheduler's gossip services with the given messenger.
	RegisterGossip(messenger gossip.Messenger)
	// Close closes the database.
	Close() error
	// SwitchIndexWriter drains the database and reopens every shard with the
	// requested writer while retaining the root ownership lock.
	SwitchIndexWriter(ctx context.Context, native bool) error
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
	// NativeWriter selects the native owner and query/mutation path for every
	// shard this database opens, existing and newly created alike. False keeps
	// the legacy writer path for rollback and compatibility operation.
	NativeWriter bool
}

// Config holds the configuration for the property database.
type Config struct {
	Snapshot               SnapshotConfig
	Location               string
	MetricsScopeName       string
	Repair                 RepairConfig
	Index                  IndexConfig
	FlushInterval          time.Duration
	ExpireToDeleteDuration time.Duration
}

type database struct {
	omr                 observability.MetricsRegistry
	metricsScope        meter.Scope
	lfs                 fs.FileSystem
	lock                fs.File
	nativeLease         *native.FileRootLease
	logger              *logger.Logger
	repairScheduler     *repairScheduler
	snapshotFunc        func(context.Context) (string, error)
	groups              sync.Map
	repairBaseDir       string
	snapshotDir         string
	repairBuildTreeCron string
	location            string
	indexConfig         IndexConfig
	flushInterval       time.Duration
	expireDelete        time.Duration
	repairTreeSlotCount int
	quickBuildTreeTime  time.Duration
	mu                  sync.RWMutex
	closed              atomic.Bool
	transition          atomic.Bool
	repairEnabled       bool
}

// OpenDB opens a property database with the given configuration.
//
// The exclusive <Location>/lock is acquired before any directory is scanned
// or any shard writer is opened, so a process that cannot establish ownership
// fails before touching a shard. Once the lock is held, any later startup
// failure closes all opened resources before returning.
func OpenDB(ctx context.Context, cfg Config, omr observability.MetricsRegistry, lfs fs.FileSystem) (Database, error) {
	if cfg.MetricsScopeName == "" {
		return nil, errors.New("metrics scope name must not be empty")
	}
	loc := filepath.Clean(cfg.Location)
	lfs.MkdirIfNotExist(loc, storage.DirPerm)
	l := logger.GetLogger("property")
	metricsScope := observability.RootScope.SubScope(cfg.MetricsScopeName)

	lockPath := filepath.Join(loc, lockFilename)
	lock, err := lfs.CreateLockFile(lockPath, storage.FilePerm)
	if err != nil {
		return nil, fmt.Errorf("cannot create lock file %s: %w", lockPath, err)
	}
	opened := false
	var db *database
	defer func() {
		if !opened {
			if db != nil && db.nativeLease != nil {
				_ = db.nativeLease.Revoke()
			}
			_ = lock.Close()
		}
	}()

	db = &database{
		location:            loc,
		logger:              l,
		omr:                 omr,
		metricsScope:        metricsScope,
		flushInterval:       cfg.FlushInterval,
		expireDelete:        cfg.ExpireToDeleteDuration,
		repairTreeSlotCount: cfg.Repair.TreeSlotCount,
		repairEnabled:       cfg.Repair.Enabled,
		repairBuildTreeCron: cfg.Repair.BuildTreeCron,
		quickBuildTreeTime:  cfg.Repair.QuickBuildTreeTime,
		snapshotFunc:        cfg.Snapshot.Func,
		repairBaseDir:       cfg.Repair.Location,
		snapshotDir:         cfg.Snapshot.Location,
		lfs:                 lfs,
		indexConfig:         cfg.Index,
		lock:                lock,
	}
	if cfg.Index.NativeWriter {
		db.nativeLease, err = native.NewFileRootLease(lock, loc)
		if err != nil {
			return nil, err
		}
	}
	// init repair scheduler
	if cfg.Repair.Enabled {
		scheduler, schedulerErr := newRepairScheduler(l, omr, metricsScope, cfg.Repair.BuildTreeCron, cfg.Repair.QuickBuildTreeTime,
			cfg.Repair.TreeSlotCount, db, cfg.Snapshot.Func)
		if schedulerErr != nil {
			return nil, pkgerrors.Wrapf(schedulerErr, "failed to create repair scheduler for %s", loc)
		}
		db.repairScheduler = scheduler
	}
	if err = db.load(ctx); err != nil {
		_ = db.cleanupStartup()
		return nil, err
	}
	db.logger.Info().Str("path", loc).Msg("initialized")
	obsservice.MetricsCollector.Register(loc, db.collect)
	opened = true
	return db, nil
}

// cleanupStartup closes resources opened before a failed database startup.
// The root lock remains held until all shard writers and scheduler callbacks
// have stopped, preventing another opener from racing leaked resources.
func (db *database) cleanupStartup() error {
	if db.repairScheduler != nil {
		db.repairScheduler.close()
		db.repairScheduler = nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	var err error
	db.groups.Range(func(_, value any) bool {
		gs := value.(*groupShards)
		if shards := gs.shards.Load(); shards != nil {
			for _, shardRef := range *shards {
				multierr.AppendInto(&err, shardRef.close())
			}
		}
		return true
	})
	db.releaseNativeLease(&err)
	return err
}

func (db *database) releaseNativeLease(err *error) {
	if db.nativeLease != nil {
		multierr.AppendInto(err, db.nativeLease.Revoke())
		db.nativeLease = nil
	}
	if db.lock != nil {
		multierr.AppendInto(err, db.lock.Close())
		db.lock = nil
	}
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
	if db.transition.Load() {
		return errors.New("database writer transition in progress")
	}
	sd, err := db.loadShard(ctx, property.Metadata.Group, shardID)
	if err != nil {
		return err
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed.Load() {
		return errors.New("database is closed")
	}
	if currentShard, shardExists := db.getShard(property.Metadata.Group, shardID); !shardExists || currentShard != sd {
		return errors.New("shard is closed")
	}
	err = sd.update(ctx, id, property)
	if err != nil {
		return err
	}
	return nil
}

func (db *database) Delete(ctx context.Context, docIDs [][]byte, delTime time.Time) error {
	if db.transition.Load() {
		return errors.New("database writer transition in progress")
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed.Load() {
		return errors.New("database is closed")
	}
	var err error
	db.groups.Range(func(_, value any) bool {
		gs := value.(*groupShards)
		sLst := gs.shards.Load()
		if sLst == nil {
			return true
		}
		for _, s := range *sLst {
			multierr.AppendInto(&err, s.deleteFromTime(ctx, docIDs, delTime))
		}
		return true
	})
	return err
}

func (db *database) Query(ctx context.Context, req *propertyv1.QueryRequest) ([]QueriedProperty, error) {
	if req == nil {
		return nil, errors.New("property query is nil")
	}
	if len(req.Groups) == 0 {
		return nil, errors.New("property query requires at least one group")
	}
	if db.transition.Load() {
		return nil, errors.New("database writer transition in progress")
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed.Load() {
		return nil, errors.New("database is closed")
	}
	requestedGroups := make(map[string]bool, len(req.Groups))
	for _, group := range req.Groups {
		requestedGroups[group] = true
	}
	shards := db.collectGroupShards(requestedGroups)
	if len(shards) == 0 {
		return nil, nil
	}
	if db.indexConfig.NativeWriter {
		if req.OrderBy == nil {
			var result []QueriedProperty
			for _, shardRef := range shards {
				hits, err := shardRef.searchNative(ctx, req, nil, int(req.Limit))
				if err != nil {
					return nil, err
				}
				for _, hit := range hits {
					result = append(result, hit)
				}
			}
			return result, nil
		}
		return db.queryNativeSorted(ctx, shards, req)
	}
	iq, err := inverted.BuildPropertyQuery(req, groupField, entityID)
	if err != nil {
		return nil, err
	}
	if req.OrderBy == nil {
		var result []QueriedProperty
		for _, shardRef := range shards {
			hits, searchErr := shardRef.search(ctx, iq, nil, int(req.Limit))
			if searchErr != nil {
				return nil, searchErr
			}
			for _, hit := range hits {
				result = append(result, hit)
			}
		}
		return result, nil
	}
	iters := make([]sort.Iterator[*queryProperty], 0, len(shards))
	for _, shardRef := range shards {
		hits, searchErr := shardRef.search(ctx, iq, req.OrderBy, int(req.Limit))
		if searchErr != nil {
			return nil, searchErr
		}
		if len(hits) > 0 {
			iters = append(iters, newQueryPropertyIterator(hits))
		}
	}
	if len(iters) == 0 {
		return nil, nil
	}
	mergeIter := sort.NewItemIter(iters, req.OrderBy.Sort == modelv1.Sort_SORT_DESC)
	defer mergeIter.Close()
	result := make([]QueriedProperty, 0, queryCapacityUint(req.Limit))
	for mergeIter.Next() {
		result = append(result, mergeIter.Val())
	}
	return result, nil
}

func (db *database) queryNativeSorted(ctx context.Context, shards []*shard, req *propertyv1.QueryRequest) ([]QueriedProperty, error) {
	iters := make([]*queryPropertyIterator, 0, len(shards))
	for _, shardRef := range shards {
		hits, err := shardRef.searchNative(ctx, req, req.OrderBy, int(req.Limit))
		if err != nil {
			return nil, err
		}
		if len(hits) > 0 {
			iters = append(iters, newQueryPropertyIterator(hits))
		}
	}
	if len(iters) == 0 {
		return nil, nil
	}
	mergeIter := newNativeQueryPropertyMergeIterator(iters, req.OrderBy.Sort == modelv1.Sort_SORT_DESC)
	defer mergeIter.Close()
	result := make([]QueriedProperty, 0, queryCapacityUint(req.Limit))
	for mergeIter.Next() {
		if err := query.Charge(ctx, 128); err != nil {
			return nil, err
		}
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
	if db.closed.Load() || db.transition.Load() {
		return nil, errors.New("database is closed")
	}
	if s, ok := db.getShard(group, id); ok {
		return s, nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed.Load() || db.transition.Load() {
		return nil, errors.New("database is closed")
	}
	return db.loadShardLocked(ctx, group, id)
}

func (db *database) loadShardLocked(ctx context.Context, group string, id common.ShardID) (*shard, error) {
	if db.closed.Load() {
		return nil, errors.New("database is closed")
	}
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

// Drop closes and removes all shards for the given group and deletes the group directory.
func (db *database) Drop(groupName string) (err error) {
	if db.transition.Load() {
		return errors.New("database writer transition in progress")
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	value, ok := db.groups.LoadAndDelete(groupName)
	if !ok {
		return nil
	}
	gs := value.(*groupShards)
	sLst := gs.shards.Load()
	if sLst != nil {
		for _, s := range *sLst {
			multierr.AppendInto(&err, s.close())
		}
		if err != nil {
			return err
		}
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("failed to remove group directory %s: %v", gs.location, r)
		}
	}()
	db.lfs.MustRMAll(gs.location)
	return nil
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
	db.mu.Lock()
	defer db.mu.Unlock()
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
	db.releaseNativeLease(&err)
	return err
}

// SwitchIndexWriter performs a lease-preserving writer transition. Admission
// is stopped and existing callbacks are drained before shards are closed; all
// shards are then reopened in the requested mode while the root lock remains
// held. A failed reopen fails closed and releases the lease only after every
// opened resource has been cleaned up.
func (db *database) SwitchIndexWriter(ctx context.Context, useNative bool) error {
	if db.closed.Load() {
		return errors.New("database is closed")
	}
	if !db.transition.CompareAndSwap(false, true) {
		return errors.New("database writer transition already in progress")
	}
	// The transition flag rejects new callbacks before the scheduler is
	// drained. Scheduler callbacks acquire db.mu themselves, so this must
	// happen before taking the database lock to avoid a wait cycle.
	if db.repairScheduler != nil {
		db.repairScheduler.close()
		db.repairScheduler = nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()

	// Taking the lock here drains operations that were already admitted.
	if db.closed.Load() {
		db.transition.Store(false)
		return errors.New("database is closed")
	}

	closeShards := func() error {
		var closeErr error
		db.groups.Range(func(_, value any) bool {
			gs := value.(*groupShards)
			if shards := gs.shards.Load(); shards != nil {
				for _, shardRef := range *shards {
					multierr.AppendInto(&closeErr, shardRef.close())
				}
			}
			return true
		})
		db.groups.Range(func(key, _ any) bool {
			db.groups.Delete(key)
			return true
		})
		return closeErr
	}
	if closeErr := closeShards(); closeErr != nil {
		db.closed.Store(true)
		db.releaseNativeLease(&closeErr)
		db.transition.Store(false)
		return fmt.Errorf("close shards for writer transition: %w", closeErr)
	}

	if useNative && db.nativeLease == nil {
		lease, ownerErr := native.NewFileRootLease(db.lock, db.location)
		if ownerErr != nil {
			db.closed.Store(true)
			_ = db.lock.Close()
			db.transition.Store(false)
			return fmt.Errorf("acquire native writer ownership: %w", ownerErr)
		}
		db.nativeLease = lease
	}
	db.indexConfig.NativeWriter = useNative

	var loadErr error
	for _, groupDir := range lfs.ReadDir(db.location) {
		if !groupDir.IsDir() {
			continue
		}
		groupName := groupDir.Name()
		groupPath := filepath.Join(db.location, groupName)
		if walkErr := walkDir(groupPath, "shard-", func(suffix string) error {
			id, parseErr := strconv.Atoi(suffix)
			if parseErr != nil {
				return parseErr
			}
			_, shardErr := db.loadShardLocked(ctx, groupName, common.ShardID(id))
			return shardErr
		}); walkErr != nil {
			loadErr = walkErr
			break
		}
	}
	if loadErr != nil {
		if db.repairScheduler != nil {
			db.repairScheduler.close()
			db.repairScheduler = nil
		}
		cleanupErr := closeShards()
		db.closed.Store(true)
		db.releaseNativeLease(&cleanupErr)
		db.transition.Store(false)
		return fmt.Errorf("reopen shards for writer transition: %w", multierr.Append(loadErr, cleanupErr))
	}
	if db.repairEnabled {
		scheduler, schedulerErr := newRepairScheduler(db.logger, db.omr, db.metricsScope,
			db.repairBuildTreeCron, db.quickBuildTreeTime, db.repairTreeSlotCount, db, db.snapshotFunc)
		if schedulerErr != nil {
			cleanupErr := closeShards()
			db.closed.Store(true)
			db.releaseNativeLease(&cleanupErr)
			db.transition.Store(false)
			return fmt.Errorf("recreate repair scheduler for writer transition: %w", multierr.Append(schedulerErr, cleanupErr))
		}
		db.repairScheduler = scheduler
		db.groups.Range(func(_, value any) bool {
			gs := value.(*groupShards)
			if shards := gs.shards.Load(); shards != nil {
				for _, shardRef := range *shards {
					shardRef.repairState.scheduler = scheduler
				}
			}
			return true
		})
	}
	db.transition.Store(false)
	return nil
}

func (db *database) collect() {
	if db.closed.Load() {
		return
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
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
			if s.nativeStore != nil {
				s.nativeStore.collectMetrics()
			} else if s.store != nil {
				s.store.CollectMetrics()
			}
		}
		return true
	})
}

func (db *database) Repair(ctx context.Context, id []byte, shardID uint64, property *propertyv1.Property, deleteTime int64) error {
	if db.transition.Load() {
		return errors.New("database writer transition in progress")
	}
	s, err := db.loadShard(ctx, property.Metadata.Group, common.ShardID(shardID))
	if err != nil {
		return pkgerrors.WithMessagef(err, "failed to load shard %d", id)
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed.Load() {
		return pkgerrors.WithMessagef(errors.New("database is closed"), "failed to load shard %d", id)
	}
	if currentShard, shardExists := db.getShard(property.Metadata.Group, common.ShardID(shardID)); !shardExists || currentShard != s {
		return pkgerrors.WithMessagef(errors.New("shard is closed"), "failed to load shard %d", id)
	}
	_, _, err = s.repair(ctx, id, property, deleteTime)
	return err
}

func (db *database) TakeSnapShot(ctx context.Context, sn string) *databasev1.Snapshot {
	if db.transition.Load() {
		return nil
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed.Load() {
		return nil
	}
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
			var snapshotErr error
			if shardRef.nativeStore != nil {
				snapshotErr = shardRef.nativeStore.takeFileSnapshot(snpDir)
			} else {
				snapshotErr = shardRef.store.TakeFileSnapshot(snpDir)
			}
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
			return pkgerrors.WithMessagef(errWalk, "failed to load: %s", f.Name())
		}
	}
	return nil
}

type queryProperty struct {
	id          []byte
	source      []byte
	sortedValue []byte
	sortMissing bool
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

// nativeQueryPropertyMergeIterator preserves missing-value ordering across
// shards without encoding missing as a byte sentinel that could collide with
// a valid property tag value.
type nativeQueryPropertyMergeIterator struct {
	heap    *nativeQueryPropertyMergeHeap
	current *queryProperty
}

func newNativeQueryPropertyMergeIterator(iters []*queryPropertyIterator, desc bool) *nativeQueryPropertyMergeIterator {
	frontier := &nativeQueryPropertyMergeHeap{desc: desc, items: make([]*nativeQueryPropertyMergeHead, 0, len(iters))}
	for _, iter := range iters {
		if iter.Next() {
			frontier.items = append(frontier.items, &nativeQueryPropertyMergeHead{item: iter.Val(), iter: iter})
		}
	}
	heap.Init(frontier)
	return &nativeQueryPropertyMergeIterator{heap: frontier}
}

func (it *nativeQueryPropertyMergeIterator) Next() bool {
	if it.heap.Len() == 0 {
		it.current = nil
		return false
	}
	head := heap.Pop(it.heap).(*nativeQueryPropertyMergeHead)
	it.current = head.item
	if head.iter.Next() {
		head.item = head.iter.Val()
		heap.Push(it.heap, head)
	}
	return true
}

func (it *nativeQueryPropertyMergeIterator) Val() *queryProperty { return it.current }

func (it *nativeQueryPropertyMergeIterator) Close() error { return nil }

type nativeQueryPropertyMergeHead struct {
	item *queryProperty
	iter *queryPropertyIterator
}

type nativeQueryPropertyMergeHeap struct {
	items []*nativeQueryPropertyMergeHead
	desc  bool
}

func (h nativeQueryPropertyMergeHeap) Len() int { return len(h.items) }

func (h nativeQueryPropertyMergeHeap) Less(left, right int) bool {
	return nativeQueryPropertyLess(h.items[left].item, h.items[right].item, h.desc)
}

func (h nativeQueryPropertyMergeHeap) Swap(left, right int) {
	h.items[left], h.items[right] = h.items[right], h.items[left]
}

func (h *nativeQueryPropertyMergeHeap) Push(value any) {
	h.items = append(h.items, value.(*nativeQueryPropertyMergeHead))
}

func (h *nativeQueryPropertyMergeHeap) Pop() any {
	last := len(h.items) - 1
	value := h.items[last]
	h.items = h.items[:last]
	return value
}

func nativeQueryPropertyLess(left, right *queryProperty, desc bool) bool {
	if left.sortMissing != right.sortMissing {
		return !left.sortMissing
	}
	if !left.sortMissing {
		cmp := bytes.Compare(left.sortedValue, right.sortedValue)
		if cmp != 0 {
			if desc {
				return cmp > 0
			}
			return cmp < 0
		}
	}
	return bytes.Compare(left.id, right.id) < 0
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
