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
	"os"
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
	"github.com/apache/skywalking-banyandb/pkg/index/native"
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
	// TakeSnapShot takes a snapshot of the database under the configured snapshot directory.
	TakeSnapShot(ctx context.Context, sn string) *databasev1.Snapshot
	// SnapshotShards copies every shard into <dstDir>/<group>/shard-N.
	SnapshotShards(ctx context.Context, dstDir string) error
	// Drop closes and removes all shards for the given group and deletes the group directory.
	Drop(groupName string) error
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
	groups              sync.Map
	repairBaseDir       string
	snapshotDir         string
	location            string
	indexConfig         IndexConfig
	flushInterval       time.Duration
	expireDelete        time.Duration
	repairTreeSlotCount int
	mu                  sync.RWMutex
	closed              atomic.Bool
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
		repairBaseDir:       cfg.Repair.Location,
		snapshotDir:         cfg.Snapshot.Location,
		lfs:                 lfs,
		indexConfig:         cfg.Index,
		lock:                lock,
	}
	db.nativeLease, err = native.NewFileRootLease(lock, loc)
	if err != nil {
		return nil, err
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
	if db.closed.Load() {
		return nil, errors.New("database is closed")
	}
	if s, ok := db.getShard(group, id); ok {
		return s, nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed.Load() {
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
			}
		}
		return true
	})
}

func (db *database) Repair(ctx context.Context, id []byte, shardID uint64, property *propertyv1.Property, deleteTime int64) error {
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

// errSnapshotDatabaseClosed is returned by SnapshotShards once the database is closed.
var errSnapshotDatabaseClosed = errors.New("database is closed")

func (db *database) TakeSnapShot(ctx context.Context, sn string) *databasev1.Snapshot {
	snp := &databasev1.Snapshot{Name: sn, Catalog: commonv1.Catalog_CATALOG_PROPERTY}
	if err := db.SnapshotShards(ctx, path.Join(db.snapshotDir, sn, storage.DataDir)); err != nil {
		if errors.Is(err, errSnapshotDatabaseClosed) {
			return nil
		}
		snp.Error = err.Error()
	}
	return snp
}

// SnapshotShards writes every shard into <dstDir>/<group>/shard-N through the index's
// backup, which copies the segment files (they are not hard links), so a snapshot needs as
// much free space as the shards it copies. A destination shard directory that already
// exists is an error. It stops at the first failure or when ctx is done, returning
// ctx.Err() in the latter case, and fails with errSnapshotDatabaseClosed once the database
// is closed.
func (db *database) SnapshotShards(ctx context.Context, dstDir string) error {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed.Load() {
		return errSnapshotDatabaseClosed
	}
	var err error
	db.groups.Range(func(_, value any) bool {
		gs := value.(*groupShards)
		sLst := gs.shards.Load()
		if sLst == nil {
			return true
		}
		for _, shardRef := range *sLst {
			if err = ctx.Err(); err != nil {
				return false
			}
			snpDir := path.Join(dstDir, shardRef.group, filepath.Base(shardRef.location))
			if err = os.MkdirAll(filepath.Dir(snpDir), storage.DirPerm); err != nil {
				return false
			}
			if err = os.Mkdir(snpDir, storage.DirPerm); err != nil {
				err = fmt.Errorf("create shard snapshot directory: %w", err)
				return false
			}
			if err = shardRef.nativeStore.takeFileSnapshot(snpDir); err != nil {
				db.logger.Error().Err(err).Str("group", shardRef.group).
					Str("shard", filepath.Base(shardRef.location)).Msg("fail to take shard snapshot")
				return false
			}
		}
		return true
	})
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
