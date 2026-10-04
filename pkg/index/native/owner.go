// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses
// this file to You under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License. You may
// obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package native

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"

	roaringpkg "github.com/RoaringBitmap/roaring"

	"github.com/apache/skywalking-banyandb/pkg/index/internal/nativeice"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

var (
	// ErrOwnerClosed reports a mutation or acquire after owner shutdown.
	ErrOwnerClosed = errors.New("native: owner is closed")
	// ErrViewClosed reports a read through a released pinned view.
	ErrViewClosed = errors.New("native: view is closed")
	// ErrInvalidDocument reports a mutation that cannot be admitted.
	ErrInvalidDocument = errors.New("native: invalid document")
	// ErrLeaseUnavailable reports a missing or invalid database ownership lease.
	ErrLeaseUnavailable = errors.New("native: root lease unavailable")
	// ErrPersistenceBackpressure reports a full persistence queue.
	ErrPersistenceBackpressure = errors.New("native: persistence queue is full")
	// ErrStaleCompaction reports a compaction built from a root superseded by
	// another admission. Its output is discarded rather than resurrecting late
	// deletes or updates.
	ErrStaleCompaction = errors.New("native: stale compaction root")
	// ErrPersistenceConfiguration reports mutually exclusive test and durable
	// persistence configurations.
	ErrPersistenceConfiguration = errors.New("native: invalid persistence configuration")
	// ErrPersistenceBusy reports garbage collection while persistence work or
	// an active immutable view still retains files.
	ErrPersistenceBusy = errors.New("native: persistence or view references remain")
	// ErrPersistenceFailed reports an owner that encountered an uncertain
	// durability result. The in-memory root remains readable, but new writes
	// are rejected until the owner is reopened and the on-disk snapshot is
	// validated again.
	ErrPersistenceFailed = errors.New("native: persistence failed")
)

// Term is one already encoded exact indexed term.
type Term struct {
	Value     []byte
	Frequency uint64
}

// Field is an opaque native field. Product schema and analyzer types stay
// outside this package; callers provide encoded names, values, and terms.
type Field struct {
	Name  string
	Value []byte
	Terms []Term
	Store bool
	Index bool
	Sort  bool
}

// Document is one physical document admitted to a native segment.
type Document struct {
	Identifier []byte
	Fields     []Field
	Timestamp  int64
}

// Batch is one serialized admission operation. Deletes are applied before
// Documents, so an update cannot leave two live physical documents.
//
//nolint:govet // mutation fields are grouped by ownership and callback role.
type Batch struct {
	Documents []Document
	Deletes   [][]byte
	// InsertOnly preserves every physical document, including duplicate
	// identifiers. The default false mode keeps update/upsert semantics.
	InsertOnly         bool
	PersistentCallback func(error)
}

// RootLease is supplied by the database owner. Native indexing never creates
// or manages an index-local lock; it only verifies this capability before
// admitting writes.
type RootLease interface {
	Validate() error
}

// PathRootLease binds a lease to the directory whose immutable files the
// owner publishes. Disk-backed owners refuse an unbound lease so a caller
// cannot accidentally publish outside the database-owned root.
type PathRootLease interface {
	RootLease
	ValidatePath(string) error
}

// PersistFunc receives a pinned immutable view after in-memory publication.
// It must persist only the new immutable segments and publication metadata;
// it must not mutate the view. The owner releases the view after the callback.
type PersistFunc func(context.Context, *ReadView) error

// OwnerOptions supplies database ownership and asynchronous persistence.
type OwnerOptions struct {
	Lease   RootLease
	Persist PersistFunc
	// Path enables the built-in ICE snapshot publisher. An empty path keeps the
	// owner in memory-only mode unless Persist is supplied for a test seam.
	Path string
	// QueueSize bounds asynchronous persistence admission. Zero uses the
	// package default.
	QueueSize int
	// CompactionThreshold schedules one serialized background compaction after
	// this many immutable segments are published. Zero uses a conservative
	// default; a negative value disables scheduling for maintenance tests.
	CompactionThreshold int
	// DeduplicateExternal controls whether external segment introduction masks
	// existing identifiers. It defaults to false to preserve legacy receiver
	// semantics; callers that enabled external deduplication opt in explicitly.
	DeduplicateExternal bool
}

// Owner serializes mutation admission and publishes immutable roots. A root
// is a shallow copy-on-write list of immutable segments; acquiring a view is
// O(1) and does not clone documents. Deletes clone only affected segment
// masks. Persistence runs after publication, so a durability failure reports
// an error without rolling back the already-visible in-memory root.
//
//nolint:govet // synchronization and lifecycle fields are intentionally grouped.
type Owner struct {
	mu        sync.Mutex
	stateCond *sync.Cond
	root      *publishedRoot
	// roots is a non-owning registry of every root published by this owner.
	// It lets garbage collection conservatively account for views and queued
	// persistence work that still retain an older immutable root.
	roots                map[*publishedRoot]struct{}
	options              OwnerOptions
	closed               bool
	closing              bool
	closeErr             error
	collecting           bool
	activeOps            int
	nextSegmentID        uint64
	persistQ             *persistenceQueue
	durable              atomic.Uint64
	persistMu            sync.Mutex
	persistErr           error
	externalMu           sync.Mutex
	external             map[*externalSegmentStreamer]struct{}
	maintenanceWake      chan struct{}
	maintenanceGCWake    chan struct{}
	maintenanceCancel    context.CancelFunc
	maintenanceTask      *run.Task
	maintenanceThreshold int
	// durabilityFault is guarded by mu. It is terminal for this owner: after a
	// publication has become uncertain, retrying the same immutable filenames
	// could conflict with a manifest that is already visible on disk.
	durabilityFault error
}

type persistenceTask struct {
	root     *publishedRoot
	callback func(error)
	compact  bool
}

//nolint:govet // queue channels and lifecycle mutex are intentionally grouped.
type persistenceQueue struct {
	tasks chan persistenceTask
	slots chan struct{}
	done  chan struct{}
}

func (q *persistenceQueue) reserve() bool {
	select {
	case q.slots <- struct{}{}:
		return true
	default:
		return false
	}
}

func (q *persistenceQueue) releaseReservation() {
	<-q.slots
}

//nolint:govet // root state is grouped by publication and reference ownership.
type publishedRoot struct {
	generation uint64
	segments   []rootSegment
	nextNumber uint64
	refs       atomic.Int64
}

func (r *publishedRoot) release() {
	if r == nil || r.refs.Add(-1) != 0 {
		return
	}
	releaseSegments(r.segments)
	r.segments = nil
}

type rootSegment interface {
	Delete(identifier []byte) (rootSegment, bool, error)
	Lookup(context.Context, []byte) (Document, bool, error)
	VisitIdentifiers(context.Context, func([]byte) bool) error
	MatchTerms(context.Context, MatchRequest) (MatchResult, error)
	Len() uint64
	retain()
	release()
}

//nolint:govet // memory segment fields are grouped by immutable data and mask.
type memorySegment struct {
	handle  *segmentHandle
	deleted map[uint64]struct{}
}

//nolint:govet // reader, payload, and atomic lifecycle state share ownership.
type segmentHandle struct {
	reader        *nativeice.Reader
	payload       []byte
	sourcePath    string
	refs          atomic.Int64
	count         uint64
	id            uint64
	size          uint64
	timeMin       uint64
	timeMax       uint64
	hasTime       bool
	indexedFields []string
	persisted     atomic.Bool
}

type persistedPromotion struct {
	original    *segmentHandle
	replacement *segmentHandle
}

// ReadView pins one immutable published root. It must be closed by its
// caller; closing is idempotent and releases exactly one root reference. The
// caller must not close a view concurrently with an operation on that same
// view; independent views may be used concurrently.
type ReadView struct {
	owner  *Owner
	root   *publishedRoot
	closed atomic.Bool
}

// NewOwner creates or recovers a native publication root. A valid database
// lease is required even for an empty owner, making ownership an executable
// invariant.
func NewOwner(options OwnerOptions) (*Owner, error) {
	if options.Lease == nil {
		return nil, ErrLeaseUnavailable
	}
	if err := options.Lease.Validate(); err != nil {
		return nil, fmt.Errorf("validate native root lease: %w", err)
	}
	if options.Path != "" && options.Persist != nil {
		return nil, ErrPersistenceConfiguration
	}
	if options.Path != "" {
		pathLease, ok := options.Lease.(PathRootLease)
		if !ok {
			return nil, ErrLeaseUnavailable
		}
		if err := pathLease.ValidatePath(options.Path); err != nil {
			return nil, fmt.Errorf("validate native root lease path: %w", err)
		}
	}
	generation := uint64(1)
	nextSegmentID := uint64(0)
	if options.Path != "" {
		nextSegment, nextSnapshot, idErr := nativeice.NextPublicationIDs(options.Path)
		if idErr != nil {
			return nil, fmt.Errorf("allocate native publication identifiers: %w", idErr)
		}
		nextSegmentID = nextSegment
		generation = nextSnapshot
		if nextSnapshot > 0 {
			generation = nextSnapshot - 1
		}
	}
	root := &publishedRoot{generation: generation}
	var durableGeneration uint64
	if options.Path != "" {
		loaded, loadErr := loadPersistedRoot(options.Path)
		if loadErr != nil {
			return nil, loadErr
		}
		if loaded != nil {
			root = loaded
			durableGeneration = loaded.generation
		}
	}
	root.refs.Store(1) // owner-held reference
	owner := &Owner{
		options: options, root: root, roots: map[*publishedRoot]struct{}{root: {}},
		external: make(map[*externalSegmentStreamer]struct{}), nextSegmentID: nextSegmentID,
	}
	owner.stateCond = sync.NewCond(&owner.mu)
	owner.durable.Store(durableGeneration)
	if options.Persist != nil || options.Path != "" {
		queueSize := options.QueueSize
		if queueSize <= 0 {
			queueSize = 16
		}
		queue := &persistenceQueue{tasks: make(chan persistenceTask, queueSize), slots: make(chan struct{}, queueSize), done: make(chan struct{})}
		owner.persistQ = queue
		run.Go(context.Background(), "native.owner.persistence", nil, func(ctx context.Context) {
			owner.runPersistence(ctx, queue)
		})
		if options.Path != "" && options.CompactionThreshold >= 0 {
			threshold := options.CompactionThreshold
			if threshold == 0 {
				threshold = 16
			}
			owner.maintenanceThreshold = threshold
			owner.maintenanceWake = make(chan struct{}, 1)
			owner.maintenanceGCWake = make(chan struct{}, 1)
			maintenanceContext, cancel := context.WithCancel(context.Background())
			owner.maintenanceCancel = cancel
			owner.maintenanceTask = run.Go(maintenanceContext, "native.owner.compaction", nil, owner.runMaintenance)
		}
	}
	return owner, nil
}

func (o *Owner) runMaintenance(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-o.maintenanceWake:
		case <-o.maintenanceGCWake:
		}
		o.mu.Lock()
		shouldCompact := !o.closed && !o.closing && !o.collecting && o.durabilityFault == nil &&
			o.root != nil && len(o.root.segments) >= o.maintenanceThreshold
		o.mu.Unlock()
		if shouldCompact {
			// One attempt per wake bounds work under continuous admissions. A
			// stale build or backpressure result waits for the next batch signal
			// instead of repeatedly remerging the full root.
			_ = o.Compact(ctx)
		} else {
			// Persistence completion and view release both wake this path so
			// obsolete files are collected as soon as no root still pins them.
			_ = o.CollectGarbage(ctx)
		}
	}
}

func (o *Owner) requestMaintenance() {
	if o.maintenanceWake == nil {
		return
	}
	o.mu.Lock()
	shouldCompact := !o.closed && !o.closing && o.durabilityFault == nil && o.root != nil &&
		len(o.root.segments) >= o.maintenanceThreshold
	o.mu.Unlock()
	if shouldCompact {
		select {
		case o.maintenanceWake <- struct{}{}:
		default:
		}
	}
}

func (o *Owner) requestGarbageCollection() {
	if o.maintenanceGCWake == nil {
		return
	}
	select {
	case o.maintenanceGCWake <- struct{}{}:
	default:
	}
}

func loadPersistedRoot(path string) (*publishedRoot, error) {
	reader, openErr := nativeice.OpenStrict(path)
	if errors.Is(openErr, nativeice.ErrNoSnapshot) {
		return nil, nil
	}
	if openErr != nil {
		return nil, fmt.Errorf("open native owner snapshot: %w", openErr)
	}
	metadata := reader.SnapshotMetadata()
	if closeErr := reader.Close(); closeErr != nil {
		return nil, fmt.Errorf("close native owner snapshot: %w", closeErr)
	}
	root := &publishedRoot{generation: metadata.ID}
	root.refs.Store(1)
	for _, segmentMetadata := range metadata.Segments {
		segmentPath := filepath.Join(path, fmt.Sprintf("%012x.seg", segmentMetadata.ID))
		segmentReader, segmentErr := nativeice.OpenSnapshotSegment(segmentPath, segmentMetadata)
		if segmentErr != nil {
			root.release()
			return nil, fmt.Errorf("open native owner segment %d: %w", segmentMetadata.ID, segmentErr)
		}
		deleted, deletionErr := decodeDeletionBitmap(segmentMetadata.DeletionBitmap)
		if deletionErr != nil {
			_ = segmentReader.Close()
			root.release()
			return nil, deletionErr
		}
		handle := &segmentHandle{
			reader: segmentReader, count: segmentMetadata.DocumentCount, id: segmentMetadata.ID,
			size: segmentMetadata.Size, timeMin: segmentMetadata.TimeMin, timeMax: segmentMetadata.TimeMax,
			hasTime: segmentMetadata.TimeMin != 0 || segmentMetadata.TimeMax != 0,
		}
		fields, fieldsErr := segmentReader.Fields()
		if fieldsErr != nil {
			_ = segmentReader.Close()
			root.release()
			return nil, fmt.Errorf("enumerate native owner segment %d fields: %w", segmentMetadata.ID, fieldsErr)
		}
		handle.indexedFields = fields
		handle.refs.Store(1)
		handle.persisted.Store(true)
		root.segments = append(root.segments, &memorySegment{handle: handle, deleted: deleted})
		root.nextNumber += segmentMetadata.DocumentCount
	}
	return root, nil
}

func decodeDeletionBitmap(payload []byte) (map[uint64]struct{}, error) {
	if len(payload) == 0 {
		return nil, nil
	}
	bitmap := roaringpkg.New()
	if unmarshalErr := bitmap.UnmarshalBinary(payload); unmarshalErr != nil {
		return nil, fmt.Errorf("decode native owner deletion mask: %w", unmarshalErr)
	}
	deleted := make(map[uint64]struct{}, bitmap.GetCardinality())
	iterator := bitmap.Iterator()
	for iterator.HasNext() {
		deleted[uint64(iterator.Next())] = struct{}{}
	}
	return deleted, nil
}

func (o *Owner) runPersistence(ctx context.Context, queue *persistenceQueue) {
	defer close(queue.done)
	for task := range queue.tasks {
		queue.releaseReservation()
		view := &ReadView{root: task.root}
		persistErr := o.persistenceFault()
		if persistErr == nil {
			persistErr = safePersist(func() error {
				if o.options.Persist != nil {
					return o.options.Persist(ctx, view)
				}
				return o.persistRoot(task.root)
			})
			if persistErr != nil {
				persistErr = o.markPersistenceFailure(persistErr)
			}
		}
		_ = view.Close()
		if persistErr == nil {
			for {
				current := o.durable.Load()
				if current >= task.root.generation || o.durable.CompareAndSwap(current, task.root.generation) {
					break
				}
			}
		} else if task.callback == nil {
			o.persistMu.Lock()
			if o.persistErr == nil {
				o.persistErr = persistErr
			}
			o.persistMu.Unlock()
		}
		if task.callback != nil {
			// Keep user callbacks outside the serial persistence worker. A
			// callback is allowed to close the owner; doing so here would make
			// Close wait on the worker that is currently invoking it.
			run.Go(ctx, "native.owner.persistence-callback", nil, func(context.Context) {
				safeCallback(task.callback, persistErr)
			})
		}
		if task.compact {
			o.requestGarbageCollection()
		}
	}
}

func (o *Owner) persistenceFault() error {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.durabilityFault
}

func (o *Owner) markPersistenceFailure(err error) error {
	if err == nil {
		return nil
	}
	o.mu.Lock()
	if o.durabilityFault == nil {
		o.durabilityFault = fmt.Errorf("%w: %w", ErrPersistenceFailed, err)
	}
	fault := o.durabilityFault
	o.mu.Unlock()
	return fault
}

func safePersist(persist func() error) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("native persistence panic: %v", recovered)
		}
	}()
	return persist()
}

func safeCallback(callback func(error), err error) {
	defer func() { _ = recover() }()
	callback(err)
}

// DurableGeneration returns the newest generation whose persistence completed
// successfully. It is monotonic and does not imply caller-visible query state.
func (o *Owner) DurableGeneration() uint64 {
	if o == nil {
		return 0
	}
	return o.durable.Load()
}

func (o *Owner) validateLease() error {
	if err := o.options.Lease.Validate(); err != nil {
		return err
	}
	if o.options.Path != "" {
		pathLease, ok := o.options.Lease.(PathRootLease)
		if !ok {
			return ErrLeaseUnavailable
		}
		if err := pathLease.ValidatePath(o.options.Path); err != nil {
			return err
		}
	}
	return nil
}

// pruneRootsLocked drops registry entries after their last reader or
// persistence task releases them. The registry itself never owns a root
// reference, so this is safe to call while the owner is shutting down.
func (o *Owner) pruneRootsLocked() {
	for root := range o.roots {
		if root != o.root && root.refs.Load() == 0 {
			delete(o.roots, root)
		}
	}
}

// hasBusyRootsLocked reports whether any root reference could still require
// an obsolete segment. The current owner reference is the only permitted
// reference while garbage collection is preparing its deletion set.
func (o *Owner) hasBusyRootsLocked() bool {
	o.pruneRootsLocked()
	if o.root == nil || o.root.refs.Load() != 1 {
		return true
	}
	for root := range o.roots {
		if root != o.root && root.refs.Load() != 0 {
			return true
		}
	}
	return false
}

func (o *Owner) endOperation() {
	o.mu.Lock()
	o.activeOps--
	o.stateCond.Broadcast()
	o.mu.Unlock()
}

func (o *Owner) persistRoot(root *publishedRoot) error {
	if err := o.validateLease(); err != nil {
		return fmt.Errorf("validate native root lease before persistence: %w", err)
	}
	segments := make([]nativeice.SnapshotSegmentPayload, 0, len(root.segments))
	handles := make([]*segmentHandle, 0, len(root.segments))
	unpersisted := make(map[*segmentHandle]struct{}, len(root.segments))
	for _, current := range root.segments {
		segment, ok := current.(*memorySegment)
		if !ok {
			return fmt.Errorf("persist native root: unsupported segment type %T", current)
		}
		metadata := nativeice.SnapshotSegment{
			ID: segment.handle.id, Size: segment.handle.size, DocumentCount: segment.handle.count,
			TimeMin: segment.handle.timeMin, TimeMax: segment.handle.timeMax,
		}
		if len(segment.deleted) != 0 {
			deleted := roaringpkg.New()
			for number := range segment.deleted {
				if number > uint64(^uint32(0)) {
					return fmt.Errorf("persist deletion %d exceeds mask range: %w", number, ErrInvalidDocument)
				}
				deleted.Add(uint32(number))
			}
			bitmap, marshalErr := deleted.MarshalBinary()
			if marshalErr != nil {
				return fmt.Errorf("persist deletion mask: %w", marshalErr)
			}
			metadata.DeletionBitmap = bitmap
		}
		payload := []byte(nil)
		if !segment.handle.persisted.Load() {
			// The captured root pins this immutable handle for the synchronous
			// publisher call; avoid copying the payload solely for persistence.
			payload = segment.handle.payload
			unpersisted[segment.handle] = struct{}{}
		}
		segments = append(segments, nativeice.SnapshotSegmentPayload{
			SnapshotSegment: metadata, Payload: payload, SourcePath: segment.handle.sourcePath,
			TrustedExisting: payload == nil && segment.handle.sourcePath == "",
		})
		handles = append(handles, segment.handle)
	}
	if err := nativeice.PublishSnapshot(o.options.Path, root.generation, segments); err != nil {
		return err
	}
	for _, handle := range handles {
		handle.persisted.Store(true)
	}
	promotions := make([]persistedPromotion, 0, len(unpersisted))
	for handle := range unpersisted {
		segmentPath := filepath.Join(o.options.Path, fmt.Sprintf("%012x.seg", handle.id))
		reader, openErr := nativeice.OpenSnapshotSegment(segmentPath, nativeice.SnapshotSegment{
			ID: handle.id, Size: handle.size, DocumentCount: handle.count,
			TimeMin: handle.timeMin, TimeMax: handle.timeMax,
		})
		if openErr != nil {
			for _, promotion := range promotions {
				_ = promotion.replacement.reader.Close()
			}
			return fmt.Errorf("open persisted native segment %d for promotion: %w", handle.id, openErr)
		}
		replacement := &segmentHandle{
			reader: reader, count: handle.count, id: handle.id, size: handle.size,
			timeMin: handle.timeMin, timeMax: handle.timeMax, hasTime: handle.hasTime,
			indexedFields: append([]string(nil), handle.indexedFields...),
		}
		replacement.persisted.Store(true)
		promotions = append(promotions, persistedPromotion{original: handle, replacement: replacement})
	}
	o.promotePersistedHandles(promotions)
	return nil
}

func (o *Owner) promotePersistedHandles(promotions []persistedPromotion) {
	if len(promotions) == 0 {
		return
	}
	byOriginal := make(map[*segmentHandle]*segmentHandle, len(promotions))
	for _, promotion := range promotions {
		byOriginal[promotion.original] = promotion.replacement
	}
	o.mu.Lock()
	if o.root == nil || o.closed {
		o.mu.Unlock()
		for _, promotion := range promotions {
			_ = promotion.replacement.reader.Close()
		}
		return
	}
	next := &publishedRoot{
		generation: o.root.generation,
		nextNumber: o.root.nextNumber,
		segments:   append([]rootSegment(nil), o.root.segments...),
	}
	next.refs.Store(1)
	for _, current := range next.segments {
		current.retain()
	}
	used := make(map[*segmentHandle]struct{}, len(promotions))
	for index, current := range next.segments {
		segment, ok := current.(*memorySegment)
		if !ok {
			continue
		}
		replacement, found := byOriginal[segment.handle]
		if !found {
			continue
		}
		current.release()
		replacement.refs.Add(1)
		next.segments[index] = &memorySegment{handle: replacement, deleted: segment.deleted}
		used[replacement] = struct{}{}
	}
	if len(used) == 0 {
		next.release()
		o.mu.Unlock()
		for _, promotion := range promotions {
			_ = promotion.replacement.reader.Close()
		}
		return
	}
	old := o.root
	o.root = next
	o.roots[next] = struct{}{}
	old.release()
	o.pruneRootsLocked()
	o.mu.Unlock()
	for _, promotion := range promotions {
		if _, retained := used[promotion.replacement]; !retained {
			_ = promotion.replacement.reader.Close()
		}
	}
}

// Acquire pins the currently published root without copying its documents.
func (o *Owner) Acquire(ctx context.Context) (*ReadView, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed || o.closing {
		return nil, ErrOwnerClosed
	}
	// Collection retains CURRENT and its segment handles before releasing the
	// lock, so pinning this same root while it unlinks obsolete files is safe.
	if err := o.validateLease(); err != nil {
		return nil, fmt.Errorf("validate native root lease: %w", err)
	}
	o.root.refs.Add(1)
	return &ReadView{owner: o, root: o.root}, nil
}

// Batch admits one operation and publishes the resulting root before starting
// asynchronous persistence. The callback is invoked exactly once, with the
// persistence result; admission errors are reported synchronously and do not
// publish a root.
func (o *Owner) Batch(ctx context.Context, batch Batch) error {
	if err := ctx.Err(); err != nil {
		return finishCallback(batch.PersistentCallback, err)
	}
	queue := o.persistQ
	reserved := queue != nil
	if reserved && !queue.reserve() {
		return finishCallback(batch.PersistentCallback, ErrPersistenceBackpressure)
	}
	o.mu.Lock()
	if o.closed || o.closing {
		o.mu.Unlock()
		if reserved {
			queue.releaseReservation()
		}
		return finishCallback(batch.PersistentCallback, ErrOwnerClosed)
	}
	// Collection only removes files below its captured root's keep set. New
	// admissions publish fresh segment IDs and therefore cannot be collected.
	if o.durabilityFault != nil {
		fault := o.durabilityFault
		o.mu.Unlock()
		if reserved {
			queue.releaseReservation()
		}
		return finishCallback(batch.PersistentCallback, fault)
	}
	if err := o.validateLease(); err != nil {
		o.mu.Unlock()
		if reserved {
			queue.releaseReservation()
		}
		return finishCallback(batch.PersistentCallback, fmt.Errorf("validate native root lease: %w", err))
	}
	if queue == nil && batch.PersistentCallback != nil {
		o.mu.Unlock()
		return finishCallback(batch.PersistentCallback, ErrPersistenceConfiguration)
	}
	next, err := o.publishBatchLocked(batch)
	if err == nil {
		old := o.root
		o.root = next
		old.release() // release the owner-held reference to the old root
		o.pruneRootsLocked()
	}
	if err != nil {
		o.mu.Unlock()
		if reserved {
			queue.releaseReservation()
		}
		return finishCallback(batch.PersistentCallback, err)
	}
	o.roots[next] = struct{}{}
	if queue == nil {
		o.mu.Unlock()
		return finishCallback(batch.PersistentCallback, nil)
	}
	// Pin before the bounded send while the publication lock is held. This
	// prevents Close from releasing the owner reference before the worker owns
	// this task; the worker releases this pin after persistence completes.
	next.refs.Add(1)
	queue.tasks <- persistenceTask{root: next, callback: batch.PersistentCallback}
	o.mu.Unlock()
	o.requestMaintenance()
	return nil
}

// Compact merges the pinned root's immutable segments outside the admission
// lock, then publishes the result only if no newer root appeared meanwhile.
// A stale build is discarded, leaving all old segment files and references
// untouched for readers and prior manifests.
//
//nolint:gocyclo // admission, merge, and stale-root checks are kept explicit.
func (o *Owner) Compact(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	queue := o.persistQ
	reserved := queue != nil
	if reserved && !queue.reserve() {
		return ErrPersistenceBackpressure
	}
	o.mu.Lock()
	if o.closed || o.closing {
		o.mu.Unlock()
		if reserved {
			queue.releaseReservation()
		}
		return ErrOwnerClosed
	}
	if o.collecting {
		o.mu.Unlock()
		if reserved {
			queue.releaseReservation()
		}
		return ErrPersistenceBusy
	}
	if o.durabilityFault != nil {
		fault := o.durabilityFault
		o.mu.Unlock()
		if reserved {
			queue.releaseReservation()
		}
		return fault
	}
	if err := o.validateLease(); err != nil {
		o.mu.Unlock()
		if reserved {
			queue.releaseReservation()
		}
		return fmt.Errorf("validate native root lease: %w", err)
	}
	o.activeOps++
	defer o.endOperation()
	base := o.root
	base.refs.Add(1)
	baseGeneration := base.generation
	segmentID := o.nextSegmentID
	if segmentID == ^uint64(0) {
		base.release()
		o.mu.Unlock()
		if reserved {
			queue.releaseReservation()
		}
		return fmt.Errorf("native segment identifiers exhausted: %w", ErrInvalidDocument)
	}
	o.mu.Unlock()

	inputs := make([]nativeice.MergeInput, 0, len(base.segments))
	for _, current := range base.segments {
		segment, ok := current.(*memorySegment)
		if !ok {
			base.release()
			if reserved {
				queue.releaseReservation()
			}
			return fmt.Errorf("compact native root: unsupported segment type %T", current)
		}
		drop := roaringpkg.New()
		for number := range segment.deleted {
			if number <= uint64(^uint32(0)) {
				drop.Add(uint32(number))
			}
		}
		inputs = append(inputs, nativeice.MergeInput{Reader: segment.handle.reader, Drop: drop, IndexedFields: segment.handle.indexedFields})
	}
	merged, mergeErr := nativeice.MergeSegments(ctx, inputs)
	base.release()
	if mergeErr != nil {
		if reserved {
			queue.releaseReservation()
		}
		return mergeErr
	}
	if err := ctx.Err(); err != nil {
		if reserved {
			queue.releaseReservation()
		}
		return err
	}

	o.mu.Lock()
	if o.closed || o.closing {
		o.mu.Unlock()
		if reserved {
			queue.releaseReservation()
		}
		return ErrOwnerClosed
	}
	if o.root.generation != baseGeneration {
		o.mu.Unlock()
		if reserved {
			queue.releaseReservation()
		}
		return ErrStaleCompaction
	}
	var compacted rootSegment
	if len(merged.Payload) != 0 {
		compacted, mergeErr = newSegmentFromPayload(merged.Payload, segmentID)
		if mergeErr != nil {
			o.mu.Unlock()
			if reserved {
				queue.releaseReservation()
			}
			return mergeErr
		}
	}
	next := &publishedRoot{generation: baseGeneration + 1, nextNumber: 0, refs: atomic.Int64{}}
	next.refs.Store(1)
	if compacted != nil {
		next.segments = []rootSegment{compacted}
		next.nextNumber = compacted.Len()
	}
	old := o.root
	o.root = next
	o.roots[next] = struct{}{}
	o.nextSegmentID++
	old.release()
	o.pruneRootsLocked()
	if queue != nil {
		next.refs.Add(1)
		queue.tasks <- persistenceTask{root: next, compact: true}
	}
	o.mu.Unlock()
	o.requestMaintenance()
	return nil
}

// CollectGarbage removes obsolete owner-created manifests and segments after
// validating the newest committed snapshot. It never removes unknown files,
// newer uncertain snapshots, the current root, or a root with active readers.
//
//nolint:gocyclo // conservative validation and deletion ordering are explicit.
func (o *Owner) CollectGarbage(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if o == nil || o.options.Path == "" {
		return ErrPersistenceConfiguration
	}
	o.mu.Lock()
	if o.closed || o.closing {
		o.mu.Unlock()
		return ErrOwnerClosed
	}
	if o.collecting {
		o.mu.Unlock()
		return ErrPersistenceBusy
	}
	if err := o.validateLease(); err != nil {
		o.mu.Unlock()
		return fmt.Errorf("validate native root lease: %w", err)
	}
	// Every root, not just the current root, is checked. An older root can be
	// retained by a read view or by a persistence task after it is no longer
	// current; deleting its segment here would invalidate that owner.
	if o.hasBusyRootsLocked() {
		o.mu.Unlock()
		return ErrPersistenceBusy
	}
	o.collecting = true
	o.activeOps++
	root := o.root
	root.refs.Add(1)
	o.mu.Unlock()
	defer func() {
		o.mu.Lock()
		o.collecting = false
		o.activeOps--
		o.pruneRootsLocked()
		o.stateCond.Broadcast()
		o.mu.Unlock()
	}()
	defer root.release()

	latest, openErr := nativeice.OpenStrict(o.options.Path)
	if openErr != nil {
		return openErr
	}
	metadata := latest.SnapshotMetadata()
	if closeErr := latest.Close(); closeErr != nil {
		return closeErr
	}
	keepSegments := make(map[uint64]struct{}, len(metadata.Segments)+len(root.segments))
	for _, segment := range metadata.Segments {
		keepSegments[segment.ID] = struct{}{}
	}
	maxKeep := uint64(0)
	for _, current := range root.segments {
		segment, ok := current.(*memorySegment)
		if !ok {
			return fmt.Errorf("collect native root: unsupported segment type %T", current)
		}
		keepSegments[segment.handle.id] = struct{}{}
		if segment.handle.id > maxKeep {
			maxKeep = segment.handle.id
		}
	}
	for segmentID := range keepSegments {
		if segmentID > maxKeep {
			maxKeep = segmentID
		}
	}
	entries, readErr := os.ReadDir(o.options.Path)
	if readErr != nil {
		return fmt.Errorf("read native owner directory: %w", readErr)
	}
	removeSnapshots := make([]string, 0)
	removeSegments := make([]string, 0)
	for _, entry := range entries {
		if !entry.Type().IsRegular() {
			continue
		}
		name := entry.Name()
		extension := filepath.Ext(name)
		if extension != ".snp" && extension != ".seg" {
			continue
		}
		identifier, parseErr := strconv.ParseUint(name[:len(name)-len(extension)], 16, 64)
		if parseErr != nil {
			continue
		}
		if extension == ".snp" {
			if identifier < metadata.ID {
				removeSnapshots = append(removeSnapshots, filepath.Join(o.options.Path, name))
			}
			continue
		}
		if identifier < maxKeep {
			if _, keep := keepSegments[identifier]; !keep {
				removeSegments = append(removeSegments, filepath.Join(o.options.Path, name))
			}
		}
	}
	for _, path := range removeSnapshots {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := os.Remove(path); err != nil {
			return fmt.Errorf("remove obsolete snapshot %q: %w", path, err)
		}
	}
	// A manifest is the crash-consistency boundary for segment reachability.
	// Ensure obsolete manifests are durable before unlinking any segment they
	// may have referenced.
	if err := syncOwnerDirectory(o.options.Path); err != nil {
		return err
	}
	for _, path := range removeSegments {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := os.Remove(path); err != nil {
			return fmt.Errorf("remove obsolete segment %q: %w", path, err)
		}
	}
	return syncOwnerDirectory(o.options.Path)
}

func syncOwnerDirectory(path string) error {
	directory, openErr := os.Open(path)
	if openErr != nil {
		return openErr
	}
	syncErr := directory.Sync()
	closeErr := directory.Close()
	return errors.Join(syncErr, closeErr)
}

// Collect is the concise alias for CollectGarbage.
func (o *Owner) Collect(ctx context.Context) error { return o.CollectGarbage(ctx) }

func (o *Owner) publishBatchLocked(batch Batch) (*publishedRoot, error) {
	if o.root.generation == ^uint64(0) {
		return nil, fmt.Errorf("native generation exhausted: %w", ErrInvalidDocument)
	}
	for documentIndex, document := range batch.Documents {
		for _, field := range document.Fields {
			if field.Name == identifierField || field.Name == timestampField {
				return nil, fmt.Errorf("document %d contains reserved field %q: %w", documentIndex, field.Name, ErrInvalidDocument)
			}
		}
	}
	next := &publishedRoot{
		generation: o.root.generation + 1,
		segments:   append([]rootSegment(nil), o.root.segments...),
		nextNumber: o.root.nextNumber,
	}
	next.refs.Store(1) // owner-held reference
	for _, current := range next.segments {
		current.retain()
	}
	for _, identifier := range batch.Deletes {
		if len(identifier) == 0 {
			releaseSegments(next.segments)
			return nil, fmt.Errorf("delete has no identifier: %w", ErrInvalidDocument)
		}
		for segmentIndex, current := range next.segments {
			updated, changed, deleteErr := current.Delete(identifier)
			if deleteErr != nil {
				releaseSegments(next.segments)
				return nil, deleteErr
			}
			if changed {
				current.release()
				next.segments[segmentIndex] = updated
			}
		}
	}
	if len(batch.Documents) == 0 {
		return next, nil
	}
	newDocuments := make([]Document, 0, len(batch.Documents))
	newDocumentIndexes := make(map[string]int, len(batch.Documents))
	for documentIndex, document := range batch.Documents {
		if len(document.Identifier) == 0 {
			releaseSegments(next.segments)
			return nil, fmt.Errorf("document %d has no identifier: %w", documentIndex, ErrInvalidDocument)
		}
		if !batch.InsertOnly {
			for segmentIndex, current := range next.segments {
				updated, changed, deleteErr := current.Delete(document.Identifier)
				if deleteErr != nil {
					releaseSegments(next.segments)
					return nil, deleteErr
				}
				if changed {
					current.release()
					next.segments[segmentIndex] = updated
				}
			}
		}
		key := string(document.Identifier)
		if !batch.InsertOnly {
			if previousIndex, found := newDocumentIndexes[key]; found {
				// A batch is one admission boundary: retain only the last physical
				// value for an identifier so it cannot publish two live updates.
				newDocuments[previousIndex] = cloneDocument(document)
				continue
			}
			newDocumentIndexes[key] = len(newDocuments)
		}
		newDocuments = append(newDocuments, cloneDocument(document))
	}
	if o.nextSegmentID == ^uint64(0) {
		releaseSegments(next.segments)
		return nil, fmt.Errorf("native segment identifiers exhausted: %w", ErrInvalidDocument)
	}
	newSegment, segmentErr := newMemorySegment(newDocuments, o.nextSegmentID)
	if segmentErr != nil {
		releaseSegments(next.segments)
		return nil, segmentErr
	}
	if uint64(len(newDocuments)) > ^uint64(0)-next.nextNumber {
		releaseSegments(next.segments)
		newSegment.release()
		return nil, fmt.Errorf("native document numbering exhausted: %w", ErrInvalidDocument)
	}
	o.nextSegmentID++
	next.segments = append(next.segments, newSegment)
	next.nextNumber += uint64(len(newDocuments))
	return next, nil
}

func releaseSegments(segments []rootSegment) {
	for _, current := range segments {
		current.release()
	}
}

func newMemorySegment(documents []Document, segmentID uint64) (rootSegment, error) {
	encoded := nativeice.Generation{Documents: make([]nativeice.EncodeDocument, 0, len(documents))}
	var timeMin, timeMax uint64
	hasTime := false
	indexedFieldSet := make(map[string]struct{})
	for documentIndex, document := range documents {
		if len(document.Identifier) == 0 {
			return nil, fmt.Errorf("document %d has no identifier: %w", documentIndex, ErrInvalidDocument)
		}
		nativeDocument := nativeice.EncodeDocument{Identifier: bytes.Clone(document.Identifier)}
		for _, field := range document.Fields {
			if field.Index {
				indexedFieldSet[field.Name] = struct{}{}
			}
			var terms []nativeice.EncodeTerm
			if field.Terms != nil {
				terms = make([]nativeice.EncodeTerm, len(field.Terms))
				for termIndex, term := range field.Terms {
					terms[termIndex] = nativeice.EncodeTerm{Value: bytes.Clone(term.Value), Frequency: term.Frequency}
				}
			}
			nativeDocument.Fields = append(nativeDocument.Fields, nativeice.EncodeField{
				Name: field.Name, Value: bytes.Clone(field.Value), Terms: terms,
				Store: field.Store, Index: field.Index, Sort: field.Sort,
			})
		}
		if document.Timestamp != 0 {
			encodedTimestamp := uint64(document.Timestamp)
			if !hasTime || encodedTimestamp < timeMin {
				timeMin = encodedTimestamp
			}
			if !hasTime || encodedTimestamp > timeMax {
				timeMax = encodedTimestamp
			}
			hasTime = true
			timestamp := nativeice.EncodePrefixCodedInt64(document.Timestamp)
			timestampTerms := make([]nativeice.EncodeTerm, 0, 16)
			for shift := uint(0); shift <= 60; shift += 4 {
				timestampTerms = append(timestampTerms, nativeice.EncodeTerm{
					Value: nativeice.EncodePrefixCodedInt64Shift(document.Timestamp, shift), Frequency: 1,
				})
			}
			nativeDocument.Fields = append(nativeDocument.Fields, nativeice.EncodeField{
				Name: timestampField, Value: timestamp, Terms: timestampTerms, Store: true, Index: true, Sort: true,
			})
		}
		nativeDocument.Deleted = false
		encoded.Documents = append(encoded.Documents, nativeDocument)
	}
	encoded.TimeMin, encoded.TimeMax = timeMin, timeMax
	payload, encodeErr := nativeice.EncodeSegment(encoded)
	if encodeErr != nil {
		return nil, encodeErr
	}
	reader, openErr := nativeice.OpenSegmentBorrowed(payload)
	if openErr != nil {
		return nil, openErr
	}
	handle := &segmentHandle{reader: reader, payload: payload, count: uint64(len(documents)), id: segmentID, size: uint64(len(payload))}
	for fieldName := range indexedFieldSet {
		handle.indexedFields = append(handle.indexedFields, fieldName)
	}
	sort.Strings(handle.indexedFields)
	for _, document := range documents {
		if document.Timestamp == 0 {
			continue
		}
		encodedTimestamp := uint64(document.Timestamp)
		if !handle.hasTime || encodedTimestamp < handle.timeMin {
			handle.timeMin = encodedTimestamp
		}
		if !handle.hasTime || encodedTimestamp > handle.timeMax {
			handle.timeMax = encodedTimestamp
		}
		handle.hasTime = true
	}
	handle.refs.Store(1)
	return &memorySegment{handle: handle}, nil
}

func newSegmentFromPayload(payload []byte, segmentID uint64) (rootSegment, error) {
	reader, openErr := nativeice.OpenSegmentBorrowed(payload)
	if openErr != nil {
		return nil, openErr
	}
	handle := &segmentHandle{
		reader: reader, payload: bytes.Clone(payload), count: reader.DocumentCount(),
		id: segmentID, size: uint64(len(payload)), persisted: atomic.Bool{},
	}
	handle.refs.Store(1)
	if fields, fieldsErr := reader.Fields(); fieldsErr == nil {
		handle.indexedFields = fields
	}
	return &memorySegment{handle: handle}, nil
}

func newSegmentFromFile(path string, metadata nativeice.SnapshotSegment, segmentID uint64) (rootSegment, error) {
	reader, openErr := nativeice.OpenSnapshotSegment(path, metadata)
	if openErr != nil {
		return nil, openErr
	}
	fields, fieldsErr := reader.Fields()
	if fieldsErr != nil {
		_ = reader.Close()
		return nil, fieldsErr
	}
	timeMin, timeMax := reader.TimeBounds()
	handle := &segmentHandle{
		reader: reader, sourcePath: path, count: metadata.DocumentCount, id: segmentID,
		size: metadata.Size, timeMin: uint64(timeMin), timeMax: uint64(timeMax),
		hasTime: timeMin != 0 || timeMax != 0, indexedFields: fields,
	}
	handle.refs.Store(1)
	return &memorySegment{handle: handle}, nil
}

func cloneSegmentWithDelete(source *memorySegment, documentIndex uint64) *memorySegment {
	deleted := make(map[uint64]struct{}, len(source.deleted)+1)
	for number := range source.deleted {
		deleted[number] = struct{}{}
	}
	deleted[documentIndex] = struct{}{}
	source.handle.refs.Add(1)
	return &memorySegment{handle: source.handle, deleted: deleted}
}

//nolint:contextcheck // nativeice performs a bounded synchronous dictionary lookup.
func (s *memorySegment) Delete(identifier []byte) (rootSegment, bool, error) {
	posting, found, err := s.handle.reader.TermPosting(identifierField, identifier)
	if err != nil || !found {
		return s, false, err
	}
	changed := false
	ownedClone := false
	for _, documentIndex := range postingDocuments(posting) {
		if _, deleted := s.deleted[documentIndex]; deleted {
			continue
		}
		updated := cloneSegmentWithDelete(s, documentIndex)
		if ownedClone {
			s.release()
		}
		s = updated
		ownedClone = true
		changed = true
	}
	return s, changed, nil
}

//nolint:contextcheck // nativeice exact posting lookup is bounded and synchronous.
func (s *memorySegment) Lookup(ctx context.Context, identifier []byte) (Document, bool, error) {
	posting, found, err := s.handle.reader.TermPosting(identifierField, identifier)
	if err != nil || !found {
		return Document{}, false, err
	}
	for _, documentNumber := range postingDocuments(posting) {
		if err := ctx.Err(); err != nil {
			return Document{}, false, err
		}
		if _, deleted := s.deleted[documentNumber]; deleted {
			continue
		}
		var result Document
		var timestampErr error
		visitErr := s.handle.reader.VisitDocument(documentNumber, func(document nativeice.StoredDocument) error {
			return document.VisitStoredFields(func(name string, value []byte) bool {
				if name == identifierField {
					result.Identifier = bytes.Clone(value)
					return true
				}
				if name == timestampField {
					result.Timestamp, timestampErr = nativeice.DecodePrefixCodedInt64(value)
					return timestampErr == nil
				}
				if name == seriesIDField || name == versionField {
					return true
				}
				result.Fields = append(result.Fields, Field{Name: name, Value: bytes.Clone(value), Store: true})
				return true
			})
		})
		if visitErr != nil {
			return Document{}, false, visitErr
		}
		if timestampErr != nil {
			return Document{}, false, fmt.Errorf("decode timestamp: %w", timestampErr)
		}
		return result, true, nil
	}
	return Document{}, false, nil
}

//nolint:contextcheck // nativeice iterator cancellation is checked between entries here.
func (s *memorySegment) VisitIdentifiers(ctx context.Context, visit func([]byte) bool) error {
	iterator, err := s.handle.reader.NewDictionaryTermIterator(identifierField, nil, nil, nil)
	if err != nil {
		return err
	}
	defer func() { _ = iterator.Close() }()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		identifier, err := iterator.NextTerm()
		if err != nil || identifier == nil {
			return err
		}
		if !visit(identifier) {
			return nil
		}
	}
}

//nolint:contextcheck // nativeice exact posting lookup is bounded and synchronous.
func (s *memorySegment) MatchTerms(ctx context.Context, request MatchRequest) (MatchResult, error) {
	posting, found, err := s.handle.reader.TermPosting(request.Field, request.Term)
	if err != nil || !found {
		return MatchResult{}, err
	}
	var series map[uint64]struct{}
	if request.SeriesField != "" {
		seriesPosting, seriesFound, seriesErr := s.handle.reader.TermPosting(request.SeriesField, request.SeriesID)
		if seriesErr != nil || !seriesFound {
			return MatchResult{}, seriesErr
		}
		series = make(map[uint64]struct{})
		for _, number := range postingDocuments(seriesPosting) {
			series[number] = struct{}{}
		}
	}
	result := MatchResult{}
	for _, documentNumber := range postingDocuments(posting) {
		if err := ctx.Err(); err != nil {
			return MatchResult{}, err
		}
		if _, deleted := s.deleted[documentNumber]; deleted {
			continue
		}
		if series != nil {
			if _, found := series[documentNumber]; !found {
				continue
			}
		}
		var identifier []byte
		var timestamp int64
		var hasTimestamp bool
		var timestampErr error
		visitErr := s.handle.reader.VisitDocument(documentNumber, func(document nativeice.StoredDocument) error {
			return document.VisitStoredFields(func(name string, value []byte) bool {
				if name == identifierField {
					identifier = bytes.Clone(value)
					return true
				}
				if name == timestampField {
					timestamp, timestampErr = nativeice.DecodePrefixCodedInt64(value)
					if timestampErr == nil {
						hasTimestamp = true
					}
					if timestampErr != nil {
						return false
					}
				}
				return true
			})
		})
		if visitErr != nil {
			return MatchResult{}, visitErr
		}
		if timestampErr != nil {
			return MatchResult{}, fmt.Errorf("decode timestamp: %w", timestampErr)
		}
		if identifier == nil {
			return MatchResult{}, fmt.Errorf("document %d has no identifier: %w", documentNumber, ErrInvalidDocument)
		}
		if request.TimeRange != nil && (!hasTimestamp || !request.TimeRange.contains(timestamp)) {
			continue
		}
		result.Identifiers = append(result.Identifiers, identifier)
		result.Timestamps = append(result.Timestamps, timestamp)
	}
	return result, nil
}

func (s *memorySegment) Len() uint64 { return s.handle.count }

func (s *memorySegment) retain() { s.handle.refs.Add(1) }

func (s *memorySegment) release() {
	if s.handle.refs.Add(-1) == 0 {
		_ = s.handle.reader.Close()
		if s.handle.sourcePath != "" {
			_ = os.Remove(s.handle.sourcePath)
			s.handle.sourcePath = ""
		}
		s.handle.payload = nil
	}
}

func postingDocuments(posting nativeice.TermPosting) []uint64 {
	if posting.OneHit {
		return []uint64{posting.DocumentNumber}
	}
	return posting.Documents
}

func finishCallback(callback func(error), err error) error {
	if callback != nil {
		callback(err)
	}
	return err
}

// Generation returns the immutable generation number pinned by the view.
func (v *ReadView) Generation() uint64 {
	if v == nil || v.root == nil {
		return 0
	}
	return v.root.generation
}

// Lookup returns a copied live document by external identifier.
func (v *ReadView) Lookup(ctx context.Context, identifier []byte) (Document, bool, error) {
	if err := v.check(ctx); err != nil {
		return Document{}, false, err
	}
	for _, current := range v.root.segments {
		document, found, err := current.Lookup(ctx, identifier)
		if err != nil {
			return Document{}, false, err
		}
		if found {
			return document, true, nil
		}
	}
	return Document{}, false, nil
}

// VisitIdentifiers enumerates physical identifiers in snapshot order,
// including deleted metadata entries. Returning false stops without error.
func (v *ReadView) VisitIdentifiers(ctx context.Context, visit func([]byte) bool) error {
	if err := v.check(ctx); err != nil {
		return err
	}
	for _, current := range v.root.segments {
		stopped := false
		err := current.VisitIdentifiers(ctx, func(identifier []byte) bool {
			if !visit(identifier) {
				stopped = true
				return false
			}
			return true
		})
		if err != nil {
			return err
		}
		if stopped {
			return nil
		}
	}
	return nil
}

// MatchRequest describes an exact indexed term plus optional series/time
// constraints. Names and terms are already encoded by the caller.
//
//nolint:govet // request fields mirror the neutral native query contract.
type MatchRequest struct {
	Field       string
	Term        []byte
	SeriesField string
	SeriesID    []byte
	TimeRange   *TimeRange
}

// TimeRange is an inclusive/exclusive timestamp interval.
type TimeRange struct {
	Lower         int64
	Upper         int64
	IncludesLower bool
	IncludesUpper bool
}

// MatchResult returns copied external identifiers and timestamps. Physical
// segment ordinals are an implementation detail and never escape this API.
type MatchResult struct {
	Identifiers [][]byte
	Timestamps  []int64
}

// MatchTerms performs exact membership against the pinned immutable root.
// Segment-local adapters can replace the in-memory matcher with dictionary and
// posting cursors without changing this read-view contract.
func (v *ReadView) MatchTerms(ctx context.Context, request MatchRequest) (MatchResult, error) {
	if err := v.check(ctx); err != nil {
		return MatchResult{}, err
	}
	result := MatchResult{}
	for _, current := range v.root.segments {
		segmentResult, err := current.MatchTerms(ctx, request)
		if err != nil {
			return MatchResult{}, err
		}
		for index, identifier := range segmentResult.Identifiers {
			result.Identifiers = append(result.Identifiers, bytes.Clone(identifier))
			result.Timestamps = append(result.Timestamps, segmentResult.Timestamps[index])
		}
	}
	return result, nil
}

func (v *ReadView) check(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if v == nil || v.root == nil || v.closed.Load() {
		return ErrViewClosed
	}
	return nil
}

// Close releases the pinned root reference exactly once.
func (v *ReadView) Close() error {
	if v == nil || v.closed.Swap(true) || v.root == nil {
		return nil
	}
	v.root.release()
	if v.owner != nil {
		v.owner.requestGarbageCollection()
	}
	return nil
}

// Close stops new admissions and waits for owner-started persistence calls.
// Existing views remain valid until their own Close.
func (o *Owner) Close() error {
	if o == nil {
		return nil
	}
	o.mu.Lock()
	if o.closed {
		closeErr := o.closeErr
		o.mu.Unlock()
		return closeErr
	}
	if o.closing {
		for !o.closed {
			o.stateCond.Wait()
		}
		closeErr := o.closeErr
		o.mu.Unlock()
		return closeErr
	}
	// Prevent new admissions, then let an in-flight compaction or collection
	// finish before releasing the owner-held root and lease responsibility.
	o.closing = true
	for o.activeOps != 0 {
		o.stateCond.Wait()
	}
	oldRoot := o.root
	o.root = nil
	oldRoot.release()
	o.pruneRootsLocked()
	queue := o.persistQ
	maintenanceCancel := o.maintenanceCancel
	maintenanceTask := o.maintenanceTask
	if queue != nil {
		close(queue.tasks)
	}
	o.mu.Unlock()
	if maintenanceCancel != nil {
		maintenanceCancel()
	}
	if maintenanceTask != nil {
		<-maintenanceTask.Done()
	}
	o.abortExternalStreamers()
	if queue != nil {
		<-queue.done
	}
	o.persistMu.Lock()
	persistErr := o.persistErr
	o.persistMu.Unlock()
	o.mu.Lock()
	o.closeErr = persistErr
	o.closed = true
	o.stateCond.Broadcast()
	o.mu.Unlock()
	return persistErr
}

func (r TimeRange) contains(timestamp int64) bool {
	if timestamp < r.Lower || timestamp > r.Upper {
		return false
	}
	if timestamp == r.Lower && !r.IncludesLower || timestamp == r.Upper && !r.IncludesUpper {
		return false
	}
	return true
}

func cloneDocument(document Document) Document {
	clone := Document{Identifier: bytes.Clone(document.Identifier), Timestamp: document.Timestamp}
	clone.Fields = make([]Field, len(document.Fields))
	for fieldIndex, field := range document.Fields {
		clone.Fields[fieldIndex] = Field{Name: field.Name, Value: bytes.Clone(field.Value), Store: field.Store, Index: field.Index, Sort: field.Sort}
		if field.Terms != nil {
			clone.Fields[fieldIndex].Terms = make([]Term, len(field.Terms))
			for termIndex, term := range field.Terms {
				clone.Fields[fieldIndex].Terms[termIndex] = Term{Value: bytes.Clone(term.Value), Frequency: term.Frequency}
			}
		}
	}
	return clone
}
