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
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	roaringpkg "github.com/RoaringBitmap/roaring"

	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/index/internal/nativeice"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

// fileSystem performs the owner's file operations. It is stateless, so one
// package-level instance serves every owner.
var fileSystem = fs.NewLocalFileSystem()

var (
	// ErrOwnerClosed reports a mutation or acquire after owner shutdown.
	ErrOwnerClosed = errors.New("native: owner is closed")
	// ErrViewClosed reports a read through a released pinned view.
	ErrViewClosed = errors.New("native: view is closed")
	// ErrInvalidDocument reports a mutation that cannot be admitted.
	ErrInvalidDocument = errors.New("native: invalid document")
	// ErrLeaseUnavailable reports a missing or invalid database ownership lease.
	ErrLeaseUnavailable = errors.New("native: root lease unavailable")
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

// BatchMode selects how Batch treats a document whose identifier may already
// be live elsewhere in the owner.
type BatchMode uint8

const (
	// BatchUpsert replaces any live document sharing an identifier. This is
	// the default (zero value) and Batch's original, only behavior.
	BatchUpsert BatchMode = iota
	// BatchInsertOnly preserves every physical document, including duplicate
	// identifiers, admitting them as new physical documents rather than
	// replacing an earlier live one.
	BatchInsertOnly
	// BatchInsertIfAbsent skips a document when some segment already has a
	// live posting for its identifier and that segment's field-name set
	// contains every field name the document carries; otherwise it upserts
	// exactly like BatchUpsert. The check runs under the owner lock against
	// the admission root, so it is serialized with every other admission.
	BatchInsertIfAbsent
)

// Batch is one serialized admission operation. Deletes are applied before
// Documents, so an update cannot leave two live physical documents.
//
//nolint:govet // mutation fields are grouped by ownership and callback role.
type Batch struct {
	Documents []Document
	Deletes   [][]byte
	// Mode selects upsert, insert-only, or insert-if-absent admission for
	// Documents. The default zero value is BatchUpsert.
	Mode               BatchMode
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

// revocableLease is implemented by leases that can report revocation without
// touching the filesystem. Pinning a read view only needs that: the view reads
// an already published immutable root and writes nothing the lease protects,
// so the full Validate (a stat on the lock file) is reserved for mutations.
type revocableLease interface {
	Revoked() bool
}

// PersistFunc receives a pinned immutable view after in-memory publication.
// It must persist only the new immutable segments and publication metadata;
// it must not mutate the view. The owner releases the view after the callback.
type PersistFunc func(context.Context, *ReadView) error

// MergeDocument describes one visible physical document while a root is being
// compacted. StoredFields is borrowed for the duration of the callback; a
// callback must copy values it retains. Returning true adds the document to
// the private merge drop set. Existing root deletion masks are applied before
// this callback is invoked.
//
//nolint:govet // the callback payload keeps identity, deletion, and borrowed-field access together.
type MergeDocument struct {
	SegmentID      uint64
	DocumentNumber uint64
	StoredFields   func(func(name string, value []byte) bool) error
}

// PrepareMergeCallback supplies product-specific expiry/tombstone policy
// without exposing ICE readers or allowing a callback to mutate a published
// root. It runs synchronously while Compact owns a pinned immutable root.
type PrepareMergeCallback func(context.Context, MergeDocument) (drop bool, err error)

// ExternalDedupMode controls how introducing an external segment
// (EnableExternalSegments) treats an identifier the incoming segment shares
// with one already live elsewhere in the owner.
type ExternalDedupMode uint8

const (
	// ExternalDedupNone introduces every incoming document unconditionally.
	// Neither copy is masked, so a shared identifier ends up live in more than
	// one segment. This is the default and preserves the legacy native
	// receiver's append-only semantics.
	ExternalDedupNone ExternalDedupMode = iota
	// ExternalDedupPreferIncoming masks the existing live copy of every
	// identifier the incoming segment also carries, so the incoming document
	// wins. This is today's DeduplicateExternal=true behavior.
	ExternalDedupPreferIncoming
	// ExternalDedupKeepExisting masks an incoming document whose identifier is
	// already live elsewhere, in the incoming segment's own deletion bitmap,
	// leaving every existing segment untouched, so the existing document wins.
	ExternalDedupKeepExisting
)

// OwnerOptions supplies database ownership and asynchronous persistence.
//
//nolint:govet // option fields are grouped by documented purpose, not padding.
type OwnerOptions struct {
	Lease                RootLease
	Persist              PersistFunc
	PrepareMergeCallback PrepareMergeCallback
	// Path enables the built-in ICE snapshot publisher. An empty path keeps the
	// owner in memory-only mode unless Persist is supplied for a test seam.
	Path string
	// CompactionThreshold schedules one serialized background compaction after
	// this many immutable segments are published. Zero uses a conservative
	// default; a negative value disables scheduling for maintenance tests.
	CompactionThreshold int
	// ExternalDedup controls whether external segment introduction masks
	// existing or incoming identifiers. It defaults to ExternalDedupNone to
	// preserve legacy receiver semantics; callers that need deduplication opt
	// into one of the other modes explicitly.
	ExternalDedup ExternalDedupMode
	// PersistInterval is the minimum time between two background persists.
	// Zero persists as soon as an admission wakes the worker. A positive
	// interval lets compaction merge a write burst's small segments in memory
	// before they are written, like the legacy engine's persister nap, so
	// callers that wait on PersistentCallback should leave it zero. Close
	// persists everything admitted regardless.
	PersistInterval time.Duration
	// PresenceCacheBytes bounds the memory an InsertIfAbsent presence cache
	// may use to remember "identifier present with this field set" per root
	// generation. Zero or negative disables the cache; every InsertIfAbsent
	// admission then recomputes presence from the admission root.
	PresenceCacheBytes int
	// IdentifierDocValues makes the encoder and merger write "_id" as a
	// doc-value column in addition to its term, matching the layout the
	// previous release's writer produces so a rolled-back node can still read
	// document identity back from a segment this owner wrote.
	IdentifierDocValues bool
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
	roots         map[*publishedRoot]struct{}
	options       OwnerOptions
	closed        bool
	closing       bool
	closeErr      error
	collecting    bool
	activeOps     int
	nextSegmentID uint64
	// admittedIdentifiers maps each identifier written by Batch to the IDs of
	// the admitted segments holding it, and admittedSegments each such
	// segment ID to its identifiers. An upsert probes only the indexed
	// segments that hold its identifier, plus every segment the index does
	// not cover (merge results, segments loaded at startup, external
	// segments), instead of every live segment. Entries leave when a merge
	// removes their segment from the root. Guarded by mu.
	admittedIdentifiers map[string][]uint64
	admittedSegments    map[uint64][][]byte
	persistQ            *persistenceQueue
	persistenceCancel   context.CancelFunc
	persistenceTask     *run.Task
	durable             atomic.Uint64
	persistMu           sync.Mutex
	persistErr          error
	// pendingCallbacks accumulates every PersistentCallback admitted since the
	// persistence worker's last successful (or failed) flush. Multiple
	// admissions that land before the worker gets a turn share the single
	// persist call that eventually flushes o.root, so their callbacks all
	// fire together off that one call rather than one each.
	pendingCallbacks []func(error)
	// pendingCompacted records whether a compaction published a root since
	// the last flush, so the post-flush garbage-collection wake (see
	// runPersistence) still fires even though it is no longer tied to one
	// specific persistenceTask.
	pendingCompacted     bool
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
	// presenceCache memoizes InsertIfAbsent presence decisions per root
	// generation. Nil when OwnerOptions.PresenceCacheBytes is not positive.
	presenceCache *presenceCache
}

// persistenceQueue wakes the background persistence worker. It is not a work
// queue: the worker always persists whatever o.root currently is when it
// gets a turn, not a queued copy of any specific admission, so any number of
// wakes that land while it is busy collapse into the single persist call it
// performs once free. This mirrors this project's legacy engine's persister
// loop, which always
// flushes its writer's latest snapshot rather than draining a backlog of
// every intermediate one.
type persistenceQueue struct {
	wake chan struct{}
}

func (q *persistenceQueue) requestPersist() {
	select {
	case q.wake <- struct{}{}:
	default:
	}
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
	reader  *nativeice.Reader
	payload []byte
	// sourcePath is the staged file a merge output or an external receive
	// is served from until publication renames it to "<id>.seg"; it is empty
	// afterwards, and for every other segment. pathMu orders persistence's
	// rename against the release that deletes an unpublished staged file.
	sourcePath string
	pathMu     sync.RWMutex
	// staged marks a handle whose sourcePath is a fsynced, immutable file
	// this owner wrote in its own directory. Publication renames it into its
	// "<id>.seg" name, so no staged name outlives the publication, and the
	// handle keeps serving from the same open file across the rename.
	staged        bool
	refs          atomic.Int64
	count         uint64
	id            uint64
	size          uint64
	timeMin       uint64
	timeMax       uint64
	hasTime       bool
	indexedFields []string
	persisted     atomic.Bool
	// fieldNamesOnce and fieldNameSet cache this handle's complete field-name
	// set (every field the segment carries, not only indexed ones), computed
	// once per handle on first InsertIfAbsent admission check.
	fieldNamesOnce sync.Once
	fieldNameSet   map[string]struct{}
	fieldNameErr   error

	// trieOnce caches whether this segment carries the coarse _timestamp term
	// levels a time-range cover relies on. A segment without them would
	// silently lose documents under a trie intersection, so it falls back to
	// the per-document time check instead.
	trieOnce        sync.Once
	trieUsableValue bool
	trieUsableErr   error
}

type persistedPromotion struct {
	original    *segmentHandle
	replacement *segmentHandle
}

// fieldNames returns this handle's complete field-name set -- every field the
// segment carries, including stored-only, sort-only, and internal fields --
// computed once per handle and cached for later InsertIfAbsent checks.
func (h *segmentHandle) fieldNames() (map[string]struct{}, error) {
	h.fieldNamesOnce.Do(func() {
		names, err := h.reader.Fields()
		if err != nil {
			h.fieldNameErr = err
			return
		}
		set := make(map[string]struct{}, len(names))
		for _, name := range names {
			set[name] = struct{}{}
		}
		h.fieldNameSet = set
	})
	return h.fieldNameSet, h.fieldNameErr
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
		if err := removeStagedFiles(options.Path); err != nil {
			return nil, fmt.Errorf("remove staged native files: %w", err)
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
	if options.PresenceCacheBytes > 0 {
		owner.presenceCache = newPresenceCache(options.PresenceCacheBytes)
	}
	if options.Persist != nil || options.Path != "" {
		queue := &persistenceQueue{wake: make(chan struct{}, 1)}
		owner.persistQ = queue
		persistenceContext, persistenceCancel := context.WithCancel(context.Background())
		owner.persistenceCancel = persistenceCancel
		owner.persistenceTask = run.Go(persistenceContext, "native.owner.persistence", nil, func(ctx context.Context) {
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
		for {
			o.mu.Lock()
			shouldCompact := !o.closed && !o.closing && !o.collecting && o.durabilityFault == nil &&
				o.root != nil && len(o.root.segments) >= o.maintenanceThreshold
			before := 0
			if o.root != nil {
				before = len(o.root.segments)
			}
			o.mu.Unlock()
			if !shouldCompact || ctx.Err() != nil {
				break
			}
			// Compact now plans and executes one bounded merge task at a time
			// (see planMerges in mergeplan.go) instead of always remerging the
			// whole root, so a single wake may need several calls to work
			// through a large backlog. A stale build or backpressure result
			// stops this inner loop and waits for the next wake instead of
			// spinning; forward progress (segment count actually dropping) is
			// what keeps it going instead of looping forever.
			if err := o.Compact(ctx); err != nil {
				break
			}
			o.mu.Lock()
			after := 0
			if o.root != nil {
				after = len(o.root.segments)
			}
			o.mu.Unlock()
			if after >= before {
				break
			}
		}
		// Collecting every wake, not only a wake where compaction found
		// nothing to do, matters under continuous admission: tiered
		// compaction almost always has some bounded task available, so a
		// compaction-or-collect choice would starve collection for the
		// entire burst, leaving superseded segment and manifest files to
		// accumulate on disk (and, on some filesystems, make the directory
		// itself progressively slower to touch) until writes finally pause.
		_ = o.CollectGarbage(ctx)
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
		// Best effort, see newMemorySegment. This is the owner's own startup
		// reopen of an existing directory: without this, the first exact
		// lookup after every process restart rebuilds every segment's
		// filter lazily, not just after a fresh publish or compaction.
		_ = segmentReader.PrepareTermFilter(identifierField)
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
	for {
		select {
		case <-ctx.Done():
			return
		case <-queue.wake:
		}
		o.drainPersistence(ctx, o.options.PersistInterval)
	}
}

// waitPersistInterval sleeps until interval has passed since started and
// reports whether persistence should continue.
func waitPersistInterval(ctx context.Context, started time.Time, interval time.Duration) bool {
	remaining := interval - time.Since(started)
	if remaining <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(remaining)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// drainPersistence persists the owner's current root for as long as it is
// newer than the last durable generation, coalescing every admission that
// landed since the previous call into whichever single persist operation
// catches it: it always reads o.root fresh rather than working through a
// backlog of every intermediate generation, so a burst of rapid admissions
// (for example OAP's schema-registry preload) shares one fsync instead of
// paying for one each. It is called both from the background worker loop
// above and, once, synchronously by Close for a final flush. A positive
// interval spaces consecutive persists at least that far apart.
func (o *Owner) drainPersistence(ctx context.Context, interval time.Duration) {
	for {
		if err := ctx.Err(); err != nil {
			return
		}
		o.mu.Lock()
		if o.durabilityFault != nil {
			// A prior call already discovered and reported this terminal
			// fault (recording it once into o.persistErr and firing every
			// callback pending at the time). o.root cannot have advanced
			// since -- every admission path rejects new writes once this is
			// set -- so retrying here would only re-attempt the same doomed
			// generation and, having no pending callbacks left to report it
			// through, overwrite the already-correct view of why.
			o.mu.Unlock()
			return
		}
		root := o.root
		if root == nil || root.generation <= o.durable.Load() {
			o.mu.Unlock()
			return
		}
		root.refs.Add(1)
		o.mu.Unlock()

		persistStarted := time.Now()
		o.persistMu.Lock()
		callbacks := o.pendingCallbacks
		o.pendingCallbacks = nil
		compacted := o.pendingCompacted
		o.pendingCompacted = false
		o.persistMu.Unlock()

		view := &ReadView{root: root}
		persistErr := o.persistenceFault()
		if persistErr == nil {
			persistErr = safePersist(func() error {
				if o.options.Persist != nil {
					return o.options.Persist(ctx, view)
				}
				return o.persistRoot(root)
			})
			if persistErr != nil {
				persistErr = o.markPersistenceFailure(persistErr)
			}
		}
		_ = view.Close()
		if persistErr == nil {
			for {
				current := o.durable.Load()
				if current >= root.generation || o.durable.CompareAndSwap(current, root.generation) {
					break
				}
			}
		} else if len(callbacks) == 0 {
			o.persistMu.Lock()
			if o.persistErr == nil {
				o.persistErr = persistErr
			}
			o.persistMu.Unlock()
		}
		for _, callback := range callbacks {
			callback := callback
			// Keep user callbacks outside the serial persistence worker. A
			// callback is allowed to close the owner; doing so here would make
			// Close wait on the worker that is currently invoking it.
			run.Go(ctx, "native.owner.persistence-callback", nil, func(context.Context) {
				safeCallback(callback, persistErr)
			})
		}
		if compacted {
			o.requestGarbageCollection()
		}
		if persistErr != nil {
			return
		}
		if interval > 0 && !waitPersistInterval(ctx, persistStarted, interval) {
			return
		}
	}
}

// schedulePersistLocked records a pending PersistentCallback and/or a pending
// compaction, then wakes the persistence worker so it coalesces this
// admission into its next (or current) flush of o.root. Called with o.mu
// held, after the new root has already been published.
func (o *Owner) schedulePersistLocked(queue *persistenceQueue, callback func(error), compacted bool) {
	if callback != nil || compacted {
		o.persistMu.Lock()
		if callback != nil {
			o.pendingCallbacks = append(o.pendingCallbacks, callback)
		}
		if compacted {
			o.pendingCompacted = true
		}
		o.persistMu.Unlock()
	}
	queue.requestPersist()
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

func (o *Owner) validateReadLease() error {
	if lease, ok := o.options.Lease.(revocableLease); ok {
		if lease.Revoked() {
			return ErrLeaseUnavailable
		}
		return nil
	}
	return o.validateLease()
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
	// unpersisted maps each admitted handle this persistence writes to the
	// promotion that follows its payload through the publication.
	unpersisted := make(map[*segmentHandle]*nativeice.Promotion, len(root.segments))
	renamedAny := false
	for _, current := range root.segments {
		segment, ok := current.(*memorySegment)
		if !ok {
			return fmt.Errorf("persist native root: unsupported segment type %T", current)
		}
		// Do not expose a fully masked segment to the legacy writer during a
		// writer transition.  Legacy merge planning treats an all-deleted
		// segment as an empty merge input; publishing that input can schedule a
		// merge whose replacement is nil and then panic while closing it.  The
		// immutable in-memory root still retains the segment for pinned views;
		// only the durable manifest omits it.  A zero-document segment is also
		// never useful in a published snapshot.
		if segmentHasNoLiveDocuments(segment) {
			continue
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
		persisted := segment.handle.persisted.Load()
		trusted := persisted
		if !persisted {
			// The captured root pins this immutable handle for the synchronous
			// publisher call; avoid copying the payload solely for persistence.
			//
			// A root built (via introduceExternalSegment/Batch) before an
			// earlier generation's persistRoot call reached this same segment
			// still holds this original, pre-promotion handle -- promotion
			// only swaps the segment reference in the *current* o.root, not in
			// roots already queued for persistence. handle.persisted is an
			// atomic bool on that shared handle, so it is the one signal that
			// correctly reaches every such copy.
			payload = segment.handle.payload
			renamed, renameErr := o.renameStagedSegment(segment.handle)
			if renameErr != nil {
				return renameErr
			}
			if renamed {
				// The owner wrote, fsynced and validated this file itself; it
				// now carries its final name and only needs the manifest.
				trusted = true
				renamedAny = true
			}
			var promotion *nativeice.Promotion
			if payload != nil {
				promotion = nativeice.NewPromotion(segment.handle.reader)
			}
			unpersisted[segment.handle] = promotion
		}
		segments = append(segments, nativeice.SnapshotSegmentPayload{
			SnapshotSegment: metadata, Payload: payload, TrustedExisting: trusted, Promotion: unpersisted[segment.handle],
		})
		handles = append(handles, segment.handle)
	}
	if renamedAny {
		// Every rename must be durable before the manifest that references
		// its "<id>.seg" can be.
		if err := syncOwnerDirectory(o.options.Path); err != nil {
			return fmt.Errorf("sync renamed native segments: %w", err)
		}
		if persistRenamedHook != nil {
			persistRenamedHook()
		}
	}
	if err := nativeice.PublishSnapshot(o.options.Path, root.generation, segments); err != nil {
		return err
	}
	for _, handle := range handles {
		handle.persisted.Store(true)
	}
	promotions := make([]persistedPromotion, 0, len(unpersisted))
	for handle, promotion := range unpersisted {
		// Once persisted, every segment is served from its file: an admitted
		// payload occupies the heap only until it is durable. A staged
		// segment's reader already serves the file publication
		// renamed to "<id>.seg"; reopening it would only discard the warm
		// reader.
		if handle.staged {
			continue
		}
		segmentPath := filepath.Join(o.options.Path, fmt.Sprintf("%012x.seg", handle.id))
		// The file is the one PublishSnapshot just wrote from this handle's
		// payload: the replacement takes over the term filters the handle
		// already built rather than rebuilding them from the file.
		reader, openErr := nativeice.OpenPromotedSegment(segmentPath, nativeice.SnapshotSegment{
			ID: handle.id, Size: handle.size, DocumentCount: handle.count,
			TimeMin: handle.timeMin, TimeMax: handle.timeMax,
		}, promotion)
		if openErr != nil {
			for _, promotion := range promotions {
				_ = promotion.replacement.reader.Close()
			}
			return fmt.Errorf("open persisted native segment %d for promotion: %w", handle.id, openErr)
		}
		// Best effort, see newMemorySegment: should the replacement have
		// adopted no filter, it is built here, before promotePersistedHandles
		// takes o.mu, so outside the lock, instead of lazily by the first
		// exact lookup.
		_ = reader.PrepareTermFilter(identifierField)
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
	if err := o.validateReadLease(); err != nil {
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
	if batch.Mode == BatchInsertIfAbsent {
		return o.batchInsertIfAbsent(ctx, batch)
	}
	queue := o.persistQ
	// Validating the batch and encoding its segment depend only on the batch,
	// so they run before taking o.mu; only applying it to the current root
	// is serialized with other admissions, compaction and persistence.
	prepared, prepareErr := prepareBatch(batch, o.options.IdentifierDocValues)
	o.mu.Lock()
	if o.closed || o.closing {
		o.mu.Unlock()
		prepared.release()
		return finishCallback(batch.PersistentCallback, ErrOwnerClosed)
	}
	if prepareErr != nil {
		o.mu.Unlock()
		return finishCallback(batch.PersistentCallback, prepareErr)
	}
	if checkErr := o.admitChecksLocked(queue, batch.PersistentCallback != nil); checkErr != nil {
		o.mu.Unlock()
		prepared.release()
		return finishCallback(batch.PersistentCallback, checkErr)
	}
	return o.finishPreparedBatchLocked(queue, prepared, batch.PersistentCallback, nil)
}

// admitChecksLocked runs the checks every admission must pass while o.mu is
// held: owner shutdown, an existing durability fault, the root lease, and
// (when the caller supplied a PersistentCallback) that persistence is
// actually configured. It never unlocks o.mu; the caller does that in every
// case, including when this returns a non-nil error.
func (o *Owner) admitChecksLocked(queue *persistenceQueue, hasCallback bool) error {
	if o.closed || o.closing {
		return ErrOwnerClosed
	}
	// Collection only removes files below its captured root's keep set. New
	// admissions publish fresh segment IDs and therefore cannot be collected.
	if o.durabilityFault != nil {
		return o.durabilityFault
	}
	if err := o.validateLease(); err != nil {
		return fmt.Errorf("validate native root lease: %w", err)
	}
	if queue == nil && hasCallback {
		return ErrPersistenceConfiguration
	}
	return nil
}

// finishPreparedBatchLocked publishes prepared to the current root, indexes
// its segment, invalidates the presence cache for every identifier prepared
// replaces or deletes, runs onPublished (if not nil, after invalidation, so
// it can safely re-populate cache entries for documents this batch just
// admitted), and schedules persistence. The common pre-publish checks
// (admitChecksLocked) must already have passed. o.mu must be held on entry;
// this always unlocks it before returning.
func (o *Owner) finishPreparedBatchLocked(queue *persistenceQueue, prepared preparedBatch, callback func(error), onPublished func()) error {
	next, err := o.publishBatchLocked(prepared)
	if err == nil {
		old := o.root
		o.root = next
		old.release() // release the owner-held reference to the old root
		o.pruneRootsLocked()
		if prepared.segment != nil {
			o.indexAdmittedLocked(prepared.segment.(*memorySegment).handle.id, prepared.identifiers)
		}
		// Every identifier prepared.deletes names either stopped being live
		// (an explicit delete) or had its earlier live copy replaced by a
		// document this batch just admitted (an upsert or a satisfied
		// InsertIfAbsent). Either way, a presence-cache entry for it,
		// positive and cached before this publish, is no longer trustworthy.
		for _, identifier := range prepared.deletes {
			o.presenceCache.invalidate(identifier)
		}
		if onPublished != nil {
			onPublished()
		}
	}
	if err != nil {
		o.mu.Unlock()
		return finishCallback(callback, err)
	}
	o.roots[next] = struct{}{}
	if queue == nil {
		o.mu.Unlock()
		return finishCallback(callback, nil)
	}
	// The persistence worker reads o.root fresh rather than this specific
	// root, so no pin is needed here on its behalf; it takes its own pin
	// after reading o.root (see drainPersistence).
	o.schedulePersistLocked(queue, callback, false)
	o.mu.Unlock()
	o.requestMaintenance()
	return nil
}

// insertIfAbsentCandidate is one BatchInsertIfAbsent document after
// within-batch duplicate collapse (last writer wins) and before its presence
// is resolved against the admission root. No nativeice encoding has happened
// yet: a candidate the admission root already satisfies is never encoded.
//
//nolint:govet // fields are grouped by meaning, not padding, like preparedBatch.
type insertIfAbsentCandidate struct {
	document Document
	fields   map[string]struct{}
}

// prepareInsertIfAbsentCandidates validates batch and collapses its
// Documents into the distinct, last-writer-wins set batchInsertIfAbsent
// resolves presence against.
func prepareInsertIfAbsentCandidates(batch Batch) ([]insertIfAbsentCandidate, error) {
	for documentIndex, document := range batch.Documents {
		for _, field := range document.Fields {
			if field.Name == identifierField || field.Name == timestampField {
				return nil, fmt.Errorf("document %d contains reserved field %q: %w", documentIndex, field.Name, ErrInvalidDocument)
			}
		}
	}
	for _, identifier := range batch.Deletes {
		if len(identifier) == 0 {
			return nil, fmt.Errorf("delete has no identifier: %w", ErrInvalidDocument)
		}
	}
	candidates := make([]insertIfAbsentCandidate, 0, len(batch.Documents))
	indexByIdentifier := make(map[string]int, len(batch.Documents))
	for documentIndex, document := range batch.Documents {
		if len(document.Identifier) == 0 {
			return nil, fmt.Errorf("document %d has no identifier: %w", documentIndex, ErrInvalidDocument)
		}
		fields := make(map[string]struct{}, len(document.Fields))
		for _, field := range document.Fields {
			fields[field.Name] = struct{}{}
		}
		candidate := insertIfAbsentCandidate{document: cloneDocument(document), fields: fields}
		key := string(document.Identifier)
		if previousIndex, found := indexByIdentifier[key]; found {
			// A batch is one admission boundary: retain only the last physical
			// value for an identifier so it cannot publish two live updates.
			candidates[previousIndex] = candidate
			continue
		}
		indexByIdentifier[key] = len(candidates)
		candidates = append(candidates, candidate)
	}
	return candidates, nil
}

// batchInsertIfAbsent implements Batch for BatchInsertIfAbsent. Presence is
// resolved before any document is encoded, so a batch whose documents are
// all already present publishes nothing: no new generation, no segment, and
// no persist wake. Only the documents admission actually admits are encoded,
// so a published segment never carries a masked, never-live document.
//
// Encoding runs outside o.mu (nativeice encoding does not touch owner
// state), so the admission root can advance between the presence resolve and
// the publish attempt. When it does, this discards the encode and re-resolves
// against the now-current root before retrying, so a document another
// admission concurrently admitted is never admitted twice.
func (o *Owner) batchInsertIfAbsent(ctx context.Context, batch Batch) error {
	candidates, prepareErr := prepareInsertIfAbsentCandidates(batch)
	if prepareErr != nil {
		return finishCallback(batch.PersistentCallback, prepareErr)
	}
	queue := o.persistQ
	for {
		if err := ctx.Err(); err != nil {
			return finishCallback(batch.PersistentCallback, err)
		}
		o.mu.Lock()
		if checkErr := o.admitChecksLocked(queue, batch.PersistentCallback != nil); checkErr != nil {
			o.mu.Unlock()
			return finishCallback(batch.PersistentCallback, checkErr)
		}
		generation := o.root.generation
		admitted, resolveErr := o.filterAbsentLocked(candidates)
		o.mu.Unlock()
		if resolveErr != nil {
			return finishCallback(batch.PersistentCallback, resolveErr)
		}
		if len(admitted) == 0 && len(batch.Deletes) == 0 {
			// Nothing admitted and nothing explicitly deleted: publish
			// nothing rather than a root whose only change is a bumped
			// generation and, for a wholly in-memory owner, an empty segment.
			return finishCallback(batch.PersistentCallback, nil)
		}
		prepared, encodeErr := prepareAbsentBatch(admitted, batch.Deletes, o.options.IdentifierDocValues)
		if encodeErr != nil {
			return finishCallback(batch.PersistentCallback, encodeErr)
		}
		o.mu.Lock()
		if checkErr := o.admitChecksLocked(queue, batch.PersistentCallback != nil); checkErr != nil {
			o.mu.Unlock()
			prepared.release()
			return finishCallback(batch.PersistentCallback, checkErr)
		}
		if o.root.generation != generation {
			// A concurrent admission published while this batch encoded
			// outside the lock. Re-resolve every candidate against the
			// current root; the encode is still valid when the admitted
			// set is unchanged, which is the common case under steady
			// concurrent writes of distinct series.
			current, recheckErr := o.filterAbsentLocked(candidates)
			if recheckErr != nil {
				o.mu.Unlock()
				prepared.release()
				return finishCallback(batch.PersistentCallback, recheckErr)
			}
			if !sameCandidates(current, admitted) {
				o.mu.Unlock()
				prepared.release()
				continue
			}
		}
		return o.finishPreparedBatchLocked(queue, prepared, batch.PersistentCallback, func() {
			// The admitted documents are now live with exactly their own
			// field sets: cache that so a repeat InsertIfAbsent of the same
			// identifier and field set -- the common series-index write
			// pattern -- hits the cache instead of rescanning segments.
			for _, candidate := range admitted {
				o.presenceCache.store(candidate.document.Identifier, candidate.fields)
			}
		})
	}
}

// sameCandidates reports whether two filterAbsentLocked results, both ordered
// subsets of the same candidate slice, select the same documents.
func sameCandidates(left, right []insertIfAbsentCandidate) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if !bytes.Equal(left[index].document.Identifier, right[index].document.Identifier) {
			return false
		}
	}
	return true
}

// filterAbsentLocked returns the subset of candidates not already live, in
// some segment of the current root, with a field set that covers what the
// candidate carries. It must run under o.mu.
func (o *Owner) filterAbsentLocked(candidates []insertIfAbsentCandidate) ([]insertIfAbsentCandidate, error) {
	absent := make([]insertIfAbsentCandidate, 0, len(candidates))
	for _, candidate := range candidates {
		present, err := o.identifierPresentLocked(candidate.document.Identifier, candidate.fields)
		if err != nil {
			return nil, err
		}
		if !present {
			absent = append(absent, candidate)
		}
	}
	return absent, nil
}

// prepareAbsentBatch encodes exactly the candidates admission decided to
// admit (identical for every repeat of an all-present batch: nothing to
// encode). explicitDeletes are the batch's own Batch.Deletes, applied
// unconditionally like any other mode.
func prepareAbsentBatch(admitted []insertIfAbsentCandidate, explicitDeletes [][]byte, identifierDocValues bool) (preparedBatch, error) {
	prepared := preparedBatch{deletes: make([][]byte, 0, len(explicitDeletes)+len(admitted))}
	prepared.deletes = append(prepared.deletes, explicitDeletes...)
	if len(admitted) == 0 {
		return prepared, nil
	}
	documents := make([]Document, len(admitted))
	for index, candidate := range admitted {
		documents[index] = candidate.document
		// The admitted document's identifier may still have an earlier live
		// copy with an insufficient field set somewhere (that is exactly why
		// it was admitted rather than skipped): replace it, the same as
		// BatchUpsert already does for every document it admits.
		prepared.deletes = append(prepared.deletes, candidate.document.Identifier)
	}
	segment, segmentErr := newMemorySegment(documents, 0, identifierDocValues)
	if segmentErr != nil {
		return preparedBatch{}, segmentErr
	}
	prepared.segment = segment
	prepared.documents = uint64(len(documents))
	prepared.identifiers = make([][]byte, len(documents))
	for index := range documents {
		prepared.identifiers[index] = documents[index].Identifier
	}
	return prepared, nil
}

// identifierPresentLocked reports whether identifier already has a live
// posting in some segment of the current root whose field-name set covers
// every name in fields. It consults the presence cache first when one is
// configured; the cache only ever holds positive decisions (see
// presenceCache), so a hit here is always "present".
func (o *Owner) identifierPresentLocked(identifier []byte, fields map[string]struct{}) (bool, error) {
	if o.presenceCache.lookup(identifier, fields) {
		return true, nil
	}
	for _, index := range o.candidateSegmentIndicesLocked(o.root.segments, identifier) {
		memSeg, ok := o.root.segments[index].(*memorySegment)
		if !ok {
			return false, fmt.Errorf("resolve insert-if-absent: unsupported segment type %T", o.root.segments[index])
		}
		live, liveErr := memSeg.hasLivePosting(identifier)
		if liveErr != nil {
			return false, liveErr
		}
		if !live {
			continue
		}
		segmentFields, fieldErr := memSeg.handle.fieldNames()
		if fieldErr != nil {
			return false, fieldErr
		}
		if fieldSetContainsAll(segmentFields, fields) {
			o.presenceCache.store(identifier, segmentFields)
			return true, nil
		}
	}
	return false, nil
}

// candidateSegmentIndicesLocked returns the indices into segments that might
// hold a live posting for identifier: segments the admission index directly
// names, plus every segment the index does not cover (merge results,
// segments loaded at startup, and external segments), exactly the probe set
// publishBatchLocked's own delete application already uses.
func (o *Owner) candidateSegmentIndicesLocked(segments []rootSegment, identifier []byte) []int {
	indices := make([]int, 0, len(segments))
	seen := make(map[int]struct{}, len(segments))
	if holders := o.admittedIdentifiers[string(identifier)]; len(holders) > 0 {
		positions := segmentPositions(segments)
		for _, segmentID := range holders {
			if index, found := positions[segmentID]; found {
				if _, dup := seen[index]; !dup {
					seen[index] = struct{}{}
					indices = append(indices, index)
				}
			}
		}
	}
	for index, segment := range segments {
		if memSeg, ok := segment.(*memorySegment); ok {
			if _, indexed := o.admittedSegments[memSeg.handle.id]; indexed {
				continue
			}
		}
		if _, dup := seen[index]; !dup {
			indices = append(indices, index)
		}
	}
	return indices
}

// prepareMergeDrop evaluates the optional product merge policy against one
// immutable segment. The reader callback is deliberately synchronous: values
// borrowed from ICE never escape the callback, and no published reader or
// deletion map is mutated.
func (o *Owner) prepareMergeDrop(ctx context.Context, segment *memorySegment) (*roaringpkg.Bitmap, error) {
	drop := roaringpkg.New()
	callback := o.options.PrepareMergeCallback
	if callback == nil {
		return drop, nil
	}
	for documentNumber := uint64(0); documentNumber < segment.handle.count; documentNumber++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if _, alreadyDeleted := segment.deleted[documentNumber]; alreadyDeleted {
			// The immutable root mask already excludes this document. Product
			// expiry policy applies only to documents still visible in the root.
			continue
		}
		document := MergeDocument{
			SegmentID:      segment.handle.id,
			DocumentNumber: documentNumber,
			StoredFields: func(visit func(name string, value []byte) bool) error {
				return segment.handle.reader.VisitDocument(documentNumber, func(stored nativeice.StoredDocument) error {
					return stored.VisitStoredFields(visit)
				})
			},
		}
		shouldDrop, err := callback(ctx, document)
		if err != nil {
			return nil, fmt.Errorf("prepare native merge document %d: %w", documentNumber, err)
		}
		if shouldDrop {
			if documentNumber > uint64(^uint32(0)) {
				return nil, fmt.Errorf("merge drop document %d exceeds mask range: %w", documentNumber, ErrInvalidDocument)
			}
			drop.Add(uint32(documentNumber))
		}
	}
	return drop, nil
}

// Compact plans and executes at most one bounded merge task outside the
// admission lock, then publishes the result only if every segment the task
// merged is still present, unchanged, in the current root. A stale build is
// discarded, leaving all old segment files and references untouched for
// readers and prior manifests.
//
// Unlike a design that always merges every live segment into one, Compact
// delegates to planMerges (see mergeplan.go) so that routine calls only
// combine a bounded batch of comparably small segments, and a segment is
// retired from further merging once it crosses a size ceiling. That keeps a
// single merge's cost proportional to its own tier instead of to the whole
// index, which is what lets compaction keep pace with a long, continuous
// burst of small writes (for example OAP's schema-registry preload)
// instead of every cycle re-merging an ever-growing backlog. Compact is a
// no-op (nil, nil) once the root is already within the tiered budget; see
// forceMergeAll for unconditionally sweeping every segment, for example to
// guarantee a product's PrepareMergeCallback reclaims a tombstone sitting
// alone in a not-yet-tiered segment.
func (o *Owner) Compact(ctx context.Context) error {
	return o.compact(ctx, func(candidates []mergeCandidate) []mergeTask {
		return planMerges(candidates, defaultMergePlanOptions)
	})
}

// forceMergeAll merges every currently live segment into one, bypassing the
// tiered merge policy entirely. The tiered policy (Compact) mathematically
// never schedules a merge for fewer than two comparably-sized segments --
// the same limitation the legacy engine's TieredMergePolicy-derived planner has --
// so a lone segment's tombstones would otherwise wait for a peer segment
// that may never arrive. This is the native-engine equivalent of Lucene's
// explicit forceMergeDeletes: an operator- or test-invoked full vacuum, not
// part of the routine, cost-bounded maintenance path.
func (o *Owner) forceMergeAll(ctx context.Context) error {
	return o.compact(ctx, func(candidates []mergeCandidate) []mergeTask {
		if len(candidates) == 0 {
			return nil
		}
		return []mergeTask{{candidates: candidates}}
	})
}

//nolint:gocyclo // admission, merge, and stale-root checks are kept explicit.
func (o *Owner) compact(ctx context.Context, plan func([]mergeCandidate) []mergeTask) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	queue := o.persistQ
	o.mu.Lock()
	if o.closed || o.closing {
		o.mu.Unlock()
		return ErrOwnerClosed
	}
	if o.collecting {
		o.mu.Unlock()
		return ErrPersistenceBusy
	}
	if o.durabilityFault != nil {
		fault := o.durabilityFault
		o.mu.Unlock()
		return fault
	}
	o.activeOps++
	defer o.endOperation()
	base := o.root
	base.refs.Add(1)
	// base.release() below may drop the last reference to base once a
	// concurrent write has superseded it, which clears base.segments. Keep a
	// copy of the slice header (the same backing array, so the segment
	// pointers it holds for the task-membership check below stay valid)
	// since merging does not require the owner's pin to remain held.
	baseSegments := base.segments
	o.mu.Unlock()

	candidates := make([]mergeCandidate, 0, len(baseSegments))
	for _, current := range baseSegments {
		segment, ok := current.(*memorySegment)
		if !ok {
			base.release()
			return fmt.Errorf("compact native root: unsupported segment type %T", current)
		}
		candidates = append(candidates, mergeCandidate{
			segment: segment, fullCount: int64(segment.handle.count),
			liveCount: int64(segment.handle.count) - int64(len(segment.deleted)),
		})
	}
	tasks := plan(candidates)
	if len(tasks) == 0 {
		base.release()
		return nil
	}
	// Validated only once there is a merge to publish, and outside o.mu: a
	// lease check must never stall readers and writers behind the owner lock.
	if err := o.validateLease(); err != nil {
		base.release()
		return fmt.Errorf("validate native root lease: %w", err)
	}
	task := tasks[0]

	inputs := make([]nativeice.MergeInput, 0, len(task.candidates))
	taskSegments := make(map[rootSegment]struct{}, len(task.candidates))
	mergeDroppedAny := false
	for _, candidate := range task.candidates {
		segment := candidate.segment.(*memorySegment)
		taskSegments[candidate.segment] = struct{}{}
		drop, prepareErr := o.prepareMergeDrop(ctx, segment)
		if prepareErr != nil {
			base.release()
			return prepareErr
		}
		if !drop.IsEmpty() {
			// PrepareMergeCallback dropped a document that was still live
			// going into this merge (for example an expired tombstone):
			// whatever the presence cache believed about its identifier no
			// longer holds. Pre-existing deletion masks, merged into drop
			// below, were already invalidated when their delete admitted.
			mergeDroppedAny = true
		}
		for number := range segment.deleted {
			if number <= uint64(^uint32(0)) {
				drop.Add(uint32(number))
			}
		}
		inputs = append(inputs, nativeice.MergeInput{Reader: segment.handle.reader, Drop: drop, IndexedFields: segment.handle.indexedFields})
	}
	// The merge runs, and its output is opened, before taking o.mu:
	// validating and indexing a large segment under the owner lock would
	// stall every concurrent admission and read view for its whole duration.
	// It stays private until published below, so its identifier can be
	// assigned afterwards. An owner with a directory streams the merge into a
	// staged file there, so merge memory does not grow with the merged
	// segment; one without a directory has nowhere to stage it.
	var compacted rootSegment
	var mergeErr error
	if o.options.Path != "" {
		compacted, mergeErr = o.mergeToStagedSegment(ctx, inputs)
		base.release()
		if mergeErr != nil {
			return mergeErr
		}
	} else {
		merged, inMemoryErr := nativeice.MergeSegments(ctx, inputs)
		base.release()
		if inMemoryErr != nil {
			return inMemoryErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		compacted, mergeErr = newSegmentFromPayload(merged.Payload, 0)
		if mergeErr != nil {
			return mergeErr
		}
	}
	releaseUnpublished := func() {
		if compacted != nil {
			compacted.release()
		}
	}

	o.mu.Lock()
	if o.closed || o.closing {
		o.mu.Unlock()
		releaseUnpublished()
		return ErrOwnerClosed
	}
	current := o.root
	// Segment IDs are allocated here, under the reacquired lock, rather than
	// before the merge ran: a concurrent admission during the merge window
	// may already have consumed the ID this call would otherwise have
	// reserved earlier, which would collide with it once rebased below.
	segmentID := o.nextSegmentID
	if segmentID == ^uint64(0) {
		o.mu.Unlock()
		releaseUnpublished()
		return fmt.Errorf("native segment identifiers exhausted: %w", ErrInvalidDocument)
	}
	if compacted != nil {
		compacted.(*memorySegment).handle.id = segmentID
	}
	// Under continuous sequential admission (for example bulk schema
	// preload), some newer root almost always exists by the time this merge
	// finishes; discarding the merge outright on every such write would
	// starve compaction indefinitely. remainder/found below check only the
	// segments this task actually merged, by identity: any of them replaced
	// by a concurrent Delete (which clones and replaces its segment rather
	// than mutating it in place) is detected and the whole task is discarded
	// as stale, but any other concurrent admission -- which only adds
	// segments or replaces segments outside this task -- cannot invalidate
	// it. That lets the compacted result replace exactly the segments it
	// merged while every other segment, old or newly admitted, carries
	// forward untouched.
	// A concurrent compaction may already have merged away some of this
	// task's segments, leaving current with fewer segments than the task;
	// the capacity must not go negative (a panic here, under o.mu, would
	// deadlock the deferred endOperation). The found check below reports it
	// as stale.
	remaining := make([]rootSegment, 0, max(len(current.segments)-len(taskSegments)+1, 1))
	found := 0
	for _, segment := range current.segments {
		if _, ok := taskSegments[segment]; ok {
			found++
			continue
		}
		remaining = append(remaining, segment)
	}
	if found != len(taskSegments) {
		o.mu.Unlock()
		if compacted != nil {
			compacted.release()
		}
		return ErrStaleCompaction
	}
	for _, segment := range remaining {
		segment.retain()
	}
	next := &publishedRoot{generation: current.generation + 1, nextNumber: 0, refs: atomic.Int64{}}
	next.refs.Store(1)
	next.segments = remaining
	for _, segment := range remaining {
		next.nextNumber += segment.Len()
	}
	if compacted != nil {
		next.segments = append(next.segments, compacted)
		next.nextNumber += compacted.Len()
	}
	old := o.root
	o.root = next
	o.roots[next] = struct{}{}
	o.nextSegmentID++
	old.release()
	o.pruneRootsLocked()
	for segment := range taskSegments {
		handle := segment.(*memorySegment).handle
		o.unindexSegmentLocked(handle.id)
	}
	if mergeDroppedAny {
		// A merge-dropped identifier is not individually known here (the
		// callback reports it by segment/document, not by identifier), so
		// the whole cache is reset rather than tracked per identifier.
		// Compaction is not the per-write hot path, so this is cheap.
		o.presenceCache.reset()
	}
	if queue != nil {
		o.schedulePersistLocked(queue, nil, true)
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
	// Segments released before this collection may still be being closed;
	// let that finish so no file is deleted under an open descriptor.
	segmentDisposals.wait()

	// Segments the pinned root already serves from their validated files
	// need not be opened and validated again; their paths need only still
	// name those files.
	held := make(map[uint64]*nativeice.Reader, len(root.segments))
	for _, current := range root.segments {
		if segment, ok := current.(*memorySegment); ok && segment.handle.persisted.Load() && segment.handle.payload == nil {
			held[segment.handle.id] = segment.handle.reader
		}
	}
	metadata, metadataErr := nativeice.ReadStrictSnapshotMetadata(o.options.Path, func(segmentID uint64) *nativeice.Reader {
		return held[segmentID]
	})
	if metadataErr != nil {
		return metadataErr
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
	entries, readErr := fileSystem.ReadDirLimit(o.options.Path, 0)
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
		if err := fileSystem.DeleteFile(path); err != nil {
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
		if err := fileSystem.DeleteFile(path); err != nil {
			return fmt.Errorf("remove obsolete segment %q: %w", path, err)
		}
	}
	return syncOwnerDirectory(o.options.Path)
}

func syncOwnerDirectory(path string) error {
	return fs.SyncDir(path)
}

// Collect is the concise alias for CollectGarbage.
func (o *Owner) Collect(ctx context.Context) error { return o.CollectGarbage(ctx) }

// preparedBatch is a validated admission whose new segment is already
// encoded; publishBatchLocked applies it to the current root.
type preparedBatch struct {
	segment     rootSegment
	deletes     [][]byte
	identifiers [][]byte
	documents   uint64
}

func (p preparedBatch) release() {
	if p.segment != nil {
		p.segment.release()
	}
}

// prepareBatch validates batch and encodes its documents into a new segment,
// keeping only the last value of a repeated identifier unless the batch is
// BatchInsertOnly. deletes lists every identifier whose earlier live
// documents the batch replaces. prepareBatch is used for BatchUpsert and
// BatchInsertOnly; BatchInsertIfAbsent resolves presence before encoding
// (see batchInsertIfAbsent) so it never encodes a document only to mask it.
// identifierDocValues is threaded straight to the segment encoder so "_id"
// gets a doc-value column when OwnerOptions.IdentifierDocValues is set.
func prepareBatch(batch Batch, identifierDocValues bool) (preparedBatch, error) {
	for documentIndex, document := range batch.Documents {
		for _, field := range document.Fields {
			if field.Name == identifierField || field.Name == timestampField {
				return preparedBatch{}, fmt.Errorf("document %d contains reserved field %q: %w", documentIndex, field.Name, ErrInvalidDocument)
			}
		}
	}
	prepared := preparedBatch{deletes: make([][]byte, 0, len(batch.Deletes)+len(batch.Documents))}
	for _, identifier := range batch.Deletes {
		if len(identifier) == 0 {
			return preparedBatch{}, fmt.Errorf("delete has no identifier: %w", ErrInvalidDocument)
		}
		prepared.deletes = append(prepared.deletes, identifier)
	}
	if len(batch.Documents) == 0 {
		return prepared, nil
	}
	collapseDuplicates := batch.Mode != BatchInsertOnly
	newDocuments := make([]Document, 0, len(batch.Documents))
	newDocumentIndexes := make(map[string]int, len(batch.Documents))
	for documentIndex, document := range batch.Documents {
		if len(document.Identifier) == 0 {
			return preparedBatch{}, fmt.Errorf("document %d has no identifier: %w", documentIndex, ErrInvalidDocument)
		}
		if collapseDuplicates {
			key := string(document.Identifier)
			if previousIndex, found := newDocumentIndexes[key]; found {
				// A batch is one admission boundary: retain only the last physical
				// value for an identifier so it cannot publish two live updates.
				newDocuments[previousIndex] = cloneDocument(document)
				continue
			}
			newDocumentIndexes[key] = len(newDocuments)
			prepared.deletes = append(prepared.deletes, document.Identifier)
		}
		newDocuments = append(newDocuments, cloneDocument(document))
	}
	// The segment identifier is assigned when the batch is published.
	segment, segmentErr := newMemorySegment(newDocuments, 0, identifierDocValues)
	if segmentErr != nil {
		return preparedBatch{}, segmentErr
	}
	prepared.segment = segment
	prepared.documents = uint64(len(newDocuments))
	prepared.identifiers = make([][]byte, len(newDocuments))
	for documentIndex := range newDocuments {
		prepared.identifiers[documentIndex] = newDocuments[documentIndex].Identifier
	}
	return prepared, nil
}

// publishBatchLocked applies a prepared batch to the current root: it
// removes every live document the batch replaces, then appends the batch's
// segment. It takes ownership of prepared.segment.
func (o *Owner) publishBatchLocked(prepared preparedBatch) (*publishedRoot, error) {
	if o.root.generation == ^uint64(0) {
		prepared.release()
		return nil, fmt.Errorf("native generation exhausted: %w", ErrInvalidDocument)
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
	var positions map[uint64]int
	for _, identifier := range prepared.deletes {
		deleteAt := func(segmentIndex int) error {
			current := next.segments[segmentIndex]
			updated, changed, deleteErr := current.Delete(identifier)
			if deleteErr != nil {
				return deleteErr
			}
			if changed {
				current.release()
				next.segments[segmentIndex] = updated
			}
			return nil
		}
		var deleteErr error
		if holders := o.admittedIdentifiers[string(identifier)]; len(holders) > 0 {
			if positions == nil {
				positions = segmentPositions(next.segments)
			}
			for _, segmentID := range holders {
				if segmentIndex, found := positions[segmentID]; found {
					if deleteErr = deleteAt(segmentIndex); deleteErr != nil {
						break
					}
				}
			}
		}
		for segmentIndex := 0; deleteErr == nil && segmentIndex < len(next.segments); segmentIndex++ {
			if segment, ok := next.segments[segmentIndex].(*memorySegment); ok {
				if _, indexed := o.admittedSegments[segment.handle.id]; indexed {
					continue
				}
			}
			deleteErr = deleteAt(segmentIndex)
		}
		if deleteErr != nil {
			releaseSegments(next.segments)
			prepared.release()
			return nil, deleteErr
		}
	}
	if prepared.segment == nil {
		return next, nil
	}
	if o.nextSegmentID == ^uint64(0) {
		releaseSegments(next.segments)
		prepared.release()
		return nil, fmt.Errorf("native segment identifiers exhausted: %w", ErrInvalidDocument)
	}
	if prepared.documents > ^uint64(0)-next.nextNumber {
		releaseSegments(next.segments)
		prepared.release()
		return nil, fmt.Errorf("native document numbering exhausted: %w", ErrInvalidDocument)
	}
	prepared.segment.(*memorySegment).handle.id = o.nextSegmentID
	o.nextSegmentID++
	next.segments = append(next.segments, prepared.segment)
	next.nextNumber += prepared.documents
	return next, nil
}

func segmentPositions(segments []rootSegment) map[uint64]int {
	positions := make(map[uint64]int, len(segments))
	for segmentIndex, current := range segments {
		if segment, ok := current.(*memorySegment); ok {
			positions[segment.handle.id] = segmentIndex
		}
	}
	return positions
}

// indexAdmittedLocked records the identifiers of a just-published admitted
// segment.
func (o *Owner) indexAdmittedLocked(segmentID uint64, identifiers [][]byte) {
	if len(identifiers) == 0 {
		return
	}
	if o.admittedSegments == nil {
		o.admittedSegments = make(map[uint64][][]byte)
		o.admittedIdentifiers = make(map[string][]uint64)
	}
	o.admittedSegments[segmentID] = identifiers
	for _, identifier := range identifiers {
		key := string(identifier)
		holders := o.admittedIdentifiers[key]
		if len(holders) == 0 || holders[len(holders)-1] != segmentID {
			o.admittedIdentifiers[key] = append(holders, segmentID)
		}
	}
}

// unindexSegmentLocked drops a segment that left the root from the
// admitted-identifier index.
func (o *Owner) unindexSegmentLocked(segmentID uint64) {
	identifiers, indexed := o.admittedSegments[segmentID]
	if !indexed {
		return
	}
	delete(o.admittedSegments, segmentID)
	for _, identifier := range identifiers {
		key := string(identifier)
		holders := o.admittedIdentifiers[key]
		kept := holders[:0]
		for _, holder := range holders {
			if holder != segmentID {
				kept = append(kept, holder)
			}
		}
		if len(kept) == 0 {
			delete(o.admittedIdentifiers, key)
		} else {
			o.admittedIdentifiers[key] = kept
		}
	}
}

func releaseSegments(segments []rootSegment) {
	for _, current := range segments {
		current.release()
	}
}

func segmentHasNoLiveDocuments(segment *memorySegment) bool {
	return segment.handle.count == 0 || uint64(len(segment.deleted)) >= segment.handle.count
}

func newMemorySegment(documents []Document, segmentID uint64, identifierDocValues bool) (rootSegment, error) {
	encoded := nativeice.Generation{
		Documents: make([]nativeice.EncodeDocument, 0, len(documents)), IdentifierDocValues: identifierDocValues,
	}
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
	handle := &segmentHandle{
		reader: reader, payload: payload, count: uint64(len(documents)), id: segmentID, size: uint64(len(payload)),
	}
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
	// Best effort: a failure here simply resurfaces, and is reported, on
	// the first lookup that then rebuilds the filter lazily instead.
	_ = reader.PrepareTermFilter(identifierField)
	return &memorySegment{handle: handle}, nil
}

func newSegmentFromPayload(payload []byte, segmentID uint64) (rootSegment, error) {
	reader, openErr := nativeice.OpenSegmentBorrowed(payload)
	if openErr != nil {
		return nil, openErr
	}
	// The reader borrows payload, which the caller hands over; the handle
	// keeps it alive for the reader's lifetime.
	// A merged segment records its time bounds in the footer, so read them here
	// too. Without this the in-memory merge path leaves a merged segment
	// reporting "no timestamps", which is what made segment-level time pruning
	// useless for exactly the segments that hold most of the data.
	timeMin, timeMax := reader.TimeBounds()
	handle := &segmentHandle{
		reader: reader, payload: payload, count: reader.DocumentCount(),
		id: segmentID, size: uint64(len(payload)), persisted: atomic.Bool{},
		timeMin: uint64(timeMin), timeMax: uint64(timeMax),
		hasTime: timeMin != 0 || timeMax != 0,
	}
	handle.refs.Store(1)
	if fields, fieldsErr := reader.Fields(); fieldsErr == nil {
		handle.indexedFields = fields
	}
	// Best effort, see newMemorySegment. A merged segment is exactly the
	// case this warm-up matters most for: it is opened once (here, before
	// the owner lock is taken) and then stays live for a long time.
	_ = reader.PrepareTermFilter(identifierField)
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
		reader: reader, sourcePath: path, staged: true, count: metadata.DocumentCount, id: segmentID,
		size: metadata.Size, timeMin: uint64(timeMin), timeMax: uint64(timeMax),
		hasTime: timeMin != 0 || timeMax != 0, indexedFields: fields,
	}
	handle.refs.Store(1)
	// Best effort, see newMemorySegment. The external-receive caller opens
	// this before taking o.mu (introduceExternalSegment), so the warm-up
	// runs outside the lock there too.
	_ = reader.PrepareTermFilter(identifierField)
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

// hasLivePosting reports whether identifier has at least one undeleted
// posting in this segment, without decoding any stored field.
//
//nolint:contextcheck // nativeice exact posting lookup is bounded and synchronous.
func (s *memorySegment) hasLivePosting(identifier []byte) (bool, error) {
	posting, found, err := s.handle.reader.TermPosting(identifierField, identifier)
	if err != nil || !found {
		return false, err
	}
	for _, documentIndex := range postingDocuments(posting) {
		if _, deleted := s.deleted[documentIndex]; !deleted {
			return true, nil
		}
	}
	return false, nil
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
	// Narrow the term posting to the query's time range before visiting any
	// document, so a segment the range misses costs no decode and a segment it
	// partly covers does not decode the documents it excludes.
	class := classifyTime(s.handle, request.TimeRange)
	if class == timeDisjoint {
		return MatchResult{}, nil
	}
	timeExact := false
	if class == timeOverlap {
		usable, usableErr := s.handle.trieUsable()
		if usableErr == nil && usable {
			inRange, narrowErr := trieCandidates(ctx, s, request.TimeRange)
			if narrowErr != nil {
				return MatchResult{}, narrowErr
			}
			posting = narrowTermPostingToBitmap(posting, inRange)
			timeExact = true
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
		if request.TimeRange != nil {
			if !hasTimestamp {
				continue
			}
			if !timeExact && !request.TimeRange.contains(timestamp) {
				continue
			}
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
		handle := s.handle
		path := handle.clearSourcePath()
		handle.payload = nil
		// Closing the reader and deleting a staged source are file I/O, and
		// the last reference often goes while o.mu is held (a new root
		// replacing the old one), so they run on their own goroutine.
		segmentDisposals.run(func() {
			_ = handle.reader.Close()
			if path != "" {
				// Close does not wait for operations in flight on the
				// reader -- a merge reading it, say -- so the staged file is
				// deleted only once nothing can read it.
				handle.reader.WhenReleased(func(error) { _ = fileSystem.DeleteFile(path) })
			}
		})
	}
}

// segmentDisposals runs the disposal of released segment handles off the
// goroutine that released them, and lets Owner.Close wait, outside o.mu, for
// the disposals issued before it.
var segmentDisposals = newDisposals()

type disposals struct {
	pending map[uint64]struct{}
	cond    *sync.Cond
	mu      sync.Mutex
	next    uint64
}

func newDisposals() *disposals {
	d := &disposals{pending: make(map[uint64]struct{})}
	d.cond = sync.NewCond(&d.mu)
	return d
}

func (d *disposals) run(dispose func()) {
	d.mu.Lock()
	ticket := d.next
	d.next++
	d.pending[ticket] = struct{}{}
	d.mu.Unlock()
	go func() {
		defer func() {
			_ = recover()
			d.mu.Lock()
			delete(d.pending, ticket)
			d.cond.Broadcast()
			d.mu.Unlock()
		}()
		dispose()
	}()
}

// wait returns once every disposal issued before it ran; later ones, of
// this or other owners, do not hold it up.
func (d *disposals) wait() {
	d.mu.Lock()
	defer d.mu.Unlock()
	limit := d.next
	for {
		earlier := false
		for ticket := range d.pending {
			if ticket < limit {
				earlier = true
				break
			}
		}
		if !earlier {
			return
		}
		d.cond.Wait()
	}
}

// narrowTermPostingToBitmap restricts a term posting to the documents in
// bitmap.
//
// It narrows Document numbers because that is what postingDocuments, the only
// reader of a posting here, iterates: the Bitmap field exists for engines that
// can combine postings directly, but this path does not. Filtering in place
// keeps the allocation free.
func narrowTermPostingToBitmap(posting nativeice.TermPosting, bitmap *roaringpkg.Bitmap) nativeice.TermPosting {
	if posting.OneHit {
		if !bitmap.Contains(uint32(posting.DocumentNumber)) {
			posting.OneHit = false
			posting.DocumentNumber = 0
			posting.Documents = nil
		}
		return posting
	}
	kept := posting.Documents[:0]
	for _, number := range posting.Documents {
		if bitmap.Contains(uint32(number)) {
			kept = append(kept, number)
		}
	}
	posting.Documents = kept
	return posting
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
//
// Close takes no context parameter by design: its final persistence flush
// must run to completion regardless of any caller's own cancellation, the
// same way draining activeOps and the maintenance goroutine already do, so
// it uses context.Background() rather than one.
//
//nolint:contextcheck // see comment above.
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
	queue := o.persistQ
	for o.activeOps != 0 {
		o.stateCond.Wait()
	}
	maintenanceCancel := o.maintenanceCancel
	maintenanceTask := o.maintenanceTask
	persistenceCancel := o.persistenceCancel
	persistenceTask := o.persistenceTask
	o.mu.Unlock()
	if maintenanceCancel != nil {
		maintenanceCancel()
	}
	if maintenanceTask != nil {
		<-maintenanceTask.Done()
	}
	o.abortExternalStreamers()
	if persistenceCancel != nil {
		// Stop the background worker before the final, synchronous drain
		// below so the two never race over the same root.
		persistenceCancel()
	}
	if persistenceTask != nil {
		<-persistenceTask.Done()
	}
	if queue != nil {
		// Anything admitted but not yet durable -- including whatever the
		// background worker was still coalescing when it was canceled --
		// gets exactly one more chance to persist and fire its callback
		// before the root it depends on is released below. Close takes no
		// context of its own to forward: this final flush must run to
		// completion regardless of any caller's cancellation, the same way
		// draining activeOps and the maintenance/persistence goroutines above
		// already do.
		o.drainPersistence(context.Background(), 0)
	}
	o.mu.Lock()
	oldRoot := o.root
	o.root = nil
	o.admittedIdentifiers, o.admittedSegments = nil, nil
	oldRoot.release()
	o.pruneRootsLocked()
	o.mu.Unlock()
	// The released segments are disposed of off o.mu; Close returns once
	// their files are closed.
	segmentDisposals.wait()
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
