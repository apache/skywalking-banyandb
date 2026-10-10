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

// Package nativeice reads the ICE v3 segment and snapshot v3 manifest grammar
// defined by BDB-NIDX-SPEC-001 revision 0.2 sections 08 and 09, using only
// BanyanDB code. It is the bounded read-only container reader that the
// read-only production paths in pkg/index/native open committed index
// directories through, and it never depends on the retired index libraries.
//
// The package is deliberately reachable only from pkg/index/native and its
// legacy adapter. Footer,
// offset, mapping, and section decoder types are private to it; the contract
// other packages observe is the behavior of the exported functions in
// pkg/index/native that call it.
package nativeice

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	roaringpkg "github.com/RoaringBitmap/roaring"
	"github.com/blevesearch/vellum"
	"github.com/klauspost/compress/s2"

	"github.com/apache/skywalking-banyandb/pkg/fs"
)

const (
	segmentFooterLength = 60
	segmentVersion      = 3
	snapshotVersion     = 3
	maxManifestSize     = 16 << 20
	maxFieldsIndexCount = 1 << 20
	maxDirectoryEntries = 1 << 16
	directoryReadSize   = maxDirectoryEntries + 1
	maxOpenAttempts     = 2

	storedDocumentsPerChunk       = 128
	maxStoredChunkCount           = 1 << 20
	maxStoredChunkTableSize       = 16 << 20
	maxStoredCompressedChunkSize  = 16 << 20
	maxStoredDecodedChunkSize     = 64 << 20
	maxStoredFieldNameLength      = 64 << 10
	maxStoredFieldsPerDocument    = 1 << 20
	segmentFooterPayloadLength    = segmentFooterLength - 4
	storedChunkTableFooterLength  = 8
	storedDocumentOffsetByteWidth = 8
	fieldsIndexAddressByteWidth   = 8
)

// ErrCorrupt is the sentinel that every structural rejection wraps: a footer,
// record framing, count, length, or section offset that violates the ICE v3 or
// snapshot v3 grammar, or that would require reading or allocating past a
// configured bound. Callers classify with errors.Is.
var ErrCorrupt = errors.New("nativeice: corrupt index")

// ErrNoSnapshot is the sentinel reported when a directory holds no committed
// generation at all: it is absent, empty, or was never flushed. It is distinct
// from ErrCorrupt because nothing is damaged -- there is simply nothing
// committed to read.
var ErrNoSnapshot = errors.New("nativeice: no committed snapshot")

var errManifestTooLarge = errors.New("nativeice: snapshot exceeds read limit")

// Reader is a bounded read-only handle on exactly one generation of an index
// directory. The generation is chosen at Open and fixed for the Reader's
// lifetime, so generations committed afterwards stay invisible to it.
//
//nolint:govet // reader fields are grouped by ownership and synchronization role.
type Reader struct {
	closeErr             error
	repairPageSortFields [repairSortFieldCount]string
	segments             []pinnedSegment
	repairPageReaders    []*repairSegmentPageReader
	snapshotID           uint64
	visibleDocCount      int64
	repairPageMu         sync.Mutex
	docValueMu           sync.Mutex
	docValueReaders      map[string]*repairDocValueReader
	closeOnce            sync.Once
	// releaseMu guards afterRelease, the functions to run once the files
	// are closed, and filesClosed, which records that they are.
	releaseMu    sync.Mutex
	afterRelease []func(error)
	filesClosed  bool
	active       atomic.Int64
	closed       atomic.Bool
	// released is set once the release of the files began; see Close.
	released  atomic.Bool
	segmentMu sync.Mutex
	// segmentReaders holds each segment's stored reader once built, read
	// without a lock on the per-lookup path.
	segmentReaders atomic.Pointer[[]atomic.Pointer[storedSegmentReader]]
	// visitDocumentCalls counts every VisitDocument call, successful or not.
	// It costs one uncontended atomic add per call and exists so a test can
	// prove a code path decoded zero documents -- for example, that a segment
	// a time range disjoints from is never asked to visit one.
	visitDocumentCalls atomic.Int64
}

// VisitDocumentCalls returns how many times VisitDocument has been called on
// this Reader. It is test instrumentation: production code never reads it.
func (r *Reader) VisitDocumentCalls() int64 {
	return r.visitDocumentCalls.Load()
}

// SnapshotSegment is the immutable metadata a snapshot manifest records for
// one segment. DeletionBitmap is copied when metadata is exported and must be
// treated as read-only by callers.
//
//nolint:govet // manifest scalar fields stay grouped for wire-format clarity.
type SnapshotSegment struct {
	ID             uint64
	Size           uint64
	DocumentCount  uint64
	TimeMin        uint64
	TimeMax        uint64
	DeletionBitmap []byte
}

// SnapshotMetadata describes the committed generation pinned by a Reader.
// It contains manifest metadata only; segment payloads remain file-backed by
// the Reader and are not materialized by this method.
//
//nolint:govet // manifest identity and records stay adjacent for API clarity.
type SnapshotMetadata struct {
	ID       uint64
	Segments []SnapshotSegment
}

// SnapshotMetadata returns a copy of the pinned snapshot's manifest metadata.
// The returned deletion bitmaps are owned by the caller and may be retained
// until the Reader closes.
func (r *Reader) SnapshotMetadata() SnapshotMetadata {
	if r == nil {
		return SnapshotMetadata{}
	}
	result := SnapshotMetadata{ID: r.snapshotID, Segments: make([]SnapshotSegment, len(r.segments))}
	for segmentIndex, segment := range r.segments {
		result.Segments[segmentIndex] = SnapshotSegment{
			ID:             segment.record.id,
			Size:           segment.size,
			DocumentCount:  segment.record.documentCount,
			TimeMin:        segment.record.timeMin,
			TimeMax:        segment.record.timeMax,
			DeletionBitmap: append([]byte(nil), segment.record.deletionBitmap...),
		}
	}
	return result
}

// SegmentCount returns the number of segments in the pinned snapshot.
func (r *Reader) SegmentCount() int {
	if r == nil {
		return 0
	}
	return len(r.segments)
}

// TimeBounds returns the timestamp bounds recorded by the segment footer.
func (r *Reader) TimeBounds() (int64, int64) {
	if len(r.segments) == 0 {
		return 0, 0
	}
	return int64(r.segments[0].record.timeMin), int64(r.segments[0].record.timeMax)
}

// StoredDocument is one live document of the pinned generation, borrowed for
// the duration of a single walk callback.
type StoredDocument interface {
	// VisitStoredFields calls visit once for every stored value the document
	// records, passing the field's name and its raw value bytes. A field the
	// document records more than once is visited once per recorded value, in
	// the order the document records them. Visiting stops early when visit
	// returns false.
	//
	// The name and value handed to visit are borrowed from the reader's decode
	// buffers and stay valid only until visit returns; a caller that keeps
	// either beyond that copies it.
	VisitStoredFields(visit func(name string, value []byte) bool) error
}

// VisitLiveDocuments streams the pinned generation's live documents to visit,
// one at a time, in ascending segment and local document order. Documents the
// pinned snapshot's deletion masks cover are skipped, so a deleted document is
// never handed to visit.
//
// The StoredDocument handed to visit is borrowed: it, and every name and value
// it yields, stay valid only until visit returns. At most one document plus the
// reader's configured decode buffers are resident at a time, so the walk's
// memory does not grow with the generation's size.
//
// The walk stops and returns ctx.Err() when ctx is canceled between two
// documents or two stored chunks, and stops and returns visit's error when
// visit fails. A stored section whose chunk table, offsets, lengths, varints or
// field identifiers violate the ICE v3 grammar, or that would require decoding
// past a configured bound, stops the walk with an error wrapping ErrCorrupt.
func (r *Reader) VisitLiveDocuments(ctx context.Context, visit func(StoredDocument) error) error {
	if useErr := r.use(); useErr != nil {
		return useErr
	}
	defer r.endUse()
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	for segmentIndex := range r.segments {
		if visitErr := walkStoredSegment(ctx, r.segments[segmentIndex], visit); visitErr != nil {
			return visitErr
		}
	}
	return nil
}

// VisitPhysicalDocuments streams every physical document in the pinned
// generation. The deleted argument reports the snapshot deletion mask for the
// document; unlike VisitLiveDocuments this method does not hide deleted
// documents. Documents are visited in ascending segment and local document
// order and are borrowed for the callback duration.
func (r *Reader) VisitPhysicalDocuments(ctx context.Context, visit func(StoredDocument, bool) error) error {
	if useErr := r.use(); useErr != nil {
		return useErr
	}
	defer r.endUse()
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	for segmentIndex := range r.segments {
		segment := r.segments[segmentIndex]
		storedReader, readerErr := r.storedReader(segmentIndex)
		if readerErr != nil {
			return readerErr
		}
		deleted, deletionErr := deletedDocuments(segment.record)
		if deletionErr != nil {
			return deletionErr
		}
		if visitErr := storedReader.visitPhysical(ctx, deleted, visit); visitErr != nil {
			return visitErr
		}
	}
	return nil
}

// Open selects the newest committed generation in the index directory at path,
// validates its snapshot manifest and every segment that manifest references
// against the grammar and the reader's configured bounds, and returns a Reader
// pinned to that generation.
//
// Open takes no exclusive directory lock and creates, removes, or modifies no
// file, so a directory a live writer owns can be inspected while it is being
// written. Directory entries outside the grammar, the writer lock file among
// them, are ignored. Snapshot and segment identifiers are numbered
// independently, so the newest generation is chosen by decoding the manifest
// rather than by pairing file names.
//
// A directory holding no committed generation reports an error wrapping
// ErrNoSnapshot. Open skips each generation whose manifest or referenced
// segments fail structural validation and pins the newest complete generation.
// It reports an error wrapping ErrCorrupt only when no committed generation
// validates.
func Open(path string) (*Reader, error) {
	return openWithSnapshots(path, committedSnapshots)
}

// OpenStrict opens only the newest committed snapshot in path. Unlike Open,
// it never falls back to an older generation when that newest manifest or one
// of its segments is damaged. Writers use this fail-closed mode at startup so
// an acknowledged generation cannot silently disappear after a restart.
func OpenStrict(path string) (*Reader, error) {
	return openStrict(path)
}

// ReadStrictSnapshotMetadata returns the manifest metadata of the newest
// committed snapshot in path after validating it as a restart would --
// OpenStrict, then OpenSnapshotSegment per segment -- for callers that need
// only the metadata: garbage collection scanning for live segment IDs, for
// example. It keeps no file open and shares nothing with serving readers.
// Every segment must be a regular file, named without following a symbolic
// link (Lstat). For a segment held returns an open Reader of -- one the
// caller already serves from a validated file -- the path must still name
// that very file (os.SameFile against the descriptor's fstat at open) with
// its size, which is checked without opening it. Every other segment is
// opened, checked to be the very file Lstat described, and validated,
// without page-cache advice.
func ReadStrictSnapshotMetadata(path string, held func(segmentID uint64) *Reader) (SnapshotMetadata, error) {
	pin := func(record segmentRecord) (pinnedSegment, uint64, error) {
		entry, lstatErr := segmentFileSystem.Lstat(record.path)
		if lstatErr != nil {
			return pinnedSegment{}, 0, corruptError("inspect segment %q", record.path, lstatErr)
		}
		if !entry.Mode().IsRegular() {
			return pinnedSegment{}, 0, corruptError("segment %q is not a regular file", record.path)
		}
		if held != nil {
			if reader := held(record.id); reader != nil {
				if heldErr := reader.matchesSegmentFile(entry); heldErr != nil {
					return pinnedSegment{}, 0, corruptError("segment %q", record.path, heldErr)
				}
				return pinnedSegment{record: record, size: uint64(entry.Size())}, record.documentCount, nil
			}
		}
		pinned, documentCount, pinErr := pinSegment(record, false)
		if pinErr != nil {
			return pinnedSegment{}, 0, pinErr
		}
		// The file opened must be the one Lstat described: a path swapped
		// for a symbolic link or another file in between is refused.
		validationErr := pinned.file.(*fsSegmentFile).sameAs(entry)
		if validationErr == nil {
			validationErr = validatePinnedSegments([]pinnedSegment{pinned})
		}
		closeErr := pinned.file.Close()
		pinned.file = nil
		if validationErr != nil || closeErr != nil {
			return pinnedSegment{}, 0, errors.Join(validationErr, closeErr)
		}
		return pinned, documentCount, nil
	}
	snapshotID, _, segments, openErr := openStrictWith(path, pin)
	if openErr != nil {
		return SnapshotMetadata{}, openErr
	}
	reader := &Reader{snapshotID: snapshotID, segments: segments}
	return reader.SnapshotMetadata(), nil
}

// errSegmentFileDiffers reports a path that no longer names the file a
// Reader serves.
var errSegmentFileDiffers = errors.New("names a different file than the reader serves")

// matchesSegmentFile reports whether entry, a path's Lstat, describes the
// file the Reader's one segment is served from, with the same size.
func (r *Reader) matchesSegmentFile(entry os.FileInfo) error {
	if len(r.segments) != 1 {
		return errSegmentFileDiffers
	}
	file, ok := r.segments[0].file.(*fsSegmentFile)
	if !ok || file.sameAs(entry) != nil {
		return errSegmentFileDiffers
	}
	return nil
}

func openStrict(path string) (*Reader, error) {
	snapshotID, visibleDocCount, segments, openErr := openStrictWith(path, func(record segmentRecord) (pinnedSegment, uint64, error) {
		return pinSegment(record, true)
	})
	if openErr != nil {
		return nil, openErr
	}
	if validationErr := validatePinnedSegments(segments); validationErr != nil {
		return nil, errors.Join(validationErr, closePinnedSegments(segments))
	}
	return &Reader{snapshotID: snapshotID, visibleDocCount: visibleDocCount, segments: segments}, nil
}

type segmentPinner func(segmentRecord) (pinnedSegment, uint64, error)

func openStrictWith(path string, pin segmentPinner) (uint64, int64, []pinnedSegment, error) {
	snapshotPaths, segmentPaths, snapshotErr := committedSnapshots(path)
	if snapshotErr != nil {
		return 0, 0, nil, snapshotErr
	}
	if len(snapshotPaths) == 0 {
		return 0, 0, nil, fmt.Errorf("open %q: %w", path, ErrNoSnapshot)
	}
	snapshotPath := snapshotPaths[len(snapshotPaths)-1]
	manifest, readErr := readManifest(snapshotPath)
	if readErr != nil {
		return 0, 0, nil, readErr
	}
	snapshotID, validID := parseFinalName(filepath.Base(snapshotPath), ".snp")
	if !validID {
		return 0, 0, nil, corruptError("snapshot %q has an invalid identifier", snapshotPath)
	}
	visibleDocCount, segments, parseErr := parseSnapshotSegments(segmentPaths, manifest, pin)
	if parseErr != nil {
		return 0, 0, nil, parseErr
	}
	return snapshotID, visibleDocCount, segments, nil
}

type snapshotLister func(string) ([]string, map[uint64]string, error)

func openWithSnapshots(path string, listSnapshots snapshotLister) (*Reader, error) {
	var lastCandidateErr error
	var lastCandidatePath string
	for attempt := 0; attempt < maxOpenAttempts; attempt++ {
		snapshotPaths, segmentPaths, snapshotErr := listSnapshots(path)
		if snapshotErr != nil {
			return nil, snapshotErr
		}
		for snapshotIndex := len(snapshotPaths) - 1; snapshotIndex >= 0; snapshotIndex-- {
			snapshotPath := snapshotPaths[snapshotIndex]
			manifest, readErr := readManifest(snapshotPath)
			if readErr != nil {
				lastCandidateErr = fmt.Errorf("read snapshot %q: %w", snapshotPath, readErr)
				lastCandidatePath = snapshotPath
				continue
			}
			visibleDocCount, segments, parseErr := parseSnapshotSegments(segmentPaths, manifest, func(record segmentRecord) (pinnedSegment, uint64, error) {
				return pinSegment(record, true)
			})
			if parseErr != nil {
				lastCandidateErr = parseErr
				lastCandidatePath = snapshotPath
				continue
			}
			snapshotID, validID := parseFinalName(filepath.Base(snapshotPath), ".snp")
			if !validID {
				return nil, corruptError("snapshot %q has an invalid identifier", snapshotPath)
			}
			return &Reader{snapshotID: snapshotID, visibleDocCount: visibleDocCount, segments: segments}, nil
		}
	}
	if lastCandidateErr != nil {
		return nil, corruptError("open %q has no structurally complete snapshot (candidate %q)", path, lastCandidatePath, lastCandidateErr)
	}
	return nil, corruptError("open %q has no structurally complete snapshot", path)
}

// VisibleDocCount returns the number of live documents in the pinned
// generation: the document counts the manifest records for the segments it
// references, less the documents those segments' deletion masks mark.
func (r *Reader) VisibleDocCount() (int64, error) {
	return r.visibleDocCount, nil
}

// SnapshotID returns the identifier of the committed generation the Reader pins.
func (r *Reader) SnapshotID() uint64 {
	return r.snapshotID
}

// ErrReaderClosed reports a read through a Reader, or a handle or iterator
// derived from it, after the Reader closed.
var ErrReaderClosed = errors.New("nativeice: reader is closed")

// use admits one operation on the Reader, failing once Close began. The
// Reader's files stay open until every admitted operation has returned (see
// Close); handles and iterators that outlive an operation check closed
// themselves and report ErrReaderClosed. Admission is two atomic operations:
// incrementing active before reading closed, while Close sets closed before
// reading active, guarantees that one of them sees the other.
func (r *Reader) use() error {
	r.active.Add(1)
	if r.closed.Load() {
		r.endUse()
		return ErrReaderClosed
	}
	return nil
}

// check fails once Close began, without admitting an operation. Point reads
// use it to stay free of shared writes on the per-lookup path. A point read
// that races Close still reads safely: a segment file's descriptor stays
// open until the reads in flight on it end (see fsSegmentFile), and a read
// after the file closed reports ErrReaderClosed.
func (r *Reader) check() error {
	if r.closed.Load() {
		return ErrReaderClosed
	}
	return nil
}

// endUse ends an admitted operation; the last one to end after Close
// releases the files. The release's error never becomes the operation's: it
// is delivered through WhenReleased.
func (r *Reader) endUse() {
	if r.active.Add(-1) == 0 && r.closed.Load() {
		_ = r.release()
	}
}

// Close refuses new operations at once and never waits for one. If an
// operation admitted before Close is still running, Close returns nil at
// once and the last such operation releases the files when it ends;
// otherwise Close releases them itself before returning -- closing the
// descriptors and running the WhenReleased hooks -- and returns the error
// closing them reported. Close may therefore be called from any goroutine,
// a callback of the Reader's own operations or a release hook included,
// without deadlock. Operations started after Close, and handles and
// iterators used after it, report ErrReaderClosed; so do point reads
// (lookups, VisitDocument) racing the release. Close is idempotent: once
// the files are released, it returns the same error.
//
// Because the release may happen after Close returns, a caller that must
// act once the files are closed -- deleting one, say -- does so through
// WhenReleased, which also delivers the release's error.
func (r *Reader) Close() error {
	r.closed.Store(true)
	if r.active.Load() > 0 {
		return nil
	}
	return r.release()
}

// WhenReleased runs fn with the release's error once the Reader's files are
// closed: at once, on the caller, if they already are, otherwise right
// after the release on the goroutine performing it. A panicking hook is
// recovered and does not keep the others from running.
func (r *Reader) WhenReleased(fn func(error)) {
	r.releaseMu.Lock()
	if !r.filesClosed {
		r.afterRelease = append(r.afterRelease, fn)
		r.releaseMu.Unlock()
		return
	}
	closeErr := r.closeErr
	r.releaseMu.Unlock()
	runReleaseHook(fn, closeErr)
}

// releaseState reports whether the Reader's files are released yet, and the
// error releasing them reported.
func (r *Reader) releaseState() (bool, error) {
	r.releaseMu.Lock()
	defer r.releaseMu.Unlock()
	return r.filesClosed, r.closeErr
}

// release closes the Reader's files once, then runs the release hooks
// outside the once guard, so a hook that calls Close returns rather than
// waiting for the release it runs within.
func (r *Reader) release() error {
	var hooks []func(error)
	r.closeOnce.Do(func() {
		r.released.Store(true)
		r.repairPageMu.Lock()
		r.repairPageReaders = nil
		r.repairPageSortFields = [repairSortFieldCount]string{}
		r.repairPageMu.Unlock()
		if readers := r.segmentReaders.Load(); readers != nil {
			for index := range *readers {
				if storedReader := (*readers)[index].Load(); storedReader != nil {
					storedReader.close()
				}
			}
		}
		r.docValueMu.Lock()
		r.docValueReaders = nil
		r.docValueMu.Unlock()
		closeErr := closePinnedSegments(r.segments)
		r.releaseMu.Lock()
		r.closeErr = closeErr
		r.filesClosed = true
		hooks = r.afterRelease
		r.afterRelease = nil
		r.releaseMu.Unlock()
	})
	r.releaseMu.Lock()
	closeErr := r.closeErr
	r.releaseMu.Unlock()
	for _, hook := range hooks {
		runReleaseHook(hook, closeErr)
	}
	return closeErr
}

// runReleaseHook runs one release hook, recovering a panic.
func runReleaseHook(hook func(error), closeErr error) {
	defer func() { _ = recover() }()
	hook(closeErr)
}

func committedSnapshots(path string) ([]string, map[uint64]string, error) {
	entries, readErr := readDirectoryEntries(path)
	if readErr != nil {
		if errors.Is(readErr, ErrNoSnapshot) {
			return nil, nil, fmt.Errorf("nativeice: open %q: %w", path, ErrNoSnapshot)
		}
		return nil, nil, readErr
	}
	type snapshot struct {
		path string
		id   uint64
	}
	var snapshots []snapshot
	segmentPaths := make(map[uint64]string)
	for _, entry := range entries {
		if !entry.Type().IsRegular() {
			continue
		}
		id, valid := parseFinalName(entry.Name(), ".snp")
		if valid {
			snapshots = append(snapshots, snapshot{path: filepath.Join(path, entry.Name()), id: id})
			continue
		}
		id, valid = parseFinalName(entry.Name(), ".seg")
		if !valid {
			continue
		}
		if _, exists := segmentPaths[id]; exists {
			return nil, nil, corruptError("duplicate segment identifier in %q", path)
		}
		segmentPaths[id] = filepath.Join(path, entry.Name())
	}
	if len(snapshots) == 0 {
		return nil, nil, fmt.Errorf("nativeice: open %q: %w", path, ErrNoSnapshot)
	}
	sort.Slice(snapshots, func(left, right int) bool {
		return snapshots[left].id < snapshots[right].id
	})
	if len(snapshots) > 1 && snapshots[len(snapshots)-1].id == snapshots[len(snapshots)-2].id {
		return nil, nil, corruptError("duplicate snapshot identifier in %q", path, nil)
	}
	snapshotPaths := make([]string, len(snapshots))
	for snapshotIndex, snapshot := range snapshots {
		snapshotPaths[snapshotIndex] = snapshot.path
	}
	return snapshotPaths, segmentPaths, nil
}

func readDirectoryEntries(path string) ([]fs.DirEntry, error) {
	entries, readErr := segmentFileSystem.ReadDirLimit(path, directoryReadSize)
	if errors.Is(readErr, os.ErrNotExist) {
		return nil, ErrNoSnapshot
	}
	if readErr != nil {
		return nil, corruptError("read index directory %q", path, readErr)
	}
	if len(entries) > maxDirectoryEntries {
		return nil, corruptError("index directory %q contains more than %d entries", path, maxDirectoryEntries)
	}
	return entries, nil
}

func parseSnapshotSegments(segmentPaths map[uint64]string, payload []byte, pin segmentPinner) (visibleDocCount int64, resultSegments []pinnedSegment, err error) {
	if len(payload) < 4 {
		return 0, nil, corruptError("snapshot is shorter than its reserved CRC32", nil)
	}
	decoder := byteDecoder{payload: payload[:len(payload)-4]}
	version, versionErr := decoder.uvarint()
	if versionErr != nil {
		return 0, nil, versionErr
	}
	if version != snapshotVersion {
		return 0, nil, corruptError("unsupported snapshot version %d", version)
	}
	segmentCount, countErr := decoder.uvarint()
	if countErr != nil {
		return 0, nil, countErr
	}
	if segmentCount > uint64(len(decoder.payload)) {
		return 0, nil, corruptError("snapshot segment count %d exceeds remaining bytes", segmentCount)
	}
	segments := make([]pinnedSegment, 0, int(segmentCount))
	defer func() {
		if err != nil {
			err = errors.Join(err, closePinnedSegments(segments))
		}
	}()
	for segmentIndex := uint64(0); segmentIndex < segmentCount; segmentIndex++ {
		record, recordErr := decoder.segmentRecord(segmentPaths)
		if recordErr != nil {
			return 0, nil, recordErr
		}
		pinnedRecord, segmentDocCount, segmentErr := pin(record)
		if segmentErr != nil {
			return 0, nil, segmentErr
		}
		segments = append(segments, pinnedRecord)
		if segmentDocCount != record.documentCount {
			return 0, nil, corruptError("segment %d document count differs from snapshot", record.id)
		}
		deletedCount, deletionErr := deletionCount(record.deletionBitmap, record.documentCount)
		if deletionErr != nil {
			return 0, nil, deletionErr
		}
		if record.documentCount > uint64(math.MaxInt64) || deletedCount > record.documentCount {
			return 0, nil, corruptError("invalid document count for segment %d", record.id)
		}
		segmentVisibleCount := int64(record.documentCount - deletedCount)
		if segmentVisibleCount > math.MaxInt64-visibleDocCount {
			return 0, nil, corruptError("visible document count overflows int64", nil)
		}
		visibleDocCount += segmentVisibleCount
	}
	if decoder.remaining() != 0 {
		return 0, nil, corruptError("snapshot has trailing bytes before its reserved CRC32", nil)
	}
	return visibleDocCount, segments, nil
}

type segmentRecord struct {
	path           string
	deletionBitmap []byte
	documentCount  uint64
	id             uint64
	timeMin        uint64
	timeMax        uint64
}

type pinnedSegment struct {
	file   segmentFile
	record segmentRecord
	size   uint64
}

type segmentFile interface {
	ReadAt([]byte, int64) (int, error)
	Close() error
}

// byteSegmentFile serves an admitted, not yet persisted segment from its
// encoded payload. Every persisted segment is an fsSegmentFile.
type byteSegmentFile struct {
	data []byte
}

func (f *byteSegmentFile) ReadAt(destination []byte, offset int64) (int, error) {
	if offset < 0 || offset >= int64(len(f.data)) {
		return 0, io.EOF
	}
	read := copy(destination, f.data[offset:])
	if read != len(destination) {
		return read, io.EOF
	}
	return read, nil
}

// segmentFileSystem is the sole fs.FileSystem instance nativeice uses to
// open and read segment files; it is stateless (a thin
// wrapper over the os package plus a logger), so one package-level instance
// is equivalent to constructing one per call.
var segmentFileSystem fs.FileSystem = fs.NewLocalFileSystem()

func (*byteSegmentFile) Close() error { return nil }

// RenameSegmentFile renames the file a single-segment Reader was opened from
// to newPath, keeping the Reader serving it. It is how an owner publishes a
// staged segment under its final name without reopening it.
func (r *Reader) RenameSegmentFile(newPath string) error {
	if useErr := r.use(); useErr != nil {
		return useErr
	}
	defer r.endUse()
	if len(r.segments) != 1 {
		return fmt.Errorf("nativeice: rename requires one segment, got %d", len(r.segments))
	}
	if file, ok := r.segments[0].file.(*fsSegmentFile); ok {
		return file.renameTo(newPath)
	}
	return segmentFileSystem.Rename(r.segments[0].record.path, newPath)
}

// DecodedField is one stored value read from a native ICE segment.
type DecodedField struct {
	Name  string
	Value []byte
}

// DecodedDocument is one physical document read from a native ICE segment.
type DecodedDocument struct {
	Fields []DecodedField
}

// SegmentTermDocuments identifies the physical documents containing a term.
type SegmentTermDocuments struct {
	DocumentNumber []uint64
	Segment        uint64
}

// TermFrequency is one document's stored occurrence frequency for a term.
type TermFrequency struct {
	DocumentNumber uint64
	Frequency      uint64
}

// TermPosting is the exact posting for one dictionary term. OneHit is set for
// the compact single-document encoding, allowing callers to consume it without
// allocating document and frequency slices.
//
//nolint:govet // keep scalar one-hit fields adjacent to the general posting slices.
type TermPosting struct {
	OneHit bool
	// Bitmap is the caller-owned multi-document posting bitmap. It is exposed
	// so query engines that support bitmap optimization can combine postings
	// without materializing a document slice first.
	Bitmap         *roaringpkg.Bitmap
	Documents      []uint64
	Frequencies    []TermFrequency
	DocumentNumber uint64
}

// Dictionary is a single-segment exact-term handle bound to one field's
// term dictionary. The parent Reader remains responsible for the
// dictionary's lifetime.
type Dictionary struct {
	owner *storedSegmentReader
	index termIndex
	field string
}

// Dictionary opens a field dictionary without retaining a reader lock or
// looking up field state on each exact-term operation.
func (r *Reader) Dictionary(field string) (*Dictionary, error) {
	if useErr := r.check(); useErr != nil {
		return nil, useErr
	}
	dictionary, dictionaryErr := r.DictionaryValue(field)
	if dictionaryErr != nil {
		return nil, dictionaryErr
	}
	return &dictionary, nil
}

// DictionaryValue opens a field dictionary as an embeddable value. It avoids
// a second heap object when a segment adapter stores the handle in its own
// per-search dictionary wrapper.
func (r *Reader) DictionaryValue(field string) (Dictionary, error) {
	if useErr := r.check(); useErr != nil {
		return Dictionary{}, useErr
	}
	if len(r.segments) != 1 {
		return Dictionary{}, fmt.Errorf("nativeice: dictionary requires one segment, got %d", len(r.segments))
	}
	owner, readerErr := r.storedReader(0)
	if readerErr != nil {
		return Dictionary{}, readerErr
	}
	index, dictionaryErr := owner.dictionary(field)
	if dictionaryErr != nil || index == nil {
		return Dictionary{owner: owner, field: field}, dictionaryErr
	}
	return Dictionary{owner: owner, field: field, index: index}, nil
}

// Close releases no shared state; the parent Reader owns the dictionary FST.
func (d *Dictionary) Close() error { return nil }

// Bound reports whether this handle resolved an indexed field dictionary.
func (d *Dictionary) Bound() bool { return d.index != nil }

// TermPosting resolves one exact term through this handle's reusable decoder.
func (d *Dictionary) TermPosting(term []byte) (TermPosting, bool, error) {
	if d.index == nil {
		return TermPosting{}, false, nil
	}
	if d.owner.isClosed() {
		return TermPosting{}, false, ErrReaderClosed
	}
	return d.owner.termPostingFrom(d.index, term)
}

// TermExists reports whether an exact term is present in this dictionary.
func (d *Dictionary) TermExists(term []byte) (bool, error) {
	if d.index == nil {
		return false, nil
	}
	if d.owner.isClosed() {
		return false, ErrReaderClosed
	}
	_, found, lookupErr := lookupTermPosting(d.index, term)
	if lookupErr != nil {
		return false, lookupError(d.owner.path, lookupErr)
	}
	return found, nil
}

// SegmentTermFrequencies identifies term frequencies in one segment.
type SegmentTermFrequencies struct {
	Values  []TermFrequency
	Segment uint64
}

// SegmentDocValues contains one field's values in physical document order.
// Missing values are represented by nil entries.
type SegmentDocValues struct {
	Values  [][][]byte
	Segment uint64
}

// OpenSegment validates and opens one immutable in-memory ICE segment. The
// payload is copied, so callers may reuse it after this returns.
func OpenSegment(payload []byte) (*Reader, error) {
	owned := append([]byte(nil), payload...)
	return openSegmentBytes(owned)
}

// OpenSegmentBorrowed opens an immutable segment view without copying payload.
// The caller must keep payload unchanged and live until Reader.Close returns.
// It is intended for adapters whose segment data already has that lifetime;
// callers needing ownership should use OpenSegment.
func OpenSegmentBorrowed(payload []byte) (*Reader, error) {
	return openSegmentBytes(payload)
}

func openSegmentBytes(payload []byte) (*Reader, error) {
	file := &byteSegmentFile{data: payload}
	footer, footerErr := readSegmentFooter(file, uint64(len(payload)), "memory")
	if footerErr != nil {
		return nil, footerErr
	}
	record := segmentRecord{path: "memory", documentCount: footer.documentCount, timeMin: footer.timeMin, timeMax: footer.timeMax}
	if _, readerErr := newStoredSegmentReader(file, uint64(len(payload)), record); readerErr != nil {
		return nil, readerErr
	}
	return &Reader{
		segments:        []pinnedSegment{{file: file, record: record, size: uint64(len(payload))}},
		visibleDocCount: int64(footer.documentCount),
	}, nil
}

// DocumentCount returns the number of physical documents in this reader's
// generation. It is useful to segment adapters that preserve physical
// document numbering while decoding individual documents lazily.
func (r *Reader) DocumentCount() uint64 {
	if len(r.segments) != 1 {
		return 0
	}
	return r.segments[0].record.documentCount
}

// storedReader returns segment segmentIndex's stored reader, building it on
// first use. It refuses only once the files were released: an operation
// admitted before Close keeps it until the operation returns.
func (r *Reader) storedReader(segmentIndex int) (*storedSegmentReader, error) {
	if r.released.Load() {
		return nil, ErrReaderClosed
	}
	readers := r.segmentReaders.Load()
	if readers != nil {
		if storedReader := (*readers)[segmentIndex].Load(); storedReader != nil {
			return storedReader, nil
		}
	}
	r.segmentMu.Lock()
	defer r.segmentMu.Unlock()
	if readers = r.segmentReaders.Load(); readers == nil {
		created := make([]atomic.Pointer[storedSegmentReader], len(r.segments))
		readers = &created
		r.segmentReaders.Store(readers)
	}
	if storedReader := (*readers)[segmentIndex].Load(); storedReader != nil {
		return storedReader, nil
	}
	segment := r.segments[segmentIndex]
	reader, readerErr := newStoredSegmentReader(segment.file, segment.size, segment.record)
	if readerErr != nil {
		return nil, readerErr
	}
	reader.owner = r
	// A point read may build this while the Reader releases its files:
	// store it only if the release has not begun, and undo a store the
	// release missed (it marks the Reader released before clearing).
	if r.released.Load() {
		return nil, ErrReaderClosed
	}
	(*readers)[segmentIndex].Store(reader)
	if r.released.Load() {
		(*readers)[segmentIndex].CompareAndSwap(reader, nil)
		reader.close()
		return nil, ErrReaderClosed
	}
	return reader, nil
}

// VisitDocument visits one physical stored document without decoding the
// surrounding generation.
//
// Like a point lookup it is not an admitted operation (see check): it does
// not hold the files open, so if a Close releases them while it reads it
// reports ErrReaderClosed. Its callback may close the Reader.
func (r *Reader) VisitDocument(number uint64, visit func(StoredDocument) error) error {
	r.visitDocumentCalls.Add(1)
	if useErr := r.check(); useErr != nil {
		return useErr
	}
	if len(r.segments) != 1 || number >= r.segments[0].record.documentCount {
		return fmt.Errorf("nativeice: document %d out of range", number)
	}
	storedReader, readerErr := r.storedReader(0)
	if readerErr != nil {
		return readerErr
	}
	return storedReader.visitDocument(number, visit)
}

// DocumentValues returns one document's values for field. Returned bytes are
// owned by the caller.
func (r *Reader) DocumentValues(field string, number uint64) ([][]byte, error) {
	if useErr := r.use(); useErr != nil {
		return nil, useErr
	}
	defer r.endUse()
	if len(r.segments) != 1 || number >= r.segments[0].record.documentCount {
		return nil, fmt.Errorf("nativeice: document %d out of range", number)
	}
	r.docValueMu.Lock()
	defer r.docValueMu.Unlock()
	if r.docValueReaders == nil {
		r.docValueReaders = make(map[string]*repairDocValueReader)
	}
	docValueReader := r.docValueReaders[field]
	if docValueReader == nil {
		fields := [repairSortFieldCount]string{field, "_nativeice_unused_1", "_nativeice_unused_2", "_nativeice_unused_3"}
		pageReader, readerErr := newRepairSegmentPageReader(r.segments[0], fields)
		if readerErr != nil {
			return nil, readerErr
		}
		docValueReader = pageReader.sortReaders[0]
		r.docValueReaders[field] = docValueReader
	}
	if docValueReader == nil {
		return nil, nil
	}
	values, valueErr := docValueReader.values(number)
	if valueErr != nil {
		return nil, valueErr
	}
	result := make([][]byte, len(values))
	for valueIndex, value := range values {
		result[valueIndex] = append([]byte(nil), value...)
	}
	return result, nil
}

// VisitFieldDocumentValues streams one segment's values in physical document
// order. Values are borrowed until visit returns; callers retaining them must
// copy each value. A document with no values is still visited with an empty
// slice.
func (r *Reader) VisitFieldDocumentValues(field string, visit func(uint64, [][]byte) error) error {
	if useErr := r.use(); useErr != nil {
		return useErr
	}
	defer r.endUse()
	if len(r.segments) != 1 {
		return fmt.Errorf("nativeice: document values requires one segment, got %d", len(r.segments))
	}
	r.docValueMu.Lock()
	defer r.docValueMu.Unlock()
	fields := [repairSortFieldCount]string{field, "_nativeice_unused_1", "_nativeice_unused_2", "_nativeice_unused_3"}
	pageReader, readerErr := newRepairSegmentPageReader(r.segments[0], fields)
	if readerErr != nil {
		return readerErr
	}
	docValueReader := pageReader.sortReaders[0]
	for documentNumber := uint64(0); documentNumber < r.segments[0].record.documentCount; documentNumber++ {
		var values [][]byte
		if docValueReader != nil {
			values, readerErr = docValueReader.values(documentNumber)
			if readerErr != nil {
				return readerErr
			}
		}
		if visitErr := visit(documentNumber, values); visitErr != nil {
			return visitErr
		}
	}
	return nil
}

// TermDocumentCounts returns posting cardinalities aligned with terms.
func (r *Reader) TermDocumentCounts(field string, terms [][]byte) ([]uint64, error) {
	if useErr := r.check(); useErr != nil {
		return nil, useErr
	}
	result := make([]uint64, len(terms))
	for segmentIndex := range r.segments {
		storedReader, readerErr := r.storedReader(segmentIndex)
		if readerErr != nil {
			return nil, readerErr
		}
		counts, countsErr := storedReader.termDocumentCounts(field, terms)
		if countsErr != nil {
			return nil, countsErr
		}
		for termIndex := range counts {
			result[termIndex] += counts[termIndex]
		}
	}
	return result, nil
}

// TermExists reports whether an exact term is present in one segment's field
// dictionary without decoding its posting list.
func (r *Reader) TermExists(field string, term []byte) (bool, error) {
	if useErr := r.check(); useErr != nil {
		return false, useErr
	}
	for segmentIndex := range r.segments {
		storedReader, readerErr := r.storedReader(segmentIndex)
		if readerErr != nil {
			return false, readerErr
		}
		found, foundErr := storedReader.termExists(field, term)
		if foundErr != nil {
			return false, foundErr
		}
		if found {
			return true, nil
		}
	}
	return false, nil
}

// TermDocumentsBatch returns local document membership aligned with terms,
// resolving all terms through one cached field dictionary.
func (r *Reader) TermDocumentsBatch(field string, terms [][]byte) ([][]uint64, error) {
	if useErr := r.check(); useErr != nil {
		return nil, useErr
	}
	if len(r.segments) != 1 {
		return nil, fmt.Errorf("nativeice: term documents requires one segment, got %d", len(r.segments))
	}
	result := make([][]uint64, len(terms))
	for segmentIndex := range r.segments {
		storedReader, readerErr := r.storedReader(segmentIndex)
		if readerErr != nil {
			return nil, readerErr
		}
		memberships, membershipErr := storedReader.termDocumentsBatch(field, terms)
		if membershipErr != nil {
			return nil, membershipErr
		}
		for termIndex := range memberships {
			result[termIndex] = append(result[termIndex], memberships[termIndex]...)
		}
	}
	return result, nil
}

// VisitTermPostings resolves all terms in one segment while retaining a single
// dictionary decoder. A nil terms argument enumerates the dictionary. The
// callback receives local document numbers and frequencies one term at a time.
func (r *Reader) VisitTermPostings(field string, terms [][]byte, visit func([]byte, []uint64, []TermFrequency) error) error {
	if useErr := r.use(); useErr != nil {
		return useErr
	}
	defer r.endUse()
	if len(r.segments) != 1 {
		return fmt.Errorf("nativeice: term postings requires one segment, got %d", len(r.segments))
	}
	storedReader, readerErr := r.storedReader(0)
	if readerErr != nil {
		return readerErr
	}
	return storedReader.visitTermPostings(field, terms, visit)
}

// TermPosting resolves one exact term without enumerating the field
// dictionary. Compact one-hit postings remain scalar; general postings retain
// the validated slices used by VisitTermPostings.
func (r *Reader) TermPosting(field string, term []byte) (TermPosting, bool, error) {
	if useErr := r.check(); useErr != nil {
		return TermPosting{}, false, useErr
	}
	if len(r.segments) != 1 {
		return TermPosting{}, false, fmt.Errorf("nativeice: term posting requires one segment, got %d", len(r.segments))
	}
	storedReader, readerErr := r.storedReader(0)
	if readerErr != nil {
		return TermPosting{}, false, readerErr
	}
	return storedReader.termPosting(field, term)
}

// TermPostingBitmap resolves one exact term to a caller-owned document bitmap.
// Unlike TermPosting it neither decodes the posting's frequency stream nor
// materializes a document slice, which is all a membership test needs.
func (r *Reader) TermPostingBitmap(field string, term []byte) (*roaringpkg.Bitmap, bool, error) {
	if useErr := r.check(); useErr != nil {
		return nil, false, useErr
	}
	if len(r.segments) != 1 {
		return nil, false, fmt.Errorf("nativeice: term posting requires one segment, got %d", len(r.segments))
	}
	storedReader, readerErr := r.storedReader(0)
	if readerErr != nil {
		return nil, false, readerErr
	}
	return storedReader.termPostingBitmap(field, term)
}

// Fields returns every field represented in the segment, including stored-only,
// indexed-only and doc-value-only fields.
func (r *Reader) Fields() ([]string, error) {
	if useErr := r.check(); useErr != nil {
		return nil, useErr
	}
	result := make([]string, 0)
	for segmentIndex := range r.segments {
		storedReader, readerErr := r.storedReader(segmentIndex)
		if readerErr != nil {
			return nil, readerErr
		}
		result = append(result, storedReader.fieldNames...)
		for fieldID := uint64(0); fieldID < storedReader.footer.fieldsIndexEntries; fieldID++ {
			fieldName, fieldErr := storedReader.readFieldName(fieldID)
			if fieldErr != nil {
				return nil, fieldErr
			}
			result = append(result, fieldName)
		}
	}
	sort.Strings(result)
	unique := result[:0]
	for _, fieldName := range result {
		if len(unique) == 0 || unique[len(unique)-1] != fieldName {
			unique = append(unique, fieldName)
		}
	}
	return unique, nil
}

// FieldStats returns the encoded document count and total term frequency for
// field. The values are segment metadata, independent of posting score norms.
func (r *Reader) FieldStats(field string) (uint64, uint64, error) {
	if useErr := r.check(); useErr != nil {
		return 0, 0, useErr
	}
	var documents, frequency uint64
	for segmentIndex := range r.segments {
		storedReader, readerErr := r.storedReader(segmentIndex)
		if readerErr != nil {
			return 0, 0, readerErr
		}
		fieldDocuments, fieldFrequency, found, statsErr := storedReader.fieldStats(field)
		if statsErr != nil {
			return 0, 0, statsErr
		}
		if found {
			documents += fieldDocuments
			frequency += fieldFrequency
		}
	}
	return documents, frequency, nil
}

// Terms returns all exact dictionary terms for field in lexical order.
func (r *Reader) Terms(field string) ([][]byte, error) {
	if useErr := r.use(); useErr != nil {
		return nil, useErr
	}
	defer r.endUse()
	result := make([][]byte, 0)
	for segmentIndex := range r.segments {
		storedReader, readerErr := r.storedReader(segmentIndex)
		if readerErr != nil {
			return nil, readerErr
		}
		dictionary, dictionaryErr := storedReader.dictionary(field)
		if dictionaryErr != nil {
			return nil, dictionaryErr
		}
		if dictionary == nil {
			continue
		}
		iterator, iteratorErr := dictionary.Iterator(nil, nil)
		if iteratorErr != nil {
			if errors.Is(iteratorErr, vellum.ErrIteratorDone) || strings.Contains(strings.ToLower(iteratorErr.Error()), "iterator") {
				continue
			}
			return nil, corruptError("iterate term dictionary", iteratorErr)
		}
		for {
			term, value := iterator.Current()
			// Vellum represents the valid empty key with a nil slice.  A
			// nil key and zero value, on the other hand, means the iterator
			// is not positioned on an entry.  Do not discard the empty key:
			// it is a legitimate indexed term and sorts before every other
			// term in the dictionary.
			if term == nil && value == 0 {
				break
			}
			result = append(result, append([]byte(nil), term...))
			nextErr := iterator.Next()
			if nextErr != nil {
				if errors.Is(nextErr, vellum.ErrIteratorDone) || strings.Contains(strings.ToLower(nextErr.Error()), "iterator") {
					break
				}
				_ = iterator.Close()
				return nil, corruptError("iterate term dictionary", nextErr)
			}
		}
		_ = iterator.Close()
	}
	return result, nil
}

// TermDocuments returns document membership for one exact indexed term.
func (r *Reader) TermDocuments(field string, term []byte) ([]SegmentTermDocuments, error) {
	if useErr := r.check(); useErr != nil {
		return nil, useErr
	}
	result := make([]SegmentTermDocuments, 0)
	for segmentIndex := range r.segments {
		storedReader, readerErr := r.storedReader(segmentIndex)
		if readerErr != nil {
			return nil, readerErr
		}
		memberships, membershipErr := storedReader.termDocumentsBatch(field, [][]byte{term})
		if membershipErr != nil {
			return nil, membershipErr
		}
		if len(memberships) == 0 {
			continue
		}
		documents := memberships[0]
		if len(documents) == 0 {
			continue
		}
		values := append([]uint64(nil), documents...)
		result = append(result, SegmentTermDocuments{Segment: uint64(segmentIndex), DocumentNumber: values})
	}
	return result, nil
}

// TermFrequencies returns per-document frequencies, defaulting to one for
// legacy postings without a frequency stream.
func (r *Reader) TermFrequencies(field string, term []byte) ([]SegmentTermFrequencies, error) {
	if useErr := r.check(); useErr != nil {
		return nil, useErr
	}
	result := make([]SegmentTermFrequencies, 0)
	for segmentIndex := range r.segments {
		storedReader, readerErr := r.storedReader(segmentIndex)
		if readerErr != nil {
			return nil, readerErr
		}
		values, valuesErr := storedReader.termFrequencies(field, term)
		if valuesErr != nil {
			return nil, valuesErr
		}
		if len(values) > 0 {
			result = append(result, SegmentTermFrequencies{Segment: uint64(segmentIndex), Values: values})
		}
	}
	return result, nil
}

// DocValues returns one segment's doc values in physical document order. It
// includes doc-value-only fields and copies every returned value.
func (r *Reader) DocValues(field string) ([]SegmentDocValues, error) {
	if useErr := r.use(); useErr != nil {
		return nil, useErr
	}
	defer r.endUse()
	result := make([]SegmentDocValues, 0, len(r.segments))
	for segmentIndex := range r.segments {
		fields := [repairSortFieldCount]string{field, "_nativeice_unused_1", "_nativeice_unused_2", "_nativeice_unused_3"}
		pageReader, readerErr := newRepairSegmentPageReader(r.segments[segmentIndex], fields)
		if readerErr != nil {
			return nil, readerErr
		}
		values := make([][][]byte, r.segments[segmentIndex].record.documentCount)
		for documentNumber := range values {
			if pageReader.sortReaders[0] == nil {
				continue
			}
			docValues, valueErr := pageReader.sortReaders[0].values(uint64(documentNumber))
			if valueErr != nil {
				return nil, valueErr
			}
			values[documentNumber] = make([][]byte, len(docValues))
			for valueIndex, value := range docValues {
				values[documentNumber][valueIndex] = append([]byte(nil), value...)
			}
		}
		result = append(result, SegmentDocValues{Segment: uint64(segmentIndex), Values: values})
	}
	return result, nil
}

// DecodeStoredSegment decodes every physical stored document from one native
// ICE segment. It deliberately ignores snapshot deletion masks: those masks
// belong to the manifest, while a Segment contract represents physical docs.
func DecodeStoredSegment(payload []byte) ([]DecodedDocument, error) {
	file := &byteSegmentFile{data: payload}
	footer, footerErr := readSegmentFooter(file, uint64(len(payload)), "memory")
	if footerErr != nil {
		return nil, footerErr
	}
	record := segmentRecord{documentCount: footer.documentCount, timeMin: footer.timeMin, timeMax: footer.timeMax}
	reader, readerErr := newStoredSegmentReader(file, uint64(len(payload)), record)
	if readerErr != nil {
		return nil, readerErr
	}
	result := make([]DecodedDocument, 0, reader.footer.documentCount)
	visitErr := reader.visit(context.Background(), roaringpkg.New(), func(document StoredDocument) error {
		decoded := DecodedDocument{}
		documentErr := document.VisitStoredFields(func(name string, value []byte) bool {
			decoded.Fields = append(decoded.Fields, DecodedField{Name: name, Value: append([]byte(nil), value...)})
			return true
		})
		if documentErr != nil {
			return documentErr
		}
		result = append(result, decoded)
		return nil
	})
	if visitErr != nil {
		return nil, visitErr
	}
	return result, nil
}

type byteDecoder struct {
	payload []byte
	offset  int
}

func (d *byteDecoder) remaining() int {
	return len(d.payload) - d.offset
}

func (d *byteDecoder) uvarint() (uint64, error) {
	if d.offset >= len(d.payload) {
		return 0, corruptError("unexpected end of variable-length integer", nil)
	}
	value, width := binary.Uvarint(d.payload[d.offset:])
	if width <= 0 {
		return 0, corruptError("invalid variable-length integer", nil)
	}
	d.offset += width
	return value, nil
}

func (d *byteDecoder) bytes(length uint64) ([]byte, error) {
	if length > uint64(d.remaining()) {
		return nil, corruptError("length %d exceeds remaining bytes", length)
	}
	end := d.offset + int(length)
	value := d.payload[d.offset:end]
	d.offset = end
	return value, nil
}

func (d *byteDecoder) uint32() (uint32, error) {
	value, valueErr := d.bytes(4)
	if valueErr != nil {
		return 0, valueErr
	}
	return binary.BigEndian.Uint32(value), nil
}

func (d *byteDecoder) uint64() (uint64, error) {
	value, valueErr := d.bytes(8)
	if valueErr != nil {
		return 0, valueErr
	}
	return binary.BigEndian.Uint64(value), nil
}

func (d *byteDecoder) segmentRecord(segmentPaths map[uint64]string) (segmentRecord, error) {
	typeLength, typeErr := d.uvarint()
	if typeErr != nil {
		return segmentRecord{}, typeErr
	}
	segmentType, nameErr := d.bytes(typeLength)
	if nameErr != nil {
		return segmentRecord{}, nameErr
	}
	if string(segmentType) != "ice" {
		return segmentRecord{}, corruptError("unsupported segment type %q", string(segmentType))
	}
	version, versionErr := d.uint32()
	if versionErr != nil {
		return segmentRecord{}, versionErr
	}
	if version != segmentVersion {
		return segmentRecord{}, corruptError("unsupported segment version %d", version)
	}
	id, idErr := d.uvarint()
	if idErr != nil {
		return segmentRecord{}, idErr
	}
	if _, sizeErr := d.uint64(); sizeErr != nil {
		return segmentRecord{}, sizeErr
	}
	documentCount, documentCountErr := d.uint64()
	if documentCountErr != nil {
		return segmentRecord{}, documentCountErr
	}
	timeMin, timeMinErr := d.uint64()
	if timeMinErr != nil {
		return segmentRecord{}, timeMinErr
	}
	timeMax, timeMaxErr := d.uint64()
	if timeMaxErr != nil {
		return segmentRecord{}, timeMaxErr
	}
	deletionLength, deletionLengthErr := d.uvarint()
	if deletionLengthErr != nil {
		return segmentRecord{}, deletionLengthErr
	}
	deletionBitmap, bitmapErr := d.bytes(deletionLength)
	if bitmapErr != nil {
		return segmentRecord{}, bitmapErr
	}
	segmentPath, found := segmentPaths[id]
	if !found {
		return segmentRecord{}, corruptError("snapshot references missing segment %d", id)
	}
	return segmentRecord{deletionBitmap: deletionBitmap, path: segmentPath, documentCount: documentCount, id: id, timeMin: timeMin, timeMax: timeMax}, nil
}

type segmentFooter struct {
	documentCount      uint64
	storedIndexOffset  uint64
	fieldsIndexOffset  uint64
	docValueOffset     uint64
	chunkMode          uint32
	timeMin            uint64
	timeMax            uint64
	footerOffset       uint64
	fieldsIndexEntries uint64
}

func pinSegment(record segmentRecord, willNeed bool) (pinnedSegment, uint64, error) {
	file, size, openErr := openSegmentFile(record.path, willNeed)
	if errors.Is(openErr, os.ErrNotExist) {
		return pinnedSegment{}, 0, corruptError("open missing segment %d", record.id)
	}
	if openErr != nil {
		return pinnedSegment{}, 0, corruptError("open segment %q", record.path, openErr)
	}
	if size < segmentFooterLength {
		return pinnedSegment{}, 0, errors.Join(corruptError("segment %q is shorter than its footer", record.path), file.Close())
	}
	footer, footerErr := readSegmentFooter(file, size, record.path)
	if footerErr != nil {
		return pinnedSegment{}, 0, errors.Join(footerErr, file.Close())
	}
	for fieldIndexOffset := footer.fieldsIndexOffset; fieldIndexOffset < footer.footerOffset; fieldIndexOffset += fieldsIndexAddressByteWidth {
		var fieldRecord [fieldsIndexAddressByteWidth]byte
		if _, readErr := file.ReadAt(fieldRecord[:], int64(fieldIndexOffset)); readErr != nil {
			return pinnedSegment{}, 0, errors.Join(corruptError("read fields index from segment %q", record.path, readErr), file.Close())
		}
		fieldRecordOffset := binary.BigEndian.Uint64(fieldRecord[:])
		if fieldRecordOffset >= footer.fieldsIndexOffset {
			return pinnedSegment{}, 0, errors.Join(corruptError("segment %q has a field record outside its section", record.path), file.Close())
		}
	}
	if footer.timeMin != record.timeMin || footer.timeMax != record.timeMax {
		return pinnedSegment{}, 0, errors.Join(corruptError("segment %d time bounds differ from snapshot", record.id), file.Close())
	}
	return pinnedSegment{file: file, record: record, size: size}, footer.documentCount, nil
}

func closePinnedSegments(segments []pinnedSegment) error {
	var closeErr error
	for segmentIndex := range segments {
		if segments[segmentIndex].file == nil {
			continue
		}
		if fileErr := segments[segmentIndex].file.Close(); fileErr != nil {
			closeErr = errors.Join(closeErr, fmt.Errorf("close segment %q: %w", segments[segmentIndex].record.path, fileErr))
		}
	}
	return closeErr
}

func readSegmentFooter(file segmentFile, size uint64, path string) (segmentFooter, error) {
	if size < segmentFooterLength {
		return segmentFooter{}, corruptError("segment %q is shorter than its footer", path)
	}
	footerOffset := size - segmentFooterLength
	var payload [segmentFooterPayloadLength]byte
	if _, readErr := file.ReadAt(payload[:], int64(footerOffset)); readErr != nil {
		return segmentFooter{}, corruptError("read footer from segment %q", path, readErr)
	}
	footer := segmentFooter{
		documentCount:     binary.BigEndian.Uint64(payload[0:8]),
		storedIndexOffset: binary.BigEndian.Uint64(payload[8:16]),
		fieldsIndexOffset: binary.BigEndian.Uint64(payload[16:24]),
		docValueOffset:    binary.BigEndian.Uint64(payload[24:32]),
		chunkMode:         binary.BigEndian.Uint32(payload[32:36]),
		timeMin:           binary.BigEndian.Uint64(payload[36:44]),
		timeMax:           binary.BigEndian.Uint64(payload[44:52]),
		footerOffset:      footerOffset,
	}
	if binary.BigEndian.Uint32(payload[52:56]) != segmentVersion {
		return segmentFooter{}, corruptError("unsupported segment version %d", binary.BigEndian.Uint32(payload[52:56]))
	}
	storedIndexEnd := footer.docValueOffset
	if storedIndexEnd == math.MaxUint64 {
		storedIndexEnd = footer.fieldsIndexOffset
	}
	if footer.chunkMode == 0 || footer.storedIndexOffset > storedIndexEnd ||
		storedIndexEnd > footer.fieldsIndexOffset || footer.fieldsIndexOffset > footer.footerOffset {
		return segmentFooter{}, corruptError("segment %q has invalid section roots", path)
	}
	if footer.documentCount > uint64(math.MaxInt64) ||
		footer.documentCount > (storedIndexEnd-footer.storedIndexOffset)/storedDocumentOffsetByteWidth {
		return segmentFooter{}, corruptError("segment %q has invalid document count", path)
	}
	if (footer.footerOffset-footer.fieldsIndexOffset)%fieldsIndexAddressByteWidth != 0 {
		return segmentFooter{}, corruptError("segment %q has a misaligned fields index", path)
	}
	footer.fieldsIndexEntries = (footer.footerOffset - footer.fieldsIndexOffset) / fieldsIndexAddressByteWidth
	if footer.fieldsIndexEntries > maxFieldsIndexCount {
		return segmentFooter{}, corruptError("segment %q has too many fields index entries", path)
	}
	return footer, nil
}

// storedSegmentReader serves one segment. What a persisted segment keeps in
// the Go heap while it is open was chosen by benchmark (see the commit that
// introduced pagedFST): its fixed metadata -- footer, field table, stored
// chunk table -- and, per field once used, the dictionary's top page and its
// term filter (a bloom filter of about two bytes per term, or a tiny
// segment's exact term set), so existence checks skip segments without
// reading them. Dictionaries are read in place, and stored documents,
// offsets and postings per operation, through the OS page cache. Segment
// bytes themselves are never held.
//
//nolint:govet // decoder buffers are grouped with their parsed segment metadata.
type storedSegmentReader struct {
	file         segmentFile
	path         string
	chunkOffsets []uint64
	// chunkDocumentOffsets holds the loaded chunk's per-document offsets,
	// read with it, so decoding a document needs no extra read.
	chunkDocumentOffsets []byte
	compressedBuffer     []byte
	decodedBuffer        []byte
	loadedChunk          uint64
	fieldNames           []string
	fieldNameBuffer      []byte
	footer               segmentFooter
	size                 uint64
	storedDataEnd        uint64
	// owner is the Reader this reader serves, whose Close it observes; nil
	// for a transient validation reader.
	owner *Reader
	// dictionaries, termFilters and termSets hold, by field, what the reader
	// keeps for the segment's lifetime. They are read without locks on the
	// per-lookup path.
	dictionaries    sync.Map
	termFilters     sync.Map
	termSets        sync.Map
	fieldStatsMu    sync.RWMutex
	fieldStatsCache map[string]cachedFieldStats
	walkMu          sync.Mutex
	fieldNameMu     sync.Mutex
}

type cachedFieldStats struct {
	documents uint64
	frequency uint64
	found     bool
}

type storedField struct {
	name  string
	value []byte
}

type storedDocument struct {
	fields []storedField
}

func (d storedDocument) VisitStoredFields(visit func(name string, value []byte) bool) error {
	for _, field := range d.fields {
		if !visit(field.name, field.value) {
			break
		}
	}
	return nil
}

func walkStoredSegment(ctx context.Context, segment pinnedSegment, visit func(StoredDocument) error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	storedReader, readerErr := newStoredSegmentReader(segment.file, segment.size, segment.record)
	if readerErr != nil {
		return readerErr
	}
	deleted, deletionErr := deletedDocuments(segment.record)
	if deletionErr != nil {
		return deletionErr
	}
	return storedReader.visit(ctx, deleted, visit)
}

func newStoredSegmentReader(file segmentFile, size uint64, record segmentRecord) (*storedSegmentReader, error) {
	footer, footerErr := readSegmentFooter(file, size, record.path)
	if footerErr != nil {
		return nil, footerErr
	}
	if footer.documentCount != record.documentCount {
		return nil, corruptError("segment %d document count differs from snapshot", record.id)
	}
	if footer.timeMin != record.timeMin || footer.timeMax != record.timeMax {
		return nil, corruptError("segment %d time bounds differ from snapshot", record.id)
	}
	storedReader := &storedSegmentReader{file: file, path: record.path, size: size, footer: footer, loadedChunk: math.MaxUint64}
	if chunkErr := storedReader.loadChunkOffsets(); chunkErr != nil {
		return nil, chunkErr
	}
	if offsetErr := storedReader.loadDocumentOffsets(); offsetErr != nil {
		return nil, offsetErr
	}
	if fieldsErr := storedReader.loadFieldNames(); fieldsErr != nil {
		return nil, fieldsErr
	}
	return storedReader, nil
}

func validatePinnedSegments(segments []pinnedSegment) error {
	for _, segment := range segments {
		storedReader, readerErr := newStoredSegmentReader(segment.file, segment.size, segment.record)
		if readerErr != nil {
			return readerErr
		}
		for _, fieldName := range storedReader.fieldNames {
			if _, dictionaryErr := storedReader.dictionary(fieldName); dictionaryErr != nil {
				storedReader.close()
				return dictionaryErr
			}
		}
		storedReader.close()
	}
	return nil
}

// close drops the reader's retained structures. The pinned file belongs to
// the Reader.
func (s *storedSegmentReader) close() {
	s.dictionaries.Clear()
	s.termFilters.Clear()
	s.termSets.Clear()
}

// isClosed reports whether the Reader this reader serves has closed.
func (s *storedSegmentReader) isClosed() bool {
	return s.owner != nil && s.owner.closed.Load()
}

// released reports whether the Reader this reader serves released its files.
func (s *storedSegmentReader) released() bool {
	return s.owner != nil && s.owner.released.Load()
}

// keep stores value under key in m, unless the Reader released its files,
// and returns the value kept. A point read races the release, so a store is
// checked again after it: the release marks the Reader released before it
// clears the maps, so a store the clearing missed sees the mark and removes
// itself rather than keeping state on a released Reader.
func (s *storedSegmentReader) keep(m *sync.Map, key, value any) (any, error) {
	if s.released() {
		return nil, ErrReaderClosed
	}
	cached, _ := m.LoadOrStore(key, value)
	if s.released() {
		m.Delete(key)
		return nil, ErrReaderClosed
	}
	return cached, nil
}

func (s *storedSegmentReader) visit(ctx context.Context, deleted *roaringpkg.Bitmap, visit func(StoredDocument) error) error {
	s.walkMu.Lock()
	defer s.walkMu.Unlock()
	if s.footer.documentCount == 0 {
		return nil
	}
	chunkCount := (s.footer.documentCount + storedDocumentsPerChunk - 1) / storedDocumentsPerChunk
	for chunkIndex := uint64(0); chunkIndex < chunkCount; chunkIndex++ {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		chunk, chunkErr := s.loadChunk(chunkIndex)
		if chunkErr != nil {
			return chunkErr
		}
		firstDocument := chunkIndex * storedDocumentsPerChunk
		lastDocument := firstDocument + storedDocumentsPerChunk
		if lastDocument > s.footer.documentCount {
			lastDocument = s.footer.documentCount
		}
		for documentNumber := firstDocument; documentNumber < lastDocument; documentNumber++ {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			if documentNumber <= math.MaxUint32 && deleted.Contains(uint32(documentNumber)) {
				continue
			}
			document, documentErr := s.decodeDocument(documentNumber, chunk)
			if documentErr != nil {
				return documentErr
			}
			if visitErr := visit(document); visitErr != nil {
				return visitErr
			}
		}
	}
	return nil
}

func (s *storedSegmentReader) visitPhysical(ctx context.Context, deleted *roaringpkg.Bitmap, visit func(StoredDocument, bool) error) error {
	s.walkMu.Lock()
	defer s.walkMu.Unlock()
	if s.footer.documentCount == 0 {
		return nil
	}
	chunkCount := (s.footer.documentCount + storedDocumentsPerChunk - 1) / storedDocumentsPerChunk
	for chunkIndex := uint64(0); chunkIndex < chunkCount; chunkIndex++ {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		chunk, chunkErr := s.loadChunk(chunkIndex)
		if chunkErr != nil {
			return chunkErr
		}
		firstDocument := chunkIndex * storedDocumentsPerChunk
		lastDocument := firstDocument + storedDocumentsPerChunk
		if lastDocument > s.footer.documentCount {
			lastDocument = s.footer.documentCount
		}
		for documentNumber := firstDocument; documentNumber < lastDocument; documentNumber++ {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			isDeleted := documentNumber <= math.MaxUint32 && deleted.Contains(uint32(documentNumber))
			document, documentErr := s.decodeDocument(documentNumber, chunk)
			if documentErr != nil {
				return documentErr
			}
			if visitErr := visit(document, isDeleted); visitErr != nil {
				return visitErr
			}
		}
	}
	return nil
}

func (s *storedSegmentReader) visitDocument(documentNumber uint64, visit func(StoredDocument) error) error {
	s.walkMu.Lock()
	defer s.walkMu.Unlock()
	chunkIndex := documentNumber / storedDocumentsPerChunk
	chunk, chunkErr := s.loadChunk(chunkIndex)
	if chunkErr != nil {
		return chunkErr
	}
	document, documentErr := s.decodeDocument(documentNumber, chunk)
	if documentErr != nil {
		return documentErr
	}
	return visit(document)
}

func (s *storedSegmentReader) loadChunkOffsets() error {
	if s.footer.storedIndexOffset < storedChunkTableFooterLength {
		return corruptError("segment %q has a truncated stored chunk table", s.path)
	}
	var tableFooter [storedChunkTableFooterLength]byte
	if readErr := s.readInto(s.footer.storedIndexOffset-storedChunkTableFooterLength, tableFooter[:]); readErr != nil {
		return readErr
	}
	offsetLength := uint64(binary.BigEndian.Uint32(tableFooter[0:4]))
	chunkCount := uint64(binary.BigEndian.Uint32(tableFooter[4:8]))
	if offsetLength > maxStoredChunkTableSize || chunkCount == 0 || chunkCount > maxStoredChunkCount || chunkCount > offsetLength {
		return corruptError("segment %q has an invalid stored chunk table", s.path)
	}
	if offsetLength > s.footer.storedIndexOffset-storedChunkTableFooterLength {
		return corruptError("segment %q has a stored chunk table outside its section", s.path)
	}
	tableStart := s.footer.storedIndexOffset - storedChunkTableFooterLength - offsetLength
	table, tableErr := s.readBytes(tableStart, offsetLength)
	if tableErr != nil {
		return tableErr
	}
	decoder := byteDecoder{payload: table}
	offsets := make([]uint64, int(chunkCount))
	for chunkIndex := range offsets {
		offset, offsetErr := decoder.uvarint()
		if offsetErr != nil {
			return offsetErr
		}
		offsets[chunkIndex] = offset
	}
	if decoder.remaining() != 0 {
		return corruptError("segment %q has trailing bytes in its stored chunk table", s.path)
	}
	if offsets[0] != 0 {
		return corruptError("segment %q has a stored chunk table without a zero origin", s.path)
	}
	dataChunks := (s.footer.documentCount + storedDocumentsPerChunk - 1) / storedDocumentsPerChunk
	expectedChunkCount := dataChunks + 1
	// A legacy ICE merger at an exact 128-document boundary wrote one
	// duplicated terminal offset. It describes no data chunk and is safe to
	// ignore, but only this exact table shape is compatible.
	legacyTerminalChunk := s.footer.documentCount > 0 && s.footer.documentCount%storedDocumentsPerChunk == 0 &&
		chunkCount == expectedChunkCount+1 &&
		offsets[dataChunks] == tableStart && offsets[dataChunks+1] == tableStart
	for offsetIndex := 1; offsetIndex < len(offsets); offsetIndex++ {
		if offsets[offsetIndex] < offsets[offsetIndex-1] || offsets[offsetIndex] > tableStart {
			return corruptError("segment %q has invalid stored chunk offsets", s.path)
		}
	}
	if s.footer.documentCount == 0 {
		if chunkCount > 2 {
			return corruptError("segment %q has too many empty stored chunks", s.path)
		}
	} else if !legacyTerminalChunk && chunkCount != expectedChunkCount {
		return corruptError("segment %q has %d stored chunks for %d documents", s.path, chunkCount, s.footer.documentCount)
	}
	for chunkIndex := uint64(0); chunkIndex < dataChunks; chunkIndex++ {
		if offsets[chunkIndex] >= offsets[chunkIndex+1] {
			return corruptError("segment %q has an empty stored document chunk", s.path)
		}
	}
	s.chunkOffsets = offsets
	s.storedDataEnd = tableStart
	return nil
}

func (s *storedSegmentReader) loadFieldNames() error {
	fieldNames := make([]string, int(s.footer.fieldsIndexEntries))
	for fieldID := range fieldNames {
		fieldName, fieldErr := s.readFieldName(uint64(fieldID))
		if fieldErr != nil {
			return fieldErr
		}
		fieldNames[fieldID] = fieldName
	}
	s.fieldNames = fieldNames
	return nil
}

func (s *storedSegmentReader) readFieldName(fieldID uint64) (string, error) {
	s.fieldNameMu.Lock()
	defer s.fieldNameMu.Unlock()
	if fieldID >= s.footer.fieldsIndexEntries {
		return "", corruptError("segment %q has an out-of-range stored field identifier %d", s.path, fieldID)
	}
	indexOffset := s.footer.fieldsIndexOffset + fieldID*fieldsIndexAddressByteWidth
	var addressData [fieldsIndexAddressByteWidth]byte
	if readErr := s.readInto(indexOffset, addressData[:]); readErr != nil {
		return "", readErr
	}
	offset := binary.BigEndian.Uint64(addressData[:])
	if offset >= s.footer.fieldsIndexOffset {
		return "", corruptError("segment %q has a field record outside its section", s.path)
	}
	if _, dictErr := s.readUvarint(&offset, s.footer.fieldsIndexOffset); dictErr != nil {
		return "", dictErr
	}
	nameLength, lengthErr := s.readUvarint(&offset, s.footer.fieldsIndexOffset)
	if lengthErr != nil {
		return "", lengthErr
	}
	if nameLength > maxStoredFieldNameLength || nameLength > s.footer.fieldsIndexOffset-offset {
		return "", corruptError("segment %q has an invalid stored field name length", s.path)
	}
	if cap(s.fieldNameBuffer) < int(nameLength) {
		s.fieldNameBuffer = make([]byte, int(nameLength))
	} else {
		s.fieldNameBuffer = s.fieldNameBuffer[:int(nameLength)]
	}
	if readErr := s.readInto(offset, s.fieldNameBuffer); readErr != nil {
		return "", readErr
	}
	offset += nameLength
	if _, documentCountErr := s.readUvarint(&offset, s.footer.fieldsIndexOffset); documentCountErr != nil {
		return "", documentCountErr
	}
	if _, frequencyErr := s.readUvarint(&offset, s.footer.fieldsIndexOffset); frequencyErr != nil {
		return "", frequencyErr
	}
	return string(s.fieldNameBuffer), nil
}

func (s *storedSegmentReader) readUvarint(offset *uint64, end uint64) (uint64, error) {
	if *offset >= end {
		return 0, corruptError("segment %q has a truncated variable-length integer", s.path)
	}
	length := uint64(binary.MaxVarintLen64)
	if remaining := end - *offset; remaining < length {
		length = remaining
	}
	var encoded [binary.MaxVarintLen64]byte
	if readErr := s.readInto(*offset, encoded[:int(length)]); readErr != nil {
		return 0, readErr
	}
	value, width := binary.Uvarint(encoded[:int(length)])
	if width <= 0 {
		return 0, corruptError("segment %q has an invalid variable-length integer", s.path)
	}
	*offset += uint64(width)
	return value, nil
}

func (s *storedSegmentReader) loadChunk(chunkIndex uint64) ([]byte, error) {
	if s.loadedChunk == chunkIndex {
		return s.decodedBuffer, nil
	}
	// The buffers are overwritten below, so no chunk is loaded until this
	// one is completely: a failed load must not leave the previous chunk's
	// index naming them.
	s.loadedChunk = math.MaxUint64
	if chunkIndex+1 >= uint64(len(s.chunkOffsets)) {
		return nil, corruptError("segment %q has no stored chunk %d", s.path, chunkIndex)
	}
	start := s.chunkOffsets[chunkIndex]
	end := s.chunkOffsets[chunkIndex+1]
	if start >= end || end > s.storedDataEnd {
		return nil, corruptError("segment %q has invalid bounds for stored chunk %d", s.path, chunkIndex)
	}
	compressedLength := end - start
	if compressedLength > maxStoredCompressedChunkSize {
		return nil, corruptError("segment %q has an oversized stored chunk", s.path)
	}
	if cap(s.compressedBuffer) < int(compressedLength) {
		s.compressedBuffer = make([]byte, int(compressedLength))
	} else {
		s.compressedBuffer = s.compressedBuffer[:int(compressedLength)]
	}
	if readErr := s.readInto(start, s.compressedBuffer); readErr != nil {
		return nil, readErr
	}
	decodedLength, lengthErr := storedChunkDecodedLength(s.compressedBuffer)
	if lengthErr != nil {
		return nil, corruptError("decode stored chunk length in segment %q: %w", s.path, lengthErr)
	}
	if decodedLength < 0 || decodedLength > maxStoredDecodedChunkSize {
		return nil, corruptError("segment %q has an oversized decoded stored chunk", s.path)
	}
	if cap(s.decodedBuffer) < decodedLength {
		s.decodedBuffer = make([]byte, decodedLength)
	} else {
		s.decodedBuffer = s.decodedBuffer[:decodedLength]
	}
	decoded, decodeErr := decodeStoredChunk(s.decodedBuffer[:0], s.compressedBuffer)
	if decodeErr != nil {
		return nil, corruptError("decode stored chunk in segment %q: %w", s.path, decodeErr)
	}
	if len(decoded) != decodedLength {
		return nil, corruptError("segment %q decoded a stored chunk to an unexpected length", s.path)
	}
	firstDocument := chunkIndex * storedDocumentsPerChunk
	documents := min(storedDocumentsPerChunk, s.footer.documentCount-firstDocument)
	offsetsLength := int(documents * storedDocumentOffsetByteWidth)
	if cap(s.chunkDocumentOffsets) < offsetsLength {
		s.chunkDocumentOffsets = make([]byte, offsetsLength)
	}
	s.chunkDocumentOffsets = s.chunkDocumentOffsets[:offsetsLength]
	if readErr := s.readInto(s.footer.storedIndexOffset+firstDocument*storedDocumentOffsetByteWidth, s.chunkDocumentOffsets); readErr != nil {
		return nil, readErr
	}
	s.decodedBuffer = decoded
	s.loadedChunk = chunkIndex
	return decoded, nil
}

func storedChunkDecodedLength(compressed []byte) (decodedLength int, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("stored chunk length decoder panicked: %v", recovered)
		}
	}()
	return s2.DecodedLen(compressed)
}

func decodeStoredChunk(dst, compressed []byte) (decoded []byte, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("stored chunk decoder panicked: %v", recovered)
		}
	}()
	return s2.Decode(dst, compressed)
}

func (s *storedSegmentReader) decodeDocument(documentNumber uint64, chunk []byte) (storedDocument, error) {
	fields, decodeErr := s.decodeDocumentInto(documentNumber, chunk, nil)
	return storedDocument{fields: fields}, decodeErr
}

// decodeDocumentInto decodes one stored document's fields, appending them to
// fields[:0] so a sequential walk can reuse one field buffer. The returned
// names and values reference chunk and the reader's field-name table.
func (s *storedSegmentReader) decodeDocumentInto(documentNumber uint64, chunk []byte, fields []storedField) ([]storedField, error) {
	fields = fields[:0]
	documentOffset, offsetErr := s.documentOffset(documentNumber)
	if offsetErr != nil {
		return nil, offsetErr
	}
	if documentOffset >= uint64(len(chunk)) {
		return nil, corruptError("segment %q has a stored document offset outside its chunk", s.path)
	}
	decoder := byteDecoder{payload: chunk[documentOffset:]}
	metaLength, metaLengthErr := decoder.uvarint()
	if metaLengthErr != nil {
		return nil, metaLengthErr
	}
	dataLength, dataLengthErr := decoder.uvarint()
	if dataLengthErr != nil {
		return nil, dataLengthErr
	}
	meta, metaErr := decoder.bytes(metaLength)
	if metaErr != nil {
		return nil, metaErr
	}
	data, dataErr := decoder.bytes(dataLength)
	if dataErr != nil {
		return nil, dataErr
	}
	metaDecoder := byteDecoder{payload: meta}
	for fieldCount := 0; metaDecoder.remaining() > 0; fieldCount++ {
		if fieldCount >= maxStoredFieldsPerDocument {
			return nil, corruptError("segment %q has too many stored field values in one document", s.path)
		}
		fieldID, fieldIDErr := metaDecoder.uvarint()
		if fieldIDErr != nil {
			return nil, fieldIDErr
		}
		valueOffset, valueOffsetErr := metaDecoder.uvarint()
		if valueOffsetErr != nil {
			return nil, valueOffsetErr
		}
		valueLength, valueLengthErr := metaDecoder.uvarint()
		if valueLengthErr != nil {
			return nil, valueLengthErr
		}
		if fieldID >= uint64(len(s.fieldNames)) || valueOffset > uint64(len(data)) || valueLength > uint64(len(data))-valueOffset {
			return nil, corruptError("segment %q has invalid stored field metadata", s.path)
		}
		fields = append(fields, storedField{
			name:  s.fieldNames[fieldID],
			value: data[valueOffset : valueOffset+valueLength],
		})
	}
	return fields, nil
}

// loadDocumentOffsets validates the bounds of the fixed-width per-document
// offset table. Entries are read with their chunk (see documentOffset).
func (s *storedSegmentReader) loadDocumentOffsets() error {
	documentCount := s.footer.documentCount
	if documentCount == 0 {
		return nil
	}
	storedIndexEnd := s.footer.docValueOffset
	if storedIndexEnd == math.MaxUint64 {
		storedIndexEnd = s.footer.fieldsIndexOffset
	}
	if documentCount > maxStoredChunkTableSize/storedDocumentOffsetByteWidth {
		return corruptError("segment %q has too many stored documents for its offset index", s.path)
	}
	tableLength := documentCount * storedDocumentOffsetByteWidth
	if s.footer.storedIndexOffset > storedIndexEnd || tableLength > storedIndexEnd-s.footer.storedIndexOffset ||
		s.footer.storedIndexOffset+tableLength > s.size {
		return corruptError("segment %q has an invalid stored document offset index", s.path)
	}
	return nil
}

// documentOffset returns a stored document's offset within its chunk. The
// loaded chunk's offsets are read with the chunk (see loadChunk), so a walk
// or a lookup decodes a document without another read; any other document
// reads its eight-byte entry.
func (s *storedSegmentReader) documentOffset(documentNumber uint64) (uint64, error) {
	if documentNumber >= s.footer.documentCount {
		return 0, corruptError("segment %q has an out-of-range stored document number", s.path)
	}
	if documentNumber/storedDocumentsPerChunk == s.loadedChunk {
		start := (documentNumber % storedDocumentsPerChunk) * storedDocumentOffsetByteWidth
		if start+storedDocumentOffsetByteWidth <= uint64(len(s.chunkDocumentOffsets)) {
			return binary.BigEndian.Uint64(s.chunkDocumentOffsets[start:]), nil
		}
	}
	var entry [storedDocumentOffsetByteWidth]byte
	if readErr := s.readInto(s.footer.storedIndexOffset+documentNumber*storedDocumentOffsetByteWidth, entry[:]); readErr != nil {
		return 0, readErr
	}
	return binary.BigEndian.Uint64(entry[:]), nil
}

func (s *storedSegmentReader) readBytes(offset, length uint64) ([]byte, error) {
	if length > maxStoredChunkTableSize {
		return nil, corruptError("segment %q requested an oversized read", s.path)
	}
	data := make([]byte, int(length))
	if readErr := s.readInto(offset, data); readErr != nil {
		return nil, readErr
	}
	return data, nil
}

func (s *storedSegmentReader) readPostingBytes(offset, length uint64) ([]byte, error) {
	if length > maxSelectionPostingsSize {
		return nil, corruptError("segment %q requested an oversized posting read", s.path)
	}
	if offset > s.size || length > s.size-offset {
		return nil, corruptError("segment %q posting read exceeds file bounds", s.path)
	}
	if memory, ok := s.file.(*byteSegmentFile); ok {
		if offset > uint64(len(memory.data)) || length > uint64(len(memory.data))-offset {
			return nil, corruptError("segment %q posting read exceeds file bounds", s.path)
		}
		return memory.data[int(offset):int(offset+length)], nil
	}
	data := make([]byte, int(length))
	if readErr := s.readInto(offset, data); readErr != nil {
		return nil, readErr
	}
	return data, nil
}

func (s *storedSegmentReader) readInto(offset uint64, data []byte) error {
	length := uint64(len(data))
	if offset > s.size || length > s.size-offset {
		return corruptError("segment %q read exceeds file bounds", s.path)
	}
	return readSegmentBytes(s.file, offset, data, s.path)
}

func deletedDocuments(record segmentRecord) (*roaringpkg.Bitmap, error) {
	deleted := roaringpkg.New()
	if len(record.deletionBitmap) == 0 {
		return deleted, nil
	}
	if unmarshalErr := deleted.UnmarshalBinary(record.deletionBitmap); unmarshalErr != nil {
		return nil, corruptError("decode deletion bitmap", unmarshalErr)
	}
	return deleted, nil
}

func deletionCount(payload []byte, documentCount uint64) (uint64, error) {
	if len(payload) == 0 {
		return 0, nil
	}
	bitmap := roaringpkg.New()
	if unmarshalErr := bitmap.UnmarshalBinary(payload); unmarshalErr != nil {
		return 0, corruptError("decode deletion bitmap", unmarshalErr)
	}
	deletedCount := bitmap.GetCardinality()
	if deletedCount > documentCount {
		return 0, corruptError("deletion bitmap exceeds document count", nil)
	}
	iterator := bitmap.Iterator()
	for iterator.HasNext() {
		if uint64(iterator.Next()) >= documentCount {
			return 0, corruptError("deletion bitmap contains an out-of-range document", nil)
		}
	}
	return deletedCount, nil
}

func readManifest(path string) ([]byte, error) {
	file, openErr := segmentFileSystem.OpenFile(path)
	if openErr != nil {
		return nil, openErr
	}
	defer func() {
		_ = file.Close()
	}()
	size, sizeErr := file.Size()
	if sizeErr != nil {
		return nil, sizeErr
	}
	if size < 0 {
		return nil, fmt.Errorf("unsupported snapshot file %q", path)
	}
	if size > maxManifestSize {
		return nil, fmt.Errorf("%w: %q is %d bytes", errManifestTooLarge, path, size)
	}
	payload := make([]byte, int(size))
	if len(payload) == 0 {
		return payload, nil
	}
	if _, readErr := file.Read(0, payload); readErr != nil {
		return nil, readErr
	}
	return payload, nil
}

func parseFinalName(name, extension string) (uint64, bool) {
	if filepath.Ext(name) != extension {
		return 0, false
	}
	identifier := name[:len(name)-len(extension)]
	if identifier == "" {
		return 0, false
	}
	for _, character := range identifier {
		if !(character >= '0' && character <= '9' || character >= 'a' && character <= 'f') {
			return 0, false
		}
	}
	id, parseErr := strconv.ParseUint(identifier, 16, 64)
	return id, parseErr == nil
}

func corruptError(format string, arguments ...any) error {
	if len(arguments) == 0 || arguments[len(arguments)-1] == nil {
		return fmt.Errorf("nativeice: "+format+": %w", append(arguments[:max(0, len(arguments)-1)], ErrCorrupt)...)
	}
	if cause, ok := arguments[len(arguments)-1].(error); ok {
		return fmt.Errorf("nativeice: "+format+": %w: %w", append(arguments[:len(arguments)-1], cause, ErrCorrupt)...)
	}
	return fmt.Errorf("nativeice: "+format+": %w", append(arguments, ErrCorrupt)...)
}
