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
// read-only production paths in pkg/index/inverted open committed index
// directories through, and it never depends on the retired index libraries.
//
// The package is deliberately reachable only from pkg/index/inverted. Footer,
// offset, mapping, and section decoder types are private to it; the contract
// other packages observe is the behavior of the exported functions in
// pkg/index/inverted that call it.
package nativeice

import (
	"bytes"
	"container/heap"
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
	"sync"

	roaringpkg "github.com/RoaringBitmap/roaring"
	"github.com/klauspost/compress/s2"
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
type Reader struct {
	closeErr             error
	repairPageSortFields [repairSortFieldCount]string
	segments             []pinnedSegment
	repairPageReaders    []*repairSegmentPageReader
	snapshotID           uint64
	visibleDocCount      int64
	repairPageMu         sync.Mutex
	closeOnce            sync.Once
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
			visibleDocCount, segments, parseErr := parseSnapshotSegments(segmentPaths, manifest)
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

// Close releases the file handles the Reader pinned at Open. It is idempotent.
// Callers must not close a Reader concurrently with a document visit.
func (r *Reader) Close() error {
	r.closeOnce.Do(func() {
		r.repairPageReaders = nil
		r.repairPageSortFields = [repairSortFieldCount]string{}
		r.closeErr = closePinnedSegments(r.segments)
	})
	return r.closeErr
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

func readDirectoryEntries(path string) ([]os.DirEntry, error) {
	directory, openErr := os.Open(path)
	if errors.Is(openErr, os.ErrNotExist) {
		return nil, ErrNoSnapshot
	}
	if openErr != nil {
		return nil, corruptError("open index directory %q", path, openErr)
	}
	entries, readErr := directory.ReadDir(directoryReadSize)
	closeErr := directory.Close()
	if closeErr != nil {
		return nil, corruptError("close index directory %q", path, closeErr)
	}
	if len(entries) > maxDirectoryEntries {
		return nil, corruptError("index directory %q contains more than %d entries", path, maxDirectoryEntries)
	}
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		return nil, corruptError("read index directory %q", path, readErr)
	}
	return entries, nil
}

func parseSnapshotSegments(segmentPaths map[uint64]string, payload []byte) (visibleDocCount int64, resultSegments []pinnedSegment, err error) {
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
		pinnedRecord, segmentDocCount, segmentErr := pinSegment(record)
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
	file   *os.File
	record segmentRecord
	size   uint64
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

func pinSegment(record segmentRecord) (pinnedSegment, uint64, error) {
	file, openErr := os.Open(record.path)
	if errors.Is(openErr, os.ErrNotExist) {
		return pinnedSegment{}, 0, corruptError("open missing segment %d", record.id)
	}
	if openErr != nil {
		return pinnedSegment{}, 0, corruptError("open segment %q", record.path, openErr)
	}
	info, statErr := file.Stat()
	if statErr != nil {
		return pinnedSegment{}, 0, errors.Join(corruptError("stat segment %q", record.path, statErr), file.Close())
	}
	if !info.Mode().IsRegular() || info.Size() < segmentFooterLength {
		return pinnedSegment{}, 0, errors.Join(corruptError("segment %q is shorter than its footer", record.path), file.Close())
	}
	footer, footerErr := readSegmentFooter(file, uint64(info.Size()), record.path)
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
	return pinnedSegment{file: file, record: record, size: uint64(info.Size())}, footer.documentCount, nil
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

func readSegmentFooter(file *os.File, size uint64, path string) (segmentFooter, error) {
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

type storedSegmentReader struct {
	file             *os.File
	path             string
	chunkOffsets     []uint64
	compressedBuffer []byte
	decodedBuffer    []byte
	fieldNames       []string
	fieldNameBuffer  []byte
	footer           segmentFooter
	size             uint64
	storedDataEnd    uint64
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

func newStoredSegmentReader(file *os.File, size uint64, record segmentRecord) (*storedSegmentReader, error) {
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
	storedReader := &storedSegmentReader{file: file, path: record.path, size: size, footer: footer}
	if chunkErr := storedReader.loadChunkOffsets(); chunkErr != nil {
		return nil, chunkErr
	}
	if fieldsErr := storedReader.loadFieldNames(); fieldsErr != nil {
		return nil, fieldsErr
	}
	return storedReader, nil
}

func (s *storedSegmentReader) visit(ctx context.Context, deleted *roaringpkg.Bitmap, visit func(StoredDocument) error) error {
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
	for offsetIndex := 1; offsetIndex < len(offsets); offsetIndex++ {
		if offsets[offsetIndex] < offsets[offsetIndex-1] || offsets[offsetIndex] > tableStart {
			return corruptError("segment %q has invalid stored chunk offsets", s.path)
		}
	}
	dataChunks := (s.footer.documentCount + storedDocumentsPerChunk - 1) / storedDocumentsPerChunk
	if s.footer.documentCount == 0 {
		if chunkCount > 2 {
			return corruptError("segment %q has too many empty stored chunks", s.path)
		}
	} else if chunkCount != dataChunks+1 {
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
	s.decodedBuffer = decoded
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
	documentOffset, offsetErr := s.documentOffset(documentNumber)
	if offsetErr != nil {
		return storedDocument{}, offsetErr
	}
	if documentOffset >= uint64(len(chunk)) {
		return storedDocument{}, corruptError("segment %q has a stored document offset outside its chunk", s.path)
	}
	decoder := byteDecoder{payload: chunk[documentOffset:]}
	metaLength, metaLengthErr := decoder.uvarint()
	if metaLengthErr != nil {
		return storedDocument{}, metaLengthErr
	}
	dataLength, dataLengthErr := decoder.uvarint()
	if dataLengthErr != nil {
		return storedDocument{}, dataLengthErr
	}
	meta, metaErr := decoder.bytes(metaLength)
	if metaErr != nil {
		return storedDocument{}, metaErr
	}
	data, dataErr := decoder.bytes(dataLength)
	if dataErr != nil {
		return storedDocument{}, dataErr
	}
	metaDecoder := byteDecoder{payload: meta}
	document := storedDocument{}
	for fieldCount := 0; metaDecoder.remaining() > 0; fieldCount++ {
		if fieldCount >= maxStoredFieldsPerDocument {
			return storedDocument{}, corruptError("segment %q has too many stored field values in one document", s.path)
		}
		fieldID, fieldIDErr := metaDecoder.uvarint()
		if fieldIDErr != nil {
			return storedDocument{}, fieldIDErr
		}
		valueOffset, valueOffsetErr := metaDecoder.uvarint()
		if valueOffsetErr != nil {
			return storedDocument{}, valueOffsetErr
		}
		valueLength, valueLengthErr := metaDecoder.uvarint()
		if valueLengthErr != nil {
			return storedDocument{}, valueLengthErr
		}
		if fieldID >= uint64(len(s.fieldNames)) || valueOffset > uint64(len(data)) || valueLength > uint64(len(data))-valueOffset {
			return storedDocument{}, corruptError("segment %q has invalid stored field metadata", s.path)
		}
		document.fields = append(document.fields, storedField{
			name:  s.fieldNames[fieldID],
			value: data[valueOffset : valueOffset+valueLength],
		})
	}
	return document, nil
}

func (s *storedSegmentReader) documentOffset(documentNumber uint64) (uint64, error) {
	if documentNumber >= s.footer.documentCount {
		return 0, corruptError("segment %q has an out-of-range stored document number", s.path)
	}
	storedIndexEnd := s.footer.docValueOffset
	if storedIndexEnd == math.MaxUint64 {
		storedIndexEnd = s.footer.fieldsIndexOffset
	}
	indexOffset := s.footer.storedIndexOffset + documentNumber*storedDocumentOffsetByteWidth
	if indexOffset > storedIndexEnd-storedDocumentOffsetByteWidth {
		return 0, corruptError("segment %q has a stored document offset outside its index", s.path)
	}
	var offsetData [storedDocumentOffsetByteWidth]byte
	if readErr := s.readInto(indexOffset, offsetData[:]); readErr != nil {
		return 0, readErr
	}
	return binary.BigEndian.Uint64(offsetData[:]), nil
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

func (s *storedSegmentReader) readInto(offset uint64, data []byte) error {
	length := uint64(len(data))
	if offset > s.size || length > s.size-offset {
		return corruptError("segment %q read exceeds file bounds", s.path)
	}
	read, readErr := s.file.ReadAt(data, int64(offset))
	if readErr != nil || read != len(data) {
		return corruptError("read segment %q", s.path, readErr)
	}
	return nil
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
	file, openErr := os.Open(path)
	if openErr != nil {
		return nil, openErr
	}
	defer func() {
		_ = file.Close()
	}()
	info, statErr := file.Stat()
	if statErr != nil {
		return nil, statErr
	}
	if !info.Mode().IsRegular() || info.Size() < 0 {
		return nil, fmt.Errorf("unsupported snapshot file %q", path)
	}
	if info.Size() > maxManifestSize {
		return nil, fmt.Errorf("%w: %q is %d bytes", errManifestTooLarge, path, info.Size())
	}
	payload := make([]byte, int(info.Size()))
	if _, readErr := io.ReadFull(file, payload); readErr != nil {
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

const (
	repairSortFieldCount           = 4
	maxRepairPageSize              = 1 << 16
	maxRepairSortValueLength       = 64 << 10
	docValueDocumentsPerChunk      = 1024
	docValueChunkTableFooterLength = 16
	maxDocValueChunkCount          = 1 << 20
	maxDocValueCompressedChunkSize = 16 << 20
	maxDocValueDecodedChunkSize    = 64 << 20
)

// ErrInvalidRepairPage reports a bounded repair page request that the native
// reader cannot serve.
var ErrInvalidRepairPage = errors.New("nativeice: invalid repair page request")

// RepairTupleRow is one bounded repair row returned by Reader.RepairTuplePage.
type RepairTupleRow struct {
	SortValues [][]byte
	Value      []byte
}

// RepairTuplePage returns a bounded page from the Reader's pinned generation.
func (r *Reader) RepairTuplePage(
	ctx context.Context,
	sortFields [repairSortFieldCount]string,
	projectField string,
	after [][]byte,
	pageSize int,
) ([]RepairTupleRow, error) {
	if requestErr := validateRepairPageRequest(sortFields, projectField, after, pageSize); requestErr != nil {
		return nil, requestErr
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, ctxErr
	}

	r.repairPageMu.Lock()
	defer r.repairPageMu.Unlock()
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, ctxErr
	}
	segmentReaders, readersErr := r.repairPageReadersFor(sortFields)
	if readersErr != nil {
		return nil, readersErr
	}

	candidates := make(repairTupleHeap, 0, pageSize)
	heap.Init(&candidates)
	for segmentIndex, segmentReader := range segmentReaders {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		documentCount := r.segments[segmentIndex].record.documentCount
		for documentNumber := uint64(0); documentNumber < documentCount; documentNumber++ {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, ctxErr
			}
			if documentNumber <= math.MaxUint32 && segmentReader.deleted.Contains(uint32(documentNumber)) {
				continue
			}
			candidate, candidateErr := segmentReader.candidate(documentNumber, r.segments[segmentIndex].record.id)
			if candidateErr != nil {
				return nil, candidateErr
			}
			if compareRepairCandidateCursor(candidate, after) <= 0 {
				continue
			}
			if len(candidates) == pageSize && compareRepairCandidates(candidate, candidates[0]) >= 0 {
				continue
			}
			if len(candidates) < pageSize {
				stableCandidate := repairTupleCandidate{}
				copyRepairTupleCandidate(&stableCandidate, candidate)
				projectedValue, projectedErr := segmentReader.projectedValue(documentNumber, projectField, stableCandidate.value)
				if projectedErr != nil {
					return nil, projectedErr
				}
				stableCandidate.value = projectedValue
				heap.Push(&candidates, stableCandidate)
				continue
			}
			replacedCandidate := heap.Pop(&candidates).(repairTupleCandidate)
			copyRepairTupleCandidate(&replacedCandidate, candidate)
			projectedValue, projectedErr := segmentReader.projectedValue(documentNumber, projectField, replacedCandidate.value)
			if projectedErr != nil {
				return nil, projectedErr
			}
			replacedCandidate.value = projectedValue
			heap.Push(&candidates, replacedCandidate)
		}
	}

	sort.Slice(candidates, func(leftIndex, rightIndex int) bool {
		return compareRepairCandidates(candidates[leftIndex], candidates[rightIndex]) < 0
	})
	rows := make([]RepairTupleRow, len(candidates))
	for rowIndex, candidate := range candidates {
		sortValues := make([][]byte, repairSortFieldCount)
		copy(sortValues, candidate.sortValues[:])
		rows[rowIndex] = RepairTupleRow{SortValues: sortValues, Value: candidate.value}
	}
	return rows, nil
}

func (r *Reader) repairPageReadersFor(
	sortFields [repairSortFieldCount]string,
) ([]*repairSegmentPageReader, error) {
	if r.repairPageReaders != nil && r.repairPageSortFields == sortFields {
		return r.repairPageReaders, nil
	}
	readers := make([]*repairSegmentPageReader, len(r.segments))
	for segmentIndex := range r.segments {
		segmentReader, readerErr := newRepairSegmentPageReader(r.segments[segmentIndex], sortFields)
		if readerErr != nil {
			return nil, readerErr
		}
		readers[segmentIndex] = segmentReader
	}
	r.repairPageReaders = readers
	r.repairPageSortFields = sortFields
	return readers, nil
}

func validateRepairPageRequest(
	sortFields [repairSortFieldCount]string,
	projectField string,
	after [][]byte,
	pageSize int,
) error {
	if pageSize <= 0 || pageSize > maxRepairPageSize {
		return repairPageRequestError("page size %d is outside the allowed range", pageSize)
	}
	if projectField == "" {
		return repairPageRequestError("projection field is empty")
	}
	if len(after) != 0 && len(after) != repairSortFieldCount {
		return repairPageRequestError("cursor has %d components", len(after))
	}
	for fieldIndex, fieldName := range sortFields {
		if fieldName == "" {
			return repairPageRequestError("sort field %d is empty", fieldIndex)
		}
		for previousIndex := 0; previousIndex < fieldIndex; previousIndex++ {
			if fieldName == sortFields[previousIndex] {
				return repairPageRequestError("sort field %q is repeated", fieldName)
			}
		}
	}
	for componentIndex, component := range after {
		if len(component) > maxRepairSortValueLength {
			return repairPageRequestError("cursor component %d exceeds %d bytes", componentIndex, maxRepairSortValueLength)
		}
	}
	return nil
}

func repairPageRequestError(format string, arguments ...any) error {
	return fmt.Errorf("nativeice: invalid repair page: "+format+": %w", append(arguments, ErrInvalidRepairPage)...)
}

type repairTupleCandidate struct {
	sortValues     [repairSortFieldCount][]byte
	value          []byte
	documentNumber uint64
	segmentID      uint64
}

func copyRepairTupleCandidate(destination *repairTupleCandidate, source repairTupleCandidate) {
	destination.documentNumber = source.documentNumber
	destination.segmentID = source.segmentID
	for valueIndex, value := range source.sortValues {
		destination.sortValues[valueIndex] = copyRepairPageValueInto(destination.sortValues[valueIndex], value)
	}
}

func compareRepairCandidates(left, right repairTupleCandidate) int {
	for valueIndex := range left.sortValues {
		if comparison := compareRepairValues(left.sortValues[valueIndex], right.sortValues[valueIndex]); comparison != 0 {
			return comparison
		}
	}
	switch {
	case left.segmentID < right.segmentID:
		return -1
	case left.segmentID > right.segmentID:
		return 1
	case left.documentNumber < right.documentNumber:
		return -1
	case left.documentNumber > right.documentNumber:
		return 1
	default:
		return 0
	}
}

func compareRepairCandidateCursor(candidate repairTupleCandidate, cursor [][]byte) int {
	if len(cursor) == 0 {
		return 1
	}
	for valueIndex := range candidate.sortValues {
		if comparison := compareRepairValues(candidate.sortValues[valueIndex], cursor[valueIndex]); comparison != 0 {
			return comparison
		}
	}
	return 0
}

func compareRepairValues(left, right []byte) int {
	switch {
	case left == nil && right == nil:
		return 0
	case left == nil:
		return 1
	case right == nil:
		return -1
	default:
		return bytes.Compare(left, right)
	}
}

type repairTupleHeap []repairTupleCandidate

func (h repairTupleHeap) Len() int {
	return len(h)
}

func (h repairTupleHeap) Less(leftIndex, rightIndex int) bool {
	return compareRepairCandidates(h[leftIndex], h[rightIndex]) > 0
}

func (h repairTupleHeap) Swap(leftIndex, rightIndex int) {
	h[leftIndex], h[rightIndex] = h[rightIndex], h[leftIndex]
}

func (h *repairTupleHeap) Push(value any) {
	*h = append(*h, value.(repairTupleCandidate))
}

func (h *repairTupleHeap) Pop() any {
	current := *h
	lastIndex := len(current) - 1
	value := current[lastIndex]
	*h = current[:lastIndex]
	return value
}

type repairSegmentPageReader struct {
	deleted           *roaringpkg.Bitmap
	sortReaders       [repairSortFieldCount]*repairDocValueReader
	stored            *storedSegmentReader
	storedChunk       []byte
	storedChunkNumber uint64
}

func newRepairSegmentPageReader(
	segment pinnedSegment,
	sortFields [repairSortFieldCount]string,
) (*repairSegmentPageReader, error) {
	storedReader, readerErr := newStoredSegmentReader(segment.file, segment.size, segment.record)
	if readerErr != nil {
		return nil, readerErr
	}
	deleted, deletionErr := deletedDocuments(segment.record)
	if deletionErr != nil {
		return nil, deletionErr
	}
	pageReader := &repairSegmentPageReader{
		deleted:           deleted,
		stored:            storedReader,
		storedChunkNumber: math.MaxUint64,
	}
	if segment.record.documentCount == 0 || storedReader.footer.docValueOffset == math.MaxUint64 {
		return pageReader, nil
	}

	locationOffset := storedReader.footer.docValueOffset
	for fieldIndex, fieldName := range storedReader.fieldNames {
		fieldStart, startErr := storedReader.readUvarint(&locationOffset, storedReader.footer.fieldsIndexOffset)
		if startErr != nil {
			return nil, startErr
		}
		fieldEnd, endErr := storedReader.readUvarint(&locationOffset, storedReader.footer.fieldsIndexOffset)
		if endErr != nil {
			return nil, endErr
		}
		if fieldStart == math.MaxUint64 || fieldEnd == math.MaxUint64 {
			if fieldStart != math.MaxUint64 || fieldEnd != math.MaxUint64 {
				return nil, corruptError("segment %q has an incomplete doc-value location for field %d", storedReader.path, fieldIndex)
			}
			continue
		}
		fieldReader, fieldErr := newRepairDocValueReader(storedReader, fieldStart, fieldEnd)
		if fieldErr != nil {
			return nil, fieldErr
		}
		for sortIndex, sortField := range sortFields {
			if sortField == fieldName {
				pageReader.sortReaders[sortIndex] = fieldReader
			}
		}
	}
	return pageReader, nil
}

func (r *repairSegmentPageReader) candidate(documentNumber, segmentID uint64) (repairTupleCandidate, error) {
	candidate := repairTupleCandidate{documentNumber: documentNumber, segmentID: segmentID}
	for sortIndex, fieldReader := range r.sortReaders {
		if fieldReader == nil {
			continue
		}
		value, valueErr := fieldReader.smallestValue(documentNumber)
		if valueErr != nil {
			return repairTupleCandidate{}, valueErr
		}
		candidate.sortValues[sortIndex] = value
	}
	return candidate, nil
}

func (r *repairSegmentPageReader) projectedValue(documentNumber uint64, projectField string, destination []byte) ([]byte, error) {
	chunkNumber := documentNumber / storedDocumentsPerChunk
	if r.storedChunkNumber != chunkNumber {
		chunk, chunkErr := r.stored.loadChunk(chunkNumber)
		if chunkErr != nil {
			return nil, chunkErr
		}
		r.storedChunk = chunk
		r.storedChunkNumber = chunkNumber
	}
	document, documentErr := r.stored.decodeDocument(documentNumber, r.storedChunk)
	if documentErr != nil {
		return nil, documentErr
	}
	for _, field := range document.fields {
		if field.name == projectField {
			return copyRepairPageValueInto(destination, field.value), nil
		}
	}
	return nil, nil
}

type repairDocValueReader struct {
	path               string
	file               *os.File
	chunkOffsets       []uint64
	compressedBuffer   []byte
	decodedBuffer      []byte
	header             []repairDocValueMeta
	chunkNumber        uint64
	fieldDataOffset    uint64
	size               uint64
	totalDocumentCount uint64
}

type repairDocValueMeta struct {
	documentNumber uint64
	valueEnd       uint64
}

func newRepairDocValueReader(storedReader *storedSegmentReader, fieldStart, fieldEnd uint64) (*repairDocValueReader, error) {
	if storedReader.footer.documentCount > math.MaxUint64/storedDocumentOffsetByteWidth {
		return nil, corruptError("segment %q has an oversized stored document index", storedReader.path)
	}
	minimumDataOffset := storedReader.footer.storedIndexOffset + storedReader.footer.documentCount*storedDocumentOffsetByteWidth
	if minimumDataOffset < storedReader.footer.storedIndexOffset || fieldStart < minimumDataOffset ||
		fieldStart >= fieldEnd || fieldEnd > storedReader.footer.docValueOffset {
		return nil, corruptError("segment %q has invalid doc-value bounds", storedReader.path)
	}
	if fieldEnd-fieldStart <= docValueChunkTableFooterLength {
		return nil, corruptError("segment %q has a truncated doc-value chunk table", storedReader.path)
	}

	var tableFooter [docValueChunkTableFooterLength]byte
	if readErr := storedReader.readInto(fieldEnd-docValueChunkTableFooterLength, tableFooter[:]); readErr != nil {
		return nil, readErr
	}
	chunkOffsetsLength := binary.BigEndian.Uint64(tableFooter[0:8])
	chunkCount := binary.BigEndian.Uint64(tableFooter[8:16])
	expectedChunkCount := (storedReader.footer.documentCount + docValueDocumentsPerChunk - 1) / docValueDocumentsPerChunk
	if chunkCount == 0 || chunkCount != expectedChunkCount || chunkCount > maxDocValueChunkCount ||
		chunkOffsetsLength < chunkCount || chunkOffsetsLength > chunkCount*binary.MaxVarintLen64 {
		return nil, corruptError("segment %q has an invalid doc-value chunk table", storedReader.path)
	}
	tableStart := fieldEnd - docValueChunkTableFooterLength
	if chunkOffsetsLength > tableStart-fieldStart {
		return nil, corruptError("segment %q has a doc-value chunk table outside its field data", storedReader.path)
	}
	tableStart -= chunkOffsetsLength
	table, tableErr := storedReader.readBytes(tableStart, chunkOffsetsLength)
	if tableErr != nil {
		return nil, tableErr
	}
	decoder := byteDecoder{payload: table}
	offsets := make([]uint64, int(chunkCount))
	for offsetIndex := range offsets {
		offset, offsetErr := decoder.uvarint()
		if offsetErr != nil {
			return nil, offsetErr
		}
		offsets[offsetIndex] = offset
	}
	if decoder.remaining() != 0 {
		return nil, corruptError("segment %q has trailing bytes in a doc-value chunk table", storedReader.path)
	}
	fieldDataLength := tableStart - fieldStart
	for offsetIndex, offset := range offsets {
		if offset > fieldDataLength || (offsetIndex > 0 && offset < offsets[offsetIndex-1]) {
			return nil, corruptError("segment %q has invalid doc-value chunk offsets", storedReader.path)
		}
	}
	return &repairDocValueReader{
		chunkNumber:        math.MaxUint64,
		chunkOffsets:       offsets,
		fieldDataOffset:    fieldStart,
		path:               storedReader.path,
		file:               storedReader.file,
		size:               storedReader.size,
		totalDocumentCount: storedReader.footer.documentCount,
	}, nil
}

func (r *repairDocValueReader) smallestValue(documentNumber uint64) ([]byte, error) {
	chunkNumber := documentNumber / docValueDocumentsPerChunk
	if r.chunkNumber != chunkNumber {
		if chunkErr := r.loadChunk(chunkNumber); chunkErr != nil {
			return nil, chunkErr
		}
	}
	headerIndex := sort.Search(len(r.header), func(index int) bool {
		return r.header[index].documentNumber >= documentNumber
	})
	if headerIndex == len(r.header) || r.header[headerIndex].documentNumber != documentNumber {
		return nil, nil
	}
	start := uint64(0)
	if headerIndex > 0 {
		start = r.header[headerIndex-1].valueEnd
	}
	end := r.header[headerIndex].valueEnd
	if end < start || end > uint64(len(r.decodedBuffer)) {
		return nil, corruptError("segment %q has invalid doc-value bounds in a chunk", r.path)
	}
	return decodeRepairDocValueTerms(r.decodedBuffer[start:end], r.path)
}

func (r *repairDocValueReader) loadChunk(chunkNumber uint64) error {
	if chunkNumber >= uint64(len(r.chunkOffsets)) {
		return corruptError("segment %q has no doc-value chunk %d", r.path, chunkNumber)
	}
	start := uint64(0)
	if chunkNumber > 0 {
		start = r.chunkOffsets[chunkNumber-1]
	}
	end := r.chunkOffsets[chunkNumber]
	if start > end {
		return corruptError("segment %q has unordered doc-value chunk bounds", r.path)
	}
	if start == end {
		r.chunkNumber = chunkNumber
		r.header = r.header[:0]
		r.compressedBuffer = r.compressedBuffer[:0]
		r.decodedBuffer = r.decodedBuffer[:0]
		return nil
	}
	compressedLength := end - start
	if compressedLength > maxDocValueCompressedChunkSize {
		return corruptError("segment %q has an oversized doc-value chunk", r.path)
	}
	if cap(r.compressedBuffer) < int(compressedLength) {
		r.compressedBuffer = make([]byte, int(compressedLength))
	} else {
		r.compressedBuffer = r.compressedBuffer[:int(compressedLength)]
	}
	if readErr := readRepairDocValueBytes(r.file, r.size, r.fieldDataOffset+start, r.compressedBuffer, r.path); readErr != nil {
		return readErr
	}

	decoder := byteDecoder{payload: r.compressedBuffer}
	documentCount, countErr := decoder.uvarint()
	if countErr != nil {
		return countErr
	}
	if documentCount == 0 || documentCount > docValueDocumentsPerChunk {
		return corruptError("segment %q has an invalid doc-value chunk document count", r.path)
	}
	if cap(r.header) < int(documentCount) {
		r.header = make([]repairDocValueMeta, int(documentCount))
	} else {
		r.header = r.header[:int(documentCount)]
	}
	chunkFirstDocument := chunkNumber * docValueDocumentsPerChunk
	chunkLastDocument := chunkFirstDocument + docValueDocumentsPerChunk
	if chunkLastDocument > r.totalDocumentCount {
		chunkLastDocument = r.totalDocumentCount
	}
	var previousDocument, previousValueEnd uint64
	for headerIndex := range r.header {
		documentDelta, documentErr := decoder.uvarint()
		if documentErr != nil {
			return documentErr
		}
		if headerIndex > 0 && documentDelta == 0 {
			return corruptError("segment %q has repeated doc-value document numbers", r.path)
		}
		if previousDocument > math.MaxUint64-documentDelta {
			return corruptError("segment %q has overflowing doc-value document numbers", r.path)
		}
		documentNumber := previousDocument + documentDelta
		if documentNumber < chunkFirstDocument || documentNumber >= chunkLastDocument {
			return corruptError("segment %q has a doc-value document outside its chunk", r.path)
		}
		valueDelta, valueErr := decoder.uvarint()
		if valueErr != nil {
			return valueErr
		}
		if valueDelta == 0 || previousValueEnd > math.MaxUint64-valueDelta {
			return corruptError("segment %q has invalid doc-value value offsets", r.path)
		}
		valueEnd := previousValueEnd + valueDelta
		r.header[headerIndex] = repairDocValueMeta{documentNumber: documentNumber, valueEnd: valueEnd}
		previousDocument = documentNumber
		previousValueEnd = valueEnd
	}
	compressedValues := decoder.payload[decoder.offset:]
	decodedLength, lengthErr := storedChunkDecodedLength(compressedValues)
	if lengthErr != nil {
		return corruptError("decode doc-value chunk length in segment %q: %w", r.path, lengthErr)
	}
	if decodedLength < 0 || decodedLength > maxDocValueDecodedChunkSize {
		return corruptError("segment %q has an oversized decoded doc-value chunk", r.path)
	}
	if cap(r.decodedBuffer) < decodedLength {
		r.decodedBuffer = make([]byte, decodedLength)
	} else {
		r.decodedBuffer = r.decodedBuffer[:decodedLength]
	}
	decoded, decodeErr := decodeStoredChunk(r.decodedBuffer[:0], compressedValues)
	if decodeErr != nil {
		return corruptError("decode doc-value chunk in segment %q: %w", r.path, decodeErr)
	}
	if len(decoded) != decodedLength || previousValueEnd > uint64(len(decoded)) {
		return corruptError("segment %q has invalid decoded doc-value offsets", r.path)
	}
	r.decodedBuffer = decoded
	r.chunkNumber = chunkNumber
	return nil
}

func readRepairDocValueBytes(file *os.File, size, offset uint64, data []byte, path string) error {
	length := uint64(len(data))
	if offset > size || length > size-offset {
		return corruptError("segment %q reads outside its doc-value data", path)
	}
	readCount, readErr := file.ReadAt(data, int64(offset))
	if readErr != nil || readCount != len(data) {
		return corruptError("read doc-value data from segment %q", path, readErr)
	}
	return nil
}

func decodeRepairDocValueTerms(encoded []byte, path string) ([]byte, error) {
	var smallest []byte
	found := false
	for len(encoded) > 0 {
		value, remaining, valueErr := decodeRepairDocValueTerm(encoded)
		if valueErr != nil {
			return nil, corruptError("decode doc-value term in segment %q: %w", path, valueErr)
		}
		if !found || bytes.Compare(value, smallest) < 0 {
			smallest = value
			found = true
		}
		encoded = remaining
	}
	if !found {
		return nil, corruptError("segment %q has an empty doc-value entry", path)
	}
	return smallest, nil
}

func decodeRepairDocValueTerm(encoded []byte) ([]byte, []byte, error) {
	if len(encoded) == 0 {
		return nil, nil, errors.New("empty term")
	}
	var unescaped []byte
	for sourceIndex := 0; sourceIndex < len(encoded); sourceIndex++ {
		switch encoded[sourceIndex] {
		case 0xff:
			if unescaped == nil {
				if sourceIndex > maxRepairSortValueLength {
					return nil, nil, fmt.Errorf("term exceeds %d bytes", maxRepairSortValueLength)
				}
				return encoded[:sourceIndex], encoded[sourceIndex+1:], nil
			}
			return unescaped, encoded[sourceIndex+1:], nil
		case '\\':
			if sourceIndex+1 >= len(encoded) {
				return nil, nil, errors.New("truncated term escape")
			}
			if unescaped == nil {
				if sourceIndex > maxRepairSortValueLength {
					return nil, nil, fmt.Errorf("term exceeds %d bytes", maxRepairSortValueLength)
				}
				unescaped = make([]byte, sourceIndex, maxRepairSortValueLength)
				copy(unescaped, encoded[:sourceIndex])
			}
			sourceIndex++
		}
		if unescaped != nil {
			if len(unescaped) >= maxRepairSortValueLength {
				return nil, nil, fmt.Errorf("term exceeds %d bytes", maxRepairSortValueLength)
			}
			unescaped = append(unescaped, encoded[sourceIndex])
		}
	}
	return nil, nil, errors.New("unterminated term")
}

func copyRepairPageValueInto(destination, value []byte) []byte {
	if value == nil {
		return nil
	}
	if destination == nil || cap(destination) < len(value) {
		destination = make([]byte, len(value))
	} else {
		destination = destination[:len(value)]
	}
	copy(destination, value)
	return destination
}
