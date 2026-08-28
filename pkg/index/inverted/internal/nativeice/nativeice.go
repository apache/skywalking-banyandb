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
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"

	roaringpkg "github.com/RoaringBitmap/roaring"
)

const (
	segmentFooterLength = 60
	segmentVersion      = 3
	snapshotVersion     = 3
	maxManifestSize     = 16 << 20
	maxFieldsIndexCount = 1 << 20
	maxDirectoryEntries = 1 << 16
	directoryReadSize   = maxDirectoryEntries + 1
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
	visibleDocCount int64
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
	snapshotPaths, segmentPaths, snapshotErr := committedSnapshots(path)
	if snapshotErr != nil {
		return nil, snapshotErr
	}
	for snapshotIndex := len(snapshotPaths) - 1; snapshotIndex >= 0; snapshotIndex-- {
		snapshotPath := snapshotPaths[snapshotIndex]
		manifest, readErr := readManifest(snapshotPath)
		if readErr != nil {
			continue
		}
		visibleDocCount, parseErr := parseSnapshot(segmentPaths, manifest)
		if parseErr != nil {
			continue
		}
		return &Reader{visibleDocCount: visibleDocCount}, nil
	}
	return nil, corruptError("open %q has no structurally complete snapshot", path)
}

// VisibleDocCount returns the number of live documents in the pinned
// generation: the document counts the manifest records for the segments it
// references, less the documents those segments' deletion masks mark.
func (r *Reader) VisibleDocCount() (int64, error) {
	return r.visibleDocCount, nil
}

// Close releases the file handles and mappings the Reader pinned at Open.
func (r *Reader) Close() error {
	return nil
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

func parseSnapshot(segmentPaths map[uint64]string, payload []byte) (int64, error) {
	if len(payload) < 4 {
		return 0, corruptError("snapshot is shorter than its reserved CRC32", nil)
	}
	decoder := byteDecoder{payload: payload[:len(payload)-4]}
	version, versionErr := decoder.uvarint()
	if versionErr != nil {
		return 0, versionErr
	}
	if version != snapshotVersion {
		return 0, corruptError("unsupported snapshot version %d", version)
	}
	segmentCount, countErr := decoder.uvarint()
	if countErr != nil {
		return 0, countErr
	}
	if segmentCount > uint64(len(decoder.payload)) {
		return 0, corruptError("snapshot segment count %d exceeds remaining bytes", segmentCount)
	}
	var visibleDocCount int64
	for segmentIndex := uint64(0); segmentIndex < segmentCount; segmentIndex++ {
		segmentRecord, recordErr := decoder.segmentRecord(segmentPaths)
		if recordErr != nil {
			return 0, recordErr
		}
		segmentDocCount, segmentErr := validateSegment(segmentRecord)
		if segmentErr != nil {
			return 0, segmentErr
		}
		if segmentDocCount != segmentRecord.documentCount {
			return 0, corruptError("segment %d document count differs from snapshot", segmentRecord.id)
		}
		deletedCount, deletionErr := deletionCount(segmentRecord.deletionBitmap, segmentRecord.documentCount)
		if deletionErr != nil {
			return 0, deletionErr
		}
		if segmentRecord.documentCount > uint64(math.MaxInt64) || deletedCount > segmentRecord.documentCount {
			return 0, corruptError("invalid document count for segment %d", segmentRecord.id)
		}
		segmentVisibleCount := int64(segmentRecord.documentCount - deletedCount)
		if segmentVisibleCount > math.MaxInt64-visibleDocCount {
			return 0, corruptError("visible document count overflows int64", nil)
		}
		visibleDocCount += segmentVisibleCount
	}
	if decoder.remaining() != 0 {
		return 0, corruptError("snapshot has trailing bytes before its reserved CRC32", nil)
	}
	return visibleDocCount, nil
}

type segmentRecord struct {
	path           string
	deletionBitmap []byte
	documentCount  uint64
	id             uint64
	timeMin        uint64
	timeMax        uint64
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

func validateSegment(record segmentRecord) (uint64, error) {
	file, openErr := os.Open(record.path)
	if errors.Is(openErr, os.ErrNotExist) {
		return 0, corruptError("open missing segment %d", record.id)
	}
	if openErr != nil {
		return 0, corruptError("open segment %q", record.path, openErr)
	}
	defer func() {
		_ = file.Close()
	}()
	info, statErr := file.Stat()
	if statErr != nil {
		return 0, corruptError("stat segment %q", record.path, statErr)
	}
	if !info.Mode().IsRegular() || info.Size() < segmentFooterLength {
		return 0, corruptError("segment %q is shorter than its footer", record.path)
	}
	footerOffset := info.Size() - segmentFooterLength
	var footer [segmentFooterLength]byte
	if _, readErr := file.ReadAt(footer[:], footerOffset); readErr != nil {
		return 0, corruptError("read footer from segment %q", record.path, readErr)
	}
	documentCount := binary.BigEndian.Uint64(footer[0:8])
	storedIndex := binary.BigEndian.Uint64(footer[8:16])
	fieldsIndex := binary.BigEndian.Uint64(footer[16:24])
	docValues := binary.BigEndian.Uint64(footer[24:32])
	chunkMode := binary.BigEndian.Uint32(footer[32:36])
	timeMin := binary.BigEndian.Uint64(footer[36:44])
	timeMax := binary.BigEndian.Uint64(footer[44:52])
	version := binary.BigEndian.Uint32(footer[52:56])
	if version != segmentVersion {
		return 0, corruptError("unsupported segment version %d", version)
	}
	if chunkMode == 0 || storedIndex > docValues || docValues > fieldsIndex || fieldsIndex > uint64(footerOffset) {
		return 0, corruptError("segment %q has invalid section roots", record.path)
	}
	if documentCount > uint64(math.MaxInt64) || documentCount > (docValues-storedIndex)/8 {
		return 0, corruptError("segment %q has invalid document count", record.path)
	}
	if (uint64(footerOffset)-fieldsIndex)%8 != 0 {
		return 0, corruptError("segment %q has a misaligned fields index", record.path)
	}
	if (uint64(footerOffset)-fieldsIndex)/8 > maxFieldsIndexCount {
		return 0, corruptError("segment %q has too many fields index entries", record.path)
	}
	for fieldIndexOffset := fieldsIndex; fieldIndexOffset < uint64(footerOffset); fieldIndexOffset += 8 {
		var fieldRecord [8]byte
		if _, readErr := file.ReadAt(fieldRecord[:], int64(fieldIndexOffset)); readErr != nil {
			return 0, corruptError("read fields index from segment %q", record.path, readErr)
		}
		fieldRecordOffset := binary.BigEndian.Uint64(fieldRecord[:])
		if fieldRecordOffset >= fieldsIndex {
			return 0, corruptError("segment %q has a field record outside its section", record.path)
		}
	}
	if timeMin != record.timeMin || timeMax != record.timeMax {
		return 0, corruptError("segment %d time bounds differ from snapshot", record.id)
	}
	return documentCount, nil
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
