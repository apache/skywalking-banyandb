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

package inverted

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"testing"
	"time"

	roaringpkg "github.com/RoaringBitmap/roaring"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
)

// The NIDX-01B corpus extends the NIDX-01A one from a single committed
// generation to the multi-generation directory issue #14009 specifies: a valid
// generation 10 over two segments holding five physical documents with one
// snapshot deletion, and an invalid newer generation 11 that references a
// segment absent from the directory. Every byte is produced by BanyanDB's
// compatibility writer through the pkg/index/inverted store boundary and
// checked in; production code never generates it, and the generator lives in
// this test file behind nidx01bGenerateEnv.
const (
	nidx01bRoot        = "testdata/nidx01b"
	nidx01bIndexDir    = nidx01bRoot + "/index"
	nidx01bDetachedDir = nidx01bRoot + "/detached"
	nidx01bManifest    = nidx01bRoot + "/provenance.json"

	// nidx01bGenerateEnv gates regeneration so an ordinary test run can never
	// overwrite the checked-in bytes the contract is pinned to.
	nidx01bGenerateEnv = "GENERATE_NIDX01B_FIXTURE"
	nidx01bGenerateCmd = "GENERATE_NIDX01B_FIXTURE=1 go test ./pkg/index/inverted/ -run TestGenerateNIDX01BFixture -count=1"

	// The literals below are declared by issue #14009 and are not values any
	// reader computes. Generation 10 is the newest structurally complete
	// generation; it spans two segments holding five physical documents of
	// which one, doc-22, carries a snapshot deletion, so four are visible.
	// Generation 11 is newer and references a segment the directory does not
	// hold; restoring that segment makes generation 11 complete, and its five
	// undeleted documents then become the answer instead.
	nidx01bSelectedGeneration = uint64(10)
	nidx01bNewerGeneration    = uint64(11)
	nidx01bVisibleCount       = int64(4)
	nidx01bRestoredCount      = int64(5)
	nidx01bPhysicalDocuments  = uint64(5)
	nidx01bSelectedSegments   = 2
	nidx01bDeletions          = 1

	// nidx01bDeletedDocID is doc-22, the document issue #14009 deletes in the
	// selected generation. nidx01bPublishedDocID is doc-26, the document a
	// retained compatibility writer publishes while counts run.
	nidx01bDeletedDocID   = uint64(22)
	nidx01bPublishedDocID = uint64(26)

	// nidx01bDetachedDocuments is the physical document count of the one
	// segment generation 11 references that the index directory withholds. The
	// segment holds doc-26 alone.
	nidx01bDetachedDocuments = uint64(1)

	compatibilityCaptureInterval = time.Millisecond
	compatibilityCaptureLimit    = 15 * time.Second
)

// Grammar constants from BDB-NIDX-SPEC-001 revision 0.2 sections 08 and 09,
// read off the compatibility writer's serialization order rather than off the
// native reader, so a corpus assertion cannot agree with the reader by
// construction. A segment footer is fixed at 60 big-endian bytes holding
// numDocs, the stored-fields index root, the fields index root, the doc-values
// root, the chunk mode, the two timestamp bounds, the version, and a reserved
// CRC32; a manifest is a snapshot version, a segment-record count, the records,
// and a reserved CRC32.
const (
	indexFileNameFormat = "%012x"

	iceSegmentType    = "ice"
	iceSegmentVersion = uint32(3)

	snapshotGrammarVersion = uint64(3)
	reservedCRC32Width     = 4

	iceFooterStoredIndexStart = 8
	iceFooterStoredIndexEnd   = 16
	iceFooterVersionStart     = 52
	iceFooterVersionEnd       = 56
)

// nidx01bFirstBatch and nidx01bSecondBatch are the two commits that seal the
// two segments of the selected generation, in insertion order. doc-22 is the
// second document of the first segment, so its local ordinal is 1; the
// generator records the ordinal it observes rather than assuming that.
var (
	nidx01bFirstBatch  = []uint64{21, 22, 23}
	nidx01bSecondBatch = []uint64{24, 25}
)

// nidx01bProvenance is the manifest checked in beside the corpus bytes. It
// declares generation identifiers, segment membership, physical document
// counts, the deleted ordinal, every file hash, and the expected counts, all
// derived from the compatibility writer's own serialization rather than from
// the native reader under test.
type nidx01bProvenance struct {
	Oracle           map[string]string   `json:"oracle"`
	FileSHA256       map[string]string   `json:"file_sha256"`
	ReservedCRC32    map[string]string   `json:"reserved_crc32"`
	GeneratorCommand string              `json:"generator_command"`
	Notes            string              `json:"notes"`
	DetachedSegment  string              `json:"detached_segment"`
	Generations      []nidx01bGeneration `json:"generations"`
	DeletedDocID     uint64              `json:"deleted_doc_id"`
	VisibleCount     int64               `json:"visible_count"`
	RestoredCount    int64               `json:"restored_count"`
}

// nidx01bGeneration describes one committed generation of the corpus.
type nidx01bGeneration struct {
	File                 string           `json:"file"`
	Segments             []nidx01bSegment `json:"segments"`
	ID                   uint64           `json:"id"`
	StructurallyComplete bool             `json:"structurally_complete"`
}

// nidx01bSegment describes one segment record inside a generation's manifest.
type nidx01bSegment struct {
	File              string   `json:"file"`
	DeletedOrdinals   []uint32 `json:"deleted_ordinals"`
	ID                uint64   `json:"id"`
	PhysicalDocuments uint64   `json:"physical_documents"`
	PresentInIndexDir bool     `json:"present_in_index_dir"`
}

// snapshotSegmentRecord is one segment record of a snapshot v3 manifest,
// decoded straight from the grammar BDB-NIDX-SPEC-001 section 09 defines and
// the compatibility writer emits: a variable-length type string, a four-byte
// big-endian segment version, a variable-length segment identifier, four
// eight-byte big-endian values for accounting size, physical document count and
// the two timestamp bounds, and a variable-length deletion bitmap. The byte
// offsets let tests damage one named field of one named record.
type snapshotSegmentRecord struct {
	deletedOrdinals   []uint32
	deletionBytes     []byte
	id                uint64
	physicalDocuments uint64
	docCountOffset    int
	deletionLenStart  int
	deletionEnd       int
}

// capturedGeneration is one committed generation lifted out of a live index
// directory: the manifest bytes and the bytes of every segment it references.
type capturedGeneration struct {
	segments map[uint64][]byte
	source   string
	manifest []byte
	records  []snapshotSegmentRecord
}

// TestGenerateNIDX01BFixture rebuilds the checked-in NIDX-01B corpus and its
// provenance manifest with BanyanDB's compatibility writer. It runs only when
// nidx01bGenerateEnv is set, so a normal test run reads the committed bytes
// instead of producing fresh ones.
//
// The writer reclaims deletions by merging, so the two-segment generation that
// carries the deletion of doc-22 is superseded within milliseconds of the
// delete landing. The generator therefore lifts each generation out of the live
// directory as soon as the commit that produced it returns, rather than reading
// the directory's final state.
func TestGenerateNIDX01BFixture(t *testing.T) {
	if os.Getenv(nidx01bGenerateEnv) != "1" {
		t.Skipf("set %s=1 to regenerate the NIDX-01B corpus", nidx01bGenerateEnv)
	}
	tester := require.New(t)

	staging := t.TempDir()
	writer, err := NewStore(StoreOpts{Path: staging})
	tester.NoError(err)
	tester.NoError(writer.Batch(nidx01bBatch(nidx01bFirstBatch)))
	tester.NoError(writer.Batch(nidx01bBatch(nidx01bSecondBatch)))
	tester.NoError(writer.Delete([][]byte{convert.Uint64ToBytes(nidx01bDeletedDocID)}))

	selected := captureGeneration(t, staging, "the selected generation", func(records []snapshotSegmentRecord) bool {
		return len(records) == nidx01bSelectedSegments &&
			totalPhysicalDocuments(records) == nidx01bPhysicalDocuments &&
			totalDeletions(records) == nidx01bDeletions
	})

	tester.NoError(writer.Batch(nidx01bBatch([]uint64{nidx01bPublishedDocID})))
	selectedIDs := segmentIDsOf(selected.records)
	newer := captureGeneration(t, staging, "the newer generation", func(records []snapshotSegmentRecord) bool {
		if len(records) != nidx01bSelectedSegments || totalPhysicalDocuments(records) != nidx01bPhysicalDocuments {
			return false
		}
		if totalDeletions(records) != 0 {
			return false
		}
		for _, record := range records {
			if _, shared := selectedIDs[record.id]; shared {
				return false
			}
		}
		return true
	})
	tester.NoError(writer.Close())

	withheld := smallestSegmentRecord(newer.records)
	tester.Equal(nidx01bDetachedDocuments, withheld.physicalDocuments,
		"generation 11 must withhold the segment holding doc-26 alone")

	tester.NoError(os.RemoveAll(nidx01bRoot))
	tester.NoError(os.MkdirAll(nidx01bIndexDir, 0o755))
	tester.NoError(os.MkdirAll(nidx01bDetachedDir, 0o755))

	hashes := map[string]string{}
	record := func(relativeDir, name string, payload []byte) {
		tester.NoError(os.WriteFile(filepath.Join(nidx01bRoot, relativeDir, name), payload, 0o600))
		sum := sha256.Sum256(payload)
		hashes[relativeDir+"/"+name] = hex.EncodeToString(sum[:])
	}
	record("index", generationFileName(nidx01bSelectedGeneration), selected.manifest)
	record("index", generationFileName(nidx01bNewerGeneration), newer.manifest)
	for _, segmentID := range sortedSegmentIDs(selected.segments) {
		record("index", segmentFileName(segmentID), withReservedCRC32Fill(selected.segments[segmentID]))
	}
	for _, segmentID := range sortedSegmentIDs(newer.segments) {
		payload := withReservedCRC32Fill(newer.segments[segmentID])
		if segmentID == withheld.id {
			record("detached", segmentFileName(segmentID), payload)
			continue
		}
		record("index", segmentFileName(segmentID), payload)
	}

	manifest := nidx01bProvenance{
		Oracle:           readCompatibilityOracleIdentity(t),
		GeneratorCommand: nidx01bGenerateCmd,
		FileSHA256:       hashes,
		DeletedDocID:     nidx01bDeletedDocID,
		VisibleCount:     nidx01bVisibleCount,
		RestoredCount:    nidx01bRestoredCount,
		DetachedSegment:  "detached/" + segmentFileName(withheld.id),
		Generations: []nidx01bGeneration{
			describeGeneration(nidx01bSelectedGeneration, selected.records, map[uint64]bool{}),
			describeGeneration(nidx01bNewerGeneration, newer.records, map[uint64]bool{withheld.id: true}),
		},
		ReservedCRC32: map[string]string{
			segExt: hex.EncodeToString(reservedCRC32FillSegment) + " (arbitrary, no writer computed it)",
			snpExt: "as written by the oracle; the compatibility loader validates this slot, " +
				"so the corpus keeps it readable and the reserved-field contract is proved by run-time mutation",
		},
		Notes: "Generation 10 spans two segments holding five physical documents with one snapshot deletion for doc-22, " +
			"so four are visible. Generation 11 is newer and references a segment held in detached/ rather than index/, " +
			"so it is structurally incomplete until that segment is restored, after which five are visible.",
	}
	encoded, err := json.MarshalIndent(manifest, "", "  ")
	tester.NoError(err)
	tester.NoError(os.WriteFile(nidx01bManifest, append(encoded, '\n'), 0o600))
}

// nidx01bBatch builds a commit of keyword documents with the given numeric
// document identifiers, matching the shape the NIDX-01A corpus uses.
func nidx01bBatch(docIDs []uint64) index.Batch {
	fieldKey := index.FieldKey{
		Analyzer:    index.AnalyzerKeyword,
		SeriesID:    common.SeriesID(1),
		IndexRuleID: 1,
	}
	documents := make(index.Documents, 0, len(docIDs))
	for _, docID := range docIDs {
		field := index.NewStringField(fieldKey, "nidx01b")
		field.Index = true
		field.Store = true
		documents = append(documents, index.Document{DocID: docID, Fields: []index.Field{field}})
	}
	return index.Batch{Documents: documents}
}

// captureGeneration polls the live index directory until it holds a committed
// manifest the accept predicate selects, and returns that manifest together
// with the bytes of every segment it references. Files are read twice and
// compared, because the compatibility writer streams each file into its final
// name in place and a reader can otherwise lift a half-written prefix out.
func captureGeneration(t *testing.T, dir, label string, accept func([]snapshotSegmentRecord) bool) capturedGeneration {
	t.Helper()
	deadline := time.Now().Add(compatibilityCaptureLimit)
	for {
		manifests, globErr := filepath.Glob(filepath.Join(dir, "*"+snpExt))
		require.NoError(t, globErr)
		sort.Strings(manifests)
		for _, manifestPath := range manifests {
			captured, ok := captureFrom(dir, manifestPath, accept)
			if ok {
				return captured
			}
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("%s never appeared in %s within %s", label, dir, compatibilityCaptureLimit)
		}
		time.Sleep(compatibilityCaptureInterval)
	}
}

// captureFrom lifts one candidate manifest and its segments out of a live
// directory, reporting whether the candidate was complete, stable and accepted.
func captureFrom(dir, manifestPath string, accept func([]snapshotSegmentRecord) bool) (capturedGeneration, bool) {
	payload, stable := readStable(manifestPath)
	if !stable {
		return capturedGeneration{}, false
	}
	records, parseErr := parseCompatibilityManifest(payload)
	if parseErr != nil || !accept(records) {
		return capturedGeneration{}, false
	}
	segments := make(map[uint64][]byte, len(records))
	for _, segmentRecord := range records {
		segmentPayload, segmentStable := readStable(filepath.Join(dir, segmentFileName(segmentRecord.id)))
		if !segmentStable || !hasCompleteSegmentFooter(segmentPayload) {
			return capturedGeneration{}, false
		}
		segments[segmentRecord.id] = segmentPayload
	}
	return capturedGeneration{manifest: payload, segments: segments, records: records, source: manifestPath}, true
}

// readStable reads path twice and reports the bytes only when both reads agree,
// so a file still being streamed into place is skipped rather than captured.
func readStable(path string) ([]byte, bool) {
	first, firstErr := os.ReadFile(path)
	if firstErr != nil {
		return nil, false
	}
	second, secondErr := os.ReadFile(path)
	if secondErr != nil || !bytes.Equal(first, second) {
		return nil, false
	}
	return first, true
}

// hasCompleteSegmentFooter reports whether payload ends in a whole ICE v3
// footer, which is the cheapest proof that a segment file was fully written.
func hasCompleteSegmentFooter(payload []byte) bool {
	if len(payload) < iceFooterLength {
		return false
	}
	footer := payload[len(payload)-iceFooterLength:]
	return binary.BigEndian.Uint32(footer[iceFooterVersionStart:iceFooterVersionEnd]) == iceSegmentVersion
}

// parseCompatibilityManifest decodes the segment records of a snapshot v3
// manifest and the byte offsets of the fields tests damage.
func parseCompatibilityManifest(payload []byte) ([]snapshotSegmentRecord, error) {
	if len(payload) < reservedCRC32Width {
		return nil, fmt.Errorf("manifest of %d bytes is shorter than its reserved CRC32", len(payload))
	}
	body := payload[:len(payload)-reservedCRC32Width]
	cursor := 0
	readUvarint := func() (uint64, error) {
		value, width := binary.Uvarint(body[cursor:])
		if width <= 0 {
			return 0, fmt.Errorf("invalid variable-length integer at offset %d", cursor)
		}
		cursor += width
		return value, nil
	}
	readFixed := func(width int) ([]byte, error) {
		if cursor+width > len(body) {
			return nil, fmt.Errorf("want %d bytes at offset %d, have %d", width, cursor, len(body)-cursor)
		}
		value := body[cursor : cursor+width]
		cursor += width
		return value, nil
	}
	version, versionErr := readUvarint()
	if versionErr != nil {
		return nil, versionErr
	}
	if version != snapshotGrammarVersion {
		return nil, fmt.Errorf("unsupported snapshot version %d", version)
	}
	count, countErr := readUvarint()
	if countErr != nil {
		return nil, countErr
	}
	if count > uint64(len(body)) {
		return nil, fmt.Errorf("segment count %d exceeds remaining bytes", count)
	}
	records := make([]snapshotSegmentRecord, 0, count)
	for range int(count) {
		parsed, recordErr := parseCompatibilitySegmentRecord(&cursor, readUvarint, readFixed)
		if recordErr != nil {
			return nil, recordErr
		}
		records = append(records, parsed)
	}
	if cursor != len(body) {
		return nil, fmt.Errorf("manifest has %d trailing bytes before its reserved CRC32", len(body)-cursor)
	}
	return records, nil
}

// parseCompatibilitySegmentRecord decodes one segment record at the cursor.
func parseCompatibilitySegmentRecord(cursor *int,
	readUvarint func() (uint64, error), readFixed func(int) ([]byte, error),
) (snapshotSegmentRecord, error) {
	typeLength, typeLengthErr := readUvarint()
	if typeLengthErr != nil {
		return snapshotSegmentRecord{}, typeLengthErr
	}
	segmentType, typeErr := readFixed(int(typeLength))
	if typeErr != nil {
		return snapshotSegmentRecord{}, typeErr
	}
	if string(segmentType) != iceSegmentType {
		return snapshotSegmentRecord{}, fmt.Errorf("unsupported segment type %q", string(segmentType))
	}
	segmentVersionBytes, segmentVersionErr := readFixed(4)
	if segmentVersionErr != nil {
		return snapshotSegmentRecord{}, segmentVersionErr
	}
	if binary.BigEndian.Uint32(segmentVersionBytes) != iceSegmentVersion {
		return snapshotSegmentRecord{}, fmt.Errorf("unsupported segment version %d", binary.BigEndian.Uint32(segmentVersionBytes))
	}
	id, idErr := readUvarint()
	if idErr != nil {
		return snapshotSegmentRecord{}, idErr
	}
	if _, sizeErr := readFixed(8); sizeErr != nil {
		return snapshotSegmentRecord{}, sizeErr
	}
	docCountOffset := *cursor
	docCountBytes, docCountErr := readFixed(8)
	if docCountErr != nil {
		return snapshotSegmentRecord{}, docCountErr
	}
	if _, boundsErr := readFixed(16); boundsErr != nil {
		return snapshotSegmentRecord{}, boundsErr
	}
	deletionLenStart := *cursor
	deletionLength, deletionLengthErr := readUvarint()
	if deletionLengthErr != nil {
		return snapshotSegmentRecord{}, deletionLengthErr
	}
	deletionBytes, deletionErr := readFixed(int(deletionLength))
	if deletionErr != nil {
		return snapshotSegmentRecord{}, deletionErr
	}
	ordinals, ordinalErr := decodeDeletedOrdinals(deletionBytes)
	if ordinalErr != nil {
		return snapshotSegmentRecord{}, ordinalErr
	}
	return snapshotSegmentRecord{
		deletedOrdinals:   ordinals,
		deletionBytes:     deletionBytes,
		id:                id,
		physicalDocuments: binary.BigEndian.Uint64(docCountBytes),
		docCountOffset:    docCountOffset,
		deletionLenStart:  deletionLenStart,
		deletionEnd:       *cursor,
	}, nil
}

// decodeDeletedOrdinals lists the local document numbers a deletion payload
// marks, in ascending order.
func decodeDeletedOrdinals(payload []byte) ([]uint32, error) {
	if len(payload) == 0 {
		return nil, nil
	}
	bitmap := roaringpkg.New()
	if unmarshalErr := bitmap.UnmarshalBinary(payload); unmarshalErr != nil {
		return nil, fmt.Errorf("decode deletion payload: %w", unmarshalErr)
	}
	return bitmap.ToArray(), nil
}

// encodeDeletedOrdinals builds a deletion payload marking the given local
// document numbers.
func encodeDeletedOrdinals(t *testing.T, ordinals ...uint32) []byte {
	t.Helper()
	payload, err := roaringpkg.BitmapOf(ordinals...).ToBytes()
	require.NoError(t, err)
	return payload
}

func totalPhysicalDocuments(records []snapshotSegmentRecord) uint64 {
	var total uint64
	for _, record := range records {
		total += record.physicalDocuments
	}
	return total
}

func totalDeletions(records []snapshotSegmentRecord) int {
	total := 0
	for _, record := range records {
		total += len(record.deletedOrdinals)
	}
	return total
}

func segmentIDsOf(records []snapshotSegmentRecord) map[uint64]struct{} {
	ids := make(map[uint64]struct{}, len(records))
	for _, record := range records {
		ids[record.id] = struct{}{}
	}
	return ids
}

// smallestSegmentRecord returns the record with the fewest physical documents,
// which is the segment a fresh commit just sealed.
func smallestSegmentRecord(records []snapshotSegmentRecord) snapshotSegmentRecord {
	smallest := records[0]
	for _, record := range records[1:] {
		if record.physicalDocuments < smallest.physicalDocuments {
			smallest = record
		}
	}
	return smallest
}

func sortedSegmentIDs(segments map[uint64][]byte) []uint64 {
	ids := make([]uint64, 0, len(segments))
	for id := range segments {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}

// describeGeneration records a generation's membership for the provenance
// manifest, marking which of its segments the index directory withholds.
func describeGeneration(id uint64, records []snapshotSegmentRecord, withheld map[uint64]bool) nidx01bGeneration {
	segments := make([]nidx01bSegment, 0, len(records))
	complete := true
	for _, record := range records {
		present := !withheld[record.id]
		complete = complete && present
		ordinals := record.deletedOrdinals
		if ordinals == nil {
			ordinals = []uint32{}
		}
		segments = append(segments, nidx01bSegment{
			File:              "index/" + segmentFileName(record.id),
			DeletedOrdinals:   ordinals,
			ID:                record.id,
			PhysicalDocuments: record.physicalDocuments,
			PresentInIndexDir: present,
		})
	}
	return nidx01bGeneration{
		File:                 "index/" + generationFileName(id),
		Segments:             segments,
		ID:                   id,
		StructurallyComplete: complete,
	}
}

// withReservedCRC32Fill overwrites a segment's trailing reserved CRC32 slot
// with arbitrary bytes no writer computed, so the "carried, never checked"
// contract is observable on the checked-in bytes themselves.
func withReservedCRC32Fill(payload []byte) []byte {
	filled := append([]byte(nil), payload...)
	copy(filled[len(filled)-len(reservedCRC32FillSegment):], reservedCRC32FillSegment)
	return filled
}

// generationFileName returns the manifest file name of a generation. Snapshot
// identifiers live in the file name alone and are never embedded in the
// manifest, so a captured generation can be renumbered by renaming it.
func generationFileName(id uint64) string {
	return fmt.Sprintf(indexFileNameFormat, id) + snpExt
}

// segmentFileName returns the file name of a segment. Segment identifiers are
// embedded in every manifest record that references them, so a segment cannot
// be renumbered by renaming it.
func segmentFileName(id uint64) string {
	return fmt.Sprintf(indexFileNameFormat, id) + segExt
}

// loadNIDX01BProvenance reads the checked-in manifest.
func loadNIDX01BProvenance(t *testing.T) nidx01bProvenance {
	t.Helper()
	raw, err := os.ReadFile(nidx01bManifest)
	require.NoError(t, err, "the NIDX-01B provenance manifest must be checked in")
	var manifest nidx01bProvenance
	require.NoError(t, json.Unmarshal(raw, &manifest))
	return manifest
}
